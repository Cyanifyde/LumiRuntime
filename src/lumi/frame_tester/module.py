from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import contextlib
import hashlib
import logging
import secrets
import time
from pathlib import Path

import msgspec
from redis.asyncio import Redis

from lumi.contracts import (
    CaptureSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    ModalFieldSpec,
    ModalSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ReactionOfferSpec,
    ResultEnvelope,
    TemporaryAttachment,
    TypedRequest,
    decode,
    encode,
)

from .artwork import Artwork, ArtworkLoader
from .catalogue import CATEGORY_ORDER, FrameCatalogue, FrameRecord, normalize_search
from .renderer import GRID_PAGE_SIZE, FrameRenderer

LOGGER = logging.getLogger(__name__)
KARUTA_BOT_ID = -2
SESSION_TTL = 900
ATTACHMENT_TTL_MS = 120_000


class FrameTesterSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    source_url: str
    source_reference: str
    artwork_hash: str
    lookup_label: str
    character_name: str = ""
    series_name: str = ""
    edition: int = 0
    category: str = "All"
    search_query: str = ""
    page: int = 0
    selected_frame_id: str = ""


class FrameTesterModule:
    def __init__(
        self,
        *,
        generation: int,
        cache_redis: Redis,
        temp_root: Path,
        catalogue: FrameCatalogue | None = None,
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._temp_root = temp_root
        self._catalogue = catalogue or FrameCatalogue.bundled()
        self._artwork = ArtworkLoader()
        self._renderer = FrameRenderer(self._catalogue.asset_root)

    @property
    def manifest(self) -> Manifest:
        session = r"(?P<session>[a-f0-9]{16})"
        page = r"(?P<page>[0-9]{1,10})"

        def component(
            action: str,
            *,
            input_kind: str = "none",
            modal: ModalSpec | None = None,
        ) -> ComponentSpec:
            return ComponentSpec(
                namespace="frame_tester",
                action=action,
                handler="frame_tester.component",
                grammar=rf"^{session}~{page}$",
                captures=(
                    CaptureSpec("session"),
                    CaptureSpec("page", "integer", minimum=0, maximum=2_147_483_647),
                ),
                deadline_ms=120_000,
                public=True,
                input_kind=input_kind,
                modal=modal,
                session_bound=True,
                presentation_mode="v2",
            )

        return Manifest(
            schema=2,
            module="frame_tester",
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Frame Tester",
            summary="Preview Karuta frames.",
            help=HelpSpec(
                "React 🖌️ on a Karuta card view.",
                "Preview frames and their ticket estimates.",
            ),
            presentation_mode="v2",
            reaction_offers=(
                ReactionOfferSpec(
                    key="karuta_lookup",
                    callback="frame_tester.open",
                    pattern=(
                        r"^(?is)[\s\S]*?\bCharacter[ \t]*·[ \t]*\*\*"
                        r"(?P<character_name>[^\r\n*]{1,100})\*\*[ \t]*\n"
                        r"Series[ \t]*·[ \t]*\*\*"
                        r"(?P<series_name>[^\r\n*]{1,200})\*\*"
                        r"[\s\S]*\bShowing\s+edition\s+(?P<edition>[0-9]{1,3})"
                        r"\s+of\s+(?P<edition_count>[0-9]{1,3})\b[\s\S]*$"
                    ),
                    emoji="🖌️",
                    bot_ids=(KARUTA_BOT_ID,),
                    requires_embeds=True,
                    requires_image=True,
                    captures=(
                        CaptureSpec("character_name", "normalized_text", maximum=100),
                        CaptureSpec("series_name", "normalized_text", maximum=200),
                        CaptureSpec("edition", "integer", minimum=1, maximum=100),
                        CaptureSpec("edition_count", "integer", minimum=1, maximum=100),
                    ),
                    context_fields=(
                        "primary_image_url",
                        "interaction_user_id",
                        "reply_author_id",
                    ),
                    audience_fields=("interaction_user_id", "reply_author_id"),
                    claim_if_missing=True,
                    ttl_ms=SESSION_TTL * 1000,
                    remove_unauthorized=True,
                    remove_after_dispatch=True,
                    presentation_mode="v2",
                ),
            ),
            components=(
                component("previous"),
                component("next"),
                component(
                    "search",
                    input_kind="modal",
                    modal=ModalSpec(
                        "Search frames",
                        (
                            ModalFieldSpec(
                                "query",
                                "Name",
                                minimum=1,
                                maximum=100,
                                placeholder="Frame name",
                            ),
                        ),
                    ),
                ),
                component("clear"),
                component("category", input_kind="string_values"),
                component("select", input_kind="string_values"),
                component("back"),
            ),
            version="1.2.2",
        )

    async def start(self) -> None:
        self._temp_root.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._prune_temp_files)

    async def handle(
        self, request: TypedRequest, job_id: str, generation: int
    ) -> ResultEnvelope:
        try:
            if request.handler == "frame_tester.open":
                intent = await self._open(request)
            elif request.handler == "frame_tester.component":
                intent = await self._component(request)
            else:
                raise ValueError("unknown frame tester handler")
            return self._result(job_id, intent)
        except Exception:
            LOGGER.exception(
                "frame_tester_request_failed",
                extra={
                    "handler": request.handler,
                    "guild_id": request.event.get("guild_id"),
                },
            )
            return self._result(
                job_id,
                DiscordIntent(
                    kind=(
                        "ephemeral"
                        if request.event.get("dispatch") == "component"
                        else "send"
                    ),
                    pages=(PresentationPage(description="Unavailable."),),
                    presentation_mode="v2",
                ),
                status="error",
                error_code="frame_tester_failed",
            )

    async def _open(self, request: TypedRequest) -> DiscordIntent:
        owner_id = int(request.event.get("offer_owner_id") or request.event.get("actor_id") or 0)
        guild_id = int(request.event.get("guild_id") or 0)
        source_url = str(request.event.get("primary_image_url") or "")
        if owner_id <= 0 or guild_id <= 0 or not source_url:
            raise ValueError("frame tester requires a guild owner and source image")
        edition = int(request.arguments.get("edition") or 0)
        edition_count = int(request.arguments.get("edition_count") or 0)
        if edition <= 0 or edition_count <= 0 or edition > edition_count:
            raise ValueError("frame tester received invalid edition metadata")
        artwork = await self._artwork.fetch(source_url)
        source_reference = await self._write_source(artwork.data)
        character_name = str(
            request.arguments.get("character_name")
            or request.arguments.get("lookup_label")
            or ""
        )[:100]
        if character_name.casefold() == "character lookup":
            character_name = ""
        series_name = str(request.arguments.get("series_name") or "")[:200]
        lookup_label = character_name or "Karuta lookup"
        session = FrameTesterSession(
            token=secrets.token_hex(8),
            owner_id=owner_id,
            guild_id=guild_id,
            source_url=source_url,
            source_reference=source_reference,
            artwork_hash=artwork.sha256,
            lookup_label=lookup_label,
            character_name=character_name,
            series_name=series_name,
            edition=edition,
        )
        await self._save_session(session)
        return await self._browser(session, artwork, kind="send")

    async def _component(self, request: TypedRequest) -> DiscordIntent:
        token = str(request.arguments.get("session") or "")
        session = await self._load_session(token)
        actor_id = int(request.event.get("actor_id") or 0)
        if session is None or session.owner_id != actor_id:
            return self._ephemeral("Unavailable.")
        action = request.route.removeprefix("frame_tester.")
        supplied_page = int(request.arguments.get("page") or 0)
        if action in {"previous", "next"}:
            session = msgspec.structs.replace(
                session,
                page=max(0, supplied_page),
                selected_frame_id="",
            )
        elif action == "search":
            values = request.arguments.get("values")
            raw = values.get("query") if isinstance(values, dict) else None
            query = normalize_search(str(raw or ""))
            if not query or len(query) > 100:
                return self._ephemeral("Enter a frame name.")
            session = msgspec.structs.replace(
                session,
                search_query=query,
                page=0,
                selected_frame_id="",
            )
        elif action == "clear":
            session = msgspec.structs.replace(
                session,
                category="All",
                search_query="",
                page=0,
                selected_frame_id="",
            )
        elif action == "category":
            value = self._one_value(request)
            categories = {"All", *CATEGORY_ORDER}
            if value not in categories:
                return self._ephemeral("Unavailable.")
            session = msgspec.structs.replace(
                session,
                category=value,
                page=0,
                selected_frame_id="",
            )
        elif action == "select":
            identifier = self._one_value(request)
            if self._catalogue.get(identifier) is None:
                return self._ephemeral("Unavailable.")
            visible = self._visible_frames(session)
            start = session.page * GRID_PAGE_SIZE
            shown_ids = {
                frame.identifier for frame in visible[start : start + GRID_PAGE_SIZE]
            }
            if identifier not in shown_ids:
                return self._ephemeral("Unavailable.")
            session = msgspec.structs.replace(session, selected_frame_id=identifier)
        elif action == "back":
            session = msgspec.structs.replace(session, selected_frame_id="")
        else:
            return self._ephemeral("Unavailable.")
        artwork, session = await self._session_artwork(session)
        await self._save_session(session)
        return await self._browser(session, artwork, kind="edit")

    async def _browser(
        self,
        session: FrameTesterSession,
        artwork: Artwork,
        *,
        kind: str,
    ) -> DiscordIntent:
        if session.selected_frame_id:
            frame = self._catalogue.get(session.selected_frame_id)
            if frame is None:
                raise ValueError("selected frame no longer exists")
            data = await self._renderer.composite(
                artwork.image,
                artwork.sha256,
                frame,
                character_name=session.character_name,
                series_name=session.series_name,
            )
            attachment = await self._attachment(data, f"frame-{frame.identifier}.webp")
            return DiscordIntent(
                kind=kind,
                pages=(
                    PresentationPage(
                        title=frame.name,
                        description=(
                            f"Category: {frame.category}\n"
                            f"Cost: {frame.cost.detail}\n"
                            f"Acquisition: {frame.requirements or 'No requirements.'}\n"
                            f"Cost snapshot: {frame.cost.as_of}"
                        ),
                        file_name=attachment.file_name,
                        controls=(
                            PresentationControl(
                                kind="button",
                                namespace="frame_tester",
                                action="back",
                                payload=f"{session.token}~{session.page}",
                                label="Back",
                            ),
                        ),
                    ),
                ),
                attachments=(attachment,),
                session_id=session.token,
                presentation_mode="v2",
            )

        visible = self._visible_frames(session)
        page_count = max(1, (len(visible) + GRID_PAGE_SIZE - 1) // GRID_PAGE_SIZE)
        page = min(session.page, page_count - 1)
        if page != session.page:
            session = msgspec.structs.replace(session, page=page)
            await self._save_session(session)
        start = page * GRID_PAGE_SIZE
        shown = visible[start : start + GRID_PAGE_SIZE]
        controls = self._controls(session, shown, page, page_count)
        description_parts = [f"{len(visible)} frames"]
        if session.search_query:
            description_parts.append(f"Search: {session.search_query}")
        if not shown:
            return DiscordIntent(
                kind=kind,
                pages=(
                    PresentationPage(
                        title="Frame Tester",
                        description="No frames.",
                        controls=controls,
                    ),
                ),
                session_id=session.token,
                presentation_mode="v2",
            )
        data = await self._renderer.grid(
            artwork.image,
            artwork.sha256,
            shown,
            character_name=session.character_name,
            series_name=session.series_name,
        )
        attachment = await self._attachment(
            data,
            f"frames-{page + 1}-{artwork.sha256[:12]}.webp",
        )
        return DiscordIntent(
            kind=kind,
            pages=(
                PresentationPage(
                    title="Frame Tester",
                    description=" · ".join(description_parts),
                    file_name=attachment.file_name,
                    controls=controls,
                ),
            ),
            attachments=(attachment,),
            session_id=session.token,
            presentation_mode="v2",
        )

    def _controls(
        self,
        session: FrameTesterSession,
        shown: tuple[FrameRecord, ...],
        page: int,
        page_count: int,
    ) -> tuple[PresentationControl, ...]:
        payload = f"{session.token}~{page}"
        controls: list[PresentationControl] = [
            PresentationControl(
                kind="button",
                namespace="frame_tester",
                action="previous",
                payload=f"{session.token}~{max(0, page - 1)}",
                label="Previous",
                disabled=page == 0,
            ),
            PresentationControl(
                kind="button",
                namespace="frame_tester",
                action="back",
                payload=payload,
                label=f"{page + 1}/{page_count}",
                disabled=True,
            ),
            PresentationControl(
                kind="button",
                namespace="frame_tester",
                action="next",
                payload=f"{session.token}~{min(page_count - 1, page + 1)}",
                label="Next",
                disabled=page >= page_count - 1,
            ),
            PresentationControl(
                kind="button",
                namespace="frame_tester",
                action="search",
                payload=payload,
                label="Search",
            ),
            PresentationControl(
                kind="button",
                namespace="frame_tester",
                action="clear",
                payload=payload,
                label="Clear",
                disabled=session.category == "All" and not session.search_query,
            ),
            PresentationControl(
                kind="string_select",
                namespace="frame_tester",
                action="category",
                payload=payload,
                placeholder="Category",
                options=tuple(
                    PresentationOption(
                        label=category,
                        value=category,
                        default=session.category == category,
                    )
                    for category in ("All", *CATEGORY_ORDER)
                ),
            ),
        ]
        if shown:
            controls.append(
                PresentationControl(
                    kind="string_select",
                    namespace="frame_tester",
                    action="select",
                    payload=payload,
                    placeholder="Frame",
                    options=tuple(
                        PresentationOption(
                            label=f"{index}. {frame.name}"[:100],
                            value=frame.identifier,
                            description=frame.category,
                        )
                        for index, frame in enumerate(shown, start=1)
                    ),
                )
            )
        return tuple(controls)

    def _visible_frames(self, session: FrameTesterSession) -> tuple[FrameRecord, ...]:
        return self._catalogue.filter(
            category=session.category,
            query=session.search_query,
        )

    async def _session_artwork(
        self, session: FrameTesterSession
    ) -> tuple[Artwork, FrameTesterSession]:
        path = self._temp_path(session.source_reference)
        data: bytes | None = None
        with contextlib.suppress(OSError):
            data = await asyncio.to_thread(path.read_bytes)
        if data:
            artwork = await self._artwork.decode(data)
            if artwork.sha256 == session.artwork_hash:
                return artwork, session
        artwork = await self._artwork.fetch(session.source_url)
        reference = await self._write_source(artwork.data)
        return artwork, msgspec.structs.replace(
            session,
            source_reference=reference,
            artwork_hash=artwork.sha256,
        )

    async def _save_session(self, session: FrameTesterSession) -> None:
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.setex(
            f"lumi:frame_tester:session:{session.token}",
            SESSION_TTL,
            encode(session),
        )
        pipeline.setex(
            ui_key("owner", session.token),
            SESSION_TTL,
            str(session.owner_id),
        )
        await pipeline.execute()

    async def _load_session(self, token: str) -> FrameTesterSession | None:
        payload = await self._cache.get(f"lumi:frame_tester:session:{token}")
        if not payload:
            return None
        session = decode(payload, FrameTesterSession)
        pipeline = self._cache.pipeline(transaction=False)
        pipeline.expire(f"lumi:frame_tester:session:{token}", SESSION_TTL)
        pipeline.expire(ui_key("owner", token), SESSION_TTL)
        await pipeline.execute()
        return session

    async def _write_source(self, data: bytes) -> str:
        identifier = secrets.token_hex(24)
        await asyncio.to_thread((self._temp_root / identifier).write_bytes, data)
        return f"tmp:{identifier}"

    async def _attachment(self, data: bytes, file_name: str) -> TemporaryAttachment:
        identifier = secrets.token_hex(24)
        await asyncio.to_thread((self._temp_root / identifier).write_bytes, data)
        return TemporaryAttachment(
            reference=f"tmp:{identifier}",
            file_name=file_name,
            sha256=hashlib.sha256(data).hexdigest(),
            size=len(data),
            expires_at_ms=int(time.time() * 1000) + ATTACHMENT_TTL_MS,
        )

    def _temp_path(self, reference: str) -> Path:
        identifier = reference.removeprefix("tmp:")
        if not identifier or any(character not in "0123456789abcdef" for character in identifier):
            raise ValueError("invalid temporary artwork reference")
        root = self._temp_root.resolve()
        path = (root / identifier).resolve()
        if path.parent != root or path.is_symlink():
            raise ValueError("invalid temporary artwork path")
        return path

    def _prune_temp_files(self) -> None:
        cutoff = time.time() - SESSION_TTL * 2
        with contextlib.suppress(OSError):
            for path in self._temp_root.iterdir():
                if (
                    path.is_file()
                    and len(path.name) == 48
                    and all(character in "0123456789abcdef" for character in path.name)
                    and path.stat().st_mtime < cutoff
                ):
                    path.unlink()

    @staticmethod
    def _one_value(request: TypedRequest) -> str:
        values = request.arguments.get("values")
        if not isinstance(values, (list, tuple)) or len(values) != 1:
            raise ValueError("one selection is required")
        return str(values[0])

    @staticmethod
    def _ephemeral(text: str) -> DiscordIntent:
        return DiscordIntent(
            kind="ephemeral",
            pages=(PresentationPage(description=text),),
            presentation_mode="v2",
        )

    def _result(
        self,
        job_id: str,
        intent: DiscordIntent,
        *,
        status: str = "ok",
        error_code: str | None = None,
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="frame_tester",
            group="cards",
            generation=self._generation,
            status=status,
            intents=(intent,),
            error_code=error_code,
            completed_at_ms=int(time.time() * 1000),
        )

    async def close(self) -> None:
        await self._artwork.close()
