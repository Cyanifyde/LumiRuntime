from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import secrets
import time
from pathlib import Path

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import LockError, RedisError

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    ModalFieldSpec,
    ModalSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
    TemporaryAttachment,
    TypedRequest,
    decode,
    encode,
)
from lumi.frame_tester.artwork import ArtworkLoader
from lumi.karuta_collections.artifacts import ArtifactStore
from lumi.transport import response_state_key
from lumi.ui_keys import session_keys, ui_key
from lumi.worker.actions import ActionUnavailable, EdgeActionClient

from .filters import MAX_QUERY, FilterError, parse_query
from .models import HASH, SketchRecord
from .renderer import PAGE_SIZE, SketchRenderer
from .scanner import fetch_webp, observation, passive_routes

LOGGER = logging.getLogger(__name__)
MODULE = "karuta_sketch_library"
PREFIX = "lumi:" + MODULE + ":"
TTL_MS = 900_000
HELP = (
    "Filters: character/c, series/s, edition/e, print/p, ink.\n"
    "Use : or =. Names match in full, ignoring case. "
    "Numeric filters accept <, <=, >, >=, "
    "ranges such as ink:20k..30k, and edition lists such as e:1,2.\n"
    "Chain comparisons: p<=30>=2 or 2<p<23 (also ink and edition).\n"
    "Order by ink, print/p, edition/e, character/c or series/s. "
    "o0:field ascends; o1:field descends. "
    "o:field uses A-Z names, lowest print, highest ink/edition.\n"
    "Examples: `c:Toji Fushiguro s=Jujutsu Kaisen 2nd Season`, `ink>=20k p<10 o0:print`."
)


class LibrarySession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner: int
    guild: int
    channel: int
    snapshot: str
    query: str
    count: int
    expires: int
    grid_page: int = 0
    detail: int = -1
    revision: int = 0
    last_job: str = ""
    pending: bool = False
    detail_code: str = ""
    visible_codes: tuple[str, ...] = ()
    catalogue_revision: int = -1
    records: tuple[dict, ...] = ()


class KarutaSketchLibraryModule:
    def __init__(
        self,
        *,
        generation,
        cache_redis,
        queue_redis,
        store,
        artifact_root,
        temp_root,
        actions=None,
        clock=None,
        loader=None,
        catalogue_observer=None,
    ):
        self.generation, self.cache, self.store = generation, cache_redis, store
        self.artifacts, self.temp_root = ArtifactStore(Path(artifact_root)), Path(temp_root)
        self.renderer = SketchRenderer(self.artifacts)
        self.actions = actions or EdgeActionClient(
            queue_redis, module=MODULE, group="cards", generation=generation
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.loader = loader or ArtworkLoader()
        self.catalogue_observer = catalogue_observer

    @property
    def manifest(self):
        def component(action, *, input_kind="none", modal=None):
            return ComponentSpec(
                namespace=MODULE,
                action=action,
                handler=MODULE + ".component",
                grammar=r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$",
                captures=(CaptureSpec("session"), CaptureSpec("revision", "integer", minimum=0)),
                input_kind=input_kind,
                modal=modal,
                deadline_ms=60_000,
                acl_key="klsketch",
                public=True,
                session_bound=True,
                presentation_mode="v2",
                modal_defaults=modal is not None,
            )

        return Manifest(
            schema=2,
            module=MODULE,
            group="cards",
            generation=self.generation,
            job_stream="lumi:jobs:cards",
            display_name="Karuta Sketch Library",
            summary="Search and browse saved Karuta sketches.",
            commands=(
                CommandSpec(
                    root="klsketch",
                    handler=MODULE + ".open",
                    grammar=r"^(?P<query>[^\r\n]{0,1000})$",
                    captures=(CaptureSpec("query", required=False, maximum=MAX_QUERY),),
                    help=HelpSpec(
                        ".klsketch [c:name] [s:series] [e:edition] [p:print] [ink:amount]\n"
                        ".klsketch ink>=20k p<10 o0:print\n.klsketch <o0:field|o1:field>",
                        "Browse saved sketches; o0 sorts ascending, o1 descending.",
                    ),
                    acl_key="klsketch",
                    public=True,
                    deadline_ms=60_000,
                    context_fields=("prefix",),
                    presentation_mode="v2",
                ),
            ),
            components=(
                component("previous"),
                component("next"),
                component("select", input_kind="string_values"),
                component("back"),
                component(
                    "search",
                    input_kind="modal",
                    modal=ModalSpec(
                        "Search sketches",
                        (
                            ModalFieldSpec(
                                "query",
                                "Filters",
                                required=False,
                                maximum=MAX_QUERY,
                                placeholder="c:Kishibe ink>=20k o0:print",
                            ),
                        ),
                    ),
                ),
                component(
                    "jump",
                    input_kind="modal",
                    modal=ModalSpec(
                        "Go to page", (ModalFieldSpec("page", "Page", minimum=1, maximum=6),)
                    ),
                ),
            ),
            passive=passive_routes(MODULE, TTL_MS),
            capabilities=("edit_message",),
            version="1.1.1",
        )

    async def start(self):
        self.temp_root.mkdir(parents=True, exist_ok=True)

        try:
            async for key in session_keys(self.cache, "cards", PREFIX + "session:*"):
                raw = await self.cache.get(key)
                if raw:
                    try:
                        session = decode(raw, LibrarySession)
                    except ValueError:
                        continue
                    if session.expires > self.clock():
                        await self._save(session)
        except RedisError:
            LOGGER.warning("sketch_library_resume_unavailable")

    async def close(self):
        await self.loader.close()
        await self.store.close()

    async def handle(self, request: TypedRequest, job_id: str, generation: int):
        if generation != self.generation or self.clock() >= int(
            request.event.get("_expires_at_ms", self.clock() + 1)
        ):
            return self._result(job_id)
        if request.handler == MODULE + ".observe":
            try:
                await self._observe(request)
            except (OSError, ValueError, KeyError, TypeError, RedisError, MySQLError, TimeoutError):

                LOGGER.warning("sketch_library_observation_unavailable")
            return self._result(job_id)
        try:
            if request.handler == MODULE + ".open":
                return self._result(job_id, await self._open(request))
            if request.handler == MODULE + ".component":
                intent = await self._component(request, job_id)
                return self._result(job_id, intent)
            return self._result(job_id)
        except FilterError as exc:
            return self._result(job_id, self._notice(request, str(exc)))
        except (OSError, ValueError, RedisError, MySQLError, ActionUnavailable, TimeoutError):
            LOGGER.warning("sketch_library_unavailable", extra={"handler": request.handler})
            return self._result(job_id, self._notice(request, "Unavailable."))

    async def _observe(self, request):
        info, url = observation(request, MODULE)
        await self.learn_observation(info, url, request.event)

    async def learn_observation(self, info, url, event, *, update_catalogue=True):
        if not await self.store.observe(info, update_catalogue=update_catalogue):
            return
        if update_catalogue and self.catalogue_observer:

            from lumi.karuta_collections.models import Stamp

            await self.catalogue_observer(
                info.character, info.series, info.edition, Stamp(info.created_ms)
            )
        if not info.sketched or not url or await self.store.is_published(info):
            return
        data, source_hash = await fetch_webp(self.loader, url)
        if self.clock() >= int(event.get("_expires_at_ms", self.clock() + 1)):
            return
        blob = await self.artifacts.store(data)
        await self.store.publish(
            info,
            SketchRecord(
                info.code,
                info.character,
                info.series,
                info.edition,
                info.print_num,
                info.ink,
                blob,
                source_hash,
            ),
        )

    async def _open(self, request):
        raw = str(request.arguments.get("query") or "").strip()
        if raw.casefold() == "help":
            prefix = str(request.event.get("prefix") or ".")
            text = (
                f"`{prefix}klsketch c:Toji Fushiguro s=Jujutsu Kaisen 2nd Season`\n"
                f"`{prefix}klsketch ink>=20k p<10 e:2 o0:print`\n\n" + HELP
            )
            return DiscordIntent(
                "send", pages=(PresentationPage(description=text),), presentation_mode="v2"
            )
        query = parse_query(raw)
        owner, guild, channel = (
            int(request.event.get(k) or 0) for k in ("actor_id", "guild_id", "channel_id")
        )
        if min(owner, guild, channel) <= 0:
            raise ValueError("guild context required")
        snapshot, total = await self.store.current()
        if not total or not HASH.fullmatch(snapshot):
            raise ValueError("library not imported")
        session = LibrarySession(
            secrets.token_hex(8),
            owner,
            guild,
            channel,
            snapshot,
            raw,
            0,
            self.clock() + TTL_MS,
        )
        session = self._from_view(session, await self.store.view(snapshot, query, 0))
        intent = await self._render(session, "send")
        await self._save(session)
        return intent

    async def _load(self, token):
        raw = await self.cache.get(PREFIX + "session:" + token)
        session = decode(raw, LibrarySession) if raw else None
        return session if session and session.expires > self.clock() else None

    async def _save(self, session):
        remaining = session.expires - self.clock()
        if remaining <= 0:
            raise ValueError("session expired")
        pipeline = self.cache.pipeline(transaction=True)
        pipeline.set(PREFIX + "session:" + session.token, encode(session), px=remaining)
        pipeline.set(ui_key("owner", session.token), str(session.owner), px=remaining)
        payload = f"{session.token}~{session.revision}"
        for action, values in (
            ("search", {"query": session.query}),
            (
                "jump",
                {"page": str(session.detail + 1 if session.detail >= 0 else session.grid_page + 1)},
            ),
        ):
            context = {
                "owner": session.owner,
                "guild": session.guild,
                "generation": self.generation,
                "values": values,
            }
            pipeline.set(
                f"lumi:modal-context:{MODULE}:{payload}:{action}", json.dumps(context), px=remaining
            )
        await pipeline.execute()

    async def _component(self, request, job_id):
        token = str(request.arguments.get("session") or "")
        if len(token) != 16 or any(c not in "abcdef0123456789" for c in token):
            return self._notice(request, "Unavailable.")
        lock = self.cache.lock(PREFIX + "lock:" + token, timeout=60, blocking_timeout=5)
        if not await lock.acquire():
            return self._notice(request, "Busy. Try again.")
        try:
            session = await self._load(token)
            bound = await self.cache.get(ui_key("message", token))
            if (
                session is None
                or session.owner != int(request.event.get("actor_id") or 0)
                or session.guild != int(request.event.get("guild_id") or 0)
                or session.channel != int(request.event.get("channel_id") or 0)
                or bound is None
                or int(bound) != int(request.event.get("message_id") or 0)
            ):
                return self._notice(request, "This menu has expired.")
            retry = session.pending and session.last_job == job_id
            if session.pending and not retry:


                recovered = await self._render(session, "edit")
                response = await self._deliver(session, recovered, job_id, int(bound))
                if not response.ok and session.last_job != job_id and await lock.owned():


                    session = msgspec.structs.replace(session, last_job=job_id)
                    await self._save(session)
                    recovered = await self._render(session, "edit")
                    response = await self._deliver(session, recovered, job_id, int(bound))
                if not response.ok or not await lock.owned():
                    return self._notice(request, "Unavailable.")
                session = msgspec.structs.replace(session, pending=False)
                await self._save(session)
            if not retry and int(request.arguments.get("revision", -1)) != session.revision:
                return self._notice(request, "Menu updated. Try again.")
            old = session
            if not retry:
                session = await self._change(session, request)
                session = msgspec.structs.replace(
                    session, revision=old.revision + 1, last_job=job_id, pending=True
                )
            intent = await self._render(session, "edit")
            if not await lock.owned() or self.clock() >= session.expires:
                return self._notice(request, "This menu has expired.")
            await self._save(session)
            response = await self._deliver(session, intent, job_id, int(bound))
            if response.ok and await lock.owned():
                await self._save(msgspec.structs.replace(session, pending=False))
                return None
            if (
                getattr(response, "error_code", "") != "ambiguous"
                and not retry
                and await lock.owned()
            ):
                await self._save(old)
            return self._notice(request, "Unavailable.")
        finally:
            with contextlib.suppress(LockError):
                await lock.release()

    async def _deliver(self, session, intent, job_id, message_id):
        response = await self.actions.request(
            job_id=job_id,
            kind="edit_message",
            guild_id=session.guild,
            channel_id=session.channel,
            message_id=message_id,
            intent=intent,
            idempotency_key=f"klsketch:{session.token}:{session.revision}:{session.last_job}",
            wait_seconds=20,
        )
        if response.ok:


            state_key = response_state_key(job_id, "cards")
            state = await self.cache.get(state_key)
            if state in {f"queued:{message_id}", f"queued:{message_id}".encode()}:
                await self.cache.set(state_key, "final", ex=180)
        return response

    async def _change(self, session, request):
        action = request.route.rpartition(".")[2]
        query = parse_query(session.query)
        offset, detail, code, delta = session.grid_page * PAGE_SIZE, False, "", 0
        changed = session
        if action in {"previous", "next"}:
            delta = -1 if action == "previous" else 1
            if session.detail >= 0 and session.detail_code:
                detail, code, offset = True, session.detail_code, session.detail
            else:
                offset = max(0, offset + delta * PAGE_SIZE)
                delta = 0
        values = request.arguments.get("values")
        if action == "back":
            pass
        elif action == "search":
            if not isinstance(values, dict) or not isinstance(values.get("query", ""), str):
                raise FilterError("Invalid filters.")
            raw = values.get("query", "").strip()
            query, offset = parse_query(raw), 0
            changed = msgspec.structs.replace(session, query=raw)
        elif action == "jump":
            raw = str(values.get("page", "")) if isinstance(values, dict) else ""
            if not raw.isascii() or not raw.isdecimal():
                raise FilterError("Enter a valid page number.")
            page = int(raw) - 1
            detail = session.detail >= 0
            offset = page if detail else page * PAGE_SIZE
            if page < 0 or offset > 1_000_000:
                raise FilterError("Enter a valid page number.")
            view = await self.store.view(
                session.snapshot, query, offset, 1 if detail else PAGE_SIZE
            )
            maximum = view.count if detail else max(1, (view.count + PAGE_SIZE - 1) // PAGE_SIZE)
            if not 0 <= page < maximum:
                raise FilterError(f"Enter a page from 1 to {maximum}.")
            return self._from_view(session, view, detail)
        elif action == "select":
            if session.detail >= 0 or not isinstance(values, (list, tuple)) or len(values) != 1:
                raise FilterError("Invalid selection.")
            visible = session.visible_codes
            if session.catalogue_revision < 0:

                visible = tuple(
                    r.code
                    for r in await self.store.page(
                        session.snapshot, query, session.grid_page * PAGE_SIZE
                    )
                )
            if values[0] not in visible:
                raise FilterError("Invalid selection.")
            detail, code = True, values[0]
        elif action not in {"previous", "next"}:
            raise FilterError("Invalid action.")
        view = await self.store.view(
            changed.snapshot, query, offset, 1 if detail else PAGE_SIZE, code=code, delta=delta
        )
        if code and not view.anchor_found:

            if action == "select":
                raise FilterError("Sketch changed. Search again.")
            detail = False
            view = await self.store.view(changed.snapshot, query, session.grid_page * PAGE_SIZE)
        return self._from_view(changed, view, detail)

    @staticmethod
    def _from_view(session, view, detail=False):
        return msgspec.structs.replace(
            session,
            count=view.count,
            catalogue_revision=view.revision,
            grid_page=session.grid_page if detail else view.offset // PAGE_SIZE,
            detail=view.offset if detail and view.records else -1,
            detail_code=view.records[0].code if detail and view.records else "",
            visible_codes=session.visible_codes if detail else tuple(r.code for r in view.records),
            records=tuple(r.to_dict() for r in view.records),
        )

    @staticmethod
    def _pages(session):
        return max(1, (session.count + PAGE_SIZE - 1) // PAGE_SIZE)

    async def _render(self, session, kind):
        query = parse_query(session.query)
        detail = session.detail >= 0
        rows = (
            tuple(SketchRecord.from_dict(r) for r in session.records)
            if session.catalogue_revision >= 0
            else await self.store.page(
                session.snapshot,
                query,
                session.detail if detail else session.grid_page * PAGE_SIZE,
                1 if detail else PAGE_SIZE,
            )
        )
        page = session.detail if detail else session.grid_page
        total = session.count if detail else self._pages(session)
        controls = self._controls(session, page, max(1, total), rows)
        if not rows:
            return DiscordIntent(
                kind,
                pages=(PresentationPage(description="No sketches found.", controls=controls),),
                session_id=session.token,
                presentation_mode="v2",
            )
        if detail:
            record = rows[0]
            data = await self.artifacts.read(record.blob)
            title = record.character
            description = (
                f"{record.series}\n{record.code} · Edition {record.edition} · "
                f"Print {record.print_num:,} · {record.ink:,} ink"
            )
        else:
            data = await self.renderer.grid(
                session.snapshot + ":" + str(session.catalogue_revision), rows
            )
            title, description = "Sketches", f"{session.count:,} sketches"
        attachment = await self._attachment(data)
        return DiscordIntent(
            kind,
            pages=(
                PresentationPage(
                    title=title,
                    description=description,
                    file_name=attachment.file_name,
                    controls=controls,
                ),
            ),
            attachments=(attachment,),
            session_id=session.token,
            presentation_mode="v2",
        )

    def _controls(self, session, page, total, rows):
        payload = f"{session.token}~{session.revision}"

        def button(action, label, disabled=False):
            return PresentationControl(
                "button", MODULE, action, payload, label=label, disabled=disabled
            )

        controls = [
            button("previous", "Previous", page <= 0),
            button("jump", f"{page + 1}/{total}", not session.count),
            button("next", "Next", page + 1 >= total),
            button("search", "Search"),
        ]
        if session.detail >= 0:
            controls.append(button("back", "Back to Results"))
        elif rows:
            controls.append(
                PresentationControl(
                    "string_select",
                    MODULE,
                    "select",
                    payload,
                    placeholder="Open sketch",
                    options=tuple(
                        PresentationOption(
                            label=f"{n}. {r.character}"[:100],
                            value=r.code,
                            description=(
                                f"{r.code} · E{r.edition} · #{r.print_num:,} · {r.ink:,} ink"
                            ),
                        )
                        for n, r in enumerate(rows, 1)
                    ),
                )
            )
        return tuple(controls)

    async def _attachment(self, data):
        reference = secrets.token_hex(32)
        path = self.temp_root / reference
        await asyncio.to_thread(path.write_bytes, data)
        path.chmod(0o640)
        digest = hashlib.sha256(data).hexdigest()
        return TemporaryAttachment(
            "tmp:" + reference,
            "sketch-" + digest[:16] + ".webp",
            digest,
            len(data),
            self.clock() + 120_000,
        )

    @staticmethod
    def _notice(request, text):
        return DiscordIntent(
            "ephemeral" if request.event.get("dispatch") == "component" else "send",
            pages=(PresentationPage(description=text),),
            presentation_mode="v2",
        )

    def _result(self, job_id, intent=None):
        return ResultEnvelope(
            1,
            job_id,
            MODULE,
            "cards",
            self.generation,
            "ok",
            intents=(intent,) if intent else (),
            completed_at_ms=self.clock(),
        )
