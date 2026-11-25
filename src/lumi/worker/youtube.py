from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import contextlib
import hashlib
import logging
import os
import secrets
import time
from pathlib import Path

import msgspec
from redis.asyncio import Redis
from redis.exceptions import RedisError

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    PresentationControl,
    PresentationOption,
    ResultEnvelope,
    TemporaryAttachment,
    TypedRequest,
    decode,
    encode,
)

from .youtube_service import (
    MAX_QUERY_CHARS,
    RedisYouTubeQuota,
    YouTubeBusyError,
    YouTubeNoResultsError,
    YouTubeQuotaError,
    YouTubeResponseError,
    YouTubeResult,
    YouTubeSearchService,
    YouTubeUnavailableError,
    normalize_query,
)

LOGGER = logging.getLogger(__name__)
SESSION_TTL_SECONDS = 15 * 60
GRID_RETENTION_SECONDS = SESSION_TTL_SECONDS * 2
ATTACHMENT_TTL_MS = 120_000
MAX_CAPTURE_CHARS = 1_900


class YouTubeSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    query: str
    results: tuple[YouTubeResult, ...]
    current_video_id: str
    grid_id: str
    grid_sha256: str


class YouTubeModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        temp_root: Path,
        group: str = "tools",
        service: YouTubeSearchService | None = None,
    ) -> None:
        self._generation = generation
        self._group = group
        self._cache = cache_redis
        self._temp_root = temp_root
        self._grid_root = temp_root / "youtube-cache"
        self._storage_ready = False
        self._service = service or YouTubeSearchService(
            quota=RedisYouTubeQuota(queue_redis),
            api_key_file=Path(
                os.getenv(
                    "LUMI_YOUTUBE_API_KEY_FILE",
                    "/run/secrets/youtube_api_key",
                )
            ),
        )

    @property
    def manifest(self) -> Manifest:
        token = r"(?P<session>[a-f0-9]{16})"

        def component(action: str, *, input_kind: str = "none") -> ComponentSpec:
            return ComponentSpec(
                namespace="youtube",
                action=action,
                handler="youtube.component",
                grammar=rf"^{token}$",
                captures=(CaptureSpec("session"),),
                deadline_ms=120_000,
                acl_key="youtube",
                public=True,
                input_kind=input_kind,
                session_bound=True,
                presentation_mode="plain",
            )

        return Manifest(
            schema=2,
            module="youtube",
            group=self._group,
            generation=self._generation,
            job_stream=f"lumi:jobs:{self._group}",
            display_name="YouTube",
            summary="Video search.",
            presentation_mode="plain",
            commands=(
                CommandSpec(
                    root="youtube",
                    handler="youtube.lookup",
                    grammar=r"^(?:\s+(?P<query>[\s\S]+))?$",
                    captures=(
                        CaptureSpec(
                            "query",
                            "normalized_text",
                            required=False,
                            maximum=MAX_CAPTURE_CHARS,
                        ),
                    ),
                    help=HelpSpec(
                        ".youtube <search terms>", "Find YouTube videos by title or keywords."
                    ),
                    acl_key="youtube",
                    deadline_ms=120_000,
                    public=True,
                    context_fields=("prefix",),
                    presentation_mode="plain",
                ),
            ),
            components=(
                component("videos"),
                component("select", input_kind="string_values"),
                component("back"),
            ),
            version="1.0.1",
        )

    async def start(self) -> None:
        try:
            await asyncio.to_thread(self._temp_root.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._grid_root.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._prune_grids)
            self._storage_ready = True
        except OSError:
            LOGGER.warning("youtube_grid_storage_unavailable")
        await self._service.start()

    async def handle(
        self,
        request: TypedRequest,
        job_id: str,
        generation: int,
    ) -> ResultEnvelope:
        del generation
        if request.handler == "youtube.lookup":
            return await self._lookup(request, job_id)
        if request.handler == "youtube.component":
            return await self._component(request, job_id)
        return self._result(
            job_id,
            self._plain("Unavailable."),
            status="error",
            error_code="unknown_handler",
        )

    async def _lookup(self, request: TypedRequest, job_id: str) -> ResultEnvelope:
        query = normalize_query(str(request.arguments.get("query") or ""))
        prefix = str(request.event.get("prefix") or ".")
        if not query or query.casefold() == "help":
            return self._result(job_id, self._plain(f"{prefix}youtube <search terms>"))
        if len(query) > MAX_QUERY_CHARS:
            return self._result(job_id, self._plain("Maximum 200 characters."))

        try:
            bundle = await self._service.search_and_render(query)
        except YouTubeBusyError:
            return self._result(
                job_id,
                self._plain("Busy. Try again."),
                status="error",
                error_code="busy",
            )
        except YouTubeQuotaError:
            return self._result(
                job_id,
                self._plain("Daily limit reached."),
                status="error",
                error_code="quota",
            )
        except YouTubeNoResultsError:
            return self._result(
                job_id,
                self._plain("No videos found."),
                status="error",
                error_code="no_results",
            )
        except (YouTubeUnavailableError, YouTubeResponseError, RedisError):
            return self._result(
                job_id,
                self._plain("Unavailable."),
                status="error",
                error_code="provider",
            )
        except Exception as exc:
            LOGGER.warning("youtube_lookup_failed category=%s", type(exc).__name__)
            return self._result(
                job_id,
                self._plain("Unavailable."),
                status="error",
                error_code="failed",
            )

        first = bundle.results[0]
        if not self._storage_ready:
            return self._result(job_id, self._plain(first.watch_url))
        grid_id: str | None = None
        try:
            grid_id, digest = await self._store_grid(bundle.grid)
            session = YouTubeSession(
                token=secrets.token_hex(8),
                owner_id=int(request.event.get("actor_id") or 0),
                guild_id=int(request.event.get("guild_id") or 0),
                query=query,
                results=bundle.results,
                current_video_id=first.video_id,
                grid_id=grid_id,
                grid_sha256=digest,
            )
            if session.owner_id <= 0 or session.guild_id <= 0:
                raise ValueError("missing command scope")
            await self._save_session(session)
        except (OSError, RedisError):
            if grid_id is not None:
                with contextlib.suppress(OSError):
                    self._grid_path(grid_id).unlink()
            LOGGER.warning("youtube_session_cache_unavailable")
            return self._result(job_id, self._plain(first.watch_url))
        except Exception as exc:
            if grid_id is not None:
                with contextlib.suppress(OSError):
                    self._grid_path(grid_id).unlink()
            LOGGER.warning("youtube_session_failed category=%s", type(exc).__name__)
            return self._result(job_id, self._plain(first.watch_url))

        return self._result(job_id, self._video(session, first))

    async def _component(self, request: TypedRequest, job_id: str) -> ResultEnvelope:
        token = str(request.arguments.get("session") or "")
        try:
            session = await self._load_session(token)
        except (RedisError, ValueError, msgspec.DecodeError):
            session = None
        actor_id = int(request.event.get("actor_id") or 0)
        guild_id = int(request.event.get("guild_id") or 0)
        if (
            session is None
            or session.owner_id != actor_id
            or session.guild_id != guild_id
        ):
            return self._result(
                job_id,
                self._plain("Unavailable.", kind="ephemeral"),
                status="error",
                error_code="session",
            )

        action = request.route.removeprefix("youtube.")
        try:
            if action == "videos":
                session, attachment = await self._grid_attachment(session)
                await self._save_session(session)
                intent = self._browser(session, attachment)
            elif action == "select":
                selected = self._one_value(request)
                result = next(
                    (item for item in session.results if item.video_id == selected),
                    None,
                )
                if result is None:
                    raise ValueError("unknown video")
                session = msgspec.structs.replace(
                    session,
                    current_video_id=result.video_id,
                )
                await self._save_session(session)
                intent = self._video(session, result, kind="edit")
            elif action == "back":
                result = next(
                    (
                        item
                        for item in session.results
                        if item.video_id == session.current_video_id
                    ),
                    session.results[0],
                )
                intent = self._video(session, result, kind="edit")
            else:
                raise ValueError("unknown component")
            return self._result(job_id, intent)
        except Exception as exc:
            LOGGER.warning(
                "youtube_component_failed action=%s category=%s",
                action,
                type(exc).__name__,
            )
            return self._result(
                job_id,
                self._plain("Unavailable.", kind="ephemeral"),
                status="error",
                error_code="component",
            )

    def _video(
        self,
        session: YouTubeSession,
        result: YouTubeResult,
        *,
        kind: str = "send",
    ) -> DiscordIntent:
        return DiscordIntent(
            kind=kind,
            content=result.watch_url,
            session_id=session.token,
            controls=(
                PresentationControl(
                    kind="button",
                    namespace="youtube",
                    action="videos",
                    payload=session.token,
                    label="Videos",
                ),
            ),
            presentation_mode="plain",
        )

    def _browser(
        self,
        session: YouTubeSession,
        attachment: TemporaryAttachment,
    ) -> DiscordIntent:
        options = tuple(
            PresentationOption(
                label=self._truncate(f"{index}. {result.title}", 100),
                value=result.video_id,
                description=self._truncate(result.channel_name, 100),
                default=result.video_id == session.current_video_id,
            )
            for index, result in enumerate(session.results, start=1)
        )
        return DiscordIntent(
            kind="edit",
            attachments=(attachment,),
            session_id=session.token,
            controls=(
                PresentationControl(
                    kind="string_select",
                    namespace="youtube",
                    action="select",
                    payload=session.token,
                    placeholder="Videos",
                    options=options,
                ),
                PresentationControl(
                    kind="button",
                    namespace="youtube",
                    action="back",
                    payload=session.token,
                    label="Back",
                ),
            ),
            presentation_mode="plain",
        )

    async def _save_session(self, session: YouTubeSession) -> None:
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.setex(
            f"lumi:youtube:session:{session.token}",
            SESSION_TTL_SECONDS,
            encode(session),
        )
        pipeline.setex(
            ui_key("owner", session.token),
            SESSION_TTL_SECONDS,
            str(session.owner_id),
        )
        await pipeline.execute()

    async def _load_session(self, token: str) -> YouTubeSession | None:
        payload = await self._cache.get(f"lumi:youtube:session:{token}")
        if not payload:
            return None
        session = decode(payload, YouTubeSession)
        pipeline = self._cache.pipeline(transaction=False)
        pipeline.expire(f"lumi:youtube:session:{token}", SESSION_TTL_SECONDS)
        pipeline.expire(ui_key("owner", token), SESSION_TTL_SECONDS)
        await pipeline.execute()
        return session

    async def _store_grid(self, data: bytes) -> tuple[str, str]:
        identifier = secrets.token_hex(24)
        await asyncio.to_thread(self._grid_path(identifier).write_bytes, data)
        await asyncio.to_thread(self._prune_grids)
        return identifier, hashlib.sha256(data).hexdigest()

    async def _grid_attachment(
        self,
        session: YouTubeSession,
    ) -> tuple[YouTubeSession, TemporaryAttachment]:
        data: bytes | None = None
        with contextlib.suppress(OSError, ValueError):
            candidate = await asyncio.to_thread(self._grid_path(session.grid_id).read_bytes)
            if hashlib.sha256(candidate).hexdigest() == session.grid_sha256:
                data = candidate
        if data is None:
            data = await self._service.render_results(session.results)
            grid_id, digest = await self._store_grid(data)
            session = msgspec.structs.replace(
                session,
                grid_id=grid_id,
                grid_sha256=digest,
            )

        identifier = secrets.token_hex(24)
        await asyncio.to_thread((self._temp_root / identifier).write_bytes, data)
        attachment = TemporaryAttachment(
            reference=f"tmp:{identifier}",
            file_name="youtube-results.jpg",
            sha256=hashlib.sha256(data).hexdigest(),
            size=len(data),
            expires_at_ms=int(time.time() * 1000) + ATTACHMENT_TTL_MS,
        )
        return session, attachment

    def _grid_path(self, identifier: str) -> Path:
        if len(identifier) != 48 or any(
            character not in "0123456789abcdef" for character in identifier
        ):
            raise ValueError("invalid grid identifier")
        root = self._grid_root.resolve()
        path = (root / identifier).resolve()
        if path.parent != root or path.is_symlink():
            raise ValueError("invalid grid path")
        return path

    def _prune_grids(self) -> None:
        cutoff = time.time() - GRID_RETENTION_SECONDS
        with contextlib.suppress(OSError):
            for path in self._grid_root.iterdir():
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
            raise ValueError("one selection required")
        return str(values[0])

    @staticmethod
    def _truncate(value: str, limit: int) -> str:
        return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"

    @staticmethod
    def _plain(text: str, *, kind: str = "send") -> DiscordIntent:
        return DiscordIntent(kind=kind, content=text, presentation_mode="plain")

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
            module="youtube",
            group=self._group,
            generation=self._generation,
            status=status,
            intents=(intent,),
            error_code=error_code,
            completed_at_ms=int(time.time() * 1000),
        )

    async def close(self) -> None:
        await self._service.close()
