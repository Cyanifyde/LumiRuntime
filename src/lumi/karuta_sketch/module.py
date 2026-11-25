from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import contextlib
import hashlib
import io
import logging
import os
import re
import secrets
import time
from dataclasses import replace
from pathlib import Path

import msgspec
import numpy as np
from redis.asyncio import Redis

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

from .engine import (
    COVERAGE_CUTOFF_PERCENT,
    MODES,
    PRESETS,
    TEMPERATURES,
    SketchEngine,
    SketchResult,
    SketchSettings,
    presets_for,
)
from .export import build_image_download, colour_swatch
from .image import ImageTooLargeError, PreparedImage, SketchImageLoader, UnsupportedImageError
from .scheduler import FairRenderScheduler, RenderBusyError, RenderSupersededError

LOGGER = logging.getLogger(__name__)
SESSION_TTL = 15 * 60
ATTACHMENT_TTL_MS = 120_000


class SketchSessionExpiredError(RuntimeError):
    pass


class SketchSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    source_url: str
    source_size: int
    source_hash: str
    source_key: str
    result_key: str
    detail_scale: int
    edge_protection: int
    minimum_region: int
    colour_limit: int
    preset: str
    used_labels: tuple[int, ...]
    visible_pixels: float
    page: int = 0
    mode: str = "Simplify"
    revision: int = 0
    expires_at_ms: int = 0
    cutoff_percent: float = COVERAGE_CUTOFF_PERCENT
    temperature: str = "Normal"

    @property
    def settings(self) -> SketchSettings:
        return SketchSettings(
            self.detail_scale,
            self.edge_protection,
            self.minimum_region,
            self.colour_limit,
            self.preset,
            self.mode,
            self.cutoff_percent,
            self.temperature,
        )


class KarutaSketchModule:
    def __init__(
        self,
        *,
        generation: int,
        cache_redis: Redis,
        temp_root: Path,
        loader: SketchImageLoader | None = None,
        engine: SketchEngine | None = None,
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._temp_root = temp_root
        self._internal_root = temp_root / "karuta-sketch"
        self._loader = loader or SketchImageLoader()
        self._engine = engine or SketchEngine()
        self._scheduler = FairRenderScheduler(maximum=16)
        self._cleanup_task: asyncio.Task[None] | None = None

    @property
    def manifest(self) -> Manifest:
        token = r"(?P<session>[a-f0-9]{16})"
        page = r"(?P<page>[0-9]{1,5})"

        def component(
            action: str,
            *,
            input_kind: str = "none",
            modal: ModalSpec | None = None,
        ) -> ComponentSpec:
            return ComponentSpec(
                namespace="karuta_sketch",
                action=action,
                handler="karuta_sketch.component",
                grammar=rf"^{token}~{page}(?:~(?P<revision>[0-9]{{1,9}}))?$",
                captures=(
                    CaptureSpec("session"),
                    CaptureSpec("page", "integer", minimum=0, maximum=65),
                    CaptureSpec("revision", "integer", required=False, minimum=0),
                ),
                deadline_ms=120_000,
                public=True,
                input_kind=input_kind,
                modal=modal,
                session_bound=True,
                presentation_mode="v2",
                ephemeral=action == "download",
            )

        return Manifest(
            schema=2,
            module="karuta_sketch",
            group="media",
            generation=self._generation,
            job_stream="lumi:jobs:media",
            display_name="Karuta Sketch",
            summary="Split an image into cleaned Karuta colour layers.",
            presentation_mode="v2",
            commands=(
                CommandSpec(
                    root="ksketch",
                    handler="karuta_sketch.open",
                    grammar=r"^$",
                    help=HelpSpec(
                        ".ksketch", "Extract colour layers from an attached or replied image."
                    ),
                    acl_key="ksketch",
                    deadline_ms=120_000,
                    public=True,
                    context_fields=("prefix", "image_source"),
                    presentation_mode="v2",
                ),
            ),
            components=(
                component("previous"),
                component("next"),
                component(
                    "jump",
                    input_kind="modal",
                    modal=ModalSpec(
                        "Go to page",
                        (ModalFieldSpec("page", "Page", minimum=1, maximum=3),),
                    ),
                ),
                component(
                    "settings",
                    input_kind="modal",
                    modal=ModalSpec(
                        "Sketch settings",
                        (
                            ModalFieldSpec("detail_scale", "Detail scale (1-16)", maximum=2),
                            ModalFieldSpec("edge_protection", "Edge protection (0-100)", maximum=3),
                            ModalFieldSpec("minimum_region", "Minimum region (0-1000)", maximum=4),
                            ModalFieldSpec("colour_limit", "Colour limit (2-64)", maximum=2),
                            ModalFieldSpec(
                                "cutoff_percent",
                                "Colour cutoff % (0-100)",
                                required=False,
                                maximum=7,
                                placeholder="0.25 (0 disables; blank keeps current)",
                            ),
                        ),
                    ),
                ),
                component(
                    "cutoff",
                    input_kind="modal",
                    modal=ModalSpec(
                        "Colour cutoff",
                        (
                            ModalFieldSpec(
                                "cutoff_percent",
                                "Coverage % (0-100)",
                                minimum=1,
                                maximum=7,
                                placeholder="0.25 (0 disables)",
                            ),
                        ),
                    ),
                ),
                component("reset"),
                component("temperature"),
                component("regenerate"),
                component("download"),
                component("preset", input_kind="string_values"),
                component("mode", input_kind="string_values"),
            ),
            version="2.7.1",
        )

    async def start(self) -> None:
        self._temp_root.mkdir(parents=True, exist_ok=True)
        self._internal_root.mkdir(parents=True, exist_ok=True)
        await self._loader.start()
        await self._scheduler.start()
        await asyncio.to_thread(self._prune_internal)
        self._cleanup_task = asyncio.create_task(
            self._prune_loop(), name="karuta-sketch-cache-cleaner"
        )

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        try:
            if request.handler == "karuta_sketch.open":
                intent = await self._open(request)
            elif request.handler == "karuta_sketch.component":
                intent = await self._component(request)
            else:
                raise ValueError("unknown karuta sketch handler")
            return self._result(job_id, intent)
        except ImageTooLargeError:
            return self._result(job_id, self._failure_intent(request, "Image too large."))
        except UnsupportedImageError:
            return self._result(job_id, self._failure_intent(request, "Unsupported image."))
        except (RenderBusyError, RenderSupersededError):
            return self._result(job_id, self._failure_intent(request, "Busy. Try again."))
        except SketchSessionExpiredError:
            return self._result(job_id, self._failure_intent(request, "Unavailable."))
        except Exception:
            LOGGER.exception(
                "karuta_sketch_request_failed",
                extra={"handler": request.handler, "guild_id": request.event.get("guild_id")},
            )
            return self._result(
                job_id,
                self._failure_intent(request, "Unavailable."),
                status="error",
                error_code="karuta_sketch_failed",
            )

    async def _open(self, request: TypedRequest) -> DiscordIntent:
        prefix = str(request.event.get("prefix") or ".")
        raw_source = request.event.get("image_source")
        if not isinstance(raw_source, dict) or not raw_source.get("url"):
            return DiscordIntent(
                kind="send",
                pages=(PresentationPage(description=f"{prefix}ksketch"),),
                presentation_mode="v2",
            )
        owner_id = int(request.event.get("actor_id") or 0)
        guild_id = int(request.event.get("guild_id") or 0)
        if owner_id <= 0 or guild_id <= 0:
            raise UnsupportedImageError
        source_url = str(raw_source["url"])
        source_size = max(0, int(raw_source.get("size") or 0))
        prepared = await self._loader.fetch(source_url, declared_size=source_size)
        source_key = await self._store_source(prepared)
        settings = PRESETS["Balanced"]
        result, result_key = await self._generate(owner_id, prepared, settings)
        session = SketchSession(
            token=secrets.token_hex(8),
            owner_id=owner_id,
            guild_id=guild_id,
            source_url=source_url,
            source_size=source_size,
            source_hash=prepared.sha256,
            source_key=source_key,
            result_key=result_key,
            detail_scale=settings.detail_scale,
            edge_protection=settings.edge_protection,
            minimum_region=settings.minimum_region,
            colour_limit=settings.colour_limit,
            preset=settings.preset,
            used_labels=result.used_labels,
            visible_pixels=result.visible_pixels,
            mode=settings.mode,
            cutoff_percent=settings.cutoff_percent,
            temperature=settings.temperature,
            expires_at_ms=int(time.time() * 1000) + SESSION_TTL * 1000,
        )
        return await self._commit_page(session, result, kind="send")

    async def _component(self, request: TypedRequest) -> DiscordIntent:
        token = str(request.arguments.get("session") or "")
        if not re.fullmatch(r"[a-f0-9]{16}", token):
            return self._ephemeral("Unavailable.")
        lock = f"lumi:karuta_sketch:lock:{token}"
        claim = secrets.token_hex(16)
        if not await self._cache.set(lock, claim, nx=True, ex=180):
            return self._ephemeral("Busy. Try again.")
        try:
            return await self._component_locked(request, token)
        finally:
            await self._cache.eval(
                "if redis.call('GET',KEYS[1]) == ARGV[1] then "
                "return redis.call('DEL',KEYS[1]) end return 0",
                1,
                lock,
                claim,
            )

    async def _component_locked(self, request: TypedRequest, token: str) -> DiscordIntent:
        session = await self._load_session(token)
        actor_id = int(request.event.get("actor_id") or 0)
        if (
            session is None
            or session.owner_id != actor_id
            or session.guild_id != int(request.event.get("guild_id") or 0)
        ):
            return self._ephemeral("Unavailable.")
        revision = request.arguments.get("revision")
        if revision is not None and int(revision) != session.revision:
            return self._ephemeral("Busy. Try again.")
        action = request.route.removeprefix("karuta_sketch.")
        page_count = 1 + len(session.used_labels)
        if action in {"previous", "next"}:
            target = int(request.arguments.get("page") or 0)
            session = msgspec.structs.replace(session, page=max(0, min(target, page_count - 1)))
        elif action == "jump":
            values = request.arguments.get("values")
            raw = values.get("page") if isinstance(values, dict) else None
            try:
                target = int(str(raw)) - 1
            except (TypeError, ValueError):
                return self._ephemeral("Invalid input.")
            if not 0 <= target < page_count:
                return self._ephemeral("Invalid input.")
            session = msgspec.structs.replace(session, page=target)
        elif action == "settings":
            values = request.arguments.get("values")
            if not isinstance(values, dict):
                return self._ephemeral("Invalid input.")
            try:
                settings = SketchSettings(
                    int(str(values["detail_scale"])),
                    int(str(values["edge_protection"])),
                    int(str(values["minimum_region"])),
                    int(str(values["colour_limit"])),
                    "Custom",
                    session.mode,
                    self._parse_cutoff(values.get("cutoff_percent"), session.cutoff_percent),
                    session.temperature,
                )
                settings.validate()
            except (KeyError, TypeError, ValueError):
                return self._ephemeral("Invalid input.")
            session = self._stage_settings(session, settings)
        elif action == "cutoff":
            values = request.arguments.get("values")
            if not isinstance(values, dict) or not str(values.get("cutoff_percent") or "").strip():
                return self._ephemeral("Invalid input.")
            try:
                cutoff = self._parse_cutoff(values["cutoff_percent"], session.cutoff_percent)
                settings = SketchSettings(
                    session.detail_scale,
                    session.edge_protection,
                    session.minimum_region,
                    session.colour_limit,
                    "Custom",
                    session.mode,
                    cutoff,
                    session.temperature,
                )
                settings.validate()
            except (KeyError, TypeError, ValueError):
                return self._ephemeral("Invalid input.")
            session = self._stage_settings(session, settings)
        elif action == "preset":
            value = self._one_value(request)
            settings = presets_for(session.mode).get(value)
            if settings is None:
                return self._ephemeral("Invalid input.")
            settings = SketchSettings(
                settings.detail_scale,
                settings.edge_protection,
                settings.minimum_region,
                settings.colour_limit,
                settings.preset,
                session.mode,
                session.cutoff_percent,
                session.temperature,
            )
            session = self._stage_settings(session, settings)
        elif action == "mode":
            value = self._one_value(request)
            if value not in MODES:
                return self._ephemeral("Invalid input.")
            settings = SketchSettings(
                session.detail_scale,
                session.edge_protection,
                session.minimum_region,
                session.colour_limit,
                session.preset,
                value,
                session.cutoff_percent,
                session.temperature,
            )
            session = self._stage_settings(session, settings)
        elif action == "temperature":
            next_temperature = TEMPERATURES[
                (TEMPERATURES.index(session.temperature) + 1) % len(TEMPERATURES)
            ]
            session = self._stage_settings(
                session, replace(session.settings, temperature=next_temperature)
            )
        elif action == "reset":
            session = self._stage_settings(session, PRESETS["Balanced"])
        elif action == "regenerate":
            session, result = await self._regenerate(session, session.settings)
            return await self._commit_page(session, result, kind="edit")
        elif action == "download":

            result = await self._load_result(session.result_key)
            data = await self._scheduler.submit(
                session.owner_id, "download", lambda: build_image_download(result)
            )
            if session.expires_at_ms <= int(time.time() * 1000):
                raise SketchSessionExpiredError
            attachment = await self._attachment(
                data, f"karuta-sketch-{session.source_hash[:12]}-images.zip"
            )
            return DiscordIntent(
                kind="ephemeral",
                pages=(PresentationPage(file_name=attachment.file_name),),
                attachments=(attachment,),
                presentation_mode="v2",
            )
        else:
            return self._ephemeral("Unavailable.")
        result = await self._load_result(session.result_key)


        session = msgspec.structs.replace(session, revision=session.revision + 1)
        return await self._commit_page(session, result, kind="edit")

    @staticmethod
    def _parse_cutoff(value: object, current: float) -> float:
        if value is None or value == "":
            return current
        if not isinstance(value, str):
            raise ValueError("cutoff input")
        text = value.strip()
        if not text:
            return current
        if not re.fullmatch(r"(?:[0-9]{1,3}(?:\.[0-9]{1,2})?|\.[0-9]{1,2})%?", text):
            raise ValueError("cutoff input")
        cutoff = float(text.removesuffix("%"))
        if not 0 <= cutoff <= 100:
            raise ValueError("cutoff input")
        return cutoff

    @staticmethod
    def _stage_settings(session: SketchSession, settings: SketchSettings) -> SketchSession:
        return msgspec.structs.replace(
            session,
            detail_scale=settings.detail_scale,
            edge_protection=settings.edge_protection,
            minimum_region=settings.minimum_region,
            colour_limit=settings.colour_limit,
            preset=settings.preset,
            mode=settings.mode,
            cutoff_percent=settings.cutoff_percent,
            temperature=settings.temperature,
        )

    async def _commit_page(
        self, session: SketchSession, result: SketchResult, *, kind: str
    ) -> DiscordIntent:


        page = max(0, min(session.page, len(session.used_labels)))
        session = msgspec.structs.replace(session, page=page)
        intent = await self._page(session, result, kind=kind)
        await self._save_session(session)
        return intent

    async def _regenerate(
        self, session: SketchSession, settings: SketchSettings
    ) -> tuple[SketchSession, SketchResult]:
        prepared = await self._load_source(session.source_key)
        if prepared is None:
            prepared = await self._loader.fetch(
                session.source_url, declared_size=session.source_size
            )
            source_key = await self._store_source(prepared)
        else:
            source_key = session.source_key
        result, result_key = await self._generate(session.owner_id, prepared, settings)
        return (
            msgspec.structs.replace(
                session,
                source_hash=prepared.sha256,
                source_key=source_key,
                result_key=result_key,
                detail_scale=settings.detail_scale,
                edge_protection=settings.edge_protection,
                minimum_region=settings.minimum_region,
                colour_limit=settings.colour_limit,
                preset=settings.preset,
                mode=settings.mode,
                cutoff_percent=settings.cutoff_percent,
                temperature=settings.temperature,
                revision=session.revision + 1,
                used_labels=result.used_labels,
                visible_pixels=result.visible_pixels,
                page=0,
            ),
            result,
        )

    async def _generate(
        self, actor_id: int, prepared: PreparedImage, settings: SketchSettings
    ) -> tuple[SketchResult, str]:
        key = self._engine.cache_key(prepared.sha256, settings)
        path = self._internal_path(f"{key}.result.npz")
        with contextlib.suppress(OSError, ValueError):
            result = await asyncio.to_thread(self._engine.deserialize, path.read_bytes())
            if result.version == self._engine.expected_version(settings.mode):
                await asyncio.to_thread(os.utime, path, None)
                return result, path.name
        result = await self._scheduler.submit(
            actor_id,
            "regenerate",
            lambda: self._engine.optimize(prepared, settings),
        )
        data = await asyncio.to_thread(self._engine.serialize, result)
        await asyncio.to_thread(self._atomic_write, path, data)
        return result, path.name

    async def _page(
        self, session: SketchSession, result: SketchResult, *, kind: str
    ) -> DiscordIntent:
        page_count = 1 + len(session.used_labels)
        page = max(0, min(session.page, page_count - 1))
        swatch = None
        if page == 0:
            data = await asyncio.to_thread(self._engine.render_composite, result)
            description = (
                f"{result.settings.preset} · {result.settings.mode} · "
                f"{len(session.used_labels)} colours\n"
                f"Detail {result.settings.detail_scale} · Edge {result.settings.edge_protection} · "
                f"Minimum {result.settings.minimum_region} · "
                f"Limit {result.settings.colour_limit} · "
                f"Cutoff {result.settings.cutoff_percent:g}% · "
                f"Temperature {result.settings.temperature}"
            )
            file_name = f"karuta-sketch-{session.source_hash[:12]}.webp"
        else:
            colour = session.used_labels[page - 1]
            data = await asyncio.to_thread(self._engine.render_layer, result, colour)
            coverage = result.counts[colour] / max(1e-12, session.visible_pixels) * 100
            description = f"Coverage: {coverage:.2f}%"
            file_name = f"karuta-layer-{colour + 1:02d}-{session.source_hash[:12]}.webp"
            swatch = await self._attachment(
                colour_swatch(colour), f"karuta-colour-{colour + 1:02d}.webp"
            )
        if session.settings != result.settings:
            description += (
                f"\n\nPending: {session.preset} · {session.mode}\n"
                f"Detail {session.detail_scale} · Edge {session.edge_protection} · "
                f"Minimum {session.minimum_region} · Limit {session.colour_limit} · "
                f"Cutoff {session.cutoff_percent:g}% · Temperature {session.temperature}\n"
                "Press Regenerate to apply."
            )
        attachment = await self._attachment(data, file_name)
        if page != session.page:
            session = msgspec.structs.replace(session, page=page)
        return DiscordIntent(
            kind=kind,
            pages=(
                PresentationPage(
                    title="Karuta Sketch" if page == 0 else None,
                    description=description,
                    thumbnail_url=f"attachment://{swatch.file_name}" if swatch else None,
                    file_name=attachment.file_name,
                    controls=self._controls(session, page, page_count),
                ),
            ),
            attachments=(attachment, swatch) if swatch else (attachment,),
            session_id=session.token,
            presentation_mode="v2",
        )

    def _controls(
        self, session: SketchSession, page: int, page_count: int
    ) -> tuple[PresentationControl, ...]:
        payload = f"{session.token}~{page}~{session.revision}"
        return (
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="previous",
                payload=f"{session.token}~{max(0, page - 1)}~{session.revision}",
                label="Previous",
                disabled=page == 0,
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="jump",
                payload=payload,
                label=f"{page + 1}/{page_count}",
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="next",
                payload=f"{session.token}~{min(page_count - 1, page + 1)}~{session.revision}",
                label="Next",
                disabled=page >= page_count - 1,
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="settings",
                payload=payload,
                label="Settings",
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="reset",
                payload=payload,
                label="Reset",
                disabled=session.settings == PRESETS["Balanced"],
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="regenerate",
                payload=payload,
                label="Regenerate",
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="cutoff",
                payload=payload,
                label=f"Cutoff {session.cutoff_percent:g}%",
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="temperature",
                payload=payload,
                label=f"Temperature: {session.temperature}",
            ),
            PresentationControl(
                kind="button",
                namespace="karuta_sketch",
                action="download",
                payload=payload,
                label="Download images",
            ),
            PresentationControl(
                kind="string_select",
                namespace="karuta_sketch",
                action="preset",
                payload=payload,
                placeholder="Preset",
                options=tuple(
                    PresentationOption(
                        label=name,
                        value=name,
                        default=session.preset == name,
                    )
                    for name in presets_for(session.mode)
                ),
            ),
            PresentationControl(
                kind="string_select",
                namespace="karuta_sketch",
                action="mode",
                payload=payload,
                placeholder="Mode",
                options=tuple(
                    PresentationOption(label=name, value=name, default=session.mode == name)
                    for name in MODES
                ),
            ),
        )

    async def _save_session(self, session: SketchSession) -> None:
        now = int(time.time() * 1000)
        if not session.expires_at_ms:
            session = msgspec.structs.replace(session, expires_at_ms=now + SESSION_TTL * 1000)
        remaining = session.expires_at_ms - now
        if remaining <= 0:
            raise SketchSessionExpiredError
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.set(f"lumi:karuta_sketch:session:{session.token}", encode(session), px=remaining)
        pipeline.set(ui_key("owner", session.token), str(session.owner_id), px=remaining)
        await pipeline.execute()

    async def _load_session(self, token: str) -> SketchSession | None:
        payload = await self._cache.get(f"lumi:karuta_sketch:session:{token}")
        if not payload:
            return None
        session = decode(payload, SketchSession)
        now = int(time.time() * 1000)
        if not session.expires_at_ms:
            remaining = await self._cache.pttl(f"lumi:karuta_sketch:session:{token}")
            if remaining == -2:
                return None
            session = msgspec.structs.replace(
                session, expires_at_ms=now + (remaining if remaining >= 0 else SESSION_TTL * 1000)
            )
        if session.expires_at_ms <= now:
            return None
        return session

    async def _store_source(self, source: PreparedImage) -> str:
        path = self._internal_path(f"{source.sha256}.source.npz")
        if not path.exists():
            output = io.BytesIO()
            np.savez_compressed(
                output,
                rgba=source.rgba,
                sha256=np.asarray(source.sha256),
                source_format=np.asarray(source.source_format),
            )
            await asyncio.to_thread(self._atomic_write, path, output.getvalue())
        else:
            await asyncio.to_thread(os.utime, path, None)
        return path.name

    async def _load_source(self, key: str) -> PreparedImage | None:
        try:
            path = self._internal_path(key)
            data = await asyncio.to_thread(path.read_bytes)
            await asyncio.to_thread(os.utime, path, None)
            with np.load(io.BytesIO(data), allow_pickle=False) as stored:
                return PreparedImage(
                    rgba=stored["rgba"].astype(np.uint8),
                    sha256=str(stored["sha256"]),
                    source_format=str(stored["source_format"]),
                )
        except (OSError, ValueError, KeyError):
            return None

    async def _load_result(self, key: str) -> SketchResult:
        path = self._internal_path(key)
        data = await asyncio.to_thread(path.read_bytes)
        await asyncio.to_thread(os.utime, path, None)
        return await asyncio.to_thread(self._engine.deserialize, data)

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

    def _internal_path(self, key: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{64}\.(?:source|result)\.npz", key):
            raise ValueError("invalid sketch cache key")
        root = self._internal_root.resolve()
        path = (root / key).resolve()
        if path.parent != root or path.is_symlink():
            raise ValueError("invalid sketch cache path")
        return path

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        temporary = path.with_suffix(path.suffix + f".{secrets.token_hex(4)}.tmp")
        temporary.write_bytes(data)
        temporary.replace(path)

    def _prune_internal(self) -> None:
        cutoff = time.time() - SESSION_TTL * 2
        with contextlib.suppress(OSError):
            for path in self._internal_root.iterdir():
                if path.is_file() and path.stat().st_mtime < cutoff:
                    path.unlink()

    async def _prune_loop(self) -> None:
        while True:
            await asyncio.sleep(SESSION_TTL)
            await asyncio.to_thread(self._prune_internal)

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

    @staticmethod
    def _failure_intent(request: TypedRequest, text: str) -> DiscordIntent:
        kind = "ephemeral" if request.event.get("dispatch") == "component" else "send"
        return DiscordIntent(
            kind=kind,
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
            module="karuta_sketch",
            group="media",
            generation=self._generation,
            status=status,
            intents=(intent,),
            error_code=error_code,
            completed_at_ms=int(time.time() * 1000),
        )

    def begin_drain(self) -> None:
        self._scheduler.begin_drain()

    def resume(self) -> None:
        self._scheduler.resume()

    def drain_ready(self) -> bool:
        return self._scheduler.drain_ready()

    async def close(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
        await self._scheduler.close()
        await self._loader.close()
