from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
import weakref
from dataclasses import replace

import msgspec
from redis.asyncio import Redis

from lumi.contracts import (
    ChannelTransitionSpec,
    CaptureSpec,
    CommandSpec,
    ComponentCaptureSpec,
    ComponentExtractionSpec,
    ComponentSpec,
    DiscordIntent,
    FieldExtraction,
    HelpSpec,
    Manifest,
    MatcherExpr,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    RepeatedExtraction,
    ResultEnvelope,
    ScheduledCallbackSpec,
    TypedRequest,
    decode,
    encode,
)
from lumi.cooldowns.service import deliver_reminder, flush_spec
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from .store import MySQLSofiStore

LOGGER = logging.getLogger(__name__)
SOFI_BOT_ID = -3
SESSION_TTL = 900
DROP_COOLDOWN_MS = 8 * 60 * 1_000
SERIES_COOLDOWN_MS = 24 * 60 * 60 * 1_000
DELIVERY_WINDOW_MS = 5 * 60 * 1_000


class SofiSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int


class PogDropSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int


def _passive(
    *,
    route: str,
    handler: str,
    event: str,
    pattern: str,
    captures: tuple[CaptureSpec, ...],
    subject_capture: str | None = "owner",
    subject_field: str | None = None,
    context_fields: tuple[str, ...] = (),
    repeated: tuple[RepeatedExtraction, ...] = (),
    component_extractions: tuple[ComponentExtractionSpec, ...] = (),
    watch_ttl_ms: int = 0,
    presentation_mode: str | None = None,
    reference_author_max_age_ms: int = 0,
) -> PassiveMatcherSpec:
    return PassiveMatcherSpec(
        route=route,
        handler=handler,
        event=event,
        bot_ids=(SOFI_BOT_ID,),
        expression=MatcherExpr("regex", "message_document", pattern),
        extractions=(FieldExtraction("message_document", pattern, captures),) if captures else (),
        repeated=repeated,
        component_extractions=component_extractions,
        subject_capture=subject_capture,
        subject_field=subject_field,
        subject_required=True,
        hydrate=event == "message_edit",
        watch_ttl_ms=watch_ttl_ms,
        deadline_ms=120_000,
        context_fields=context_fields,
        presentation_mode=presentation_mode,
        reference_author_max_age_ms=reference_author_max_age_ms,
    )


class _SchedulePublisher:
    def __init__(
        self,
        store: MySQLSofiStore,
        client: ScheduledCallbackClient,
    ) -> None:
        self._store = store
        self._client = client
        self._stopped = False

    async def run(self) -> None:
        await self.replay()
        while not self._stopped:
            await self.flush_safely()
            await asyncio.sleep(1)

    def stop(self) -> None:
        self._stopped = True

    async def replay(self) -> None:
        now_ms = int(time.time() * 1_000)
        for timer, channel_id in await self._store.active_timers():
            if timer.due_at_ms is None or timer.due_at_ms + DELIVERY_WINDOW_MS <= now_ms:
                await self._store.finish_timer(
                    timer.guild_id, timer.user_id, timer.kind, timer.revision
                )
                continue
            await self._client.publish(
                operation="upsert",
                schedule_key=self._key(timer.guild_id, timer.user_id, timer.kind),
                route="sofi_tracker.reminder",
                revision=timer.revision,
                guild_id=timer.guild_id,
                channel_id=channel_id,
                actor_id=timer.user_id,
                due_at_ms=timer.due_at_ms,
                expires_at_ms=timer.due_at_ms + DELIVERY_WINDOW_MS,
                arguments={"kind": timer.kind, "revision": timer.revision},
            )

    async def flush_safely(self) -> int:
        count = 0
        try:
            now_ms = int(time.time() * 1_000)
            for item in await self._store.pending_schedule_outbox():
                due_at_ms = item.due_at_ms
                expires_at_ms = due_at_ms + DELIVERY_WINDOW_MS
                if item.operation == "cancel":
                    due_at_ms = now_ms
                    expires_at_ms = now_ms + DELIVERY_WINDOW_MS
                elif expires_at_ms <= now_ms:
                    await self._store.finish_timer(
                        item.guild_id, item.user_id, item.kind, item.revision
                    )
                    await self._store.mark_schedule_published(item.identifier)
                    continue
                await self._client.publish(
                    operation=item.operation,
                    schedule_key=self._key(item.guild_id, item.user_id, item.kind),
                    route="sofi_tracker.reminder",
                    revision=item.revision,
                    guild_id=item.guild_id,
                    channel_id=item.channel_id,
                    actor_id=item.user_id,
                    due_at_ms=due_at_ms,
                    expires_at_ms=expires_at_ms,
                    arguments={"kind": item.kind, "revision": item.revision},
                )
                await self._store.mark_schedule_published(item.identifier)
                count += 1
        except Exception:
            LOGGER.exception("sofi_schedule_outbox_publish_failed")
        return count

    @staticmethod
    def _key(guild_id: int, user_id: int, kind: str) -> str:
        return f"sofi_tracker:{guild_id}:{user_id}:{kind}"


class SofiTrackerModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        store: MySQLSofiStore,
        coalescer=None,
    ) -> None:
        self._generation = generation
        self._queue = queue_redis
        self._cache = cache_redis
        self._store = store
        self._cooldowns = coalescer
        self._actions = EdgeActionClient(
            queue_redis, module="sofi_tracker", group="cards", generation=generation
        )
        client = ScheduledCallbackClient(
            queue_redis, module="sofi_tracker", group="cards", generation=generation
        )
        self._publisher = _SchedulePublisher(store, client)
        self._publisher_task: asyncio.Task[None] | None = None
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    @property
    def manifest(self) -> Manifest:
        token = r"(?P<session>[a-f0-9]{16})"
        normal = (
            r"(?is)^[\s\S]*?<@!?(?P<owner>\d{17,20})>[\s*]*(?:is|just)"
            r"[\s*]+dropping\b[\s\S]*$"
        )
        series = (
            r"(?is)^<@!?(?P<owner>\d{17,20})>\s+(?:.+?\s+was\s+selected|"
            r"selected\s+.+?)!\s+Dropping\s+[1-3]\s+characters?\.{3}[\s\S]*$"
        )
        normal_wait = (
            r"(?is)^[\s\S]*?<@!?(?P<owner>\d{17,20})>[\s\S]*?Your\s+\*{0,2}Drop"
            r"\*{0,2}\s+will\s+be\s+ready\s+in:\s*\*{0,2}(?P<wait>"
            r"(?:\d+[smhdw]\s*)+)\*{0,2}[\s\S]*$"
        )
        series_wait = (
            r"(?is)^[\s\S]*?<@!?(?P<owner>\d{17,20})>[\s\S]*?you\s+can\s+use\s+"
            r"series\s+drop\s+again\s+in\s+\*{0,2}(?P<wait>(?:\d+[smhdw]\s*)+)"
            r"\*{0,2}[\s\S]*$"
        )
        owner = CaptureSpec("owner", "snowflake")
        wait = CaptureSpec("wait", "duration", minimum=1, maximum=86_400)
        routes = []
        for event in ("message_create", "message_edit"):
            suffix = "create" if event == "message_create" else "edit"
            routes.extend(
                (
                    _passive(
                        route=f"sofi_tracker.normal_{suffix}",
                        handler="sofi_tracker.normal",
                        event=event,
                        pattern=normal,
                        captures=(owner,),
                        context_fields=("event_order",),
                        watch_ttl_ms=600_000,
                    ),
                    _passive(
                        route=f"sofi_tracker.series_{suffix}",
                        handler="sofi_tracker.series",
                        event=event,
                        pattern=series,
                        captures=(owner,),
                        context_fields=("event_order",),
                        watch_ttl_ms=15_300_000,
                    ),
                    _passive(
                        route=f"sofi_tracker.normal_wait_{suffix}",
                        handler="sofi_tracker.normal_wait",
                        event=event,
                        pattern=normal_wait,
                        captures=(owner, wait),
                        context_fields=("event_order",),
                        watch_ttl_ms=600_000,
                    ),
                    _passive(
                        route=f"sofi_tracker.series_wait_{suffix}",
                        handler="sofi_tracker.series_wait",
                        event=event,
                        pattern=series_wait,
                        captures=(owner, wait),
                        context_fields=("event_order",),
                        watch_ttl_ms=15_300_000,
                    ),
                )
            )
        return Manifest(
            schema=2,
            module="sofi_tracker",
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Sofi Tracker",
            summary="Personal Sofi cooldown reminders.",
            presentation_mode="v2",
            version="1.0.2",
            commands=(
                CommandSpec(
                    root="sofi",
                    handler="sofi_tracker.open",
                    grammar=r"^(?:\s+(?P<help>help))?$",
                    captures=(CaptureSpec("help", required=False, maximum=4),),
                    help=HelpSpec(".sofi", "Set your Sofi card-drop and series-drop reminders."),
                    acl_key="sofi",
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
            passive=tuple(routes),
            components=(
                ComponentSpec(
                    "sofi",
                    "drop",
                    "sofi_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="sofi",
                    public=True,
                    session_bound=True,
                ),
                ComponentSpec(
                    "sofi",
                    "series",
                    "sofi_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="sofi",
                    public=True,
                    session_bound=True,
                ),
                ComponentSpec(
                    "sofi",
                    "delivery",
                    "sofi_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="sofi",
                    public=True,
                    input_kind="string_values",
                    session_bound=True,
                ),
                ComponentSpec(
                    "sofi",
                    "channel",
                    "sofi_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="sofi",
                    public=True,
                    input_kind="channel_ids",
                    channel_types=(0, 5),
                    session_bound=True,
                ),
            ),
            scheduled=(
                ScheduledCallbackSpec(
                    route="sofi_tracker.reminder",
                    handler="sofi_tracker.reminder",
                    deadline_ms=DELIVERY_WINDOW_MS,
                    presentation_mode="plain",
                ),
                flush_spec("sofi_tracker"),
            ),
            capabilities=("send_dm", "read_mention_batch_members", "send_mention_batch"),
        )

    async def start(self) -> None:
        self._publisher_task = asyncio.create_task(
            self._publisher.run(), name="sofi-schedule-publisher"
        )

    async def close(self) -> None:
        self._publisher.stop()
        if self._publisher_task is not None:
            self._publisher_task.cancel()
            await asyncio.gather(self._publisher_task, return_exceptions=True)

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        del generation
        try:
            if request.handler == "sofi_tracker.open":
                intent = await self._open(request)
            elif request.handler == "sofi_tracker.action":
                intent = await self._action(request)
            elif request.handler == "sofi_tracker.reminder":
                await self._reminder(request, job_id)
                intent = None
            elif request.handler == "sofi_tracker.cooldown_flush":
                await self._cooldowns.flush("sofi_tracker", request, job_id, self._actions)
                intent = None
            elif request.handler.startswith("sofi_tracker."):
                await self._event(request)
                intent = None
            else:
                intent = self._simple("Unavailable.")
            return self._result(job_id, intent)
        except Exception:
            LOGGER.exception("sofi_tracker_request_failed")
            return self._result(job_id, self._simple("Unavailable."), error="handler_failed")

    async def _open(self, request: TypedRequest) -> DiscordIntent:
        prefix = str(request.event.get("prefix") or ".")
        if request.arguments.get("help"):
            return self._simple(f"{prefix}sofi")
        owner_id = int(request.event.get("actor_id") or 0)
        guild_id = int(request.event.get("guild_id") or 0)
        token = secrets.token_hex(8)
        session = SofiSession(token, owner_id, guild_id)
        await self._cache.setex(f"lumi:sofi:session:{token}", SESSION_TTL, encode(session))
        return await self._dashboard(session, kind="send")

    async def _action(self, request: TypedRequest) -> DiscordIntent:
        token = str(request.arguments.get("session") or "")
        raw = await self._cache.get(f"lumi:sofi:session:{token}")
        if raw is None:
            return self._simple("Unavailable.", kind="edit")
        session = decode(raw, SofiSession)
        if session.owner_id != int(request.event.get("actor_id") or 0):
            return self._simple("Unavailable.", kind="edit")
        lock = self._locks.setdefault(f"{session.guild_id}:{session.owner_id}", asyncio.Lock())
        async with lock:
            state = await self._store.subscription(session.guild_id, session.owner_id)
            action = request.route.partition(".")[2]
            cancel: str | None = None
            if action == "drop":
                state = replace(state, drop_enabled=not state.drop_enabled)
                cancel = None if state.drop_enabled else "drop"
            elif action == "series":
                state = replace(state, series_enabled=not state.series_enabled)
                cancel = None if state.series_enabled else "series"
            elif action == "delivery":
                values = tuple(request.arguments.get("values", ()))
                if values and str(values[0]) in {"dm", "channel"}:
                    state = replace(state, delivery_mode=str(values[0]))
            elif action == "channel":
                values = tuple(int(value) for value in request.arguments.get("values", ()))
                if values:
                    state = replace(state, channel_id=values[0], delivery_mode="channel")
            if state.channel_id is None:
                state = replace(state, channel_id=int(request.event.get("channel_id") or 0) or None)
            await self._store.save_subscription(
                state, modified_by=session.owner_id, cancel_kind=cancel
            )
            await self._publisher.flush_safely()
            await self._cache.setex(f"lumi:sofi:session:{token}", SESSION_TTL, encode(session))
            return await self._dashboard(session, kind="edit")

    async def _dashboard(self, session: SofiSession, *, kind: str) -> DiscordIntent:
        state = await self._store.subscription(session.guild_id, session.owner_id)
        timers = {
            timer.kind: timer
            for timer in await self._store.timers(session.guild_id, session.owner_id)
        }

        def line(label: str, enabled: bool, timer_kind: str) -> str:
            text = f"{label}: {'On' if enabled else 'Off'}"
            timer = timers.get(timer_kind)
            if enabled and timer and timer.active and timer.due_at_ms:
                text += f" - <t:{timer.due_at_ms // 1000}:R>"
            return text

        description = "\n".join(
            (
                line("Drop", state.drop_enabled, "drop"),
                line("Series", state.series_enabled, "series"),
                f"Delivery: {'DM' if state.delivery_mode == 'dm' else 'Channel'}",
                f"Channel: <#{state.channel_id}>" if state.channel_id else "Channel: None",
            )
        )
        controls = (
            PresentationControl("button", "sofi", "drop", session.token, label="Drop"),
            PresentationControl("button", "sofi", "series", session.token, label="Series"),
            PresentationControl(
                "string_select",
                "sofi",
                "delivery",
                session.token,
                placeholder="Delivery",
                options=(
                    PresentationOption("DM", "dm", default=state.delivery_mode == "dm"),
                    PresentationOption(
                        "Channel", "channel", default=state.delivery_mode == "channel"
                    ),
                ),
            ),
            PresentationControl(
                "channel_select",
                "sofi",
                "channel",
                session.token,
                placeholder="Channel",
                default_ids=(state.channel_id,) if state.channel_id else (),
                channel_types=(0, 5),
            ),
        )
        return DiscordIntent(
            kind=kind,
            pages=(PresentationPage(title="Sofi", description=description, controls=controls),),
            session_id=session.token,
        )

    async def _event(self, request: TypedRequest) -> None:
        guild_id = int(request.event.get("guild_id") or 0)
        user_id = int(request.arguments.get("owner") or 0)
        source_message_id = int(request.event.get("message_id") or 0)
        source_at_ms = int(request.event.get("created_at_ms") or 0)
        if not all((guild_id, user_id, source_message_id, source_at_ms)):
            return
        if request.handler in {"sofi_tracker.normal", "sofi_tracker.normal_wait"}:
            kind = "drop"
            offset = DROP_COOLDOWN_MS
        else:
            kind = "series"
            offset = SERIES_COOLDOWN_MS
        wait = request.arguments.get("wait")
        due_at_ms = source_at_ms + (int(wait) * 1_000 if wait is not None else offset)
        changed = await self._store.reconcile_timer(
            guild_id=guild_id,
            user_id=user_id,
            kind=kind,
            due_at_ms=due_at_ms,
            source_message_id=source_message_id,
            source_at_ms=source_at_ms,
        )
        if changed is not None:
            await self._publisher.flush_safely()

    async def _reminder(self, request: TypedRequest, job_id: str) -> None:
        guild = int(request.event.get("guild_id") or 0)
        user = int(request.event.get("actor_id") or 0)
        kind = str(request.arguments.get("kind") or "")
        revision = int(request.arguments.get("revision") or 0)
        await deliver_reminder(
            tracker="sofi_tracker",
            store=self._store,
            actions=self._actions,
            coalescer=getattr(self, "_cooldowns", None),
            guild=guild,
            user=user,
            kind=kind,
            revision=revision,
            job=job_id,
            text=(
                "Your SOFI drop is ready again."
                if kind == "drop"
                else "Your SOFI series drop is ready again."
            ),
        )

    @staticmethod
    def _simple(text: str, *, kind: str = "send") -> DiscordIntent:
        return DiscordIntent(kind=kind, pages=(PresentationPage(description=text),))

    def _result(
        self, job_id: str, intent: DiscordIntent | None, *, error: str | None = None
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="sofi_tracker",
            group="cards",
            generation=self._generation,
            status="error" if error else "ok",
            intents=(intent,) if intent else (),
            error_code=error,
            completed_at_ms=int(time.time() * 1_000),
        )


class PogDropModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        store: MySQLSofiStore,
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._store = store
        self._actions = EdgeActionClient(
            queue_redis, module="pogdrop", group="cards", generation=generation
        )
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()
        self._prune_task: asyncio.Task[None] | None = None

    @property
    def manifest(self) -> Manifest:
        token = r"(?P<session>[a-f0-9]{16})"
        owner = CaptureSpec("owner", "snowflake")
        card_extraction = ComponentExtractionSpec(
            name="cards",
            maximum=3,
            expression=MatcherExpr(
                "all",
                children=(
                    MatcherExpr("exact", "type", "2"),
                    MatcherExpr(
                        "any",
                        children=(


                            MatcherExpr("regex", "custom_id", r"^(?:drop|sdrop)_\d{17,20}_[0-2]$"),
                            MatcherExpr("exact", "emoji.name", "dropheart"),
                            MatcherExpr("exact", "emoji.name", "❤"),
                            MatcherExpr("exact", "emoji.name", "❤️"),
                        ),
                    ),
                ),
            ),
            captures=(
                ComponentCaptureSpec(
                    "wishlist", "label", "compact_integer", minimum=0, maximum=2_147_483_647
                ),
                ComponentCaptureSpec("disabled", "disabled", "boolean", required=False),
                ComponentCaptureSpec("source", "custom_id", required=False, maximum=100),
                ComponentCaptureSpec(
                    "source_message_id",
                    "custom_id",
                    "snowflake",
                    pattern=r"^(?:drop|sdrop)_(?P<source_message_id>\d{17,20})_[0-2]$",
                    required=False,
                ),
                ComponentCaptureSpec(
                    "position",
                    "custom_id",
                    "integer",
                    pattern=r"^(?:drop|sdrop)_\d{17,20}_(?P<position>[0-2])$",
                    required=False,
                    minimum=0,
                    maximum=2,
                ),
            ),
        )
        card_identity_extraction = RepeatedExtraction(
            field="message_document",
            pattern=(
                r"(?m)^[ \t]*`?(?P<position>[1-3])\.`?[ \t]+[^|\r\n]{1,200}\|"
                r"[ \t]*G[ \t]*[•·:][ \t]*`?[ \t]*(?P<generation>\d{1,10})?"
                r"[ \t]*`?[ \t]*\|[ \t]*(?P<name>[^\r\n|•·]{1,200}?)[ \t]+"
                r"[•·][ \t]+(?P<series>[^\r\n]{1,200}?)[ \t]*$"
            ),
            maximum=3,
            captures=(
                CaptureSpec("position", "integer", minimum=1, maximum=3),
                CaptureSpec(
                    "generation", "integer", required=False, minimum=0, maximum=2_147_483_647
                ),
                CaptureSpec("name", "normalized_text", minimum=1, maximum=200),
                CaptureSpec("series", "normalized_text", minimum=1, maximum=200),
            ),
        )
        patterns = {
            "normal": (
                r"(?is)^[\s\S]*?<@!?(?P<owner>\d{17,20})>[\s*]+is[\s*]+"
                r"dropping[\s*]+cards\b[\s\S]*$"
            ),
            "mass": (
                r"(?is)^Mass\s+drop\s+(?P<page>[1-9]\d*)/(?P<total>[1-9]\d*)\s+by\s+"
                r"<@!?(?P<owner>\d{17,20})>[\s\S]*$"
            ),
            "series": (
                r"(?is)^<@!?(?P<owner>\d{17,20})>\s+(?:(?P<series_before>.{1,200}?)"
                r"\s+was\s+selected|selected\s+(?P<series_after>.{1,200}?))!\s+"
                r"Dropping\s+(?P<count>[1-3])\s+"
                r"characters?\.{3}[\s\S]*$"
            ),
            "vote": r"(?is)^The\s+vote\s+has\s+concluded!\s*$",
        }
        routes: list[PassiveMatcherSpec] = []
        context = (
            "primary_image_url",
            "author_name",
            "author_avatar_url",
            "embed_author_name",
            "embed_author_icon_url",
            "channel_parent_id",
            "channel_nsfw",
            "reply_author_id",
            "reference_message_id",
        )
        for event in ("message_create", "message_edit"):
            suffix = "create" if event == "message_create" else "edit"
            for kind in ("normal", "mass", "series"):
                captures = (owner,)
                if kind == "mass":
                    captures += (
                        CaptureSpec("page", "integer", minimum=1, maximum=10_000),
                        CaptureSpec("total", "integer", minimum=1, maximum=10_000),
                    )
                elif kind == "series":
                    captures += (
                        CaptureSpec(
                            "series_before",
                            "normalized_text",
                            required=False,
                            minimum=1,
                            maximum=200,
                        ),
                        CaptureSpec(
                            "series_after",
                            "normalized_text",
                            required=False,
                            minimum=1,
                            maximum=200,
                        ),
                        CaptureSpec("count", "integer", minimum=1, maximum=3),
                    )
                routes.append(
                    _passive(
                        route=f"pogdrop.{kind}_{suffix}",
                        handler=f"pogdrop.{kind}",
                        event=event,
                        pattern=patterns[kind],
                        captures=captures,
                        context_fields=context,
                        repeated=(card_identity_extraction,) if kind in {"normal", "mass"} else (),
                        component_extractions=(card_extraction,),
                        watch_ttl_ms=15_300_000 if kind == "series" else 600_000,
                        presentation_mode="legacy",
                    )
                )
            routes.append(
                _passive(
                    route=f"pogdrop.vote_{suffix}",
                    handler="pogdrop.vote",
                    event=event,
                    pattern=patterns["vote"],
                    captures=(),
                    subject_capture=None,
                    subject_field="reply_author_id",
                    context_fields=context,
                    component_extractions=(card_extraction,),
                    watch_ttl_ms=15_300_000,
                    presentation_mode="legacy",
                    reference_author_max_age_ms=20 * 60 * 1_000,
                )
            )
        return Manifest(
            schema=2,
            module="pogdrop",
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Pog Drop",
            summary="High-wishlist card feeds.",
            presentation_mode="v2",
            version="1.1.2",
            commands=(
                CommandSpec(
                    root="pogdrop",
                    handler="pogdrop.open",
                    grammar=r"^(?:\s+(?P<help>help))?$",
                    captures=(CaptureSpec("help", required=False, maximum=4),),
                    help=HelpSpec(".pogdrop", "Manage high-wishlist Sofi drop feeds."),
                    acl_key="pogdrop",
                    public=False,
                    required_permissions=("administrator",),
                    context_fields=("prefix",),
                ),
            ),
            passive=tuple(routes),
            components=(
                ComponentSpec(
                    "pogdrop",
                    "toggle",
                    "pogdrop.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="pogdrop",
                    required_permissions=("administrator",),
                    session_bound=True,
                ),
                ComponentSpec(
                    "pogdrop",
                    "channel",
                    "pogdrop.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="pogdrop",
                    required_permissions=("administrator",),
                    input_kind="channel_ids",
                    channel_types=(0, 5),
                    session_bound=True,
                ),
                ComponentSpec(
                    "pogdrop",
                    "threshold",
                    "pogdrop.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="pogdrop",
                    required_permissions=("administrator",),
                    input_kind="modal",
                    modal=ModalSpec(
                        "Wishlist",
                        (
                            ModalFieldSpec(
                                "threshold",
                                "Wishlist",
                                minimum=1,
                                maximum=10,
                                placeholder="100",
                            ),
                        ),
                    ),
                    session_bound=True,
                ),
            ),
            capabilities=("send_message",),
        )

    async def start(self) -> None:
        self._prune_task = asyncio.create_task(self._prune_loop(), name="pogdrop-prune")

    async def close(self) -> None:
        if self._prune_task is not None:
            self._prune_task.cancel()
            await asyncio.gather(self._prune_task, return_exceptions=True)

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        del generation
        try:
            if request.handler == "pogdrop.open":
                intent = await self._open(request)
            elif request.handler == "pogdrop.action":
                intent = await self._action(request)
            elif request.handler.startswith("pogdrop."):
                await self._drop(request, job_id)
                intent = None
            else:
                intent = self._simple("Unavailable.")
            return self._result(job_id, intent)
        except Exception:
            LOGGER.exception("pogdrop_request_failed")
            return self._result(job_id, self._simple("Unavailable."), error="handler_failed")

    async def _open(self, request: TypedRequest) -> DiscordIntent:
        prefix = str(request.event.get("prefix") or ".")
        if request.arguments.get("help"):
            return self._simple(f"{prefix}pogdrop")
        session = PogDropSession(
            secrets.token_hex(8),
            int(request.event.get("actor_id") or 0),
            int(request.event.get("guild_id") or 0),
        )
        await self._cache.setex(
            f"lumi:pogdrop:session:{session.token}", SESSION_TTL, encode(session)
        )
        return await self._dashboard(session, kind="send")

    async def _action(self, request: TypedRequest) -> DiscordIntent:
        token = str(request.arguments.get("session") or "")
        raw = await self._cache.get(f"lumi:pogdrop:session:{token}")
        if raw is None:
            return self._simple("Unavailable.", kind="edit")
        session = decode(raw, PogDropSession)
        if session.owner_id != int(request.event.get("actor_id") or 0):
            return self._simple("Unavailable.", kind="edit")
        lock = self._locks.setdefault(str(session.guild_id), asyncio.Lock())
        async with lock:
            feed = await self._store.feed(session.guild_id)
            action = request.route.partition(".")[2]
            notice: str | None = None
            if action == "toggle":
                if not feed.enabled and feed.channel_id is None:
                    notice = "Choose a channel."
                else:
                    feed = replace(feed, enabled=not feed.enabled, needs_repair=False)
                    feed = await self._store.save_feed(feed, modified_by=session.owner_id)
            elif action == "channel":
                values = tuple(int(value) for value in request.arguments.get("values", ()))
                if values:
                    feed = replace(feed, channel_id=values[0], enabled=True, needs_repair=False)
                    feed = await self._store.save_feed(feed, modified_by=session.owner_id)
            elif action == "threshold":
                values = request.arguments.get("values", {})
                try:
                    threshold = int(str(values.get("threshold") or ""))
                except (TypeError, ValueError):
                    threshold = 0
                if not 1 <= threshold <= 2_147_483_647:
                    notice = "Invalid input."
                else:
                    feed = replace(feed, threshold=threshold)
                    feed = await self._store.save_feed(feed, modified_by=session.owner_id)
            await self._cache.setex(f"lumi:pogdrop:session:{token}", SESSION_TTL, encode(session))
            return await self._dashboard(session, kind="edit", notice=notice)

    async def _dashboard(
        self, session: PogDropSession, *, kind: str, notice: str | None = None
    ) -> DiscordIntent:
        feed = await self._store.feed(session.guild_id)
        lines = [
            "Enabled" if feed.enabled else "Disabled",
            f"Channel: <#{feed.channel_id}>" if feed.channel_id else "Channel: None",
            f"Wishlist: {feed.threshold}+",
        ]
        if feed.needs_repair:
            lines.append("Needs repair.")
        if notice:
            lines.append(notice)
        controls = (
            PresentationControl(
                "button",
                "pogdrop",
                "toggle",
                session.token,
                label="Disable" if feed.enabled else "Enable",
            ),
            PresentationControl(
                "channel_select",
                "pogdrop",
                "channel",
                session.token,
                placeholder="Channel",
                default_ids=(feed.channel_id,) if feed.channel_id else (),
                channel_types=(0, 5),
            ),
            PresentationControl("button", "pogdrop", "threshold", session.token, label="Wishlist"),
        )
        return DiscordIntent(
            kind=kind,
            pages=(
                PresentationPage(title="Pog Drop", description="\n".join(lines), controls=controls),
            ),
            session_id=session.token,
        )

    async def _drop(self, request: TypedRequest, job_id: str) -> None:
        guild_id = int(request.event.get("guild_id") or 0)
        source_channel_id = int(request.event.get("channel_id") or 0)
        source_message_id = int(request.event.get("message_id") or 0)
        owner_id = int(
            request.arguments.get("owner")
            or request.event.get("reply_author_id")
            or request.event.get("actor_id")
            or 0
        )
        if not all((guild_id, source_channel_id, source_message_id, owner_id)):
            return
        feed = await self._store.feed(guild_id)
        if not feed.enabled or feed.needs_repair or feed.channel_id is None:
            return
        if feed.channel_id in {
            source_channel_id,
            int(request.event.get("channel_parent_id") or 0),
        }:
            return
        cards = request.arguments.get("cards", ())
        if not isinstance(cards, (list, tuple)) or not 1 <= len(cards) <= 3:
            return
        kind = request.handler.partition(".")[2]
        if kind in {"normal", "mass"} and len(cards) != 3:
            return
        if kind == "series" and len(cards) != int(request.arguments.get("count") or 0):
            return
        if kind == "mass" and int(request.arguments.get("page") or 0) > int(
            request.arguments.get("total") or 0
        ):
            return
        if kind == "vote":
            reference_id = int(request.event.get("reference_message_id") or 0)
            if not reference_id or any(
                int(card.get("source_message_id") or 0) != reference_id
                or int(card.get("position") if card.get("position") is not None else -1) != index
                for index, card in enumerate(cards)
                if isinstance(card, dict)
            ):
                return
        if not any(
            isinstance(card, dict)
            and isinstance(card.get("wishlist"), int)
            and int(card["wishlist"]) >= feed.threshold
            for card in cards
        ):
            return
        image_url = str(request.event.get("primary_image_url") or "")
        if not image_url.startswith("https://"):
            return
        lines = self._drop_lines(request, cards, feed.threshold, kind)
        if not lines:
            return
        if not await self._store.reserve_delivery(
            "sofi", source_message_id, guild_id, feed.channel_id, feed.revision
        ):
            return
        jump = f"https://discord.com/channels/{guild_id}/{source_channel_id}/{source_message_id}"
        noun = "card" if len(cards) == 1 else "cards"
        description = (
            f"<@{owner_id}> is dropping {len(cards)} {noun}!\n\n"
            + "\n".join(lines)
            + f"\n\n[Open message]({jump})"
        )
        page = PresentationPage(
            description=description,
            image_url=image_url,
            author_name=str(request.event.get("author_name") or "SOFI")[:256],
            author_icon_url=str(request.event.get("author_avatar_url") or "") or None,
            timestamp_ms=int(request.event.get("created_at_ms") or 0) or None,
        )
        intent = DiscordIntent(
            kind="send",
            pages=(page,),
            presentation_mode="legacy",
        )
        try:
            result = await self._actions.request(
                job_id=job_id,
                kind="send_message",
                guild_id=guild_id,
                channel_id=feed.channel_id,
                member_id=owner_id,
                intent=intent,
                idempotency_key=f"pogdrop:sofi:{source_message_id}",
                forbid_source_channel=True,
                respect_nsfw=True,
            )
        except ActionUnavailable:
            await self._store.finish_delivery("sofi", source_message_id, "uncertain")
            return
        if result.ok:
            await self._store.finish_delivery(
                "sofi", source_message_id, "sent", result.data.get("message_id")
            )
        else:
            status = "uncertain" if result.error_code == "ambiguous" else "failed"
            await self._store.finish_delivery("sofi", source_message_id, status)
            if result.error_code in {"channel_unavailable", "permission_denied"}:
                await self._store.mark_feed_repair(guild_id)

    @classmethod
    def _drop_lines(
        cls,
        request: TypedRequest,
        cards: list[dict] | tuple[dict, ...],
        threshold: int,
        kind: str,
    ) -> tuple[str, ...]:
        identities = request.arguments.get("message_document", ())
        by_position: dict[int, dict] = {}
        if isinstance(identities, (list, tuple)):
            for identity in identities:
                if not isinstance(identity, dict):
                    continue
                position = identity.get("position")
                if isinstance(position, int) and position not in by_position:
                    by_position[position] = identity
        if kind in {"normal", "mass"} and set(by_position) != set(range(1, len(cards) + 1)):
            return ()

        series_name = request.arguments.get("series_before") or request.arguments.get(
            "series_after"
        )
        lines: list[str] = []
        for index, card in enumerate(cards, start=1):
            if not isinstance(card, dict):
                return ()
            wishlist = card.get("wishlist")
            if not isinstance(wishlist, int) or wishlist < threshold:
                continue
            identity = by_position.get(index)
            if identity is not None:
                generation = identity.get("generation")
                name = identity.get("name")
                series = identity.get("series")
                if (
                    (generation is not None and not isinstance(generation, int))
                    or not isinstance(name, str)
                    or not isinstance(series, str)
                ):
                    return ()
                generation_text = f" - #{generation}" if generation is not None else ""
                lines.append(
                    f"♡ {wishlist:,}{generation_text} - {cls._escape_markdown(name)} - "
                    f"{cls._escape_markdown(series)}"
                )
                continue
            line = f"♡ {wishlist:,} - Card {index}"
            if isinstance(series_name, str) and series_name:
                line += f" - {cls._escape_markdown(series_name.strip('*` '))}"
            lines.append(line)
        return tuple(lines)

    @staticmethod
    def _escape_markdown(value: str) -> str:
        escaped: list[str] = []
        for character in value:
            if character in "\\`*_{}[]()#+-.!|>~":
                escaped.append("\\")
            escaped.append("@\u200b" if character == "@" else character)
        return "".join(escaped)

    async def _prune_loop(self) -> None:
        while True:
            with contextlib.suppress(Exception):
                await self._store.prune_receipts()
            await asyncio.sleep(3_600)

    @staticmethod
    def _simple(text: str, *, kind: str = "send") -> DiscordIntent:
        return DiscordIntent(kind=kind, pages=(PresentationPage(description=text),))

    def _result(
        self, job_id: str, intent: DiscordIntent | None, *, error: str | None = None
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="pogdrop",
            group="cards",
            generation=self._generation,
            status="error" if error else "ok",
            intents=(intent,) if intent else (),
            error_code=error,
            completed_at_ms=int(time.time() * 1_000),
        )
