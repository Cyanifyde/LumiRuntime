from __future__ import annotations

import asyncio
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
    ComponentSpec,
    DiscordIntent,
    FieldExtraction,
    HelpSpec,
    Manifest,
    MatcherExpr,
    PassiveMatcherSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
    ScheduledCallbackSpec,
    TypedRequest,
    decode,
    encode,
)
from lumi.cooldowns.service import deliver_reminder, flush_spec
from lumi.worker.actions import EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from .store import MySQLKarutaTrackerStore

LOGGER = logging.getLogger(__name__)
KARUTA_BOT_ID = -2
SESSION_TTL = 900
DROP_NORMAL_MS = 30 * 60 * 1_000
DROP_BLESSED_MS = 15 * 60 * 1_000
PICKUP_NORMAL_MS = 20 * 60 * 1_000
PICKUP_BLESSED_MS = 10 * 60 * 1_000
DELIVERY_WINDOW_MS = 5 * 60 * 1_000


class KarutaSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
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
) -> PassiveMatcherSpec:
    return PassiveMatcherSpec(
        route=route,
        handler=handler,
        event=event,
        bot_ids=(KARUTA_BOT_ID,),
        expression=MatcherExpr("regex", "message_document", pattern),
        extractions=(FieldExtraction("message_document", pattern, captures),),
        subject_capture="owner",
        subject_required=True,
        hydrate=event == "message_edit",
        watch_ttl_ms=60 * 60 * 1_000,
        deadline_ms=120_000,
        context_fields=("event_order",),
    )


class _SchedulePublisher:
    def __init__(self, store: MySQLKarutaTrackerStore, client: ScheduledCallbackClient) -> None:
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
                route="karuta_tracker.reminder",
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
                    route="karuta_tracker.reminder",
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
            LOGGER.exception("karuta_schedule_outbox_publish_failed")
        return count

    @staticmethod
    def _key(guild_id: int, user_id: int, kind: str) -> str:
        return f"karuta_tracker:{guild_id}:{user_id}:{kind}"


class KarutaTrackerModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        store: MySQLKarutaTrackerStore,
        coalescer=None,
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._store = store
        self._cooldowns = coalescer
        self._actions = EdgeActionClient(
            queue_redis, module="karuta_tracker", group="cards", generation=generation
        )
        self._publisher = _SchedulePublisher(
            store,
            ScheduledCallbackClient(
                queue_redis, module="karuta_tracker", group="cards", generation=generation
            ),
        )
        self._publisher_task: asyncio.Task[None] | None = None
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    @property
    def manifest(self) -> Manifest:
        token = r"(?P<session>[a-f0-9]{16})"
        owner = CaptureSpec("owner", "snowflake")
        count = CaptureSpec("wait_count", "integer", minimum=1, maximum=120)
        unit = CaptureSpec("wait_unit", "normalized_text", minimum=6, maximum=7)
        drop_success = (
            r"(?is)^\s*\*{0,2}<@!?(?P<owner>\d{17,20})>\*{0,2}\s+is\s+"
            r"dropping\s+(?:3|4)\s+cards?\b[\s\S]*$"
        )
        pickup_success = (
            r"(?is)^\s*\*{0,2}<@!?(?P<owner>\d{17,20})>\*{0,2}\s+took\s+"
            r"the\s+card\b[\s\S]*$"
        )
        drop_wait = (
            r"(?is)^[\s\S]*?<@!?(?P<owner>\d{17,20})>[\s\S]*?you\s+must\s+wait\s+"
            r"[`*_~]{0,3}(?P<wait_count>\d{1,3})\s+(?P<wait_unit>seconds?|minutes?)"
            r"[`*_~]{0,3}\s+before\s+dropping\s+more\s+cards\b[\s\S]*$"
        )
        pickup_wait = (
            r"(?is)^[\s\S]*?<@!?(?P<owner>\d{17,20})>[\s\S]*?you\s+must\s+wait\s+"
            r"[`*_~]{0,3}(?P<wait_count>\d{1,3})\s+(?P<wait_unit>seconds?|minutes?)"
            r"[`*_~]{0,3}\s+before\s+grabbing\s+another\s+card\b[\s\S]*$"
        )
        routes: list[PassiveMatcherSpec] = []
        patterns = (
            ("drop_success", drop_success, (owner,)),
            ("pickup_success", pickup_success, (owner,)),
            ("drop_wait", drop_wait, (owner, count, unit)),
            ("pickup_wait", pickup_wait, (owner, count, unit)),
        )
        for event in ("message_create", "message_edit"):
            suffix = "create" if event == "message_create" else "edit"
            for action, pattern, captures in patterns:
                routes.append(
                    _passive(
                        route=f"karuta_tracker.{action}_{suffix}",
                        handler=f"karuta_tracker.{action}",
                        event=event,
                        pattern=pattern,
                        captures=captures,
                    )
                )
        return Manifest(
            schema=2,
            module="karuta_tracker",
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Karuta Tracker",
            summary="Personal Karuta cooldown reminders.",
            presentation_mode="v2",
            version="1.0.2",
            commands=(
                CommandSpec(
                    root="karuta",
                    handler="karuta_tracker.open",
                    grammar=r"^(?:\s+(?P<help>help))?$",
                    captures=(CaptureSpec("help", required=False, maximum=4),),
                    help=HelpSpec(".karuta", "Set your Karuta drop and pickup reminders."),
                    acl_key="karuta",
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
            passive=tuple(routes),
            components=(
                ComponentSpec(
                    "karuta",
                    "drop",
                    "karuta_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="karuta",
                    public=True,
                    session_bound=True,
                ),
                ComponentSpec(
                    "karuta",
                    "pickup",
                    "karuta_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="karuta",
                    public=True,
                    session_bound=True,
                ),
                ComponentSpec(
                    "karuta",
                    "delivery",
                    "karuta_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="karuta",
                    public=True,
                    input_kind="string_values",
                    session_bound=True,
                ),
                ComponentSpec(
                    "karuta",
                    "channel",
                    "karuta_tracker.action",
                    rf"^{token}$",
                    captures=(CaptureSpec("session"),),
                    acl_key="karuta",
                    public=True,
                    input_kind="channel_ids",
                    channel_types=(0, 5),
                    session_bound=True,
                ),
            ),
            scheduled=(
                ScheduledCallbackSpec(
                    route="karuta_tracker.reminder",
                    handler="karuta_tracker.reminder",
                    deadline_ms=DELIVERY_WINDOW_MS,
                    presentation_mode="plain",
                ),
                flush_spec("karuta_tracker"),
            ),
            capabilities=("send_dm", "read_mention_batch_members", "send_mention_batch"),
        )

    async def start(self) -> None:
        self._publisher_task = asyncio.create_task(
            self._publisher.run(), name="karuta-schedule-publisher"
        )

    async def close(self) -> None:
        self._publisher.stop()
        if self._publisher_task is not None:
            self._publisher_task.cancel()
            await asyncio.gather(self._publisher_task, return_exceptions=True)

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        del generation
        try:
            if request.handler == "karuta_tracker.open":
                intent = await self._open(request)
            elif request.handler == "karuta_tracker.action":
                intent = await self._action(request)
            elif request.handler == "karuta_tracker.reminder":
                await self._reminder(request, job_id)
                intent = None
            elif request.handler == "karuta_tracker.cooldown_flush":
                await self._cooldowns.flush("karuta_tracker", request, job_id, self._actions)
                intent = None
            elif request.handler.startswith("karuta_tracker."):
                await self._event(request)
                intent = None
            else:
                intent = self._simple("Unavailable.")
            return self._result(job_id, intent)
        except Exception:
            LOGGER.exception("karuta_tracker_request_failed")
            return self._result(job_id, self._simple("Unavailable."), error="handler_failed")

    async def _open(self, request: TypedRequest) -> DiscordIntent:
        prefix = str(request.event.get("prefix") or ".")
        if request.arguments.get("help"):
            return self._simple(f"{prefix}karuta")
        session = KarutaSession(
            secrets.token_hex(8),
            int(request.event.get("actor_id") or 0),
            int(request.event.get("guild_id") or 0),
        )
        await self._cache.setex(
            f"lumi:karuta:session:{session.token}", SESSION_TTL, encode(session)
        )
        return await self._dashboard(session, kind="send")

    async def _action(self, request: TypedRequest) -> DiscordIntent:
        token = str(request.arguments.get("session") or "")
        raw = await self._cache.get(f"lumi:karuta:session:{token}")
        if raw is None:
            return self._simple("Unavailable.", kind="edit")
        session = decode(raw, KarutaSession)
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
            elif action == "pickup":
                state = replace(state, pickup_enabled=not state.pickup_enabled)
                cancel = None if state.pickup_enabled else "pickup"
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
            await self._cache.setex(f"lumi:karuta:session:{token}", SESSION_TTL, encode(session))
            return await self._dashboard(session, kind="edit")

    async def _dashboard(self, session: KarutaSession, *, kind: str) -> DiscordIntent:
        state = await self._store.subscription(session.guild_id, session.owner_id)
        timers = {
            timer.kind: timer
            for timer in await self._store.timers(session.guild_id, session.owner_id)
        }

        def line(label: str, enabled: bool, timer_kind: str) -> str:
            text = f"{label}: {'On' if enabled else 'Off'}"
            timer = timers.get(timer_kind)
            if enabled and timer and timer.active and timer.due_at_ms:
                text += f" - <t:{timer.due_at_ms // 1_000}:R>"
            return text

        description = "\n".join(
            (
                line("Drop", state.drop_enabled, "drop"),
                line("Pickup", state.pickup_enabled, "pickup"),
                f"Delivery: {'DM' if state.delivery_mode == 'dm' else 'Channel'}",
                f"Channel: <#{state.channel_id}>" if state.channel_id else "Channel: None",
            )
        )
        controls = (
            PresentationControl("button", "karuta", "drop", session.token, label="Drop"),
            PresentationControl("button", "karuta", "pickup", session.token, label="Pickup"),
            PresentationControl(
                "string_select",
                "karuta",
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
                "karuta",
                "channel",
                session.token,
                placeholder="Channel",
                default_ids=(state.channel_id,) if state.channel_id else (),
                channel_types=(0, 5),
            ),
        )
        return DiscordIntent(
            kind=kind,
            pages=(PresentationPage(title="Karuta", description=description, controls=controls),),
            session_id=session.token,
        )

    async def _event(self, request: TypedRequest) -> None:
        guild_id = int(request.event.get("guild_id") or 0)
        user_id = int(request.arguments.get("owner") or 0)
        source_message_id = int(request.event.get("message_id") or 0)
        source_at_ms = int(request.event.get("created_at_ms") or 0)
        source_order = int(request.event.get("event_order") or 0)
        if not all((guild_id, user_id, source_message_id, source_at_ms)):
            return
        action = request.handler.removeprefix("karuta_tracker.")
        kind = "drop" if action.startswith("drop_") else "pickup"
        if kind == "drop":
            normal_ms, half_ms = DROP_NORMAL_MS, DROP_BLESSED_MS
        else:
            normal_ms, half_ms = PICKUP_NORMAL_MS, PICKUP_BLESSED_MS
        if action.endswith("_success"):
            changed = await self._store.record_success(
                guild_id=guild_id,
                user_id=user_id,
                kind=kind,
                source_message_id=source_message_id,
                source_at_ms=source_at_ms,
                source_order=source_order,
                normal_ms=normal_ms,
                half_ms=half_ms,
            )
        else:
            count = int(request.arguments.get("wait_count") or 0)
            unit = str(request.arguments.get("wait_unit") or "").casefold()
            if count <= 0 or unit not in {"second", "seconds", "minute", "minutes"}:
                return
            seconds = count if unit.startswith("second") else count * 60
            changed = await self._store.record_wait(
                guild_id=guild_id,
                user_id=user_id,
                kind=kind,
                source_message_id=source_message_id,
                source_at_ms=source_at_ms,
                source_order=source_order,
                remaining_ms=seconds * 1_000,
                rounding_ms=1_000 if unit.startswith("second") else 60_000,
                normal_ms=normal_ms,
                half_ms=half_ms,
            )
        if changed is not None:
            await self._publisher.flush_safely()

    async def _reminder(self, request: TypedRequest, job_id: str) -> None:
        guild = int(request.event.get("guild_id") or 0)
        user = int(request.event.get("actor_id") or 0)
        kind = str(request.arguments.get("kind") or "")
        revision = int(request.arguments.get("revision") or 0)
        await deliver_reminder(
            tracker="karuta_tracker",
            store=self._store,
            actions=self._actions,
            coalescer=getattr(self, "_cooldowns", None),
            guild=guild,
            user=user,
            kind=kind,
            revision=revision,
            job=job_id,
            text=(
                "Your Karuta drop is ready again."
                if kind == "drop"
                else "Your Karuta pickup is ready again."
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
            module="karuta_tracker",
            group="cards",
            generation=self._generation,
            status="error" if error else "ok",
            intents=(intent,) if intent else (),
            error_code=error,
            completed_at_ms=int(time.time() * 1_000),
        )
