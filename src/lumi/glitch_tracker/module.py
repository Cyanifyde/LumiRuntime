from __future__ import annotations

import asyncio
import logging
import secrets
import time
import weakref
from dataclasses import replace

import msgspec

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

from .drop_sources import WINDOW_MS, GlitchDropSources
from .store import COOLDOWNS, DELIVERY_WINDOW_MS, MySQLGlitchTrackerStore, Observation

LOGGER = logging.getLogger(__name__)
MODULE = "glitch_tracker"
GLITCH_BOT_ID = -1
SESSION_TTL = 900
SESSION_PREFIX = "lumi:glitch:session:"


class GlitchSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int


def optional(pattern: str, capture: CaptureSpec) -> FieldExtraction:
    return FieldExtraction(
        "message_document",
        "(?is)^(?:[\\s\\S]*?" + pattern + r"[\s\S]*|[\s\S]*)$",
        (capture,),
    )


def passive(
    action, event, pattern, extractions, *, subject="owner", field=None, guard=None, required=True,
    unless_routes_match=(),
):
    checks = [
        MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
        MatcherExpr("regex", "message_document", pattern),
    ]
    if guard:
        checks.append(guard)
    return PassiveMatcherSpec(
        route=f"{MODULE}.{action}_{'edit' if event == 'message_edit' else 'create'}"
        + ("_" + field if field else ""),
        handler=f"{MODULE}.{action}",
        event=event,
        bot_ids=(GLITCH_BOT_ID,),
        expression=MatcherExpr("all", children=tuple(checks)),
        extractions=tuple(extractions),
        subject_capture=subject,
        subject_field=field,
        subject_required=required,
        hydrate=event == "message_edit",
        watch_ttl_ms=25 * 60_000,
        deadline_ms=120_000,
        context_fields=(
            "event_order",
            "reference_message_id",
            "source_revision_ms",
        ),
        reference_author_max_age_ms=120_000 if field == "reply_author_id" else 0,
        unless_routes_match=unless_routes_match,
    )


class SchedulePublisher:
    def __init__(self, store, client, clock):
        self.store, self.client, self.clock = store, client, clock

    async def run(self):
        while True:
            try:
                await self.replay()
                break
            except Exception:
                LOGGER.exception("glitch_schedule_replay_failed")
                await asyncio.sleep(1)
        while True:
            await self.flush_safely()
            await asyncio.sleep(1)

    async def publish(self, guild, user, kind, revision, operation, due, channel):
        now = self.clock()
        if operation == "cancel":
            due = now
        await self.client.publish(
            operation=operation,
            schedule_key=f"{MODULE}:{guild}:{user}:{kind}",
            route=f"{MODULE}.reminder",
            revision=revision,
            guild_id=guild,
            channel_id=channel,
            actor_id=user,
            due_at_ms=due,
            expires_at_ms=due + DELIVERY_WINDOW_MS,
            arguments={"kind": kind, "revision": revision},
        )

    async def replay(self):
        for timer, channel in await self.store.active_timers():
            if timer.due_at_ms is None or timer.due_at_ms + DELIVERY_WINDOW_MS <= self.clock():
                await self.store.finish_timer(
                    timer.guild_id,
                    timer.user_id,
                    timer.kind,
                    timer.revision,
                )
                continue
            await self.publish(
                timer.guild_id,
                timer.user_id,
                timer.kind,
                timer.revision,
                "upsert",
                timer.due_at_ms,
                channel,
            )

    async def flush_safely(self):
        count = 0
        try:
            for item in await self.store.pending_schedule_outbox():
                if (
                    item.operation == "upsert"
                    and item.due_at_ms + DELIVERY_WINDOW_MS <= self.clock()
                ):
                    await self.store.finish_timer(
                        item.guild_id,
                        item.user_id,
                        item.kind,
                        item.revision,
                    )
                else:
                    await self.publish(
                        item.guild_id,
                        item.user_id,
                        item.kind,
                        item.revision,
                        item.operation,
                        item.due_at_ms,
                        item.channel_id,
                    )
                await self.store.mark_schedule_published(item.identifier)
                count += 1
        except Exception:
            LOGGER.exception("glitch_schedule_outbox_publish_failed")
        return count


class GlitchTrackerModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        cache_redis,
        store: MySQLGlitchTrackerStore,
        clock=None,
        coalescer=None,
    ):
        self._generation, self._cache, self._store = generation, cache_redis, store
        self._cooldowns = coalescer
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._actions = EdgeActionClient(
            queue_redis,
            module=MODULE,
            group="cards",
            generation=generation,
        )
        self._scheduler = ScheduledCallbackClient(
            queue_redis, module=MODULE, group="cards", generation=generation,
        )
        self._publisher = SchedulePublisher(store, self._scheduler, self.clock)
        self._drop_sources = GlitchDropSources(cache_redis, self.clock)
        self._publisher_task = None
        self._locks = weakref.WeakValueDictionary()

    @property
    def manifest(self):
        token = r"(?P<session>[a-f0-9]{16})"
        owner = CaptureSpec("owner", "snowflake")
        owner_pattern = r"<@!?(?P<owner>\d{17,20})>"
        extra = optional(
            r"\((?P<extra>extra[ \t]+(?:drop|grab))[ \t]*[·•-][^\r\n)]*\)",
            CaptureSpec("extra", "normalized_text", required=False, maximum=10),
        )
        interaction = MatcherExpr("regex", "interaction_user_id", r"^[1-9]\d{16,19}$")
        routes = []
        for event in ("message_create", "message_edit"):
            event_suffix = event.removeprefix("message_")
            notice = r"(?is)^[\s\S]*?\(extra[ \t]+drop[ \t]*[·•-][^\r\n)]*\)[\s\S]*$"
            routes.append(
                passive(
                    "extra_unbound",
                    event,
                    notice,
                    (),
                    subject=None,
                    required=False,
                    unless_routes_match=(
                        f"{MODULE}.extra_drop_{event_suffix}_interaction_user_id",
                        f"{MODULE}.extra_drop_{event_suffix}_reply_author_id",
                    ),
                )
            )
            for kind, ending in (
                ("drop", r"dropped[ \t]+[1-9]\d?[ \t]+cards?![^\r\n]*"),
                (
                    "grab",
                    r"grabbed[ \t]+#[\d,]+[ \t]+[^\r\n]{1,300}![ \t]+"
                    r"`[a-z0-9]{1,32}`[^\r\n]*",
                ),
            ):
                pattern = r"(?is)^[ \t*]*" + owner_pattern + r"[ \t*]+" + ending + r"[\s\S]*$"
                routes.append(
                    passive(
                        "success_" + kind,
                        event,
                        pattern,
                        (FieldExtraction("message_document", pattern, (owner,)), extra),
                    )
                )
                if kind == "drop":


                    routes.append(passive(
                        "drop_source", event, pattern,
                        (FieldExtraction("message_document", pattern, (owner,)),),
                        subject=None, required=False,
                    ))
                wait_pattern = (
                    r"(?is)^[\s\S]*?(?:Your[ \t]+next[ \t]+free[ \t]+"
                    + kind
                    + r"[ \t]+(?:is[ \t]+)?ready[ \t]+in|"
                    + kind
                    + r"[ \t]+(?:will[ \t]+be[ \t]+ready|ready)[ \t]+in)"
                    r"[ \t:*`]*(?P<wait>(?:\d+[smh][ \t]*)+)[\s\S]*$"
                )
                wait = FieldExtraction(
                    "message_document",
                    wait_pattern,
                    (CaptureSpec("wait", "duration", minimum=1, maximum=COOLDOWNS[kind] // 1000),),
                )
                due = optional(
                    r"\*\*" + kind + r"\*\*[^\r\n]*?<t:(?P<due>\d{1,10}):[A-Za-z]>",
                    CaptureSpec("due", "integer", required=False, minimum=1, maximum=9_999_999_999),
                )
                ready = optional(
                    r"\*\*" + kind + r"\*\*[^\r\n]*?(?P<ready>ready[ \t]+now)",
                    CaptureSpec("ready", "normalized_text", required=False, maximum=12),
                )


                for field, guard in (
                    ("interaction_user_id", interaction),
                    ("reply_author_id", MatcherExpr("not", children=(interaction,))),
                ):
                    status_guard = MatcherExpr(
                        "all",
                        children=(
                            guard,
                            MatcherExpr(
                                "regex",
                                "embeds.title",
                                r"(?i)^(?:<[^>]+>\s*)?Your\s+Cooldowns\s*$",
                            ),
                        ),
                    )
                    routes.append(
                        passive(
                            "status_" + kind,
                            event,
                            r"(?s)^[\s\S]*$",
                            (due, ready),
                            subject=None,
                            field=field,
                            guard=status_guard,
                        )
                    )
                    routes.append(
                        passive(
                            "wait_" + kind,
                            event,
                            wait_pattern,
                            (wait,),
                            subject=None,
                            field=field,
                            guard=MatcherExpr(
                                "all",
                                children=(
                                    guard,
                                    MatcherExpr(
                                        "not",
                                        children=(
                                            MatcherExpr(
                                                "regex",
                                                "message_document",
                                                r"(?is)^\s*<@!?\d{17,20}>",
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        )
                    )
                    routes.append(
                        passive(
                            "extra_" + kind,
                            event,
                            r"(?is)^[\s\S]*?\(extra[ \t]+"
                            + kind
                            + r"[ \t]*[·•-][^\r\n)]*\)[\s\S]*$",
                            (),
                            subject=None,
                            field=field,
                            guard=guard,
                        )
                    )

                mentioned_wait = r"(?is)^\s*" + owner_pattern + wait_pattern.removeprefix("(?is)^")
                routes.append(
                    passive(
                        "mentioned_wait_" + kind,
                        event,
                        mentioned_wait,
                        (
                            FieldExtraction(
                                "message_document",
                                mentioned_wait,
                                (
                                    owner,
                                    CaptureSpec(
                                        "wait",
                                        "duration",
                                        minimum=1,
                                        maximum=COOLDOWNS[kind] // 1000,
                                    ),
                                ),
                            ),
                        ),
                    )
                )
        return Manifest(
            schema=2,
            module=MODULE,
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Glitch Tracker",
            summary="Glitch drop and grab reminders.",
            presentation_mode="v2",
            version="1.0.2",
            commands=(
                CommandSpec(
                    root="glitch",
                    handler=f"{MODULE}.open",
                    grammar=r"^(?:\s+(?P<help>help))?$",
                    captures=(CaptureSpec("help", required=False, maximum=4),),
                    help=HelpSpec(".glitch", "Set drop and grab reminders."),
                    acl_key="glitch",
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
            passive=tuple(routes),
            components=tuple(
                ComponentSpec(
                    "glitch",
                    action,
                    f"{MODULE}.action",
                    "^" + token + "$",
                    captures=(CaptureSpec("session"),),
                    acl_key="glitch",
                    public=True,
                    input_kind="string_values"
                    if action == "delivery"
                    else ("channel_ids" if action == "channel" else "none"),
                    channel_types=(0, 5) if action == "channel" else (),
                    session_bound=True,
                )
                for action in ("drop", "grab", "delivery", "channel")
            ),
            scheduled=(
                ScheduledCallbackSpec(
                    f"{MODULE}.reminder",
                    f"{MODULE}.reminder",
                    deadline_ms=DELIVERY_WINDOW_MS,
                    presentation_mode="plain",
                ),
                flush_spec(MODULE),
                ScheduledCallbackSpec(
                    f"{MODULE}.source_window", f"{MODULE}.source_window",
                    deadline_ms=WINDOW_MS * 3, presentation_mode="plain", scope="guild",
                ),
            ),
            capabilities=("send_dm", "read_mention_batch_members", "send_mention_batch", "resolve_members"),
        )

    async def start(self):
        self._publisher_task = asyncio.create_task(self._publisher.run(), name="glitch-reminders")

    async def close(self):
        if self._publisher_task:
            self._publisher_task.cancel()
            await asyncio.gather(self._publisher_task, return_exceptions=True)

    async def handle(self, request: TypedRequest, job_id: str, generation: int):
        del generation
        key = (int(request.event.get("guild_id") or 0), int(request.event.get("actor_id") or 0))
        if request.handler in {f"{MODULE}.drop_source", f"{MODULE}.extra_unbound", f"{MODULE}.source_window"}:

            try:
                await self._source_event(request, job_id)
            except Exception:
                LOGGER.exception("glitch_extra_drop_source_unavailable")
            return self._result(job_id, None)
        lock = self._locks.setdefault(key, asyncio.Lock())
        try:
            async with lock:
                if request.handler == f"{MODULE}.open":
                    intent = await self._open(request)
                elif request.handler == f"{MODULE}.action":
                    intent = await self._action(request)
                elif request.handler == f"{MODULE}.reminder":
                    await self._reminder(request, job_id)
                    intent = None
                elif request.handler == f"{MODULE}.cooldown_flush":
                    await self._cooldowns.flush(MODULE, request, job_id, self._actions)
                    intent = None
                else:
                    await self._event(request)
                    intent = None
            return self._result(job_id, intent)
        except Exception:
            LOGGER.exception("glitch_tracker_request_failed")
            return self._result(job_id, self._simple("Unavailable."), error="handler_failed")

    async def _open(self, request):
        if request.arguments.get("help"):
            return self._simple(f"{request.event.get('prefix') or '.'}glitch")
        session = GlitchSession(
            secrets.token_hex(8), int(request.event["actor_id"]), int(request.event["guild_id"])
        )
        await self._cache.setex(SESSION_PREFIX + session.token, SESSION_TTL, encode(session))
        return await self._dashboard(session, "send")

    async def _action(self, request):
        token = str(request.arguments.get("session") or "")
        raw = await self._cache.get(SESSION_PREFIX + token)
        if raw is None:
            return self._simple("Unavailable.", kind="edit")
        session = decode(raw, GlitchSession)
        if session.owner_id != int(request.event.get("actor_id") or 0) or session.guild_id != int(
            request.event.get("guild_id") or 0
        ):
            return self._simple("Unavailable.", kind="edit")
        state = await self._store.subscription(session.guild_id, session.owner_id)
        action = request.route.partition(".")[2]
        cancel = None
        if action in COOLDOWNS:
            field = action + "_enabled"
            enabled = not getattr(state, field)
            state = replace(state, **{field: enabled})
            cancel = None if enabled else action
        elif action == "delivery":
            values = request.arguments.get("values", ())
            if len(values) != 1 or values[0] not in {"dm", "channel"}:
                return self._simple("Unavailable.", kind="edit")
            state = replace(state, delivery_mode=values[0])
        elif action == "channel":
            values = request.arguments.get("values", ())
            if len(values) != 1 or int(values[0]) <= 0:
                return self._simple("Unavailable.", kind="edit")
            state = replace(state, delivery_mode="channel", channel_id=int(values[0]))
        else:
            return self._simple("Unavailable.", kind="edit")
        if not state.channel_id:
            state = replace(state, channel_id=int(request.event.get("channel_id") or 0) or None)
        await self._store.save_subscription(state, modified_by=session.owner_id, cancel_kind=cancel)
        await self._publisher.flush_safely()
        await self._cache.setex(SESSION_PREFIX + token, SESSION_TTL, raw)
        return await self._dashboard(session, "edit")

    async def _dashboard(self, session, kind):
        state = await self._store.subscription(session.guild_id, session.owner_id)
        timers = {
            timer.kind: timer
            for timer in await self._store.timers(
                session.guild_id,
                session.owner_id,
            )
        }
        lines = []
        for name in COOLDOWNS:
            line = f"{name.title()}: {'On' if getattr(state, name + '_enabled') else 'Off'}"
            timer = timers.get(name)
            if getattr(state, name + "_enabled") and timer and timer.active and timer.due_at_ms:
                line += f" - <t:{timer.due_at_ms // 1000}:R>"
            lines.append(line)
        lines.extend(
            (
                f"Delivery: {'DM' if state.delivery_mode == 'dm' else 'Channel'}",
                f"Channel: <#{state.channel_id}>" if state.channel_id else "Channel: None",
            )
        )
        controls = (
            PresentationControl("button", "glitch", "drop", session.token, label="Drop"),
            PresentationControl("button", "glitch", "grab", session.token, label="Grab"),
            PresentationControl(
                "string_select",
                "glitch",
                "delivery",
                session.token,
                placeholder="Delivery",
                options=tuple(
                    PresentationOption(label, value, default=state.delivery_mode == value)
                    for label, value in (("DM", "dm"), ("Channel", "channel"))
                ),
            ),
            PresentationControl(
                "channel_select",
                "glitch",
                "channel",
                session.token,
                placeholder="Channel",
                channel_types=(0, 5),
                default_ids=(state.channel_id,) if state.channel_id else (),
            ),
        )
        return DiscordIntent(
            kind,
            pages=(
                PresentationPage(
                    title="Glitch",
                    description="\n".join(lines),
                    controls=controls,
                ),
            ),
            session_id=session.token,
        )

    async def _source_event(self, request, job_id):
        event = request.event
        guild = int(event.get("guild_id") or 0)
        channel = int(event.get("channel_id") or 0)
        if min(guild, channel) <= 0:
            return
        if request.handler == f"{MODULE}.source_window":
            if event.get("dispatch") != "scheduled" or event.get("actor_id") != 0:
                return
            notice = int(request.arguments.get("notice") or 0)
            at = int(request.arguments.get("at") or 0)
            order = int(request.arguments.get("order") or 0)
            now = self.clock()
            if min(notice, at) <= 0 or not at + WINDOW_MS <= now < at + WINDOW_MS * 3:
                return
            match = await self._drop_sources.match(guild, channel, notice, at)
            if match is None:
                return
            source, owner = match
            result = await self._actions.request(
                job_id=job_id, kind="resolve_members", guild_id=guild, channel_id=channel,
                member_ids=(owner,), idempotency_key=f"glitch-extra-subject:{job_id}:{owner}",
            )
            if not result.ok or not any(
                row.get("id") == owner and row.get("allowed") is True
                and row.get("blocked") is False and row.get("bot") is False
                for row in result.data.get("members", ())
            ):
                return
            async with self._locks.setdefault((guild, owner), asyncio.Lock()):
                await self._store.record(
                    guild, owner, "drop", Observation("extra", notice, at, order, channel, related_message_id=source),
                )
                await self._publisher.flush_safely()
            return
        if event.get("dispatch") != "passive" or event.get("author_id") != GLITCH_BOT_ID:
            return
        message = int(event.get("message_id") or 0)
        at = int(event.get("created_at_ms") or 0)
        if request.handler.endswith(".drop_source"):
            await self._drop_sources.observe(guild, channel, message, int(request.arguments.get("owner") or 0), at)
            return
        now = self.clock()
        if min(message, at) <= 0 or abs(now - at) >= WINDOW_MS * 2:
            return
        order = int(event.get("event_order") or 0)
        await self._scheduler.publish(
            operation="upsert", schedule_key=f"{MODULE}:source:{message}",
            route=f"{MODULE}.source_window", revision=max(1, order),
            guild_id=guild, channel_id=channel, actor_id=0,
            due_at_ms=max(now, at) + WINDOW_MS, expires_at_ms=at + WINDOW_MS * 3,
            arguments={"notice": message, "at": at, "order": order},
        )

    async def _event(self, request):
        if request.event.get("author_id") != GLITCH_BOT_ID:
            return
        action = request.handler.removeprefix(MODULE + ".")
        kind = action.rpartition("_")[2]
        if kind not in COOLDOWNS:
            return
        guild, user = (
            int(request.event.get("guild_id") or 0),
            int(request.event.get("actor_id") or 0),
        )
        message, channel = (
            int(request.event.get(key) or 0) for key in ("message_id", "channel_id")
        )
        at = int(request.event.get("created_at_ms") or 0)
        if not all((guild, user, message, channel, at)):
            return
        command = int(request.event.get("reference_message_id") or 0) or None
        due, rounding = None, 0
        if action.startswith("success_"):
            disposition = "extra" if request.arguments.get("extra") else "success"
        elif action.startswith("extra_"):
            disposition = "extra"
        else:


            at = max(at, int(request.event.get("edited_at_ms") or 0))
            if request.arguments.get("due"):
                due = int(request.arguments["due"]) * 1000
                disposition = "reported"
            elif request.arguments.get("ready"):
                disposition = "ready"
            elif request.arguments.get("wait"):
                due = at + int(request.arguments["wait"]) * 1000
                disposition, rounding = "reported", 1000
            else:
                return
            if (
                due is not None
                and not at - DELIVERY_WINDOW_MS <= due <= at + COOLDOWNS[kind] + 1000
            ):
                return
        await self._store.record(
            guild,
            user,
            kind,
            Observation(
                disposition,
                message,
                at,
                int(request.event.get("event_order") or 0),
                channel,
                command_id=command,
                due_at_ms=due,
                rounding_ms=rounding,
            ),
        )
        await self._publisher.flush_safely()

    async def _reminder(self, request, job):
        guild = int(request.event.get("guild_id") or 0)
        user = int(request.event.get("actor_id") or 0)
        kind = str(request.arguments.get("kind") or "")
        revision = int(request.arguments.get("revision") or 0)
        await deliver_reminder(
            tracker="glitch_tracker",
            store=self._store,
            actions=self._actions,
            coalescer=getattr(self, "_cooldowns", None),
            guild=guild,
            user=user,
            kind=kind,
            revision=revision,
            job=job,
            text=f"Glitch {kind} ready.",
        )

    @staticmethod
    def _simple(text, *, kind="send"):
        return DiscordIntent(kind, pages=(PresentationPage(description=text),))

    def _result(self, job, intent, *, error=None):
        return ResultEnvelope(
            schema=1,
            job_id=job,
            module=MODULE,
            group="cards",
            generation=self._generation,
            status="error" if error else "ok",
            intents=(intent,) if intent else (),
            error_code=error,
            completed_at_ms=self.clock(),
        )
