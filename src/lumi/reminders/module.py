from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.ai import AIClient, AIUnavailable, profile_hash
from lumi.contracts import (
    BoundReplySpec,
    ChannelTransitionSpec,
    AICallRequest,
    Announcement,
    CaptureSpec,
    CommandSpec,
    DiscordActionResult,
    DiscordIntent,
    GuildPolicySnapshot,
    HelpSpec,
    Manifest,
    MatcherExpr,
    PassiveMatcherSpec,
    ResultEnvelope,
    ScheduledCallbackSpec,
)
from lumi.transport import outbound
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from .commands import ReminderError, ReminderSyntaxError, command_head, parse, pretty_duration
from .natural import REMINDER_PROFILE, TimezoneRequired, interpret, model_input, saved_zone
from .reactions import PROMPT_WINDOW
from .store import WINDOW

LOGGER = logging.getLogger(__name__)
MODULE, GROUP, ACL = "reminders", "community", "remind"
BEFORE_SEND = frozenset(
    {
        "policy_denied",
        "stale_generation",
        "unknown_job",
        "expired_job",
        "invalid_action",
        "capability_denied",
        "action_scope_mismatch",
        "guild_unavailable",
        "blocked",
    }
)
PERMANENT = frozenset(
    {
        "permission_denied",
        "channel_unavailable",
        "member_unavailable",
        "discord_rejected",
        "dm_forbidden",
        "blocked",
    }
)


def send_key(occurrence, attempt):
    return f"remind:{occurrence}:{attempt}"


class RemindersModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        store,
        actions=None,
        scheduler=None,
        clock=None,
        ai_client=None,
        region_provider=None,
        shortcut_ui=None,
    ):
        self.generation, self.queue, self.store = generation, queue_redis, store
        self.actions = actions or EdgeActionClient(
            queue_redis, module=MODULE, group=GROUP, generation=generation
        )
        self.scheduler = scheduler or ScheduledCallbackClient(
            queue_redis, module=MODULE, group=GROUP, generation=generation
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.ai = ai_client or AIClient()
        self.region_provider = region_provider
        self.shortcut_ui = shortcut_ui
        self.task = None
        self.stopped = False
        self.draining = False
        self.ticking = False
        self.wake = asyncio.Event()
        self.last_cleanup = 0

    @property
    def manifest(self):
        from lumi.notes.module import components

        return Manifest(
            schema=2,
            module=MODULE,
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group=GROUP,
            generation=self.generation,
            job_stream="lumi:jobs:community",
            display_name="Reminders",
            summary="Personal one-off and repeating channel reminders.",
            presentation_mode="plain",
            version="1.3.1",
            commands=(
                CommandSpec(
                    root="remindme",
                    handler="reminders.command",
                    grammar=r"(?s)^\s*(?P<raw>.{0,1000}.{0,200})$",
                    captures=(CaptureSpec("raw", required=False, maximum=1200),),
                    acl_key=ACL,
                    public=True,
                    deadline_ms=75_000,
                    context_fields=("used_prefix", "prefix"),
                    presentation_modes=("plain", "v2"),
                    help=HelpSpec(
                        ".remindme <duration> <text>\n.remindme every <duration> <text>\n"
                        ".remindme list [page]\n.remindme edit <id> [every] <duration> <text>\n"
                        ".remindme cancel <id>\n.remindme settings",
                        "Set, repeat, edit or cancel your channel reminders.",
                    ),
                ),
            ),
            passive=(
                PassiveMatcherSpec(
                    "reminders.reaction",
                    "reminders.reaction",
                    "reaction_add",
                    MatcherExpr("exists", field="reaction_observation"),
                    subject_field="actor_id",
                    subject_required=True,
                    personal_reaction=True,
                    acl_key=ACL,
                    context_fields=("reaction_observation",),
                ),
                PassiveMatcherSpec(
                    "reminders.reply",
                    "reminders.reply",
                    "message_create",
                    MatcherExpr("exists", field="bound_reply"),
                    subject_field="actor_id",
                    subject_required=True,
                    bound_human_reply=True,
                    bound_reply_activity_exempt=True,
                    acl_key=ACL,
                    context_fields=("bound_reply", "content"),
                ),
            ),
            components=components("reminders"),
            scheduled=(
                ScheduledCallbackSpec(
                    route="reminders.deliver",
                    handler="reminders.deliver",
                    acl_key=ACL,
                    deadline_ms=WINDOW,
                    presentation_mode="plain",
                ),
            ),
            capabilities=("resolve_members", "send_message", "send_dm", "bind_message_reply"),
            action_presentation_modes=("plain",),
        )

    @property
    def ai_profiles(self):
        return (REMINDER_PROFILE,)

    async def start(self):
        self.task = asyncio.create_task(self._run(), name="personal-reminders")

    async def close(self):
        self.stopped = True
        self.wake.set()
        if self.task:
            await self.task
        await self.ai.close()
        await self.store.close()

    def begin_drain(self):
        self.draining = True
        self.wake.set()

    def resume(self):
        self.draining = False
        self.wake.set()

    def drain_ready(self):
        return not self.ticking

    async def _run(self):
        while not self.stopped:
            self.wake.clear()
            if not self.draining:
                self.ticking = True
                try:
                    await self.tick()
                except (RedisError, MySQLError, TimeoutError, ValueError, msgspec.DecodeError):

                    LOGGER.warning("reminders_reconcile_unavailable")
                finally:
                    self.ticking = False
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=5)
            except TimeoutError:
                pass

    async def enabled_guilds(self):
        raw_manifest = await self.queue.hget(outbound("lumi:manifests:current", GROUP), MODULE)
        if not raw_manifest:
            return None
        announcement = msgspec.json.decode(raw_manifest, type=Announcement)
        if (announcement.manifest.group, announcement.manifest.generation) != (
            GROUP,
            self.generation,
        ):
            return None
        enabled = set()
        for raw in await self.queue.hvals("lumi:policy:current"):
            policy = msgspec.json.decode(raw, type=GuildPolicySnapshot)
            if (
                policy.schema == 2
                and MODULE in policy.enabled_modules
                and f"{MODULE}:{ACL}" in policy.enabled_commands
            ):
                enabled.add(policy.guild_id)
        return enabled

    async def tick(self):
        now = self.clock()
        guilds = await self.enabled_guilds()
        if guilds is None:
            return
        gate = getattr(self, "handover_gate", None)
        if gate:
            guilds = {guild for guild in guilds if not gate.critical(guild)}

        for occurrence in await self.store.orphans(now):
            if gate and gate.critical(occurrence["guild_id"]):
                continue
            receipt_key = send_key(occurrence["id"], occurrence["send_attempt"])
            raw = await self.queue.get(f"lumi:edge-action-receipt:{MODULE}:{receipt_key}")
            result = msgspec.json.decode(raw, type=DiscordActionResult) if raw else None
            if result and not result.ok and result.error_code in BEFORE_SEND - PERMANENT:
                await self.store.retry(
                    occurrence["id"], occurrence["dispatch_revision"], occurrence["guild_id"], now
                )
            else:
                outcome = (
                    "sent"
                    if result and result.ok
                    else "failed"
                    if result and result.error_code in PERMANENT
                    else "ambiguous"
                )
                await self.store.recover_orphan(
                    occurrence["id"],
                    occurrence["guild_id"],
                    now,
                    outcome=outcome,
                    error=None
                    if outcome == "sent"
                    else "destination_unavailable"
                    if outcome == "failed"
                    else "delivery_uncertain",
                    message_id=result.data.get("message_id") if result and result.ok else None,
                )
        for identifier, guild in await self.store.candidates(now, guilds):
            if self.draining:
                return
            await self.store.reconcile(identifier, guild, now)
        await self.publish_pending(guilds)
        if now - self.last_cleanup >= 60_000:
            await self.store.cleanup(now)
            self.last_cleanup = now

    async def publish_pending(self, guilds):
        now = self.clock()
        for entry in await self.store.pending(guilds):
            if self.draining:
                return
            if entry["expires_ms"] <= now or not await self.store.current_outbox(entry):
                await self.store.published(entry["id"])
                continue
            await self.scheduler.publish(
                operation=entry["operation"],
                schedule_key=f"reminders:{entry['occurrence_id']}",
                route="reminders.deliver",
                revision=entry["revision"],
                guild_id=entry["guild_id"],
                channel_id=entry["channel_id"],
                actor_id=entry["user_id"],
                due_at_ms=entry["due_ms"],
                expires_at_ms=entry["expires_ms"],
                arguments={
                    "occurrence": entry["occurrence_id"],
                    "revision": entry["revision"],
                    **(
                        {"source_author_id": entry["source_author_id"]}
                        if entry.get("source_author_id")
                        else {}
                    ),
                },
            )
            await self.store.published(entry["id"])

    def result(self, job_id, text=None):
        return ResultEnvelope(
            1,
            job_id,
            MODULE,
            GROUP,
            self.generation,
            "ok",
            intents=(DiscordIntent(kind="send", content=text, presentation_mode="plain"),)
            if text
            else (),
        )

    async def handle(self, request, job_id, generation):
        if (
            generation != self.generation
            or request.event.get("_expires_at_ms", self.clock() + 1) <= self.clock()
        ):
            return self.result(job_id)
        try:
            if request.handler == "reminders.ui" or (
                request.handler == "reminders.command"
                and str(request.arguments.get("raw") or "").strip().casefold() == "settings"
            ):
                if self.shortcut_ui is None:
                    return self.result(job_id, "Unavailable.")
                return await self.shortcut_ui.handle_as(request, job_id, generation, "reminders")
            if request.handler in {"reminders.reaction", "reminders.reply"}:
                await self.reaction(request, job_id)
                return self.result(job_id)
            if request.handler == "reminders.deliver":
                await self.deliver(request, job_id)
                return self.result(job_id)
            return self.result(job_id, await self.command(request))
        except ReminderError as error:
            return self.result(job_id, str(error))
        except (RedisError, MySQLError, ActionUnavailable, TimeoutError):
            LOGGER.warning("reminders_request_unavailable")
            return self.result(
                job_id,
                None if request.handler == "reminders.deliver" else "Unavailable. Try again.",
            )

    async def command(self, request):
        event = request.event
        guild, user, channel, message = (
            int(event.get(key) or 0) for key in ("guild_id", "actor_id", "channel_id", "message_id")
        )
        if min(guild, user, channel, message) <= 0:
            return "Use this command in a server channel."
        prefix = event.get("prefix") or (
            event.get("used_prefix") if isinstance(event.get("used_prefix"), str) else "."
        )
        raw = request.arguments.get("raw") or ""
        try:
            command = parse(raw)
        except ReminderSyntaxError:

            receipt = await self.store.command_receipt(guild, message)
            if receipt:
                return self.confirmation(receipt)
            command = await self.natural_command(raw, request)
        if command.kind == "help":
            return "\n".join(
                (
                    f"{prefix}remindme 2h check cards",
                    f"{prefix}remindme every 1d check cards",
                    f"{prefix}remindme list [page]",
                    f"{prefix}remindme edit <id> [every] <duration> <text>",
                    f"{prefix}remindme cancel <id>",
                    f"{prefix}remindme settings",
                    "Times: 2hs, 2 hours, 1h30m, or natural language.",
                    f"Calendar times use your saved {prefix}region timezone.",
                    "Minimum wait: 1 minute. Minimum repeat: 15 minutes. Maximum: 365 days.",
                )
            )
        if command.kind == "list":
            rows, total = await self.store.list_page(guild, user, channel, command.page)
            if not total:
                return "No reminders in this channel."
            pages = (total + 4) // 5
            if command.page > pages:
                return f"Choose a page from 1 to {pages}."
            lines = []
            for row in rows:
                timing = (
                    f"<t:{row['next_ms'] // 1000}:f>"
                    if row["status"] == "active"
                    else row["status"].capitalize()
                )
                repeat = (
                    f" · every {pretty_duration(row['interval_ms'])}" if row["interval_ms"] else ""
                )
                snippet = re.sub(r"([\\`*_~|>])", r"\\\1", " ".join(row["text"].split())[:100])
                lines.append(f"{row['id']} · {timing}{repeat}\n{snippet}")
            lines.append(f"Page {command.page}/{pages}")
            return "\n\n".join(lines)
        receipt = await self.store.command(
            command, guild=guild, user=user, channel=channel, message=message, now=self.clock()
        )
        self.wake.set()
        return self.confirmation(receipt)

    @staticmethod
    def confirmation(receipt):
        if receipt.get("error"):
            return receipt["error"]
        if receipt["kind"] == "cancel":
            return f"Cancelled {receipt['id']}."
        repeat = (
            f" · every {pretty_duration(receipt['interval_ms'])}" if receipt["interval_ms"] else ""
        )
        return f"Reminder {receipt['id']} · <t:{receipt['due_ms'] // 1000}:f>{repeat}"

    async def natural_command(self, raw, request):
        kind, identifier, body = command_head(raw, preserve_body=True)

        if kind not in {"create", "edit"}:
            raise ReminderError("Use remindme help.")
        now = self.clock()
        expires = min(request.event.get("_expires_at_ms", now + 65_000), now + 65_000)
        if self.draining or expires <= now:
            raise ReminderError("Unavailable. Try again.")
        region = (
            await self.region_provider(int(request.event["actor_id"]))
            if self.region_provider
            else None
        )
        zone = saved_zone(region)
        call = AICallRequest(
            schema=1,
            call_id=uuid.uuid4().hex,
            profile_key=REMINDER_PROFILE.key,
            profile_hash=profile_hash(REMINDER_PROFILE),
            caller_module=MODULE,
            caller_group=GROUP,
            caller_generation=self.generation,
            actor_id=int(request.event["actor_id"]),
            expires_at_ms=expires,
            input=model_input(body, now, zone),
        )
        try:
            response = await self.ai.call(call)
        except AIUnavailable as exc:
            raise ReminderError("Couldn't read that time. Try 2hs or 1h30m.") from exc
        if response.status != "ok":
            raise ReminderError(
                "Busy. Try again."
                if response.status == "busy"
                else "Couldn't read that time. Try 2hs or 1h30m."
            )

        if self.draining or self.clock() >= expires:
            raise ReminderError("Unavailable. Try again.")
        try:
            command = interpret(
                response.value, kind=kind, identifier=identifier, raw=body, now=now, zone=zone
            )
        except TimezoneRequired as exc:
            prefix = request.event.get("used_prefix") or "."
            raise ReminderError(
                f"Which timezone? Set {prefix}region <timezone>, then try again."
            ) from exc
        if command.due_ms <= self.clock():
            raise ReminderError("That time has passed. Choose a later time.")
        return command

    async def deliver(self, request, job_id):
        event, args = request.event, request.arguments
        identifier, revision = args.get("occurrence"), args.get("revision")
        guild, user, channel = (
            int(event.get(k) or 0) for k in ("guild_id", "actor_id", "channel_id")
        )
        if (
            event.get("dispatch") != "scheduled"
            or not isinstance(identifier, str)
            or not re.fullmatch(r"[a-f0-9]{32}", identifier)
            or type(revision) is not int
            or revision <= 0
            or min(guild, user, channel) <= 0
            or guild not in (await self.enabled_guilds() or set())
        ):
            return
        record = await self.store.claim(
            identifier, revision, guild=guild, user=user, channel=channel, now=self.clock()
        )
        if record is None:
            return
        try:
            members = await self.actions.request(
                job_id=job_id,
                kind="resolve_members",
                guild_id=guild,
                channel_id=channel,
                member_ids=(user,),
                idempotency_key=f"remind-check:{identifier}:{revision}",
            )
        except (ActionUnavailable, RedisError, TimeoutError):
            await self.store.retry(identifier, revision, guild, self.clock())
            return
        if not members.ok:
            if members.error_code in PERMANENT:
                await self.store.finish(
                    identifier,
                    revision,
                    guild,
                    self.clock(),
                    outcome="failed",
                    error="destination_unavailable",
                )
            else:
                await self.store.retry(identifier, revision, guild, self.clock())
            return
        person = next(
            (row for row in members.data.get("members", ()) if row.get("id") == user), None
        )
        if not person or person.get("bot") or person.get("blocked") or not person.get("allowed"):
            await self.store.finish(
                identifier,
                revision,
                guild,
                self.clock(),
                outcome="failed",
                error="recipient_unavailable",
            )
            return
        attempt = await self.store.begin_send(identifier, revision, guild, self.clock())
        if not attempt:
            return
        destination = "channel"
        if record.get("reaction"):
            destination = await self.store.reaction_delivery(guild, user)
        try:
            sent = await self.actions.request(
                job_id=job_id,
                kind="send_dm" if destination == "dm" else "send_message",
                guild_id=guild,
                channel_id=channel,
                member_id=user,
                idempotency_key=send_key(identifier, attempt),
                intent=DiscordIntent(
                    kind="send",
                    content=record["text"],
                    presentation_mode="plain",
                    mention_actor=destination == "channel",
                ),
            )
        except (ActionUnavailable, RedisError, TimeoutError):

            return
        if not sent.ok and sent.error_code in BEFORE_SEND - PERMANENT:
            await self.store.retry(identifier, revision, guild, self.clock())
            return
        outcome = "sent" if sent.ok else "failed" if sent.error_code in PERMANENT else "ambiguous"
        await self.store.finish(
            identifier,
            revision,
            guild,
            self.clock(),
            outcome=outcome,
            error=None
            if sent.ok
            else "destination_unavailable"
            if outcome == "failed"
            else "delivery_uncertain",
            message_id=sent.data.get("message_id") if sent.ok else None,
        )

    async def reaction(self, request, job_id):
        from lumi.notes.models import emoji_key
        from lumi.notes.module import capture_from_request
        from lumi.notes.publisher import shortcut_tag

        from .commands import duration

        if self.shortcut_ui is None or request.event.get("dispatch") != "passive":
            return
        event = request.event
        guild, user, channel = (int(event[k]) for k in ("guild_id", "actor_id", "channel_id"))
        if request.handler == "reminders.reaction":
            try:
                capture, tag = capture_from_request(request)
            except (ValueError, TypeError, msgspec.ValidationError):
                return
            prefs = await self.shortcut_ui.store.shortcuts(user)
            mapping = next((
                mapping for mapping in prefs.reminders
                if emoji_key(mapping.emoji) == emoji_key(capture.emoji)
                and tag == shortcut_tag(mapping.emoji, mapping.duration_ms)
            ), None)
            if mapping is None:
                return
            capture = msgspec.structs.replace(capture, duration_ms=mapping.duration_ms)
            receipt = await self.store.react(capture, user=user, now=self.clock())
            self.wake.set()
            if receipt.get("kind") == "prompt" and not receipt.get("message_id"):
                token = receipt["token"]
                sent = await self.actions.request(
                    job_id=job_id,
                    kind="bind_message_reply",
                    guild_id=guild,
                    channel_id=channel,
                    idempotency_key=f"quick-prompt:{token}",
                    content=f"<@{user}> When?",
                    bound_reply=BoundReplySpec(
                        "open", token, "reminders.reply", ttl_ms=PROMPT_WINDOW, ping_actor=True,
                        source_author_id=capture.message.author_id,
                    ),
                )
                if sent.ok:
                    await self.store.bind_reaction_prompt(
                        guild,
                        user,
                        token,
                        int(sent.data["message_id"]),
                        expires_at_ms=sent.data.get("expires_at_ms"),
                    )
            return
        binding = event.get("bound_reply") or {}
        if binding.get("owner_id") != user:
            return
        token = binding.get("token", "")
        text = str(event.get("content") or "").strip()
        if text.casefold().startswith("in "):
            text = text[3:]
        try:
            wait = duration(text, False)
        except ReminderError:
            await self.actions.request(
                job_id=job_id,
                kind="bind_message_reply",
                guild_id=guild,
                channel_id=channel,
                idempotency_key=f"quick-invalid:{event['message_id']}",
                content="Use 1h 30m.",
                bound_reply=BoundReplySpec("edit", token),
            )
            return
        receipt = await self.store.reaction_reply(
            guild=guild,
            user=user,
            channel=channel,
            token=token,
            prompt_id=int(binding.get("prompt_id") or 0),
            delay=wait,
            received_ms=int(event.get("created_at_ms") or self.clock()),
            now=self.clock(),
        )
        self.wake.set()
        if receipt and receipt.get("due_ms"):
            await self.actions.request(
                job_id=job_id,
                kind="bind_message_reply",
                guild_id=guild,
                channel_id=channel,
                idempotency_key=f"quick-answer:{token}",
                content=f"<t:{receipt['due_ms'] // 1000}:R>",
                bound_reply=BoundReplySpec("close", token),
            )
