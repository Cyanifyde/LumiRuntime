from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import json
import logging
import re
import secrets
import time

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts import (
    ChannelPublicationSpec,
    ChannelTransitionSpec,
    ChannelSelectionSpec,
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    GatewayCaptureSpec,
    GuardedRepliesSpec,
    GuildPolicySnapshot,
    HelpSpec,
    Manifest,
    MessageMutationSpec,
    PresentationBlock,
    PrivateChannelFactsSpec,
    ResultEnvelope,
    ScheduledCallbackSpec,
)
from lumi.contracts.models import ScheduledDeliverySpec
from lumi.contracts.native_mutations import NATIVE_MUTATION_KINDS
from lumi.shared.discord_effects import MUTATE_MESSAGE, PUBLISH_MESSAGE
from lumi.contracts import (
    PresentationControl as Control,
)
from lumi.contracts import (
    PresentationOption as Option,
)
from lumi.contracts import (
    PresentationPage as Page,
)
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from .assets import Assets
from .capture import publish_capture_admission, rehydrate_capture_admission
from .evidence import Evidence
from .models import CATEGORIES, EVENTS, SESSION_TTL, TITLES, ModlogSpec, escape
from lumi.contracts.gateway import GUILD_CAPTURE_EVENTS as GATEWAY
from .presentation import render, summary
from .publication import Publisher
from .recovery import RECOVER, STATES, identity as recovery_identity, recover
from .replies import Replies, ReplySnapshot
from .settings import SETTINGS, configure
from .store import HistoryAccess

PREFIX = "lumi:modlog:session:"
LOGGER = logging.getLogger(__name__)
CHANNEL_FACTS = PrivateChannelFactsSpec(
    bot_permissions=(
        "view_channel", "send_messages", "embed_links", "attach_files", "read_message_history",
    ),
    bot_guild_permissions=("view_audit_log",),
)


class ModlogModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        cache_redis,
        store,
        artifact_root,
        journal=None,
        capture=None,
        actions=None,
        clock=None,
        block_owner_id=0,
        scoped_blocks=True,
        block_reader=None,
        temp_root=None,
    ):
        self.generation, self.queue, self.cache, self.store = (
            generation,
            queue_redis,
            cache_redis,
            store,
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.journal = journal
        self.capture = capture
        self.block_owner_id, self.scoped_blocks = block_owner_id, scoped_blocks
        self.actions = actions or EdgeActionClient(
            queue_redis, module="modlog", group="security", generation=generation
        )
        self.scheduler = ScheduledCallbackClient(
            queue_redis, module="modlog", group="security", generation=generation
        )
        self.assets = Assets(artifact_root, store, clock=self.clock)
        self.evidence = Evidence(
            store, self.assets, temp_root, block_reader,
            owner_id=block_owner_id, clock=lambda: self.clock(),
        )
        self.publisher = Publisher(store, queue_redis, self.actions, self.evidence, lambda: self.clock())
        self.replies = Replies(store, cache_redis, lambda: self.clock(), generation)
        self.task = None
        self.draining = False
        self.active = 0
        self.published = {}
        self.admission_refresh_due = 0

    @property
    def manifest(self):
        grammar = r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$"
        components = []
        for action in (
            "open",
            "nav",
            "section",
            "pick_category",
            "destination",
            "events",
            "excluded",
            "bots",
            "filter",
            "user",
            "actor",
            "channel",
            "window",
            "event",
            "previous",
            "next",
            "pick",
            "detail_previous",
            "detail_next",
            "files",
            "refresh",
        ):
            input_kind = (
                "channel_ids"
                if action in {"destination", "excluded", "channel"}
                else "user_ids"
                if action in {"user", "actor"}
                else "string_values"
                if action
                in {
                    "nav",
                    "section",
                    "pick_category",
                    "events",
                    "filter",
                    "window",
                    "event",
                    "pick",
                }
                else "none"
            )
            components.append(
                ComponentSpec(
                    "modlog",
                    action,
                    "modlog.panel",
                    grammar,
                    captures=(
                        CaptureSpec("session"),
                        CaptureSpec("revision", "integer", minimum=0),
                    ),
                    acl_key="modlog",
                    public=True,
                    required_permissions=("administrator",) if action in SETTINGS else ("view_audit_log",),
                    private_channel_facts=CHANNEL_FACTS,
                    ephemeral=False,
                    session_bound=True,
                    input_kind=input_kind,
                    channel_types=(0,) if action == "destination" else (),
                    channel_selection=(
                        ChannelSelectionSpec(1, (
                            "view_channel", "read_message_history", "send_messages",
                            "embed_links", "attach_files",
                        ), require_private=True) if action == "destination"
                        else ChannelSelectionSpec(25) if action == "excluded" else None
                    ),
                    deadline_ms=30000,
                )
            )
        components.append(
            ComponentSpec(
                "modlog",
                "record",
                "modlog.record",
                r"^(?P<record>[a-f0-9]{32})$",
                captures=(CaptureSpec("record"),),
                acl_key="modlog",
                public=True,
                required_permissions=("view_audit_log",),
                private_channel_facts=CHANNEL_FACTS,
                source_message_facts=True,
                ephemeral=False,
                deadline_ms=30000,
            )
        )
        return Manifest(
            schema=2,
            module="modlog",
            guarded_replies=GuardedRepliesSpec(CHANNEL_FACTS.bot_permissions),
            gateway_capture=GatewayCaptureSpec(tuple(sorted(GATEWAY)), (
                "CHANNEL_UPDATE", "GUILD_EMOJIS_UPDATE", "GUILD_MEMBER_UPDATE",
                "GUILD_ROLE_UPDATE", "GUILD_SCHEDULED_EVENT_UPDATE", "GUILD_STICKERS_UPDATE",
                "GUILD_UPDATE",
            ), mutation_kinds=tuple(sorted(NATIVE_MUTATION_KINDS))),
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group="security",
            generation=self.generation,
            job_stream="lumi:jobs:security",
            display_name="Modlog",
            summary="Server activity logs and staff history.",
            presentation_mode="v2",
            version="1.0.8",
            commands=(
                CommandSpec(
                    root="modlog",
                    handler="modlog.command",
                    grammar=r"^(?:\s+(?:(?P<operation>setup|help)|<@!?(?P<mention>[0-9]{17,20})>|(?P<user_id>[0-9]{17,20})))?$",
                    captures=(
                        CaptureSpec("operation", required=False),
                        CaptureSpec("mention", "snowflake", required=False),
                        CaptureSpec("user_id", "snowflake", required=False),
                    ),
                    help=HelpSpec(
                        ".modlog [@user|ID]\n.modlog setup",
                        "Browse moderation logs or configure what gets logged.",
                    ),
                    acl_key="modlog",
                    public=True,
                    required_permissions=("view_audit_log",),
                    private_channel_facts=CHANNEL_FACTS,
                    deadline_ms=30000,
                ),
            ),
            components=tuple(components),
            capabilities=("read_interaction_context", "read_owned_history", PUBLISH_MESSAGE, MUTATE_MESSAGE),
            action_presentation_modes=("v2", "legacy"),
            scheduled=(
                ScheduledCallbackSpec(
                    "modlog.deliver", "modlog.deliver", scope="guild", deadline_ms=300000,
                    channel_publication=ChannelPublicationSpec(
                        channel_types=(0,), require_history=True,
                        delivery=ScheduledDeliverySpec(private=True, bot_permissions=("embed_links", "attach_files")),
                    ),
                    message_mutation=MessageMutationSpec(
                        channel_types=(0,), allow_replacement=True,
                        delivery=ScheduledDeliverySpec(
                            private=True, bot_permissions=("embed_links", "attach_files"), recorded_target=True,
                        ),
                    ),
                ),
                ScheduledCallbackSpec(
                    RECOVER, RECOVER, scope="guild", deadline_ms=300000,
                    own_history=True, own_history_private=True,
                    own_history_bot_permissions=("send_messages", "embed_links", "attach_files"),
                ),
            ),
        )

    async def start(self):
        self.task = asyncio.create_task(self.maintain(), name="modlog-prepare")

    def begin_drain(self):
        self.draining = True

    def resume(self):
        self.draining = False

    def drain_ready(self):
        return self.active == 0

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.store.close()

    async def enabled(self):
        snapshots = [
            msgspec.json.decode(v, type=GuildPolicySnapshot)
            for v in await self.queue.hvals("lumi:policy:current")
        ]
        return {s.guild_id for s in snapshots if "modlog" in s.enabled_modules}

    async def maintain(self):
        while True:
            try:
                now = self.clock()
                if not self.draining:
                    self.active += 1
                    try:
                        try:
                            await self.refresh_capture_admission(now)
                        except (MySQLError, RedisError, ValueError, OSError, TimeoutError):
                            LOGGER.warning("modlog_capture_receive_unavailable")
                        await self.assets.cleanup(await self.store.cleanup(now))
                        enabled = await self.enabled()
                        if self.capture is not None:
                            try:
                                await self.capture.receive(enabled, now)
                            except (MySQLError, RedisError, OSError, ValueError, KeyError, TypeError, TimeoutError):
                                LOGGER.warning("modlog_capture_receive_unavailable")
                        if self.journal is not None:
                            try:
                                await self.journal.reconcile(enabled, now)
                            except (MySQLError, OSError, ValueError, KeyError, TypeError, TimeoutError):
                                LOGGER.warning("modlog_journal_unavailable")
                        pending = await self.store.files_pending(now, guilds=enabled)
                        await asyncio.gather(*(self.assets.ingest(f) for f in pending))
                        for out in await self.store.pending("prepare", now, guilds=enabled):
                            if out["guild"] in enabled:
                                await self.store.prepare(
                                    out["guild"], out["id"], out["revision"], now
                                )
                        await self.release_legacy_approvals(enabled, now)
                        await self.schedule_publications(enabled, now)
                        await self.schedule_recovery(enabled, now)
                        self.published = {
                            k: v for k, v in self.published.items() if v + 600000 > now
                        }
                    finally:
                        self.active -= 1
            except (MySQLError, RedisError, OSError, ValueError, TimeoutError):
                LOGGER.warning("modlog_maintenance_unavailable")
            await asyncio.sleep(1)

    async def refresh_capture_admission(self, now):
        if self.capture is None or now < self.admission_refresh_due:
            return
        await rehydrate_capture_admission(self.queue, await self.store.configurations())
        self.admission_refresh_due = now + 5000

    def result(self, job, intent=None):
        return ResultEnvelope(
            1, job, "modlog", "security", self.generation, "ok", intents=(intent,) if intent else ()
        )

    async def schedule_publications(self, enabled, now):
        for out in await self.store.pending("ready", now, guilds=enabled):
            key = (out["guild"], out["id"])
            if (
                out["guild"] not in enabled
                or self.published.get(key, 0) + 5000 > now
            ):
                continue
            candidate = await self.store.publication_candidate(
                out["guild"], out["id"], out["revision"], now,
            )
            if candidate is None:
                continue
            row, _, cfg = candidate
            destinations = cfg["destinations"]
            destination = destinations.get(row["category"]) or destinations.get("all")
            if not destination:
                continue
            revision = await self.queue.incr("lumi:modlog:schedule:" + out["id"])
            await self.scheduler.publish(
                operation="upsert",
                schedule_key="modlog:" + out["id"],
                route="modlog.deliver",
                revision=revision,
                guild_id=out["guild"],
                channel_id=destination,
                actor_id=0,
                due_at_ms=now,
                expires_at_ms=min(out["expires"], now + 300000),
                arguments={"record": out["id"], "revision": out["revision"]},
            )
            self.published[key] = now

    async def schedule_recovery(self, enabled, now):
        for state in STATES:
            for out in await self.store.pending(state, now, guilds=enabled, limit=5):
                destination = out.get("destination")
                key = (out["guild"], out["id"], RECOVER)
                if not destination or self.published.get(key, 0) + 5000 > now:
                    continue
                revision = await self.queue.incr("lumi:modlog:schedule:" + out["id"])
                await self.scheduler.publish(
                    operation="upsert", schedule_key="modlog:recover:" + out["id"],
                    route=RECOVER, revision=revision, guild_id=out["guild"],
                    channel_id=destination, actor_id=0, due_at_ms=now,
                    expires_at_ms=min(out["expires"], now + 300000),
                    arguments={"record": out["id"], "attempt": recovery_identity(out)},
                )
                self.published[key] = now

    async def release_legacy_approvals(self, enabled, now):
        for out in await self.store.pending("approved", now, guilds=enabled):
            await self.store.transition(
                out["guild"], out["id"], {"approved"}, previous=out, state="ready", due=now,
            )

    def session(self, event, **values):
        return dict(
            token=secrets.token_hex(8),
            owner=int(event["actor_id"]),
            guild=int(event["guild_id"]),
            channel=int(event["channel_id"]),
            expires=self.clock() + SESSION_TTL,
            revision=0,
            screen="history",
            section="destinations",
            category="all",
            filter="user",
            page=0,
            detail_page=0,
            record="",
            subject=0,
            actor=0,
            source=0,
            hours=168,
            event="",
            **values,
        )

    async def save(self, s):
        await self.cache.set(
            PREFIX + s["token"], json.dumps(s), px=max(1, s["expires"] - self.clock())
        )

    def control(self, s, action, label=None, **kwargs):
        return Control(
            kwargs.pop("kind", "button"),
            "modlog",
            action,
            f"{s['token']}~{s['revision']}",
            label=label,
            **kwargs,
        )

    def select(self, s, action, placeholder, options, **kwargs):
        return self.control(
            s,
            action,
            kind="string_select",
            placeholder=placeholder,
            options=tuple(options),
            **kwargs,
        )

    async def call(self, request, job, s):
        cfg = await self.store.configuration(s["guild"])
        destinations = cfg["destinations"]
        result = await self.actions.request(
            job_id=job,
            kind="read_interaction_context",
            guild_id=s["guild"],
            channel_id=request.event["channel_id"],
            target_channel_ids=tuple(sorted(set(destinations.values()) - {0, None})),
            idempotency_key="modlog-ui:"
            + job
            + ":context"
            + ":" + secrets.token_hex(4),
            wait_seconds=12,
        )
        if not result.ok:
            raise ValueError(
                "Access unavailable."
                if result.error_code == "permission_denied"
                else "Unavailable. Refresh."
            )
        facts = result.data
        channels = {channel["id"]: channel for channel in facts["channels"]}
        readable = {cid for cid, channel in channels.items() if channel["readable"]}
        target = facts["origin_parent"]
        target_channel = channels.get(target)
        administrator = facts["guild_administrator"]
        return dict(
            administrator=administrator,
            config=cfg if administrator else {
                **cfg,
                "destinations": {
                    key: cid for key, cid in destinations.items()
                    if key == "all" or (cid or destinations.get("all")) in readable
                },
            },
            status_facts=dict(
                audit_log_available="view_audit_log" in facts["bot_guild_permissions"],
                unavailable_destinations=[
                    key for key, cid in destinations.items() if cid and cid not in channels
                ],
            ),
            history_destination=target if target in readable else 0,
            history_nsfw=bool(target_channel and target_channel["nsfw"] and facts["origin_nsfw"]),
        )

    async def handle(self, request, job_id, generation):
        if (
            generation != self.generation
            or self.draining
            or request.event.get("_expires_at_ms", self.clock() + 1) <= self.clock()
        ):
            return self.result(job_id)
        try:
            if request.handler == RECOVER:
                await recover(
                    self.store, self.queue, self.actions, request, job_id, generation=self.generation, clock=self.clock,
                )
                return self.result(job_id)
            if request.handler == "modlog.deliver":
                event, args = request.event, request.arguments
                guild, revision, record = event.get("guild_id"), args.get("revision"), args.get("record")
                now = self.clock()
                deadline = event.get("_expires_at_ms", 0)
                if (
                    request.route != "modlog.deliver" or event.get("dispatch") != "scheduled"
                    or event.get("actor_id") != 0 or event.get("_job_generation") != self.generation
                    or type(deadline) is not int or deadline <= now
                    or type(guild) is not int or guild <= 0
                    or type(revision) is not int or revision <= 0
                    or not isinstance(record, str) or not re.fullmatch(r"[a-f0-9]{32}", record)
                ):
                    return self.result(job_id)
                await self.publisher.send(
                    request, job_id,
                    guild=guild, record=record, revision=revision, generation=self.generation,
                )
                return self.result(job_id)
            if request.handler == "modlog.command":
                if request.arguments.get("operation") == "help":
                    intent = DiscordIntent(
                        "send", pages=(Page(description=".modlog [@user|id] · .modlog setup"),),
                    )
                    await self.replies.approve(request, job_id, intent)
                    return self.result(job_id, intent)
                s = self.session(request.event)
                s["screen"] = (
                    "setup" if request.arguments.get("operation") == "setup" else "history"
                )
                s["subject"] = int(
                    request.arguments.get("mention") or request.arguments.get("user_id") or 0
                )
                await self.save(s)
                context = await self.call(request, job_id, s)
                if s["screen"] == "setup" and not context["administrator"]:
                    return self.result(job_id)
                return self.result(
                    job_id, await self.panel(request, job_id, s, kind="send", context=context)
                )
            if request.handler == "modlog.record":
                s = self.session(request.event)
                s.update(screen="detail", record=str(request.arguments["record"]))
                await self.save(s)
                return self.result(
                    job_id, await self.panel(request, job_id, s, kind="send")
                )
            token = str(request.arguments.get("session") or "")
            async with self.cache.lock(PREFIX + token + ":lock", timeout=30, blocking_timeout=1):
                raw = await self.cache.get(PREFIX + token)
                s = json.loads(raw) if raw else None
                bound = await self.cache.get(ui_key("message", token))
                if (
                    not s
                    or s["expires"] <= self.clock()
                    or (s["owner"], s["guild"], s["channel"], s["revision"])
                    != (
                        request.event.get("actor_id"),
                        request.event.get("guild_id"),
                        request.event.get("channel_id"),
                        request.arguments.get("revision"),
                    )
                    or not bound
                    or int(bound) != request.event.get("message_id")
                ):
                    raise ValueError("Panel expired.")
                action = request.route.rpartition(".")[2]
                if action in SETTINGS:
                    config = await configure(
                        self.store, request, s, generation=self.generation, now=self.clock(),
                        before_commit=self.publisher.revoke,
                    )
                    await publish_capture_admission(self.queue, config)

                    return self.result(job_id, await self.panel(request, job_id, s))
                context = await self.call(request, job_id, s)
                values = request.arguments.get("values", [])
                selected = str(values[0]) if values else ""
                kind = "edit"
                if action == "open":


                    if not s.get("opener"):
                        raise ValueError("Panel expired.")
                    s.pop("opener")
                elif action == "nav":
                    if selected not in {"history", "status", "setup"}:
                        raise ValueError("Unavailable.")
                    s.update(screen=selected, page=0)
                elif action == "section":
                    if selected not in {"destinations", "events", "excluded", "bots"}:
                        raise ValueError("Unavailable.")
                    s["section"] = selected
                elif action == "pick_category":
                    if selected not in (*CATEGORIES, "all"):
                        raise ValueError("Unavailable.")
                    s["category"] = selected
                    s.update(event="", page=0)
                    if s["screen"] == "history":
                        s["filter"] = "event" if selected != "all" else "category"
                elif action == "filter":
                    if selected not in {"user", "actor", "channel", "window", "category", "event"}:
                        raise ValueError("Unavailable.")
                    s["filter"] = selected
                elif action in {"user", "actor", "channel"}:
                    s[{"user": "subject", "actor": "actor", "channel": "source"}[action]] = int(
                        selected or 0
                    )
                    s["page"] = 0
                elif action == "window":
                    if selected not in {"1", "24", "168"}:
                        raise ValueError("Unavailable.")
                    s.update(hours=int(selected), page=0)
                elif action == "event":
                    if selected and selected not in {"all", *TITLES}:
                        raise ValueError("Unavailable.")
                    s.update(event="" if selected == "all" else selected, page=0)
                elif action in {"previous", "next"}:
                    s["page"] = max(0, s["page"] + (-1 if action == "previous" else 1))
                elif action == "pick":
                    if selected not in s.get("records", []):
                        raise ValueError("Unavailable.")
                    s.update(record=selected, screen="detail", detail_page=0)
                elif action in {"detail_previous", "detail_next"}:
                    s["detail_page"] = max(
                        0, s["detail_page"] + (-1 if action == "detail_previous" else 1)
                    )
                elif action == "files":
                    if s["screen"] != "detail":
                        raise ValueError("Evidence access unavailable.")
                    snapshot = ReplySnapshot(self.replies)
                    row, _, subjects = await self.evidence.read(
                        request, s, context, witness=snapshot.capture,
                    )
                    attachments = await self.evidence.files(request, job_id, s, row)
                    intent = DiscordIntent(
                        "send",
                        pages=(Page(blocks=tuple(
                            PresentationBlock("file", file_name=item.file_name, spoiler=True)
                            for item in attachments
                        )) if attachments else Page(description="Files unavailable."),),
                        attachments=attachments,
                    )
                    try:
                        await self.replies.approve(
                            request, job_id, intent,
                            config_revision=context["config"]["revision"],
                            private_target=context["history_destination"], records=(row,),
                            participants=subjects, session_expires=s["expires"], snapshot=snapshot,
                        )
                    except BaseException:
                        self.evidence.discard(job_id, attachments)
                        raise
                    return self.result(job_id, intent)


                intent = await self.panel(request, job_id, s, kind=kind, context=context)
                return self.result(job_id, intent)
        except (ValueError, ActionUnavailable, MySQLError, RedisError, TimeoutError, OSError):
            LOGGER.warning("modlog_request_unavailable")
            intent = DiscordIntent(
                "send" if request.handler == "modlog.command" else "ephemeral",
                pages=(Page(description="Unavailable. Try again."),),
            )
            try:
                await self.replies.approve(request, job_id, intent)
            except (ValueError, RedisError, OSError, TimeoutError):
                return self.result(job_id)
            return self.result(job_id, intent)

    async def panel(self, request, job, s, *, kind="edit", context=None):
        context = context or await self.call(request, job, s)
        config = context["config"]
        s["config_revision"] = config["revision"]
        screen = s["screen"]
        records, subjects = (), ()
        snapshot = ReplySnapshot(self.replies)
        controls = []
        description = ""
        if screen == "detail":
            row, snapshots, subjects = await self.evidence.read(
                request, s, context, witness=snapshot.capture,
            )
            record, more = self.evidence.detail(row, snapshots, s["detail_page"])
            records = (row,)
            page = render(record, private=True, page=s["detail_page"])


            controls.extend(
                (
                    self.control(s, "detail_previous", "Previous", disabled=s["detail_page"] == 0),
                    self.control(s, "detail_next", "Next", disabled=not more),
                    self.control(s, "files", "Files", disabled=not record.get("files")),
                )
            )
        elif screen == "history":
            target = context.get("history_destination")
            if type(target) is not int or target < 0 or type(context.get("history_nsfw")) is not bool:
                raise ValueError("History access unavailable.")
            spec = ModlogSpec(
                "query",
                subject=s["subject"],
                actor=s["actor"],
                channel=s["source"],
                hours=s["hours"],
                category="" if s["category"] == "all" else s["category"],
                event=s["event"],
                page=s["page"],
            )
            records, more, available = await self.store.public_history(
                s["guild"], spec, self.clock(), destination=target, revision=config["revision"],
                access=HistoryAccess(self.block_owner_id, self.scoped_blocks, context["history_nsfw"]),
                witness=snapshot.capture,
            )
            s["records"] = [r["id"] for r in records]
            description = "\n\n".join(summary(r) for r in records) or "No records."
            if not available:
                description = "View history in the configured log channel."
            controls.append(
                self.select(
                    s,
                    "filter",
                    "Filter",
                    [
                        Option(x.title(), x)
                        for x in ("user", "actor", "channel", "window", "category", "event")
                    ],
                )
            )
            mode = s["filter"]
            if mode in {"user", "actor", "channel"}:
                controls.append(
                    self.control(
                        s,
                        mode,
                        kind="channel_select" if mode == "channel" else "user_select",
                        placeholder=mode.title(),
                        min_values=0,
                        max_values=1,
                        default_ids=tuple(
                            x
                            for x in (
                                s["source"]
                                if mode == "channel"
                                else s["subject"]
                                if mode == "user"
                                else s["actor"],
                            )
                            if x
                        ),
                    )
                )
            elif mode == "window":
                controls.append(
                    self.select(
                        s,
                        "window",
                        "Time",
                        [
                            Option(label, val)
                            for label, val in (
                                ("1 hour", "1"),
                                ("24 hours", "24"),
                                ("7 days", "168"),
                            )
                        ],
                    )
                )
            elif mode == "category" or s["category"] == "all":
                controls.append(
                    self.select(
                        s,
                        "pick_category",
                        "Event category",
                        [Option(x.title(), x) for x in ("all", *CATEGORIES)],
                    )
                )
            else:
                controls.append(
                    self.select(
                        s,
                        "event",
                        "Event",
                        [
                            Option("All events", "all"),
                            *(
                                Option(TITLES[x], x)
                                for x in EVENTS[
                                    s["category"] if s["category"] != "all" else "messages"
                                ]
                            ),
                        ],
                    )
                )
            if s["records"]:
                controls.append(
                    self.select(
                        s,
                        "pick",
                        "View record",
                        [
                            Option(
                                TITLES[r["kind"]],
                                r["id"],
                                description=escape(r.get("name", ""), 80),
                            )
                            for r in records
                        ],
                    )
                )
            controls.extend(
                (
                    self.control(s, "previous", "Previous", disabled=s["page"] == 0),
                    self.control(s, "next", "Next", disabled=not more),
                )
            )
            page = Page(title="History", description=description)
        elif screen == "setup":
            if not context["administrator"]:
                raise ValueError("Administrator required.")
            section = s["section"]
            description = "Private-channel content is copied to the selected staff audience."
            controls.append(
                self.select(
                    s,
                    "section",
                    "Setup",
                    [Option(x.title(), x) for x in ("destinations", "events", "excluded", "bots")],
                )
            )
            if section in {"destinations", "events"}:
                categories = ("all", *CATEGORIES) if section == "destinations" else CATEGORIES
                if s["category"] not in categories:
                    s["category"] = "messages"
                controls.append(
                    self.select(
                        s,
                        "pick_category",
                        "Category",
                        [
                            Option(
                                "Default" if x == "all" else x.title(),
                                x,
                                default=x == s["category"],
                            )
                            for x in categories
                        ],
                    )
                )
            if section == "destinations":
                controls.append(
                    self.control(
                        s,
                        "destination",
                        kind="channel_select",
                        placeholder="Private log channel",
                        min_values=0,
                        max_values=1,
                        channel_types=(0,),
                        default_ids=(config["destinations"][s["category"]],)
                        if config["destinations"].get(s["category"])
                        else (),
                    )
                )
            elif section == "events":
                controls.append(
                    self.select(
                        s,
                        "events",
                        "Enabled events",
                        [
                            Option(TITLES[x], x, default=x in config["events"])
                            for x in EVENTS[s["category"]]
                        ],
                        min_values=0,
                        max_values=len(EVENTS[s["category"]]),
                    )
                )
            elif section == "excluded":
                controls.append(
                    self.control(
                        s,
                        "excluded",
                        kind="channel_select",
                        placeholder="Excluded channels/categories",
                        min_values=0,
                        max_values=25,
                        default_ids=tuple(config["excluded"]),
                    )
                )
            else:
                controls.append(
                    self.control(s, "bots", "Bot messages: " + ("On" if config["bots"] else "Off"))
                )
            page = Page(title="Setup", description=description)
        else:
            stats = await self.store.stats(s["guild"], self.clock())
            facts = context["status_facts"]
            problems = []
            if not facts["audit_log_available"]:
                problems.append("Audit-log permission missing.")
            if not config["destinations"].get("all"):
                problems.append("Default log channel not set.")
            problems.extend(
                key.title() + " destination unavailable."
                for key in facts["unavailable_destinations"]
            )
            description = (
                f"Queued · {stats['queued']}\nUncertain · {stats['uncertain']}\n"
                f"Archive · {stats['archive_bytes'] // 1024**2} / 1,024 MiB\n"
                f"Skipped files · {stats['skipped']}"
            )
            if problems:
                description += "\n" + "\n".join(problems)
            if stats.get("gaps"):
                description += "\nCoverage gap recorded."
            page = Page(title="Status", description=description)
        nav = [
            Option(x.title(), x)
            for x in ("history", "status", "setup")
            if x != "setup" or context["administrator"]
        ]
        controls.insert(0, self.select(s, "nav", "Modlog", nav))
        controls.append(self.control(s, "refresh", "Refresh"))
        s["revision"] += 1

        controls = [
            msgspec.structs.replace(c, payload=f"{s['token']}~{s['revision']}") for c in controls
        ]
        if page.blocks:
            control_rows = tuple(PresentationBlock("action_row", controls=(c,)) for c in controls)
            page = msgspec.structs.replace(page, blocks=(*page.blocks, *control_rows))
        else:
            page = msgspec.structs.replace(page, controls=tuple(controls))
        await self.save(s)
        intent = DiscordIntent(kind, pages=(page,), session_id=s["token"])
        await self.replies.approve(
            request, job, intent, config_revision=config["revision"],
            private_target=context["history_destination"] if records else 0,
            records=records, participants=subjects, session_expires=s["expires"],
            require_admin=screen == "setup",
            snapshot=snapshot,
        )
        return intent
