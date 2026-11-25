from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import json
import logging
import secrets
import time
import unicodedata

from redis.asyncio import Redis
from redis.exceptions import RedisError
from asyncmy.errors import MySQLError

from lumi.contracts import (
    ChannelTransitionSpec,
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    PresentationBlock,
    PresentationControl,
    PresentationPage,
    ResultEnvelope,
    ScheduledCallbackSpec,
    TypedRequest,
)
from lumi.contracts.member_profiles import (
    LIST_GUILD_MEMBERS, READ_MEMBER_PROFILE, SET_MEMBER_NICKNAME, MemberProfileAccess,
)
from lumi.contracts.channel_messages import DELETE_CHANNEL_MESSAGES, READ_CHANNEL_MESSAGES, ChannelMessageAccess
from lumi.contracts.channel_configuration import READ_CHANNEL_CONFIGURATION, ChannelConfigurationAccess
from lumi.contracts.channel_resources import READ_CHANNEL_RESOURCES, READS, WRITE_CHANNEL_RESOURCE, WRITES, ChannelResourceAccess
from lumi.moderation.channel_resource_transport import ChannelResourceTransport
from lumi.moderation.reset_runner import ROUTE as RESET_WORK, ResetRunner
from lumi.moderation.member_operations import EFFECTS, apply as apply_member_operation
from lumi.contracts.role_access import READ_GUILD_ROLES, SET_ROLE_MEMBERSHIP, RoleAccessSpec
from lumi.moderation.purge import HELP as PURGE_HELP
from lumi.moderation.purge import PurgeError
from lumi.moderation.purge import parse as parse_purge
from lumi.moderation.purge_controls import BOT_CHANNEL_FACTS, PurgeControls
from lumi.moderation.purge_runner import CALLBACK_MS as PURGE_CALLBACK_MS, ROUTE as PURGE_WORK, PurgeRunner
from lumi.moderation.purge_transport import PurgeChannelTransport
from lumi.moderation.rename import HELP as RENAME_HELP
from lumi.moderation.rename import page as rename_page
from lumi.moderation.rename import parse as parse_rename
from lumi.moderation.rename_controls import RenameControls
from lumi.moderation.rename_runner import RenameRunner
from lumi.moderation.rename_transport import CALLBACK_MS, ROUTE as RENAME_WORK, RenameTransport
from lumi.moderation.rules import (
    PERMISSIONS,
    SESSION_PREFIX,
    SESSION_TTL,
    ModerationError,
)
from lumi.moderation.warnings import apply_warning
from lumi.shared.access_receipts import admitted_request
from lumi.shared.discord_effects import UPDATE_BOUND_PANEL
from lumi.shared.display_text import escape_embed_text

from .actions import ActionUnavailable, EdgeActionClient
from .role_editor import RoleEditor
from .scheduler import ScheduledCallbackClient

MAX_REASON_CHARS = 400
LOGGER = logging.getLogger(__name__)
MODERATION_ACTION_GIF_URL = "https://media.tenor.com/XHLM0wQVtQ0AAAAC/anime-girl.gif"
RENAME_ACCESS = MemberProfileAccess(
    ("manage_nicknames",), ("manage_nicknames",), owner_only=True,
    allow_directory=True, allow_nickname_write=True,
)


def normalize_reason(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    without_controls = "".join(
        character
        for character in normalized
        if character.isspace() or unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )
    return " ".join(without_controls.split())[:MAX_REASON_CHARS]


class ModerationModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        group: str = "security",
        cache_redis: Redis | None = None,
        warning_store=None,
        rename_store=None,
        purge_store=None,
        channel_handovers=None,
        clock=None,
    ) -> None:
        self._generation = generation
        self._group = group
        self._cache = cache_redis if cache_redis is not None else queue_redis
        self._queue = queue_redis
        self._warnings = warning_store
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._rename_runner = self._rename_controls = None
        self._purge_runner = self._purge_controls = None
        self._reset_runner = None
        self._retention_task = None
        self._draining = False
        self._retention_active = False
        self._actions = EdgeActionClient(
            queue_redis,
            module="moderation",
            group=group,
            generation=generation,
        )
        if rename_store is not None:
            scheduler = ScheduledCallbackClient(queue_redis, module="moderation", group=group, generation=generation)
            transport = RenameTransport(queue_redis, self._cache, self._actions, scheduler, group, generation, self._clock)
            self._rename_runner = RenameRunner(rename_store, transport, self._clock)
            self._rename_controls = RenameControls(
                rename_store, queue_redis, self._cache, group, self._clock,
                wake=self._rename_runner, revoke=transport.revoke,
            )
        if purge_store is not None:
            scheduler = ScheduledCallbackClient(queue_redis, module="moderation", group=group, generation=generation)
            transport = PurgeChannelTransport(queue_redis, self._actions, group, generation, self._clock)
            self._purge_runner = PurgeRunner(purge_store, transport, scheduler, self._cache, group, self._clock)
            self._purge_controls = PurgeControls(self._purge_runner, queue_redis, self._cache, channel_handovers, group, self._clock)

    def install_reset(self, journal, references):
        scheduler = ScheduledCallbackClient(self._queue, module="moderation", group=self._group, generation=self._generation)
        transport = ChannelResourceTransport(self._queue, self._actions, "moderation", self._group, self._generation, self._clock)
        self._reset_runner = ResetRunner(self._purge_runner.store, transport, scheduler, self._cache, self._group,
                                         self._clock, journal, references, self._generation)
        self._purge_controls.reset_revoke = transport.revoke

    @property
    def manifest(self) -> Manifest:
        reset = getattr(self, "_reset_runner", None)
        return Manifest(
            schema=2,
            module="moderation",
            channel_transition=ChannelTransitionSpec(
                control_handlers=("moderation.purge", "moderation.purge_confirm"),
            ),
            group=self._group,
            generation=self._generation,
            job_stream=f"lumi:jobs:{self._group}",
            display_name="Moderation",
            summary="Server moderation.",
            presentation_mode="v2",
            commands=(
                CommandSpec(
                    root="ban",
                    handler="moderation.ban",
                    grammar=(
                        r"^(?:\s+(?P<help>(?i:help))|\s+(?:"
                        r"<@!?(?P<mention>[0-9]{17,20})>|(?P<user_id>[0-9]{17,20})"
                        r")(?:\s+(?P<reason>[\s\S]{1,400}))?)?$"
                    ),
                    captures=(
                        CaptureSpec("help", required=False, maximum=4),
                        CaptureSpec("mention", "snowflake", required=False),
                        CaptureSpec("user_id", "snowflake", required=False),
                        CaptureSpec("reason", required=False, maximum=MAX_REASON_CHARS),
                    ),
                    help=HelpSpec(".ban <@user|ID> [reason]", "Ban a member or user ID."),
                    acl_key="ban",
                    deadline_ms=30_000,
                    queue_class="security",
                    public=True,
                    required_permissions=("ban_members",),
                    presentation_mode="v2",
                    context_fields=("prefix",),
                    member_effect=EFFECTS["ban"],
                ),
                *self._commands(),
            ),
            components=tuple(
                ComponentSpec(
                    namespace="moderation",
                    action=action,
                    handler="moderation.history",
                    grammar=r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$",
                    captures=(
                        CaptureSpec("session"),
                        CaptureSpec("revision", "integer", minimum=0),
                    ),
                    acl_key="warnings",
                    required_permissions=("moderate_members",),
                    public=True,
                    ephemeral=True,
                    session_bound=True,
                    private_access_checkpoint=True,
                    queue_class="security",
                    deadline_ms=30_000,
                )
                for action in ("warnings_open", "warnings_previous", "warnings_next")
            )
            + tuple(
                ComponentSpec(
                    namespace="moderation",
                    action=action,
                    handler="moderation.purge_confirm",
                    grammar=r"^(?P<session>[a-f0-9]{16})~(?P<purge_id>[a-f0-9]{8})$",
                    captures=(CaptureSpec("session"), CaptureSpec("purge_id")),
                    acl_key="purge",
                    required_permissions=("manage_messages",),
                    bot_channel_facts=BOT_CHANNEL_FACTS,
                    channel_configuration=ChannelConfigurationAccess(),
                    public=True,
                    session_bound=True,
                    queue_class="security",
                    deadline_ms=30_000,
                )
                for action in ("purge_confirm", "purge_cancel")
            )
            + tuple(
                ComponentSpec(
                    namespace="moderation",
                    action=action,
                    handler="moderation.rename_confirm",
                    grammar=r"^(?P<session>[a-f0-9]{16})~(?P<rename_id>[a-f0-9]{8})$",
                    captures=(CaptureSpec("session"), CaptureSpec("rename_id")),
                    acl_key="rename",
                    required_permissions=("manage_nicknames",),
                    bot_guild_permissions=("manage_nicknames",),
                    owner_scope="server",
                    public=True,
                    session_bound=True,
                    queue_class="security",
                    deadline_ms=30_000,
                )
                for action in ("rename_confirm", "rename_decline")
            )
            + tuple(
                ComponentSpec(
                    namespace="moderation",
                    action="roles_" + action,
                    handler="moderation.roles",
                    grammar=r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$",
                    captures=(
                        CaptureSpec("session"),
                        CaptureSpec("revision", "integer", minimum=0),
                    ),
                    input_kind="role_ids"
                    if action == "stage"
                    else "string_values"
                    if action == "choose"
                    else "none",
                    acl_key="role",
                    required_permissions=("manage_roles",),
                    public=True,
                    session_bound=True,
                    queue_class="security",
                    deadline_ms=120_000,
                    role_access=RoleAccessSpec(allow_write=action in {"apply", "choose"}),
                )
                for action in ("stage", "choose", "apply", "clear", "previous", "next")
            ),
            capabilities=(
                *(effect.kind for effect in EFFECTS.values()),
                READ_GUILD_ROLES, SET_ROLE_MEMBERSHIP,
                LIST_GUILD_MEMBERS, READ_MEMBER_PROFILE, SET_MEMBER_NICKNAME, UPDATE_BOUND_PANEL,
                READ_CHANNEL_MESSAGES, DELETE_CHANNEL_MESSAGES, READ_CHANNEL_CONFIGURATION,
                *((READ_CHANNEL_RESOURCES, WRITE_CHANNEL_RESOURCE) if reset is not None else ()),
            ),
            scheduled=(ScheduledCallbackSpec(
                RENAME_WORK, RENAME_WORK, deadline_ms=CALLBACK_MS, queue_class="security", acl_key="rename",
                member_profiles=RENAME_ACCESS, bound_panel=True,
            ), ScheduledCallbackSpec(
                PURGE_WORK, PURGE_WORK, deadline_ms=PURGE_CALLBACK_MS, queue_class="security", acl_key="purge",
                channel_messages=ChannelMessageAccess(include_content=True, allow_delete=True), bound_panel=True,
            ), *((ScheduledCallbackSpec(
                RESET_WORK, RESET_WORK, deadline_ms=PURGE_CALLBACK_MS, queue_class="security", acl_key="purge",
                channel_resources=ChannelResourceAccess(tuple(sorted(READS | WRITES))),
            ),) if reset is not None else ())),
            version="1.4.4",
        )

    def _commands(self):
        user = r"(?:<@!?(?P<mention>[0-9]{17,20})>|(?P<user_id>[0-9]{17,20}))"
        target_captures = (
            CaptureSpec("mention", "snowflake", required=False),
            CaptureSpec("user_id", "snowflake", required=False),
        )
        help_capture = CaptureSpec("help", required=False, maximum=4)
        reason_capture = CaptureSpec("reason", required=False, maximum=MAX_REASON_CHARS)
        optional_reason = r"(?:\s+(?P<reason>[\s\S]{1,400}))?"
        for name, permission in PERMISSIONS.items():
            if name == "role":
                yield CommandSpec(
                    root="role",
                    handler="moderation.role",
                    grammar=(r"^(?:\s+(?P<help>(?i:help))|\s+" + user
                             + r"(?:\s+(?P<role_query>[\s\S]{1,100}))?)?$"),
                    captures=(*target_captures, help_capture,
                              CaptureSpec("role_query", required=False, maximum=100)),
                    help=HelpSpec(
                        ".role <@user|ID>\n.role <@user|ID> <role>",
                        "Open the role picker or toggle a matching role.",
                    ),
                    acl_key="role",
                    required_permissions=("manage_roles",),
                    public=True,
                    deadline_ms=120_000,
                    queue_class="security",
                    context_fields=("prefix",),
                    role_access=RoleAccessSpec(("mention", "user_id"), allow_write=True),
                )
                continue
            if name == "rename":
                yield CommandSpec(
                    root="rename",
                    handler="moderation.rename",
                    grammar=r"^(?: (?P<raw>[\s\S]*))?$",
                    captures=(CaptureSpec("raw", required=False, maximum=4000),),
                    help=HelpSpec(
                        ".rename <name>\n.rename <undo|status|cancel|resume>",
                        "Temporarily rename server members or undo a rename."
                    ),
                    acl_key="rename",
                    required_permissions=(permission,),
                    owner_scope="server",
                    public=True,
                    deadline_ms=30_000,
                    queue_class="security",
                    context_fields=("prefix",),
                    bot_guild_permissions=("manage_nicknames",),
                )
                continue
            aliases = {"timeout": ("mute",), "untimeout": ("unmute",), "purge": ("clear",)}.get(
                name, ()
            )
            tail, captures = r"\s+" + user + optional_reason, (*target_captures, reason_capture)
            usage = f".{name} <@user|ID> [reason]"
            summary = {
                "kick": "Remove a member from the server.",
                "warn": "Record a warning with a reason.",
                "warnings": "View a member's warning history.",
                "unwarn": "Revoke a warning by its ID.",
                "timeout": "Temporarily mute a member, for example 10m or 2h.",
                "untimeout": "End a member's timeout early.",
                "unban": "Lift a user's server ban.",
                "purge": "Delete matching messages; count limits messages checked.",
            }[name]
            if name == "warn":
                tail = r"\s+" + user + r"\s+(?P<reason>[\s\S]{1,400})"
                usage = ".warn <@user|ID> <reason>"
            elif name == "warnings":
                tail = r"\s+" + user + r"(?:\s+(?P<page>[0-9]{1,6}))?"
                captures = (
                    *target_captures,
                    CaptureSpec("page", "integer", required=False, minimum=1, maximum=999999),
                )
                usage = ".warnings <@user|ID> [page]"
            elif name == "unwarn":
                tail = r"\s+(?P<warning_id>[a-fA-F0-9]{8})" + optional_reason
                captures = (CaptureSpec("warning_id", required=False, maximum=8), reason_capture)
                usage = ".unwarn <warning-id> [reason]"
            elif name == "timeout":
                tail = r"\s+" + user + r"\s+(?P<duration>[0-9a-zA-Z]{1,64})" + optional_reason
                captures = (
                    *target_captures,
                    CaptureSpec("duration", required=False, maximum=64),
                    reason_capture,
                )
                usage = ".timeout <@user|ID> <duration> [reason]"
            elif name == "purge":
                tail = r"\s+(?P<raw>[\s\S]{1,1000}[\s\S]{0,1000})"
                captures = (CaptureSpec("raw", required=False, maximum=2000),)
                usage = (
                    ".purge <count|channel> [filters]\n.purge 50 bots:on\n"
                    ".purge <status|cancel> [id]\n.purge resume <id>"
                )
            yield CommandSpec(
                root=name,
                aliases=aliases,
                handler="moderation." + name,
                grammar=r"^(?:\s+(?P<help>(?i:help))|" + tail + r")?$",
                captures=(help_capture, *captures),
                help=HelpSpec(usage, summary),
                acl_key=name,
                deadline_ms=30_000,
                queue_class="security",
                public=True,
                required_permissions=(permission,),
                context_fields=("prefix",),
                member_target=("mention", "user_id") if name == "warn" else (),
                member_effect=EFFECTS.get(name),
                bot_channel_facts=BOT_CHANNEL_FACTS if name == "purge" else (),
                channel_configuration=ChannelConfigurationAccess(other_channel=True) if name == "purge" else None,
            )

    async def handle(
        self,
        request: TypedRequest,
        job_id: str,
        generation: int,
    ) -> ResultEnvelope:
        if request.route == request.handler == RESET_WORK:
            if generation == self._generation and self._reset_runner is not None:
                try:
                    await self._reset_runner.callback(request, job_id)
                except (MySQLError, OSError, RedisError, RuntimeError, TimeoutError, ValueError):
                    LOGGER.warning("reset_callback_unavailable")
            return ResultEnvelope(1, job_id, "moderation", self._group, self._generation, "ok")
        if request.route == request.handler == PURGE_WORK:
            if generation == self._generation and self._purge_runner is not None:
                try:
                    await self._purge_runner.callback(request, job_id)
                except (MySQLError, OSError, RedisError, RuntimeError, TimeoutError, ValueError):
                    LOGGER.warning("purge_callback_unavailable")
            return ResultEnvelope(1, job_id, "moderation", self._group, self._generation, "ok")
        if request.route == request.handler == RENAME_WORK:
            if generation == self._generation and self._rename_runner is not None:
                try:
                    await self._rename_runner.run_callback(request, job_id)
                except (MySQLError, OSError, RedisError, RuntimeError, TimeoutError, ValueError):
                    LOGGER.warning("rename_callback_unavailable")
            return ResultEnvelope(1, job_id, "moderation", self._group, self._generation, "ok")
        if generation != self._generation:
            return self._text(job_id, "Unavailable.", error_code="stale_generation")
        if request.handler in {"moderation.role", "moderation.roles"}:
            try:
                editor = self._role_editor()
                intent, revision = await editor.handle(request, job_id)
                return self._page(job_id, intent, revision=revision)
            except (ActionUnavailable, MySQLError, TimeoutError, RedisError, OSError, RuntimeError):
                return self._text(
                    job_id,
                    "Changes unconfirmed.",
                    kind="ephemeral" if request.event.get("dispatch") == "component" else "send",
                )
        if request.handler == "moderation.history":
            return await self._history(request, job_id)
        if request.handler in {"moderation.purge", "moderation.purge_confirm"}:
            return await self._purge(request, job_id)
        if request.handler in {"moderation.rename", "moderation.rename_confirm"}:
            return await self._rename(request, job_id)
        if request.handler != "moderation.ban":
            return await self._command(request, job_id)
        prefix = str(request.event.get("prefix") or ".")
        usage = f"{prefix}ban <user> [reason]"
        target_id = request.arguments.get("mention") or request.arguments.get("user_id")
        if request.arguments.get("help") or not isinstance(target_id, int):
            return self._text(job_id, usage)

        reason = normalize_reason(str(request.arguments.get("reason") or ""))
        if not self._admitted(request, "ban"):
            return self._text(job_id, "Unavailable.", error_code="unavailable")
        try:
            status, data = await apply_member_operation(
                self._warnings, self._queue, self._actions, request, job_id, self._clock,
                group=self._group,
            )
        except (ActionUnavailable, MySQLError, OSError, RuntimeError, TimeoutError, RedisError):
            LOGGER.warning("moderation_storage_unavailable")
            return self._text(job_id, "Unavailable.", error_code="unavailable")
        if status == "banned":
            display_name = str(data.get("display_name") or f"User {target_id}")[:80]
            return self._success(job_id, display_name, reason)
        if status == "target_protected":
            return self._text(job_id, "Cannot ban that user.", error_code="cannot_ban")
        if status == "uncertain":
            return self._text(job_id, "Could not confirm.", error_code="uncertain")
        return self._text(job_id, "Unavailable.", error_code="unavailable")

    async def _command(self, request, job_id):
        name = request.handler.rpartition(".")[2]
        spec = next((s for s in self._commands() if s.root == name), None)
        if spec is None:
            return self._text(job_id, "Unavailable.", error_code="unavailable")
        args, event = request.arguments, request.event
        target = args.get("mention") or args.get("user_id")
        missing = not args.get("warning_id") if name == "unwarn" else not target
        if args.get("help") or missing:
            return self._text(job_id, str(event.get("prefix") or ".") + spec.help.usage[1:])
        try:
            if not self._admitted(request, name):
                return self._text(job_id, "Missing permission.")
            if name == "warnings":
                s = dict(
                    token=secrets.token_hex(8),
                    guild=event["guild_id"],
                    owner=event["actor_id"],
                    channel=event["channel_id"],
                    target=target,
                    page=args.get("page") or 1,
                    revision=0,
                    screen="opener",
                    expires=self._clock() + SESSION_TTL,
                )
                await self._save_session(s)
                return self._page(
                    job_id,
                    DiscordIntent(
                        "send",
                        session_id=s["token"],
                        pages=(
                            PresentationPage(
                                title="Warnings",
                                controls=(self._control(s, "warnings_open", "Open"),),
                            ),
                        ),
                    ),
                )
            if name in {"warn", "unwarn"}:
                return await self._apply_warning(request, job_id, target)
            status, data = await apply_member_operation(
                self._warnings, self._queue, self._actions, request, job_id, self._clock,
                group=self._group,
            )
            subject = escape_embed_text(str(data.get("display_name") or f"User {target}")[:80])
            text = {
                "kicked": f"{subject} was kicked.",
                "unbanned": f"{subject} was unbanned.",
                "not_banned": "User is not banned.",
                "timed_out": f"{subject} timed out until <t:{int(data.get('until', 0)) // 1000}:f>.",
                "timeout_removed": f"{subject} is no longer timed out.",
                "not_timed_out": "User is not timed out.",
                "permission_denied": "Missing permission.",
                "target_protected": "Cannot moderate that user.",
                "member_unavailable": "User is not in this server.",
                "uncertain": "Could not confirm.",
            }.get(status, "Unavailable.")
            return self._text(job_id, text)
        except ModerationError as exc:
            return self._text(job_id, str(exc))
        except (ActionUnavailable, MySQLError, OSError, RuntimeError, TimeoutError, RedisError):
            LOGGER.warning("moderation_storage_unavailable")
            return self._text(job_id, "Unavailable.", error_code="unavailable")

    def _admitted(self, request, acl):
        event = request.event
        return admitted_request(
            request, module="moderation", group=self._group, generation=self._generation,
            actor=event.get("actor_id"), guild=event.get("guild_id"), channel=event.get("channel_id"),
            acl=acl,
        )

    async def _apply_warning(self, request, job_id, target):
        if self._warnings is None:
            return self._text(job_id, "Unavailable.")
        try:
            status, data = await apply_warning(self._warnings, self._queue, request, self._clock)
        except (ValueError, MySQLError, RedisError, TimeoutError, OSError, RuntimeError):
            LOGGER.warning("moderation_warning_unavailable")
            return self._text(job_id, "Unavailable.")
        text = {
            "warned": f"<@{target}> warned · {data.get('warning_id')}",
            "revoked": f"Warning {data.get('warning_id')} removed.",
            "already_revoked": "Warning already removed.",
            "warning_missing": "Warning not found.",
            "target_protected": "Cannot moderate that user.",
            "member_unavailable": "User is not in this server.",
            "uncertain": "Could not confirm.",
            "invalid_reason": "Include a reason.",
        }.get(status, "Unavailable.")
        return self._text(job_id, text)

    @staticmethod
    def _failure_text(result):
        deleted = result.data.get("deleted", 0)
        if deleted:
            return f"Deleted {deleted} messages. Could not confirm the rest."
        return {
            "permission_denied": "Missing permission.",
            "target_protected": "Cannot moderate that user.",
            "member_unavailable": "User is not in this server.",
            "uncertain": "Could not confirm.",
            "panel_expired": "Panel expired.",
        }.get(result.error_code, "Unavailable.")

    async def _purge(self, request, job_id):
        event, args = request.event, request.arguments
        confirming = request.handler == "moderation.purge_confirm"
        try:
            if confirming:
                operation, code, spec = "confirm", args.get("purge_id"), None
            else:
                operation, code, spec = parse_purge(args.get("raw") or "")
                if operation == "help" or args.get("help"):
                    return self._text(job_id, PURGE_HELP.format(p=event.get("prefix") or "."))
            if self._purge_controls is None or not self._admitted(request, "purge"):
                return self._text(job_id, "Unavailable.", kind="ephemeral" if confirming else "send")
            result = await self._purge_controls.handle(request, job_id, operation, code, spec)
            if not result.ok:
                return self._text(
                    job_id,
                    result.data.get("message") or self._failure_text(result),
                    kind="ephemeral" if confirming else "send",
                )
            data = result.data
            texts = {
                "purge_missing": "Purge not found.",
                "purge_changed": "Purge changed. Try again.",
                "purge_busy": "A purge is already pending here.",
                "purge_full": "Purge queue full.",
                "purge_reset_unavailable": "Reset unavailable.",
            }
            if result.status in texts:
                return self._text(
                    job_id, texts[result.status], kind="ephemeral" if confirming else "send"
                )
            controls = ()
            token = data.get("token")
            if result.status == "purge_confirmation":
                text = (
                    "Recreate channel?\nAll messages and threads will be lost, including new ones. "
                    "New channel ID; external integrations need updating."
                    if data["mode"] == "reset"
                    else "Clear existing history?\nPins stay unless pins:off was selected."
                )
                controls = tuple(
                    PresentationControl(
                        "button",
                        "moderation",
                        action,
                        f"{token}~{data['id']}",
                        label=label,
                        style=style,
                    )
                    for action, label, style in (
                        ("purge_confirm", "Confirm", "danger"),
                        ("purge_cancel", "Cancel", "secondary"),
                    )
                )
            else:
                text = (
                    f"Purge {data['id']} · {data['state'].capitalize()}\n"
                    f"Deleted {data['deleted']} messages."
                )
                if data["mode"] == "reset":
                    text = (
                        "Channel reset."
                        if data["state"] == "done"
                        else f"Reset {data['id']} · {data['state'].capitalize()}"
                    )
                if data.get("error"):
                    text += "\n" + data["error"]
                if data.get("unconfirmed"):
                    text += f" {data['unconfirmed']} could not be confirmed."
            return self._page(
                job_id,
                DiscordIntent(
                    "edit" if confirming else "send",
                    session_id=token,
                    pages=(PresentationPage(description=text, controls=controls),),
                ),
            )
        except PurgeError as exc:
            return self._text(job_id, str(exc), kind="ephemeral" if confirming else "send")
        except (ActionUnavailable, MySQLError, OSError, TimeoutError, RedisError, ValueError):
            return self._text(job_id, "Unavailable.", kind="ephemeral" if confirming else "send")

    async def _rename(self, request, job_id):
        event, args = request.event, request.arguments
        confirming = request.handler == "moderation.rename_confirm"
        feedback = "ephemeral" if confirming else "send"
        try:
            if not self._admitted(request, "rename"):
                return self._text(job_id, "Unavailable.", kind=feedback)
            if not confirming and parse_rename(args.get("raw"))[0] == "help":
                return self._text(job_id, RENAME_HELP.format(p=event.get("prefix") or "."))
            if self._rename_controls is None:
                return self._text(job_id, "Unavailable.", kind=feedback)
            status, row, bind = await self._rename_controls.handle(request)
            if row is None:
                text = {
                    "missing": "No rename batch.", "busy": "Undo the previous rename first.",
                    "full": "Rename queue full.", "expired": "Panel expired.",
                }.get(status, "Unavailable.")
                return self._text(job_id, text, kind=feedback)
            panel = rename_page(row, await self._rename_controls.store.counts(row))
            if not bind and not confirming:
                panel = PresentationPage(description=panel.description)
            return self._page(
                job_id,
                DiscordIntent(
                    "edit" if confirming else "send",
                    pages=(panel,),
                    session_id=row["token"] if bind or confirming else None,
                ),
            )
        except (ValueError, MySQLError, OSError, RuntimeError, TimeoutError, RedisError):
            return self._text(job_id, "Unavailable.", kind=feedback)

    async def _save_session(self, session):
        remaining = session["expires"] - self._clock()
        if remaining <= 0:
            raise ModerationError("Panel expired.")
        await self._cache.set(SESSION_PREFIX + session["token"], json.dumps(session), px=remaining)

    @staticmethod
    def _control(s, action, label, **kwargs):
        return PresentationControl(
            "button", "moderation", action, f"{s['token']}~{s['revision']}", label=label, **kwargs
        )

    async def _history(self, request, job_id):
        token = request.arguments.get("session")
        if (
            self._warnings is None or not self._admitted(request, "warnings")
            or request.event.get("dispatch") != "component"
            or "_access_checkpoint" not in request.event
            or not isinstance(token, str)
            or len(token) != 16
            or any(c not in "0123456789abcdef" for c in token)
        ):
            return self._text(job_id, "Panel expired.", kind="ephemeral")
        try:
            async with self._cache.lock(
                SESSION_PREFIX + token + ":lock", timeout=30, blocking_timeout=1
            ):
                raw = await self._cache.get(SESSION_PREFIX + token)
                s = json.loads(raw) if raw else None
                e = request.event
                bound = await self._cache.get(ui_key("message", token))
                if (
                    not s
                    or s["expires"] <= self._clock()
                    or s["revision"] != request.arguments.get("revision")
                    or (s["guild"], s["owner"], s["channel"])
                    != (e.get("guild_id"), e.get("actor_id"), e.get("channel_id"))
                    or not bound
                    or int(bound) != e.get("message_id")
                ):
                    return self._text(job_id, "Panel expired.", kind="ephemeral")
                action = request.route.rpartition(".")[2]
                if action not in {"warnings_open", "warnings_previous", "warnings_next"}:
                    return self._text(job_id, "Panel expired.", kind="ephemeral")
                opening = action == "warnings_open"
                if opening != (s["screen"] == "opener"):
                    return self._text(job_id, "Panel expired.", kind="ephemeral")
                page = max(
                    1,
                    s["page"]
                    + {"warnings_open": 0, "warnings_previous": -1, "warnings_next": 1}[action],
                )
                result = await self._warnings.history(s["guild"], s["target"], page)
                if opening:
                    s = {**s, "token": secrets.token_hex(8), "screen": "private", "revision": 0}
                s.update(page=result["page"], revision=s["revision"] + 1)
                await self._save_session(s)
                lines = []
                for row in result["warnings"]:
                    state = "Removed" if row["revoked"] else "Active"
                    detail = escape_embed_text(row["reason"])
                    if len(detail) > 600:
                        detail = detail[:599].rstrip("\\") + "…"
                    lines.append(
                        f"{row['id']} · {state} · <@{row['moderator']}> · "
                        f"<t:{row['created'] // 1000}:d>\n" + detail
                    )
                    if row["revoked"]:
                        removed = row["revoked"]
                        lines[-1] += (
                            f"\nRemoved by <@{removed['actor']}> · <t:{removed['at'] // 1000}:d>"
                        )
                controls = (
                    self._control(s, "warnings_previous", "Previous", disabled=s["page"] <= 1),
                    self._control(
                        s, "warnings_next", "Next", disabled=s["page"] >= result["pages"]
                    ),
                )
                return self._page(
                    job_id,
                    DiscordIntent(
                        "ephemeral" if opening else "edit",
                        session_id=s["token"],
                        pages=(
                            PresentationPage(
                                title=f"Warnings · <@{s['target']}>",
                                description="\n\n".join(lines) or "No warnings.",
                                footer=f"{s['page']}/{result['pages']}",
                                controls=controls,
                            ),
                        ),
                    ),
                )
        except (MySQLError, TimeoutError, RedisError, ValueError, KeyError):
            return self._text(job_id, "Unavailable.", kind="ephemeral")

    def _role_editor(self):
        if self._warnings is None:
            raise ActionUnavailable("Role store unavailable")
        return RoleEditor(
            self._actions, self._clock, cache=self._cache, queue=self._queue, store=self._warnings,
            group=self._group, generation=self._generation,
        )

    def _page(self, job, intent, *, revision=None):
        return ResultEnvelope(
            1, job, "moderation", self._group, self._generation, "ok", intents=(intent,),
            response_revision=revision,
        )

    def _success(self, job_id: str, display_name: str, reason: str) -> ResultEnvelope:
        now_ms = int(time.time() * 1_000)
        blocks = [
            PresentationBlock(kind="media", media_url=MODERATION_ACTION_GIF_URL),
            PresentationBlock(
                kind="text",
                text=f"{escape_embed_text(display_name)} was banned",
            ),
        ]
        if reason:
            blocks.append(PresentationBlock(kind="text", text=escape_embed_text(reason)))
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="moderation",
            group=self._group,
            generation=self._generation,
            status="ok",
            intents=(
                DiscordIntent(
                    kind="send",
                    pages=(
                        PresentationPage(
                            blocks=tuple(blocks),
                        ),
                    ),
                    presentation_mode="v2",
                ),
            ),
            completed_at_ms=now_ms,
        )

    def _text(
        self, job_id: str, text: str, *, error_code: str | None = None, kind="send"
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="moderation",
            group=self._group,
            generation=self._generation,
            status="ok" if error_code is None else "error",
            intents=(DiscordIntent(kind=kind, pages=(PresentationPage(description=text),)),),
            error_code=error_code,
            completed_at_ms=int(time.time() * 1_000),
        )

    async def start(self):
        if self._purge_controls is not None:
            await self._purge_controls.recover_unprepared()
        if self._rename_runner is not None:
            self._rename_runner.start()
        if self._purge_runner is not None:
            self._purge_runner.start()
        if self._reset_runner is not None:
            self._reset_runner.start()
        if self._warnings is not None or self._rename_runner is not None or self._purge_runner is not None:
            self._retention_task = asyncio.create_task(
                self._maintain_operations(), name="moderation-retention",
            )

    def begin_drain(self):
        self._draining = True
        if self._rename_runner is not None:
            self._rename_runner.draining = True
        if self._purge_runner is not None:
            self._purge_runner.draining = True
        if self._reset_runner is not None:
            self._reset_runner.draining = True

    def resume(self):
        self._draining = False
        if self._rename_runner is not None:
            self._rename_runner.draining = False
            self._rename_runner.wake.set()
        if self._purge_runner is not None:
            self._purge_runner.draining = False
            self._purge_runner.wake.set()
        if self._reset_runner is not None:
            self._reset_runner.draining = False
            self._reset_runner.wake.set()

    def drain_ready(self):
        return not self._retention_active and not any(
            runner and runner.active for runner in (self._rename_runner, self._purge_runner, self._reset_runner)
        )

    async def _clean_operations(self):
        if self._draining:
            return
        self._retention_active = True
        try:
            now = self._clock()
            for store in (
                self._warnings, getattr(self._rename_controls, "store", None),
                getattr(self._purge_runner, "store", None), getattr(self._purge_controls, "handovers", None),
            ):
                if store is not None:
                    try:
                        await store.cleanup(now)
                    except (MySQLError, OSError, RuntimeError, TimeoutError):
                        LOGGER.warning("moderation_retention_unavailable")
        finally:
            self._retention_active = False

    async def _maintain_operations(self):
        while True:
            if not self._draining and self._warnings is not None:
                self._retention_active = True
                try:
                    await self._role_editor().recover_stopped()
                except (MySQLError, OSError, RedisError, RuntimeError, TimeoutError, ValueError):
                    LOGGER.warning("moderation_role_edit_unavailable")
                finally:
                    self._retention_active = False
            await asyncio.sleep(3600)
            await self._clean_operations()

    async def close(self) -> None:
        if self._rename_runner is not None:
            await self._rename_runner.close()
        if self._purge_runner is not None:
            await self._purge_runner.close()
        if self._reset_runner is not None:
            await self._reset_runner.close()
        if self._retention_task is not None:
            self._retention_task.cancel()
            await asyncio.gather(self._retention_task, return_exceptions=True)
