import time
import unicodedata
from datetime import UTC, datetime

import discord
import msgspec

from lumi.contracts.member_effects import (
    BAN_MEMBER,
    CLEAR_MEMBER_TIMEOUT,
    KICK_MEMBER,
    MEMBER_EFFECT_PERMISSIONS,
    SET_MEMBER_TIMEOUT,
    TIMEOUT_MAX_MS,
    UNBAN_MEMBER,
    member_effect_key,
)
from lumi.shared.request_scope import memo

from .admission import admitted, declared_scope
from .members import fetch_member, lookup, member_mutation

AUDIT_VERBS = {
    BAN_MEMBER: "ban",
    KICK_MEMBER: "kick",
    UNBAN_MEMBER: "unban",
    SET_MEMBER_TIMEOUT: "timeout",
    CLEAR_MEMBER_TIMEOUT: "untimeout",
}
ACTION_FIELDS = frozenset({
    "schema", "action_id", "job_id", "module", "group", "generation", "expires_at_ms", "kind",
    "guild_id", "idempotency_key", "channel_id", "message_id", "member_id", "reason",
    "timeout_until_ms",
})


def scope_matches(snapshot, job, action):
    key = id(snapshot), getattr(snapshot, "revision", 0), id(job), id(action)
    return memo(
        "member-effect-scope", key, lambda: _scope_matches(snapshot, job, action),
    )


def _scope_matches(snapshot, job, action):
    if job.request.event.get("dispatch") != "command":
        return False
    spec = declared_scope(snapshot, job, "member_effect")
    args = job.request.arguments
    if spec is None or spec.kind != action.kind:
        return False
    targets = {args[field] for field in spec.target_fields if args.get(field) is not None}
    raw_reason = args.get(spec.reason_field) if spec.reason_field else None
    return (
        admitted(job) and job.generation == action.generation
        and (job.module, job.group, job.job_id, job.guild_id, job.channel_id)
        == (action.module, action.group, action.job_id, action.guild_id, action.channel_id)
        and action.idempotency_key == member_effect_key(job.job_id)
        and type(action.member_id) is int and 0 < action.member_id < 2**64
        and targets == {action.member_id}
        and action.message_id == job.request.event.get("message_id")
        and type(action.message_id) is int and 0 < action.message_id < 2**64
        and action.reason == raw_reason
        and (raw_reason is None or (isinstance(raw_reason, str) and len(raw_reason) <= 400))
        and (action.timeout_until_ms is not None) == (action.kind == SET_MEMBER_TIMEOUT)
        and msgspec.to_builtins(action).keys() <= ACTION_FIELDS
    )


def audit_reason(action, actor):
    raw = unicodedata.normalize("NFKC", action.reason or "")
    cleaned = " ".join("".join(
        c for c in raw if c.isspace() or unicodedata.category(c) not in {"Cc", "Cf", "Cs"}
    ).split())[:400]
    return f"Lumi {AUDIT_VERBS[action.kind]} by {actor}: {cleaned or 'No reason provided.'}"


async def execute(edge, action, job, guild, *, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)

    def failure(code):
        return edge._action_result(action, False, "failed", code)

    def current():
        route = edge.registry.snapshot.modules.get(job.module)
        return (
            route is not None and route.manifest.generation == job.generation
            and route.manifest.group == job.group
            and now() < min(action.expires_at_ms, job.expires_at_ms)
            and not edge.control.maintenance and not edge.blocklist.contains(job.actor_id)
        )

    if guild.id != job.guild_id or not scope_matches(edge.registry.snapshot, job, action):
        return failure("action_scope_mismatch")
    if not current():
        return failure("action_scope_mismatch")
    kind, target_id = action.kind, action.member_id
    members = lookup(edge)
    epoch = members.epoch
    try:
        actor = await fetch_member(edge, guild, job.actor_id)
    except discord.NotFound:
        return failure("permission_denied")
    except discord.HTTPException:
        return failure("permission_denied")

    me = guild.me
    channel = guild.get_channel_or_thread(job.channel_id)
    permission = MEMBER_EFFECT_PERMISSIONS[kind]
    if (
        not actor or not me or not channel or actor.id != job.actor_id or actor.bot
        or channel.guild.id != guild.id or not channel.permissions_for(actor).view_channel
    ):
        return failure("permission_denied")
    admin = actor.guild_permissions.administrator
    channels, categories = edge._channel_access(channel)
    if not edge.policy.allowed(
        guild_id=guild.id, module=job.module, acl_key=job.request.event["_acl_key"],
        public=job.request.event["_public"], role_ids=frozenset(r.id for r in actor.roles),
        administrator=admin, channel_ids=channels, category_ids=categories,
    ):
        return failure("policy_denied")
    if not (admin or getattr(actor.guild_permissions, permission)) or not getattr(
        me.guild_permissions, permission,
    ):
        return failure("permission_denied")
    if kind != UNBAN_MEMBER and target_id in {actor.id, me.id, guild.owner_id}:
        return failure("target_protected")
    target = None
    if kind != UNBAN_MEMBER:
        try:
            target = await fetch_member(edge, guild, target_id)
        except discord.NotFound:
            pass
        except discord.HTTPException:
            return failure("discord_rejected")
    if kind not in {BAN_MEMBER, UNBAN_MEMBER} and target is None:
        return failure("member_unavailable")
    if members.epoch != epoch and members.cached(guild, actor.id) is not actor:


        return failure("permission_denied")
    if target is not None and (
        target.id != target_id
        or (actor.id != guild.owner_id and actor.top_role <= target.top_role)
        or me.top_role <= target.top_role
        or (kind in {SET_MEMBER_TIMEOUT, CLEAR_MEMBER_TIMEOUT}
            and target.guild_permissions.administrator)
    ):
        return failure("target_protected")

    until = action.timeout_until_ms
    if until is not None and (type(until) is not int or not 0 < until - now() <= TIMEOUT_MAX_MS):
        return failure("action_scope_mismatch")
    if not current():
        return failure("action_scope_mismatch")
    previous = getattr(target, "timed_out_until", None)
    data = dict(
        before_until=int(previous.timestamp() * 1000) if previous is not None else 0,
        display_name=str(getattr(target, "display_name", None) or f"User {target_id}")[:80],
    )
    if kind == CLEAR_MEMBER_TIMEOUT and data["before_until"] <= now():
        return edge._action_result(action, True, "not_timed_out")
    audit = audit_reason(action, actor.id)
    try:
        with member_mutation(edge, guild, target_id):
            if kind == UNBAN_MEMBER:
                await guild.unban(discord.Object(id=target_id), reason=audit)
                status = "unbanned"
            elif kind == BAN_MEMBER:
                await guild.ban(
                    target or discord.Object(id=target_id), reason=audit, delete_message_seconds=0,
                )
                status = "banned"
            elif kind == KICK_MEMBER:
                await target.kick(reason=audit)
                status = "kicked"
            elif kind == SET_MEMBER_TIMEOUT:
                await target.timeout(datetime.fromtimestamp(until / 1000, UTC), reason=audit)
                status = "timed_out"
                data["until"] = until
            else:
                await target.timeout(None, reason=audit)
                status = "timeout_removed"
    except discord.NotFound as exc:
        if kind == UNBAN_MEMBER and exc.code == 10026:
            return edge._action_result(action, True, "not_banned")
        if kind == BAN_MEMBER and exc.code == 10013:
            return failure("target_protected")
        if kind in {BAN_MEMBER, UNBAN_MEMBER}:
            return failure("discord_rejected")
        return failure("member_unavailable")
    except discord.Forbidden:
        return failure("permission_denied")
    except discord.HTTPException as exc:
        return failure("uncertain" if exc.status >= 500 else "discord_rejected")
    except (TimeoutError, OSError):
        return failure("uncertain")
    return edge._action_result(action, True, status, data=data)
