import asyncio
import re
import time

import discord
import msgspec

from lumi.contracts import DiscordEffectGrant, decode
from lumi.contracts.member_profiles import (
    LIST_GUILD_MEMBERS,
    MEMBER_PAGE_SIZE,
    NICKNAME_WIRE_LIMIT,
    READ_MEMBER_PROFILE,
    SET_MEMBER_NICKNAME,
)
from lumi.contracts.native_mutations import EditNickname, nickname_audit_reason, nickname_change
from lumi.shared.discord_effects import effect_key, member_operation_lane, source_digest
from lumi.shared.request_scope import memo

from .admission import declared_scope
from .effect_grants import ApprovedEffect, claim, snowflake
from .gateway_capture import record_mutation
from .members import fetch_member, lookup, member_mutation

_BASE = frozenset(
    {
        "schema",
        "action_id",
        "job_id",
        "module",
        "group",
        "generation",
        "expires_at_ms",
        "kind",
        "guild_id",
        "idempotency_key",
        "channel_id",
    }
)


class Denied(Exception):
    pass


def profile_scope(snapshot, job, action):
    return memo(
        "native-profile-scope",
        (id(snapshot), id(job), id(action)),
        lambda: _profile_scope(snapshot, job, action),
    )


def _profile_scope(snapshot, job, action):
    spec = declared_scope(snapshot, job, "member_profiles", actor_callback=True)
    if (
        spec is None
        or not snowflake(job.actor_id)
        or not snowflake(job.channel_id)
        or (
            action.job_id,
            action.module,
            action.group,
            action.generation,
            action.guild_id,
            action.channel_id,
        )
        != (job.job_id, job.module, job.group, job.generation, job.guild_id, job.channel_id)
        or job.request.event.get("dispatch") != "scheduled"
        or (job.request.event.get("actor_id"), job.request.event.get("channel_id"))
        != (job.actor_id, job.channel_id)
    ):
        return None
    fields = msgspec.to_builtins(action).keys()
    if action.kind == LIST_GUILD_MEMBERS:
        after = action.member_directory_after
        valid = (
            spec.allow_directory
            and type(after) is int
            and (after == 0 or snowflake(after))
            and fields <= _BASE | {"member_directory_after"}
        )
    elif action.kind == READ_MEMBER_PROFILE:
        valid = snowflake(action.member_id) and fields <= _BASE | {
            "member_id",
            "refresh_member_profile",
        }
    elif action.kind == SET_MEMBER_NICKNAME:
        mutation = action.nickname_mutation
        valid = (
            spec.allow_nickname_write
            and snowflake(action.member_id)
            and mutation is not None
            and mutation.nickname != mutation.before
            and len(mutation.joined_at) <= 40
            and bool(mutation.joined_at)
            and all(
                nick is None or len(nick) <= NICKNAME_WIRE_LIMIT
                for nick in (mutation.before, mutation.nickname)
            )
            and re.fullmatch(r"[a-f0-9]{8}", mutation.operation_id)
            and re.fullmatch(r"effect:[a-f0-9]{32}", action.idempotency_key)
            and fields <= _BASE | {"member_id", "nickname_mutation"}
        )
    else:
        valid = False
    return spec if valid else None


def joined_at(member):
    return member.joined_at.isoformat() if member.joined_at is not None else None


async def authority(edge, job, guild, spec, *, fresh=False):
    channel = guild.get_channel_or_thread(job.channel_id)
    bot = guild.me
    if (
        getattr(guild, "unavailable", False)
        or channel is None
        or bot is None
        or channel.guild.id != guild.id
    ):
        raise Denied("guild_unavailable")
    if spec.owner_only and edge.owner_id != job.actor_id:
        raise Denied("permission_denied")
    actor = await fetch_member(edge, guild, job.actor_id, fresh=fresh)
    if (
        actor is None
        or actor.bot
        or not channel.permissions_for(actor).view_channel
        or not all(getattr(actor.guild_permissions, flag, False) for flag in spec.actor_permissions)
    ):
        raise Denied("permission_denied")
    if not all(getattr(bot.guild_permissions, flag, False) for flag in spec.bot_permissions):
        raise Denied("bot_permission_denied")
    roles, administrator = edge._member_access(actor)
    channels, categories = edge._channel_access(channel)
    if not edge.policy.allowed(
        guild_id=guild.id,
        module=job.module,
        acl_key=job.request.event.get("_acl_key", ""),
        public=True,
        role_ids=roles,
        administrator=administrator,
        channel_ids=channels,
        category_ids=categories,
    ):
        raise Denied("policy_denied")
    return actor, bot, channel


def manageable(guild, actor, bot, target):
    return (
        target.id not in {guild.owner_id, bot.id}
        and bot.top_role > target.top_role
        and (
            target.id == actor.id or actor.id == guild.owner_id or actor.top_role > target.top_role
        )
    )


async def mutation_grant(edge, action, job, clock):
    key = effect_key(job.group, job.job_id, action.idempotency_key)
    raw = await edge.queue_redis.get(key)
    grant = decode(raw, DiscordEffectGrant) if raw else None
    if grant is None or (
        grant.module,
        grant.generation,
        grant.kind,
        grant.guild_id,
        grant.channel_id,
        grant.message_id,
        grant.digest,
        grant.source_digest,
        grant.mutation_digest,
        grant.delivery,
    ) != (
        job.module,
        job.generation,
        SET_MEMBER_NICKNAME,
        job.guild_id,
        job.channel_id,
        None,
        source_digest((action.member_id, action.nickname_mutation)),
        None,
        None,
        None,
    ):
        raise Denied("effect_not_authorized")
    deadline = min(grant.expires_at_ms, action.expires_at_ms, job.expires_at_ms)
    if deadline <= clock():
        raise Denied("expired_job")
    return key, raw, deadline


async def execute(edge, action, job, guild, *, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)
    snapshot = edge.registry.snapshot
    spec = profile_scope(snapshot, job, action)
    expires = min(job.expires_at_ms, action.expires_at_ms)

    def failed(code, **data):
        return edge._action_result(action, False, "failed", code, data=data)

    def live():
        accepted = edge.registry.snapshot
        return (
            now() < expires
            and not edge.control.maintenance
            and not edge.blocklist.contains(job.actor_id)
            and (accepted is snapshot or profile_scope(accepted, job, action) == spec)
        )

    if not spec or guild.id != job.guild_id or not live():
        return failed("action_scope_mismatch")
    try:
        async with asyncio.timeout(max(0.001, (expires - now()) / 1000)):
            watch_users = {job.actor_id, action.member_id, getattr(guild.me, "id", None)}
            policy_epoch = getattr(edge.policy, "_epoch", 0)
            with lookup(edge).observe(guild.id, watch_users) as changed:
                return await execute_scoped(
                    edge,
                    action,
                    job,
                    guild,
                    spec,
                    now,
                    expires,
                    live,
                    failed,
                    changed,
                    policy_epoch,
                )
    except Denied as exc:
        return failed(str(exc))
    except discord.RateLimited as exc:
        return failed("rate_limited", retry_after_ms=max(1, int(exc.retry_after * 1000)))
    except discord.NotFound:
        return failed("member_unavailable")
    except discord.Forbidden:
        return failed("permission_denied")
    except discord.HTTPException as exc:
        return failed(
            "uncertain" if exc.status >= 500 else "discord_rejected", http_status=exc.status
        )
    except (OSError, TimeoutError):
        return failed("uncertain")
    except (ValueError, TypeError):
        return failed("invalid_action")


async def execute_scoped(
    edge, action, job, guild, spec, now, expires, live, failed, changed, policy_epoch
):
    def current():
        return live() and not changed[0] and getattr(edge.policy, "_epoch", 0) == policy_epoch

    ingress = getattr(edge, "private_ingress", None)

    def disclosed(member):
        return ingress is None or not ingress.gate.blocks_identity(member.id)

    key, raw, deadline = (None, None, expires)
    if action.kind == SET_MEMBER_NICKNAME:
        key, raw, deadline = await mutation_grant(edge, action, job, now)
    before = action.nickname_mutation
    fresh = before is not None or action.refresh_member_profile
    actor, bot, channel = await authority(
        edge,
        job,
        guild,
        spec,
        fresh=fresh and action.member_id == job.actor_id,
    )
    if action.kind == LIST_GUILD_MEMBERS:
        page = [
            member
            async for member in guild.fetch_members(
                limit=MEMBER_PAGE_SIZE,
                after=discord.Object(id=action.member_directory_after),
            )
        ]
        if not current():
            return failed("access_changed")
        return edge._action_result(
            action,
            True,
            "observed",
            data={
                "members": [(member.id, joined_at(member)) for member in page if disclosed(member)],
                "cursor": max(
                    (member.id for member in page), default=action.member_directory_after
                ),
                "more": len(page) == MEMBER_PAGE_SIZE,
            },
        )

    target = (
        actor
        if action.member_id == actor.id
        else await fetch_member(
            edge,
            guild,
            action.member_id,
            fresh=fresh,
        )
    )
    if not live() or now() >= deadline:
        return failed("expired_job")
    if target is None or not disclosed(target):
        return failed("member_unavailable")
    if not current():
        return failed("access_changed")
    if before is None:
        return edge._action_result(
            action,
            True,
            "observed",
            data={
                "joined_at": joined_at(target),
                "nickname": target.nick,
                "manageable": manageable(guild, actor, bot, target),
            },
        )
    if not manageable(guild, actor, bot, target):
        return failed("target_protected")
    if joined_at(target) != before.joined_at:
        return failed("member_changed")
    if target.nick != before.before:
        return failed("value_changed")
    lane = member_operation_lane(job.group, job.module, job.guild_id, before.operation_id)
    if not await claim(edge, ApprovedEffect(channel, key, raw, deadline), lane=lane):
        return failed("effect_not_authorized")
    if not current() or now() >= deadline or not disclosed(target):
        return failed("access_changed")
    await record_mutation(
        edge,
        EditNickname(
            guild.id,
            actor.id,
            before.operation_id,
            target.id,
            nickname_change(before.before, before.nickname),
        ),
    )
    if not current() or now() >= deadline or not disclosed(target):
        return failed("access_changed")
    with member_mutation(edge, guild, target.id):
        result = await target.edit(
            nick=before.nickname, reason=nickname_audit_reason(before.operation_id)
        )
    return edge._action_result(action, True, "updated", data={"nickname": result.nick})
