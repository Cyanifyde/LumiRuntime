import asyncio
import hashlib
import re
import time

import discord
import msgspec

from lumi.contracts import decode, encode
from lumi.contracts.role_access import (
    MAX_ACTOR_ROLES,
    READ_ACTOR_ROLES,
    READ_GUILD_ROLES,
    ROLE_FACT_PAGE,
    SET_ROLE_MEMBERSHIP,
    WRITE_ACTOR_ROLES,
    RoleFact,
    RoleTargetBinding,
    actor_role_attempt,
    role_target_key,
    role_write_key,
)
from lumi.shared.request_scope import memo
from lumi.ui_keys import read_ui

from .admission import admitted, declared_scope
from .members import fetch_member, guild_roles, lookup, member_mutation


class ScopeError(ValueError):
    pass


BASE_FIELDS = frozenset(
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
        "message_id",
        "member_id",
    }
)


def member_roles(member):
    raw = getattr(member, "_roles", None)
    return set(raw) if raw is not None else {role.id for role in member.roles}


def membership(member):
    value = getattr(member, "joined_at", None)
    return value.isoformat() if value else None


def snowflake(value):
    return type(value) is int and 0 < value < 2**64


def actor_role_scope(snapshot, job, action):
    key = id(snapshot), getattr(snapshot, "revision", 0), id(job), id(action)
    return memo("native-actor-role-scope", key, lambda: _actor_role_scope(snapshot, job, action))


def _actor_role_scope(snapshot, job, action):
    if (
        job.request.event.get("dispatch") != "component"
        or declared_scope(snapshot, job, "actor_role_access") is not True
        or not admitted(job)
        or (job.job_id, job.module, job.group, job.generation, job.guild_id, job.channel_id,
            job.actor_id, job.request.event.get("message_id")) != (
                action.job_id, action.module, action.group, action.generation, action.guild_id,
                action.channel_id, action.member_id, action.message_id,
            )
        or not all(snowflake(value) for value in (
            action.guild_id, action.channel_id, action.member_id, action.message_id,
        ))
    ):
        return False
    source = job.request.event.get("_component_message")
    if (
        not isinstance(source, dict)
        or source.get("current_lumi") is not True or source.get("webhook") is not False
        or (source.get("guild_id"), source.get("channel_id"), source.get("message_id"))
        != (action.guild_id, action.channel_id, action.message_id)
    ):
        return False
    patch = action.actor_role_changes
    if action.kind == READ_ACTOR_ROLES:
        return patch is None and msgspec.to_builtins(action).keys() <= BASE_FIELDS
    if action.kind != WRITE_ACTOR_ROLES or patch is None:
        return False
    roles = set(patch.required)
    changes = {item.role for item in patch.changes}
    return (
        len(roles) == len(patch.required) <= MAX_ACTOR_ROLES
        and len(changes) == len(patch.changes) and changes <= roles
        and all(snowflake(role) for role in roles)
        and re.fullmatch(
            re.escape(actor_role_attempt(job.job_id, "")) + r"[0-9]+", action.idempotency_key,
        ) is not None
        and msgspec.to_builtins(action).keys() <= (BASE_FIELDS | {"actor_role_changes"})
    )


async def execute_actor_roles(edge, action, job, guild):
    applied = []

    def result(status, code=None, **data):
        if action.kind == WRITE_ACTOR_ROLES:
            data["applied"] = applied
            data["joined"] = action.actor_role_changes.joined if action.actor_role_changes else None
        return edge._action_result(action, code is None, status, code, data=data)

    if (
        guild.id != job.guild_id or getattr(guild, "unavailable", False)
        or edge.control.maintenance or edge.blocklist.contains(job.actor_id)
        or not actor_role_scope(edge.registry.snapshot, job, action)
    ):
        return result("rejected", "action_scope_mismatch")
    if action.kind == READ_ACTOR_ROLES:
        try:
            member = await fetch_member(edge, guild, job.actor_id, fresh=True)
        except (discord.HTTPException, TimeoutError, OSError):
            return result("rejected", "member_unavailable")
        assigned = sorted(member_roles(member) - {guild.id})
        if len(assigned) > MAX_ACTOR_ROLES:
            return result("rejected", "role_unavailable")
        return result("observed", assigned=assigned, joined=membership(member))

    patch = action.actor_role_changes
    cache = lookup(edge)
    member = cache.cached(guild, job.actor_id)
    if cache.departed(guild, job.actor_id) or (
        member is not None and membership(member) != patch.joined
    ):
        return result("rejected", "member_changed")
    me = guild.me
    if me is None or not me.guild_permissions.manage_roles:
        return result("rejected", "role_unavailable")
    roles = [guild.get_role(role_id) for role_id in patch.required]
    if any(
        role is None or role.is_default() or role.managed or me.top_role <= role for role in roles
    ):
        return result("rejected", "role_unavailable")
    if not patch.changes:
        return result("unchanged")
    audit = f"Lumi role · {job.job_id[:12]}"
    try:
        with member_mutation(edge, guild, job.actor_id):
            for change in patch.changes:
                if time.time_ns() // 1_000_000 >= min(action.expires_at_ms, job.expires_at_ms):
                    return result("rejected", "expired_job")
                write = edge.http.add_role if change.present else edge.http.remove_role
                await write(guild.id, job.actor_id, change.role, reason=audit)
                applied.append({"role": change.role, "present": change.present})
    except discord.RateLimited:
        return result("rejected", "rate_limited")
    except (discord.NotFound, discord.Forbidden):
        return result("rejected", "discord_rejected")
    except discord.HTTPException as exc:
        if exc.status < 500:
            return result("rejected", "discord_rejected")
        return result("uncertain", "discord_unavailable")
    except (TimeoutError, OSError):
        return result("uncertain", "discord_unavailable")
    return result("applied")


def role_scope(snapshot, job, action):
    key = id(snapshot), getattr(snapshot, "revision", 0), id(job), id(action)
    return memo("native-role-scope", key, lambda: _role_scope(snapshot, job, action))


def _role_scope(snapshot, job, action):
    spec = declared_scope(snapshot, job, "role_access")
    if (
        spec is None
        or not admitted(job)
        or not snowflake(action.member_id)
        or (job.module, job.group, job.job_id, job.generation, job.guild_id, job.channel_id)
        != (
            action.module,
            action.group,
            action.job_id,
            action.generation,
            action.guild_id,
            action.channel_id,
        )
        or action.message_id != job.request.event.get("message_id")
        or not snowflake(action.message_id)
    ):
        return None
    if spec.target_fields:
        selected = [job.request.arguments.get(field) for field in spec.target_fields]
        if {value for value in selected if snowflake(value)} != {action.member_id}:
            return None
    if action.kind == READ_GUILD_ROLES:
        if (
            action.role_facts is None
            or action.role_membership is not None
            or action.role_id is not None
        ):
            return None
        allowed = BASE_FIELDS | {"role_facts"}
    elif action.kind == SET_ROLE_MEMBERSHIP:
        if (
            not spec.allow_write
            or action.role_facts is not None
            or action.role_membership is None
            or not snowflake(action.role_id)
            or action.role_membership.before == action.role_membership.present
            or re.fullmatch(
                re.escape(role_write_key(job.job_id, action.role_id, "")) + r"[0-9]+",
                action.idempotency_key,
            )
            is None
        ):
            return None
        allowed = BASE_FIELDS | {"role_id", "role_membership"}
    else:
        return None
    return spec if msgspec.to_builtins(action).keys() <= allowed else None


async def authorize_target(edge, job, spec, target, now):
    if spec.target_fields:
        return job.request.event.get("dispatch") == "command"
    token = job.request.arguments.get("session")
    if not isinstance(token, str) or not re.fullmatch(r"[a-f0-9]{16}", token):
        return False
    raw, bound = await asyncio.gather(
        edge.cache_redis.get(role_target_key(job.group, token)),
        read_ui(edge.cache_redis, "message", token, job.group),
    )
    binding = decode(raw, RoleTargetBinding) if raw else None
    return bool(
        binding
        and bound
        and int(bound) == job.request.event.get("message_id")
        and (binding.module, binding.guild, binding.channel, binding.actor, binding.target)
        == (job.module, job.guild_id, job.channel_id, job.actor_id, target)
        and now() < binding.expires
    )


async def native_authority(edge, guild, job, target_id, *, fresh=False):
    channel = guild.get_channel_or_thread(job.channel_id)
    me = guild.me
    if guild.unavailable or channel is None or me is None:
        raise ScopeError("guild_unavailable")
    member_cache = lookup(edge)
    epoch = member_cache.epoch
    if job.actor_id == target_id:
        actor, roles = await asyncio.gather(
            fetch_member(edge, guild, target_id, fresh=fresh),
            guild_roles(edge, guild),
        )
        target = actor
    else:
        actor, target, roles = await asyncio.gather(
            fetch_member(edge, guild, job.actor_id),
            fetch_member(edge, guild, target_id, fresh=fresh),
            guild_roles(edge, guild),
        )
    if not actor or not target:
        raise ScopeError("member_unavailable")
    if member_cache.epoch != epoch and member_cache.cached(guild, job.actor_id) is not actor:
        raise ScopeError("permission_denied")
    roles = {role.id: role for role in roles}
    default = roles.get(guild.id)
    if default is None:
        raise ScopeError("role_unavailable")

    def permissions(member):
        bits = default.permissions.value
        for role in member_roles(member):
            if role in roles:
                bits |= roles[role].permissions.value
        return (
            discord.Permissions.all() if member.id == guild.owner_id else discord.Permissions(bits)
        )

    actor_perm, bot_perm = permissions(actor), permissions(me)
    if not (actor_perm.administrator or actor_perm.manage_roles):
        raise ScopeError("permission_denied")
    if not (bot_perm.administrator or bot_perm.manage_roles):
        raise ScopeError("bot_permission_denied")
    channels, categories = edge._channel_access(channel)
    if not channel.permissions_for(actor).view_channel or not edge.policy.allowed(
        guild_id=guild.id,
        module=job.module,
        acl_key=job.request.event["_acl_key"],
        public=job.request.event["_public"],
        role_ids=frozenset(member_roles(actor)),
        administrator=actor_perm.administrator,
        channel_ids=channels,
        category_ids=categories,
    ):
        raise ScopeError("policy_denied")

    def highest(member):
        return max((roles[rid] for rid in member_roles(member) if rid in roles), default=default)

    actor_top, bot_top = highest(actor), highest(me)

    def manageable(role):
        return (
            not role.is_default()
            and not role.managed
            and bot_top > role
            and (actor.id == guild.owner_id or actor_top > role)
        )

    return target, roles, manageable


async def execute(edge, action, job, guild, *, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)

    def failed(code, **data):
        return edge._action_result(action, False, "failed", code, data=data)

    def live():
        return (
            now() < min(action.expires_at_ms, job.expires_at_ms)
            and not edge.control.maintenance
            and not edge.blocklist.contains(job.actor_id)
            and role_scope(edge.registry.snapshot, job, action) is not None
        )

    spec = role_scope(edge.registry.snapshot, job, action)
    if spec is None or guild.id != job.guild_id or not live():
        return failed("action_scope_mismatch")
    try:
        if not await authorize_target(edge, job, spec, action.member_id, now):
            return failed("action_scope_mismatch")
        fresh = not action.role_facts or action.role_facts.refresh_target
        target, roles, manageable = await native_authority(
            edge,
            guild,
            job,
            action.member_id,
            fresh=fresh,
        )
        if not live():
            return failed("expired_job")
        assigned = member_roles(target) - {guild.id}
        if query := action.role_facts:
            facts = [
                RoleFact(role.id, role.name[:100], manageable(role))
                for role in sorted(
                    roles.values(),
                    reverse=True,
                )
            ]
            current = dict(joined=membership(target), assigned=sorted(assigned))
            revision = hashlib.sha256(encode(current) + msgspec.json.encode(facts)).hexdigest()
            i = query.page * ROLE_FACT_PAGE
            page = facts[i : i + ROLE_FACT_PAGE]
            return edge._action_result(
                action,
                True,
                "observed",
                data={
                    **current,
                    "revision": revision,
                    "roles": msgspec.to_builtins(page),
                    "more": i + ROLE_FACT_PAGE < len(facts),
                },
            )
        write = action.role_membership
        role = roles.get(action.role_id)
        if role is None or not manageable(role):
            return failed("role_unavailable")
        if membership(target) != write.joined:
            return failed("member_changed")
        if (role.id in assigned) != write.before:
            return failed("role_changed")
        audit = f"Lumi role by {job.actor_id} · {job.job_id[:12]}"
        with member_mutation(edge, guild, target.id):
            change = target.add_roles if write.present else target.remove_roles
            await change(role, reason=audit, atomic=True)
        return edge._action_result(action, True, "added" if write.present else "removed")
    except ScopeError as exc:
        return failed(str(exc))
    except discord.RateLimited as exc:
        return failed("rate_limited", retry_after_ms=max(1, int(exc.retry_after * 1000)))
    except discord.NotFound:
        return failed("member_unavailable")
    except discord.Forbidden:
        return failed("permission_denied")
    except discord.HTTPException as exc:
        return failed("uncertain" if exc.status >= 500 else "discord_rejected")
    except (TimeoutError, OSError):
        return failed("uncertain")
    except (ValueError, TypeError):
        return failed("invalid_action")
