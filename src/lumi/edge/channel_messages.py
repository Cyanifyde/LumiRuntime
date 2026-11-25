import asyncio
import re
import time

import discord
import msgspec

from lumi.contracts import DiscordEffectGrant, decode
from lumi.contracts.channel_messages import DELETE_CHANNEL_MESSAGES, MESSAGE_PAGE, READ_CHANNEL_MESSAGES
from lumi.contracts.native_mutations import DeleteMessages
from lumi.shared.discord_effects import channel_operation_lane, effect_key, source_digest
from lumi.shared.discord_message_facts import body_facts

from .admission import declared_scope
from .effect_grants import ApprovedEffect, claim, snowflake
from .gateway_capture import record_mutation
from .members import fetch_member, lookup
from .message_fetches import fetch_message

BASE = frozenset({
    "schema", "action_id", "job_id", "module", "group", "generation", "expires_at_ms",
    "kind", "guild_id", "idempotency_key", "channel_id",
})


def openers(manifest, acl):
    return tuple(
        command for command in manifest.commands if command.acl_key == acl
        and command.public and command.guild_only and command.configure_in_guild
        and "manage_messages" in command.required_permissions
    )


def channel_message_scope(snapshot, job, action):
    spec = declared_scope(snapshot, job, "channel_messages", actor_callback=True)
    if (
        spec is None or not snowflake(job.actor_id) or not snowflake(job.channel_id) or not snowflake(job.guild_id)
        or (action.job_id, action.module, action.group, action.generation, action.guild_id, action.channel_id)
        != (job.job_id, job.module, job.group, job.generation, job.guild_id, job.channel_id)
    ):
        return None
    fields = msgspec.to_builtins(action).keys()
    if action.kind == READ_CHANNEL_MESSAGES:
        query = action.channel_message_query
        if (
            query is None or (query.content and not spec.include_content)
            or (query.author_roles and not query.content)
            or ((query.message is None) == (query.before is None))
            or (query.message is not None and (not snowflake(query.message) or query.limit != MESSAGE_PAGE))
            or (query.before is not None and not snowflake(query.before))
            or type(query.limit) is not int or not 1 <= query.limit <= MESSAGE_PAGE
            or not fields <= BASE | {"channel_message_query"}
        ):
            return None
    elif action.kind == DELETE_CHANNEL_MESSAGES:
        mutation = action.channel_message_deletion
        if (
            not spec.allow_delete or mutation is None
            or not re.fullmatch(r"[a-f0-9]{8}", mutation.operation)
            or not 1 <= len(mutation.messages) <= MESSAGE_PAGE
            or len(set(mutation.messages)) != len(mutation.messages)
            or not all(map(snowflake, mutation.messages))
            or (mutation.audit_reason is not None and len(mutation.audit_reason) > 512)
            or not re.fullmatch(r"effect:[a-f0-9]{32}", action.idempotency_key)
            or not fields <= BASE | {"channel_message_deletion"}
        ):
            return None
    else:
        return None
    return spec


class Denied(Exception):
    pass


async def authority(edge, job, guild, manifest, acl):
    channel, bot = await edge._action_channel(guild, job.channel_id), guild.me
    if not channel or not bot or channel.id != job.channel_id or channel.guild.id != guild.id:
        raise Denied("channel_unavailable")
    actor = await fetch_member(edge, guild, job.actor_id)
    if actor is None or actor.id != job.actor_id or actor.bot:
        raise Denied("permission_denied")
    actor_perms, bot_perms = channel.permissions_for(actor), channel.permissions_for(bot)
    if (
        not (actor_perms.view_channel and actor_perms.read_message_history and bot_perms.view_channel
             and bot_perms.read_message_history and bot_perms.manage_messages)
        or not any(
            (not command.owner_scope or actor.id == edge.owner_id)
            and edge._has_permissions(actor, command.required_permissions, channel)
            and all(getattr(bot.guild_permissions, flag, False) for flag in command.bot_guild_permissions)
            for command in openers(manifest, acl)
        )
    ):
        raise Denied("permission_denied")
    roles, admin = edge._member_access(actor)
    channels, categories = edge._channel_access(channel)
    if not edge.policy.allowed(
        guild_id=guild.id, module=job.module, acl_key=acl, public=True, role_ids=roles,
        administrator=admin, channel_ids=channels, category_ids=categories,
    ):
        raise Denied("policy_denied")
    return channel


def message_facts(message):
    if len(message.content) > 4000:
        raise Denied("unsupported_message")
    return dict(id=message.id, created_at_ms=int(message.created_at.timestamp() * 1000), **body_facts(message))


async def read(edge, guild, channel, spec, *, private):
    if spec.message is not None:
        try:
            messages = [await fetch_message(edge, channel, spec.message)]
        except discord.NotFound as exc:
            if exc.code != 10008:
                raise
            messages = []
    else:
        messages = [message async for message in channel.history(limit=spec.limit, before=discord.Object(id=spec.before))]
    if not spec.content:
        return {"ids": [message.id for message in messages]}
    observations, roles = [], {}
    for message in messages:
        author = message.author
        if private is not None and private.gate.blocks_identity(author.id):


            observations.append({"id": message.id, "restricted": True})
            continue
        facts = message_facts(message)
        if spec.author_roles and not facts["webhook"]:
            if author.id not in roles:
                try:
                    member = author if isinstance(author, discord.Member) else await fetch_member(edge, guild, author.id)
                    roles[author.id] = [role.id for role in member.roles] if member else None
                except discord.NotFound:
                    roles[author.id] = None
            facts["roles"] = roles[author.id]
        observations.append(facts)
    return {"messages": observations}


async def deletion_grant(edge, action, job, clock):
    key = effect_key(job.group, job.job_id, action.idempotency_key)
    raw = await edge.queue_redis.get(key)
    grant = decode(raw, DiscordEffectGrant) if raw else None
    if grant is None or (
        grant.module, grant.generation, grant.kind, grant.guild_id, grant.channel_id, grant.message_id,
        grant.digest, grant.source_digest, grant.mutation_digest, grant.delivery,
    ) != (
        job.module, job.generation, action.kind, job.guild_id, job.channel_id, None,
        source_digest(action.channel_message_deletion), None, None, None,
    ):
        raise Denied("effect_not_authorized")
    deadline = min(grant.expires_at_ms, action.expires_at_ms, job.expires_at_ms)
    if clock() >= deadline:
        raise Denied("expired_job")
    return key, raw, deadline


async def execute(edge, action, job, guild, *, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)

    def result(ok, status, code=None, **data):
        return edge._action_result(action, ok, status, code, data=data)

    snapshot = edge.registry.snapshot
    spec = channel_message_scope(snapshot, job, action)
    if not spec or guild.id != job.guild_id:
        return result(False, "failed", "action_scope_mismatch")
    module = snapshot.modules[job.module].manifest
    callback = snapshot.scheduled[(job.module, job.request.route)].spec
    acl = callback.acl_key
    if acl != job.request.event.get("_acl_key"):
        return result(False, "failed", "action_scope_mismatch")
    deadline = min(job.expires_at_ms, action.expires_at_ms)
    mutation = action.channel_message_deletion
    submitted = False
    try:
        async with asyncio.timeout(max(0.001, (deadline - now()) / 1000)):
            if mutation:
                key, raw, deadline = await deletion_grant(edge, action, job, now)
            cache, epoch = lookup(edge), getattr(edge.policy, "_epoch", 0)
            with cache.observe(guild.id, (job.actor_id, getattr(guild.me, "id", None))) as changed:

                def current():
                    ready = edge.registry.snapshot
                    latest = ready.scheduled.get((job.module, job.request.route))
                    return (
                        not changed[0] and now() < deadline and getattr(edge.policy, "_epoch", 0) == epoch
                        and not edge.control.maintenance and not edge.blocklist.contains(job.actor_id)
                        and latest is not None and latest.spec == callback and latest.generation == job.generation
                    )

                if not current():
                    raise Denied("access_changed")
                channel = await authority(edge, job, guild, module, acl)
                if not current():
                    raise Denied("access_changed")
                if mutation is None:
                    facts = await read(edge, guild, channel, action.channel_message_query, private=getattr(edge, "private_ingress", None))
                    if not current():
                        raise Denied("access_changed")
                    return result(True, "observed", **facts)
                lane = channel_operation_lane(job.group, job.module, guild.id, channel.id, mutation.operation)
                if not await claim(edge, ApprovedEffect(channel, key, raw, deadline), lane=lane):
                    raise Denied("effect_not_authorized")
                if not current():
                    raise Denied("access_changed")
                await record_mutation(edge, DeleteMessages(guild.id, job.actor_id, mutation.operation, channel.id, mutation.messages))
                if not current():
                    raise Denied("access_changed")
                submitted = True
                async with asyncio.timeout(max(0.001, (deadline - now()) / 1000)):
                    if len(mutation.messages) == 1:
                        await channel.get_partial_message(mutation.messages[0]).delete()
                    else:
                        await channel.delete_messages(
                            [discord.Object(id=message) for message in mutation.messages], reason=mutation.audit_reason,
                        )
                return result(True, "deleted")
    except Denied as exc:
        return result(False, "failed", str(exc))
    except discord.RateLimited as exc:
        return result(False, "failed", "rate_limited", retry_after_ms=max(1, int(exc.retry_after * 1000)))
    except discord.NotFound as exc:
        if submitted and len(mutation.messages) == 1 and exc.code == 10008:
            return result(True, "missing")
        return result(False, "failed", "resource_unavailable")
    except discord.Forbidden:
        return result(False, "failed", "permission_denied")
    except discord.HTTPException as exc:
        code = "too_old" if exc.code == 50034 else "uncertain" if submitted and exc.status >= 500 else "discord_rejected"
        return result(False, "failed", code, http_status=exc.status)
    except (TimeoutError, OSError):
        return result(False, "failed", "uncertain" if submitted else "unavailable")
    except (ValueError, TypeError):
        return result(False, "failed", "invalid_action")
