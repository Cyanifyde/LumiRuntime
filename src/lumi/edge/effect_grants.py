from dataclasses import dataclass
import re

import discord

from lumi.contracts import DiscordEffectGrant, decode
from lumi.contracts.models import DISCORD_MEMBER_QUERY_LIMIT
from lumi.shared.discord_effects import PUBLISH_MESSAGE, effect_key, payload_digest, source_digest
from lumi.ui_keys import bound_interaction

from .admission import admitted
from .members import fetch_member


class EffectDenied(Exception):
    def __init__(self, code, *, approved=False):
        super().__init__(code)
        self.code, self.approved = code, approved


@dataclass(frozen=True, slots=True)
class ApprovedEffect:
    channel: object
    key: str
    raw: bytes
    deadline: int
    delivery: object = None


_CLAIM_GRANT = (
    "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0"
)
_CLAIM_IN_LANE = (
    "if redis.call('GET',KEYS[1]) == ARGV[1] and redis.call('GET',KEYS[2]) == KEYS[1] "
    "then return redis.call('DEL',KEYS[1]) end return 0"
)


def snowflake(value):
    return type(value) is int and 0 < value < 2**64


def outcome(edge, action, disposition, *, error=None, message_id=0):
    return edge._action_result(
        action,
        disposition in {"sent", "edited", "deleted"},
        disposition,
        error,
        data={
            "delivery": dict(
                guild_id=action.guild_id,
                channel_id=action.channel_id,
                parent_id=action.message_id,
                attempt=action.idempotency_key,
                outcome=disposition,
                message_id=message_id,
            )
        },
    )


async def approve(edge, action, job, guild, spec, clock, *, history=False, attachments=False):
    if (
        spec is None
        or action.guild_id != job.guild_id
        or guild.id != job.guild_id
        or (action.job_id, action.module, action.group, action.generation)
        != (job.job_id, job.module, job.group, job.generation)
        or not snowflake(action.channel_id)
    ):
        raise EffectDenied("action_scope_mismatch")
    if job.actor_id:
        if not admitted(job):
            raise EffectDenied("action_scope_mismatch")
        if job.request.event["dispatch"] == "component" and not await bound_interaction(
            edge.cache_redis, job
        ):
            raise EffectDenied("effect_not_authorized")
    elif action.channel_id != job.channel_id:
        raise EffectDenied("action_scope_mismatch")

    try:
        key = effect_key(job.group, job.job_id, action.idempotency_key)
        raw = await edge.queue_redis.get(key)
        grant = decode(raw, DiscordEffectGrant) if raw is not None else None
    except ValueError:
        grant = None
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
    ) != (
        job.module,
        job.generation,
        action.kind,
        guild.id,
        action.channel_id,
        action.message_id,
        payload_digest(action.intent, action.member_ids),
        source_digest(action.embed_sources),
        source_digest(action.message_mutation),
    ):
        raise EffectDenied("effect_not_authorized")
    deadline = min(action.expires_at_ms, job.expires_at_ms, grant.expires_at_ms)
    rules = getattr(spec, "delivery", None)
    delivery = grant.delivery
    if (rules is None) != (delivery is None) or (delivery is not None and (
        job.actor_id != 0 or job.request.event.get("dispatch") != "scheduled"
        or not re.fullmatch(r"effect:[a-f0-9]{32}", action.idempotency_key)
        or len(delivery.participants) > DISCORD_MEMBER_QUERY_LIMIT
        or any(not snowflake(person) for person in delivery.participants)
        or (delivery.nonce is not None and (
            action.kind != PUBLISH_MESSAGE or not re.fullmatch(r"[A-Za-z0-9:_-]{1,25}", delivery.nonce)
        ))
    )):
        raise EffectDenied("action_scope_mismatch")
    if deadline <= clock():
        raise EffectDenied("expired_job")
    return ApprovedEffect(
        await effect_channel(
            edge, action, job, guild, spec, history=history, attachments=attachments
        ),
        key,
        raw,
        deadline,
        delivery,
    )


def delivery_allowed(edge, guild, job, approved, rules):
    if rules is None:
        return True
    channel = guild.get_channel_or_thread(job.channel_id)
    if not channel or channel.id != approved.channel.id or not guild.me:
        return False
    if rules.private and (
        not isinstance(channel, discord.TextChannel)
        or channel.permissions_for(guild.default_role).view_channel
    ):
        return False
    permissions = channel.permissions_for(guild.me)
    send = permissions.send_messages_in_threads if isinstance(channel, discord.Thread) else permissions.send_messages
    envelope = approved.delivery
    return bool(
        all(getattr(permissions, flag) for flag in rules.bot_permissions)
        and send and permissions.view_channel and permissions.read_message_history
        and (not envelope.nsfw or channel.is_nsfw())
        and not any(edge.blocklist.contains(person) for person in envelope.participants)
        and edge.policy.module_enabled(guild.id, job.module)
    )


async def effect_channel(edge, action, job, guild, spec, *, history, attachments):
    channel = await edge._action_channel(guild, action.channel_id)
    if (
        not channel
        or channel.id != action.channel_id
        or channel.guild.id != guild.id
        or getattr(channel.type, "value", None) not in spec.channel_types
        or guild.me is None
    ):
        raise EffectDenied("channel_unavailable", approved=True)
    permissions = channel.permissions_for(guild.me)
    send = (
        permissions.send_messages_in_threads
        if isinstance(channel, discord.Thread)
        else permissions.send_messages
    )
    if (
        not (permissions.view_channel and send)
        or (history and not permissions.read_message_history)
        or (attachments and not permissions.attach_files)
    ):
        raise EffectDenied("permission_denied", approved=True)
    if action.channel_id == job.channel_id:
        return channel
    if not spec.cross_channel_acl:
        raise EffectDenied("policy_denied", approved=True)
    try:
        actor = await fetch_member(edge, guild, job.actor_id)
    except (discord.NotFound, discord.Forbidden):
        actor = None
    if (
        not actor
        or actor.id != job.actor_id
        or actor.bot
        or edge.blocklist.contains(actor.id)
        or not channel.permissions_for(actor).view_channel
    ):
        raise EffectDenied("policy_denied", approved=True)
    roles, admin = edge._member_access(actor)
    channels, categories = edge._channel_access(channel)
    if not edge.policy.allowed(
        guild_id=guild.id,
        module=job.module,
        acl_key=spec.cross_channel_acl,
        public=True,
        role_ids=roles,
        administrator=admin,
        channel_ids=channels,
        category_ids=categories,
    ):
        raise EffectDenied("policy_denied", approved=True)
    return channel


async def claim(edge, approved, *, lane=None):

    if lane is None:
        return bool(await edge.queue_redis.eval(_CLAIM_GRANT, 1, approved.key, approved.raw))
    return bool(await edge.queue_redis.eval(_CLAIM_IN_LANE, 2, approved.key, lane, approved.raw))
