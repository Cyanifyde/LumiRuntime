import time

import discord
from redis.exceptions import RedisError

from lumi.contracts import decode
from lumi.shared.discord_effects import payload_digest
from lumi.shared.reply_guards import ReplyGuard, guard_key, verify_guard

from .admission import admitted
from .interaction_context import private_text_channel
from .members import fetch_member


def requires_guard(edge, job, manifest=None):
    if manifest is None:
        module = edge.registry.snapshot.modules.get(job.module)
        manifest = module.manifest if module is not None else None
    return (
        job.request.event.get("_guarded_reply") is True
        or (manifest is not None and manifest.guarded_replies is not None)
    )


async def reply_allowed(edge, job, result, *, claim=False, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)
    module = edge.registry.snapshot.modules.get(job.module)
    if module is None:
        return False
    manifest = module.manifest
    spec = manifest.guarded_replies
    if spec is None:
        return not requires_guard(edge, job, manifest)
    if (
        (manifest.group, manifest.generation) != (job.group, job.generation)
        or len(result.intents) != 1 or not admitted(job)
        or result.intents[0].kind not in {"send", "edit", "ephemeral"}
    ):
        return False
    try:
        raw = await edge.cache_redis.get(guard_key(job.group, job.job_id))
        if raw is None:
            return False
        guard = decode(raw, ReplyGuard)
        if (
            (guard.module, guard.generation, guard.actor, guard.guild, guard.channel)
            != (job.module, job.generation, job.actor_id, job.guild_id, job.channel_id)
            or not now() < guard.expires_at_ms <= job.expires_at_ms
            or guard.digest != payload_digest(result.intents[0], ())
            or (guard.require_nsfw and not guard.private_target)
            or (result.intents[0].attachments and not guard.private_target)
            or any(not 0 < uid < 2**64 for uid in guard.participants)
        ):
            return False
        ingress = getattr(edge, "private_ingress", None)
        gate = ingress.gate if ingress is not None else None
        if gate is not None and (
            job.admitted_at_ms <= getattr(gate, "protected_replies_after_ms", 0)
            or gate.fence.matches(guard)
        ):
            return False
        if any(edge.blocklist.contains(uid) for uid in (guard.actor, *guard.participants)):
            return False
        if claim and guard.capture_revision:
            await edge.gateway_captures.refresh_revision(
                (job.group, job.module), guard.guild, guard.capture_revision,
            )
        guild = edge.get_guild(guard.guild)
        channel = guild.get_channel_or_thread(guard.channel) if guild is not None else None
        if channel is None or channel.guild.id != guard.guild:
            return False
        member = await fetch_member(edge, guild, guard.actor)
        if (
            member is None or member.id != guard.actor or member.bot
            or not channel.permissions_for(member).view_channel
            or (guard.require_admin and not (
                member.id == guild.owner_id or member.guild_permissions.administrator
            ))
        ):
            return False
        if guard.private_target:
            origin = channel.parent_id if isinstance(channel, discord.Thread) else channel.id
            target = guild.get_channel(guard.private_target)
            if (
                origin != guard.private_target
                or not private_text_channel(guild, target, spec.private_bot_permissions)
                or not target.permissions_for(member).view_channel
                or (guard.require_nsfw and not (channel.is_nsfw() and target.is_nsfw()))
            ):
                return False
        return now() < guard.expires_at_ms and await verify_guard(
            edge.cache_redis, job, guard, raw, claim=claim,
        )
    except (ValueError, TypeError, discord.HTTPException, RedisError):
        return False
