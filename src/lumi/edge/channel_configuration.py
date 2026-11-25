import asyncio
import time

import discord
import msgspec

from lumi.contracts.channel_configuration import READ_CHANNEL_CONFIGURATION
from lumi.ui_keys import bound_interaction

from .admission import admitted, declared_scope
from .effect_grants import snowflake
from .members import fetch_member, lookup

BASE = frozenset({
    "schema", "action_id", "job_id", "module", "group", "generation", "expires_at_ms", "kind",
    "guild_id", "idempotency_key", "channel_id", "channel_configuration_query",
})
DEFAULT_DESTINATIONS = ("rules_channel", "public_updates_channel", "system_channel")


def configuration_scope(snapshot, job, action):
    spec = declared_scope(snapshot, job, "channel_configuration")
    query = action.channel_configuration_query
    if (
        spec is None or action.kind != READ_CHANNEL_CONFIGURATION or query is None
        or not snowflake(job.guild_id) or not snowflake(job.channel_id) or not snowflake(query.target)
        or (query.target != job.channel_id and not spec.other_channel)
        or (action.job_id, action.module, action.group, action.generation, action.guild_id, action.channel_id)
        != (job.job_id, job.module, job.group, job.generation, job.guild_id, job.channel_id)
        or job.request.event.get("dispatch") not in {"command", "component"} or not admitted(job)
        or not msgspec.to_builtins(action).keys() <= BASE
    ):
        return None
    return spec


async def channel_layout(guild):

    return {channel.id: channel for channel in await guild.fetch_channels()}


def project(guild, channel_id, layout, *, skip=0):
    channel = layout.get(channel_id)
    if channel is None:
        return None
    ordinary = isinstance(channel, discord.TextChannel) and channel.type == discord.ChannelType.text
    data = dict(
        id=channel.id, native_type=channel.type.value, ordinary_text=ordinary,
        system_destination=any(getattr(getattr(guild, name, None), "id", 0) == channel_id for name in DEFAULT_DESTINATIONS),
    )
    if not ordinary:
        return data
    siblings = sorted(
        (c for c in layout.values() if c.category_id == channel.category_id and c.id != skip
         and c._sorting_bucket == channel._sorting_bucket),
        key=lambda c: (c.position, c.id),
    )
    index = next(i for i, c in enumerate(siblings) if c.id == channel.id)
    data["settings"] = dict(
        name=channel.name, category=channel.category_id, topic=channel.topic,
        nsfw=channel.nsfw, slowmode=channel.slowmode_delay, flags=channel.flags.value,
        archive=channel.default_auto_archive_duration, thread_slowmode=channel.default_thread_slowmode_delay,
        overwrites=sorted([int(o.id), int(o.type), int(o.allow), int(o.deny)] for o in channel._overwrites),
        previous=siblings[index - 1].id if index else 0,
        following=siblings[index + 1].id if index + 1 < len(siblings) else 0, position=channel.position,
    )
    return data


def can_read(channel, actor, bot):
    flags = channel.permissions_for(actor)
    bot_flags = channel.permissions_for(bot)
    return (
        flags.view_channel and flags.manage_messages and flags.manage_channels
        and bot_flags.view_channel and bot_flags.read_message_history and bot_flags.manage_messages
        and bot_flags.manage_channels
        and all(m.guild_permissions.manage_channels or m.guild_permissions.administrator for m in (actor, bot))
    )


async def execute(edge, action, job, guild, *, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)

    def failed(code):
        return edge._action_result(action, False, "failed", code)

    snapshot = edge.registry.snapshot
    spec = configuration_scope(snapshot, job, action)
    deadline = min(job.expires_at_ms, action.expires_at_ms)
    if not spec or guild.id != job.guild_id or now() >= deadline:
        return failed("action_scope_mismatch")
    target = action.channel_configuration_query.target
    try:
        async with asyncio.timeout(max(0.001, (deadline - now()) / 1000)):
            if job.request.event.get("dispatch") == "component" and not await bound_interaction(edge.cache_redis, job):
                return failed("panel_expired")
            bot = guild.me
            with lookup(edge).observe(guild.id, (job.actor_id, getattr(bot, "id", None))) as changed:
                epoch = getattr(edge.policy, "_epoch", 0)
                channel = guild.get_channel_or_thread(target)
                if bot is None or channel is None or channel.id != target or channel.guild.id != guild.id:
                    return failed("channel_unavailable")
                actor = await fetch_member(edge, guild, job.actor_id)
                if actor is None or actor.id != job.actor_id or actor.bot:
                    return failed("permission_denied")
                if not can_read(channel, actor, bot):
                    return failed("permission_denied")
                report = {"channel_id": target}
                if action.channel_configuration_query.layout:
                    if isinstance(channel, discord.TextChannel) and channel.type == discord.ChannelType.text:
                        layout = await channel_layout(guild)
                        observed = layout.get(target)
                        if observed is None:
                            return failed("channel_unavailable")

                        if observed is not channel and not can_read(observed, actor, bot):
                            return failed("permission_denied")
                        channel = observed
                    else:

                        layout = {target: channel}
                    report["configuration"] = project(guild, target, layout)
                if target != job.channel_id:
                    roles, admin = edge._member_access(actor)
                    channels, categories = edge._channel_access(channel)
                    if not edge.policy.allowed(
                        guild_id=guild.id, module=job.module, acl_key=job.request.event.get("_acl_key", ""),
                        public=True, role_ids=roles, administrator=admin, channel_ids=channels, category_ids=categories,
                    ):
                        return failed("permission_denied")
                if (
                    changed[0] or now() >= deadline or epoch != getattr(edge.policy, "_epoch", 0)
                    or edge.control.maintenance or edge.blocklist.contains(job.actor_id)
                    or configuration_scope(edge.registry.snapshot, job, action) != spec
                ):
                    return failed("access_changed")
                return edge._action_result(action, True, "observed", data=report)
    except (discord.NotFound, discord.Forbidden):
        return failed("permission_denied")
    except (discord.HTTPException, OSError, TimeoutError):
        return failed("unavailable")
