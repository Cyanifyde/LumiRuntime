from __future__ import annotations

import discord

from lumi.contracts import DiscordAction, DiscordActionResult, DiscordIntent, JobEnvelope, encode
from lumi.shared.request_scope import memo_read

from .activity_watermark import last_activities
from .members import fetch_member


async def execute(edge, action: DiscordAction, job: JobEnvelope, guild) -> DiscordActionResult:
    scope = job.guild_sources
    channel = job.request.event.get("origin_channel_id") or job.channel_id
    if (
        scope is None or job.guild_id != guild.id or action.guild_id != guild.id
        or (action.job_id, action.module, action.group, action.generation)
        != (job.job_id, job.module, job.group, job.generation)
        or action.member_id != job.actor_id or action.channel_id != channel
        or action.content is not None or action.intent is not None or action.message_id is not None
    ):
        return edge._action_result(action, False, "failed", "action_scope_mismatch")
    if edge.blocklist.contains(job.actor_id):
        return edge._action_result(action, False, "failed", "policy_denied")
    sources = await eligible_sources(edge, job.actor_id, job.module, scope.acl_key)
    activity = {}
    if scope.include_activity:
        try:
            activity = await last_activities(edge.queue_redis, tuple(s["id"] for s in sources), job.actor_id)
        except (ValueError, UnicodeError):
            return edge._action_result(action, False, "failed", "activity_unavailable")
    result = edge._action_result(
        action, True, "completed", data={
            "guilds": sources,
            "last_spoke": [dict(
                guild_id=source_id, occurred_at_ms=boundary.occurred_at_ms,
                message_id=boundary.message_id,
            ) for source_id, boundary in activity.items()],
        },
    )
    try:
        encode(result)
    except ValueError:

        return edge._action_result(action, False, "failed", "source_scope_too_large")
    return result


async def eligible_sources(edge, actor_id, module, acl, only=None):
    if edge.blocklist.contains(actor_id):
        return []
    key = (
        id(edge), actor_id, module, acl, frozenset(only) if only is not None else None,
        getattr(edge.policy, "_epoch", 0),
        getattr(getattr(edge, "member_cache", None), "epoch", 0),
    )
    return await memo_read(
        "guild-sources", key, lambda: _eligible_sources(edge, actor_id, module, acl, only),
    )


async def _eligible_sources(edge, actor_id, module, acl, only):
    sources = []
    for source in edge.guilds:
        if only is not None and source.id not in only:
            continue
        if not edge.policy.command_enabled(source.id, module, acl):
            continue
        actor = source.get_member(actor_id)
        if actor is None:
            try:
                actor = await fetch_member(edge, source, actor_id)
            except (discord.HTTPException, AttributeError):
                continue
        if actor is None or actor.bot or actor.id != actor_id:
            continue
        roles, administrator = edge._member_access(actor)


        channels = frozenset(c.id for c in source.channels)
        categories = frozenset(c.category_id for c in source.channels if getattr(c, "category_id", None))
        if edge.policy.allowed(
            guild_id=source.id, module=module, acl_key=acl, public=True, role_ids=roles,
            administrator=administrator, channel_ids=channels, category_ids=categories,
        ):
            sources.append({"id": source.id, "name": source.name})
    return sources


async def allowed_intent(edge, job: JobEnvelope, intent: DiscordIntent) -> bool:
    if not intent.source_guild_ids:
        return True
    scope = job.guild_sources
    sources = frozenset(intent.source_guild_ids)
    if scope is None or not job.guild_id or any(type(g) is not int or g <= 0 for g in sources):
        return False
    allowed = await eligible_sources(edge, job.actor_id, job.module, scope.acl_key, sources)
    return sources == {g["id"] for g in allowed}
