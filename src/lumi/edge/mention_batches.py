from __future__ import annotations

import asyncio
import time

import discord

from lumi.contracts import MentionBatchPlan, decode
from lumi.contracts.models import MAX_MENTION_RECIPIENTS
from lumi.shared.mention_batches import plan_key, receipt_identity

from .members import lookup


def now_ms() -> int:
    return time.time_ns() // 1_000_000


async def execute(edge, action, job, guild):
    def result(status, *, users=(), error=None, **data):
        return edge._action_result(
            action, status in {"sent", "skipped"}, status, error,
            data={"recipient_ids": list(users), **data},
        )

    reference = action.mention_batch
    prefetch = action.kind == "read_mention_batch_members"
    route = edge.registry.snapshot.scheduled.get((job.module, job.request.route))
    if (
        reference is None or route is None or not route.spec.mention_templates
        or route.spec.scope != "guild" or route.spec.handler != job.request.handler
        or (route.group, route.generation) != (job.group, job.generation)
        or (action.job_id, action.module, action.group, action.generation)
        != (job.job_id, job.module, job.group, job.generation)
        or action.guild_id != job.guild_id or action.guild_id != guild.id
        or action.channel_id != job.channel_id or not job.channel_id
        or action.content is not None or action.intent is not None
        or action.page is not None or action.member_id is not None
        or job.actor_id != 0 or job.request.event.get("dispatch") != "scheduled"
        or job.request.arguments != {"batch": reference.batch_id, "revision": reference.revision}
        or (not prefetch and (action.member_ids or action.idempotency_key != receipt_identity(reference)))
    ):
        return result("failed", error="action_scope_mismatch")
    if prefetch:
        users = action.member_ids
        if (
            not 1 <= len(users) <= MAX_MENTION_RECIPIENTS or len(users) != len(set(users))
            or any(type(uid) is not int or not 0 < uid < 2**64 for uid in users)
        ):
            return result("failed", error="action_scope_mismatch")
        remaining = min(action.expires_at_ms, job.expires_at_ms) - now_ms()
        if remaining <= 0:
            return result("failed", error="expired_job")
        try:
            async with asyncio.timeout(min(5, remaining / 1000)):
                members = await lookup(edge).query(
                    guild, [uid for uid in users if not edge.blocklist.contains(uid)],
                )
        except (discord.HTTPException, discord.ClientException, TimeoutError):
            return result("failed", error="member_unavailable")
        return edge._action_result(action, True, "completed", data={
            "member_ids": [member.id for member in members if member.id in users],
        })

    raw = await edge.queue_redis.get(plan_key(job.group, job.module, reference))
    try:
        plan = decode(raw, MentionBatchPlan) if raw else None
    except ValueError:
        plan = None
    if (
        plan is None or plan.reference != reference
        or (plan.group, plan.module, plan.job_id, plan.generation)
        != (job.group, job.module, job.job_id, job.generation)
        or (plan.guild_id, plan.channel_id) != (job.guild_id, job.channel_id)
        or not 0 < plan.due_at_ms <= now_ms() < plan.expires_at_ms
        or any(
            not 0 < row.user_id < 2**64 or not plan.due_at_ms < row.expires_at_ms <= plan.expires_at_ms
            for row in plan.recipients
        )
        or len({row.user_id for row in plan.recipients}) != len(plan.recipients)
    ):
        return result("failed", error="action_scope_mismatch")
    caption = next((t.caption for t in route.spec.mention_templates if t.key == plan.template), None)
    if caption is None:
        return result("failed", error="action_scope_mismatch")
    deadline = min(plan.expires_at_ms, action.expires_at_ms, job.expires_at_ms)
    if now_ms() >= deadline:
        return result("skipped")
    channel = await edge._action_channel(guild, plan.channel_id)
    if channel is None or channel.id != plan.channel_id:
        return result("failed", error="channel_unavailable")
    permissions = channel.permissions_for(guild.me) if guild.me else discord.Permissions.none()
    can_send = (
        permissions.send_messages_in_threads if isinstance(channel, discord.Thread)
        else permissions.send_messages
    )
    if not permissions.view_channel or not can_send:
        return result("failed", error="permission_denied")

    now = now_ms()
    wanted = [row.user_id for row in plan.recipients if now < min(deadline, row.expires_at_ms)]
    if not wanted:
        return result("skipped")


    cache = lookup(edge)
    members = {uid: cache.cached(guild, uid) for uid in wanted}
    channels, categories = edge._channel_access(channel)
    eligible = []
    for row in plan.recipients:
        member = members.get(row.user_id)
        if (
            member is None or member.bot or edge.blocklist.contains(row.user_id)
            or not channel.permissions_for(member).view_channel
        ):
            continue
        roles, administrator = edge._member_access(member)
        if edge.policy.allowed(
            guild_id=plan.guild_id, module=plan.module, acl_key=route.spec.acl_key, public=True,
            role_ids=roles, administrator=administrator, channel_ids=channels, category_ids=categories,
        ):
            eligible.append(row)

    now = now_ms()
    live = [row for row in eligible if now < min(deadline, row.expires_at_ms)]
    if not live:
        return result("skipped")
    users = [row.user_id for row in live]
    content = " ".join(f"<@{user}>" for user in users) + " " + caption
    if len(content) > 2000:
        return result("failed", error="invalid_action")
    remaining = min(deadline, *(row.expires_at_ms for row in live)) - now
    try:
        async with asyncio.timeout(min(15, remaining / 1000)):
            message = await channel.send(
                content=content,
                allowed_mentions=discord.AllowedMentions(
                    everyone=False, roles=False, users=[discord.Object(id=user) for user in users],
                    replied_user=False,
                ),
            )
        return result("sent", users=users, message_id=message.id, channel_id=plan.channel_id)
    except discord.Forbidden:
        return result("failed", users=users, error="permission_denied")
    except discord.NotFound:
        return result("failed", users=users, error="channel_unavailable")
    except discord.HTTPException as exc:
        if exc.status < 500:
            return result("failed", users=users, error="discord_rejected")
        return result("uncertain", users=users, error="ambiguous")
    except (TimeoutError, OSError):
        return result("uncertain", users=users, error="ambiguous")
