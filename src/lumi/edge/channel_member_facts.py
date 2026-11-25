from __future__ import annotations

import asyncio
import time

import discord

from lumi.contracts.models import DISCORD_MEMBER_QUERY_LIMIT

from .admission import admitted, declared_scope
from .members import fetch_member, lookup
from .read_proofs import ReadProofs


READ_CHANNEL_MEMBERS = "read_channel_members"


class ChannelMemberFacts:
    def __init__(self, edge, *, clock=None):
        self.edge = edge
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.proofs = ReadProofs(self.clock)

    def issue(self, action, destination):

        return self.proofs.issue(action, (READ_CHANNEL_MEMBERS, destination))

    async def execute(self, action, job, guild):
        reply = self.edge._action_result
        spec = declared_scope(self.edge.registry.snapshot, job, "channel_members")
        users = action.member_ids
        if (
            spec is None
            or action.kind != READ_CHANNEL_MEMBERS
            or (action.job_id, action.module, action.group, action.generation)
            != (job.job_id, job.module, job.group, job.generation)
            or action.guild_id != guild.id
            or action.guild_id != job.guild_id
            or not job.channel_id
            or not action.channel_id
            or not 1 <= len(users) <= DISCORD_MEMBER_QUERY_LIMIT
            or len(set(users)) != len(users)
            or any(type(uid) is not int or not 0 < uid < 2**64 for uid in users)
            or any(
                item is not None
                for item in (
                    action.intent,
                    action.content,
                    action.page,
                    action.message_id,
                )
            )
        ):
            return reply(action, False, "failed", "action_scope_mismatch")
        if not job.actor_id:
            if action.channel_id != job.channel_id:
                return reply(action, False, "failed", "action_scope_mismatch")
        elif not admitted(job) or (
            action.channel_id != job.channel_id
            and not self.proofs.valid_until(action, (READ_CHANNEL_MEMBERS, action.channel_id))
        ):
            return reply(action, False, "failed", "action_scope_mismatch")

        deadline = min(job.expires_at_ms, action.expires_at_ms)
        if deadline <= self.clock():
            return reply(action, False, "failed", "expired_job")
        channel = await self.edge._action_channel(guild, action.channel_id)
        if (
            channel is None
            or channel.id != action.channel_id
            or channel.guild.id != guild.id
            or getattr(channel.type, "value", None) not in spec.channel_types
            or guild.me is None
        ):
            return reply(action, False, "failed", "channel_unavailable")
        permissions = channel.permissions_for(guild.me)
        if not all(getattr(permissions, flag, False) for flag in spec.bot_permissions):
            return reply(action, False, "failed", "permission_denied")
        cache = lookup(self.edge)
        try:
            async with asyncio.timeout(max(0, (deadline - self.clock()) / 1000)):
                if job.actor_id and users == (job.actor_id,):
                    try:
                        actor = await fetch_member(self.edge, guild, job.actor_id)
                    except discord.NotFound:
                        actor = None
                    members = (actor,) if actor is not None else ()
                else:
                    with cache.observe(guild.id, (*users, guild.me.id)) as changed:
                        members = await cache.query(guild, users)
                        if changed[0]:
                            return reply(action, False, "failed", "access_changed")
        except (discord.HTTPException, discord.ClientException, TimeoutError, OSError):
            return reply(action, False, "failed", "member_unavailable")
        channels, categories = self.edge._channel_access(channel)
        selected = set(users)
        facts = []
        for member in members:
            if (
                member.id not in selected
                or member.bot
                or self.edge.blocklist.contains(member.id)
                or not channel.permissions_for(member).view_channel
            ):
                continue
            roles, administrator = self.edge._member_access(member)


            if member.id == job.actor_id or self.edge.policy.allowed(
                guild_id=guild.id,
                module=job.module,
                acl_key="",
                public=True,
                role_ids=roles,
                administrator=administrator,
                channel_ids=channels,
                category_ids=categories,
            ):
                facts.append({**member_facts(member), "administrator": administrator})
        return reply(
            action,
            True,
            "observed",
            data={
                "job": job.job_id,
                "guild": guild.id,
                "channel": channel.id,
                "users": list(users),
                "members": facts,
            },
        )


def member_facts(member):
    def stamp(value):
        return int(value.timestamp() * 1000) if value is not None else None

    return dict(
        user=member.id,
        roles=[r.id for r in member.roles],
        created=stamp(getattr(member, "created_at", None)),
        joined=stamp(getattr(member, "joined_at", None)),
    )
