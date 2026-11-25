from __future__ import annotations

import asyncio
import re
import time

import discord

from lumi.contracts.models import EMBED_SOURCE_LIMIT, SOURCE_ROLE_LIMIT
from lumi.ui_keys import bound_interaction

from .admission import admitted, declared_scope
from .members import fetch_member, guild_roles
from .message_fetches import fetch_message
from .message_snapshots import embed_source
from .read_proofs import ReadProofs


READ_EMBED_SOURCES = "read_embed_sources"


def snowflake(value):
    return type(value) is int and 0 < value < 2**64


class MessageSources:
    def __init__(self, edge, *, clock=None):
        self.edge = edge
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.proofs = ReadProofs(self.clock)

    @staticmethod
    def scope(action, query):
        return READ_EMBED_SOURCES, action.channel_id, action.message_id, query

    def verified_until(self, action, query):
        return self.proofs.valid_until(action, self.scope(action, query))

    async def execute(self, action, job, guild):
        edge, query = self.edge, action.embed_sources

        def failed(code):
            return edge._action_result(action, False, "failed", code)

        spec = declared_scope(edge.registry.snapshot, job, "embed_sources")
        if (
            spec is None
            or action.kind != READ_EMBED_SOURCES
            or query is None
            or (action.job_id, action.module, action.group, action.generation)
            != (job.job_id, job.module, job.group, job.generation)
            or action.guild_id != job.guild_id
            or guild.id != job.guild_id
            or not snowflake(action.channel_id)
            or action.message_id is not None
            and not snowflake(action.message_id)
            or not snowflake(query.actor_id)
            or not snowflake(query.viewer_id)
            or len(query.sources) > EMBED_SOURCE_LIMIT
            or len(query.role_ids) > SOURCE_ROLE_LIMIT
            or len(set(query.role_ids)) != len(query.role_ids)
            or not all(
                snowflake(ref.channel_id) and snowflake(ref.message_id) for ref in query.sources
            )
            or not all(snowflake(role) for role in query.role_ids)
            or not re.fullmatch(r"[a-f0-9]{64}", query.binding)
            or any(
                value is not None
                for value in (action.intent, action.content, action.page)
            )
        ):
            return failed("action_scope_mismatch")
        if job.actor_id:
            if not admitted(job) or query.actor_id != job.actor_id:
                return failed("action_scope_mismatch")
            if job.request.event["dispatch"] == "component" and not await bound_interaction(
                edge.cache_redis, job
            ):
                return failed("session_expired")
        elif action.channel_id != job.channel_id:
            return failed("action_scope_mismatch")

        deadline = min(job.expires_at_ms, action.expires_at_ms)
        if deadline <= self.clock():
            return failed("expired_job")
        if guild.me is None:
            return failed("channel_unavailable")
        try:
            async with asyncio.timeout((deadline - self.clock()) / 1000):
                actor = await self.member(guild, query.actor_id)
                if actor is None or edge.blocklist.contains(query.actor_id):
                    return failed("destination_access_unavailable")
                is_admin = actor.guild_permissions.administrator
                if query.viewer_id != actor.id and not is_admin:
                    return failed("viewer_access_unavailable")
                viewer = (
                    actor
                    if query.viewer_id == actor.id
                    else await self.member(guild, query.viewer_id)
                )
                if viewer is None:
                    return failed("viewer_access_unavailable")
                target = await edge._action_channel(guild, action.channel_id)
                if not self.visible(target, guild, actor) or target.id != action.channel_id:
                    return failed("destination_access_unavailable")
                if not (job.actor_id and admitted(job, acl=spec.acl_key, channel_id=target.id)):
                    roles, admin = edge._member_access(actor)
                    channels, categories = edge._channel_access(target)
                    if not edge.policy.allowed(
                        guild_id=guild.id,
                        module=job.module,
                        acl_key=spec.acl_key,
                        public=True,
                        role_ids=roles,
                        administrator=admin,
                        channel_ids=channels,
                        category_ids=categories,
                    ):
                        return failed("destination_access_unavailable")
                views = await self.read_views(
                    guild, viewer, target, query.sources, frozenset(spec.author_ids)
                )
                if views is None:
                    return failed("source_unavailable")
                available = (
                    set(query.role_ids) & {r.id for r in await guild_roles(edge, guild)}
                    if query.role_ids
                    else set()
                )
        except (discord.HTTPException, discord.ClientException, TimeoutError, OSError):
            return failed("source_read_failed")
        if deadline <= self.clock():
            return failed("expired_job")
        proof, valid_until = self.proofs.issue_window(action, self.scope(action, query), deadline=deadline)
        return edge._action_result(
            action,
            True,
            "observed",
            data=dict(
                job=job.job_id,
                guild=guild.id,
                channel=target.id,
                actor=dict(
                    user=actor.id,
                    roles=[role.id for role in actor.roles],
                    permissions=[
                        key
                        for key in discord.Permissions.VALID_FLAGS
                        if getattr(actor.guild_permissions, key, False)
                    ],
                ),
                viewer=viewer.id,
                binding=query.binding,
                available_roles=sorted(available),
                views=views,
                read_proof=proof,
                read_until_ms=valid_until,
            ),
        )

    async def member(self, guild, user):
        try:
            member = await fetch_member(self.edge, guild, user)
        except discord.NotFound:
            return None
        return member if member is not None and member.id == user and not member.bot else None

    @staticmethod
    def visible(channel, guild, member):
        return (
            channel is not None
            and channel.guild.id == guild.id
            and channel.permissions_for(member).view_channel
            and channel.permissions_for(guild.me).view_channel
        )

    async def read_views(self, guild, viewer, target, sources, authors):
        if sources and self.edge.blocklist.contains(viewer.id):
            return None
        channels, seen = {}, {}
        for ref in sources:
            if ref in seen:
                continue
            if ref.channel_id not in channels:
                channel = (
                    target
                    if ref.channel_id == target.id
                    else await self.edge._action_channel(guild, ref.channel_id)
                )
                if (
                    channel is None
                    or channel.id != ref.channel_id
                    or channel.guild.id != guild.id
                    or not channel.permissions_for(viewer).view_channel
                ):
                    return None
                permissions = channel.permissions_for(guild.me)
                if not (permissions.view_channel and permissions.read_message_history):
                    return None
                channels[ref.channel_id] = channel
            channel = channels[ref.channel_id]
            try:
                message = await fetch_message(self.edge, channel, ref.message_id)
                if (
                    message.author.id not in authors
                    or not message.author.bot
                    or message.webhook_id is not None
                    or message.channel.id != channel.id
                ):
                    return None
                seen[ref] = embed_source(
                    message,
                    guild_id=guild.id,
                    channel_id=channel.id,
                    message_id=ref.message_id,
                )
            except (discord.NotFound, discord.Forbidden, ValueError):
                return None
        return [seen[ref] for ref in sources]
