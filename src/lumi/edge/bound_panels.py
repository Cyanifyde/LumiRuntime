import asyncio
import re
import time
from contextlib import suppress

import discord
from redis.exceptions import RedisError

from lumi.contracts import DiscordEffectGrant, decode
from lumi.shared.discord_effects import (
    UPDATE_BOUND_PANEL, effect_key, mutation_lane, panel_binding, panel_payload,
)
from .admission import admitted, bot_guild_allowed, declared_scope
from .effect_grants import ApprovedEffect, claim, snowflake
from .intents import InvalidIntent
from .members import fetch_member, lookup
from .message_fetches import UNKNOWN_MESSAGE, action_message_binding, remember_owned_output


def opener(manifest, acl):
    return tuple(
        command for command in manifest.commands if command.acl_key == acl
        and command.public and command.guild_only and command.configure_in_guild
        and "v2" in (command.presentation_modes or (command.presentation_mode or manifest.presentation_mode,))
    )


def token_ok(token):
    return isinstance(token, str) and re.fullmatch(r"[a-f0-9]{16,32}", token) is not None


async def remember_panel(edge, job, manifest, intent, message_id):

    event = job.request.event
    acl = event.get("_acl_key", "")
    if (
        not token_ok(intent.session_id) or intent.presentation_mode != "v2" or intent.kind != "send"
        or not snowflake(job.guild_id) or not snowflake(job.actor_id) or not snowflake(job.channel_id)
        or event.get("dispatch") != "command" or not admitted(job, acl=acl)
        or not any(callback.bound_panel and callback.acl_key == acl for callback in getattr(manifest, "scheduled", ()))
        or not any(command.handler == job.request.handler for command in opener(manifest, acl))
    ):
        return
    await remember_owned_output(
        edge.queue_redis, guild_id=job.guild_id, channel_id=job.channel_id, message_id=message_id,
        module=job.module, family="v2", panel=panel_binding(job.actor_id, acl, intent.session_id),
    )


async def bound(edge, job, action, acl, token):
    binding = await action_message_binding(edge.queue_redis, job.guild_id, action.message_id)
    return bool(binding and (
        binding["module"], binding.get("output_channel"), binding.get("family"), binding.get("panel"),
    ) == (job.module, job.channel_id, "v2", panel_binding(job.actor_id, acl, token)))


async def execute(edge, action, job, guild, *, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)

    def failed(code, **data):
        return edge._action_result(action, False, "failed", code, data=data)

    snapshot = edge.registry.snapshot
    if (
        not declared_scope(snapshot, job, "bound_panel", actor_callback=True)
        or action.kind != UPDATE_BOUND_PANEL
        or (action.job_id, action.module, action.group, action.generation)
        != (job.job_id, job.module, job.group, job.generation)
        or (action.guild_id, action.channel_id) != (job.guild_id, job.channel_id)
        or guild.id != job.guild_id or not snowflake(action.message_id) or not snowflake(job.channel_id)
        or not token_ok(token := action.bound_panel_session) or action.page is None or action.persistent
        or any(v is not None for v in (action.intent, action.content, action.member_id))
        or not re.fullmatch(r"effect:[a-f0-9]{32}", action.idempotency_key)
    ):
        return failed("action_scope_mismatch")
    manifest = snapshot.modules[job.module].manifest
    route = snapshot.scheduled[(job.module, job.request.route)].spec
    acl = route.acl_key
    if acl != job.request.event.get("_acl_key"):
        return failed("action_scope_mismatch")
    lane = mutation_lane(job.group, job.module, guild.id, job.channel_id, action.message_id)
    target_key = (guild.id, job.channel_id, action.message_id)
    async with edge.message_mutations.locks.setdefault(target_key, asyncio.Lock()):
        key = effect_key(job.group, job.job_id, action.idempotency_key)
        raw = await edge.queue_redis.get(key)
        try:
            grant = decode(raw, DiscordEffectGrant) if raw else None
        except ValueError:
            grant = None
        if grant is None or (
            grant.module, grant.generation, grant.kind, grant.guild_id, grant.channel_id,
            grant.message_id, grant.digest, grant.source_digest, grant.mutation_digest, grant.delivery,
        ) != (
            job.module, job.generation, UPDATE_BOUND_PANEL, job.guild_id, job.channel_id,
            action.message_id, panel_payload(token, action.page), None, None, None,
        ):
            return failed("effect_not_authorized")
        deadline = min(action.expires_at_ms, job.expires_at_ms, grant.expires_at_ms)
        if now() >= deadline:
            return failed("expired_job")
        if not await bound(edge, job, action, acl, token):
            return failed("message_not_owned")
        channel = await edge._action_channel(guild, job.channel_id)
        if channel is None or guild.me is None or edge.user is None or channel.guild.id != guild.id:
            return failed("channel_unavailable")
        cache = lookup(edge)
        epoch = getattr(edge.policy, "_epoch", 0)
        with cache.observe(guild.id, (job.actor_id, guild.me.id)) as changed:
            try:
                actor = await fetch_member(edge, guild, job.actor_id)
            except discord.HTTPException:
                return failed("member_unavailable")
            if (
                not actor or actor.id != job.actor_id or actor.bot or not channel.permissions_for(actor).view_channel
                or not channel.permissions_for(guild.me).view_channel
                or not any(
                    (not spec.owner_scope or actor.id == edge.owner_id)
                    and edge._has_permissions(actor, spec.required_permissions, channel)
                    and bot_guild_allowed(guild, spec.bot_guild_permissions)
                    for spec in opener(manifest, acl)
                )
            ):
                return failed("permission_denied")
            roles, administrator = edge._member_access(actor)
            channels, categories = edge._channel_access(channel)
            if not edge.policy.allowed(
                guild_id=guild.id, module=job.module, acl_key=acl, public=True,
                role_ids=roles, administrator=administrator, channel_ids=channels, category_ids=categories,
            ):
                return failed("policy_denied")

            def current():
                live = edge.registry.snapshot
                module = live.modules.get(job.module)
                callback = live.scheduled.get((job.module, job.request.route))
                return (
                    not changed[0] and now() < deadline and getattr(edge.policy, "_epoch", 0) == epoch
                    and not edge.control.maintenance and not edge.blocklist.contains(job.actor_id)
                    and module is not None and module.manifest == manifest
                    and callback is not None and callback.spec == route
                )

            try:
                view = await edge._action_view(action)
            except InvalidIntent:
                return failed("invalid_presentation")
            if not current():
                return failed("access_changed")
            if not await claim(edge, ApprovedEffect(channel, key, raw, deadline), lane=lane):
                return failed("effect_not_authorized")
            if not current():
                return failed("access_changed")
            target = channel.get_partial_message(action.message_id)
            try:
                async with asyncio.timeout(max(0.001, (deadline - now()) / 1000)):
                    edited = await target.edit(view=view, allowed_mentions=discord.AllowedMentions.none())
            except discord.NotFound as exc:
                if exc.code == UNKNOWN_MESSAGE:
                    return failed("message_unavailable", deleted=True)
                return failed("message_unavailable")
            except discord.Forbidden:
                return failed("permission_denied")
            except (discord.HTTPException, TimeoutError, OSError):
                return failed("uncertain")
        edge._bind_view({"view": view}, edited)
        if action.page.controls:
            with suppress(RedisError, TimeoutError):
                await edge._bind_ui_session(
                    session_id=token, group=job.group, owner_id=job.actor_id, message_id=action.message_id,
                    guild_id=job.guild_id, channel_id=job.channel_id,
                )
        return edge._action_result(action, True, "edited")
