from __future__ import annotations

import discord

from lumi.shared.access_receipts import admitted_request

from .members import fetch_member


def stamp(route, *, actor_id, guild_id, channel_id, snapshot=None):
    route.request.event["_edge_access"] = dict(
        schema=1, module=route.module, group=route.group, generation=route.generation,
        route=route.request.route, handler=route.request.handler, acl=route.acl_key,
        public=route.public, actor=actor_id, guild=guild_id, channel=channel_id,
    )
    registered = snapshot.modules.get(route.module) if snapshot is not None else None
    if registered is not None:
        manifest = registered.manifest
        if (
            (manifest.group, manifest.generation) == (route.group, route.generation)
            and manifest.guarded_replies is not None
        ):

            route.request.event["_guarded_reply"] = True


def admitted(job, *, acl=None, channel_id=None, passive=False):
    return (
        (channel_id is None or channel_id == job.channel_id)
        and admitted_request(
            job.request, module=job.module, group=job.group, generation=job.generation,
            actor=job.actor_id, guild=job.guild_id, channel=job.channel_id, acl=acl, passive=passive,
        )
    )


def access_epoch(edge):
    return [
        getattr(getattr(edge, "member_cache", None), "epoch", 0),
        getattr(edge.policy, "_epoch", 0),
    ]


def bot_guild_allowed(guild, required):
    if not required:
        return True
    bot = getattr(guild, "me", None)
    return bool(bot and all(getattr(bot.guild_permissions, flag, False) for flag in required))


def project_bot_channel(route, channel, guild, flags):
    if flags:
        bot = getattr(guild, "me", None)
        permissions = channel.permissions_for(bot) if bot and channel is not None else None
        route.request.event["_bot_channel_facts"] = {
            flag: bool(getattr(permissions, flag, False)) for flag in flags
        }


async def private_reply_allowed(edge, job, interaction, checkpoint):

    namespace, _, action = job.request.route.partition(".")
    route = edge.registry.snapshot.components.get((namespace, action))
    if (
        route is None or (route.module, route.group, route.generation)
        != (job.module, job.group, job.generation)
        or route.spec.handler != job.request.handler
        or not route.spec.private_access_checkpoint
        or not admitted(job, acl=route.spec.acl_key)
        or interaction.user.id != job.actor_id
        or not isinstance(checkpoint, list) or len(checkpoint) != 2
    ):
        return False
    if checkpoint == access_epoch(edge):
        return True
    guild = edge.get_guild(job.guild_id)
    channel = guild.get_channel_or_thread(job.channel_id) if guild is not None else None
    if channel is None:
        return False
    epoch = access_epoch(edge)
    try:
        member = await fetch_member(edge, guild, job.actor_id)
    except discord.HTTPException:
        return False
    if (
        member is None or epoch != access_epoch(edge) or edge.blocklist.contains(job.actor_id)
        or not edge._has_permissions(member, route.spec.required_permissions, channel)
    ):
        return False
    roles, administrator = edge._member_access(member)
    channels, categories = edge._channel_access(channel)
    return edge.policy.allowed(
        guild_id=job.guild_id, module=job.module, acl_key=route.spec.acl_key,
        public=route.spec.public, role_ids=roles, administrator=administrator,
        channel_ids=channels, category_ids=categories,
    )


def declared_scope(snapshot, job, field, *, actor_callback=False):

    registered = snapshot.modules.get(job.module)
    if registered is None or (registered.manifest.group, registered.manifest.generation) != (
        job.group,
        job.generation,
    ):
        return None
    request = job.request
    dispatch = request.event.get("dispatch")
    if dispatch == "scheduled":
        route = snapshot.scheduled.get((job.module, request.route))
        if (
            route
            and route.spec.handler == request.handler
            and (
                (route.spec.scope == "actor" and job.actor_id > 0)
                if actor_callback else (route.spec.scope == "guild" and not job.actor_id)
            )
        ):
            return getattr(route.spec, field)
    elif actor_callback:
        return None
    elif dispatch == "command" and job.actor_id > 0 and request.route == request.handler:
        scopes = {
            getattr(spec, field)
            for spec in registered.manifest.commands
            if spec.handler == request.handler
        }
        return next(iter(scopes)) if len(scopes) == 1 else None
    elif dispatch == "component" and job.actor_id > 0:
        namespace, _, name = request.route.partition(".")
        route = snapshot.components.get((namespace, name))
        if route and (route.module, route.group, route.spec.handler) == (
            job.module,
            job.group,
            request.handler,
        ):
            return getattr(route.spec, field)
    return None
