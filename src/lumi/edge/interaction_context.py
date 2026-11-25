import discord

from lumi.contracts.models import MAX_CHANNEL_FACT_TARGETS
from lumi.ui_keys import bound_interaction

from .admission import admitted, declared_scope
from .command_loading import NATIVE_COMMANDS
from .members import fetch_member
from .message_fetches import fetch_message


def private_text_channel(guild, channel, permissions):
    if (
        not isinstance(channel, discord.TextChannel) or channel.guild.id != guild.id
        or guild.me is None or channel.permissions_for(guild.default_role).view_channel
    ):
        return False
    effective = channel.permissions_for(guild.me)
    return all(getattr(effective, name, False) for name in permissions)


async def private_channel_facts(edge, action, job, guild, origin):
    def failed(code):
        return edge._action_result(action, False, "failed", code)

    snapshot = edge.registry.snapshot
    scope = declared_scope(snapshot, job, "private_channel_facts")
    candidates = action.target_channel_ids
    event = job.request.event
    if (
        scope is None or action.message_id is not None or action.member_id is not None
        or action.message_link_field is not None
        or (origin.guild.id, origin.id) != (job.guild_id, job.channel_id)
        or len(candidates) > MAX_CHANNEL_FACT_TARGETS or len(set(candidates)) != len(candidates)
        or any(type(cid) is not int or not 0 < cid < 2**64 for cid in candidates)
        or not admitted(job, channel_id=origin.id)
    ):
        return failed("action_scope_mismatch")
    if event.get("dispatch") == "component":
        namespace, _, name = job.request.route.partition(".")
        route = snapshot.components[(namespace, name)].spec
        if route.session_bound:
            bound = await bound_interaction(edge.cache_redis, job)
        else:
            bound = admitted_component_source(edge, job, origin.id, event.get("message_id"))
        if not bound:
            return failed("session_expired")
    if edge.blocklist.contains(job.actor_id):
        return failed("policy_denied")
    try:
        actor = await fetch_member(edge, guild, job.actor_id)
    except discord.HTTPException:
        return failed("member_unavailable")
    if (
        actor is None or actor.bot or actor.id != job.actor_id
        or not origin.permissions_for(actor).view_channel
    ):
        return failed("policy_denied")
    channels = []
    for cid in candidates:
        channel = guild.get_channel(cid)
        if private_text_channel(guild, channel, scope.bot_permissions):
            readable = channel.permissions_for(actor).view_channel
            channels.append({"id": cid, "readable": readable, "nsfw": readable and channel.nsfw})
    return edge._action_result(action, True, "ok", data={
        "guild_administrator": actor.id == guild.owner_id or actor.guild_permissions.administrator,
        "origin_parent": origin.parent_id if isinstance(origin, discord.Thread) else origin.id,
        "origin_nsfw": origin.is_nsfw(),
        "channels": channels,
        "bot_guild_permissions": [
            name for name in scope.bot_guild_permissions
            if getattr(guild.me.guild_permissions, name, False)
        ],
    })


def native_string_selection(message, custom_id, submitted, *, bot_id):

    if (
        not bot_id or getattr(getattr(message, "author", None), "id", None) != bot_id
        or getattr(message, "webhook_id", None) is not None
        or not isinstance(submitted, list) or not submitted or len(submitted) > 25
        or not all(isinstance(value, str) for value in submitted)
    ):
        return None
    pending = [component.to_dict() for component in getattr(message, "components", ())]
    for _ in range(40):
        if not pending:
            break
        element = pending.pop()
        if element.get("type") == 3 and element.get("custom_id") == custom_id:
            if element.get("disabled"):
                return None
            offered = {option.get("value") for option in element.get("options", ())}
            if set(submitted) <= offered and len(set(submitted)) == len(submitted):
                return list(submitted)
            return None
        pending.extend(element.get("components", ()))
    return None


def component_source(message, *, guild_id, channel_id, bot_id, component_ids):

    if (
        message is None
        or not channel_id
        or getattr(getattr(message, "guild", None), "id", None) != guild_id
        or getattr(getattr(message, "channel", None), "id", None) != channel_id
    ):
        return None
    ids = component_ids([component.to_dict() for component in message.components])

    if len(ids) > 40 or any(not 1 <= len(value) <= 100 for value in ids):
        return None
    return {
        "guild_id": guild_id,
        "channel_id": channel_id,
        "message_id": message.id,
        "current_lumi": bot_id is not None and message.author.id == bot_id,
        "webhook": message.webhook_id is not None,
        "components_v2": message.flags.components_v2,
        "component_custom_ids": list(ids),
    }


def admitted_component_source(edge, job, channel_id, message_id):

    source = job.request.event.get("_component_message")
    if not isinstance(source, dict) or job.request.event.get("dispatch") != "component":
        return None
    component_ids = source.get("component_custom_ids")
    namespace, _, action = job.request.route.partition(".")
    registered = edge.registry.snapshot.components.get((namespace, action))
    if (
        registered is None or not registered.spec.source_message_facts
        or (registered.module, registered.group, registered.generation)
        != (job.module, job.group, job.generation)
        or registered.spec.handler != job.request.handler
        or not admitted(job, acl=registered.spec.acl_key, channel_id=channel_id)
        or (source.get("guild_id"), source.get("channel_id"), source.get("message_id"))
        != (job.guild_id, channel_id, message_id)
        or source.get("current_lumi") is not True or source.get("webhook") is not False
        or type(source.get("components_v2")) is not bool
        or not isinstance(component_ids, list)
        or not any(
            isinstance(value, str) and value.startswith(f"lumi:{namespace}:{action}:")
            for value in component_ids
        )
    ):
        return None
    return source


def selected_channels(guild, values, spec, channel_types):

    if (
        guild is None or not isinstance(values, (tuple, list)) or len(values) > spec.max_values
        or any(type(value) is not int or not 0 < value < 2**64 for value in values)
        or len(set(values)) != len(values)
    ):
        return None
    for identifier in values:
        channel = guild.get_channel_or_thread(identifier)
        if (
            channel is None or channel.id != identifier or channel.guild.id != guild.id
            or (channel_types and channel.type.value not in channel_types)
        ):
            return None
        if spec.require_private and channel.permissions_for(guild.default_role).view_channel:
            return None
        if spec.bot_permissions:
            permissions = channel.permissions_for(guild.me) if guild.me else None
            if not all(getattr(permissions, flag, False) for flag in spec.bot_permissions):
                return None
    return {"guild": guild.id, "ids": list(values)}


async def execute(edge, action, job, guild):
    def failed(code):
        return edge._action_result(action, False, "failed", code)

    event = job.request.event
    dispatch = event.get("dispatch")
    if (
        (action.module, action.group, action.generation, action.job_id)
        != (job.module, job.group, job.generation, job.job_id)
        or (action.guild_id, action.channel_id) != (job.guild_id, job.channel_id)
        or guild.id != job.guild_id
        or dispatch not in {"command", "component"}
        or any(
            value is not None
            for value in (action.content, action.intent, action.page)
        )
        or (action.member_id is not None and action.member_id <= 0)
        or (action.message_id is not None and (
            action.message_id <= 0 or dispatch != "command" or action.member_id is not None
            or action.message_id != event.get("reference_message_id")
        ))
    ):
        return failed("action_scope_mismatch")

    channel = guild.get_channel_or_thread(job.channel_id or 0)
    if channel is None or guild.me is None:
        return failed("channel_unavailable")
    if action.target_channel_ids is not None:
        return await private_channel_facts(edge, action, job, guild, channel)
    try:
        actor = await fetch_member(edge, guild, job.actor_id)
    except discord.HTTPException:
        return failed("member_unavailable")
    if actor is None or actor.bot:
        return failed("policy_denied")
    if not admitted(job, channel_id=channel.id):
        roles, admin = edge._member_access(actor)
        channels, categories = edge._channel_access(channel)
        if not edge.policy.allowed(
            guild_id=guild.id, module=job.module, acl_key=event.get("_acl_key", ""),
            public=event.get("_public", False), role_ids=roles, administrator=admin,
            channel_ids=channels, category_ids=categories,
        ):
            return failed("policy_denied")

    actor_permissions = channel.permissions_for(actor)
    bot_permissions = channel.permissions_for(guild.me)
    can_send = (
        bot_permissions.send_messages_in_threads if isinstance(channel, discord.Thread)
        else bot_permissions.send_messages
    )
    if not (actor_permissions.view_channel and bot_permissions.view_channel and can_send):
        return failed("channel_unavailable")

    if dispatch == "component":
        if not await bound_interaction(edge.cache_redis, job):
            return failed("session_expired")

    target_id = action.member_id
    if action.message_id is not None:
        if not bot_permissions.read_message_history:
            return failed("target_unavailable")
        try:
            reply = await fetch_message(edge, channel, action.message_id, cached=True)
        except discord.HTTPException:
            return failed("target_unavailable")
        target_id = reply.author.id
    if target_id is not None:
        try:
            target = actor if target_id == actor.id else await fetch_member(edge, guild, target_id)
        except discord.HTTPException:
            return failed("target_unavailable")
        if not channel.permissions_for(target).view_channel:
            return failed("target_unavailable")

    return edge._action_result(action, True, "ok", data=dict(
        actor_id=actor.id, target_id=target_id,
        guild_administrator=bool(actor.guild_permissions.administrator),
        guild_manage_messages=bool(actor.guild_permissions.manage_messages),
        actor_can_embed=bool(actor_permissions.embed_links),
        bot_can_embed=bool(bot_permissions.embed_links),
        registered_roots=sorted(set(edge.registry.snapshot.commands) | set(NATIVE_COMMANDS)),
    ))
