import re

import discord

from lumi.ui_keys import bound_interaction

from .admission import admitted
from .members import fetch_member, guild_roles
from .message_fetches import fetch_message
from .message_snapshots import embed_source


MAX_GUILD_CHANNELS = 1000
BOT_POST_PERMISSIONS = ("view_channel", "send_messages", "read_message_history", "attach_files")
MESSAGE_LINK = re.compile(
    r"https://(?:www\.)?discord(?:app)?\.com/channels/([0-9]{17,20})/([0-9]{17,20})/([0-9]{17,20})"
)


def route_spec(snapshot, job):
    module = snapshot.modules.get(job.module)
    if module is None or module.manifest.group != job.group:
        return None
    request = job.request
    dispatch = request.event.get("dispatch")
    if dispatch == "command" and request.route == request.handler:
        matches = [spec for spec in module.manifest.commands if spec.handler == request.handler]
        if (
            not matches or not all(spec.guild_only for spec in matches)
            or len({(s.acl_key, s.public, s.owner_scope) for s in matches}) != 1
        ):
            return None
        spec = matches[0]
    elif dispatch == "component":
        namespace, _, action = request.route.partition(".")
        registered = snapshot.components.get((namespace, action))
        if (
            registered is None or (registered.module, registered.group) != (job.module, job.group)
            or registered.spec.handler != request.handler or not registered.spec.session_bound
        ):
            return None
        spec = registered.spec
    else:
        return None
    return spec if not spec.owner_scope else None


async def execute(edge, action, job, guild):
    spec = route_spec(edge.registry.snapshot, job)
    targets = action.target_channel_ids
    private_admin = (
        spec is not None and not spec.public and job.request.event.get("dispatch") == "component"
        and "administrator" in spec.required_permissions and targets == ()
    )
    if (
        (not private_admin and (spec is None or not spec.public or not spec.acl_key))
        or job.guild_id != guild.id or action.guild_id != guild.id
        or (action.job_id, action.module, action.group, action.generation)
        != (job.job_id, job.module, job.group, job.generation)
        or action.member_id != job.actor_id or action.channel_id != job.channel_id
        or any(value is not None for value in (action.intent, action.content, action.page))
        or (targets is not None and (
            len(targets) > 2 or len(set(targets)) != len(targets)
            or any(type(target) is not int or not 0 < target < 2**64 for target in targets)
        ))
    ):
        return edge._action_result(action, False, "failed", "action_scope_mismatch")
    event = job.request.event
    message_id, field = action.message_id, action.message_link_field
    if (
        private_admin and (message_id is not None or field is not None)
        or message_id is not None and (
            field is not None or event.get("dispatch") != "command"
            or "reference_message_id" not in spec.context_fields
            or not 0 < message_id < 2**64 or message_id != event.get("reference_message_id")
        )
        or field is not None and (
            event.get("dispatch") != "component" or spec.modal is None
            or not any(option.key == field and option.kind == "text" for option in spec.modal.fields)
        )
    ):
        return edge._action_result(action, False, "failed", "action_scope_mismatch")
    if job.request.event.get("dispatch") == "component" and not await bound_interaction(edge.cache_redis, job):
        return edge._action_result(action, False, "failed", "session_expired", data={"message": "Expired."})

    def unavailable():
        return edge._action_result(action, False, "failed", "policy_denied", data={"message": "Unavailable."})

    if edge.blocklist.contains(job.actor_id) or guild.me is None:
        return unavailable()
    try:
        actor = await fetch_member(edge, guild, job.actor_id)
    except (discord.NotFound, discord.Forbidden):
        return unavailable()
    if actor is None or actor.bot or actor.id != job.actor_id:
        return unavailable()
    origin = await edge._action_channel(guild, job.channel_id)
    role_ids = frozenset(role.id for role in actor.roles)
    guild_permissions = actor.guild_permissions
    admin = guild_permissions.administrator

    def eligible(channel, *, origin=False):
        if channel is None or channel.guild.id != guild.id or not channel.permissions_for(actor).view_channel:
            return False
        if origin and admitted(job, acl=spec.acl_key, channel_id=channel.id):
            return True
        channels, categories = edge._channel_access(channel)
        return edge.policy.allowed(
            guild_id=guild.id, module=job.module, acl_key=spec.acl_key, public=spec.public,
            role_ids=role_ids, administrator=admin, channel_ids=channels, category_ids=categories,
        )

    if not eligible(origin, origin=True):
        return unavailable()
    if targets is None:
        candidates = getattr(guild, "channels", None)
        if candidates is None or getattr(guild, "unavailable", False):
            return unavailable()
        if len(candidates) > MAX_GUILD_CHANNELS:
            return edge._action_result(
                action, False, "failed", "source_scope_too_large",
                data={"message": "Too many channels. Try again."},
            )
    else:
        candidates = [
            origin if target == origin.id else await edge._action_channel(guild, target)
            for target in targets
        ]
    def postable(channel):
        if not isinstance(channel, discord.TextChannel) or channel.guild.id != guild.id:
            return False
        permissions = channel.permissions_for(guild.me)
        return all(getattr(permissions, key) for key in BOT_POST_PERMISSIONS)

    by_id = {channel.id: channel for channel in [*candidates, origin] if channel is not None}
    channels = sorted({
        channel.id for channel in candidates
        if postable(channel) and (channel.id == origin.id or eligible(channel))
    })
    selected = None
    kind = getattr(spec, "input_kind", None)
    if admin and "administrator" in spec.required_permissions and kind in {"channel_ids", "role_ids"}:
        values = job.request.arguments.get("values", ())
        if (
            not isinstance(values, (tuple, list)) or len(values) > 25
            or any(type(value) is not int or not 0 < value < 2**64 for value in values)
        ):
            return edge._action_result(action, False, "failed", "invalid_selection")
        identifiers = list(values)
        if kind == "channel_ids":
            available = []

            if len(identifiers) == 1:
                identifier = identifiers[0]
                channel = by_id.get(identifier) or await edge._action_channel(guild, identifier)
                if postable(channel):
                    available.append(identifier)
        else:
            current_roles = {role.id for role in await guild_roles(edge, guild)} if identifiers else set()
            available = [identifier for identifier in identifiers if identifier in current_roles]
        selected = {"kind": kind, "values": identifiers, "available": available}
    source = None
    target = origin
    if field is not None:
        link = job.request.arguments.get("fields", {}).get(field)
        matched = MESSAGE_LINK.fullmatch(link.strip()) if isinstance(link, str) else None
        if not matched or int(matched[1]) != guild.id:
            return edge._action_result(action, False, "failed", "invalid_message_link")
        requested_channel, message_id = int(matched[2]), int(matched[3])
        if not 0 < requested_channel < 2**64 or not 0 < message_id < 2**64:
            return edge._action_result(action, False, "failed", "invalid_message_link")
        target = by_id.get(requested_channel) or await edge._action_channel(guild, requested_channel)
    if message_id is not None:
        if target is None or target.guild.id != guild.id or not target.permissions_for(actor).view_channel:
            return edge._action_result(action, False, "failed", "source_unavailable")
        if field is None and "reference_embed" in spec.context_fields and "reference_embed" in event:
            source = event["reference_embed"]
            if not isinstance(source, dict) or (
                source.get("guild"), source.get("channel"), source.get("message")
            ) != (guild.id, target.id, message_id):
                return edge._action_result(action, False, "failed", "source_unavailable")
        else:
            try:
                message = await fetch_message(edge, target, message_id, cached=field is None)
                source = embed_source(message, guild_id=guild.id, channel_id=target.id, message_id=message_id)
            except (discord.NotFound, discord.Forbidden, ValueError):
                return edge._action_result(action, False, "failed", "source_unavailable")
    permissions = [key for key in discord.Permissions.VALID_FLAGS if getattr(guild_permissions, key, False)]
    facts = getattr(edge, "channel_member_facts", None)
    grants = (
        {str(target_id): facts.issue(action, target_id) for target_id in channels if target_id != origin.id}
        if facts is not None and spec.channel_members is not None and targets is not None else {}
    )
    return edge._action_result(action, True, "completed", data={"access": {
        "job": job.job_id, "guild": guild.id, "actor": actor.id, "origin": origin.id,
        "channels": channels, "roles": sorted(role_ids), "permissions": permissions,
        "selection": selected,
        "source": source,
        **({"member_read_grants": grants} if grants else {}),
    }})
