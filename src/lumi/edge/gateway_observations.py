from lumi.shared.discord_observations import Observation, identifier, project


def observation(edge, admission, guild_id, name, raw, *, order, now) -> Observation | None:
    actor = int((raw.get("author") or raw.get("user") or {}).get("id") or raw.get("user_id") or 0)
    if not guild_id or (actor and edge.blocklist.contains(actor)):
        return None
    if (
        name == "GUILD_AUDIT_LOG_ENTRY_CREATE"
        and raw.get("target_id")
        and edge.blocklist.contains(int(raw["target_id"]))
    ):
        return None
    is_message = name.startswith("MESSAGE_")
    automated = False
    if is_message:
        author = raw.get("author") or {}
        if actor == getattr(edge.user, "id", None):
            return None
        automated = bool(author.get("bot") or raw.get("webhook_id"))
    guild = edge.get_guild(guild_id)
    if guild is None:
        return None
    channel_id = int(
        raw.get("channel_id")
        or (raw.get("id") if name.startswith(("CHANNEL_", "THREAD_")) else 0)
        or 0
    )
    old_voice = (
        getattr(guild, "voice_states", {}).get(actor) if name == "VOICE_STATE_UPDATE" else None
    )
    scopes = []
    if name == "VOICE_STATE_UPDATE" and old_voice and old_voice.channel:
        old = old_voice.channel
        scopes.extend((old.id, getattr(old, "category_id", None)))
        if not old.permissions_for(guild.me).view_channel:
            return None
        channel_id = channel_id or old.id
    channel = guild.get_channel_or_thread(channel_id) if channel_id else None
    if channel_id:
        scopes.extend((
            channel_id,
            getattr(channel, "parent_id", None),
            getattr(channel, "category_id", None),
        ))
        if (
            channel
            and getattr(guild, "me", None)
            and not channel.permissions_for(guild.me).view_channel
        ):
            return None
    if not admission.permits(guild_id, channels=scopes, automated=automated):
        return None
    rid = int(
        raw.get("id")
        or (raw.get("role") or {}).get("id")
        or raw.get("role_id")
        or actor
        or channel_id
        or guild_id
    )
    if name.startswith("GUILD_MEMBER_") or name == "VOICE_STATE_UPDATE":
        rid = actor
    if name.startswith(("GUILD_EMOJIS_", "GUILD_STICKERS_")):
        rid = guild_id
    payload = project(name, raw, role_name=lambda role: cached_role_name(guild, role))
    payload["channel_name"] = str(getattr(channel, "name", "") or "")
    payload.setdefault("nsfw", bool(getattr(channel, "nsfw", False)))
    if channel:
        payload["parent_id"] = getattr(channel, "category_id", None) or getattr(
            channel, "parent_id", None
        )
    if name == "VOICE_STATE_UPDATE" and channel_id:
        payload["voice_name"] = str(getattr(channel, "name", channel_id))
    if name == "VOICE_STATE_UPDATE":
        previous = old_voice
        if previous:
            payload["old_voice_name"] = previous.channel.name if previous.channel else ""
            payload["old_voice_parent_id"] = getattr(previous.channel, "category_id", None)
            payload["_voice_before"] = {
                key: getattr(previous, key, False)
                for key in (
                    "self_mute",
                    "self_deaf",
                    "mute",
                    "deaf",
                    "self_video",
                    "self_stream",
                    "suppress",
                )
            }
            payload["_voice_before"]["channel_id"] = (
                previous.channel.id if previous.channel else None
            )
    if name.startswith(("CHANNEL_", "THREAD_")):
        payload["resource_type"] = (
            "Thread" if name.startswith("THREAD") else str(raw.get("type", "Channel"))
        )
        parent = guild.get_channel(int(raw.get("parent_id") or 0))
        payload["parent"] = str(getattr(parent, "name", "") or "")
    ws = getattr(edge, "ws", None)
    source = str(getattr(ws, "session_id", "")) + ":" + str(getattr(ws, "sequence", order))

    return Observation(
        identifier(guild_id, source, name, rid),
        guild_id,
        name,
        rid,
        now,
        order,
        channel_id,
        0,
        payload,
    )


def cached_role_name(guild, role_id):
    name = getattr(guild.get_role(role_id), "name", None)
    return name if isinstance(name, str) else None
