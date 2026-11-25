import discord


def cached_baselines(guild, requested, *, allows_channel):
    if "GUILD_EMOJIS_UPDATE" in requested:
        yield "GUILD_EMOJIS_UPDATE", guild.id, {
            "emojis": [
                dict(id=e.id, name=e.name, animated=e.animated) for e in guild.emojis
            ],
        }
    if "GUILD_STICKERS_UPDATE" in requested:
        yield "GUILD_STICKERS_UPDATE", guild.id, {
            "stickers": [dict(id=s.id, name=s.name) for s in guild.stickers],
        }
    if "GUILD_UPDATE" in requested:
        yield "GUILD_UPDATE", guild.id, dict(
            name=guild.name,
            verification_level=guild.verification_level.value,
            explicit_content_filter=guild.explicit_content_filter.value,
            default_message_notifications=guild.default_notifications.value,
            afk_timeout=guild.afk_timeout,
        )
    if "GUILD_SCHEDULED_EVENT_UPDATE" in requested:
        for event in guild.scheduled_events:
            yield "GUILD_SCHEDULED_EVENT_UPDATE", event.id, dict(
                name=event.name,
                status=event.status.value,
                scheduled_start_time=event.start_time.isoformat(),
                scheduled_end_time=event.end_time.isoformat() if event.end_time else None,
            )
    if "CHANNEL_UPDATE" in requested:
        for channel in guild.channels:
            if not allows_channel(channel):
                continue
            yield "CHANNEL_UPDATE", channel.id, dict(
                name=channel.name,
                type=channel.type.value,
                parent_id=getattr(channel, "category_id", None),
                topic=getattr(channel, "topic", None),
                nsfw=getattr(channel, "nsfw", False),
                rate_limit_per_user=getattr(channel, "slowmode_delay", 0),
                permission_overwrites=[
                    dict(
                        id=target.id,
                        type=0 if isinstance(target, discord.Role) else 1,
                        allow=str(allow.value), deny=str(deny.value),
                    )
                    for target, overwrite in channel.overwrites.items()
                    for allow, deny in (overwrite.pair(),)
                ],
            )
    if "GUILD_ROLE_UPDATE" in requested:
        for role in guild.roles:
            yield "GUILD_ROLE_UPDATE", role.id, dict(
                name=role.name,
                permissions=str(role.permissions.value),
                color=role.color.value,
                hoist=role.hoist,
                mentionable=role.mentionable,
            )
