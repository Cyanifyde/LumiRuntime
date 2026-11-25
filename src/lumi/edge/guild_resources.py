import discord


def role_manageable(guild, role):
    me = guild.me
    return bool(
        role and me and me.guild_permissions.manage_roles
        and not role.is_default() and not role.managed and me.top_role > role
    )


def has_any(permissions, flags):
    return any(getattr(permissions, flag, False) for flag in flags)


def restricted_viewers(guild, channel, permissions):
    if channel.permissions_for(guild.default_role).view_channel:
        return False

    def permitted(subject):
        return (
            getattr(subject, "bot", False)
            or getattr(subject, "managed", False)
            or subject.id == guild.owner_id
            or has_any(
                getattr(subject, "guild_permissions", getattr(subject, "permissions", None)),
                permissions,
            )
        )

    return not any(
        channel.permissions_for(role).view_channel and not permitted(role)
        for role in getattr(guild, "roles", ())
    ) and not any(
        getattr(overwrite, "view_channel", None) is True and not permitted(subject)
        for subject, overwrite in getattr(channel, "overwrites", {}).items()
    )


def project(guild, spec, values):
    if guild is None:
        return {}
    result = {}
    if spec.roles:
        roles = (
            (guild.get_role(identifier) for identifier in values)
            if spec.selected_only else guild.roles[:250]
        )
        result["roles"] = [
            {
                "id": role.id,
                "manageable": role_manageable(guild, role),
                "permissions": [
                    flag for flag in spec.role_permissions if getattr(role.permissions, flag, False)
                ],
            }
            for role in roles if role is not None
        ]
    if spec.text_channels:
        channels = (
            (
                channel for identifier in values
                if isinstance(channel := guild.get_channel(identifier), discord.TextChannel)
            )
            if spec.selected_only else guild.text_channels[:250]
        )
        allowed = []
        for channel in channels:
            if spec.channel_bot_permissions:
                if guild.me is None:
                    continue
                permissions = channel.permissions_for(guild.me)
                if not all(getattr(permissions, flag, False) for flag in spec.channel_bot_permissions):
                    continue
            if spec.channel_viewer_permissions and not restricted_viewers(
                guild, channel, spec.channel_viewer_permissions,
            ):
                continue
            allowed.append(channel.id)
        result["text_channel_ids"] = allowed
    return result
