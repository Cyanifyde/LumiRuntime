NAMES = (
    "create_instant_invite",
    "kick_members",
    "ban_members",
    "administrator",
    "manage_channels",
    "manage_guild",
    "add_reactions",
    "view_audit_log",
    "priority_speaker",
    "stream",
    "view_channel",
    "send_messages",
    "send_tts_messages",
    "manage_messages",
    "embed_links",
    "attach_files",
    "read_message_history",
    "mention_everyone",
    "use_external_emojis",
    "view_guild_insights",
    "connect",
    "speak",
    "mute_members",
    "deafen_members",
    "move_members",
    "use_voice_activation",
    "change_nickname",
    "manage_nicknames",
    "manage_roles",
    "manage_webhooks",
    "manage_expressions",
    "use_application_commands",
    "request_to_speak",
    "manage_events",
    "manage_threads",
    "create_public_threads",
    "create_private_threads",
    "use_external_stickers",
    "send_messages_in_threads",
    "use_embedded_activities",
    "moderate_members",
    "view_creator_monetization_analytics",
    "use_soundboard",
    "create_expressions",
    "create_events",
    "use_external_sounds",
    "send_voice_messages",
    None,
    "set_voice_channel_status",
    "send_polls",
    "use_external_apps",
    "pin_messages",
    "bypass_slowmode",
)
FLAGS = {name if name else f"permission_bit_{i}": 1 << i for i, name in enumerate(NAMES)}
FLAGS.update({f"permission_bit_{i}": 1 << i for i in range(len(NAMES), 64)})


def labels(value):
    return (
        " · ".join(
            name.replace("_", " ").title() for name, bit in FLAGS.items() if int(value or 0) & bit
        )
        or "None"
    )
