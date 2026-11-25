from lumi.contracts import GuildResourcesSpec

PRIVILEGED_ROLE_PERMISSIONS = (
    "administrator", "manage_guild", "manage_roles", "manage_channels", "ban_members",
    "kick_members", "moderate_members", "manage_webhooks", "manage_messages", "mention_everyone",
)
STAFF_CHANNEL_VIEWER_PERMISSIONS = (
    "administrator", "manage_guild", "manage_messages", "manage_roles", "kick_members",
    "ban_members", "moderate_members",
)
STAFF_CHANNEL_BOT_PERMISSIONS = (
    "view_channel", "read_message_history", "send_messages", "attach_files", "embed_links",
)
SELECTED_ROLE_FACTS = GuildResourcesSpec(
    roles=True, selected_only=True, role_permissions=PRIVILEGED_ROLE_PERMISSIONS,
)
SELECTED_CHANNEL_FACTS = GuildResourcesSpec(
    text_channels=True, selected_only=True, channel_bot_permissions=STAFF_CHANNEL_BOT_PERMISSIONS,
    channel_viewer_permissions=STAFF_CHANNEL_VIEWER_PERMISSIONS,
)
ENFORCEMENT_RESOURCE_FACTS = GuildResourcesSpec(
    roles=True, text_channels=True, role_permissions=PRIVILEGED_ROLE_PERMISSIONS,
    channel_bot_permissions=STAFF_CHANNEL_BOT_PERMISSIONS,
    channel_viewer_permissions=STAFF_CHANNEL_VIEWER_PERMISSIONS,
)
