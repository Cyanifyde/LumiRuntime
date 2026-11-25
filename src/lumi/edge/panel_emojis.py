from __future__ import annotations

import discord


def usable_panel_emoji(
    emoji: discord.Emoji | None,
    channel: discord.TextChannel | discord.Thread,
) -> bool:

    me = channel.guild.me
    if emoji is None or me is None:
        return False
    source = emoji.guild
    if source is None or source.me is None or not emoji.is_usable():
        return False
    return emoji.guild_id == channel.guild.id or channel.permissions_for(me).external_emojis
