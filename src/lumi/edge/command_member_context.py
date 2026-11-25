from __future__ import annotations

import discord

from .members import fetch_member


async def hydrate(edge, route, message, captures):
    guild, actor = message.guild, message.author
    identifiers = [route.request.arguments.get(name) for name in captures]
    targets = {identifier for identifier in identifiers if type(identifier) is int and identifier > 0}
    if guild is None or len(targets) != 1 or not hasattr(actor, "top_role"):
        return
    identifier = targets.pop()
    event = route.request.event
    try:
        member = await fetch_member(edge, guild, identifier)
    except discord.NotFound:
        member = None
    except discord.HTTPException:
        return
    if member is None:
        event["member_target"] = {"id": identifier, "present": False}
        return
    event["member_target"] = dict(
        id=member.id,
        present=True,
        display_name=str(member.display_name)[:80],
        actor_owns_guild=actor.id == guild.owner_id,
        target_owns_guild=member.id == guild.owner_id,
        target_is_client=member.id == getattr(edge.user, "id", 0),
        actor_outranks_target=actor.top_role > member.top_role,
    )
