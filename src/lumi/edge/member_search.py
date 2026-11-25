from __future__ import annotations

import discord
import msgspec

from lumi.contracts import MemberSearchResult
from lumi.members.search import (
    MAX_QUERY,
    TOKEN,
    MemberIdentity,
    identifier,
    match_name,
    search,
)
from lumi.ui_keys import read_ui

from .admission import admitted
from .members import fetch_member


def identity(member):
    return MemberIdentity(member.id, member.name, member.global_name, member.nick)


async def execute(edge, action, job, guild):
    def result(value):
        return edge._action_result(action, True, "searched", data=msgspec.to_builtins(value))

    def denied():
        return edge._action_result(action, False, "failed", "invalid_member_search")

    spec = action.member_search
    if (
        spec is None
        or len(spec.query) > MAX_QUERY
        or "\n" in spec.query
        or "\r" in spec.query
        or action.guild_id != job.guild_id
        or guild.id != job.guild_id
        or action.channel_id not in {None, job.channel_id}
    ):
        return denied()
    channel = guild.get_channel_or_thread(job.channel_id)
    if channel is None:
        return denied()
    try:
        actor = await fetch_member(edge, guild, job.actor_id)
    except discord.NotFound:
        return edge._action_result(action, False, "failed", "policy_denied")
    channels, categories = edge._channel_access(channel)
    roles, administrator = edge._member_access(actor)
    if (
        not channel.permissions_for(actor).view_channel
        or edge.blocklist.contains(actor.id)
        or (
            not admitted(job, channel_id=channel.id)
            and not edge.policy.allowed(
                guild_id=guild.id,
                module=job.module,
                acl_key=edge._job_acl_key(job),
                public=edge._job_public(job),
                role_ids=roles,
                administrator=administrator,
                channel_ids=channels,
                category_ids=categories,
            )
        )
    ):
        return edge._action_result(action, False, "failed", "policy_denied")
    selected = spec.selected_id
    if selected is not None:
        token = str(job.request.arguments.get("session") or "")
        values = job.request.arguments.get("values")
        namespace, _, component = job.request.route.partition(".")
        registration = edge.registry.snapshot.components.get((namespace, component))
        if (
            registration is None or not registration.spec.native_selection
            or (registration.module, registration.group, registration.generation)
            != (job.module, job.group, job.generation)
            or registration.spec.handler != job.request.handler
            or job.request.event.get("dispatch") != "component"
            or not TOKEN.fullmatch(token) or list(values or ()) != [str(selected)]
            or job.request.event.get("_native_selection") != [str(selected)]
        ):
            return denied()


        bound = await read_ui(edge.cache_redis, "message", token, job.group)
        if not bound or int(bound) != int(job.request.event.get("message_id") or 0):
            return denied()
    elif spec.query != str(job.request.arguments.get("query") or "").strip():
        return denied()

    async def lookup_identity(uid, match):
        if edge.blocklist.contains(uid):
            return MemberSearchResult("not_found")
        try:
            member = actor if uid == actor.id else await fetch_member(edge, guild, uid)
        except discord.NotFound:
            return MemberSearchResult("not_found")
        current = identity(member)
        if spec.query and identifier(spec.query) is None:
            score = match_name(spec.query, current)
            if score is None or (selected is None and score[0] == 3):
                return MemberSearchResult("changed")
            match = ("exact", "prefix", "substring", "typo")[score[0]]
        return MemberSearchResult("matched", (current.candidate(match),))

    if selected is not None:
        return result(await lookup_identity(selected, "selected"))
    direct = identifier(spec.query) if spec.query else job.actor_id
    if direct is not None:
        return result(await lookup_identity(direct, "exact"))
    members = await edge._guild_members(guild)
    return result(search(
        spec.query, (identity(m) for m in members if not edge.blocklist.contains(m.id)),
    ))
