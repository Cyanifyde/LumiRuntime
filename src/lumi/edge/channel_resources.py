import asyncio
import re
import time
from contextlib import AsyncExitStack

import discord
import msgspec
from redis.exceptions import RedisError

from lumi.contracts import DiscordEffectGrant, decode
from lumi.contracts.channel_fence import CRITICAL, INDEX, SCHEMA, SCHEMA_FIELD, TERMINAL
from lumi.contracts.channel_messages import MESSAGE_PAGE
from lumi.contracts.channel_resources import (
    POST_READS, POST_WRITES, READ_CHANNEL_RESOURCES, READS, ROLLBACK_POST_LIMIT, WRITE_CHANNEL_RESOURCE, WRITES,
    configuration_digest, resource_payload,
)
from lumi.shared.discord_effects import channel_operation_lane, effect_key
from lumi.shared.discord_write_journal import MAX_RECONCILE, WriteJournalUnavailable
from lumi.contracts import DiscordActionResult

from . import channel_posts
from .admission import declared_scope
from .channel_configuration import can_read, channel_layout, project
from .channel_fence import FenceUnavailable
from .effect_grants import ApprovedEffect, snowflake
from .members import fetch_member, lookup

BASE = frozenset({
    "schema", "action_id", "job_id", "module", "group", "generation", "expires_at_ms", "kind",
    "guild_id", "idempotency_key", "channel_id", "channel_resource", "intent",
})
_CLAIM = """
local view = redis.call('HMGET', KEYS[3], ARGV[2], ARGV[3])
if view[1] == ARGV[4] and view[2] == ARGV[5]
   and redis.call('GET', KEYS[1]) == ARGV[1] and redis.call('GET', KEYS[2]) == KEYS[1]
then return redis.call('DEL', KEYS[1]) end
return 0
"""


def registered_generations(snapshot):
    groups = {}
    for entry in snapshot.modules.values():
        seen = groups.setdefault(entry.manifest.group, set())
        seen.add(entry.manifest.generation if entry.runtime_state == "online" else None)
    return {group: next(iter(seen)) for group, seen in groups.items() if len(seen) == 1 and None not in seen}


def observed_revisions(edge, guild_id):
    return dict(policy_revision=edge.policy._revisions.get(guild_id, 0),
                reaction_revision=edge.reaction_index.revision)


def resource_scope(snapshot, job, action):
    access = declared_scope(snapshot, job, "channel_resources", actor_callback=True)
    query = action.channel_resource
    if (
        access is None or query is None or query.operation not in access.operations
        or not snowflake(job.guild_id) or not snowflake(job.channel_id) or not snowflake(job.actor_id)
        or (action.job_id, action.module, action.group, action.generation, action.guild_id, action.channel_id)
        != (job.job_id, job.module, job.group, job.generation, job.guild_id, query.target)
        or not snowflake(query.target) or not re.fullmatch(r"[0-9a-f]{8}", query.transition)
        or not msgspec.to_builtins(action).keys() <= BASE
    ):
        return None
    operation = query.operation
    post = operation in POST_READS | POST_WRITES
    if (
        (action.kind == READ_CHANNEL_RESOURCES and operation not in READS)
        or (action.kind == WRITE_CHANNEL_RESOURCE and operation not in WRITES)
        or bool(query.compare) != (operation in WRITES and not post)
        or (query.compare is not None and not re.fullmatch(r"[0-9a-f]{64}", query.compare))
        or bool(query.reason) != (operation in {"find_created", "pin"} or (operation in WRITES and not post))
        or (query.reason is not None and (len(query.reason) > 512 or any(ord(c) < 32 for c in query.reason)))
        or (query.name is not None) != (operation == "clone")
        or (query.name is not None and not 1 <= len(query.name) <= 100)
        or (query.edit is not None) != (operation == "edit")
        or (query.flags is not None) != (operation == "flags")
        or (query.flags is not None and (type(query.flags) is not int or not 0 <= query.flags < 2**32))
        or (operation in WRITES and not re.fullmatch(r"effect:[0-9a-f]{32}", action.idempotency_key))
        or (query.relative_to_peer and (post or operation in {"find_created", "transport_state", "clone"}))
        or not channel_posts.valid(query, action.intent)
        or (query.delivery_state and operation != "transport_state")
        or ((query.companion or query.cleanup) and operation != "observe")
        or (query.cleanup and query.companion)
        or (query.peer_compare is not None and (operation != "delete" or not re.fullmatch(r"[0-9a-f]{64}", query.peer_compare)))
        or (query.only_owned_posts is not None and (
            operation != "delete" or len(query.only_owned_posts) > ROLLBACK_POST_LIMIT
            or len(set(query.only_owned_posts)) != len(query.only_owned_posts)
            or not all(map(snowflake, query.only_owned_posts))
        ))
    ):
        return None
    edit = query.edit
    if edit and (
        not 1 <= len(edit.name) <= 100 or (edit.topic is not None and len(edit.topic) > 1024)
        or any(type(value) is not int or not 0 <= value < 2**31 for value in (
            edit.position, edit.slowmode, edit.archive, edit.thread_slowmode,
        ))
    ):
        return None
    return access


async def _grant(edge, action, job, deadline):
    key = effect_key(job.group, job.job_id, action.idempotency_key)
    raw = await edge.queue_redis.get(key)
    try:
        grant = decode(raw, DiscordEffectGrant) if raw else None
    except msgspec.DecodeError:
        return None
    if not grant or (
        grant.module, grant.generation, grant.kind, grant.guild_id, grant.channel_id, grant.message_id, grant.digest,
    ) != (
        job.module, job.generation, action.kind, job.guild_id, action.channel_id, None,
        resource_payload(action.channel_resource, action.intent),
    ):
        return None
    return ApprovedEffect(None, key, raw, min(deadline, grant.expires_at_ms))


async def confirmed_writes(edge, gate, guild, channel):
    pending = await gate.store.pending_actions(guild, channel)
    for item in pending:
        raw = await edge.queue_redis.get(item["receipt_key"])
        if not raw or not decode(raw, DiscordActionResult).ok:
            return False
        if await gate.store.finish_action(item["id"], False, time.time_ns() // 1_000_000) is False:
            return False
    return len(pending) < MAX_RECONCILE


async def execute(edge, action, job, guild, *, clock=None):
    now = clock or (lambda: time.time_ns() // 1_000_000)

    def response(ok, status, error=None, **data):
        return edge._action_result(action, ok, status, error, data=data)

    spec = resource_scope(edge.registry.snapshot, job, action)
    gate = getattr(edge, "handover", None)
    query = action.channel_resource
    deadline = min(job.expires_at_ms, action.expires_at_ms)
    if not spec or not gate or guild.id != job.guild_id or not guild.me or deadline <= now():
        return response(False, "failed", "action_scope_mismatch")
    operation = query.operation
    post = operation in POST_READS | POST_WRITES
    discarding = query.only_owned_posts is not None
    cleanup = discarding or query.cleanup
    original_panel = operation == "edit_post" and bool(query.post.session)
    native_started = False
    try:
        async with asyncio.timeout(max(0.001, (deadline - now()) / 1000)):
            scopes = await gate.index(guild.id)
            fence = next((record for record in scopes if (
                record["id"] == query.transition and record["old"] == job.channel_id
                and tuple(record.get("executor") or ()) == (job.group, job.module)
                and (record["phase"] not in TERMINAL or original_panel and record["phase"] == "rolled_back")
            )), None)
            if (
                not fence or query.target not in {fence["old"], fence["new"]}
                or (operation in WRITES and fence["phase"] not in CRITICAL and not original_panel)
                or (operation in {"find_created", "clone"} and (query.target != fence["old"] or fence["new"]))
                or (operation == "delete" and not fence["new"])
                or (operation == "delete" and discarding != (query.target == fence["new"]))
                or (cleanup and (fence["phase"] != "rollback" or query.target != fence["new"]))
                or (query.peer_compare is not None and query.target != fence["old"])
                or (post and query.target != (fence["old"] if operation == "inspect_post" or original_panel else fence["new"]))
                or (operation == "transport_state" and query.target != fence["old"])
            ):
                return response(False, "failed", "action_scope_mismatch")
            if operation == "transport_state":
                return response(
                    True, "observed", generations=registered_generations(edge.registry.snapshot),
                    quiet=gate.idle(guild.id, fence["old"], fence.get("new", 0)),
                    phase=fence["phase"], peer=fence.get("new", 0),
                    settled=await confirmed_writes(edge, gate, guild.id, fence["old"]) if query.delivery_state else None,
                    **observed_revisions(edge, guild.id),
                )
            approved = await _grant(edge, action, job, deadline) if operation in WRITES else None
            if operation in WRITES and not approved:
                return response(False, "failed", "effect_not_authorized")
            epoch = getattr(edge.policy, "_epoch", 0)
            with lookup(edge).observe(guild.id, (job.actor_id, guild.me.id)) as changed:
                bot = guild.me
                actor = None
                if not cleanup:
                    actor = await fetch_member(edge, guild, job.actor_id)
                    if not actor or actor.id != job.actor_id or actor.bot:
                        return response(False, "failed", "permission_denied")
                if post:


                    origin = guild.get_channel_or_thread(job.channel_id)
                    target = await edge._action_channel(guild, query.target)
                    authority = origin or target
                    facts = None
                else:
                    layout = await channel_layout(guild)
                    origin = layout.get(job.channel_id)
                    target = layout.get(query.target)
                    authority = origin or layout.get(fence["new"])
                    peer = fence["new"] if query.target == fence["old"] else fence["old"]
                    facts = project(guild, query.target, layout, skip=peer if query.relative_to_peer else 0)
                if cleanup:

                    if origin is None:
                        return response(False, "failed", "peer_missing")
                    if target is None:
                        return response(False, "failed", "channel_unavailable")
                    flags = target.permissions_for(bot)
                    if not (bot.guild_permissions.manage_channels and flags.manage_channels
                            and flags.view_channel and flags.read_message_history):
                        return response(False, "failed", "permission_denied")
                else:
                    if not authority or not can_read(authority, actor, bot) or (
                        target and (getattr(target.guild, "id", 0) != guild.id or not can_read(target, actor, bot))
                    ):
                        return response(False, "failed", "permission_denied")
                    roles, admin = edge._member_access(actor)
                    channels, categories = edge._channel_access(authority)
                    callback = edge.registry.snapshot.scheduled.get((job.module, job.request.route))
                    if not callback or not edge.policy.allowed(
                        guild_id=guild.id, module=job.module, acl_key=callback.spec.acl_key,
                        public=True, role_ids=roles, administrator=admin, channel_ids=channels, category_ids=categories,
                    ):
                        return response(False, "failed", "permission_denied")
                def current():
                    return (
                        not changed[0] and epoch == getattr(edge.policy, "_epoch", 0)
                        and not edge.control.maintenance and not edge.blocklist.contains(job.actor_id)
                        and resource_scope(edge.registry.snapshot, job, action) == spec and now() < deadline
                    )

                if not current():
                    return response(False, "failed", "access_changed")
                if post and (not isinstance(target, discord.TextChannel) or target.type != discord.ChannelType.text):
                    return response(False, "failed", "channel_unavailable")
                if operation in POST_READS:
                    data = await channel_posts.read(edge, job, guild, target, query)
                    return response(True, "observed", **data) if current() else response(False, "failed", "access_changed")
                if operation == "observe":
                    if query.cleanup:
                        return response(True, "observed", compare=configuration_digest(facts["settings"])
                                        if facts and facts["ordinary_text"] and not facts["system_destination"] else None)
                    if query.companion:
                        counterpart = layout.get(peer)
                        if counterpart is not None and not can_read(counterpart, actor, bot):
                            return response(False, "failed", "permission_denied")
                        return response(True, "observed", configuration=facts, companion=project(
                            guild, peer, layout, skip=query.target if query.relative_to_peer else 0,
                        ))
                    return response(True, "observed", configuration=facts)
                if operation == "find_created":
                    if not bot.guild_permissions.view_audit_log:
                        return response(False, "failed", "permission_denied")
                    matches = [entry.target.id async for entry in guild.audit_logs(
                        limit=MESSAGE_PAGE, action=discord.AuditLogAction.channel_create,
                    ) if getattr(entry.user, "id", None) == bot.id and entry.reason == query.reason]
                    return response(True, "observed", channels=matches) if current() else response(
                        False, "failed", "access_changed",
                    )
                if not post and (not facts or not facts["ordinary_text"] or facts["system_destination"]):
                    return response(False, "failed", "channel_unavailable")
                if not post and configuration_digest(facts["settings"]) != query.compare:
                    return response(False, "failed", "resource_changed")
                if query.peer_compare is not None:
                    other = project(guild, peer, layout, skip=query.target if query.relative_to_peer else 0)
                    if not other or not other["ordinary_text"] or other["system_destination"] or (
                        configuration_digest(other["settings"]) != query.peer_compare
                        or not can_read(layout[peer], actor, bot)
                    ):
                        return response(False, "failed", "resource_changed")
                if discarding:
                    posts = await channel_posts.list_facts(guild, target)
                    allowed = set(query.only_owned_posts)
                    if not posts["complete"] or any(not p["owned"] or p["id"] not in allowed for p in posts["messages"]):
                        return response(False, "failed", "foreign_posts")
                lane = channel_operation_lane(job.group, job.module, guild.id, job.channel_id, query.transition)
                async with AsyncExitStack() as stack:
                    apply = await channel_posts.prepare(
                        edge, job, guild, target, query, action.intent, stack,
                        deadline=approved.deadline,
                    ) if post else None
                    if now() >= approved.deadline or not await edge.queue_redis.eval(
                        _CLAIM, 3, approved.key, lane, INDEX,
                        approved.raw, SCHEMA_FIELD, str(guild.id), SCHEMA, msgspec.json.encode(scopes),
                    ):
                        return response(False, "failed", "effect_not_authorized")
                    if not current() or now() >= approved.deadline:
                        return response(False, "failed", "access_changed")
                    if discarding and guild.get_channel_or_thread(fence["old"]) is None:
                        return response(False, "failed", "peer_missing")
                    native_started = True
                    if apply is not None:
                        status, data = await apply()
                        return response(True, status, **data)
                if operation == "clone":
                    created = await target.clone(name=query.name, reason=query.reason)
                    return response(True, "created", channel_id=created.id)
                if operation == "edit":
                    edit = query.edit
                    await target.edit(
                        name=edit.name, topic=edit.topic, position=edit.position, nsfw=edit.nsfw,
                        slowmode_delay=edit.slowmode, default_auto_archive_duration=edit.archive,
                        default_thread_slowmode_delay=edit.thread_slowmode, reason=query.reason,
                    )
                elif operation == "flags":
                    await edge.http.edit_channel(target.id, flags=query.flags, reason=query.reason)
                else:
                    await target.delete(reason=query.reason)
                return response(True, "deleted" if operation == "delete" else "edited", channel_id=target.id)
    except channel_posts.PostDenied as exc:
        return response(False, "failed", str(exc))
    except discord.NotFound as exc:
        return response(False, "failed", "message_not_found" if exc.code == 10008 else "channel_unavailable")
    except discord.Forbidden:
        return response(False, "failed", "permission_denied")
    except discord.HTTPException as exc:
        if exc.status < 500 and exc.status != 408:
            return response(False, "failed", "discord_rejected")
        return response(False, "uncertain" if native_started else "failed", "unavailable")
    except (OSError, TimeoutError, RedisError, FenceUnavailable, WriteJournalUnavailable):
        return response(False, "uncertain" if native_started else "failed", "unavailable")
