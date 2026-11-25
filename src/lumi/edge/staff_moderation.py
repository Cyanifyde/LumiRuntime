from types import SimpleNamespace

import discord
import msgspec

from lumi.contracts import DiscordEffectGrant, decode
from lumi.contracts.staff_moderation import STAFF_EFFECT, policy_lane, policy_token
from lumi.shared.discord_effects import effect_key, source_digest

from .attachment_delivery import uploads
from .guild_resources import has_any, restricted_viewers, role_manageable
from .intents import InvalidIntent
from .members import fetch_member, member_mutation
from .message_fetches import action_message_binding, fetch_message, remember_owned_output
from .registry import module_colour

_CLAIM = (
    "if redis.call('GET',KEYS[1]) == ARGV[1] and redis.call('GET',KEYS[2]) == ARGV[2] "
    "then return redis.call('DEL',KEYS[1]) end return 0"
)
BASE = frozenset({
    "schema", "action_id", "job_id", "module", "group", "generation", "expires_at_ms", "kind",
    "guild_id", "idempotency_key", "channel_id", "message_id", "member_id", "staff_effect",
})


def registered(scope, job):
    for candidates in scope.passive.values():
        for item in candidates:
            if (item.module, item.group, item.generation, item.spec.route, item.spec.handler) == (
                job.module, job.group, job.generation, job.request.route, job.request.handler,
            ) and item.spec.staff_moderation is not None:
                return item
    return None


def ready(action, job, route):
    data = action.staff_effect
    return bool(
        route and data and action.kind == STAFF_EFFECT
        and data.operation in route.spec.staff_moderation.operations
        and (action.module, action.group, action.generation, action.job_id, action.guild_id, action.channel_id)
        == (job.module, job.group, job.generation, job.job_id, job.guild_id, job.channel_id)
        and job.request.event.get("dispatch") == "passive"
        and action.member_id == job.actor_id and action.message_id == job.request.event.get("message_id")
        and all(type(value) is int and 0 < value < 2**64 for value in (
            job.guild_id, job.channel_id, action.member_id, action.message_id, data.log, data.revision,
        ))
        and msgspec.to_builtins(action).keys() <= BASE | {"intent"}
        and (action.intent is not None) == (data.operation in {"send", "edit"})
        and (data.role is not None) == (data.operation in {"add_role", "remove_role"})
        and (data.evidence is not None) == (data.operation == "edit")
        and all(type(v) is int and 0 < v < 2**64 for v in (
            *data.inspect_roles, *(v for v in (data.role, data.evidence) if v is not None),
        ))
        and bool(data.proof) == (data.operation == "delete")
        and (not data.inspect_roles or data.operation == "inspect")
        and len(data.inspect_roles) <= 250 and len(data.reason) <= 128
    )


def source_scope(key, version):
    return "staff_source", key, version


def staff_channel(channel, guild, spec):
    if not isinstance(channel, discord.TextChannel) or channel.guild.id != guild.id:
        return False
    flags = channel.permissions_for(guild.me)
    return all(getattr(flags, flag, False) for flag in spec.log_permissions) and restricted_viewers(
        guild, channel, spec.viewer_permissions,
    )


def exempt(guild, member):
    return member.bot or member.id == guild.owner_id or member.guild_permissions.administrator


def source_authored_by(source, guild, member):
    return bool(
        source.guild and source.guild.id == guild.id and source.author.id == member.id
        and not getattr(source, "webhook_id", None) and not exempt(guild, member)
    )


async def execute(edge, action, job, guild):
    def result(ok, status, error=None, **data):
        return edge._action_result(action, ok, status, error, data=data)

    now = edge.message_author_proofs.clock
    route = registered(edge.registry.snapshot, job)
    if not ready(action, job, route) or guild.id != job.guild_id or not guild.me:
        return result(False, "failed", "action_scope_mismatch")
    data = action.staff_effect
    spec = route.spec.staff_moderation
    operation = data.operation
    source_key = (job.channel_id, action.message_id)
    deadline = min(action.expires_at_ms, job.expires_at_ms)

    def current():
        return (
            now() < deadline and registered(edge.registry.snapshot, job) is not None
            and not edge.control.maintenance and not edge.blocklist.contains(job.actor_id)
            and edge.policy.module_enabled(job.guild_id, job.module)
        )

    try:
        log = await edge._action_channel(guild, data.log)
        channel = await edge._action_channel(guild, job.channel_id)
        if not current() or not staff_channel(log, guild, spec):
            return result(False, "failed", "staff_log_unavailable")
        if not channel or channel.id != job.channel_id or not channel.permissions_for(guild.me).manage_messages:
            return result(False, "failed", "permission_denied")
        member = None
        if operation != "edit":
            member = await fetch_member(edge, guild, job.actor_id, fresh=operation in {"add_role", "remove_role"})
            if not member or member.id != job.actor_id or exempt(guild, member):
                return result(False, "failed", "member_exempt")
        if operation == "inspect":
            roles = {role.id: role for role in member.roles}
            found = {identifier: guild.get_role(identifier) for identifier in (*roles, *data.inspect_roles)}
            manageable = [key for key, role in found.items() if role_manageable(guild, role)]
            grantable = [
                key for key in manageable
                if not has_any(found[key].permissions, spec.protected_role_permissions)
            ]
            return result(
                True, "observed", roles=list(roles), manageable=manageable, grantable=grantable,
                manage_roles=bool(guild.me.guild_permissions.manage_roles),
            )
        if operation == "observe_source":
            version = edge.source_versions.watch(source_key, deadline)
            source = await fetch_message(edge, channel, action.message_id)
            changed = getattr(source, "edited_at", None) or source.created_at
            if (
                not source_authored_by(source, guild, member)
                or edge.source_versions.version(source_key) != version
            ):
                return result(False, "failed", "stale_source")
            event = {
                "message_id": source.id, "guild_id": guild.id, "channel_id": channel.id,
                "actor_id": member.id, "author_id": member.id, "author_bot": False,
                "administrator": False, "webhook_id": None, "content": source.content or "",
                "attachments": [{"url": item.url, "id": item.id} for item in source.attachments],
            }
            by_source = SimpleNamespace(passive={(route.spec.event, None): (route,)})
            matches = edge.matcher.match_passive(by_source, event_type=route.spec.event, event=event)
            if not matches or not current():
                return result(False, "failed", "stale_source")
            return result(
                True, "observed", revision=int(changed.timestamp() * 1000),
                arguments=matches[0].request.arguments,
                proof=edge.message_author_proofs.issue(action, source_scope(source_key, version), deadline=deadline),
            )
        key = effect_key(job.group, job.job_id, action.idempotency_key)
        lane = policy_lane(job.group, job.module, job.guild_id)
        raw = await edge.queue_redis.get(key)
        grant = decode(raw, DiscordEffectGrant) if raw else None
        if (
            grant is None or (grant.module, grant.generation, grant.kind, grant.guild_id,
                              grant.channel_id, grant.message_id, grant.digest) != (
                job.module, job.generation, STAFF_EFFECT, job.guild_id, job.channel_id,
                action.message_id, source_digest((data, action.intent)),
            )
            or now() >= min(deadline, grant.expires_at_ms)
        ):
            return result(False, "failed", "effect_not_authorized")
        intent = action.intent
        if operation in {"send", "edit"}:
            if (
                intent.kind != operation or intent.presentation_mode != "legacy"
                or len(intent.pages) != 1 or intent.pages[0].controls or intent.controls
                or intent.content or intent.mention_actor or (operation == "edit" and intent.attachments)
                or (operation == "send" and (
                    len(intent.attachments) != 1 or not intent.attachments[0].file_name.startswith("SPOILER_")
                ))
            ):
                return result(False, "failed", "invalid_action")
            intent = edge.validator.validate(intent, presentation_mode="legacy", scheduled_files=True)
        if operation == "edit":
            binding = await action_message_binding(edge.queue_redis, guild.id, data.evidence)
            if not binding or (binding["module"], binding.get("output_channel"), binding.get("panel")) != (
                job.module, data.log, "staff:" + job.job_id,
            ):
                return result(False, "failed", "message_not_owned")
        if not await edge.queue_redis.eval(_CLAIM, 2, key, lane, raw, policy_token(job.generation, data.revision)):
            return result(False, "failed", "effect_not_authorized")
        if not current() or not staff_channel(log, guild, spec):
            return result(False, "failed", "permission_denied")
        if operation in {"send", "edit"}:
            embed = edge.layouts.legacy_embed(
                intent.pages[0], accent_color=module_colour(edge.registry.snapshot, job.module),
            )
            if operation == "edit":
                await log.get_partial_message(data.evidence).edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                return result(True, "edited")
            async with uploads(edge, intent, job, action=action, deadline=grant.expires_at_ms) as files:
                if not current() or not staff_channel(log, guild, spec):
                    return result(False, "failed", "permission_denied")
                evidence = await log.send(
                    embed=embed, file=files[0], allowed_mentions=discord.AllowedMentions.none(),
                )
            await remember_owned_output(
                edge.queue_redis, guild_id=guild.id, channel_id=log.id, message_id=evidence.id,
                module=job.module, family="legacy", panel="staff:" + job.job_id,
            )
            return result(True, "sent", evidence=evidence.id)


        member = await fetch_member(edge, guild, job.actor_id)
        if not member or exempt(guild, member) or not current():
            return result(False, "failed", "member_exempt")
        if operation == "delete":
            version = edge.source_versions.version(source_key)
            if not version or not edge.message_author_proofs.with_proof(
                action, data.proof, source_scope(source_key, version),
            ) or not channel.permissions_for(guild.me).manage_messages:
                return result(False, "failed", "stale_source")
            await channel.get_partial_message(action.message_id).delete(reason=data.reason)
            return result(True, "deleted", deleted_source=[guild.id, channel.id, action.message_id])
        if operation in {"add_role", "remove_role"}:
            role = guild.get_role(data.role)
            if not role_manageable(guild, role) or (
                operation == "add_role" and has_any(role.permissions, spec.protected_role_permissions)
            ):
                return result(False, "failed", "role_unavailable", skipped=True)
            adding = operation == "add_role"
            if adding != (role in member.roles):
                with member_mutation(edge, guild, member.id):
                    method = member.add_roles if adding else member.remove_roles
                    await method(role, reason=data.reason, atomic=True)
            return result(True, "added" if adding else "removed")
        if operation in {"kick", "ban"}:
            if (
                guild.me.top_role <= member.top_role
                or not getattr(guild.me.guild_permissions, operation + "_members", False)
            ):
                return result(False, "failed", "terminal_unavailable")
            with member_mutation(edge, guild, member.id):
                if operation == "ban":
                    await guild.ban(member, reason=data.reason, delete_message_seconds=0)
                else:
                    await member.kick(reason=data.reason)
            return result(True, operation)
    except InvalidIntent:
        return result(False, "failed", "invalid_action")
    except discord.NotFound:
        return result(False, "failed", "discord_target_missing")
    except discord.HTTPException as exc:
        if exc.status >= 500:
            return result(False, "uncertain", "discord_unavailable")
        return result(False, "failed", "discord_rejected")
    except (TimeoutError, OSError):
        return result(False, "uncertain", "discord_unavailable")
    return result(False, "failed", "invalid_action")
