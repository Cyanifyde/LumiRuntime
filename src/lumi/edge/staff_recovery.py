from contextlib import suppress

import discord
import msgspec

from lumi.contracts import DiscordIntent
from lumi.contracts.staff_recovery import EDIT_RECOVERY, OBSERVE_RECOVERY, StaffRecoveryTarget, snowflake

from .admission import declared_scope
from .guild_resources import restricted_viewers
from .members import fetch_member
from .message_fetches import action_message_binding
from .registry import module_colour

BASE = frozenset({
    "schema", "action_id", "job_id", "module", "group", "generation", "expires_at_ms",
    "kind", "guild_id", "idempotency_key", "channel_id",
})


def bound_recovery(edge, action, job):
    spec = declared_scope(edge.registry.snapshot, job, "staff_recovery")
    if (
        spec is None or action.kind not in {OBSERVE_RECOVERY, EDIT_RECOVERY}
        or (action.job_id, action.module, action.group, action.generation, action.guild_id)
        != (job.job_id, job.module, job.group, job.generation, job.guild_id)
        or action.channel_id != job.channel_id
    ):
        return None
    try:
        target = msgspec.convert(job.request.arguments, StaffRecoveryTarget)
    except (TypeError, ValueError):
        return None
    if not target.valid(job.channel_id):
        return None
    extra = msgspec.to_builtins(action).keys() - BASE
    if action.kind == OBSERVE_RECOVERY:
        if extra:
            return None
    elif (
        extra != {"message_id", "page", "read_proof"}
        or not snowflake(action.message_id) or not action.read_proof or action.page is None
        or action.page.footer != target.marker or action.page.controls
        or action.page.blocks or action.page.image_url or action.page.thumbnail_url
        or action.page.url or action.page.fields or action.page.file_name
        or action.page.author_name or action.page.author_icon_url or action.page.timestamp_ms is not None
    ):
        return None
    return spec, target


def staff_log(guild, channel, spec):
    if not isinstance(channel, discord.TextChannel) or channel.guild.id != guild.id or not guild.me:
        return False
    permissions = channel.permissions_for(guild.me)
    return all(getattr(permissions, flag, False) for flag in spec.bot_permissions) and restricted_viewers(
        guild, channel, spec.viewer_permissions,
    )


def proof_scope(target, message):
    return "staff_recovery", target.log, message, target.marker


async def execute(edge, action, job, guild):
    def fail(error):
        return edge._action_result(action, False, "failed", error)

    def live():
        now = edge.message_author_proofs.clock()
        return now < min(action.expires_at_ms, job.expires_at_ms) and bound_recovery(edge, action, job)

    scoped = live()
    if not scoped or guild.id != job.guild_id or not guild.me:
        return fail("action_scope_mismatch")
    spec, target = scoped
    log = await edge._action_channel(guild, target.log)
    if action.kind == EDIT_RECOVERY:
        if not live():
            return fail("expired_job")
        if not staff_log(guild, log, spec):
            return fail("staff_log_unavailable")
        if not edge.message_author_proofs.valid_until(action, proof_scope(target, action.message_id)):
            return fail("recovery_proof_expired")
        binding = await action_message_binding(edge.queue_redis, guild.id, action.message_id)
        if binding and (binding["module"] != job.module or binding.get("output_channel") != target.log):
            return fail("message_not_owned")
        if not live():
            return fail("expired_job")
        if not staff_log(guild, log, spec):
            return fail("staff_log_unavailable")
        if not edge.message_author_proofs.valid_until(action, proof_scope(target, action.message_id)):
            return fail("recovery_proof_expired")
        intent = edge.validator.validate(
            DiscordIntent("edit", pages=(action.page,), presentation_mode="legacy"),
            presentation_mode="legacy",
        )
        try:
            await log.get_partial_message(action.message_id).edit(
                embed=edge.layouts.legacy_embed(
                    intent.pages[0], accent_color=module_colour(edge.registry.snapshot, job.module),
                ),
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.NotFound as exc:
            if exc.code == 10008:
                return edge._action_result(action, True, "missing")
            return fail("discord_rejected")
        except discord.Forbidden:
            return fail("discord_rejected")
        except (discord.HTTPException, TimeoutError, OSError):
            return fail("uncertain")
        return edge._action_result(action, True, "edited")

    data = {}
    if target.evidence != "none" and staff_log(guild, log, spec):
        def matches(message):
            return (
                message.author.id == guild.me.id and not getattr(message, "webhook_id", None)
                and any(getattr(embed.footer, "text", None) == target.marker for embed in message.embeds)
            )

        evidence_id = None
        indexed = None
        checked_binding = False
        with suppress(discord.HTTPException):
            if target.evidence in {"confirmed", "legacy"}:
                indexed = await action_message_binding(edge.queue_redis, guild.id, target.evidence_id)
                checked_binding = True
                if indexed is not None:
                    if (indexed["module"], indexed.get("output_channel"), indexed.get("family")) == (
                        job.module, target.log, "legacy",
                    ):
                        evidence_id = target.evidence_id
                else:


                    observed = await log.fetch_message(target.evidence_id)
                    if matches(observed):
                        evidence_id = observed.id
            elif target.evidence == "uncertain":
                found = [message.id async for message in log.history(limit=25) if matches(message)]
                if len(found) == 1:
                    evidence_id = found[0]
        if evidence_id and staff_log(guild, log, spec):
            binding = indexed if checked_binding else await action_message_binding(edge.queue_redis, guild.id, evidence_id)
            if not binding or (binding["module"], binding.get("output_channel")) == (job.module, target.log):
                data["evidence"] = evidence_id
                data["proof"] = edge.message_author_proofs.issue(
                    action, proof_scope(target, evidence_id), deadline=job.expires_at_ms,
                )
    if target.check_roles:
        with suppress(discord.HTTPException):
            member = await fetch_member(edge, guild, target.member, fresh=True)
            if member and member.id == target.member and len(member.roles) <= 250:
                data["roles"] = [role.id for role in member.roles]
    if target.check_ban and guild.me.guild_permissions.ban_members:
        with suppress(discord.HTTPException):
            await guild.fetch_ban(discord.Object(id=target.member))
            data["banned"] = True
    if not live():
        return fail("expired_job")
    return edge._action_result(action, True, "observed", data=data)
