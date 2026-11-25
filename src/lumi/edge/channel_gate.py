from __future__ import annotations

import time
from collections import Counter
from contextlib import asynccontextmanager
from types import SimpleNamespace

from redis.exceptions import RedisError

from lumi.contracts import ChannelTransitionSpec, DiscordActionResult
from lumi.contracts.channel_configuration import READ_CHANNEL_CONFIGURATION
from lumi.contracts.channel_fence import DEFAULT_TRANSITION, blocked
from lumi.contracts.channel_messages import DELETE_CHANNEL_MESSAGES, READ_CHANNEL_MESSAGES
from lumi.contracts.channel_resources import READ_CHANNEL_RESOURCES, WRITE_CHANNEL_RESOURCE
from lumi.contracts.staff_moderation import STAFF_EFFECT, STAFF_READS
from lumi.contracts.staff_recovery import OBSERVE_RECOVERY
from lumi.shared.attachment_payloads import AttachmentUnavailable
from lumi.shared.discord_write_journal import WriteJournalUnavailable
from lumi.shared.request_scope import memo_read

from .channel_fence import FenceUnavailable


UNBOUND_TRANSITION = ChannelTransitionSpec(quiesce_guild=True)


class ChannelGate:
    def __init__(self, store, registry=None, *, barrier=None):
        self.store, self.channels, self.guilds = store, Counter(), Counter()
        self.barrier = store if barrier is None else barrier
        self.registry = registry

    def declared(self, module, group, generation):
        if self.registry is None:
            return DEFAULT_TRANSITION
        entry = self.registry.snapshot.modules.get(module)
        if entry is None or (entry.manifest.group, entry.manifest.generation) != (group, generation):
            return UNBOUND_TRANSITION
        return getattr(entry.manifest, "channel_transition", None) or DEFAULT_TRANSITION

    def control_command(self, root):
        registered = self.registry.snapshot.commands.get(root) if self.registry else None
        if registered is None:
            return False
        spec = self.declared(registered.module, registered.group, registered.generation)
        return registered.spec.handler in spec.control_handlers

    async def index(self, guild):
        return await memo_read("channel_fence", (self, guild), lambda: self.barrier.index(guild))

    @asynccontextmanager
    async def job(self, job, destination=None, *, transition=None):
        spec = self.declared(job.module, job.group, job.generation)
        control = transition is not None or job.request.handler in spec.control_handlers
        keys = {v for v in (job.channel_id, destination) if v}
        if not control:
            self.channels.update(keys)
            if spec.quiesce_guild:
                self.guilds[job.guild_id] += 1
        try:

            try:
                index = await self.index(job.guild_id)
            except FenceUnavailable:
                yield False
            else:
                if transition:


                    index = tuple(g for g in index if not (
                        g["id"] == transition and g["old"] == job.channel_id
                        and tuple(g.get("executor") or ()) == (job.group, job.module)
                        and g["phase"] not in {"complete", "rolled_back"}
                    ))
                yield not blocked(index, job, destination, spec=spec)
        finally:
            if not control:
                self.channels.subtract(keys)
                if spec.quiesce_guild:
                    self.guilds[job.guild_id] -= 1

    def idle(self, guild, old, new=0):
        return not self.guilds[guild] and not self.channels[old] and not self.channels[new]

    async def ingress_blocked(self, guild, channel):
        try:
            index = await self.barrier.index(guild)
        except FenceUnavailable:
            return True
        return any(
            g["phase"] != "rolled_back"
            and (channel == g["old"] or (g["phase"] != "complete" and channel == g.get("new")))
            for g in index
        )

    async def execute(self, action, job, request):

        if (
            action.kind
            in {
                "resolve_members",
                "fetch_message",
                "read_interaction_context",
                "read_guild_sources",
                "read_actor_post_channels",
                "read_owned_history",
                "read_channel_members",
                "read_embed_sources",
                "read_guild_roles",
                "read_mention_batch_members",
                READ_CHANNEL_MESSAGES,
                READ_CHANNEL_CONFIGURATION,
                READ_CHANNEL_RESOURCES,
                OBSERVE_RECOVERY,
            }
            or not job.guild_id
            or (action.kind == STAFF_EFFECT and action.staff_effect and action.staff_effect.operation in STAFF_READS)
        ):
            return await request()
        now = int(time.time() * 1000)
        try:
            code = await self.store.begin_action(action, job, now)
        except (RedisError, WriteJournalUnavailable):
            return DiscordActionResult(1, action.action_id, False, "failed", error_code="write_journal_unavailable")
        if not code:
            return DiscordActionResult(1, action.action_id, False, "failed", error_code="ambiguous")
        definite = {
            "permission_denied",
            "channel_unavailable",
            "guild_unavailable",
            "member_unavailable",
            "role_unavailable",
            "emoji_unavailable",
            "message_not_editable",
            "message_not_found",
            "message_not_owned",
            "effect_not_authorized",
            "discord_rejected",
            "source_verification_expired",
            "action_scope_mismatch",
            "policy_denied",
            "blocked",
            "stale_generation",
            "expired_job",
            "unknown_job",
            "invalid_action",
            "attachment_unavailable",
            "invalid_reason",
            "target_protected",
            "panel_expired",
        }
        if action.kind == DELETE_CHANNEL_MESSAGES:
            definite.update({"rate_limited", "too_old", "access_changed", "resource_unavailable", "unavailable"})
        if action.kind == STAFF_EFFECT:
            definite.update({
                "discord_target_missing", "member_exempt", "role_unavailable", "staff_log_unavailable",
                "stale_source", "terminal_unavailable",
            })
        if action.kind == WRITE_CHANNEL_RESOURCE:
            definite.update({"resource_changed", "access_changed", "unavailable", "foreign_posts",
                             "peer_missing", "presentation_unavailable"})
        if min(action.expires_at_ms, job.expires_at_ms) <= int(time.time() * 1000):
            await self.store.finish_action(code, False, int(time.time() * 1000))
            return DiscordActionResult(1, action.action_id, False, "failed", error_code="expired_job")
        try:
            result = await request()
        except BaseException:
            await self.store.finish_action(code, True, now)
            raise
        await self.store.finish_action(
            code, result.status == "uncertain" or (not result.ok and result.error_code not in definite),
            int(time.time() * 1000),
        )
        return result

    async def result(self, job, result, request):
        if not job.guild_id or not result.intents:
            return await request()
        marker = SimpleNamespace(
            module=job.module, group=job.group, kind="result", job_id=job.job_id,
            idempotency_key="result:" + job.job_id, channel_id=job.channel_id,
        )
        code = await self.store.begin_action(marker, job, int(time.time() * 1000))
        if not code:
            return
        if job.expires_at_ms <= int(time.time() * 1000):
            await self.store.finish_action(code, False, int(time.time() * 1000))
            return
        try:
            value = await request()
        except AttachmentUnavailable:
            await self.store.finish_action(code, False, int(time.time() * 1000))
            raise
        except BaseException:
            await self.store.finish_action(code, True, int(time.time() * 1000))
            raise
        await self.store.finish_action(code, False, int(time.time() * 1000))
        return value
