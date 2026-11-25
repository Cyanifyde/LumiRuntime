from __future__ import annotations

import asyncio
import time
import uuid

from redis.asyncio import Redis

from lumi.contracts import (
    BoundReplySpec,
    ChannelWatchSpec,
    MentionBatchRef,
    DiscordAction,
    DiscordActionResult,
    EmbedSourceQuery,
    MessageMutation,
    DiscordIntent,
    MemberSearchSpec,
    OwnedHistoryQuery,
    PresentationPage,
    decode,
    encode,
)
from lumi.contracts.role_access import ActorRoleChanges, RoleFactsQuery, RoleMembership, role_write_receipt
from lumi.contracts.channel_messages import ChannelMessageDeletion, ChannelMessageQuery
from lumi.contracts.channel_configuration import ChannelConfigurationQuery
from lumi.contracts.channel_resources import ChannelResourceRequest
from lumi.contracts.layout_message import ATTEMPT as LAYOUT_ATTEMPT, layout_receipt
from lumi.contracts.member_profiles import NicknameMutation
from lumi.contracts.staff_moderation import StaffEffect
from lumi.shared.attachment_payloads import AttachmentUnavailable, action_scope, references
from lumi.shared.discord_effects import effect_receipt_key
from lumi.transport import action_reply_key, enabled, outbound

from .attachment_delivery import ACTIVE_DELIVERY


class ActionUnavailable(RuntimeError):
    pass


class EdgeActionClient:
    STREAM = "lumi:edge-actions"

    def __init__(self, redis: Redis, *, module: str, group: str, generation: int) -> None:
        self._redis = redis
        self._module = module
        self._group = group
        self._generation = generation

    async def read_layout_receipt(self, attempt: str) -> DiscordActionResult | None:
        if not LAYOUT_ATTEMPT.fullmatch(attempt):
            raise ValueError("Invalid layout attempt")
        raw = await self._redis.get(layout_receipt(self._group, self._module, attempt))
        return decode(raw, DiscordActionResult) if raw else None

    async def read_actor_role_receipt(self, attempt: str) -> DiscordActionResult | None:
        raw = await self._redis.get(role_write_receipt(self._group, self._module, attempt))
        return decode(raw, DiscordActionResult) if raw else None

    async def read_effect_receipt(self, attempt: str) -> DiscordActionResult | None:
        raw = await self._redis.get(effect_receipt_key(self._group, self._module, attempt))
        return decode(raw, DiscordActionResult) if raw else None

    async def request(
        self,
        *,
        job_id: str,
        kind: str,
        guild_id: int | None,
        idempotency_key: str,
        channel_id: int | None = None,
        message_id: int | None = None,
        member_id: int | None = None,
        role_id: int | None = None,
        role_ids: tuple[int, ...] = (),
        emoji: str | None = None,
        emoji_keys: tuple[str, ...] = (),
        reason: str | None = None,
        page: PresentationPage | None = None,
        intent: DiscordIntent | None = None,
        content: str | None = None,
        username: str | None = None,
        avatar_url: str | None = None,
        persistent: bool = False,
        reaction_revision: int | None = None,
        forbid_source_channel: bool = False,
        respect_nsfw: bool = False,
        member_ids: tuple[int, ...] = (),
        warning_id: str | None = None,
        message_limit: int | None = None,
        page_number: int | None = None,
        rename_id: str | None = None,
        nickname: str | None = None,
        member_search: MemberSearchSpec | None = None,
        channel_watch: ChannelWatchSpec | None = None,
        mention_batch: MentionBatchRef | None = None,
        bound_reply: BoundReplySpec | None = None,
        read_proof: str | None = None,
        prove_message_author: bool = False,
        target_channel_ids: tuple[int, ...] | None = None,
        message_link_field: str | None = None,
        own_history: OwnedHistoryQuery | None = None,
        embed_sources: EmbedSourceQuery | None = None,
        message_mutation: MessageMutation | None = None,
        timeout_until_ms: int | None = None,
        role_facts: RoleFactsQuery | None = None,
        role_membership: RoleMembership | None = None,
        actor_role_changes: ActorRoleChanges | None = None,
        member_directory_after: int | None = None,
        refresh_member_profile: bool = False,
        nickname_mutation: NicknameMutation | None = None,
        bound_panel_session: str | None = None,
        channel_message_query: ChannelMessageQuery | None = None,
        channel_message_deletion: ChannelMessageDeletion | None = None,
        channel_configuration_query: ChannelConfigurationQuery | None = None,
        staff_effect: StaffEffect | None = None,
        channel_resource: ChannelResourceRequest | None = None,
        wait_seconds: float = 15.0,
    ) -> DiscordActionResult:
        action_id = uuid.uuid4().hex
        action = DiscordAction(
            schema=1,
            action_id=action_id,
            job_id=job_id,
            module=self._module,
            group=self._group,
            generation=self._generation,
            expires_at_ms=int(time.time() * 1000 + wait_seconds * 1000),
            kind=kind,
            guild_id=guild_id,
            idempotency_key=idempotency_key,
            channel_id=channel_id,
            message_id=message_id,
            member_id=member_id,
            role_id=role_id,
            role_ids=role_ids,
            emoji=emoji,
            emoji_keys=emoji_keys,
            reason=reason,
            read_proof=read_proof,
            prove_message_author=prove_message_author,
            target_channel_ids=target_channel_ids,
            message_link_field=message_link_field,
            own_history=own_history,
            embed_sources=embed_sources,
            message_mutation=message_mutation,
            timeout_until_ms=timeout_until_ms,
            role_facts=role_facts,
            role_membership=role_membership,
            actor_role_changes=actor_role_changes,
            member_directory_after=member_directory_after,
            refresh_member_profile=refresh_member_profile,
            nickname_mutation=nickname_mutation,
            bound_panel_session=bound_panel_session,
            channel_message_query=channel_message_query,
            channel_message_deletion=channel_message_deletion,
            channel_configuration_query=channel_configuration_query,
            staff_effect=staff_effect,
            channel_resource=channel_resource,
            page=page,
            intent=intent,
            content=content,
            username=username,
            avatar_url=avatar_url,
            persistent=persistent,
            reaction_revision=reaction_revision,
            forbid_source_channel=forbid_source_channel,
            respect_nsfw=respect_nsfw,
            member_ids=member_ids,
            warning_id=warning_id,
            message_limit=message_limit,
            page_number=page_number,
            rename_id=rename_id,
            nickname=nickname,
            member_search=member_search,
            channel_watch=channel_watch,
            mention_batch=mention_batch,
            bound_reply=bound_reply,
        )
        delivery = ACTIVE_DELIVERY.get()


        transferred = channel_resource is not None and channel_resource.operation == "publish_post"
        if intent is not None and delivery is not None and not transferred:
            if delivery.group != self._group:
                raise ActionUnavailable("Attachment delivery group did not match the action.")
            try:
                if references(intent):
                    await delivery.stage(
                        action_scope(job_id, action_id), intent, deadline=action.expires_at_ms,
                    )
            except AttachmentUnavailable:
                return DiscordActionResult(
                    schema=1,
                    action_id=action_id,
                    ok=False,
                    status="failed",
                    data={},
                    error_code="attachment_unavailable",
                    completed_at_ms=time.time_ns() // 1_000_000,
                )
        result_key = (
            action_reply_key(action_id, self._group)
            if enabled()
            else f"lumi:edge-action-result:{action_id}"
        )
        await self._redis.xadd(outbound(self.STREAM, self._group), {"action": encode(action)})
        async with asyncio.timeout(wait_seconds + 1):
            response = await self._redis.blpop(result_key, timeout=wait_seconds)
        if response is None:
            raise ActionUnavailable("edge action timed out")
        result = decode(response[1], DiscordActionResult)
        if result.action_id != action_id:
            raise ActionUnavailable("edge returned a mismatched action result")
        return result
