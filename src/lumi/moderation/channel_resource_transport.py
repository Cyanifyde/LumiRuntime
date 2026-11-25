import secrets
from contextlib import suppress

from redis.exceptions import RedisError

from lumi.contracts import DiscordActionResult, DiscordEffectGrant, decode, encode
from lumi.contracts.channel_resources import READ_CHANNEL_RESOURCES, READS, WRITE_CHANNEL_RESOURCE, resource_payload
from lumi.shared.discord_effects import (
    EFFECT_GRANT_SECONDS, channel_operation_lane, effect_key, effect_receipt_key, stage_mutation,
)
from lumi.worker.actions import ActionUnavailable


class ChannelResourceTransport:
    def __init__(self, queue, actions, module, group, generation, clock):
        self.queue, self.actions, self.module = queue, actions, module
        self.group, self.generation, self.clock = group, generation, clock

    @staticmethod
    def attempt():
        return "effect:" + secrets.token_hex(16)

    def lane(self, row):
        return channel_operation_lane(self.group, self.module, row["guild"], row["channel"], row["id"])

    async def revoke(self, row):
        await self.queue.delete(self.lane(row))

    async def receipt(self, attempt):
        raw = await self.queue.get(effect_receipt_key(self.group, self.module, attempt))
        return decode(raw, DiscordActionResult) if raw else None

    async def request(self, row, job, query, *, attempt=None, authorize=None, intent=None):
        read = query.operation in READS
        kind = READ_CHANNEL_RESOURCES if read else WRITE_CHANNEL_RESOURCE
        if not read:


            if not attempt or authorize is None:
                raise ValueError("A native channel mutation requires a checkpointed worker attempt")
            deadline = min(row["lease_until"], self.clock() + EFFECT_GRANT_SECONDS * 1000)
            if deadline <= self.clock():
                return DiscordActionResult(1, "", False, "failed", error_code="expired_job")
            key = effect_key(self.group, job, attempt)
            grant = DiscordEffectGrant(
                self.module, self.generation, kind, row["guild"], query.target, None, resource_payload(query, intent), deadline,
            )
            if not await stage_mutation(self.queue, key, self.lane(row), encode(grant)):
                return DiscordActionResult(1, "", False, "failed", error_code="effect_not_authorized")
            try:
                await authorize(row)
            except BaseException:
                with suppress(RedisError):
                    await self.queue.delete(key)
                raise
        try:
            return await self.actions.request(
                kind=kind, job_id=job, idempotency_key=attempt or self.attempt(), guild_id=row["guild"],
                channel_id=query.target, channel_resource=query, **({"intent": intent} if intent is not None else {}),
            )
        except (ActionUnavailable, RedisError, TimeoutError, OSError):
            return DiscordActionResult(1, "", False, "failed" if read else "uncertain", error_code="unavailable")
