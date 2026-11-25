import secrets
from contextlib import suppress

from redis.exceptions import RedisError

from lumi.contracts import DiscordActionResult, DiscordEffectGrant, PresentationPage, decode, encode
from lumi.contracts.channel_configuration import READ_CHANNEL_CONFIGURATION, ChannelConfigurationQuery
from lumi.contracts.channel_messages import (
    DELETE_CHANNEL_MESSAGES, MESSAGE_PAGE, READ_CHANNEL_MESSAGES, ChannelMessageDeletion, ChannelMessageQuery,
)
from lumi.shared.discord_effects import (
    EFFECT_GRANT_SECONDS, UPDATE_BOUND_PANEL, channel_operation_lane, effect_key, effect_receipt_key,
    mutation_lane, panel_payload, source_digest, stage_mutation,
)
from lumi.worker.actions import ActionUnavailable

MODULE = "moderation"


class PurgeChannelTransport:
    def __init__(self, queue, actions, group, generation, clock):
        self.queue, self.actions = queue, actions
        self.group, self.generation, self.clock = group, generation, clock

    @staticmethod
    def attempt():
        return "effect:" + secrets.token_hex(16)

    async def request(self, row, job, operation, key, **params):
        return await self.actions.request(
            kind=operation, job_id=job, idempotency_key=key,
            guild_id=row["guild"], channel_id=row["channel"], **params,
        )

    async def configuration(self, job, guild, origin, *, target=None, layout=True):
        return await self.request(
            {"guild": guild, "channel": origin}, job, READ_CHANNEL_CONFIGURATION, self.attempt(),
            channel_configuration_query=ChannelConfigurationQuery(target or origin, layout),
        )

    async def history(self, row, job, before, *, limit=MESSAGE_PAGE, content=False, author_roles=False):
        return await self.request(
            row, job, READ_CHANNEL_MESSAGES, self.attempt(),
            channel_message_query=ChannelMessageQuery(before=before, limit=limit, content=content, author_roles=author_roles),
        )

    async def observe(self, row, job, message, *, content=False, author_roles=False):
        return await self.request(
            row, job, READ_CHANNEL_MESSAGES, self.attempt(),
            channel_message_query=ChannelMessageQuery(message=message, content=content, author_roles=author_roles),
        )

    def checkpoint_attempt(self, row, ids, *, kind, deadline):

        return dict(
            kind=kind, ids=list(ids), native_attempt=self.attempt(),
            native_until=min(deadline, row["lease_until"], self.clock() + EFFECT_GRANT_SECONDS * 1000),
        )

    def lane(self, row):
        return channel_operation_lane(self.group, MODULE, row["guild"], row["channel"], row["id"])

    async def revoke(self, row):
        keys = [self.lane(row)]
        if row.get("message"):
            keys.append(mutation_lane(self.group, MODULE, row["guild"], row["channel"], row["message"]))
        await self.queue.delete(*keys)

    async def receipt(self, attempt):
        address = effect_receipt_key(self.group, MODULE, attempt)
        raw = await self.queue.get(address)
        return decode(raw, DiscordActionResult) if raw is not None else None

    async def pending(self, staged):
        if self.clock() < staged["native_until"]:
            return True
        address = effect_receipt_key(self.group, MODULE, staged["native_attempt"])
        return bool(await self.queue.get(address + ":processing"))

    async def delete(self, row, job, authorize):
        sending = row["sending"]
        attempt, deadline = sending["native_attempt"], sending["native_until"]
        if self.clock() >= deadline:
            return DiscordActionResult(1, "", False, "failed", error_code="expired_job")
        mutation = ChannelMessageDeletion(
            row["id"], tuple(sending["ids"]), f"Lumi purge {row['id']} by {row['actor']}",
        )
        address = effect_key(self.group, job, attempt)
        grant = DiscordEffectGrant(
            MODULE, self.generation, DELETE_CHANNEL_MESSAGES, row["guild"], row["channel"], None,
            source_digest(mutation), deadline,
        )
        if not await stage_mutation(self.queue, address, self.lane(row), encode(grant)):
            return DiscordActionResult(1, "", False, "failed", error_code="effect_not_authorized")
        checked = False
        try:


            await authorize(row)
            checked = True
        finally:
            if not checked:
                with suppress(OSError, TimeoutError, RedisError):
                    await self.queue.delete(address)
        try:
            return await self.request(row, job, DELETE_CHANNEL_MESSAGES, attempt, channel_message_deletion=mutation)
        except (ActionUnavailable, OSError, TimeoutError, RedisError):
            return DiscordActionResult(1, "", False, "failed", error_code="uncertain")

    async def progress(self, row, job, text, deadline, authorize):
        message, page, attempt = row["message"], PresentationPage(description=text), self.attempt()
        token = row["token"]
        address = effect_key(self.group, job, attempt)
        lane = mutation_lane(self.group, MODULE, row["guild"], row["channel"], message)
        grant = DiscordEffectGrant(MODULE, self.generation, UPDATE_BOUND_PANEL, row["guild"], row["channel"],
                                   message, panel_payload(token, page), deadline)
        if self.clock() >= deadline or not await stage_mutation(self.queue, address, lane, encode(grant)):
            return None
        checked = False
        try:
            await authorize(row)
            checked = True
        finally:
            if not checked:
                with suppress(OSError, TimeoutError, RedisError):
                    await self.queue.delete(address)
        return await self.request(row, job, UPDATE_BOUND_PANEL, attempt, message_id=message,
                                  page=page, bound_panel_session=token)
