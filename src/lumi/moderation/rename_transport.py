import secrets
from contextlib import suppress

from redis.exceptions import RedisError

from lumi.contracts import DiscordActionResult, DiscordEffectGrant, decode, encode
from lumi.contracts.member_profiles import LIST_GUILD_MEMBERS, READ_MEMBER_PROFILE, SET_MEMBER_NICKNAME, NicknameMutation
from lumi.shared.discord_effects import (
    EFFECT_GRANT_SECONDS, UPDATE_BOUND_PANEL, effect_key, effect_receipt_key,
    member_operation_lane, mutation_lane, panel_payload, source_digest, stage_mutation,
)
from lumi.ui_keys import ui_key
from lumi.worker.actions import ActionUnavailable

from .rename_runner import Changed, NativeFailure

MODULE = "moderation"
ROUTE = "moderation.rename_work"

CALLBACK_MS = 30_000


class RenameTransport:
    def __init__(self, queue, cache, actions, scheduler, group, generation, clock):
        self.queue, self.cache = queue, cache
        self.actions, self.scheduler = actions, scheduler
        self.group, self.generation, self.clock = group, generation, clock

    @staticmethod
    def attempt():
        return "effect:" + secrets.token_hex(16)

    async def schedule(self, row):
        now = self.clock()
        expires = now + CALLBACK_MS
        await self.scheduler.publish(
            operation="upsert", schedule_key=f"{MODULE}:work:{row['id']}:{secrets.token_hex(16)}",
            route=ROUTE, revision=1, guild_id=row["guild"], channel_id=row["channel"],
            actor_id=row["actor"], due_at_ms=now, expires_at_ms=expires,
            arguments={"batch": row["id"], "revision": row["revision"]},
        )
        return expires

    def live(self, event):
        return self.clock() < event.get("_expires_at_ms", 0)

    def deadline(self, event):
        return min(event["_expires_at_ms"], self.clock() + EFFECT_GRANT_SECONDS * 1000)

    async def panel_id(self, row):
        raw = await self.cache.get(ui_key("message", row["token"], self.group))
        try:
            bound = int(raw)
        except (TypeError, ValueError):
            return None
        return bound if 0 < bound < 2**64 else None

    async def request(self, row, job, kind, key, **fields):
        try:
            return await self.actions.request(
                kind=kind, job_id=job, guild_id=row["guild"], channel_id=row["channel"],
                idempotency_key=key, **fields,
            )
        except (ActionUnavailable, OSError, TimeoutError, RedisError) as exc:
            raise NativeFailure("uncertain") from exc

    async def list_members(self, row, job):
        return await self.request(row, job, LIST_GUILD_MEMBERS, self.attempt(), member_directory_after=row["cursor"])

    async def read_member(self, row, job, member, *, fresh):
        return await self.request(row, job, READ_MEMBER_PROFILE, self.attempt(), member_id=member, refresh_member_profile=fresh)

    async def receipt(self, attempt):
        key = effect_receipt_key(self.group, MODULE, attempt)
        raw = await self.queue.get(key)
        return decode(raw, DiscordActionResult) if raw is not None else None

    async def pending(self, attempt, deadline):
        key = effect_receipt_key(self.group, MODULE, attempt)


        return self.clock() < deadline or bool(await self.queue.get(key + ":processing"))

    async def stage(self, row, job, *, kind, attempt, message, digest, deadline, lane):
        key = effect_key(self.group, job, attempt)
        if self.clock() >= deadline:
            raise NativeFailure("expired_job")
        grant = DiscordEffectGrant(MODULE, self.generation, kind, row["guild"], row["channel"], message, digest, deadline)
        if not await stage_mutation(self.queue, key, lane, encode(grant)):
            raise NativeFailure("effect_not_authorized")
        return key

    async def write_member(self, row, job, event, record, fence):
        member, attempt = record["member"], record["native_attempt"]
        native = NicknameMutation(row["id"], record["joined"], record["native_before"], record["expected"])
        try:
            key = await self.stage(
                row, job, kind=SET_MEMBER_NICKNAME, attempt=attempt, message=None,
                digest=source_digest((member, native)), deadline=record["native_until"],
                lane=member_operation_lane(self.group, MODULE, row["guild"], row["id"]),
            )
        except NativeFailure as exc:

            return DiscordActionResult(1, "", False, "failed", error_code=exc.code, data=exc.data)
        try:
            await fence(row)
            if not self.live(event):
                raise Changed
        except Changed:
            await self.queue.delete(key)
            raise
        return await self.request(row, job, SET_MEMBER_NICKNAME, attempt, member_id=member, nickname_mutation=native)

    async def progress(self, row, job, event, page, fence):
        attempt, message, token = self.attempt(), row["message"], row["token"]
        lane = mutation_lane(self.group, MODULE, row["guild"], row["channel"], message)
        key = None
        try:
            key = await self.stage(
                row, job, kind=UPDATE_BOUND_PANEL, attempt=attempt, message=message,
                digest=panel_payload(token, page), deadline=self.deadline(event), lane=lane,
            )
            await fence(row)
            result = await self.request(
                row, job, UPDATE_BOUND_PANEL, attempt, message_id=message, page=page, bound_panel_session=token,
            )
            return result.ok or (result.error_code == "message_unavailable" and result.data.get("deleted") is True)
        except Changed:
            if key:
                with suppress(RedisError, OSError, TimeoutError):
                    await self.queue.delete(key)
            return False
        except (NativeFailure, RedisError, OSError, TimeoutError):

            return False

    async def revoke(self, row):
        keys = [member_operation_lane(self.group, MODULE, row["guild"], row["id"])]
        if row["message"]:
            keys.append(mutation_lane(self.group, MODULE, row["guild"], row["channel"], row["message"]))
        await self.queue.delete(*keys)
