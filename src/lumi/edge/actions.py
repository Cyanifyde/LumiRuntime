from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from lumi.contracts import DiscordAction, DiscordActionResult, decode, encode
from lumi.contracts.channel_messages import CHANNEL_MESSAGE_ACTIONS, READ_CHANNEL_MESSAGES
from lumi.contracts.channel_configuration import READ_CHANNEL_CONFIGURATION
from lumi.contracts.channel_resources import CHANNEL_RESOURCE_ACTIONS, READ_CHANNEL_RESOURCES
from lumi.contracts.layout_message import WRITE_LAYOUT_MESSAGE
from lumi.contracts.member_effects import MEMBER_EFFECT_PERMISSIONS
from lumi.contracts.member_profiles import (
    MEMBER_PROFILE_ACTIONS, MEMBER_PROFILE_READS,
)
from lumi.contracts.role_access import ACTOR_ROLE_ACTIONS, READ_ACTOR_ROLES, READ_GUILD_ROLES, ROLE_FACTS
from lumi.contracts.staff_recovery import OBSERVE_RECOVERY
from lumi.contracts.staff_moderation import STAFF_EFFECT, STAFF_READS
from lumi.shared.quiescence import tracked_read
from lumi.transport import (
    GROUPS,
    REPLY_GROUP,
    action_reply_key,
    current,
    incoming,
    legacy_writes,
    require_origin,
    stream_scope,
)
from lumi.shared.broker import RedisBroker
from lumi.shared.action_receipts import action_receipt_key
from .member_effects import scope_matches
from .channel_messages import channel_message_scope
from .channel_configuration import configuration_scope
from .channel_resources import resource_scope
from .layout_messages import layout_scope
from .member_profiles import profile_scope
from .role_access import actor_role_scope, role_scope
from .registry import EdgeRouteRegistry
from lumi.shared.request_scope import scoped

LOGGER = logging.getLogger(__name__)
ActionExecutor = Callable[[DiscordAction], Awaitable[DiscordActionResult]]
IDENTIFIER = re.compile(r"^[A-Za-z0-9:_-]{1,160}$")


class EdgeActionConsumer:
    STREAM = "lumi:edge-actions"
    GROUP = "lumi-edge-actions"

    def __init__(
        self,
        redis: Redis,
        broker: RedisBroker,
        registry: EdgeRouteRegistry,
        executor: ActionExecutor,
    ) -> None:
        self._redis = redis
        self._broker = broker
        self._registry = registry
        self._executor = executor
        self._consumer = f"edge-{os.getpid()}"
        self._stopped = False

    async def run(self) -> None:
        for stream in incoming(self.STREAM):
            with stream_scope(self.STREAM, stream):
                await self._ensure_group()
        reclaim = asyncio.create_task(self._reclaim(), name="edge-action-reclaim")
        try:
            while not self._stopped:
                await self._read_once()
        finally:
            reclaim.cancel()
            await asyncio.gather(reclaim, return_exceptions=True)

    def stop(self) -> None:
        self._stopped = True

    @tracked_read
    async def _read_once(self):
        batches = await self._redis.xreadgroup(
            self.GROUP,
            self._consumer,
            {stream: ">" for stream in incoming(self.STREAM)},
            count=32,
            block=5_000,
        )
        for source, entries in batches:
            for identifier, fields in entries:
                with stream_scope(self.STREAM, source):
                    await self._process(identifier, fields)

    @scoped
    async def _process(self, identifier: str | bytes, fields: dict[bytes, bytes]) -> None:
        payload = fields.get(b"action") or fields.get("action")
        if payload is None:
            await self._ack(identifier)
            return
        action: DiscordAction | None = None
        reply_token = REPLY_GROUP.set(None)
        try:
            action = decode(payload, DiscordAction)
            require_origin(self.STREAM, action.group)
            REPLY_GROUP.set(action.group)
            failure = await self._validate(action)
            if failure is not None:
                await self._deliver(failure)
                return
            if action.kind in {
                "read_interaction_context", "read_guild_sources", "read_mention_batch_members",
                "resolve_members", "read_actor_post_channels", "read_owned_history",
                "read_channel_members", "read_embed_sources", "fetch_message",
                READ_GUILD_ROLES,
                READ_ACTOR_ROLES,
                READ_CHANNEL_MESSAGES,
                READ_CHANNEL_CONFIGURATION,
                READ_CHANNEL_RESOURCES,
                OBSERVE_RECOVERY,
            } or action.kind in MEMBER_PROFILE_READS or (
                action.kind == STAFF_EFFECT and action.staff_effect and action.staff_effect.operation in STAFF_READS
            ) or (
                action.kind == "channel_watch"
                and action.channel_watch is not None
                and (action.channel_watch.operation in {"read", "verify", "authorize_stop"})
            ):


                await self._deliver(await self._executor(action))
                return
            receipt_key = action_receipt_key(action)
            cached = await self._redis.get(receipt_key)
            if cached:
                cached_result = decode(cached, DiscordActionResult)
                await self._deliver(
                    DiscordActionResult(
                        schema=cached_result.schema,
                        action_id=action.action_id,
                        ok=cached_result.ok,
                        status=cached_result.status,
                        data=cached_result.data,
                        error_code=cached_result.error_code,
                        completed_at_ms=cached_result.completed_at_ms,
                    )
                )
                return
            fence_key = f"{receipt_key}:processing"
            claimed = await self._redis.set(fence_key, action.action_id, nx=True, ex=7 * 24 * 3600)
            if not claimed:
                for _ in range(280):
                    await asyncio.sleep(0.05)
                    cached = await self._redis.get(receipt_key)
                    if cached:
                        cached_result = decode(cached, DiscordActionResult)
                        await self._deliver(
                            DiscordActionResult(
                                schema=cached_result.schema,
                                action_id=action.action_id,
                                ok=cached_result.ok,
                                status=cached_result.status,
                                data=cached_result.data,
                                error_code=cached_result.error_code,
                                completed_at_ms=cached_result.completed_at_ms,
                            )
                        )
                        return
                result = self._failure(action, "action_in_progress")
            else:
                try:
                    result = await self._executor(action)
                    if result.action_id != action.action_id:
                        raise ValueError("action executor returned a mismatched identifier")
                except Exception:
                    LOGGER.exception(
                        "discord_action_failed",
                        extra={"module_name": action.module, "kind": action.kind},
                    )
                    result = self._failure(action, "discord_action_failed")
                finally:
                    if "result" in locals():
                        await self._redis.set(receipt_key, encode(result), ex=7 * 24 * 3600)
                        await self._redis.delete(fence_key)


            await self._deliver(result)
        except Exception:
            LOGGER.exception("invalid_edge_action")
            if action is not None:
                await self._deliver(self._failure(action, "invalid_action"))
        finally:
            try:
                await self._ack(identifier)
            finally:
                REPLY_GROUP.reset(reply_token)

    async def _validate(self, action: DiscordAction) -> DiscordActionResult | None:
        if (
            action.schema != 1
            or not re.fullmatch(r"[a-f0-9]{32}", action.action_id)
            or not IDENTIFIER.fullmatch(action.idempotency_key)
            or action.expires_at_ms <= int(time.time() * 1000)
        ):
            return self._failure(action, "invalid_action")
        job = await self._broker.get_job(action.job_id)
        if job is None:
            return self._failure(action, "unknown_job")
        if job.expires_at_ms <= int(time.time() * 1000):
            return self._failure(action, "expired_job")
        if job.request.event.get("_own_output_deleted") is True:
            return self._failure(action, "action_scope_mismatch")
        if (
            job.module != action.module
            or job.group != action.group
            or job.guild_id != action.guild_id
        ):
            return self._failure(action, "action_scope_mismatch")
        current = self._registry.snapshot.modules.get(action.module)
        if (
            current is None
            or current.manifest.generation != action.generation
            or job.generation != action.generation
        ):
            return self._failure(action, "stale_generation")
        if action.kind not in current.manifest.capabilities:
            return self._failure(action, "capability_denied")
        if action.kind in MEMBER_EFFECT_PERMISSIONS and not scope_matches(
            self._registry.snapshot, job, action,
        ):
            return self._failure(action, "action_scope_mismatch")
        if action.kind in ROLE_FACTS and role_scope(self._registry.snapshot, job, action) is None:
            return self._failure(action, "action_scope_mismatch")
        if action.kind in ACTOR_ROLE_ACTIONS and not actor_role_scope(self._registry.snapshot, job, action):
            return self._failure(action, "action_scope_mismatch")
        if action.kind == WRITE_LAYOUT_MESSAGE and not layout_scope(self._registry.snapshot, job, action):
            return self._failure(action, "action_scope_mismatch")
        if action.kind in MEMBER_PROFILE_ACTIONS and profile_scope(self._registry.snapshot, job, action) is None:
            return self._failure(action, "action_scope_mismatch")
        if action.kind in CHANNEL_MESSAGE_ACTIONS and channel_message_scope(self._registry.snapshot, job, action) is None:
            return self._failure(action, "action_scope_mismatch")
        if action.kind == READ_CHANNEL_CONFIGURATION and configuration_scope(self._registry.snapshot, job, action) is None:
            return self._failure(action, "action_scope_mismatch")
        if action.kind in CHANNEL_RESOURCE_ACTIONS and resource_scope(self._registry.snapshot, job, action) is None:
            return self._failure(action, "action_scope_mismatch")
        return None

    async def _deliver(self, result: DiscordActionResult) -> None:
        legacy = f"lumi:edge-action-result:{result.action_id}"
        group = REPLY_GROUP.get()
        keys = [legacy]
        if group in GROUPS:
            scoped = action_reply_key(result.action_id, group)
            keys = [legacy, scoped] if legacy_writes() else [scoped]
        pipeline = self._redis.pipeline(transaction=True)
        for key in keys:
            pipeline.rpush(key, encode(result))
            pipeline.expire(key, 60)
        await pipeline.execute()

    @staticmethod
    def _failure(action: DiscordAction, code: str) -> DiscordActionResult:
        return DiscordActionResult(
            schema=1,
            action_id=action.action_id,
            ok=False,
            status="failed",
            error_code=code,
            completed_at_ms=int(time.time() * 1000),
        )

    async def _reclaim(self) -> None:
        cursors = {stream: "0-0" for stream in incoming(self.STREAM)}
        while not self._stopped:
            await asyncio.sleep(2)
            for stream in cursors:
                with stream_scope(self.STREAM, stream):
                    cursors[stream] = await self._reclaim_once(cursors[stream])

    @tracked_read
    async def _reclaim_once(self, cursor):
        response = await self._redis.xautoclaim(
            current(self.STREAM),
            self.GROUP,
            self._consumer,
            min_idle_time=2_000,
            start_id=cursor,
            count=32,
        )
        for identifier, fields in response[1]:
            await self._process(identifier, fields)
        return response[0].decode() if isinstance(response[0], bytes) else str(response[0])

    async def _ack(self, identifier: str | bytes) -> None:
        await self._redis.xack(current(self.STREAM), self.GROUP, identifier)

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                current(self.STREAM), self.GROUP, id="0-0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
