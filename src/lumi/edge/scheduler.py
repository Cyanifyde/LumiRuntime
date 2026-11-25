from __future__ import annotations

import asyncio
import logging
import os
import re
import time

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from lumi.contracts import ScheduledDispatch, TypedRequest, decode, encode
from lumi.shared.quiescence import tracked_read
from lumi.transport import current, incoming, require_origin, stream_scope

from lumi.shared.broker import QueueFull, QueueUnavailable, RedisBroker
from .matcher import MatchedRoute
from .registry import EdgeRouteRegistry

LOGGER = logging.getLogger(__name__)
SCHEDULE_KEY = re.compile(r"^[a-z][a-z0-9_-]{0,63}:[A-Za-z0-9:_-]{1,180}$")
MODULE_KEY = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
GROUP_KEY = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
ROUTE_KEY = re.compile(r"^[a-z][a-z0-9_-]{0,63}\.[a-z][a-z0-9_.-]{0,127}$")


class ScheduledCallbackConsumer:


    STREAM = "lumi:scheduled-control"
    GROUP = "lumi-edge-scheduled"
    HASH = "lumi:scheduled:current"
    DUE = "lumi:scheduled:due"

    def __init__(
        self,
        redis: Redis,
        broker: RedisBroker,
        registry: EdgeRouteRegistry,
        *,
        application_user_id=None,
    ) -> None:
        self._redis = redis
        self._broker = broker
        self._registry = registry
        self._application_user_id = application_user_id
        self._consumer = f"edge-{os.getpid()}"
        self._stopped = False

    async def run(self) -> None:
        for stream in incoming(self.STREAM):
            with stream_scope(self.STREAM, stream):
                await self._ensure_group()
        due = asyncio.create_task(self._due_loop(), name="scheduled-due")
        reclaim = asyncio.create_task(self._reclaim(), name="scheduled-reclaim")
        try:
            while not self._stopped:
                await self._read_once()
        finally:
            due.cancel()
            reclaim.cancel()
            await asyncio.gather(due, reclaim, return_exceptions=True)

    def stop(self) -> None:
        self._stopped = True

    @tracked_read
    async def _read_once(self):
        batches = await self._redis.xreadgroup(
            self.GROUP,
            self._consumer,
            {stream: ">" for stream in incoming(self.STREAM)},
            count=64,
            block=5_000,
        )
        for source, entries in batches:
            for identifier, fields in entries:
                with stream_scope(self.STREAM, source):
                    await self._process(identifier, fields)

    async def _process(self, identifier: str | bytes, fields: dict[bytes, bytes]) -> None:
        payload = fields.get(b"schedule") or fields.get("schedule")
        try:
            if payload is None:
                return
            request = decode(payload, ScheduledDispatch)
            require_origin(self.STREAM, request.group)
            if not self._valid(request):
                return
            existing_payload = await self._redis.hget(self.HASH, request.schedule_key)
            existing = (
                decode(existing_payload, ScheduledDispatch)
                if existing_payload is not None
                else None
            )
            if existing is not None and (existing.module, existing.group) != (
                request.module, request.group
            ):
                return
            if existing is not None and request.revision < existing.revision:
                return
            if request.operation == "cancel":
                pipeline = self._redis.pipeline(transaction=True)
                pipeline.hdel(self.HASH, request.schedule_key)
                pipeline.zrem(self.DUE, request.schedule_key)
                await pipeline.execute()
                return
            pipeline = self._redis.pipeline(transaction=True)
            pipeline.hset(self.HASH, request.schedule_key, encode(request))
            pipeline.zadd(self.DUE, {request.schedule_key: request.due_at_ms})
            await pipeline.execute()
        except Exception:
            LOGGER.warning("scheduled_control_rejected", exc_info=True)
        finally:
            await self._redis.xack(current(self.STREAM), self.GROUP, identifier)

    def _valid(self, request: ScheduledDispatch) -> bool:
        now_ms = int(time.time() * 1_000)
        current = self._registry.snapshot.modules.get(request.module)
        route = self._registry.snapshot.scheduled.get((request.module, request.route))
        structurally_valid = bool(
            request.schema == 1
            and request.operation in {"upsert", "cancel"}
            and MODULE_KEY.fullmatch(request.module)
            and GROUP_KEY.fullmatch(request.group)
            and ROUTE_KEY.fullmatch(request.route)
            and request.route.startswith(f"{request.module}.")
            and SCHEDULE_KEY.fullmatch(request.schedule_key)
            and request.schedule_key.startswith(f"{request.module}:")
            and request.generation > 0
            and request.revision > 0
            and request.guild_id > 0
            and (request.channel_id is None or request.channel_id > 0)
            and request.actor_id >= 0
            and request.expires_at_ms > request.due_at_ms
            and request.expires_at_ms - request.due_at_ms <= 300_000
            and request.expires_at_ms > now_ms
            and len(encode(request.arguments)) <= 16_384
            and len(encode(request)) <= 32_768
        )
        if not structurally_valid:
            return False
        if current is None and route is None:
            return True
        return bool(
            current is not None
            and route is not None
            and (request.actor_id > 0 or route.spec.scope == "guild")
            and current.manifest.group == request.group
            and request.generation >= current.manifest.generation
        )

    async def _due_loop(self) -> None:
        while not self._stopped:
            try:
                await self._dispatch_due()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("scheduled_dispatch_failed")
            await asyncio.sleep(0.5)

    @tracked_read
    async def _dispatch_due(self) -> None:
        now_ms = int(time.time() * 1_000)
        keys = await self._redis.zrangebyscore(self.DUE, 0, now_ms, start=0, num=32)
        for raw_key in keys:
            key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
            payload = await self._redis.hget(self.HASH, key)
            if payload is None:
                await self._redis.zrem(self.DUE, key)
                continue
            request = decode(payload, ScheduledDispatch)
            if request.expires_at_ms <= now_ms:
                await self._remove(key)
                continue
            route = self._registry.snapshot.scheduled.get((request.module, request.route))
            current = self._registry.snapshot.modules.get(request.module)
            if route is None or current is None:
                continue
            if request.actor_id == 0 and route.spec.scope != "guild":
                await self._remove(key)
                continue
            fence = f"lumi:scheduled:dispatched:{key}:{request.revision}"
            if not await self._redis.set(fence, "1", nx=True, ex=7 * 24 * 3_600):
                await self._remove(key)
                continue
            event = {
                "dispatch": "scheduled",
                "schedule_key": key,
                "schedule_revision": request.revision,
                "due_at_ms": request.due_at_ms,
                "actor_id": request.actor_id,
                "guild_id": request.guild_id,
                "channel_id": request.channel_id,
            }
            if self._application_user_id is not None:
                bot_id = self._application_user_id()
                if type(bot_id) is int and bot_id > 0:
                    event["discord_application_user_id"] = bot_id
            matched = MatchedRoute(
                module=route.module,
                group=route.group,
                generation=route.generation,
                stream=route.stream,
                deadline_ms=min(
                    route.spec.deadline_ms,
                    max(100, request.expires_at_ms - now_ms),
                ),
                queue_class=route.spec.queue_class,
                request=TypedRequest(
                    route=route.spec.route,
                    handler=route.spec.handler,
                    arguments=request.arguments,
                    event=event,
                ),
                presentation_mode=route.presentation_mode,
                presentation_modes=route.presentation_modes,
                acl_key=route.spec.acl_key,
                public=True,
                actor_id=request.actor_id,
            )
            try:
                await self._broker.submit(
                    matched,
                    actor_id=request.actor_id,
                    guild_id=request.guild_id,
                    channel_id=request.channel_id,
                    now_ms=now_ms,
                )
            except (QueueFull, QueueUnavailable):
                await self._redis.delete(fence)
                continue
            await self._remove(key)

    async def _remove(self, key: str) -> None:
        pipeline = self._redis.pipeline(transaction=True)
        pipeline.hdel(self.HASH, key)
        pipeline.zrem(self.DUE, key)
        await pipeline.execute()

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
            count=64,
        )
        for identifier, fields in response[1]:
            await self._process(identifier, fields)
        return response[0].decode() if isinstance(response[0], bytes) else str(response[0])

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                current(self.STREAM), self.GROUP, id="0-0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
