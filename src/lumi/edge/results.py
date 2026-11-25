from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from lumi.contracts import JobEnvelope, ResultEnvelope, decode
from lumi.shared.quiescence import tracked_read
from lumi.transport import current, incoming, require_origin, stream_scope

from lumi.shared.broker import RedisBroker
from .registry import EdgeRouteRegistry
from lumi.shared.request_scope import scoped

LOGGER = logging.getLogger(__name__)
ResultCallback = Callable[[JobEnvelope, ResultEnvelope], Awaitable[None]]


class ResultConsumer:
    def __init__(
        self,
        redis: Redis,
        broker: RedisBroker,
        registry: EdgeRouteRegistry,
        callback: ResultCallback,
    ) -> None:
        self.redis = redis
        self.broker = broker
        self.registry = registry
        self.callback = callback
        self.consumer_group = "lumi-edge-results"
        self.consumer = f"edge-{os.getpid()}"
        self._stopped = False

    async def run(self) -> None:
        for stream in incoming(self.broker.RESULTS_STREAM):
            with stream_scope(self.broker.RESULTS_STREAM, stream):
                await self._ensure_group()
        reclaim = asyncio.create_task(self._reclaim(), name="result-reclaim")
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
        messages = await self.redis.xreadgroup(
            self.consumer_group,
            self.consumer,
            {stream: ">" for stream in incoming(self.broker.RESULTS_STREAM)},
            count=32,
            block=5_000,
        )
        for source, entries in messages:
            for identifier, fields in entries:
                with stream_scope(self.broker.RESULTS_STREAM, source):
                    await self._process(identifier, fields)

    @scoped
    async def _process(self, identifier: str | bytes, fields: dict[bytes, bytes]) -> None:
        payload = fields.get(b"result") or fields.get("result")
        if payload is None:
            await self._ack(identifier)
            return
        try:
            result = decode(payload, ResultEnvelope)
            require_origin(self.broker.RESULTS_STREAM, result.group)
            job = await self.broker.get_job(result.job_id)
            if job is None or (job.group, job.module) != (result.group, result.module):
                await self._ack(identifier)
                return
            current = self.registry.snapshot.modules.get(result.module)
            if current is None or result.generation > current.manifest.generation:
                await self.broker.release(result.job_id)
                await self._ack(identifier)
                return
            if result.generation < current.manifest.generation:
                await self.broker.release(result.job_id)
                await self._ack(identifier)
                return
            if job.expires_at_ms <= int(time.time() * 1000):
                await self.broker.release(result.job_id)
                await self._ack(identifier)
                return
            fence = await self.redis.set(
                f"lumi:result-fence:{result.job_id}",
                "processing",
                nx=True,
                ex=7 * 24 * 3600,
            )
            if fence:
                try:
                    await self.callback(job, result)
                except Exception:
                    LOGGER.exception(
                        "final_discord_action_failed",
                        extra={"module_name": result.module, "job_id": result.job_id},
                    )
            await self.broker.release(result.job_id)
            await self._ack(identifier)
        except Exception:
            LOGGER.exception("invalid_worker_result")
            await self._ack(identifier)

    async def _reclaim(self) -> None:
        cursors = {stream: "0-0" for stream in incoming(self.broker.RESULTS_STREAM)}
        while not self._stopped:
            await asyncio.sleep(2)
            for stream in cursors:
                with stream_scope(self.broker.RESULTS_STREAM, stream):
                    cursors[stream] = await self._reclaim_once(cursors[stream])

    @tracked_read
    async def _reclaim_once(self, cursor):
        response = await self.redis.xautoclaim(
            current(self.broker.RESULTS_STREAM),
            self.consumer_group,
            self.consumer,
            min_idle_time=2_000,
            start_id=cursor,
            count=32,
        )
        for identifier, fields in response[1]:
            await self._process(identifier, fields)
        return response[0].decode() if isinstance(response[0], bytes) else str(response[0])

    async def _ack(self, identifier: str | bytes) -> None:
        await self.redis.xack(current(self.broker.RESULTS_STREAM), self.consumer_group, identifier)

    async def _ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(
                current(self.broker.RESULTS_STREAM),
                self.consumer_group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
