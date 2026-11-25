from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from lumi.contracts import GuildPolicySnapshot, decode

from .store import MySQLSettingsStore

LOGGER = logging.getLogger(__name__)


class PolicyOutboxPublisher:
    CURRENT = "lumi:policy:current"
    STREAM = "lumi:policy:updates"

    def __init__(self, store: MySQLSettingsStore, redis: Redis) -> None:
        self.store = store
        self.redis = redis
        self._stopped = asyncio.Event()
        self._flush_lock = asyncio.Lock()

    def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        while not self._stopped.is_set():
            try:
                published = await self.flush()
            except Exception:
                LOGGER.exception("policy_outbox_publish_failed")
                published = 0
            if published == 0:
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=1)
                except TimeoutError:
                    pass

    async def flush(self) -> int:
        async with self._flush_lock:
            count = 0
            for outbox_id, payload in await self.store.pending_outbox():
                snapshot = decode(payload, GuildPolicySnapshot)
                pipeline = self.redis.pipeline(transaction=True)
                pipeline.hset(self.CURRENT, str(snapshot.guild_id), payload)
                pipeline.xadd(self.STREAM, {"policy": payload}, maxlen=4096, approximate=True)
                await pipeline.execute()
                await self.store.mark_published(outbox_id)
                count += 1
            return count

    async def flush_safely(self) -> int:
        try:
            return await self.flush()
        except Exception:
            LOGGER.exception("policy_outbox_publish_failed")
            return 0
