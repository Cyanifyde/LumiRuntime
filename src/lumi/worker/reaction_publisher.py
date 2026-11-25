from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from lumi.panels import MySQLPanelStore

LOGGER = logging.getLogger(__name__)


class ReactionIndexPublisher:
    STREAM = "lumi:reaction-route-index"
    LATEST = "lumi:reaction-route-index:latest"

    def __init__(self, store: MySQLPanelStore, redis: Redis) -> None:
        self._store = store
        self._redis = redis
        self._stopped = False

    async def run(self) -> None:
        while not self._stopped:
            await self.flush_safely()
            await asyncio.sleep(1)

    def stop(self) -> None:
        self._stopped = True

    async def flush_safely(self) -> None:
        try:
            for outbox_id, payload in await self._store.pending_publications():
                pipeline = self._redis.pipeline(transaction=True)
                pipeline.set(self.LATEST, payload)
                pipeline.xadd(self.STREAM, {"snapshot": payload}, maxlen=1000, approximate=True)
                await pipeline.execute()
                await self._store.mark_publication_sent(outbox_id)
        except Exception:
            LOGGER.exception("reaction_index_publication_failed")
