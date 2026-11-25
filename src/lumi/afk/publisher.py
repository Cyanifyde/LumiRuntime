from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from lumi.contracts import AFKActiveSnapshot, ActorRoutingSnapshot, decode
from lumi.shared.actor_routing import publish_snapshot

from .store import MySQLAFKStore

LOGGER = logging.getLogger(__name__)


class AFKActivePublisher:
    def __init__(self, store: MySQLAFKStore, redis: Redis) -> None:
        self.store = store
        self.redis = redis
        self._stopped = asyncio.Event()
        self._flush_lock = asyncio.Lock()
        self.draining = False
        self.busy = False

    def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        await self.publish_current()
        while not self._stopped.is_set():
            if self.draining:
                await asyncio.sleep(0.05)
                continue
            self.busy = True
            try:
                count = await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("afk_active_publication_failed")
                count = 0
            finally:
                self.busy = False
            if not count:
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=1)
                except TimeoutError:
                    pass

    async def publish_current(self) -> None:
        for snapshot in await self.store.active_snapshots():
            await self._publish(snapshot)

    async def flush(self) -> int:
        async with self._flush_lock:
            count = 0
            for outbox_id, payload in await self.store.pending_publications():
                snapshot = decode(payload, AFKActiveSnapshot)
                await self._publish(snapshot)
                await self.store.mark_publication_sent(outbox_id)
                count += 1
            return count

    async def flush_safely(self) -> int:
        try:
            return await self.flush()
        except Exception:
            LOGGER.exception("afk_active_publication_failed")
            return 0

    async def _publish(self, snapshot: AFKActiveSnapshot) -> None:
        if not ((snapshot.schema == 1 and snapshot.guild_id > 0)
                or (snapshot.schema == 2 and snapshot.guild_id == 0)):
            raise ValueError("invalid AFK active-status snapshot")
        await publish_snapshot(self.redis, ActorRoutingSnapshot(
            1, "afk", "community", snapshot.guild_id, snapshot.revision, snapshot.user_ids,
        ))
