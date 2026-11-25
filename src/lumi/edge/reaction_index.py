from __future__ import annotations

from redis.asyncio import Redis

from lumi.contracts import ReactionRouteSnapshot, decode
from lumi.shared.quiescence import tracked_read


class ReactionRouteIndex:
    STREAM = "lumi:reaction-route-index"
    LATEST = "lumi:reaction-route-index:latest"

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._revision = 0
        self._message_ids: frozenset[int] = frozenset()
        self._stopped = False

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def message_ids(self) -> frozenset[int]:
        return self._message_ids

    def contains(self, message_id: int) -> bool:
        return message_id in self._message_ids

    async def run(self) -> None:
        await self._reconstruct()


        cursor = "0-0"
        while not self._stopped:
            cursor = await self._read_once(cursor)

    @tracked_read
    async def _reconstruct(self):
        latest = await self._redis.get(self.LATEST)
        if latest:
            self._apply(decode(latest, ReactionRouteSnapshot))

    @tracked_read
    async def _read_once(self, cursor):
        batches = await self._redis.xread({self.STREAM: cursor}, count=16, block=5_000)
        for _, entries in batches:
            for identifier, fields in entries:
                cursor = identifier
                payload = fields.get(b"snapshot") or fields.get("snapshot")
                if payload:
                    self._apply(decode(payload, ReactionRouteSnapshot))
        return cursor

    def stop(self) -> None:
        self._stopped = True

    def _apply(self, snapshot: ReactionRouteSnapshot) -> None:
        if snapshot.schema != 1 or snapshot.revision <= self._revision:
            return
        self._revision = snapshot.revision
        self._message_ids = frozenset(snapshot.message_ids)
