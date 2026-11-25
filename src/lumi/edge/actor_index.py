from __future__ import annotations

import asyncio
import logging

from lumi.contracts import ActorRoutingSnapshot, decode
from lumi.shared.quiescence import tracked_read
from lumi.shared.actor_routing import index_keys
from lumi.transport import GROUPS, require_module_owner

LOGGER = logging.getLogger(__name__)


class ActorRouteIndex:
    def __init__(self, redis):
        self._redis = redis
        self._guilds: dict[int, dict[tuple[str, str], tuple[int, frozenset[int]]]] = {}
        self._stopped = False

    def matches(self, guild_id: int, actor_id: int):
        if guild_id <= 0:
            return frozenset()
        return frozenset(
            key for scope in (guild_id, 0)
            for key, (_, users) in self._guilds.get(scope, {}).items() if actor_id in users
        )

    def apply(self, group, payload):
        try:
            snapshot = decode(payload, ActorRoutingSnapshot)
            if group not in GROUPS or group != snapshot.group or any(uid <= 0 for uid in snapshot.user_ids):
                return False
            require_module_owner(snapshot.module, group)
        except (TypeError, ValueError):
            return False
        scope = self._guilds.setdefault(snapshot.guild_id, {})
        key = (group, snapshot.module)
        if key in scope and scope[key][0] >= snapshot.revision:
            return False
        scope[key] = snapshot.revision, frozenset(snapshot.user_ids)
        return True

    async def run(self):
        cursors = None
        while not self._stopped:
            try:
                if cursors is None:
                    cursors = await self._reconstruct()
                await self._read_once(cursors)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("actor_routing_unavailable")
                await asyncio.sleep(1)

    def stop(self):
        self._stopped = True

    @tracked_read
    async def _reconstruct(self):
        cursors = {}
        for group in GROUPS:
            latest, stream = index_keys(group)
            entries = await self._redis.xrevrange(stream, count=1)
            cursors[stream] = entries[0][0] if entries else "0-0"
            for payload in await self._redis.hvals(latest):
                self.apply(group, payload)
        return cursors

    @tracked_read
    async def _read_once(self, cursors):
        for stream, entries in await self._redis.xread(cursors, count=32, block=5000):
            if isinstance(stream, bytes):
                stream = stream.decode()
            group = stream.rpartition(":")[2]
            for identifier, fields in entries:
                cursors[stream] = identifier
                payload = fields.get(b"snapshot") or fields.get("snapshot")
                if payload:
                    self.apply(group, payload)
        return cursors
