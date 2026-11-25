from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from lumi.contracts import Announcement, decode
from lumi.contracts.wire import ROUTING_REVISION
from lumi.shared.quiescence import tracked_read
from lumi.transport import (
    enabled,
    incoming,
    origin,
    require_manifest_transport,
    require_origin,
    stream_scope,
)

from .registry import EdgeRouteRegistry

LOGGER = logging.getLogger(__name__)


class AnnouncementConsumer:
    STREAM = "lumi:announcements"
    CURRENT = "lumi:manifests:current"

    def __init__(self, redis: Redis, registry: EdgeRouteRegistry) -> None:
        self.redis = redis
        self.registry = registry
        self._stopped = asyncio.Event()
        self._cursors = {}
        self._primed = None

    async def _accept(self, payload):
        announcement = decode(payload, Announcement)
        if announcement.routing_revision != ROUTING_REVISION:
            raise ValueError("incompatible route declaration")
        require_origin(self.STREAM, announcement.manifest.group)
        if enabled() and origin(self.STREAM) is None:
            raise ValueError("route declarations require an authenticated Redis lane")
        require_manifest_transport(announcement.manifest)
        await self.registry.apply(announcement)

    @tracked_read
    async def reconstruct(self) -> str:
        cursors = {}
        for stream, snapshot in zip(incoming(self.STREAM), incoming(self.CURRENT), strict=True):
            latest = await self.redis.xrevrange(stream, count=1)
            cursor = latest[0][0] if latest else b"0-0"
            cursors[stream] = cursor.decode() if isinstance(cursor, bytes) else str(cursor)
            current = await self.redis.hvals(snapshot)
            with stream_scope(self.STREAM, stream):
                for payload in current:
                    try:
                        await self._accept(payload)
                    except Exception as exc:
                        LOGGER.warning(
                            "manifest_replay_rejected", extra={"error": type(exc).__name__}
                        )
        self._cursors = cursors
        return cursors[self.STREAM]

    async def prime(self) -> None:
        self._primed = await self.reconstruct()

    async def run(self) -> None:
        cursor, self._primed = self._primed, None
        if cursor is None:
            cursor = await self.reconstruct()
        while not self._stopped.is_set():
            cursor = await self._read_once(cursor)

    @tracked_read
    async def _read_once(self, cursor):
        cursors = self._cursors or {self.STREAM: cursor}
        messages = await self.redis.xread(cursors, count=64, block=5_000)
        for source, entries in messages:
            source = source.decode() if isinstance(source, bytes) else source
            for identifier, fields in entries:
                cursor = identifier.decode() if isinstance(identifier, bytes) else str(identifier)
                cursors[source] = cursor
                payload = fields.get(b"announcement") or fields.get("announcement")
                if payload is None:
                    continue
                try:
                    with stream_scope(self.STREAM, source):
                        await self._accept(payload)
                except Exception as exc:
                    LOGGER.warning(
                        "manifest_announcement_rejected", extra={"error": type(exc).__name__}
                    )
        self._cursors = cursors
        return cursors[self.STREAM]

    def stop(self) -> None:
        self._stopped.set()
