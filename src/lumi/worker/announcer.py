from __future__ import annotations

import time

from redis.asyncio import Redis

from lumi.contracts import Announcement, Manifest, encode
from lumi.contracts.wire import ROUTING_REVISION
from lumi.transport import outbound

PUBLISH_LUA = """
if ARGV[1] == 'retire' then
  redis.call('HDEL', KEYS[1], ARGV[2])
else
  redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
end
return redis.call('XADD', KEYS[2], 'MAXLEN', '~', 10000, '*', 'announcement', ARGV[3])
"""


class ManifestAnnouncer:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._publish = redis.register_script(PUBLISH_LUA)

    async def announce(self, manifest: Manifest, *, kind: str = "announce") -> str:
        announcement = Announcement(kind, manifest, int(time.time() * 1000), ROUTING_REVISION)
        payload = encode(announcement)
        identifier = await self._publish(
            keys=[
                outbound("lumi:manifests:current", manifest.group),
                outbound("lumi:announcements", manifest.group),
            ],
            args=[kind, manifest.module, payload],
        )
        return identifier.decode() if isinstance(identifier, bytes) else str(identifier)
