from __future__ import annotations

import hashlib
import json
import time

import msgspec
from redis.asyncio import Redis

from lumi.contracts import AIProfileAnnouncement, AIProfileSpec, encode
from lumi.transport import outbound


def profile_hash(profile: AIProfileSpec) -> str:
    canonical = json.dumps(
        msgspec.to_builtins(profile),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


class AIProfileAnnouncer:
    CURRENT = "lumi:ai-profiles:current"
    STREAM = "lumi:ai-profiles"

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def announce(
        self,
        profile: AIProfileSpec,
        *,
        module: str,
        group: str,
        generation: int,
        kind: str = "ready",
    ) -> str:
        digest = profile_hash(profile)
        announcement = AIProfileAnnouncement(
            schema=1,
            kind=kind,
            module=module,
            group=group,
            generation=generation,
            profile=profile,
            profile_hash=digest,
            announced_at_ms=int(time.time() * 1_000),
        )
        payload = encode(announcement)
        pipeline = self._redis.pipeline(transaction=True)
        if kind == "retire":
            pipeline.hdel(outbound(self.CURRENT, group), profile.key)
        else:
            pipeline.hset(outbound(self.CURRENT, group), profile.key, payload)
        pipeline.xadd(
            outbound(self.STREAM, group), {"announcement": payload}, maxlen=10_000, approximate=True
        )
        await pipeline.execute()
        return digest
