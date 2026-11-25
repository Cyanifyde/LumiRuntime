from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from lumi.transport import GROUPS

FRAME_LIMIT = 100 * (65536 + 1) + 128
FRAME_TTL = 900


def route_scope(group, route):
    if group not in GROUPS or re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", route) is None:
        raise ValueError("Invalid capture relay route.")
    return f"{group}:{route}"


def keys(group, route, *, coverage=False):
    slot = "lumi:capture-relay:" + route_scope(group, route)
    if coverage:
        slot += ":coverage"
    return slot + ":frame", slot + ":receipt"


@dataclass(frozen=True, slots=True)
class Transfer:
    body: bytes

    def __post_init__(self):
        if not isinstance(self.body, bytes) or not 0 < len(self.body) <= FRAME_LIMIT:
            raise ValueError("Invalid capture relay payload.")

    @property
    def digest(self):
        return hashlib.sha256(self.body).hexdigest().encode("ascii")


class CaptureRelay:
    def __init__(self, redis, group, route, *, coverage=False):
        self.redis = redis
        self.slot, self.receipt = keys(group, route, coverage=coverage)

    async def read(self):
        raw, receipt = await self.redis.mget(self.slot, self.receipt)
        frame = Transfer(raw) if raw is not None else None
        return frame, frame is not None and frame.digest == receipt

    async def offer(self, frame):
        return await self.redis.set(self.slot, frame.body, nx=True, ex=FRAME_TTL)

    async def acknowledge(self, frame):
        return await self.redis.eval(
            "if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end "
            "redis.call('SET',KEYS[2],ARGV[2],'EX',ARGV[3]);return 1",
            2, self.slot, self.receipt, frame.body, frame.digest, FRAME_TTL,
        )

    async def discard(self, frame):


        return await self.redis.eval(
            "if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end "
            "return redis.call('DEL',KEYS[1])",
            1, self.slot, frame.body,
        )
