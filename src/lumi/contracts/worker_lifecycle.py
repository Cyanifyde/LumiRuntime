from __future__ import annotations

import hmac
import re
import time
from typing import Literal

import msgspec

from lumi.transport import GROUPS

PREFIX = "lumi:worker-lifecycle:v1:"
REQUESTS = PREFIX + "request:"
CLAIMS = PREFIX + "claim:"
REPLIES = PREFIX + "reply:"
HEALTH = PREFIX + "health"
ADMISSION_MS = 15_000
RESULT_MS = 300_000
HEALTH_MS = 20_000
HEARTBEAT_SECONDS = 5
MAX_WIRE = 4096


def clock_ms():
    return time.time_ns() // 1_000_000


class LifecycleRequest(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    identifier: str
    group: str
    operation: Literal["restart", "drain"]
    expires_at: int

    def valid(self, now):
        return (
            re.fullmatch(r"[a-f0-9]{32}", self.identifier) is not None
            and self.group in GROUPS
            and now < self.expires_at <= now + ADMISSION_MS
        )


class LifecycleReply(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    identifier: str
    group: str
    ok: bool
    hard: bool = False
    deployment: str = "restart"
    revision: str | None = None


class WorkerHealth(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    state: Literal["offline", "starting", "online", "draining", "restarting"]
    round_trip_ms: float | None
    generation: int | None


class LifecycleHealth(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    observed_at: int
    workers: dict[str, WorkerHealth]


def seal(key: bytes, lane: str, value) -> bytes:
    body = msgspec.json.encode(value)
    tag = hmac.digest(key, lane.encode("ascii") + b"\0" + body, "sha256")
    return tag + body


def unpack(key: bytes, lane: str, wire, schema):
    if not isinstance(wire, bytes) or not 32 < len(wire) <= MAX_WIRE:
        return None
    tag, body = wire[:32], wire[32:]
    expected = hmac.digest(key, lane.encode("ascii") + b"\0" + body, "sha256")
    if not hmac.compare_digest(tag, expected):
        return None
    try:
        return msgspec.json.decode(body, type=schema)
    except msgspec.DecodeError:
        return None
