from __future__ import annotations

from typing import Annotated

import msgspec


RESPONSE_LIFETIME_MS = 900_000
MAX_BYTES = 1024 * 1024

PUBLISH = """
local old = redis.call('GET', KEYS[1])
if old then
    local ok, previous = pcall(cjson.decode, old)
    if ok and type(previous) == 'table' and type(previous.generation) == 'number'
        and type(previous.revision) == 'number' then
        local generation, revision = tonumber(ARGV[1]), tonumber(ARGV[2])
        if previous.generation > generation or previous.revision > revision
            or (previous.generation == generation and previous.revision == revision) then
            return 0
        end
    end
end
redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
return 1
"""


class PrivateFeedbackItem(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    actor: Annotated[int, msgspec.Meta(gt=0)]
    component: Annotated[str, msgspec.Meta(min_length=1, max_length=100)]
    text: Annotated[str, msgspec.Meta(min_length=1, max_length=512)]


class PrivateFeedbackSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    generation: Annotated[int, msgspec.Meta(ge=0)]
    revision: Annotated[int, msgspec.Meta(gt=0)]
    final: bool
    updates: Annotated[dict[str, PrivateFeedbackItem], msgspec.Meta(max_length=5000)]


def feedback_key(group: str, module: str, guild: int, channel: int, message: int) -> str:
    return f"lumi:private-feedback:{group}:{module}:{guild}:{channel}:{message}"


def decode_feedback(raw: bytes | None) -> PrivateFeedbackSnapshot | None:
    if raw and len(raw) <= MAX_BYTES:
        try:
            return msgspec.json.decode(raw, type=PrivateFeedbackSnapshot)
        except msgspec.DecodeError:
            pass
    return None


async def publish_feedback(redis, key: str, snapshot: PrivateFeedbackSnapshot, ttl_ms: int) -> None:
    if ttl_ms <= 0 or not snapshot.updates:
        return
    raw = msgspec.json.encode(snapshot)
    if len(raw) > MAX_BYTES:
        raise ValueError("private feedback exceeds transport bound")
    await redis.eval(
        PUBLISH, 1, key, snapshot.generation, snapshot.revision, raw,
        min(ttl_ms, RESPONSE_LIFETIME_MS),
    )
