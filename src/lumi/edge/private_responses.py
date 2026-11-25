from __future__ import annotations

import re
import time
from dataclasses import dataclass, field, fields, replace

import msgspec

from lumi.shared.private_feedback import RESPONSE_LIFETIME_MS, feedback_key

KEY = "lumi:edge-private-responses:v1"
TIMEOUT_TEXT = "Couldn’t confirm the result."


MAX_BINDINGS = 10_000


MAX_BYTES = 8192

RECOVERY_PAGE = (1024 * 1024) // MAX_BYTES
INTERACTION = re.compile(r"[1-9][0-9]{0,19}\Z")

_SAVE = """
local prior = redis.call('HGET', KEYS[1], ARGV[1])
if prior and ARGV[4] == 'seed' then
  if prior == ARGV[2] then return 0 end
  return -1
end
if not prior and redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[5]) then return -2 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
-- Redis 7.4 applies field expiration, so no stale index or second key is needed.
local expires = redis.pcall('HPEXPIREAT', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
if expires.err then
  redis.call('HDEL', KEYS[1], ARGV[1])
  return -3
end
return expires[1] == 1 and 1 or 0
"""


@dataclass
class PrivateBinding:
    interaction: str
    application: int
    token: str = field(repr=False)
    job: str
    module: str
    generation: int
    guild: int
    channel: int
    message: int
    actor: int
    expires: int
    deadline: int
    group: str = ""
    component: str = ""
    timeout_text: str = TIMEOUT_TEXT
    seen_generation: int = -1
    revision: int = -1
    shown: str = ""

    def projection_key(self) -> str | None:
        if self.component and self.group:
            return feedback_key(self.group, self.module, self.guild, self.channel, self.message)
        return None


_FIELDS = frozenset(item.name for item in fields(PrivateBinding))


class PrivateResponseUnavailable(RuntimeError):
    pass


def decode_binding(identifier: str, payload: bytes, now: int) -> PrivateBinding | None:
    if not INTERACTION.fullmatch(identifier) or len(payload) > MAX_BYTES:
        return None
    try:
        data = msgspec.json.decode(payload)
        if not isinstance(data, dict) or "group" not in data or data.keys() - _FIELDS:
            return None
        record = msgspec.convert(data, type=PrivateBinding)
    except (msgspec.DecodeError, TypeError):
        return None
    if (
        record.interaction != identifier or not record.token or record.application <= 0
        or record.actor <= 0 or not record.job or record.component and not record.group
        or not now < record.expires <= now + RESPONSE_LIFETIME_MS
    ):
        return None
    return record


class PrivateResponseStore:
    def __init__(self, redis, *, clock=None):
        self.redis = redis
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    async def load(self, now: int) -> list[PrivateBinding]:
        records = {}
        async for field_id, payload in self.redis.hscan_iter(KEY, count=RECOVERY_PAGE):
            try:
                identifier = field_id.decode("ascii")
            except (UnicodeError, AttributeError):
                continue
            if record := decode_binding(identifier, payload, now):
                records[identifier] = record
        return list(records.values())

    async def save(self, record: PrivateBinding, *, seed=False) -> bool:
        now = self.clock()
        if record.expires <= now:
            return False
        if record.expires > now + RESPONSE_LIFETIME_MS:
            record = replace(record, expires=now + RESPONSE_LIFETIME_MS)
        payload = msgspec.json.encode(record)
        if decode_binding(record.interaction, payload, now) is None:
            raise PrivateResponseUnavailable("Invalid private response binding")
        result = await self.redis.eval(
            _SAVE, 1, KEY, record.interaction, payload, record.expires,
            "seed" if seed else "write", MAX_BINDINGS,
        )
        if result == -1:
            raise PrivateResponseUnavailable("Conflicting private response binding")
        if result == -2:
            raise PrivateResponseUnavailable("Private response journal is at capacity")
        if result == -3:
            raise PrivateResponseUnavailable("Private response expiration unavailable")
        return result == 1

    async def delete(self, interaction: str) -> None:
        await self.redis.hdel(KEY, interaction)
