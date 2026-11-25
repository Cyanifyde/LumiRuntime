from __future__ import annotations

import re

import msgspec

from lumi.shared.capture_packets import CAPTURE_PAGE, CaptureSignal
from lumi.shared.capture_relay import route_scope
from lumi.shared.discord_observations import Observation

PREFIX = "lumi:capture-spool:v1:"
CUTOVER = "lumi:capture-spool:ready"
CUTOVER_VERSION = b"1"
IDENTITY_TTL = 7 * 86_400_000
ITEM_BYTES = 65_536
ROUTE_ITEMS, ROUTE_BYTES = 100_000, 128 * 1024**2
GUILD_ITEMS, GUILD_BYTES = 10_000, 32 * 1024**2
_ID = re.compile(r"[0-9a-f]{32}")

_SIGNAL_HELPERS = """
local function read_signal(hash, slot)
    local old = redis.call('HGET', hash, slot)
    if old then return cjson.decode(old) end
end
local function write_signal(hash, order, slot, stamp, token, count)
    redis.call('HSET', hash, slot, cjson.encode({stamp, token, count}))
    redis.call('ZADD', order, stamp, slot)
end
"""

_APPEND = _SIGNAL_HELPERS + """
local id, guild, payload = ARGV[1], ARGV[2], ARGV[3]
local prior = redis.call('HGET', KEYS[2], id)
if prior then
    if ARGV[12] == 'seed' then
        local stream_id = string.match(prior, '^([^:]+):')
        local old = redis.call('XRANGE', KEYS[1], stream_id, stream_id)
        if #old ~= 1 or old[1][2][2] ~= payload then return -1 end
    end
    return 2
end
local bytes = #payload
local gn, gb = guild .. ':n', guild .. ':b'
local function get(k) return tonumber(redis.call('HGET', KEYS[3], k) or '0') end
if bytes > tonumber(ARGV[9])
    or redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[5])
    or get('bytes') + bytes > tonumber(ARGV[6])
    or get(gn) >= tonumber(ARGV[7])
    or get(gb) + bytes > tonumber(ARGV[8]) then
    if ARGV[12] ~= 'seed' then
        local slot = ARGV[10]
        local old = read_signal(KEYS[4], slot)
        if old then
            write_signal(KEYS[4], KEYS[5], slot, old[1], old[2], old[3] + 1)
        else
            write_signal(KEYS[4], KEYS[5], slot, tonumber(ARGV[4]), string.rep('0', 32), 1)
        end
    end
    return 0
end
local sid = redis.call('XADD', KEYS[1], '*', 'p', payload)
redis.call('HSET', KEYS[2], id, sid .. ':' .. guild .. ':' .. bytes)
redis.call('HINCRBY', KEYS[3], 'bytes', bytes)
redis.call('HINCRBY', KEYS[3], gn, 1)
redis.call('HINCRBY', KEYS[3], gb, bytes)
if #KEYS == 6 and ARGV[12] ~= 'seed' then
    redis.call('SET', KEYS[6], ARGV[11], 'PXAT', ARGV[13])
end
return 1
"""

_ACK = """
local function release(key, amount)
    if redis.call('HINCRBY', KEYS[3], key, -amount) <= 0 then
        redis.call('HDEL', KEYS[3], key)
    end
end
for i=1,#ARGV do
    local id = ARGV[i]
    local meta = redis.call('HGET', KEYS[2], id)
    if meta then
        local sid, guild, bytes = string.match(meta, '^([^:]+):(%d+):(%d+)$')
        redis.call('XDEL', KEYS[1], sid)
        redis.call('HDEL', KEYS[2], id)
        release('bytes', tonumber(bytes))
        release(guild .. ':b', tonumber(bytes))
        release(guild .. ':n', 1)
    end
end
return 1
"""

_WRITE_SIGNALS = _SIGNAL_HELPERS + """
for i=2,#ARGV,4 do
    local slot, ts, token, count = ARGV[i], tonumber(ARGV[i+1]), ARGV[i+2], tonumber(ARGV[i+3])
    local old = read_signal(KEYS[1], slot)
    if ARGV[1] == 'seed' and old and (old[1] ~= ts or old[2] ~= token or old[3] ~= count) then
        return 0
    end
    if not old or (ARGV[1] ~= 'seed' and old[1] <= ts) then
        write_signal(KEYS[1], KEYS[2], slot, ts, token, count)
    end
end
return 1
"""

_READ_SIGNALS = """
local rows = {}
local ids = redis.call('ZRANGE', KEYS[2], 0, tonumber(ARGV[1]) - 1)
for _, id in ipairs(ids) do
    local payload = redis.call('HGET', KEYS[1], id)
    if payload then rows[#rows + 1] = {id, payload} end
end
return rows
"""

_ACK_SIGNALS = _SIGNAL_HELPERS + """
for i=1,#ARGV,4 do
    local slot = ARGV[i]
    local old = read_signal(KEYS[1], slot)
    if old and old[1]==tonumber(ARGV[i+1]) and old[2]==ARGV[i+2] and old[3]==tonumber(ARGV[i+3]) then
        redis.call('HDEL', KEYS[1], slot)
        redis.call('ZREM', KEYS[2], slot)
    end
end
return 1
"""


def _signal_slot(guild: int, kind: str) -> str:

    return f"{guild:020d}:{kind}"


def _signal_args(signals):
    return [value for s in signals for value in (
        _signal_slot(s.guild, s.kind), s.occurred, s.token, s.count,
    )]


class CaptureSpool:
    def __init__(self, redis, owner):
        self.redis = redis
        self.prefix = PREFIX + route_scope(*owner) + ":"
        self.observations = tuple(self.prefix + part for part in ("stream", "ids", "limits"))
        self.signals = tuple(self.prefix + part for part in ("coverage", "coverage-order"))

    def identity_key(self, guild, message):
        return f"{self.prefix}identity:{guild}:{message}"

    async def append(self, obs: Observation, *, seed=False) -> bool:
        if not _ID.fullmatch(obs.id) or obs.guild <= 0 or obs.occurred <= 0:
            raise ValueError("Invalid capture identity.")
        keys = self.observations + self.signals
        identity = b""
        if obs.kind.startswith("MESSAGE_") and (author := obs.data.get("subject")):
            identity = msgspec.json.encode((author, int(bool(obs.data.get("bot") or obs.data.get("webhook")))))
            keys += (self.identity_key(obs.guild, obs.resource),)
        result = await self.redis.eval(
            _APPEND, len(keys), *keys, obs.id, obs.guild, msgspec.json.encode(obs), obs.occurred,
            ROUTE_ITEMS, ROUTE_BYTES, GUILD_ITEMS, GUILD_BYTES, ITEM_BYTES,
            _signal_slot(obs.guild, "spool_full"), identity, "seed" if seed else "append",
            obs.occurred + IDENTITY_TTL,
        )
        if result == -1:
            raise RuntimeError("Conflicting historical observation.")
        return bool(result)

    async def identity(self, guild, message):
        raw = await self.redis.get(self.identity_key(guild, message))
        return msgspec.json.decode(raw, type=tuple[int, int]) if raw is not None else None

    async def seed_identity(self, guild, message, author, bot, expires):
        key = self.identity_key(guild, message)
        payload = msgspec.json.encode((author, bot))
        if not await self.redis.set(key, payload, pxat=expires, nx=True):
            previous = await self.redis.get(key)
            if previous is not None and previous != payload:
                raise RuntimeError("Conflicting historical capture identity.")

    async def batch(self):
        rows = await self.redis.xrange(self.observations[0], count=CAPTURE_PAGE)
        records = [msgspec.json.decode(fields[b"p"], type=Observation) for _, fields in rows]
        return [(obs.id, obs) for obs in records]

    async def ack(self, identifier):
        await self.ack_observations((identifier,))

    async def ack_observations(self, identifiers):
        if (
            not identifiers or len(identifiers) > CAPTURE_PAGE
            or any(not _ID.fullmatch(value) for value in identifiers)
        ):
            raise ValueError("Invalid capture receipt.")
        await self.redis.eval(_ACK, len(self.observations), *self.observations, *identifiers)

    async def record_signals(self, signals, *, seed=False):
        if not signals:
            return
        for offset in range(0, len(signals), CAPTURE_PAGE):
            args = _signal_args(signals[offset:offset + CAPTURE_PAGE])
            saved = await self.redis.eval(
                _WRITE_SIGNALS, 2, *self.signals, "seed" if seed else "write", *args,
            )
            if not saved:
                raise RuntimeError("Conflicting historical capture signal.")

    async def signal_batch(self):
        records = await self.redis.eval(_READ_SIGNALS, 2, *self.signals, CAPTURE_PAGE)
        signals = []
        for slot, payload in records:
            guild, kind = slot.decode("ascii").split(":", 1)
            occurred, token, count = msgspec.json.decode(payload, type=tuple[int, str, int])
            signals.append(CaptureSignal(int(guild), kind, occurred, token, count))
        return tuple(signals)

    async def ack_signals(self, signals):
        if not signals or len(signals) > CAPTURE_PAGE:
            raise ValueError("Invalid capture signal receipt.")
        await self.redis.eval(_ACK_SIGNALS, 2, *self.signals, *_signal_args(signals))
