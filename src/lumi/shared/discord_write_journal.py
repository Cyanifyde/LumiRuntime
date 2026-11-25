from __future__ import annotations

import hashlib
import re
import secrets

import msgspec
from redis.exceptions import RedisError

from .action_receipts import action_receipt_key

ATTEMPT_KEYS = "lumi:discord-write:attempt:"
PENDING_KEYS = "lumi:discord-write:channel:"
READY = "lumi:discord-write:ready"
VERSION = b"1"
SETTLED_TTL_MS = 30 * 86_400_000
MAX_RECONCILE = 100


_BEGIN = """
if ARGV[3] ~= 'seed' and redis.call('GET', KEYS[1]) ~= ARGV[4] then return -1 end
local current = redis.call('GET', KEYS[2])
if current then
  if ARGV[3] ~= 'seed' then return 0 end
  if current ~= ARGV[2] then return redis.error_reply('Conflicting historical Discord write') end
end
for i=3,#KEYS do redis.call('ZADD', KEYS[i], ARGV[1], ARGV[2]) end
redis.call('SET', KEYS[2], ARGV[2])
return 1
"""
_SETTLE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
for i=2,#KEYS do redis.call('ZREM', KEYS[i], ARGV[1]) end
redis.call('SET', KEYS[1], 'settled', 'PX', ARGV[2])
return 1
"""
_SNOWFLAKE_MAX = 2**64
_RECEIPT = re.compile(r"lumi:edge-action-receipt:[a-zA-Z0-9:_-]{1,240}")


def pending_key(guild, channel):
    return f"{PENDING_KEYS}{guild}:{channel}"


class WriteMarker(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    code: str
    owner: str
    guild: int
    source: int
    destination: int
    receipt_key: str

    def valid(self):
        return bool(
            re.fullmatch(r"[0-9a-f]{64}", self.code) and re.fullmatch(r"[0-9a-f]{32}", self.owner)
            and type(self.guild) is int and 0 < self.guild < _SNOWFLAKE_MAX
            and all(type(n) is int and 0 <= n < _SNOWFLAKE_MAX for n in (self.source, self.destination))
            and _RECEIPT.fullmatch(self.receipt_key)
        )

    def keys(self):
        channels = dict.fromkeys(c for c in (self.source, self.destination) if c)
        return (ATTEMPT_KEYS + self.code, *(pending_key(self.guild, c) for c in channels))


class WriteJournalUnavailable(RuntimeError):
    pass


class DiscordWriteJournal:
    def __init__(self, redis):
        self.redis = redis

    async def begin_action(self, action, job, now):
        marker = WriteMarker(
            hashlib.sha256((action.module + ":" + action.idempotency_key).encode()).hexdigest(),
            secrets.token_hex(16), job.guild_id, job.channel_id or 0, action.channel_id or 0,
            action_receipt_key(action),
        )
        return marker if await self.prepare(marker, now) else None

    async def prepare(self, marker, now):
        if not marker.valid():
            raise WriteJournalUnavailable("Invalid Discord write marker")


        async with self.redis.pipeline(transaction=False) as pipe:
            stage(pipe, marker, now)
            wait_for_disk(pipe)
            created, durable = await pipe.execute()
        if created == -1:
            raise WriteJournalUnavailable("Discord write journal cutover is not ready")
        if not created:
            return False
        if durable[0] != 1:

            await self.finish_action(marker, False, now)
            raise WriteJournalUnavailable("Discord write journal was not persisted")
        return True

    async def finish_action(self, marker, uncertain, now):
        if uncertain:
            return
        keys = marker.keys()
        return bool(await self.redis.eval(
            _SETTLE, len(keys), *keys, msgspec.json.encode(marker), SETTLED_TTL_MS,
        ))

    async def pending_actions(self, guild, channel):
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                pipe.get(READY).zrange(pending_key(guild, channel), 0, MAX_RECONCILE - 1)
                version, entries = await pipe.execute()
            if version != VERSION:
                raise WriteJournalUnavailable("Discord write journal cutover is not ready")
            markers = [msgspec.json.decode(entry, type=WriteMarker) for entry in entries]
        except (RedisError, msgspec.DecodeError) as exc:
            raise WriteJournalUnavailable("Discord write journal unavailable") from exc
        if any(not item.valid() or (item.guild != guild or channel not in (item.source, item.destination)) for item in markers):
            raise WriteJournalUnavailable("Invalid pending Discord write")
        return [dict(id=marker, receipt_key=marker.receipt_key) for marker in markers]


def wait_for_disk(pipe):
    pipe.execute_command("WAITAOF", 1, 0, 1000)


def stage(pipe, marker, now, *, seed=False):
    keys = (READY, *marker.keys())
    pipe.eval(_BEGIN, len(keys), *keys, now, msgspec.json.encode(marker), "seed" if seed else "active", VERSION)
