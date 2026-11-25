from __future__ import annotations

import json

WINDOW_MS = 3_000
TTL_SECONDS = 15
MAX_CANDIDATES = 64

_OBSERVE = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
redis.call('ZADD', KEYS[1], 'NX', ARGV[2], ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
local maximum = tonumber(ARGV[5])
if redis.call('ZCARD', KEYS[1]) > maximum then
    redis.call('SET', KEYS[2], '1', 'EX', ARGV[4])
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -maximum - 1)
end
"""


class GlitchDropSources:
    def __init__(self, cache, clock):
        self.cache = cache
        self.clock = clock

    @staticmethod
    def key(guild, channel):
        return f"lumi:glitch:drop-sources:cards:{guild}:{channel}"

    async def observe(self, guild, channel, message, owner, at):
        now = self.clock()
        if min(guild, channel, message, owner, at) <= 0 or abs(now - at) > WINDOW_MS * 2:
            return
        key = self.key(guild, channel)
        value = json.dumps([message, owner, at], separators=(",", ":"))
        await self.cache.eval(
            _OBSERVE, 2, key, key + ":overflow", now - TTL_SECONDS * 1000, at, value,
            TTL_SECONDS, MAX_CANDIDATES,
        )

    async def match(self, guild, channel, notice, at):
        key = self.key(guild, channel)
        async with self.cache.pipeline(transaction=True) as pipe:
            pipe.get(key + ":overflow")
            pipe.zrangebyscore(key, at - WINDOW_MS, at + WINDOW_MS)
            overflow, values = await pipe.execute()
        if overflow or len(values) != 1:
            return None
        source, owner, _at = json.loads(values[0])
        if source != notice and source > 0 and owner > 0:
            return source, owner
        return None
