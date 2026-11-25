from __future__ import annotations

import secrets
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ActivityRules:
    minimum_messages: int = 10
    minimum_people: int = 2
    window_ms: int = 300_000
    per_user_ms: int = 5_000
    gap_ms: int = 600_000
    chance_percent: int = 10


COUNT = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local cfg = cjson.decode(raw)
if not cfg.enabled or (ARGV[10] ~= '' and cfg.token ~= ARGV[10]) then return 0 end
local now = tonumber(ARGV[1])
if redis.call('ZSCORE', KEYS[2], ARGV[3]) then return 0 end
redis.call('ZADD', KEYS[2], now, ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. (now-tonumber(ARGV[4])))
local last = tonumber(redis.call('HGET', KEYS[3], ARGV[2]) or '0')
if now-last < tonumber(ARGV[5]) then return 0 end
redis.call('HSET', KEYS[3], ARGV[2], now)
redis.call('ZADD', KEYS[4], now, ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', '(' .. (now-tonumber(ARGV[4])))
redis.call('ZADD', KEYS[5], now, ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[5], '-inf', '(' .. (now-tonumber(ARGV[4])))
for i=2,5 do redis.call('PEXPIRE', KEYS[i], 7200000) end
if now < tonumber(cfg.not_before) then return 0 end
if redis.call('ZCARD', KEYS[5]) < tonumber(ARGV[6]) then return 0 end
if redis.call('ZCARD', KEYS[4]) < tonumber(ARGV[7]) then return 0 end
if tonumber(ARGV[8]) == 0 then return 0 end
if not redis.call('SET', KEYS[6], cfg.token, 'NX', 'PX', tonumber(ARGV[9])) then return 0 end
return 1
"""


class ActivityAggregator:
    def __init__(self, redis) -> None:
        self.redis = redis
        self._count = redis.register_script(COUNT)

    @staticmethod
    def keys(module: str, guild: int) -> list[str]:
        base = f"lumi:activity:{module}:{guild}"
        return [
            base,
            *(f"{base}:{suffix}" for suffix in ("seen", "users", "people", "count", "lease")),
        ]

    async def count(
        self,
        module: str,
        guild: int,
        actor: int,
        message: int,
        now: int,
        spec: ActivityRules,
        *,
        run: str = "",
    ) -> bool:
        return bool(
            await self._count(
                keys=self.keys(module, guild),
                args=[
                    now,
                    str(actor),
                    str(message),
                    spec.window_ms,
                    spec.per_user_ms,
                    spec.minimum_messages,
                    spec.minimum_people,
                    int(secrets.randbelow(100) < spec.chance_percent),
                    spec.gap_ms,
                    run,
                ],
            )
        )
