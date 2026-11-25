from __future__ import annotations

import json
import time

RECORD = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ctx = cjson.decode(raw)
if tostring(ctx.guild) ~= ARGV[1] or tostring(ctx.channel) ~= ARGV[2]
    or tostring(ctx.message) ~= ARGV[3] then return 0 end
if not ctx.members[ARGV[4]] then return 0 end
if not ctx.payloads[ARGV[9]] then return 0 end
if tonumber(ARGV[5]) < tonumber(ctx.open_at) or tonumber(ARGV[5]) >= tonumber(ctx.close_at)
    or tonumber(ARGV[6]) >= tonumber(ctx.settle_at) then return 0 end
local old = redis.call('HGET', KEYS[2], ARGV[7])
if old then return 2 end
if redis.call('HLEN', KEYS[2]) >= 5000 then return 0 end
if tonumber(redis.call('HGET', KEYS[3], ARGV[4]) or '0') >= 5 then return 0 end
redis.call('HINCRBY', KEYS[3], ARGV[4], 1)
redis.call('EXPIRE', KEYS[3], 604800)
redis.call('HSET', KEYS[2], ARGV[7], ARGV[8])
redis.call('EXPIRE', KEYS[2], 604800)
return 1
"""


class ComponentReceipts:


    def __init__(self, redis) -> None:
        self.redis = redis
        self._record = redis.register_script(RECORD)

    @staticmethod
    def key(module: str, message: int) -> str:
        return f"lumi:component-receipts:{module}:{message}"

    async def record(self, module: str, event: dict, payload: str) -> bool:
        timestamp = int(event["interaction_created_at_ms"])
        receipt = dict(
            interaction_id=str(event["interaction_id"]),
            actor_id=str(event["actor_id"]),
            timestamp=timestamp,
            payload=payload,
        )
        key = self.key(module, int(event["message_id"]))
        result = await self._record(
            keys=[f"{key}:context", key, f"{key}:actors"],
            args=[
                str(event["guild_id"]),
                str(event["channel_id"]),
                str(event["message_id"]),
                str(event["actor_id"]),
                timestamp,
                int(time.time() * 1000),
                receipt["interaction_id"],
                json.dumps(receipt),
                payload,
            ],
        )
        return result == 1
