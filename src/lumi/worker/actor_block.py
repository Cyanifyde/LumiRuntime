import asyncio
import secrets
import time

import msgspec
from redis.exceptions import RedisError

from lumi.contracts.runtime_control import (
    BLOCK_RECEIPTS,
    BLOCK_REPLIES,
    BLOCK_REQUESTS,
    REQUEST_MS,
    BlockReceipt,
    BlockRequest,
)


class ActorBlockClient:
    def __init__(self, redis, *, clock=None):
        self.redis = redis
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    async def request(self, job, expires_at_ms):
        now = self.clock()
        query = BlockRequest(job, secrets.token_hex(16), min(now + REQUEST_MS, expires_at_ms))
        if not query.valid(now):
            return None
        try:
            async with asyncio.timeout((query.deadline - now) / 1000):
                async with self.redis.pipeline(transaction=True) as transaction:
                    transaction.rpush(BLOCK_REQUESTS, msgspec.json.encode(query))
                    transaction.pexpire(BLOCK_REQUESTS, REQUEST_MS)
                    await transaction.execute()
                reply = await self.redis.blpop(BLOCK_REPLIES + query.reply)
                if not reply or reply[1] not in (b"applied", "applied"):
                    return None
                wire = await self.redis.get(BLOCK_RECEIPTS + job)
                if not wire or len(wire) > 1024:
                    return None
                receipt = msgspec.json.decode(wire, type=BlockReceipt)
                return receipt if receipt and receipt.matches(job, self.clock()) else None
        except (TimeoutError, RedisError, msgspec.DecodeError):
            return None
