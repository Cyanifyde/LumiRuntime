import asyncio
import secrets

from redis.exceptions import RedisError

from lumi.contracts.worker_lifecycle import (
    ADMISSION_MS,
    HEALTH,
    HEALTH_MS,
    REPLIES,
    REQUESTS,
    RESULT_MS,
    LifecycleHealth,
    LifecycleReply,
    LifecycleRequest,
    clock_ms,
    seal,
    unpack,
)


class RedisWorkerLifecycle:
    def __init__(self, redis, key: bytes | None):
        self.redis = redis
        self.key = key

    async def health(self):
        if self.key is None:
            return {}
        try:
            async with asyncio.timeout(2):
                wire = await self.redis.get(HEALTH)
        except (RedisError, TimeoutError):
            return {}
        view = unpack(self.key, "health", wire, LifecycleHealth)
        return view.workers if view and 0 <= clock_ms() - view.observed_at < HEALTH_MS else {}

    async def request(self, operation, group):
        if self.key is None or group not in await self.health():
            raise RuntimeError("Host worker control unavailable.")
        identifier = secrets.token_hex(16)
        payload = LifecycleRequest(identifier, group, operation, clock_ms() + ADMISSION_MS)
        try:
            async with asyncio.timeout(RESULT_MS / 1000):
                posted = await self.redis.set(
                    REQUESTS + group,
                    seal(self.key, "request", payload),
                    nx=True,
                    px=ADMISSION_MS,
                )
                if not posted:
                    raise RuntimeError("Worker control already pending.")
                item = await self.redis.blpop(REPLIES + identifier, timeout=RESULT_MS / 1000)
                reply = unpack(self.key, "reply", item[1] if item else None, LifecycleReply)
                if (
                    not reply
                    or reply.identifier != identifier
                    or reply.group != group
                    or not reply.ok
                ):
                    raise RuntimeError("Host worker control could not be confirmed.")
                return reply
        except (RedisError, TimeoutError) as exc:

            raise RuntimeError("Host worker control outcome unconfirmed.") from exc
