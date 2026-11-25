import asyncio
import hashlib
import logging
from collections import defaultdict

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts.channel_fence import DIGEST_FIELD, INDEX, PUBLISHER, SCHEMA, SCHEMA_FIELD, WAKE, ChannelFence

from .handover_store import HEARTBEAT_TTL_MS

LOGGER = logging.getLogger(__name__)


class FencePublisher:
    def __init__(self, redis, store, generation, *, executor=("security", "moderation")):
        self.redis, self.store, self.generation = redis, store, str(generation).encode()
        self.executor = executor
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self.run(), name="channel-fence-publication")

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def publish(self):
        async with self.redis.pipeline(transaction=True) as pipe:

            await pipe.watch(PUBLISHER, INDEX)
            if await pipe.get(PUBLISHER) != self.generation:
                return False
            fields = defaultdict(list)
            for row in await self.store.index():
                entry = msgspec.structs.replace(ChannelFence.project(row), executor=self.executor)
                if not entry.valid(entry.guild):
                    raise ValueError("Invalid committed channel fence")
                fields[str(entry.guild)].append(entry)
            snapshot = {SCHEMA_FIELD: SCHEMA, **{
                guild: msgspec.json.encode(sorted(entries, key=lambda entry: entry.id))
                for guild, entries in fields.items()
            }}
            digest = hashlib.sha256(msgspec.msgpack.encode(sorted(snapshot.items()))).hexdigest().encode()
            if await pipe.hmget(INDEX, (SCHEMA_FIELD, DIGEST_FIELD)) == [SCHEMA, digest]:
                return True
            snapshot[DIGEST_FIELD] = digest
            pipe.multi()
            pipe.delete(INDEX)
            pipe.hset(INDEX, mapping=snapshot)
            await pipe.execute()
        return True

    async def run(self):
        while True:
            try:
                if not await self.publish():
                    await asyncio.sleep(1)
                    continue


                await self.redis.blpop(WAKE, timeout=HEARTBEAT_TTL_MS / 1000)
            except (MySQLError, RedisError, OSError, RuntimeError, ValueError):
                LOGGER.warning("channel_fence_sync_unavailable")
                await asyncio.sleep(1)
