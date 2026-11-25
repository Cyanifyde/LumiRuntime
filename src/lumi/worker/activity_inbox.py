import asyncio
import logging
import re

import msgspec
from redis.exceptions import RedisError

from lumi.contracts.activity_observations import ActivityObservation, stream_key

LOGGER = logging.getLogger(__name__)
SCOPE = re.compile(rb"([1-9][0-9]{0,19}):([A-Za-z0-9_-]{1,59})")


class ActivityInbox:
    def __init__(self, redis, group, module, generation, clock, *, ready, observe):
        self.redis = redis
        self.stream = stream_key(group, module)
        self.generation, self.clock = generation, clock
        self.ready, self.observe = ready, observe
        self.active = False

    async def run(self):
        while True:
            if not self.ready():
                await asyncio.sleep(1)
                continue
            try:
                await self.read_once()
            except (RedisError, OSError, TimeoutError):
                LOGGER.warning("activity_observation_unavailable")
                await asyncio.sleep(1)

    async def read_once(self):
        batches = await self.redis.xread({self.stream: "0-0"}, count=32, block=1000)
        for _, rows in batches:
            for identifier, fields in rows:
                if not self.ready():
                    return
                self.active = True
                try:
                    await self._read(fields)
                    await self.redis.xdel(self.stream, identifier)
                finally:
                    self.active = False

    async def _read(self, fields):
        scope, wire = fields.get(b"scope", b""), fields.get(b"event", b"")
        stamp = SCOPE.fullmatch(scope) if len(scope) <= 80 else None
        if stamp is None or int(stamp[1]) > self.generation or not 0 < len(wire) <= 512:
            return
        try:
            observation = msgspec.json.decode(wire, type=ActivityObservation, strict=True)
        except msgspec.DecodeError:
            return
        if observation.valid(self.clock()):
            await self.observe(observation, stamp[2].decode("ascii"))
