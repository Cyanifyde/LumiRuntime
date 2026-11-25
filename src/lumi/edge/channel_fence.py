import msgspec
from redis.exceptions import RedisError

from lumi.contracts.channel_fence import INDEX, SCHEMA, SCHEMA_FIELD, ChannelFence


class FenceUnavailable(RuntimeError):
    pass


class RedisChannelFence:
    def __init__(self, redis):
        self.redis = redis

    async def index(self, guild):
        if not guild:
            return []
        try:
            version, payload = await self.redis.hmget(INDEX, (SCHEMA_FIELD, str(guild)))
            if version != SCHEMA:
                raise FenceUnavailable("Channel fence not initialized")
            if payload is None:
                return []
            entries = msgspec.json.decode(payload, type=tuple[ChannelFence, ...])
            if any(not item.valid(guild) for item in entries) or len({item.id for item in entries}) != len(entries):
                raise FenceUnavailable("Invalid channel fence")
            return [msgspec.to_builtins(item) for item in entries]
        except (RedisError, msgspec.DecodeError) as exc:
            raise FenceUnavailable("Channel fence unavailable") from exc
