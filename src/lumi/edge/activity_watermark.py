from __future__ import annotations

from redis.asyncio import Redis

from lumi.contracts.observation_state import (
    LastSpoke as LastSpoke,
)
from lumi.contracts.observation_state import (
    activity_key as activity_key,
)
from lumi.contracts.observation_state import (
    encode_watermark,
    parse_watermark,
)

_ADVANCE = """
local previous = redis.call('GET', KEYS[1])
if not previous or #previous ~= 37 or string.sub(previous, 17, 17) ~= ':'
    or not string.match(previous, '^%d+:%d+$') or ARGV[1] > previous then
    redis.call('SET', KEYS[1], ARGV[1])
end
"""


async def record_activity(redis: Redis, guild: int, user: int, boundary: LastSpoke) -> None:
    await redis.eval(_ADVANCE, 1, activity_key(guild, user), encode_watermark(boundary))


async def last_activity(redis: Redis, guild: int, user: int) -> LastSpoke | None:
    return parse_watermark(await redis.get(activity_key(guild, user)))


async def last_activities(redis: Redis, guilds: tuple[int, ...], user: int) -> dict[int, LastSpoke]:
    if not guilds:
        return {}
    rows = await redis.mget([activity_key(guild, user) for guild in guilds])
    return {
        guild: boundary
        for guild, row in zip(guilds, rows, strict=True)
        if (boundary := parse_watermark(row)) is not None
    }
