from lumi.contracts import ActorRoutingSnapshot, encode
from lumi.transport import GROUPS

_PUBLISH = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if raw and cjson.decode(raw).revision >= tonumber(ARGV[2]) then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', 4096, '*', 'snapshot', ARGV[3])
return 1
"""


def index_keys(group):
    if group not in GROUPS:
        raise ValueError("invalid actor routing group")
    return f"lumi:actor-routing:current:{group}", f"lumi:actor-routing:updates:{group}"


async def publish_snapshot(redis, snapshot: ActorRoutingSnapshot):
    await redis.eval(
        _PUBLISH, 2, *index_keys(snapshot.group), f"{snapshot.module}:{snapshot.guild_id}",
        snapshot.revision, encode(snapshot),
    )
