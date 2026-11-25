from lumi.contracts import encode
from lumi.contracts.reaction_subscriptions import ReactionSubscriptions
from lumi.transport import GROUPS

_PUBLISH = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if raw and cjson.decode(raw).revision >= tonumber(ARGV[2]) then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', 10000, '*', 'snapshot', ARGV[3])
return 1
"""


def index_keys(group):
    if group not in GROUPS:
        raise ValueError("invalid subscription group")
    return f"lumi:actor-reactions:current:{group}", f"lumi:actor-reactions:updates:{group}"


async def publish_subscriptions(redis, snapshot: ReactionSubscriptions):
    await redis.eval(
        _PUBLISH,
        2,
        *index_keys(snapshot.group),
        f"{snapshot.module}:{snapshot.user_id}",
        snapshot.revision,
        encode(snapshot),
    )
