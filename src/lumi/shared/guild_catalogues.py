from lumi.contracts import GuildCommandSnapshot, encode
from lumi.transport import GROUPS

_PUBLISH = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current then
  local previous = cjson.decode(current)
  if previous.revision >= tonumber(ARGV[2]) then return 0 end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', 4096, '*', 'catalogue', ARGV[3])
return 1
"""


def catalogue_keys(group):
    if group not in GROUPS:
        raise ValueError("invalid catalogue group")
    return f"lumi:guild-commands:current:{group}", f"lumi:guild-commands:updates:{group}"


async def publish_catalogue(queue, snapshot: GuildCommandSnapshot) -> None:
    await queue.eval(
        _PUBLISH, 2, *catalogue_keys(snapshot.group), f"{snapshot.module}:{snapshot.guild_id}",
        snapshot.revision, encode(snapshot),
    )
