from lumi.contracts import encode
from lumi.contracts.activity_observations import LIFETIME_MS, MAX_PENDING, admission_key, stream_key

_APPEND = """
local permit = redis.call('GET', KEYS[1])
if not permit or #permit > 80 or string.sub(permit, 1, #ARGV[1]) ~= ARGV[1] then
    return 0
end
redis.call('XADD', KEYS[2], 'MAXLEN', '=', ARGV[2], '*', 'scope', permit, 'event', ARGV[3])
redis.call('PEXPIRE', KEYS[2], ARGV[4])
return 1
"""


class ActivityRelay:
    def __init__(self, redis):
        self._append = redis.register_script(_APPEND)

    async def forward(self, manifest, observation):
        return bool(
            await self._append(
                keys=[
                    admission_key(manifest.group, manifest.module, observation.guild),
                    stream_key(manifest.group, manifest.module),
                ],
                args=[
                    f"{manifest.generation}:",
                    MAX_PENDING,
                    encode(observation),
                    LIFETIME_MS,
                ],
            )
        )
