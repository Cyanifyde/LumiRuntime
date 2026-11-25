import re

import msgspec

from lumi.transport import GROUPS

ALLOW = "lumi:activity-input:v1:allow:"
STREAM = "lumi:activity-input:v1:stream:"
MAX_PENDING = 1000
LIFETIME_MS = 300_000


def stream_key(group, module):
    if group not in GROUPS or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", module):
        raise ValueError("Invalid activity observation owner.")
    return STREAM + group + ":" + module


def admission_key(group, module, guild):
    stream = stream_key(group, module)
    if type(guild) is not int or not 0 < guild < 2**64:
        raise ValueError("Invalid activity observation guild.")
    return ALLOW + stream.removeprefix(STREAM) + ":" + str(guild)


class ActivityObservation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    guild: int
    channel: int
    actor: int
    message: int
    occurred_at_ms: int

    def valid(self, now):
        return (
            all(0 < value < 2**64 for value in (self.guild, self.channel, self.actor, self.message))
            and now - LIFETIME_MS < self.occurred_at_ms <= now
        )
