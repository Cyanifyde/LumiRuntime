import re
from typing import Literal

import msgspec

from .models import ChannelTransitionSpec

INDEX = "lumi:channel-fence:index"
WAKE = "lumi:channel-fence:refresh"
PUBLISHER = "lumi:generation:security"
SCHEMA_FIELD = "_schema"
DIGEST_FIELD = "_digest"
SCHEMA = b"1"
CRITICAL = frozenset({"quiesced", "creating", "building", "applying", "publishing", "deleting", "rollback"})
TERMINAL = frozenset({"complete", "rolled_back"})


DEFAULT_TRANSITION = ChannelTransitionSpec()


def blocked(gates, job, destination=None, *, spec=DEFAULT_TRANSITION):

    handler = job.request.handler
    for gate in gates:
        if gate["guild"] != job.guild_id:
            continue
        source = job.channel_id
        old, new, phase = gate["old"], gate.get("new", 0), gate["phase"]
        if phase == "complete":
            if old in {source, destination}:
                return True
            continue
        if phase == "rolled_back" or handler in spec.control_handlers:
            continue
        if phase in CRITICAL:
            if (
                spec.quiesce_guild
                or old in {source, destination}
                or (new and new in {source, destination})
            ):
                return True
        elif phase == "waiting":
            if handler in spec.suppress_handlers:
                return True
            if old in {source, destination} and not (
                handler in spec.drain_handlers
                or job.admitted_at_ms < gate["started"]
            ):
                return True
    return False


class ChannelFence(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    id: str
    guild: int
    old: int
    new: int
    phase: Literal["waiting", "quiesced", "creating", "building", "applying", "publishing", "deleting", "rollback",
                   "complete", "rolled_back"]
    started: int
    executor: tuple[str, str] | None = None

    @classmethod
    def project(cls, record):


        return msgspec.convert({k: record.get(k, 0) for k in ("id", "guild", "old", "new", "phase", "started")}, cls)

    def valid(self, guild):
        return bool(
            re.fullmatch(r"[a-f0-9]{8}", self.id) and self.guild == guild and 0 < guild < 2**64
            and 0 < self.old < 2**64 and 0 <= self.new < 2**64 and self.new != self.old
            and self.started > 0
            and (self.executor is None or all(re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", key) for key in self.executor))
        )


async def request_refresh(redis):

    async with redis.pipeline(transaction=True) as pipe:
        pipe.lpush(WAKE, "changed").ltrim(WAKE, 0, 0)
        await pipe.execute()
