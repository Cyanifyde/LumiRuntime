from typing import Annotated, Literal

import msgspec

from lumi.contracts import decode, encode

from .capture_relay import route_scope


class CaptureAdmission(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    guild_id: int
    revision: Annotated[int, msgspec.Meta(gt=0)]

    excluded_channels: Annotated[tuple[int, ...], msgspec.Meta(max_length=25)]
    allow_automated: bool


def admission_key(group, route):
    return "lumi:capture-admission:" + route_scope(group, route)


_PUBLISH = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current then
    local ok, decoded = pcall(cjson.decode, current)
    if ok and type(decoded) == 'table' and type(decoded.revision) == 'number'
        and decoded.revision >= tonumber(ARGV[2]) then return 0 end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
"""


def decode_admission(raw):
    rules = decode(raw, CaptureAdmission)
    if any(not 0 < identifier < 2**64 for identifier in (rules.guild_id, *rules.excluded_channels)):
        raise ValueError("Invalid capture admission channel scope.")
    if len(set(rules.excluded_channels)) != len(rules.excluded_channels):
        raise ValueError("Repeated capture admission channel.")
    return rules


async def publish_admission(redis, owner, value):
    payload = encode(value)
    decode_admission(payload)
    return await redis.eval(
        _PUBLISH, 1, admission_key(*owner), value.guild_id, value.revision, payload,
    )


async def refresh_publication(redis, owner, values):
    key = admission_key(*owner)
    published = await redis.hgetall(key)
    async with redis.pipeline(transaction=False) as pipe:
        changed = False
        for value in values:
            field = str(value.guild_id)
            payload = encode(value)
            if payload == published.get(field.encode(), published.get(field)):
                continue
            decode_admission(payload)
            pipe.eval(_PUBLISH, 1, key, field, value.revision, payload)
            changed = True
        if changed:
            await pipe.execute()


class CaptureAdmissionIndex:


    def __init__(self, redis, owner):
        self.redis, self.key = redis, admission_key(*owner)
        self._rules = {}
        self._minimum_revision = {}

    @property
    def guilds(self):
        return tuple(self._rules)

    def revision(self, guild_id):
        rules = self._rules.get(guild_id)
        return rules.revision if rules is not None else 0

    def require_revision(self, guild_id, revision):

        if revision > self.revision(guild_id):
            self._minimum_revision[guild_id] = max(
                revision, self._minimum_revision.get(guild_id, 0),
            )
            self._rules.pop(guild_id, None)

    def permits(self, guild_id, *, channels=(), automated=False):
        rules = self._rules.get(guild_id)
        return (
            rules is not None and (not automated or rules.allow_automated)
            and all(channel not in rules.excluded_channels for channel in channels)
        )

    @staticmethod
    def _decode(raw):
        try:
            return decode_admission(raw)
        except (ValueError, TypeError):
            return None

    def _meets_floor(self, rules):
        return rules.revision >= self._minimum_revision.get(rules.guild_id, 0)

    async def refresh(self):
        previous = self._rules.copy()

        published = await self.redis.hgetall(self.key)
        newest = {}
        for field, raw in published.items():
            rules = self._decode(raw)
            if (
                rules is not None and self._meets_floor(rules)
                and field in (str(rules.guild_id), str(rules.guild_id).encode())
            ):
                newest[rules.guild_id] = rules
        for guild, rules in self._rules.items():
            loaded = newest.get(guild)
            if (loaded is None and previous.get(guild) != rules) or (
                loaded is not None and loaded.revision < rules.revision
            ):
                newest[guild] = rules
        self._rules = newest

    async def refresh_guild(self, guild_id):
        previous = self._rules.get(guild_id)
        rules = self._decode(await self.redis.hget(self.key, guild_id))
        if rules is None or rules.guild_id != guild_id:
            if previous is self._rules.get(guild_id):
                self._rules.pop(guild_id, None)
        elif rules.revision >= self.revision(guild_id) and self._meets_floor(rules):
            self._rules[guild_id] = rules
