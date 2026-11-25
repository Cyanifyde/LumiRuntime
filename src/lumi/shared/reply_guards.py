import hashlib
import re
import secrets
from typing import Annotated, Literal

import msgspec

from lumi.contracts import encode
from lumi.transport import GROUPS

TOKEN = Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")]


class ReplyGuard(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    schema: Literal[1]
    module: str
    generation: int
    actor: int
    guild: int
    channel: int
    expires_at_ms: int
    digest: str
    private_target: int = 0
    require_nsfw: bool = False
    require_admin: bool = False
    capture_revision: int = 0
    participants: Annotated[tuple[int, ...], msgspec.Meta(max_length=202)] = ()
    versions: Annotated[tuple[tuple[TOKEN, TOKEN], ...], msgspec.Meta(max_length=128)] = ()


def guard_key(group, job):
    if group not in GROUPS or re.fullmatch(r"[a-f0-9]{32}", job) is None:
        raise ValueError("Invalid reply guard key.")
    return f"lumi:reply-guard:{group}:{job}"


def version_key(group, module, guild, scope):
    if (
        group not in GROUPS or re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", module) is None
        or type(guild) is not int or not 0 < guild < 2**64
        or re.fullmatch(r"[a-f0-9]{32}", scope) is None
    ):
        raise ValueError("Invalid reply scope key.")
    return f"lumi:reply-guard-version:{group}:{module}:{guild}:{scope}"


def scope_id(kind, key):
    return hashlib.sha256(encode((kind, str(key)))).hexdigest()[:32]


_VERSIONS = """
local versions = {}
for i=1,#KEYS do
    local value = redis.call('GET', KEYS[i])
    if not value then value = ARGV[2] end
    redis.call('SET', KEYS[i], value, 'PX', ARGV[1])
    versions[i] = value
end
return versions
"""


_CHECK = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
for i=2,#KEYS do
    if redis.call('GET', KEYS[i]) ~= ARGV[i+1] then return 0 end
end
if ARGV[2] == 'claim' then redis.call('DEL', KEYS[1]) end
return 1
"""


async def snapshot_versions(redis, group, module, guild, identities, ttl_ms):
    scopes = sorted({scope_id(kind, key) for kind, key in identities})
    if not scopes:
        return ()
    keys = [version_key(group, module, guild, scope) for scope in scopes]
    values = await redis.eval(_VERSIONS, len(keys), *keys, ttl_ms, secrets.token_hex(16))
    return tuple(
        (key, value.decode("ascii") if isinstance(value, bytes) else value)
        for key, value in zip(scopes, values, strict=True)
    )


async def revoke_versions(redis, group, module, guild, identities):
    keys = [
        version_key(group, module, guild, scope_id(kind, key)) for kind, key in identities
    ]
    if keys:
        await redis.delete(*keys)


async def verify_guard(redis, job, guard, payload, *, claim):
    scopes = tuple(
        version_key(job.group, job.module, job.guild_id, key) for key, _ in guard.versions
    )
    return bool(await redis.eval(
        _CHECK, 1 + len(scopes), guard_key(job.group, job.job_id), *scopes,
        payload, "claim" if claim else "check", *(version for _, version in guard.versions),
    ))
