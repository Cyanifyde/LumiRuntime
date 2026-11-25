from __future__ import annotations

import hashlib
import re
import time

import msgspec
from redis.exceptions import RedisError

from lumi.transport import GROUPS

from .job_files import HEX_ID

PREFIX = "lumi:attachment-payloads:v1:"
BYTE_LIMIT = 25 * 1024 * 1024
FILE_LIMIT = 10
LIFETIME_MS = 300_000
ROLE_FIELD_BUDGET = 250

PACKED_LIMIT = BYTE_LIMIT + 1 + 5 * FILE_LIMIT
ROLE_BYTE_BUDGET = 2 * PACKED_LIMIT
REFERENCE = re.compile(r"(?:tmp:[a-f0-9]{32,64}|job-file:[a-f0-9]{32})\Z")
TRANSITION_SCOPE = re.compile(r"transition:[1-9][0-9]{0,19}:[a-f0-9]{8}\Z")
ACTION_SCOPE = re.compile(r"action:([a-f0-9]{32}):[a-f0-9]{32}\Z")

STORE = """
local hash, field = KEYS[1], ARGV[1]
local old_size = redis.call('HSTRLEN', hash, field)
if old_size > 0 then
    if old_size == #ARGV[2] and redis.call('HGET', hash, field) == ARGV[2] then
        return 1
    end
    return -1
end
local fields = redis.call('HKEYS', hash)
if #fields >= tonumber(ARGV[4]) then return 0 end
local size = #ARGV[2]
for _, name in ipairs(fields) do
    size = size + redis.call('HSTRLEN', hash, name)
end
if size > tonumber(ARGV[5]) then return 0 end
if not redis.acl_check_cmd('HPEXPIREAT', hash, ARGV[3], 'FIELDS', 1, field) then
    return -2
end
redis.call('HSET', hash, field, ARGV[2])
local expiry = redis.call('HPEXPIREAT', hash, ARGV[3], 'FIELDS', 1, field)
return expiry[1] == 1 and 1 or 0
"""


class AttachmentUnavailable(ValueError):
    pass


def references(intent):
    structured = intent.attachments
    legacy = intent.attachment_ref
    if legacy is not None and structured:
        raise AttachmentUnavailable("Mixed upload references.")
    return (legacy,) if legacy is not None else tuple(item.reference for item in structured)


def transition_scope(guild_id, transition):
    scope = f"transition:{guild_id}:{transition}"
    if not TRANSITION_SCOPE.fullmatch(scope):
        raise AttachmentUnavailable("Invalid upload binding.")
    return scope


def action_scope(job_id, action_id):
    scope = f"action:{job_id}:{action_id}"
    if not ACTION_SCOPE.fullmatch(scope):
        raise AttachmentUnavailable("Invalid upload binding.")
    return scope


def file_job(scope):
    action = ACTION_SCOPE.fullmatch(scope)
    return action[1] if action else scope


def address(group, job_id, refs):
    transition = isinstance(job_id, str) and TRANSITION_SCOPE.fullmatch(job_id)
    if (
        group not in GROUPS
        or not isinstance(job_id, str)
        or not (HEX_ID.fullmatch(job_id) or transition or ACTION_SCOPE.fullmatch(job_id))
        or not 1 <= len(refs) <= FILE_LIMIT
        or any(not isinstance(ref, str) or not REFERENCE.fullmatch(ref) for ref in refs)
        or (transition and any(not ref.startswith("tmp:") for ref in refs))
        or len(set(refs)) != len(refs)
    ):
        raise AttachmentUnavailable("Invalid upload binding.")
    digest = hashlib.sha256(msgspec.msgpack.encode(refs)).hexdigest()
    return PREFIX + group, f"{job_id}:{digest}"


def validate_bytes(intent, contents):
    refs = references(intent)
    if (
        len(contents) != len(refs)
        or any(not isinstance(data, bytes) or not data for data in contents)
        or sum(map(len, contents)) > BYTE_LIMIT
    ):
        raise AttachmentUnavailable("Invalid upload size.")
    if not intent.attachments:
        return
    for item, data in zip(intent.attachments, contents, strict=True):
        if item.size != len(data) or item.sha256 != hashlib.sha256(data).hexdigest():
            raise AttachmentUnavailable("Upload does not match its declared bytes.")


class AttachmentPayloads:
    def __init__(self, cache, *, clock=None):
        self.cache = cache
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    def _expiry(self, intent, deadline):
        expiry = min(
            deadline,
            self.clock() + LIFETIME_MS,
            *(item.expires_at_ms for item in intent.attachments),
        )
        if expiry <= self.clock():
            raise AttachmentUnavailable("Upload expired.")
        return expiry

    async def publish(self, group, job_id, intent, contents, *, deadline):
        refs = references(intent)
        if not refs:
            return
        key, field = address(group, job_id, refs)
        validate_bytes(intent, contents)
        expiry = self._expiry(intent, deadline)
        try:
            result = await self.cache.eval(
                STORE,
                1,
                key,
                field,
                msgspec.msgpack.encode(contents),
                expiry,
                ROLE_FIELD_BUDGET,
                ROLE_BYTE_BUDGET,
            )
        except RedisError:
            raise AttachmentUnavailable("Upload storage unavailable.") from None
        if result != 1:
            raise AttachmentUnavailable("Upload could not be staged.")

    async def read(self, group, job_id, intent, *, deadline):
        refs = references(intent)
        if not refs:
            return ()
        key, field = address(group, job_id, refs)
        self._expiry(intent, deadline)
        try:
            raw = await self.cache.hget(key, field)
        except RedisError:
            raise AttachmentUnavailable("Upload storage unavailable.") from None
        if not isinstance(raw, bytes) or not 0 < len(raw) <= PACKED_LIMIT:
            raise AttachmentUnavailable("Upload no longer available.")
        try:
            contents = msgspec.msgpack.decode(raw, type=tuple[bytes, ...])
        except msgspec.DecodeError:
            raise AttachmentUnavailable("Invalid upload packet.") from None
        validate_bytes(intent, contents)
        self._expiry(intent, deadline)
        return contents

    async def discard(self, group, job_id, intent):


        try:
            refs = references(intent)
            if refs:
                key, field = address(group, job_id, refs)
                await self.cache.hdel(key, field)
        except (AttachmentUnavailable, RedisError):
            pass
