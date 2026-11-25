from __future__ import annotations

import os
import re
import time

from lumi.transport import GROUPS, TransportScopeError, enabled, legacy_bindings, legacy_writes

KINDS = frozenset({"owner", "message", "origin-guild", "origin-channel"})


def legacy_key(kind, token):
    if kind not in KINDS:
        raise TransportScopeError("Unknown interface binding type.")
    return f"lumi:ui-{kind}:{token}"


def ui_key(kind, token, group=None):
    original = legacy_key(kind, token)
    if not enabled():
        return original
    group = group or os.getenv("LUMI_GROUP")
    if group not in GROUPS:
        raise TransportScopeError("An interface binding service is required.")
    return f"lumi:ui-{kind}:{group}:{token}"


def write_keys(kind, token, group):
    scoped, original = ui_key(kind, token, group), legacy_key(kind, token)
    return (scoped, original) if scoped != original and legacy_writes() else (scoped,)


async def read_ui(redis, kind, token, group):

    target = ui_key(kind, token, group)
    current = await redis.get(target)
    if current is not None or not enabled() or not legacy_bindings():
        return current
    original = legacy_key(kind, token)
    current = await redis.get(original)
    if current is None:
        return None
    remaining = await redis.pttl(original)
    if remaining <= 0:
        return None
    if await redis.set(target, current, px=remaining, nx=True):
        return current
    return await redis.get(target)


async def bound_interaction(redis, job):

    token = job.request.arguments.get("session")
    if not isinstance(token, str) or not re.fullmatch(r"[a-f0-9]{16,32}", token):
        return False
    owner = await read_ui(redis, "owner", token, job.group)
    message = await read_ui(redis, "message", token, job.group)
    channel = await read_ui(redis, "origin-channel", token, job.group)
    try:
        return (
            int(owner) == job.actor_id and int(message) == job.request.event.get("message_id")
            and int(channel) == job.channel_id
        )
    except (TypeError, ValueError):
        return False


def search_key(token, group=None):
    if not enabled():
        return "lumi:member-search:session:" + token
    group = group or os.getenv("LUMI_GROUP")
    if group not in GROUPS:
        raise TransportScopeError("A search session service is required.")
    return f"lumi:member-search:session:{group}:{token}"


def session_index(group):
    if group not in GROUPS:
        raise TransportScopeError("An interface index service is required.")
    return "lumi:ui-sessions:" + group


def remember_session(pipeline, group, token, *, ttl=900):
    if not enabled():
        return
    if not re.fullmatch(r"[a-f0-9]{16,32}", token):
        raise TransportScopeError("Invalid interface session identity.")
    now = int(time.time() * 1000)
    key = session_index(group)
    pipeline.zremrangebyscore(key, "-inf", now)
    pipeline.zadd(key, {token: now + ttl * 1000})
    pipeline.expire(key, ttl)


async def session_tokens(redis, group):
    now = int(time.time() * 1000)
    values = await redis.zrangebyscore(session_index(group), f"({now}", "+inf")
    result = []
    for value in values:
        try:
            token = value.decode("ascii") if isinstance(value, bytes) else value
            if re.fullmatch(r"[a-f0-9]{16,32}", token):
                result.append(token)
        except (UnicodeError, TypeError):
            continue
    return result


async def session_keys(redis, group, pattern, *, tokens=None):
    if not enabled():
        async for key in redis.scan_iter(match=pattern, count=100):
            yield key
        return
    if not pattern.endswith("*") or any(char in pattern[:-1] for char in "*?["):
        raise TransportScopeError("An exact interface-key prefix is required.")
    for token in await session_tokens(redis, group) if tokens is None else tokens:
        yield pattern[:-1] + token
