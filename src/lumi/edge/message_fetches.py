from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps


OWN_OUTPUT_TTL = 366 * 86400
UNKNOWN_MESSAGE = 10008


def content_digest(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def unresolved_in_channel(reference, channel_id: int) -> bool:

    return bool(
        reference is not None and reference.message_id and reference.resolved is None
        and getattr(reference, "channel_id", channel_id) == channel_id
    )


async def remember_owned_output(
    redis, *, guild_id, channel_id, message_id, module, family, ttl=OWN_OUTPUT_TTL, panel=None,
):
    binding = {"module": module, "family": family, "output_channel": channel_id}
    if panel is not None:
        binding["panel"] = panel
    await redis.set(
        f"lumi:action-message-family:{guild_id}:{message_id}",
        json.dumps(binding),
        ex=ttl,
    )


async def action_message_binding(redis, guild_id, message_id):
    raw = await redis.get(f"lumi:action-message-family:{guild_id}:{message_id}")
    if raw:
        try:
            binding = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if isinstance(binding, dict) and isinstance(binding.get("module"), str):
            return binding
    return None


async def owned_output_module(redis, guild_id, channel_id, message_id):
    binding = await action_message_binding(redis, guild_id, message_id)
    if binding and binding.get("output_channel") == channel_id:
        return binding["module"]
    return None


@dataclass
class _Scope:
    edge: object
    owner: asyncio.Task | None
    reads: dict = field(default_factory=dict)
    active: bool = True


_CURRENT: ContextVar[_Scope | None] = ContextVar("edge_message_reads", default=None)


def _scope(edge):
    scope = _CURRENT.get()
    return scope if scope and scope.active and scope.edge is edge else None


@contextlib.contextmanager
def message_scope(edge):
    current = _scope(edge)
    if current and current.owner is asyncio.current_task():
        yield
        return
    scope = _Scope(edge, asyncio.current_task())
    token = _CURRENT.set(scope)
    try:
        yield
    finally:
        scope.active = False
        scope.reads.clear()
        _CURRENT.reset(token)


def message_fetch_scope(method):
    @wraps(method)
    async def wrapped(edge, *args, **kwargs):
        with message_scope(edge):
            return await method(edge, *args, **kwargs)

    return wrapped


def _matches(message, channel, message_id):
    return bool(
        message is not None
        and getattr(message, "id", None) == message_id
        and getattr(getattr(message, "channel", None), "id", None) == channel.id
        and getattr(getattr(message, "guild", None), "id", None)
        == getattr(getattr(channel, "guild", None), "id", None)
    )


def remember_message(edge, message):

    scope = _scope(edge)
    if scope and getattr(message, "channel", None) and getattr(message, "id", 0):
        key = (message.channel.id, message.id)
        if key in scope.reads:
            return
        future = asyncio.get_running_loop().create_future()
        future.set_result(message)
        scope.reads[key] = (future, False)


def cached_message(edge, channel, message_id, *, supplied=None):

    if _matches(supplied, channel, message_id):
        return supplied
    get = getattr(getattr(edge, "_connection", None), "_get_message", None)
    cached = get(message_id) if get else None
    return cached if _matches(cached, channel, message_id) else None


def known_message(edge, channel, message_id, *, supplied=None):

    observed = cached_message(edge, channel, message_id, supplied=supplied)
    return observed if observed is not None else channel.get_partial_message(message_id)


async def fetch_message(edge, channel, message_id, *, cached=False, refresh=False):

    scope = _scope(edge)
    key = (channel.id, message_id)
    entry = scope.reads.get(key) if scope and not refresh else None
    if entry and (entry[1] or cached):
        return await entry[0]
    if cached and not refresh:
        if (message := cached_message(edge, channel, message_id)) is not None:
            remember_message(edge, message)
            return message
    if scope:
        task = asyncio.create_task(channel.fetch_message(message_id))
        scope.reads[key] = (task, True)
        return await task
    return await channel.fetch_message(message_id)
