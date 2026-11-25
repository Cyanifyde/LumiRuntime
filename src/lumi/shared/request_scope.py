from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps


@dataclass
class _Operation:
    owner: object
    values: dict = field(default_factory=dict)
    active: bool = True


@dataclass
class _Failure:
    error: Exception


_CURRENT: ContextVar[_Operation | None] = ContextVar("edge_request_work", default=None)


@contextmanager
def operation_scope():
    current = _CURRENT.get()
    if current and current.active and current.owner is asyncio.current_task():
        yield
        return
    scope = _Operation(asyncio.current_task())
    token = _CURRENT.set(scope)
    try:
        yield
    finally:
        scope.active = False
        scope.values.clear()
        _CURRENT.reset(token)


def scoped(method):
    @wraps(method)
    async def wrapped(*args, **kwargs):
        with operation_scope():
            return await method(*args, **kwargs)
    return wrapped


def memo(name, key, compute):
    scope = _CURRENT.get()
    if scope is None or not scope.active:
        return compute()
    key = (name, key)
    if key not in scope.values:
        try:
            scope.values[key] = compute()
        except Exception as error:
            scope.values[key] = _Failure(error)
    value = scope.values[key]
    if isinstance(value, _Failure):
        raise value.error
    return value


async def memo_read(name, key, compute):
    scope = _CURRENT.get()
    if scope is None or not scope.active:
        return await compute()
    key = (name, key)
    if key not in scope.values:
        scope.values[key] = asyncio.create_task(compute())
    return await asyncio.shield(scope.values[key])
