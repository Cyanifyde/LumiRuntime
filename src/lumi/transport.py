from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache
from pathlib import Path

GROUPS = ("platform", "security", "community", "cards", "tools", "ai", "media")
GUILD_CONTROL_GROUP = "platform"
_STREAMS = ContextVar("lumi_role_streams", default=None)
REPLY_GROUP = ContextVar("lumi_action_reply_group", default=None)


class TransportScopeError(ValueError):
    pass


def enabled():
    return os.getenv("LUMI_REDIS_ROLE_NAMESPACES") == "1"


def legacy_writes():


    return os.getenv("LUMI_REDIS_LEGACY_WRITES") != "0"


def legacy_bindings():
    return os.getenv("LUMI_REDIS_LEGACY_BINDINGS") != "0"


def outbound(base, group):
    if group not in GROUPS:
        raise TransportScopeError("Unknown Redis service group.")
    return base + ":" + group if enabled() else base


def incoming(base):

    return (base, *(base + ":" + group for group in GROUPS)) if enabled() else (base,)


def current(base):
    return (_STREAMS.get() or {}).get(base, base)


def origin(base):
    actual = current(base)
    if actual == base:
        return None
    group = actual.removeprefix(base + ":")
    if actual != base + ":" + group or group not in GROUPS:
        raise TransportScopeError("Unknown Redis stream origin.")
    return group


def require_origin(base, group):
    expected = origin(base)
    if expected is not None and expected != group:
        raise TransportScopeError("Redis stream origin does not match its payload.")


@contextmanager
def stream_scope(base, actual):
    if isinstance(actual, bytes):
        actual = actual.decode("ascii")
    token = _STREAMS.set({**(_STREAMS.get() or {}), base: actual})
    try:
        origin(base)
        yield
    finally:
        _STREAMS.reset(token)


def action_reply_key(identifier, group):
    if group not in GROUPS:
        raise TransportScopeError("Unknown action reply service group.")
    return f"lumi:edge-action-result:{group}:{identifier}"


def response_state_key(identifier, group):
    if group not in (*GROUPS, "edge"):
        raise TransportScopeError("Unknown response service group.")
    if enabled():
        return f"lumi:response-state:{group}:{identifier}"
    return "lumi:response-state:" + identifier


@lru_cache(maxsize=1)
def _module_owners(path, inode, modified, size):
    del inode, modified
    if size > 16384:
        raise TransportScopeError("Module ownership policy unavailable.")
    document = json.loads(Path(path).read_bytes())
    if set(document) != {"schema", "owners"} or document["schema"] != 1:
        raise TransportScopeError("Module ownership policy unavailable.")
    owners = document["owners"]
    if not isinstance(owners, dict) or not owners:
        raise TransportScopeError("Module ownership policy unavailable.")
    for module, group in owners.items():
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", module) or group not in GROUPS:
            raise TransportScopeError("Module ownership policy unavailable.")
    return owners


def configured_module_owners():
    configured = os.getenv("LUMI_MODULE_OWNERS_FILE")
    if not configured and not enabled():
        return None
    try:
        if not configured:
            raise ValueError
        path = Path(configured)
        info = path.lstat()
        if path.is_symlink() or not path.is_file():
            raise ValueError
        return _module_owners(str(path), info.st_ino, info.st_mtime_ns, info.st_size)
    except (OSError, ValueError, TypeError, KeyError):
        raise TransportScopeError("Module ownership policy unavailable or mismatched.") from None


def require_module_owner(module, group):
    owners = configured_module_owners()
    if owners is not None and owners.get(module) != group:
        raise TransportScopeError("Module ownership policy unavailable or mismatched.")


def reserved_control_roots():

    owners = configured_module_owners() or {}
    return frozenset(module for module, group in owners.items() if group == GUILD_CONTROL_GROUP)


def require_manifest_transport(manifest):
    require_module_owner(manifest.module, manifest.group)
    if enabled() and manifest.job_stream != f"lumi:jobs:{manifest.group}":
        raise TransportScopeError("A service may only register its own job stream.")
