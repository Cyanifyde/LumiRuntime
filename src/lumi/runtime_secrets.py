from __future__ import annotations

import os
import re
import stat
from pathlib import Path
from urllib.parse import urlparse


class SecretUnavailable(RuntimeError):
    pass


def read_secret(path: str | Path, *, maximum: int = 16384) -> str:
    target = Path(path)
    try:
        info = target.lstat()
        if not stat.S_ISREG(info.st_mode) or target.is_symlink():
            raise SecretUnavailable("A runtime secret is unavailable.")
        if not 0 < info.st_size <= maximum:
            raise SecretUnavailable("A runtime secret is unavailable.")
        if os.name == "posix" and info.st_mode & 0o007:
            raise SecretUnavailable("Runtime secret permissions are too broad.")
        fd = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as source:
            current = os.fstat(source.fileno())
            if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
                raise SecretUnavailable("A runtime secret changed during loading.")
            raw = source.read(maximum + 1)
        if len(raw) > maximum:
            raise SecretUnavailable("A runtime secret is unavailable.")
        value = raw.decode("utf-8").strip()
        if not value or any(ord(c) < 32 for c in value):
            raise SecretUnavailable("A runtime secret is unavailable.")
        return value
    except (OSError, UnicodeError):
        raise SecretUnavailable("A runtime secret is unavailable.") from None


def redis_url(kind: str, *, environ=None) -> str:

    env = os.environ if environ is None else environ
    if kind not in {"queue", "cache"}:
        raise ValueError("Unknown Redis connection.")
    prefix = "LUMI_REDIS_" + kind.upper() + "_URL"
    if prefix + "_FILE" in env:
        value = read_secret(env[prefix + "_FILE"])
        try:
            parsed = urlparse(value)
            if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
                raise ValueError
            if not parsed.username or not parsed.password:
                raise ValueError
            _ = parsed.port
        except ValueError:
            raise SecretUnavailable("An authenticated Redis connection is required.") from None
        return value

    if env.get("LUMI_REQUIRE_SECRET_FILES") == "1":
        raise SecretUnavailable("A Redis credential file is required.")
    try:
        return env[prefix]
    except KeyError:
        raise SecretUnavailable("A Redis connection is unavailable.") from None


def worker_lifecycle_key(path: str | Path) -> bytes:
    value = read_secret(path, maximum=65)
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise SecretUnavailable("A worker lifecycle key is unavailable.")
    return bytes.fromhex(value)
