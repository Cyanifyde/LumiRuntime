from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from lumi.events.prizepool import MAX_BYTES, snapshot

LOGGER = logging.getLogger(__name__)


@contextmanager
def writer_lock(root: Path):
    with (root / ".writer-lock").open("a+b") as lock:
        if os.name == "posix":
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX)
        yield


class ProjectionPublisher:


    def __init__(self, root: Path, clock=None):
        self.root = root
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.lock = asyncio.Lock()
        self.accepted = {}

    async def __call__(self, guild: int, state: dict) -> None:
        try:
            async with self.lock:
                value = snapshot(guild, state, self.clock())
                fence = (value["generation"], value["revision"])
                if fence < self.accepted.get(guild, (-1, -1)):
                    return
                await asyncio.to_thread(self.write, guild, value, fence)
                self.accepted[guild] = fence
        except Exception:

            LOGGER.warning("event_web_projection_unavailable")

    def write(self, guild: int, value: dict, fence: tuple) -> None:
        with writer_lock(self.root):
            self._write(guild, value, fence)

    def _write(self, guild: int, value: dict, fence: tuple) -> None:
        target = self.root / f"{guild}.json"
        try:
            previous = json.loads(target.read_bytes())
            if fence < (previous["generation"], previous["revision"]):
                return
        except (OSError, ValueError, KeyError):
            pass
        data = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        if len(data) > MAX_BYTES:
            raise ValueError("Projection too large.")
        fd, name = tempfile.mkstemp(prefix=".pending-", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
                os.fchmod(output.fileno(), 0o640) if hasattr(os, "fchmod") else None
            os.replace(name, target)
            if os.name == "posix":
                directory = os.open(self.root, os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        finally:
            if os.path.exists(name):
                os.unlink(name)
