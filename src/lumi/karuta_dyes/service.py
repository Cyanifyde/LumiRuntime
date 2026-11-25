from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
import json
import os
import re
import secrets
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import aiohttp

from .engine import VERSION, Geometry, analyse_array, decode_png
from .perceptual import MODEL_VERSION
from .renderer import RENDER_VERSION, render_dye

CDN = "https://d1wg5e521d1fbb.cloudfront.net/"
SOURCE_URL = re.compile(re.escape(CDN) + r"[a-f0-9]{32,64}\.png")
CACHE_VERSION = f"{VERSION}:{MODEL_VERSION}:{RENDER_VERSION}"
MAX_SOURCE_BYTES = 1_000_000
MAX_REPORT_BYTES = 4_000_000
MAX_METADATA_BYTES = 128_000
MAX_CACHE_BYTES = 512 * 1024 * 1024
MAX_CACHE_ENTRIES = 2048
MAX_MEMORY_BYTES = 16 * 1024 * 1024
OWN_FILE = re.compile(r"([a-f0-9]{64})(?:\.source\.png|\.png|\.json)")


@dataclass(frozen=True, slots=True)
class DyeView:
    name: str
    code: str
    source_url: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not 1 <= len(self.name) <= 100
            or any(character in self.name for character in "\r\n\x00")
            or not isinstance(self.code, str)
            or not re.fullmatch(r"\$[a-z0-9]{1,20}", self.code)
            or not isinstance(self.source_url, str)
            or not SOURCE_URL.fullmatch(self.source_url)
        ):
            raise ValueError("Unsupported dye view")

    @property
    def mystic(self) -> bool:
        return self.name.startswith("Mystic ")

    @property
    def identity(self) -> dict[str, str]:
        return {
            "name": self.name,
            "code": self.code,
            "source_url": self.source_url,
            "version": CACHE_VERSION,
        }

    @property
    def cache_key(self) -> str:
        return hashlib.sha256(
            json.dumps(self.identity, sort_keys=True, ensure_ascii=True).encode("ascii")
        ).hexdigest()


def build_report(raw: bytes, view: DyeView, geometry: Geometry) -> tuple[bytes, dict]:

    import base64

    record = analyse_array(decode_png(raw), geometry, mystic=view.mystic)
    record.update(
        name=view.name,
        code=view.code,
        mystic=view.mystic,
        image="data:image/png;base64," + base64.b64encode(raw).decode("ascii"),
    )
    output = io.BytesIO()
    render_dye(record).save(output, "PNG", compress_level=6)
    data = output.getvalue()
    if len(data) > MAX_REPORT_BYTES:
        raise ValueError("Dye report exceeds its output bound")
    record.pop("image")
    return data, record


class DyeReportService:


    def __init__(
        self,
        root: Path,
        *,
        fetcher: Callable[[str], Awaitable[bytes]] | None = None,
        max_bytes: int = MAX_CACHE_BYTES,
        max_entries: int = MAX_CACHE_ENTRIES,
    ) -> None:
        self.root = root.resolve()
        self._fetcher = fetcher or self._fetch
        self._session: aiohttp.ClientSession | None = None
        self._geometry: Geometry | None = None
        self._tasks: dict[str, asyncio.Task[bytes]] = {}
        self._memory: OrderedDict[str, bytes] = OrderedDict()
        self._memory_bytes = 0
        self._slots = asyncio.Semaphore(2)
        self._disk_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._started = False
        self._closed = False
        self._max_bytes = max_bytes
        self._max_entries = max_entries

    async def start(self) -> None:
        async with self._start_lock:
            if self._closed:
                raise RuntimeError("Dye service is closed")
            if not self._started:
                await asyncio.to_thread(self.root.mkdir, parents=True, exist_ok=True)
                self._geometry = await asyncio.to_thread(Geometry)
                await asyncio.to_thread(self._prune)
                self._started = True

    async def image(self, view: DyeView) -> bytes:
        await self.start()
        key = view.cache_key
        if key in self._memory:
            self._memory.move_to_end(key)
            return self._memory[key]
        task = self._tasks.get(key)
        if task is None:
            task = asyncio.create_task(self._load(view), name="karuta-dye-report")
            self._tasks[key] = task
            task.add_done_callback(lambda done: self._finished(key, done))

        return await asyncio.shield(task)

    def _finished(self, key: str, task: asyncio.Task[bytes]) -> None:
        if self._tasks.get(key) is task:
            self._tasks.pop(key)
        if not task.cancelled():
            task.exception()

    async def _load(self, view: DyeView) -> bytes:
        async with self._slots:
            async with self._disk_lock:
                data = await asyncio.to_thread(self._read, view)
            if data is None:
                raw = await self._fetcher(view.source_url)
                if self._geometry is None:
                    raise RuntimeError("Dye geometry is not loaded")
                data, record = await asyncio.to_thread(build_report, raw, view, self._geometry)
                async with self._disk_lock:
                    await asyncio.to_thread(self._store, view, raw, data, record)
            self._memory[view.cache_key] = data
            self._memory_bytes += len(data)
            while self._memory_bytes > MAX_MEMORY_BYTES:
                _, removed = self._memory.popitem(last=False)
                self._memory_bytes -= len(removed)
            return data

    async def _fetch(self, url: str) -> bytes:
        if not SOURCE_URL.fullmatch(url):
            raise ValueError("Unsupported dye URL")
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15, connect=5),
                connector=aiohttp.TCPConnector(limit=2),
                auto_decompress=False,
                trust_env=False,
            )
        async with self._session.get(url, allow_redirects=False) as response:
            if (
                response.status != 200
                or response.content_type != "image/png"
                or response.headers.get("Content-Encoding", "identity") != "identity"
                or (response.content_length or 0) > MAX_SOURCE_BYTES
            ):
                raise ValueError("Unsupported dye response")
            raw = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                raw.extend(chunk)
                if len(raw) > MAX_SOURCE_BYTES:
                    raise ValueError("Dye source exceeds its input bound")
            return bytes(raw)

    def _path(self, key: str, suffix: str) -> Path:
        path = self.root / (key + suffix)
        if not re.fullmatch(r"[a-f0-9]{64}", key) or path.is_symlink():
            raise ValueError("Unsafe dye cache path")
        if path.resolve().parent != self.root:
            raise ValueError("Dye cache path escapes its namespace")
        return path

    def _read_bytes(self, key: str, suffix: str, maximum: int) -> bytes:
        with self._path(key, suffix).open("rb") as stream:
            data = stream.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError("Oversized cached dye file")
        return data

    def _read(self, view: DyeView) -> bytes | None:
        try:
            key = view.cache_key
            metadata = json.loads(self._read_bytes(key, ".json", MAX_METADATA_BYTES))
            if metadata["identity"] != view.identity:
                return None
            source = self._read_bytes(key, ".source.png", MAX_SOURCE_BYTES)
            data = self._read_bytes(key, ".png", MAX_REPORT_BYTES)
            if (
                not source.startswith(b"\x89PNG\r\n\x1a\n")
                or not data.startswith(b"\x89PNG\r\n\x1a\n")
                or hashlib.sha256(source).hexdigest() != metadata["source_sha256"]
                or hashlib.sha256(data).hexdigest() != metadata["report_sha256"]
            ):
                return None
            path = self._path(key, ".json")
            if path.stat().st_mtime < time.time() - 60:
                path.touch()
            return data
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def _atomic_write(self, key: str, suffix: str, data: bytes) -> None:
        destination = self._path(key, suffix)
        temporary = self.root / f"{key}.{secrets.token_hex(8)}.part"
        try:
            with temporary.open("xb") as stream:
                stream.write(data)
            os.replace(temporary, destination)
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    def _store(self, view: DyeView, source: bytes, data: bytes, record: dict) -> None:
        metadata = json.dumps(
            {
                "identity": view.identity,
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "report_sha256": hashlib.sha256(data).hexdigest(),
                "measurements": record,
            },
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("ascii")
        if len(metadata) > MAX_METADATA_BYTES:
            raise ValueError("Dye metadata exceeds its cache bound")
        key = view.cache_key
        self._atomic_write(key, ".source.png", source)
        self._atomic_write(key, ".png", data)

        self._atomic_write(key, ".json", metadata)
        self._prune()

    def _prune(self) -> None:
        families: dict[str, list[Path]] = {}
        for path in self.root.iterdir():
            match = OWN_FILE.fullmatch(path.name)
            if match and not path.is_symlink() and path.is_file():
                families.setdefault(match[1], []).append(path)
            elif (
                re.fullmatch(r"[a-f0-9]{64}\.[a-f0-9]{16}\.part", path.name)
                and not path.is_symlink()
                and path.is_file()
                and path.stat().st_mtime < time.time() - 3600
            ):
                path.unlink()
        rows = []
        for key, paths in families.items():
            committed = self._path(key, ".json")
            last_used = committed.stat().st_mtime if committed.exists() else 0
            rows.append((last_used, key, sum(path.stat().st_size for path in paths), paths))
        total = sum(row[2] for row in rows)
        count = len(rows)
        for _, _key, size, paths in sorted(rows):
            if total <= self._max_bytes and count <= self._max_entries:
                break
            for path in paths:

                if path.resolve().parent != self.root or path.is_symlink():
                    raise ValueError("Unsafe dye cache eviction")
                path.unlink(missing_ok=True)
            total -= size
            count -= 1

    async def close(self) -> None:
        self._closed = True
        tasks = tuple(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._session is not None:
            await self._session.close()
