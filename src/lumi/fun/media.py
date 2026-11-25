from __future__ import annotations

import asyncio
import json
import re
import time
from collections import OrderedDict
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import aiohttp
import msgspec

from .rules import gif_url

API_ROOT = "https://api.klipy.com/api/v1"
MEDIA_HOSTS = frozenset({"static.klipy.com", "static1.klipy.com", "static2.klipy.com"})
MAX_RESPONSE_BYTES = 512 * 1024
REQUEST_TIMEOUT_SECONDS = 5
ATTRIBUTION = "Powered by KLIPY"


class MediaUnavailable(Exception):
    pass


class ResolvedGif(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    source_url: str
    media_url: str


def validate_media(item: ResolvedGif, source: str) -> ResolvedGif:
    if item.schema != 1 or item.source_url != source:
        raise MediaUnavailable()
    try:
        if gif_url(source)[1] != source:
            raise MediaUnavailable()
    except ValueError:
        raise MediaUnavailable() from None
    url = item.media_url
    if not isinstance(url, str) or len(url) > 2000 or any(ord(c) < 33 for c in url):
        raise MediaUnavailable()
    try:
        parsed = urlsplit(url)
    except ValueError:
        raise MediaUnavailable() from None
    if (
        parsed.scheme != "https"
        or parsed.netloc.casefold() not in MEDIA_HOSTS
        or not parsed.path.lower().endswith(".gif")
        or parsed.fragment
    ):
        raise MediaUnavailable()

    return item


def parse_media(payload: object, source: str) -> ResolvedGif:
    if not isinstance(payload, dict) or payload.get("result") is not True:
        raise MediaUnavailable()
    data = payload.get("data")
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list) or len(rows) > 32:
        raise MediaUnavailable()
    slug = urlsplit(source).path.rsplit("/", 1)[-1]
    matches = [r for r in rows if isinstance(r, dict) and r.get("slug") == slug]
    if len(matches) != 1 or matches[0].get("type", "gif") != "gif":
        raise MediaUnavailable()
    files = matches[0].get("file")
    if not isinstance(files, dict):
        raise MediaUnavailable()
    for quality in ("hd", "md", "sm"):
        formats = files.get(quality)
        image = formats.get("gif") if isinstance(formats, dict) else None
        if not isinstance(image, dict):
            continue
        try:
            return validate_media(ResolvedGif(1, source, image.get("url")), source)
        except MediaUnavailable:
            continue
    raise MediaUnavailable()


class KlipyMediaService:


    def __init__(self, store, *, key_file: Path) -> None:
        self._store = store
        self._key_file = key_file
        self._session: aiohttp.ClientSession | None = None
        self._memory: OrderedDict[str, ResolvedGif] = OrderedDict()
        self._inflight: dict[str, asyncio.Task[ResolvedGif]] = {}
        self._gate = asyncio.Semaphore(4)
        self._retry_at = 0.0

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS, connect=2),
                connector=aiohttp.TCPConnector(limit=4, ttl_dns_cache=300),
                trust_env=False,
            )

    async def close(self) -> None:
        tasks = tuple(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._inflight.clear()
        self._memory.clear()
        session, self._session = self._session, None
        if session is not None:
            await session.close()

    async def resolve(self, source: str) -> ResolvedGif:
        _, canonical = gif_url(source)
        cached = self._memory.get(canonical)
        if cached is not None:
            self._memory.move_to_end(canonical)
            return cached
        task = self._inflight.get(canonical)
        if task is None:
            if len(self._inflight) >= 64:
                raise MediaUnavailable()
            task = asyncio.create_task(self._resolve(canonical))
            self._inflight[canonical] = task
            task.add_done_callback(lambda completed: self._forget(canonical, completed))
        return await asyncio.shield(task)

    def _forget(self, source: str, task: asyncio.Task[ResolvedGif]) -> None:
        if self._inflight.get(source) is task:
            del self._inflight[source]
        if not task.cancelled():
            task.exception()

    async def _resolve(self, source: str) -> ResolvedGif:
        cached = await self._store.read_media(source)
        if cached is None:
            resolved = await self._fetch(source)

            cached = await self._store.save_media(resolved)
        result = validate_media(cached, source)
        self._memory[source] = result
        self._memory.move_to_end(source)
        while len(self._memory) > 512:
            self._memory.popitem(last=False)
        return result

    async def _fetch(self, source: str) -> ResolvedGif:
        if time.monotonic() < self._retry_at:
            raise MediaUnavailable()
        try:
            async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
                key = (await asyncio.to_thread(self._key_file.read_text, encoding="utf-8")).strip()
                if re.fullmatch(r"[A-Za-z0-9_-]{16,256}", key) is None:
                    raise MediaUnavailable()
                await self.start()
                slug = urlsplit(source).path.rsplit("/", 1)[-1]
                async with self._gate:
                    if time.monotonic() < self._retry_at:
                        raise MediaUnavailable()
                    async with self._session.get(
                        API_ROOT + "/" + key + "/gifs/items",
                        params={"slugs": slug},
                        allow_redirects=False,
                    ) as response:
                        if response.status == 429:
                            try:
                                delay = float(response.headers.get("Retry-After", "60"))
                            except ValueError:
                                delay = 60
                            self._retry_at = time.monotonic() + max(1, min(delay, 3600))
                            raise MediaUnavailable()
                        if response.status != 200 or response.content_type != "application/json":
                            raise MediaUnavailable()
                        if (response.content_length or 0) > MAX_RESPONSE_BYTES:
                            raise MediaUnavailable()
                        body = bytearray()
                        async for chunk in response.content.iter_chunked(65_536):
                            body.extend(chunk)
                            if len(body) > MAX_RESPONSE_BYTES:
                                raise MediaUnavailable()
                        item = parse_media(json.loads(body), source)
                        if key in item.media_url:
                            raise MediaUnavailable()
                        return item
        except (aiohttp.ClientError, OSError, TimeoutError, ValueError):

            raise MediaUnavailable() from None
