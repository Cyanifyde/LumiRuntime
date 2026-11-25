from __future__ import annotations

import asyncio
import base64
import io
import re
from urllib.parse import urlsplit

import aiohttp
import msgspec
from PIL import Image, ImageOps

from .archive import file_io
from .models import SavedMessage

MAX_INPUT = 25 * 1024 * 1024
MAX_PIXELS = 24_000_000
MAX_WIDTH = MAX_HEIGHT = 150
WEBP_QUALITY = 50
MAX_PREVIEW = 128 * 1024


def safe_media_url(value: str) -> bool:
    try:
        p = urlsplit(value)
        return bool(
            p.scheme == "https"
            and not p.username
            and not p.password
            and p.port is None
            and not p.fragment
            and len(value) <= 2048
            and (
                p.hostname in {"cdn.discordapp.com", "media.discordapp.net"}
                or re.fullmatch(r"images-ext-[0-9]+\.discordapp\.net", p.hostname or "")
            )
        )
    except ValueError:
        return False


def first_frame(data: bytes) -> bytes:
    if not data or len(data) > MAX_INPUT:
        raise ValueError("Image exceeds its bound.")
    try:
        with Image.open(io.BytesIO(data)) as original:
            if original.width * original.height > MAX_PIXELS:
                raise ValueError("Image exceeds its bound.")
            original.seek(0)
            image = ImageOps.exif_transpose(original).convert("RGBA")
            image.thumbnail((MAX_WIDTH, MAX_HEIGHT), Image.Resampling.LANCZOS)
            output = io.BytesIO()
            image.save(output, "WEBP", quality=WEBP_QUALITY, method=6)
            result = output.getvalue()
            if len(result) > MAX_PREVIEW:
                raise ValueError("Preview exceeds its bound.")
            return result
    except (OSError, Image.DecompressionBombError) as exc:
        raise ValueError("Image is unavailable.") from exc


class MediaService:


    def __init__(self, store, *, fetch=None):
        self.store, self.fetch = store, fetch or self.download
        self.session = None
        self.semaphore = asyncio.Semaphore(2)
        self.stopped = False
        self.draining = False

    async def download(self, url):
        if not safe_media_url(url):
            return b""
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        try:
            async with self.session.get(url, allow_redirects=False) as response:
                if response.status != 200 or not response.headers.get(
                    "Content-Type", ""
                ).startswith("image/"):
                    return b""
                if int(response.headers.get("Content-Length", "0")) > MAX_INPUT:
                    return b""
                data = bytearray()
                async for block in response.content.iter_chunked(64 * 1024):
                    data.extend(block)
                    if len(data) > MAX_INPUT:
                        return b""
                return bytes(data)
        except (aiohttp.ClientError, TimeoutError, ValueError):
            return b""

    async def process(self, row):
        async with self.semaphore:
            if self.stopped or self.draining:
                return
            message = msgspec.convert(row["document"], type=SavedMessage)
            previews = []
            for media in message.media:
                if self.stopped or self.draining:

                    return
                if media.preview:
                    previews.append(media)
                    continue
                preview = ""
                if not media.spoiler and safe_media_url(media.url):
                    try:
                        data = await self.fetch(media.url)
                        if data:
                            value = await file_io(first_frame, data)
                            preview = base64.b64encode(value).decode("ascii")
                    except (ValueError, OSError):
                        pass
                previews.append(msgspec.structs.replace(media, preview=preview))
            await self.store.media_ready(
                row, msgspec.structs.replace(message, media=tuple(previews))
            )

    async def tick(self):
        if self.stopped or self.draining:
            return
        await asyncio.gather(*(self.process(row) for row in await self.store.pending_media()))

    async def close(self):
        self.stopped = True
        if self.session:
            await self.session.close()
