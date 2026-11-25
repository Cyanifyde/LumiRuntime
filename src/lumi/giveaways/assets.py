from __future__ import annotations

import asyncio
import hashlib
import io
import os
import re
import secrets
import time
import warnings
from pathlib import Path

import aiohttp
from PIL import Image

from lumi.contracts import TemporaryAttachment

from .models import GiveawayError, image_url

MAX_BYTES = 10 * 1024 * 1024
FORMATS = {
    "PNG": (".png", "image/png"),
    "JPEG": (".jpg", "image/jpeg"),
    "WEBP": (".webp", "image/webp"),
    "GIF": (".gif", "image/gif"),
}


def inspect(data):
    if not data or len(data) > MAX_BYTES:
        raise GiveawayError("Card image is too large.")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as image:
                if image.format not in FORMATS:
                    raise GiveawayError("Unsupported card image.")
                width, height = image.size
                frames = getattr(image, "n_frames", 1)
                if (
                    min(width, height) <= 0
                    or max(width, height) > 8192
                    or width * height > 16_000_000
                    or frames > 200
                    or width * height * frames > 100_000_000
                ):
                    raise GiveawayError("Card image is too large.")
                suffix, mime = FORMATS[image.format]
                for index in range(frames):
                    image.seek(index)
                    image.load()
                return dict(
                    sha=hashlib.sha256(data).hexdigest(),
                    size=len(data),
                    suffix=suffix,
                    mime=mime,
                    width=width,
                    height=height,
                )
    except (
        OSError,
        ValueError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ) as exc:
        if isinstance(exc, GiveawayError):
            raise
        raise GiveawayError("Invalid card image.") from exc


class Assets:
    def __init__(self, root: Path, temp_root: Path):
        self.root, self.temp_root = root, temp_root
        self.limit = asyncio.Semaphore(2)

    async def start(self):
        await asyncio.to_thread(self.root.mkdir, parents=True, exist_ok=True, mode=0o700)

    async def ingest(self, url, *, upload=False):
        if upload:


            image_url(url.replace("/ephemeral-attachments/", "/attachments/", 1))
        else:
            image_url(url)
        async with self.limit:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                    async with session.get(url, allow_redirects=False) as response:
                        if response.status != 200:
                            raise GiveawayError(
                                "Card image unavailable. Run the view command again."
                            )
                        if response.content_length and response.content_length > MAX_BYTES:
                            raise GiveawayError("Card image is too large.")
                        data = bytearray()
                        async for chunk in response.content.iter_chunked(65536):
                            data.extend(chunk)
                            if len(data) > MAX_BYTES:
                                raise GiveawayError("Card image is too large.")
                payload = bytes(data)
                metadata = await asyncio.wait_for(asyncio.to_thread(inspect, payload), timeout=2)
                await asyncio.to_thread(self._save, metadata, payload)
                return metadata
            except (aiohttp.ClientError, TimeoutError) as exc:
                raise GiveawayError("Card image unavailable. Try again.") from exc

    def _save(self, image, data):
        target = self.root / image["sha"]
        if target.is_symlink():
            raise GiveawayError("Image storage unavailable.")
        if target.exists():
            if hashlib.sha256(target.read_bytes()).hexdigest() != image["sha"]:
                raise GiveawayError("Image storage unavailable.")
            return
        temporary = self.root / ("." + secrets.token_hex(16))
        try:
            with temporary.open("xb") as output:
                os.chmod(temporary, 0o600)
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    async def attachment(self, image, filename):
        return await asyncio.to_thread(self._attachment, image, filename)

    async def discard(self, attachments):
        for item in attachments:
            if re.fullmatch(r"tmp:[a-f0-9]{32}", item.reference):
                await asyncio.to_thread((self.temp_root / item.reference[4:]).unlink, missing_ok=True)

    def _attachment(self, image, filename):
        if not re.fullmatch("[a-f0-9]{64}", image["sha"]):
            raise GiveawayError("Image unavailable.")
        target = self.root / image["sha"]
        if target.is_symlink():
            raise GiveawayError("Image unavailable.")
        data = target.read_bytes()
        if len(data) != image["size"] or hashlib.sha256(data).hexdigest() != image["sha"]:
            raise GiveawayError("Image unavailable.")
        identifier = secrets.token_hex(16)
        temporary = self.temp_root / identifier
        with temporary.open("xb") as output:
            os.chmod(temporary, 0o640)
            output.write(data)
        return TemporaryAttachment(
            "tmp:" + identifier,
            filename,
            image["sha"],
            len(data),
            int(time.time() * 1000) + 120_000,
        )
