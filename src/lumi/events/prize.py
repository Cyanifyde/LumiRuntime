from __future__ import annotations

import asyncio
import base64
import hashlib
import io

from PIL import Image

from lumi.frame_tester.artwork import ArtworkLoader

MAX_PRIZE_BYTES = 1024 * 1024


def snapshot(data: bytes) -> dict:

    if not data or len(data) > MAX_PRIZE_BYTES:
        raise ValueError("prize image too large")
    with Image.open(io.BytesIO(data)) as image:
        if image.format not in {"PNG", "JPEG", "WEBP"} or getattr(image, "n_frames", 1) != 1:
            raise ValueError("unsupported prize image")
        if image.width * image.height > 16_000_000 or max(image.size) > 8192:
            raise ValueError("prize dimensions too large")
        image.verify()
        suffix = {"PNG": "png", "JPEG": "jpg", "WEBP": "webp"}[image.format]
    digest = hashlib.sha256(data).hexdigest()
    return {
        "data": base64.b64encode(data).decode("ascii"),
        "sha256": digest,
        "size": len(data),
        "file_name": f"main-prize-{digest[:16]}.{suffix}",
    }


class PrizeLoader:


    def __init__(self) -> None:
        self.loader = ArtworkLoader(cache_size=2)

    async def fetch(self, url: str) -> dict:
        artwork = await self.loader.fetch(url)
        return await asyncio.to_thread(snapshot, artwork.data)

    async def close(self) -> None:
        await self.loader.close()
