from __future__ import annotations

import asyncio
import hashlib
import io
from collections import OrderedDict

from PIL import Image, ImageDraw, ImageOps

from .artifacts import inspect_image

ARTWORK_SIZE = (520, 720)
TILE = (274, round(274 * ARTWORK_SIZE[1] / ARTWORK_SIZE[0]))
GAP = 0
PADDING = 0
SIZE = (TILE[0] * 5, TILE[1] * 2)
RENDERER_VERSION = "kc-collage-3-artwork-proportions"


def render_collage(images: tuple[bytes | None, ...]) -> bytes:
    if not 1 <= len(images) <= 10:
        raise ValueError("invalid collage page")
    canvas = Image.new("RGBA", SIZE)
    draw = ImageDraw.Draw(canvas)
    for index, data in enumerate(images):
        left = PADDING + (index % 5) * (TILE[0] + GAP)
        top = PADDING + (index // 5) * (TILE[1] + GAP)
        if data is None:
            draw.rectangle(
                (left, top, left + TILE[0] - 1, top + TILE[1] - 1), fill=(42, 44, 50, 255)
            )
            cx, cy = left + TILE[0] // 2, top + TILE[1] // 2
            draw.rectangle(
                (cx - 21, cy - 17, cx + 21, cy + 17), outline=(117, 121, 130, 255), width=3
            )
            draw.line((cx - 23, cy + 24, cx + 23, cy - 24), fill=(117, 121, 130, 255), width=3)
            continue
        inspect_image(data)
        with Image.open(io.BytesIO(data)) as source:
            source = ImageOps.exif_transpose(source).convert("RGBA")
            source = ImageOps.contain(
                source.convert("RGBa"), TILE, Image.Resampling.LANCZOS
            ).convert("RGBA")
            canvas.alpha_composite(
                source, (left + (TILE[0] - source.width) // 2, top + (TILE[1] - source.height) // 2)
            )
    output = io.BytesIO()
    canvas.save(output, "WEBP", lossless=True, method=4, exact=True)
    return output.getvalue()


class CollageRenderer:
    def __init__(self):
        self.cache: OrderedDict[str, bytes] = OrderedDict()
        self.slots = asyncio.Semaphore(1)

    async def render(self, images: tuple[bytes | None, ...]) -> tuple[str, bytes]:
        key = hashlib.sha256(
            (
                RENDERER_VERSION
                + ":"
                + ":".join(
                    hashlib.sha256(data).hexdigest() if data else "missing" for data in images
                )
            ).encode()
        ).hexdigest()
        if key in self.cache:
            self.cache.move_to_end(key)
            return key, self.cache[key]
        async with self.slots:
            data = await asyncio.to_thread(render_collage, images)
        self.cache[key] = data
        while len(self.cache) > 32:
            self.cache.popitem(last=False)
        return key, data
