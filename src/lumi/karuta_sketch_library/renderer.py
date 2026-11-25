from __future__ import annotations

import asyncio
import hashlib
import io
from collections import OrderedDict

from PIL import Image, ImageDraw, ImageFont

from lumi.karuta_collections.artifacts import ArtifactStore

from .models import IMAGE_SIZE, SketchRecord

PAGE_SIZE = 6
CAPTION_HEIGHT = 50
CELL = (IMAGE_SIZE[0], IMAGE_SIZE[1] + CAPTION_HEIGHT)
GRID_SIZE = (CELL[0] * 3, CELL[1] * 2)
RENDERER_VERSION = "klsketch-grid-2"


def _font(size: int):
    for path in (
        "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
    ):
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            pass
    return ImageFont.load_default(size=size)


def _fit(draw, text: str, font, maximum: int) -> str:
    if draw.textlength(text, font=font) <= maximum:
        return text
    while text and draw.textlength(text + "…", font=font) > maximum:
        text = text[:-1]
    return text + "…"


def render_grid(records: tuple[SketchRecord, ...], images: tuple[bytes, ...]) -> bytes:
    if not 1 <= len(records) <= PAGE_SIZE or len(records) != len(images):
        raise ValueError("invalid sketch grid")
    canvas = Image.new("RGBA", GRID_SIZE)
    draw, name_font, detail_font = ImageDraw.Draw(canvas), _font(16), _font(14)
    for n, (record, data) in enumerate(zip(records, images, strict=True)):
        x, y = (n % 3) * CELL[0], (n // 3) * CELL[1]
        with Image.open(io.BytesIO(data)) as image:
            if image.size != (record.blob.width, record.blob.height) or image.format != "WEBP":
                raise ValueError("sketch image changed")
            artwork = image.convert("RGBA")
            if artwork.size != IMAGE_SIZE:
                artwork.thumbnail(IMAGE_SIZE, Image.Resampling.LANCZOS)

            canvas.paste(
                artwork,
                (
                    x + (IMAGE_SIZE[0] - artwork.width) // 2,
                    y + (IMAGE_SIZE[1] - artwork.height) // 2,
                ),
            )
        draw.rectangle(
            (x, y + IMAGE_SIZE[1], x + CELL[0] - 1, y + CELL[1] - 1), fill=(30, 31, 35, 255)
        )
        name = _fit(draw, f"{n + 1}. {record.character}", name_font, CELL[0] - 12)
        draw.text((x + 6, y + IMAGE_SIZE[1] + 5), name, font=name_font, fill=(240, 240, 244))
        detail = f"{record.code} · {record.ink:,} ink"
        draw.text((x + 6, y + IMAGE_SIZE[1] + 28), detail, font=detail_font, fill=(174, 177, 185))
    target = io.BytesIO()
    canvas.save(target, "WEBP", lossless=True, exact=True, method=3)
    return target.getvalue()


class SketchRenderer:
    def __init__(self, artifacts: ArtifactStore, maximum: int = 24):
        self.artifacts, self.maximum = artifacts, maximum
        self.cache: OrderedDict[str, bytes] = OrderedDict()
        self.lock = asyncio.Lock()

    async def grid(self, snapshot: str, records: tuple[SketchRecord, ...]) -> bytes:
        key = hashlib.sha256(
            (
                RENDERER_VERSION
                + snapshot
                + repr([(r.code, r.character, r.ink, r.blob.sha256) for r in records])
            ).encode()
        ).hexdigest()
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            images = tuple(await asyncio.gather(*(self.artifacts.read(r.blob) for r in records)))
            data = await asyncio.to_thread(render_grid, records, images)
            self.cache[key] = data
            while len(self.cache) > self.maximum:
                self.cache.popitem(last=False)
            return data
