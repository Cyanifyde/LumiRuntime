from __future__ import annotations

import io
from functools import lru_cache
from zipfile import ZIP_STORED, ZipFile

import numpy as np
from PIL import Image

from .engine import SketchResult
from .image import ImageTooLargeError
from .palette import PALETTE, palette_rgb

MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024


@lru_cache(maxsize=64)
def colour_swatch(colour: int) -> bytes:
    if not 0 <= colour < len(PALETTE):
        raise ValueError("invalid palette colour")
    output = io.BytesIO()
    Image.new("RGB", (32, 32), PALETTE[colour][1]).save(output, "WEBP", lossless=True, method=6)
    return output.getvalue()


class _BoundedBuffer(io.BytesIO):
    def write(self, data: bytes) -> int:
        if self.tell() + len(data) > MAX_DOWNLOAD_BYTES:
            raise ImageTooLargeError
        return super().write(data)


def build_image_download(result: SketchResult) -> bytes:

    output = _BoundedBuffer()
    rgb = palette_rgb()
    visible = result.alpha > 0
    rgba = np.zeros((*result.labels.shape, 4), dtype=np.uint8)
    rgba[visible, :3] = rgb[result.labels[visible]]
    rgba[..., 3] = result.alpha

    with ZipFile(output, "w", compression=ZIP_STORED) as archive:
        _write_png(archive, "composite.png", rgba)
        for position, colour in enumerate(result.used_labels, 1):
            rgba.fill(0)
            selected = (result.labels == colour) & visible
            rgba[selected, :3] = rgb[colour]
            rgba[selected, 3] = result.alpha[selected]
            name, code = PALETTE[colour]
            slug = name.lower().replace(" ", "-")
            _write_png(archive, f"layers/{position:02d}-{slug}-{code[1:]}.png", rgba)
    return output.getvalue()


def _write_png(archive: ZipFile, name: str, rgba: np.ndarray) -> None:

    with archive.open(name, "w") as target:
        Image.fromarray(rgba).save(target, "PNG", compress_level=6)
