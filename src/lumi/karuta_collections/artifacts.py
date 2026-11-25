from __future__ import annotations

import asyncio
import hashlib
import io
import os
import tempfile
import warnings
from pathlib import Path

from PIL import Image

from .models import Blob

MAX_BYTES = 10 * 1024 * 1024
MAX_PIXELS = 16_000_000
FORMATS = {
    "JPEG": ("jpg", "image/jpeg"),
    "PNG": ("png", "image/png"),
    "WEBP": ("webp", "image/webp"),
}


def inspect_image(data: bytes) -> Blob:
    if not data or len(data) > MAX_BYTES:
        raise ValueError("invalid image size")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as image:
                if image.format not in FORMATS or getattr(image, "n_frames", 1) != 1:
                    raise ValueError("unsupported image")
                if image.width * image.height > MAX_PIXELS or max(image.size) > 8192:
                    raise ValueError("image dimensions exceeded")
                image.load()
                ext, mime = FORMATS[image.format]
                return Blob(hashlib.sha256(data).hexdigest(), ext, mime, len(data), *image.size)
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        raise ValueError("image dimensions exceeded") from exc


class ArtifactStore:
    def __init__(self, root: Path):
        self.root = root.resolve()

    def path(self, blob: Blob) -> Path:
        if (
            len(blob.sha256) != 64
            or any(c not in "0123456789abcdef" for c in blob.sha256)
            or blob.extension not in {v[0] for v in FORMATS.values()}
        ):
            raise ValueError("invalid image reference")
        path = self.root / blob.sha256[:2] / (blob.sha256 + "." + blob.extension)
        if self.root not in path.resolve().parents or path.is_symlink():
            raise ValueError("unsafe image reference")
        return path

    def _store(self, data: bytes) -> Blob:
        blob = inspect_image(data)
        path = self.path(blob)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if hashlib.sha256(path.read_bytes()).hexdigest() != blob.sha256:
                raise ValueError("corrupt stored image")
            return blob
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=path.parent, prefix=".incoming-", delete=False
            ) as stream:
                temporary = stream.name
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)
        return blob

    async def store(self, data: bytes) -> Blob:
        return await asyncio.to_thread(self._store, data)

    async def read(self, blob: Blob) -> bytes:
        def read():
            path = self.path(blob)
            if path.stat().st_size != blob.size or blob.size > MAX_BYTES:
                raise ValueError("stored image size changed")
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != blob.sha256:
                raise ValueError("stored image hash changed")
            return data

        return await asyncio.to_thread(read)
