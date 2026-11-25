from __future__ import annotations

import asyncio
import hashlib
import io
import ipaddress
import warnings
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

import aiohttp
import numpy as np
from PIL import Image, ImageCms, ImageOps, UnidentifiedImageError

MAX_BYTES = 50 * 1024 * 1024
MAX_PIXELS = 100_000_000
MAX_LONG_SIDE = 4096
ALLOWED_HOSTS = frozenset(
    {
        "cdn.discordapp.com",
        "media.discordapp.net",
        "images-ext-1.discordapp.net",
        "images-ext-2.discordapp.net",
    }
)
ALLOWED_CONTENT_TYPES = frozenset({"image/png", "image/jpeg", "image/webp"})
ALLOWED_FORMATS = frozenset({"PNG", "JPEG", "WEBP"})


class UnsupportedImageError(ValueError):
    pass


class ImageTooLargeError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PreparedImage:
    rgba: np.ndarray
    sha256: str
    source_format: str

    @property
    def width(self) -> int:
        return int(self.rgba.shape[1])

    @property
    def height(self) -> int:
        return int(self.rgba.shape[0])


class SketchImageLoader:
    def __init__(self, *, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session
        self._owns_session = session is None

    async def start(self) -> None:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_read=20)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def fetch(self, url: str, *, declared_size: int = 0) -> PreparedImage:
        if declared_size > MAX_BYTES:
            raise ImageTooLargeError
        await self.start()
        assert self._session is not None
        current = url
        for _ in range(4):
            self._validate_url(current)
            async with self._session.get(current, allow_redirects=False) as response:
                if response.status in {301, 302, 303, 307, 308}:
                    location = response.headers.get("Location")
                    if not location:
                        raise UnsupportedImageError
                    current = urljoin(current, location)
                    continue
                if response.status != 200:
                    raise UnsupportedImageError
                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                if content_type not in ALLOWED_CONTENT_TYPES:
                    raise UnsupportedImageError
                length = int(response.headers.get("Content-Length", "0") or 0)
                if length > MAX_BYTES:
                    raise ImageTooLargeError
                data = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    data.extend(chunk)
                    if len(data) > MAX_BYTES:
                        raise ImageTooLargeError
                return await self.decode(bytes(data))
        raise UnsupportedImageError

    async def decode(self, data: bytes) -> PreparedImage:
        if not data:
            raise UnsupportedImageError
        if len(data) > MAX_BYTES:
            raise ImageTooLargeError
        return await asyncio.to_thread(self._decode_sync, data)

    @staticmethod
    def _validate_url(url: str) -> None:
        if len(url) > 4096:
            raise UnsupportedImageError
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in {None, 443}
        ):
            raise UnsupportedImageError
        host = parsed.hostname.rstrip(".").lower()
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise UnsupportedImageError
        if host not in ALLOWED_HOSTS:
            raise UnsupportedImageError

    @staticmethod
    def _decode_sync(data: bytes) -> PreparedImage:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", Image.DecompressionBombWarning)
                with Image.open(io.BytesIO(data)) as opened:
                    if opened.format not in ALLOWED_FORMATS:
                        raise UnsupportedImageError
                    source_format = str(opened.format)
                    if bool(getattr(opened, "is_animated", False)) or int(
                        getattr(opened, "n_frames", 1)
                    ) != 1:
                        raise UnsupportedImageError
                    if opened.width * opened.height > MAX_PIXELS:
                        raise ImageTooLargeError
                    image = ImageOps.exif_transpose(opened)
                    image.load()
                    image = SketchImageLoader._to_srgb(image)
        except Image.DecompressionBombError as exc:
            raise ImageTooLargeError from exc
        except (UnidentifiedImageError, OSError) as exc:
            raise UnsupportedImageError from exc

        rgba = np.asarray(image.convert("RGBA"), dtype=np.uint8)
        if not np.any(rgba[..., 3]):
            raise UnsupportedImageError
        height, width = rgba.shape[:2]
        target_ratio = 274 / 405
        if width / height >= target_ratio:
            out_height = 405
            out_width = max(1, round(width * out_height / height))
        else:
            out_width = 274
            out_height = max(1, round(height * out_width / width))
        if max(out_width, out_height) > MAX_LONG_SIDE:
            raise ImageTooLargeError
        if (out_width, out_height) != (width, height):
            rgba = SketchImageLoader._resize_premultiplied(rgba, out_width, out_height)
        return PreparedImage(
            rgba=np.ascontiguousarray(rgba),
            sha256=hashlib.sha256(data).hexdigest(),
            source_format=source_format,
        )

    @staticmethod
    def _to_srgb(image: Image.Image) -> Image.Image:
        profile = image.info.get("icc_profile")
        if not profile:
            return image.convert("RGBA")
        try:
            source = ImageCms.ImageCmsProfile(io.BytesIO(profile))
            target = ImageCms.createProfile("sRGB")
            alpha = image.getchannel("A") if "A" in image.getbands() else None
            converted = ImageCms.profileToProfile(
                image.convert("RGB"), source, target, outputMode="RGB"
            )
            if alpha is not None:
                converted.putalpha(alpha)
            return converted.convert("RGBA")
        except (OSError, ValueError, ImageCms.PyCMSError):
            return image.convert("RGBA")

    @staticmethod
    def _resize_premultiplied(rgba: np.ndarray, width: int, height: int) -> np.ndarray:
        source = rgba.astype(np.float32) / 255.0
        alpha = source[..., 3:4]
        premultiplied = np.concatenate((source[..., :3] * alpha, alpha), axis=2)
        channels: list[np.ndarray] = []
        for index in range(4):
            plane = Image.fromarray(premultiplied[..., index], mode="F")
            resized = plane.resize((width, height), Image.Resampling.LANCZOS)
            channels.append(np.asarray(resized, dtype=np.float32))
        result = np.stack(channels, axis=2)
        out_alpha = np.clip(result[..., 3:4], 0.0, 1.0)
        rgb = np.zeros_like(result[..., :3])
        np.divide(result[..., :3], out_alpha, out=rgb, where=out_alpha > 1e-6)
        return np.rint(
            np.concatenate((np.clip(rgb, 0.0, 1.0), out_alpha), axis=2) * 255.0
        ).astype(np.uint8)
