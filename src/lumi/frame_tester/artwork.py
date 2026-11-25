from __future__ import annotations

import asyncio
import hashlib
import io
import warnings
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from PIL import Image

MAX_DOWNLOAD_BYTES = 10 * 1024 * 1024
MAX_IMAGE_PIXELS = 16_000_000
MAX_DIMENSION = 8_192
ALLOWED_FORMATS = frozenset({"GIF", "JPEG", "PNG", "WEBP"})
ALLOWED_CONTENT_TYPES = frozenset(
    {"image/gif", "image/jpeg", "image/png", "image/webp"}
)
ALLOWED_HOSTS = frozenset(
    {
        "cdn.discordapp.com",
        "media.discordapp.net",
        "images-ext-1.discordapp.net",
        "images-ext-2.discordapp.net",
        "d2l56h9h5tj8ue.cloudfront.net",
    }
)


@dataclass(frozen=True, slots=True)
class Artwork:
    data: bytes
    sha256: str
    image: Image.Image


class ArtworkLoader:
    def __init__(self, *, cache_size: int = 24) -> None:
        self._cache_size = cache_size
        self._url_cache: OrderedDict[str, Artwork] = OrderedDict()
        self._hash_cache: OrderedDict[str, Image.Image] = OrderedDict()
        self._inflight: dict[str, asyncio.Task[Artwork]] = {}
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    async def fetch(self, url: str) -> Artwork:
        url = normalize_artwork_url(url)
        validate_artwork_url(url)
        async with self._lock:
            cached = self._url_cache.get(url)
            if cached is not None:
                self._url_cache.move_to_end(url)
                return Artwork(cached.data, cached.sha256, cached.image.copy())
            task = self._inflight.get(url)
            if task is None:
                task = asyncio.create_task(self._download(url))
                self._inflight[url] = task
        try:
            artwork = await task
        finally:
            async with self._lock:
                if self._inflight.get(url) is task:
                    self._inflight.pop(url, None)
        async with self._lock:
            self._url_cache[url] = artwork
            self._url_cache.move_to_end(url)
            while len(self._url_cache) > self._cache_size:
                self._url_cache.popitem(last=False)
        return Artwork(artwork.data, artwork.sha256, artwork.image.copy())

    async def decode(self, data: bytes) -> Artwork:
        digest = hashlib.sha256(data).hexdigest()
        async with self._lock:
            cached = self._hash_cache.get(digest)
            if cached is not None:
                self._hash_cache.move_to_end(digest)
                return Artwork(data, digest, cached.copy())
        image = await asyncio.to_thread(decode_artwork, data)
        async with self._lock:
            self._hash_cache[digest] = image
            self._hash_cache.move_to_end(digest)
            while len(self._hash_cache) > self._cache_size:
                self._hash_cache.popitem(last=False)
        return Artwork(data, digest, image.copy())

    async def _download(self, url: str) -> Artwork:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=20, connect=5, sock_read=10)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=aiohttp.TCPConnector(limit=8, limit_per_host=4),
                headers={"User-Agent": "Lumi Frame Tester/1.0"},
            )
        async with self._session.get(url, allow_redirects=False) as response:
            if response.status != 200:
                raise ValueError("artwork download failed")
            if response.headers.get("Location"):
                raise ValueError("artwork redirects are not allowed")
            content_type = response.headers.get("Content-Type", "").partition(";")[0].lower()
            if content_type not in ALLOWED_CONTENT_TYPES:
                raise ValueError("artwork response is not a supported image")
            declared = int(response.headers.get("Content-Length") or 0)
            if declared > MAX_DOWNLOAD_BYTES:
                raise ValueError("artwork is too large")
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.content.iter_chunked(64 * 1024):
                size += len(chunk)
                if size > MAX_DOWNLOAD_BYTES:
                    raise ValueError("artwork is too large")
                chunks.append(chunk)
        data = b"".join(chunks)
        if not data:
            raise ValueError("artwork is empty")
        return await self.decode(data)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None


def validate_artwork_url(value: str) -> None:
    if not value or len(value) > 2_048:
        raise ValueError("invalid artwork URL")
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise ValueError("invalid artwork URL") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme != "https"
        or host not in ALLOWED_HOSTS
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or not parsed.path.startswith("/")
    ):
        raise ValueError("artwork URL is not allowed")


def normalize_artwork_url(value: str) -> str:

    if not value or len(value) > 2_048:
        return value
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").casefold()
        port = parsed.port
    except ValueError:
        return value
    if (
        parsed.scheme == "http"
        and host in ALLOWED_HOSTS
        and port is None
        and parsed.username is None
        and parsed.password is None
    ):
        return urlunsplit(("https", parsed.netloc, parsed.path, parsed.query, parsed.fragment))
    return value


def decode_artwork(data: bytes) -> Image.Image:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as source:
                if source.format not in ALLOWED_FORMATS:
                    raise ValueError("unsupported artwork format")
                width, height = source.size
                if (
                    width <= 0
                    or height <= 0
                    or width > MAX_DIMENSION
                    or height > MAX_DIMENSION
                    or width * height > MAX_IMAGE_PIXELS
                ):
                    raise ValueError("invalid artwork dimensions")
                source.seek(0)
                source.load()
                return source.convert("RGBA")
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        raise ValueError("artwork dimensions exceed the limit") from exc
    except (OSError, SyntaxError) as exc:
        raise ValueError("artwork could not be decoded") from exc
