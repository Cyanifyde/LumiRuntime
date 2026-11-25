from __future__ import annotations

import asyncio
import html
import io
import json
import logging
import math
import re
import unicodedata
from collections.abc import Callable, Iterable
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

import aiohttp
import msgspec
from PIL import Image, ImageDraw, ImageFont, ImageOps, UnidentifiedImageError
from redis.asyncio import Redis

from lumi.shared.downloads import read_limited_response as read_response_bytes

LOGGER = logging.getLogger(__name__)

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_WATCH_URL = "https://www.youtube.com/watch?v={video_id}"
PACIFIC = ZoneInfo("America/Los_Angeles")

MAX_QUERY_CHARS = 200
MAX_RESULTS = 6
DAILY_SEARCH_LIMIT = 99
MAX_WAITERS = 10
MAX_API_RESPONSE_BYTES = 1024 * 1024
MAX_THUMBNAIL_BYTES = 5 * 1024 * 1024
MAX_GRID_BYTES = 2 * 1024 * 1024
MAX_DECODED_PIXELS = 16_000_000
QUOTA_TTL_SECONDS = 8 * 24 * 60 * 60

_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_ALLOWED_THUMBNAIL_TYPES = frozenset({"image/jpeg", "image/png", "image/webp"})
_ALLOWED_THUMBNAIL_HOSTS = frozenset(
    {"i.ytimg.com", "img.youtube.com", "yt3.ggpht.com"}
)
_RESERVE_QUOTA_SCRIPT = """
local exhausted = redis.call('HGET', KEYS[1], 'exhausted')
if exhausted == '1' then
  return 0
end
local attempts = tonumber(redis.call('HGET', KEYS[1], 'attempts') or '0')
local limit = tonumber(ARGV[1])
if attempts >= limit then
  return 0
end
redis.call('HINCRBY', KEYS[1], 'attempts', 1)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""


class YouTubeError(RuntimeError):
    pass


class YouTubeUnavailableError(YouTubeError):
    pass


class YouTubeBusyError(YouTubeError):
    pass


class YouTubeQuotaError(YouTubeError):
    pass


class YouTubeNoResultsError(YouTubeError):
    pass


class YouTubeResponseError(YouTubeError):
    pass


class YouTubeResult(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    video_id: str
    title: str
    channel_name: str
    thumbnail_urls: tuple[str, ...] = ()

    @property
    def watch_url(self) -> str:
        return YOUTUBE_WATCH_URL.format(video_id=self.video_id)


class YouTubeSearchBundle(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    results: tuple[YouTubeResult, ...]
    grid: bytes


def normalize_query(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    normalized = "".join(
        character
        for character in normalized
        if character.isspace()
        or unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )
    return " ".join(normalized.split())


def quota_day(now: datetime | None = None) -> date:
    current = now or datetime.now(tz=PACIFIC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=PACIFIC)
    return current.astimezone(PACIFIC).date()


def _clean_api_text(value: object, *, fallback: str, limit: int) -> str:
    text = html.unescape(str(value or ""))
    text = unicodedata.normalize("NFKC", text)
    text = "".join(
        character
        for character in text
        if character.isspace()
        or unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )
    text = " ".join(text.split())
    return (text or fallback)[:limit]


def valid_thumbnail_url(value: object) -> str:
    candidate = str(value or "").strip()
    if not candidate or len(candidate) > 2_048:
        return ""
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError:
        return ""
    host = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or port not in (None, 443)
    ):
        return ""
    if host not in _ALLOWED_THUMBNAIL_HOSTS and not host.endswith(".ytimg.com"):
        return ""
    return candidate


def parse_search_response(payload: object) -> tuple[YouTubeResult, ...]:
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise YouTubeResponseError()

    results: list[YouTubeResult] = []
    seen: set[str] = set()
    for item in payload["items"]:
        if not isinstance(item, dict):
            continue
        identity, snippet = item.get("id"), item.get("snippet")
        if not isinstance(identity, dict) or not isinstance(snippet, dict):
            continue
        video_id = str(identity.get("videoId") or "")
        if not _VIDEO_ID_RE.fullmatch(video_id) or video_id in seen:
            continue

        raw_thumbnails = snippet.get("thumbnails")
        raw_thumbnails = raw_thumbnails if isinstance(raw_thumbnails, dict) else {}
        thumbnail_urls: list[str] = []
        for quality in ("medium", "high", "standard", "maxres", "default"):
            thumbnail = raw_thumbnails.get(quality)
            if not isinstance(thumbnail, dict):
                continue
            url = valid_thumbnail_url(thumbnail.get("url"))
            if url and url not in thumbnail_urls:
                thumbnail_urls.append(url)

        results.append(
            YouTubeResult(
                video_id=video_id,
                title=_clean_api_text(
                    snippet.get("title"), fallback="Untitled video", limit=500
                ),
                channel_name=_clean_api_text(
                    snippet.get("channelTitle"), fallback="Unknown channel", limit=200
                ),
                thumbnail_urls=tuple(thumbnail_urls),
            )
        )
        seen.add(video_id)
        if len(results) == MAX_RESULTS:
            break
    if not results:
        raise YouTubeNoResultsError()
    return tuple(results)


async def read_limited_response(
    response: aiohttp.ClientResponse,
    *,
    limit: int,
) -> bytes:
    try:
        return await read_response_bytes(response, limit=limit)
    except ValueError as exc:
        raise YouTubeResponseError() from exc


def _font_candidates() -> tuple[str, ...]:
    return (
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/msgothic.ttc",
    )


def _load_font(size: int) -> ImageFont.ImageFont:
    for candidate in _font_candidates():
        try:
            return ImageFont.truetype(candidate, size=size)
        except (OSError, ValueError):
            continue
    return ImageFont.load_default()


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> float:
    bounds = draw.textbbox((0, 0), text, font=font)
    return float(bounds[2] - bounds[0])


def _split_long_token(
    draw: ImageDraw.ImageDraw,
    token: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    chunks: list[str] = []
    current = ""
    for character in token:
        candidate = current + character
        if current and _text_width(draw, candidate, font) > max_width:
            chunks.append(current)
            current = character
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks or [token]


def _wrap_title(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    words: list[str] = []
    for token in text.split(" "):
        if _text_width(draw, token, font) <= max_width:
            words.append(token)
        else:
            words.extend(_split_long_token(draw, token, font, max_width))

    lines: list[str] = []
    current = ""
    consumed = 0
    for index, word in enumerate(words):
        candidate = word if not current else f"{current} {word}"
        if not current or _text_width(draw, candidate, font) <= max_width:
            current = candidate
            consumed = index + 1
            continue
        lines.append(current)
        current = word
        consumed = index
        if len(lines) == 2:
            break
    if len(lines) < 2 and current:
        lines.append(current)
        consumed = max(consumed, len(words))
    lines = lines[:2] or ["Untitled video"]
    if consumed < len(words):
        last = lines[-1].rstrip()
        while last and _text_width(draw, last + "…", font) > max_width:
            last = last[:-1].rstrip()
        lines[-1] = f"{last}…" if last else "…"
    return lines


def decode_thumbnail(data: bytes | None) -> Image.Image | None:
    if not data:
        return None
    try:
        with Image.open(io.BytesIO(data)) as source:
            if (
                source.width < 1
                or source.height < 1
                or source.width * source.height > MAX_DECODED_PIXELS
            ):
                return None
            source.verify()
        with Image.open(io.BytesIO(data)) as source:
            return ImageOps.exif_transpose(source).convert("RGB")
    except (UnidentifiedImageError, OSError, ValueError):
        return None


def render_grid(
    results: Iterable[YouTubeResult],
    thumbnails: Iterable[bytes | None],
) -> bytes:
    result_list = list(results)[:MAX_RESULTS]
    thumbnail_list = list(thumbnails)[: len(result_list)]
    if not result_list:
        raise ValueError("at least one result is required")
    thumbnail_list.extend([None] * (len(result_list) - len(thumbnail_list)))

    width = 1200
    outer = 28
    gap = 22
    columns = 2
    rows = math.ceil(len(result_list) / columns)
    tile_width = (width - 2 * outer - gap) // columns
    image_height = round(tile_width * 9 / 16)
    caption_height = 100
    tile_height = image_height + caption_height
    height = 2 * outer + rows * tile_height + max(0, rows - 1) * gap

    canvas = Image.new("RGB", (width, height), "#0f1115")
    draw = ImageDraw.Draw(canvas)
    title_font = _load_font(28)
    number_font = _load_font(30)

    for index, (result, raw_thumbnail) in enumerate(
        zip(result_list, thumbnail_list, strict=True)
    ):
        row, column = divmod(index, columns)
        left = outer + column * (tile_width + gap)
        top = outer + row * (tile_height + gap)
        right = left + tile_width
        image_bottom = top + image_height
        bottom = top + tile_height

        draw.rounded_rectangle((left, top, right, bottom), radius=14, fill="#1b1e25")
        draw.rectangle((left, top, right, image_bottom), fill="#090a0d")

        thumbnail = decode_thumbnail(raw_thumbnail)
        if thumbnail is not None:
            fitted = ImageOps.contain(
                thumbnail,
                (tile_width, image_height),
                method=Image.Resampling.LANCZOS,
            )
            canvas.paste(
                fitted,
                (
                    left + (tile_width - fitted.width) // 2,
                    top + (image_height - fitted.height) // 2,
                ),
            )
        else:
            placeholder_font = _load_font(68)
            placeholder = str(index + 1)
            bounds = draw.textbbox((0, 0), placeholder, font=placeholder_font)
            draw.text(
                (
                    left + (tile_width - (bounds[2] - bounds[0])) / 2,
                    top
                    + (image_height - (bounds[3] - bounds[1])) / 2
                    - bounds[1],
                ),
                placeholder,
                font=placeholder_font,
                fill="#737986",
            )

        badge_size = 52
        badge_left = left + 14
        badge_top = top + 14
        draw.rounded_rectangle(
            (badge_left, badge_top, badge_left + badge_size, badge_top + badge_size),
            radius=13,
            fill="#ff0033",
        )
        number = str(index + 1)
        bounds = draw.textbbox((0, 0), number, font=number_font)
        draw.text(
            (
                badge_left + (badge_size - (bounds[2] - bounds[0])) / 2,
                badge_top + (badge_size - (bounds[3] - bounds[1])) / 2 - bounds[1],
            ),
            number,
            font=number_font,
            fill="white",
        )

        for line_index, line in enumerate(
            _wrap_title(draw, result.title, title_font, tile_width - 36)
        ):
            draw.text(
                (left + 18, image_bottom + 12 + line_index * 36),
                line,
                font=title_font,
                fill="#f2f3f5",
            )

    encoded = io.BytesIO()
    for quality in (88, 82, 76):
        encoded.seek(0)
        encoded.truncate(0)
        canvas.save(
            encoded,
            format="JPEG",
            quality=quality,
            optimize=True,
            progressive=True,
        )
        if encoded.tell() <= MAX_GRID_BYTES:
            return encoded.getvalue()
    raise YouTubeResponseError()


class RedisYouTubeQuota:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @staticmethod
    def _key(day: date) -> str:
        return f"lumi:youtube:quota:{day.isoformat()}"

    async def reserve_search(self, day: date, *, limit: int = DAILY_SEARCH_LIMIT) -> bool:
        result = await self._redis.eval(
            _RESERVE_QUOTA_SCRIPT,
            1,
            self._key(day),
            int(limit),
            QUOTA_TTL_SECONDS,
        )
        return bool(int(result))

    async def mark_provider_exhausted(self, day: date) -> None:
        key = self._key(day)
        pipeline = self._redis.pipeline(transaction=True)
        pipeline.hset(key, mapping={"exhausted": 1})
        pipeline.expire(key, QUOTA_TTL_SECONDS)
        await pipeline.execute()


class YouTubeSearchService:
    def __init__(
        self,
        *,
        quota: RedisYouTubeQuota,
        api_key: str = "",
        api_key_file: Path = Path("/run/secrets/youtube_api_key"),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._quota = quota
        self._api_key = api_key.strip()
        self._api_key_file = api_key_file
        self._clock = clock or (lambda: datetime.now(tz=PACIFIC))
        self._session: aiohttp.ClientSession | None = None
        self._search_gate = asyncio.Semaphore(1)
        self._queue_lock = asyncio.Lock()
        self._waiting = 0
        self._started = False

    @property
    def ready(self) -> bool:
        return self._started and bool(self._api_key)

    async def start(self) -> None:
        if not self._api_key:
            self._api_key = await asyncio.to_thread(self._read_key)
        self._started = True
        if self._api_key:
            await self._ensure_session()

    def _read_key(self) -> str:
        try:
            value = self._api_key_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            return ""
        if not value or len(value) > 512 or any(character.isspace() for character in value):
            return ""
        return value

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15, connect=5, sock_read=10),
                connector=aiohttp.TCPConnector(limit=12, ttl_dns_cache=300),
            )
        return self._session

    async def close(self) -> None:
        session, self._session = self._session, None
        self._started = False
        if session is not None and not session.closed:
            await session.close()

    async def search_and_render(self, query: str) -> YouTubeSearchBundle:
        normalized = normalize_query(query)
        if not normalized or len(normalized) > MAX_QUERY_CHARS:
            raise ValueError("invalid query")
        if not self.ready:
            raise YouTubeUnavailableError()

        queued = False
        async with self._queue_lock:
            if self._search_gate.locked():
                if self._waiting >= MAX_WAITERS:
                    raise YouTubeBusyError()
                self._waiting += 1
                queued = True
        try:
            await self._search_gate.acquire()
        except BaseException:
            if queued:
                async with self._queue_lock:
                    self._waiting = max(0, self._waiting - 1)
            raise
        try:
            if queued:
                async with self._queue_lock:
                    self._waiting = max(0, self._waiting - 1)
            day = quota_day(self._clock())
            if not await self._quota.reserve_search(day):
                raise YouTubeQuotaError()
            results = await self._search_api(normalized, day)
            return YouTubeSearchBundle(results, await self.render_results(results))
        finally:
            self._search_gate.release()

    async def render_results(self, results: tuple[YouTubeResult, ...]) -> bytes:
        thumbnails = await self._download_thumbnails(results)
        return await asyncio.to_thread(render_grid, results, thumbnails)

    async def _search_api(
        self,
        query: str,
        day: date,
    ) -> tuple[YouTubeResult, ...]:
        session = await self._ensure_session()
        params = {
            "part": "snippet",
            "type": "video",
            "maxResults": str(MAX_RESULTS),
            "order": "relevance",
            "safeSearch": "moderate",
            "q": query,
            "key": self._api_key,
        }
        try:
            async with session.get(
                YOUTUBE_SEARCH_URL,
                params=params,
                allow_redirects=False,
            ) as response:
                body = await read_limited_response(response, limit=MAX_API_RESPONSE_BYTES)
                try:
                    payload = json.loads(body.decode("utf-8"))
                except (UnicodeError, ValueError):
                    raise YouTubeResponseError() from None
                if response.status == 403 and is_quota_response(payload):
                    await self._quota.mark_provider_exhausted(day)
                    raise YouTubeQuotaError()
                if response.status != 200:
                    raise YouTubeResponseError()
        except YouTubeError:
            raise
        except (aiohttp.ClientError, TimeoutError):
            raise YouTubeUnavailableError() from None
        return parse_search_response(payload)

    async def _download_thumbnails(
        self,
        results: tuple[YouTubeResult, ...],
    ) -> tuple[bytes | None, ...]:
        semaphore = asyncio.Semaphore(3)

        async def fetch(result: YouTubeResult) -> bytes | None:
            async with semaphore:
                for url in result.thumbnail_urls:
                    try:
                        return await self._download_thumbnail(url)
                    except (aiohttp.ClientError, TimeoutError, YouTubeResponseError):
                        continue
            return None

        downloaded = tuple(await asyncio.gather(*(fetch(result) for result in results)))
        missing = sum(item is None for item in downloaded)
        if missing:
            LOGGER.warning(
                "youtube_thumbnail_retrieval_incomplete missing=%d results=%d",
                missing,
                len(results),
            )
        return downloaded

    async def _download_thumbnail(self, url: str) -> bytes:
        session = await self._ensure_session()
        current = valid_thumbnail_url(url)
        if not current:
            raise YouTubeResponseError()
        redirects = 0
        while True:
            async with session.get(current, allow_redirects=False) as response:
                if response.status in {301, 302, 303, 307, 308}:
                    if redirects >= 3:
                        raise YouTubeResponseError()
                    redirected = valid_thumbnail_url(
                        urljoin(current, str(response.headers.get("Location") or ""))
                    )
                    if not redirected:
                        raise YouTubeResponseError()
                    current = redirected
                    redirects += 1
                    continue
                if response.status != 200:
                    raise YouTubeResponseError()
                content_type = str(response.headers.get("Content-Type") or "")
                content_type = content_type.partition(";")[0].strip().lower()
                if content_type not in _ALLOWED_THUMBNAIL_TYPES:
                    raise YouTubeResponseError()
                body = await read_limited_response(response, limit=MAX_THUMBNAIL_BYTES)
                if not body:
                    raise YouTubeResponseError()
                return body


def is_quota_response(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    error = payload.get("error")
    if not isinstance(error, dict):
        return False
    if str(error.get("status") or "").upper() in {
        "RESOURCE_EXHAUSTED",
        "QUOTA_EXCEEDED",
    }:
        return True
    errors = error.get("errors")
    if not isinstance(errors, list):
        return False
    reasons = {"quotaexceeded", "dailylimitexceeded", "ratelimitexceeded"}
    return any(
        isinstance(item, dict)
        and str(item.get("reason") or "").replace("_", "").casefold() in reasons
        for item in errors
    )
