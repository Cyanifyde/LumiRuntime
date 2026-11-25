from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import re
import threading
import time
import unicodedata
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import aiohttp
import emoji as emoji_lib
from PIL import Image, ImageDraw, ImageFont, ImageOps

from .conversations import MAX_CONVERSATION_MESSAGES

try:
    from fontTools import unicodedata as fonttools_unicode
except ImportError:
    fonttools_unicode = None

logger = logging.getLogger("discbot.afk_renderer")

RENDERER_VERSION = "afkn-render-v14-gif-preview-text"
CANVAS_WIDTH = 1000
CANVAS_PADDING = 12
REPLY_INDENT = 56
REPLY_SPACING = 6
CONNECTOR_THICKNESS = 3.5
CONNECTOR_RADIUS = 18
MAX_MESSAGE_LINES = 30
MAX_ASSET_BYTES = 2 * 1024 * 1024
TWEMOJI_VERSION = "17.0.3"

_CUSTOM_EMOJI_RE = re.compile(r"<(?P<animated>a?):(?P<name>[A-Za-z0-9_]+):(?P<id>\d+)>")
_DISCORD_ENTITY_RE = re.compile(r"<@!?\d+>|<@&\d+>|<#\d+>|<a?:[A-Za-z0-9_]+:\d+>")
_ALLOWED_ASSET_HOSTS = {
    "cdn.discordapp.com",
    "media.discordapp.net",
    "cdn.jsdelivr.net",
}


@dataclass(frozen=True)
class AFKRenderMedia:
    url: str
    preview: bytes | None = None
    original_url: str = ""


@dataclass(frozen=True)
class AFKRenderReply:
    message_id: int
    author_name: str
    avatar_url: Optional[str]
    content: str
    occurred_at: datetime
    entity_labels: tuple[tuple[str, str], ...] = ()
    media: tuple[AFKRenderMedia, ...] = ()


@dataclass(frozen=True)
class AFKRenderRequest:
    message_id: int
    channel_name: str
    author_name: str
    avatar_url: Optional[str]
    content: str
    occurred_at: datetime
    timezone_name: str
    entity_labels: tuple[tuple[str, str], ...] = ()
    replies: tuple[AFKRenderReply, ...] = ()
    media: tuple[AFKRenderMedia, ...] = ()


class _ByteLRU:
    def __init__(self, *, max_bytes: int, ttl_seconds: float) -> None:
        self.max_bytes = max(1, int(max_bytes))
        self.ttl_seconds = max(1.0, float(ttl_seconds))
        self._items: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
        self._bytes = 0

    def get(self, key: str) -> Optional[bytes]:
        item = self._items.get(key)
        if item is None:
            return None
        expires_at, value = item
        if expires_at <= time.monotonic():
            self._items.pop(key, None)
            self._bytes -= len(value)
            return None
        self._items.move_to_end(key)
        return value

    def put(self, key: str, value: bytes) -> None:
        previous = self._items.pop(key, None)
        if previous is not None:
            self._bytes -= len(previous[1])
        self._items[key] = (time.monotonic() + self.ttl_seconds, value)
        self._bytes += len(value)
        while self._bytes > self.max_bytes and self._items:
            _old_key, (_expiry, old_value) = self._items.popitem(last=False)
            self._bytes -= len(old_value)

    def clear(self) -> None:
        self._items.clear()
        self._bytes = 0


def _first_font(candidates: Iterable[str]) -> Optional[str]:
    for candidate in candidates:
        if Path(candidate).is_file():
            return candidate
    return None


_FONT_PATHS = {
    "regular": _first_font(
        (
            "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
            "C:/Windows/Fonts/segoeui.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        )
    ),
    "bold": _first_font(
        (
            "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
            "C:/Windows/Fonts/seguisb.ttf",
            "C:/Windows/Fonts/segoeuib.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        )
    ),
    "cjk": _first_font(
        (
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "C:/Windows/Fonts/NotoSansJP-VF.ttf",
            "C:/Windows/Fonts/meiryo.ttc",
        )
    ),
    "symbols": _first_font(
        (
            "/usr/share/fonts/truetype/noto/NotoSansSymbols2-Regular.ttf",
            "/usr/share/fonts/truetype/noto/NotoSansSymbols-Regular.ttf",
            "C:/Windows/Fonts/seguisym.ttf",
        )
    ),
    "math": _first_font(
        (
            "/usr/share/fonts/truetype/noto/NotoSansMath-Regular.ttf",
            "C:/Windows/Fonts/cambria.ttc",
        )
    ),
    "egyptian": _first_font(
        ("/usr/share/fonts/truetype/noto/NotoSansEgyptianHieroglyphs-Regular.ttf",)
    ),
    "arabic": _first_font(("/usr/share/fonts/truetype/noto/NotoSansArabic-Regular.ttf",)),
    "hebrew": _first_font(("/usr/share/fonts/truetype/noto/NotoSansHebrew-Regular.ttf",)),
    "devanagari": _first_font(("/usr/share/fonts/truetype/noto/NotoSansDevanagari-Regular.ttf",)),
    "bengali": _first_font(("/usr/share/fonts/truetype/noto/NotoSansBengali-Regular.ttf",)),
    "gurmukhi": _first_font(("/usr/share/fonts/truetype/noto/NotoSansGurmukhi-Regular.ttf",)),
    "gujarati": _first_font(("/usr/share/fonts/truetype/noto/NotoSansGujarati-Regular.ttf",)),
    "oriya": _first_font(("/usr/share/fonts/truetype/noto/NotoSansOriya-Regular.ttf",)),
    "tamil": _first_font(("/usr/share/fonts/truetype/noto/NotoSansTamil-Regular.ttf",)),
    "telugu": _first_font(("/usr/share/fonts/truetype/noto/NotoSansTelugu-Regular.ttf",)),
    "kannada": _first_font(("/usr/share/fonts/truetype/noto/NotoSansKannada-Regular.ttf",)),
    "malayalam": _first_font(("/usr/share/fonts/truetype/noto/NotoSansMalayalam-Regular.ttf",)),
    "sinhala": _first_font(("/usr/share/fonts/truetype/noto/NotoSansSinhala-Regular.ttf",)),
    "thai": _first_font(("/usr/share/fonts/truetype/noto/NotoSansThai-Regular.ttf",)),
    "lao": _first_font(("/usr/share/fonts/truetype/noto/NotoSansLao-Regular.ttf",)),
    "tibetan": _first_font(
        (
            "/usr/share/fonts/truetype/noto/NotoSansTibetan-Regular.ttf",
            "/usr/share/fonts/truetype/noto/NotoSerifTibetan-Regular.ttf",
        )
    ),
    "myanmar": _first_font(("/usr/share/fonts/truetype/noto/NotoSansMyanmar-Regular.ttf",)),
    "georgian": _first_font(("/usr/share/fonts/truetype/noto/NotoSansGeorgian-Regular.ttf",)),
    "armenian": _first_font(("/usr/share/fonts/truetype/noto/NotoSansArmenian-Regular.ttf",)),
    "ethiopic": _first_font(("/usr/share/fonts/truetype/noto/NotoSansEthiopic-Regular.ttf",)),
    "khmer": _first_font(("/usr/share/fonts/truetype/noto/NotoSansKhmer-Regular.ttf",)),
    "javanese": _first_font(("/usr/share/fonts/truetype/noto/NotoSansJavanese-Regular.ttf",)),
    "balinese": _first_font(("/usr/share/fonts/truetype/noto/NotoSansBalinese-Regular.ttf",)),
    "vai": _first_font(("/usr/share/fonts/truetype/noto/NotoSansVai-Regular.ttf",)),
    "tai_le": _first_font(("/usr/share/fonts/truetype/noto/NotoSansTaiLe-Regular.ttf",)),
    "lepcha": _first_font(("/usr/share/fonts/truetype/noto/NotoSansLepcha-Regular.ttf",)),
    "mongolian": _first_font(("/usr/share/fonts/truetype/noto/NotoSansMongolian-Regular.ttf",)),
    "pahlavi": _first_font(
        ("/usr/share/fonts/truetype/noto/NotoSansInscriptionalPahlavi-Regular.ttf",)
    ),
    "linear_a": _first_font(("/usr/share/fonts/truetype/noto/NotoSansLinearA-Regular.ttf",)),
    "music": _first_font(("/usr/share/fonts/truetype/noto/NotoMusic-Regular.ttf",)),
    "mayan_numerals": _first_font(
        ("/usr/share/fonts/truetype/noto/NotoSansMayanNumerals-Regular.ttf",)
    ),
    "signwriting": _first_font(("/usr/share/fonts/truetype/noto/NotoSansSignWriting-Regular.ttf",)),
    "indic_siyaq": _first_font(
        ("/usr/share/fonts/truetype/noto/NotoSansIndicSiyaqNumbers-Regular.ttf",)
    ),
}
_FONT_CACHE: dict[tuple[str, int], ImageFont.ImageFont] = {}
_FONT_DISCOVERY_LOCK = threading.RLock()
_FONT_FILES: Optional[tuple[str, ...]] = None
_SCRIPT_FONT_CACHE: dict[tuple[str, bool], Optional[str]] = {}

_UNICODE_NAME_FONTS = (
    ("MATHEMATICAL", "math"),
    ("EGYPTIAN HIEROGLYPH", "egyptian"),
    ("ARABIC", "arabic"),
    ("HEBREW", "hebrew"),
    ("DEVANAGARI", "devanagari"),
    ("BENGALI", "bengali"),
    ("GURMUKHI", "gurmukhi"),
    ("GUJARATI", "gujarati"),
    ("ORIYA", "oriya"),
    ("TAMIL", "tamil"),
    ("TELUGU", "telugu"),
    ("KANNADA", "kannada"),
    ("MALAYALAM", "malayalam"),
    ("SINHALA", "sinhala"),
    ("THAI", "thai"),
    ("LAO", "lao"),
    ("TIBETAN", "tibetan"),
    ("MYANMAR", "myanmar"),
    ("GEORGIAN", "georgian"),
    ("ARMENIAN", "armenian"),
    ("ETHIOPIC", "ethiopic"),
    ("KHMER", "khmer"),
    ("JAVANESE", "javanese"),
    ("BALINESE", "balinese"),
    ("VAI", "vai"),
    ("TAI LE", "tai_le"),
    ("LEPCHA", "lepcha"),
    ("MONGOLIAN", "mongolian"),
    ("INSCRIPTIONAL PAHLAVI", "pahlavi"),
    ("LINEAR A", "linear_a"),
    ("MUSICAL SYMBOL", "music"),
    ("MAYAN NUMERAL", "mayan_numerals"),
    ("SIGNWRITING", "signwriting"),
    ("INDIC SIYAQ NUMBER", "indic_siyaq"),
)


def _font(kind: str, size: int) -> ImageFont.ImageFont:
    key = (kind, int(size))
    cached = _FONT_CACHE.get(key)
    if cached is not None:
        return cached
    path = _FONT_PATHS.get(kind) or _FONT_PATHS.get("regular")
    try:
        loaded = (
            ImageFont.truetype(path, int(size)) if path else ImageFont.load_default(size=int(size))
        )
    except (OSError, ValueError):
        loaded = ImageFont.load_default(size=int(size))
    _FONT_CACHE[key] = loaded
    return loaded


def _available_font_files() -> tuple[str, ...]:
    global _FONT_FILES
    if _FONT_FILES is not None:
        return _FONT_FILES
    with _FONT_DISCOVERY_LOCK:
        if _FONT_FILES is not None:
            return _FONT_FILES
        paths = {path for path in _FONT_PATHS.values() if path}
        for root in (Path("/usr/share/fonts"), Path("C:/Windows/Fonts")):
            if not root.is_dir():
                continue
            for path in root.rglob("*"):
                if path.suffix.lower() in {".ttf", ".otf", ".ttc"}:
                    paths.add(str(path))
        _FONT_FILES = tuple(
            sorted(
                paths,
                key=lambda path: (
                    0 if "notosans" in Path(path).name.lower() else 1,
                    1 if "bold" in Path(path).name.lower() else 0,
                    Path(path).name.lower(),
                ),
            )
        )
        return _FONT_FILES


def _script_font_kind(character: str, *, bold: bool) -> Optional[str]:

    if fonttools_unicode is None:
        return None
    try:
        script_name = fonttools_unicode.script_name(fonttools_unicode.script(character))
    except (KeyError, TypeError, ValueError):
        return None
    script_key = re.sub(r"[^a-z0-9]", "", script_name.lower())
    if script_key in {
        "",
        "common",
        "inherited",
        "unknown",
        "latin",
        "greek",
        "cyrillic",
        "han",
        "hiragana",
        "katakana",
        "hangul",
    }:
        return None

    cache_key = (script_key, bool(bold))
    with _FONT_DISCOVERY_LOCK:
        if cache_key in _SCRIPT_FONT_CACHE:
            return _SCRIPT_FONT_CACHE[cache_key]

        script_stem = re.sub(r"[^A-Za-z0-9]", "", script_name.title())
        weights = ("Bold", "Regular") if bold else ("Regular", "Bold")
        direct_candidates: list[str] = []
        for root in (
            Path("/usr/share/fonts/truetype/noto"),
            Path("/usr/share/fonts/opentype/noto"),
            Path("C:/Windows/Fonts"),
        ):
            for family in ("NotoSans", "NotoSerif"):
                for weight in weights:
                    for extension in ("ttf", "otf", "ttc"):
                        candidate = root / f"{family}{script_stem}-{weight}.{extension}"
                        if candidate.is_file():
                            direct_candidates.append(str(candidate))
        if direct_candidates:
            path = direct_candidates[0]
            kind = f"fallback:{hashlib.sha1(path.encode('utf-8')).hexdigest()[:12]}"
            _FONT_PATHS[kind] = path
            _SCRIPT_FONT_CACHE[cache_key] = kind
            return kind

        candidates = [
            path
            for path in _available_font_files()
            if script_key in re.sub(r"[^a-z0-9]", "", Path(path).stem.lower())
        ]
        candidates.sort(
            key=lambda path: (
                0 if ("bold" in Path(path).name.lower()) == bool(bold) else 1,
                0 if "notosans" in Path(path).name.lower() else 1,
                0 if "regular" in Path(path).name.lower() else 1,
                Path(path).name.lower(),
            )
        )
        if candidates:
            path = candidates[0]
            kind = f"fallback:{hashlib.sha1(path.encode('utf-8')).hexdigest()[:12]}"
            _FONT_PATHS[kind] = path
            _SCRIPT_FONT_CACHE[cache_key] = kind
            return kind
        _SCRIPT_FONT_CACHE[cache_key] = None
        return None


def _font_kind(character: str, *, bold: bool = False) -> str:
    codepoint = ord(character)
    if (
        0x2E80 <= codepoint <= 0x9FFF
        or 0xF900 <= codepoint <= 0xFAFF
        or 0x3040 <= codepoint <= 0x30FF
        or 0xAC00 <= codepoint <= 0xD7AF
        or 0xFF00 <= codepoint <= 0xFFEF
    ):
        return "cjk"
    name = unicodedata.name(character, "")
    for prefix, kind in _UNICODE_NAME_FONTS:
        if name.startswith(prefix):
            return kind
    script_kind = _script_font_kind(character, bold=bold)
    if script_kind is not None:
        return script_kind
    if unicodedata.category(character) == "So":
        return "symbols"
    return "bold" if bold else "regular"


def _font_runs(text: str, *, size: int, bold: bool = False):
    if not text:
        return
    current_kind = _font_kind(text[0], bold=bold)
    current: list[str] = []
    for character in text:
        kind = (
            current_kind if unicodedata.combining(character) else _font_kind(character, bold=bold)
        )
        if kind != current_kind and current:
            yield "".join(current), _font(current_kind, size)
            current = []
            current_kind = kind
        current.append(character)
    if current:
        yield "".join(current), _font(current_kind, size)


def _text_width(draw: ImageDraw.ImageDraw, text: str, *, size: int, bold: bool = False) -> float:
    return sum(
        draw.textlength(run, font=font) for run, font in _font_runs(text, size=size, bold=bold)
    )


def _draw_text(
    draw: ImageDraw.ImageDraw,
    xy: tuple[float, float],
    text: str,
    *,
    size: int,
    fill: tuple[int, int, int, int],
    bold: bool = False,
) -> float:
    x, y = xy
    for run, font in _font_runs(text, size=size, bold=bold):
        draw.text((x, y), run, font=font, fill=fill)
        x += draw.textlength(run, font=font)
    return x


def _twemoji_url(value: str) -> str:
    codepoints = "-".join(f"{ord(character):x}" for character in value if ord(character) != 0xFE0F)
    return (
        "https://cdn.jsdelivr.net/gh/jdecked/twemoji@"
        f"{TWEMOJI_VERSION}/assets/72x72/{codepoints}.png"
    )


def _asset_urls(content: str) -> dict[str, str]:
    assets: dict[str, str] = {}
    for match in _CUSTOM_EMOJI_RE.finditer(content):
        extension = "gif" if match.group("animated") else "png"
        assets[match.group(0)] = (
            f"https://cdn.discordapp.com/emojis/{match.group('id')}.{extension}"
            "?size=96&quality=lossless"
        )
    for item in emoji_lib.emoji_list(content):
        value = str(item.get("emoji") or "")
        if value:
            assets[value] = _twemoji_url(value)
    return assets


def _special_spans(content: str) -> list[tuple[int, int, str, str]]:
    spans: list[tuple[int, int, str, str]] = []
    for match in _DISCORD_ENTITY_RE.finditer(content):
        value = match.group(0)
        kind = "image" if _CUSTOM_EMOJI_RE.fullmatch(value) else "entity"
        spans.append((match.start(), match.end(), kind, value))
    occupied = [(start, end) for start, end, _kind, _value in spans]
    for item in emoji_lib.emoji_list(content):
        start = int(item["match_start"])
        end = int(item["match_end"])
        if any(start < other_end and end > other_start for other_start, other_end in occupied):
            continue
        spans.append((start, end, "image", str(item["emoji"])))
    spans.sort(key=lambda value: (value[0], value[1]))
    return spans


def _plain_tokens(value: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    for part in re.split(r"(\r\n|\r|\n|[ \t]+)", value):
        if not part:
            continue
        if "\n" in part or "\r" in part:
            tokens.append(("newline", "\n"))
        elif part.isspace():
            tokens.append(("space", " "))
        else:
            tokens.append(("text", part))
    return tokens


def _tokens(content: str, labels: dict[str, str]) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    cursor = 0
    for start, end, kind, raw in _special_spans(content):
        if start > cursor:
            result.extend(_plain_tokens(content[cursor:start]))
        if kind == "entity":
            result.append(("mention", labels.get(raw, raw)))
        else:
            result.append(("image", raw))
        cursor = end
    if cursor < len(content):
        result.extend(_plain_tokens(content[cursor:]))
    return result


def _decode_asset(data: Optional[bytes], *, size: int) -> Optional[Image.Image]:
    if not data:
        return None
    try:
        image = Image.open(io.BytesIO(data))
        image.seek(0)
        image = image.convert("RGBA")
        image.thumbnail((size, size), Image.Resampling.LANCZOS)
        return image
    except (OSError, ValueError):
        return None


def _ellipsis(
    draw: ImageDraw.ImageDraw,
    value: str,
    *,
    size: int,
    max_width: float,
    bold: bool = False,
) -> str:
    if _text_width(draw, value, size=size, bold=bold) <= max_width:
        return value
    suffix = "…"
    low, high = 0, len(value)
    while low < high:
        middle = (low + high + 1) // 2
        if _text_width(draw, value[:middle] + suffix, size=size, bold=bold) <= max_width:
            low = middle
        else:
            high = middle - 1
    result = value[:low]
    return (result.rstrip() + suffix) if result else suffix


def _draw_inline_text(
    canvas: Image.Image,
    draw: ImageDraw.ImageDraw,
    xy: tuple[float, float],
    value: str,
    *,
    size: int,
    fill: tuple[int, int, int, int],
    asset_data: dict[str, Optional[bytes]],
    max_width: float,
    bold: bool = False,
    emoji_size: Optional[int] = None,
    emoji_y_offset: int = 1,
) -> float:

    start_x, y = xy
    x = start_x
    limit = start_x + max(0.0, max_width)
    image_size = int(emoji_size or size)
    image_advance = image_size + max(2, size // 8)
    ellipsis_width = _text_width(draw, "…", size=size, bold=bold)
    for kind, token in _tokens(value.replace("\r", " ").replace("\n", " "), {}):
        if kind == "newline":
            kind, token = "space", " "
        if kind == "image":
            if x + image_advance > limit:
                if x + ellipsis_width <= limit:
                    _draw_text(draw, (x, y), "…", size=size, fill=fill, bold=bold)
                    x += ellipsis_width
                break
            inline = _decode_asset(asset_data.get(token), size=image_size)
            if inline is not None:
                canvas.alpha_composite(
                    inline,
                    (int(x), int(y + emoji_y_offset)),
                )
            else:
                _draw_text(draw, (x, y), token, size=size, fill=fill, bold=bold)
            x += image_advance
            continue

        shown = " " if kind == "space" else token
        token_width = _text_width(draw, shown, size=size, bold=bold)
        if x + token_width <= limit:
            x = _draw_text(draw, (x, y), shown, size=size, fill=fill, bold=bold)
            continue
        remaining = limit - x
        if remaining >= ellipsis_width:
            clipped = _ellipsis(draw, shown, size=size, max_width=remaining, bold=bold)
            x = _draw_text(draw, (x, y), clipped, size=size, fill=fill, bold=bold)
        break
    return x


def _inline_text_width(
    draw: ImageDraw.ImageDraw,
    value: str,
    *,
    size: int,
    bold: bool = False,
    emoji_size: Optional[int] = None,
) -> float:
    image_size = int(emoji_size or size)
    image_advance = image_size + max(2, size // 8)
    width = 0.0
    for kind, token in _tokens(value.replace("\r", " ").replace("\n", " "), {}):
        if kind == "image":
            width += image_advance
        else:
            shown = " " if kind in {"space", "newline"} else token
            width += _text_width(draw, shown, size=size, bold=bold)
    return width


def _fitting_prefix_length(
    draw: ImageDraw.ImageDraw,
    value: str,
    *,
    size: int,
    max_width: float,
) -> int:

    if not value or max_width <= 0:
        return 0
    if _text_width(draw, value, size=size) <= max_width:
        return len(value)
    low, high = 1, len(value)
    while low < high:
        middle = (low + high + 1) // 2
        if _text_width(draw, value[:middle], size=size) <= max_width:
            low = middle
        else:
            high = middle - 1
    split = low
    original = split
    while (
        split > 1
        and split < len(value)
        and (
            unicodedata.combining(value[split])
            or value[split] in {"\ufe0e", "\ufe0f"}
            or value[split - 1] == "\u200d"
        )
    ):
        split -= 1
    return split or original


def _format_timestamp(
    value: datetime,
    timezone_name: str,
    *,
    now: Optional[datetime] = None,
) -> str:

    zone = ZoneInfo(timezone_name)

    aware_value = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    local_value = aware_value.astimezone(zone)
    local_now = (now or datetime.now(zone)).astimezone(zone)
    clock = local_value.strftime("%I:%M %p").lstrip("0")
    day_delta = (local_now.date() - local_value.date()).days
    if day_delta == 0:
        return clock
    if day_delta == 1:
        return f"Yesterday at {clock}"
    return f"{local_value:%d/%m/%Y} {clock}"


def _without_legacy_gif_title(content: str, page_url: str) -> str:

    try:
        if urlparse(page_url).hostname not in {"tenor.com", "www.tenor.com"}:
            return content
    except ValueError:
        return content
    first, separator, title = content.partition("\n")
    title = title.strip()
    if (
        separator
        and first.strip() == page_url
        and "\n" not in title
        and " GIF - " in title
        and title.endswith(" - Discover & Share GIFs")
    ):
        return first
    return content


def _render_message(
    request: AFKRenderRequest,
    asset_data: dict[str, Optional[bytes]],
    *,
    width: int = CANVAS_WIDTH,
    show_channel: bool = True,
) -> tuple[Image.Image, int]:
    scale = 1
    padding = CANVAS_PADDING * scale
    background = (13, 11, 30, 255)
    channel_colour = (185, 187, 201, 255)
    author_colour = (75, 150, 235, 255)
    time_colour = (163, 166, 179, 255)
    text_colour = (222, 223, 231, 255)
    mention_background = (60, 66, 112, 255)
    mention_colour = (202, 205, 255, 255)

    labels = dict(request.entity_labels)
    content = request.content
    previews = []
    for media in request.media:
        content = _without_legacy_gif_title(content, media.original_url)
        if not media.preview:
            url = media.original_url or media.url
            if url and url not in content:
                content = f"{content}\n{url}".strip()
            continue
        try:
            image = Image.open(io.BytesIO(media.preview)).convert("RGBA")
            if max(image.size) > 300:
                continue
            previews.append(image)
            for url in (media.url, media.original_url):
                if url:
                    content = content.replace(url, "")
        except (ValueError, OSError):
            continue
    tokens = _tokens(content.strip()[:4000], labels)
    probe = Image.new("RGBA", (1, 1), background)
    probe_draw = ImageDraw.Draw(probe)
    text_x = 90 * scale
    max_x = width - padding
    x = text_x
    y = 71 * scale
    line_height = 39 * scale
    placements: list[tuple[str, str, float, float, float]] = []
    max_y = (71 + (MAX_MESSAGE_LINES - 1) * 39) * scale

    for kind, value in tokens:
        if y > max_y:
            break
        if kind == "newline":
            x = text_x
            y += line_height
            continue
        if kind == "space":
            token_width = _text_width(probe_draw, " ", size=27 * scale)
            if x + token_width > max_x:
                x = text_x
                y += line_height
            else:
                x += token_width
            continue
        if kind == "image":
            token_width = 38 * scale
        else:
            shown = value
            if kind == "mention":
                token_width = (
                    min(
                        _inline_text_width(
                            probe_draw,
                            shown,
                            size=27 * scale,
                            emoji_size=27 * scale,
                        ),
                        max_x - text_x - 12 * scale,
                    )
                    + 12 * scale
                )
            else:
                token_width = _text_width(probe_draw, shown, size=27 * scale)
                remaining = shown
                while remaining:
                    if y > max_y:
                        break
                    available = max_x - x
                    remaining_width = _text_width(probe_draw, remaining, size=27 * scale)
                    if remaining_width <= available:
                        placements.append((kind, remaining, x, y, remaining_width))
                        x += remaining_width
                        remaining = ""
                        break
                    if x > text_x:
                        x = text_x
                        y += line_height
                        continue
                    split = _fitting_prefix_length(
                        probe_draw,
                        remaining,
                        size=27 * scale,
                        max_width=max_x - text_x,
                    )
                    split = max(1, split)
                    part = remaining[:split]
                    part_width = _text_width(probe_draw, part, size=27 * scale)
                    placements.append((kind, part, x, y, part_width))
                    remaining = remaining[split:]
                    x = text_x
                    if remaining:
                        y += line_height
                continue
        if x + token_width > max_x and x > text_x:
            x = text_x
            y += line_height
        placements.append((kind, value, x, y, token_width))
        x += token_width

    if y > max_y:
        placements = [placement for placement in placements if placement[3] < max_y]
        placements.append(("text", "…", text_x, max_y, 27 * scale))
        y = max_y
    packed = None
    media_top = int(y + 43) if placements else 79
    if previews:
        from lumi.notes.mosaic import pack

        packed = pack(tuple(image.size for image in previews), int(max_x - text_x))
    height = max(133 * scale, int(y + 46 * scale),
                 int(media_top + packed.height + 12) if packed else 0)
    canvas = Image.new("RGBA", (width, height), background)
    draw = ImageDraw.Draw(canvas)
    if packed:
        for tile in packed.tiles:
            size = (max(1, round(tile.width)), max(1, round(tile.height)))
            image = previews[tile.index].resize(size, Image.Resampling.LANCZOS)
            canvas.alpha_composite(image, (round(text_x + tile.x), round(media_top + tile.y)))

    if show_channel:
        channel_text = f"# {request.channel_name}"[:100]
        _draw_inline_text(
            canvas,
            draw,
            (90 * scale, 8 * scale),
            channel_text,
            size=21 * scale,
            fill=channel_colour,
            asset_data=asset_data,
            max_width=width - (90 * scale) - padding,
            emoji_size=23 * scale,
            emoji_y_offset=1 * scale,
        )

    avatar = _decode_asset(asset_data.get("__avatar__"), size=68 * scale)
    if avatar is not None:
        avatar = ImageOps.fit(avatar, (68 * scale, 68 * scale), method=Image.Resampling.LANCZOS)
        mask = Image.new("L", avatar.size, 0)
        ImageDraw.Draw(mask).ellipse((0, 0, avatar.width - 1, avatar.height - 1), fill=255)
        canvas.paste(avatar, (12 * scale, 41 * scale), mask)
    else:
        draw.ellipse((12 * scale, 41 * scale, 80 * scale, 109 * scale), fill=(52, 55, 68, 255))
        initial = (request.author_name.strip() or "?")[0].upper()
        initial_width = _text_width(draw, initial, size=32 * scale, bold=True)
        _draw_text(
            draw,
            ((46 * scale) - (initial_width / 2), 55 * scale),
            initial,
            size=32 * scale,
            fill=text_colour,
            bold=True,
        )

    timestamp = _format_timestamp(request.occurred_at, request.timezone_name)
    timestamp_width = _text_width(draw, timestamp, size=21 * scale)
    author_end = _draw_inline_text(
        canvas,
        draw,
        (90 * scale, 37 * scale),
        request.author_name or "Someone",
        size=28 * scale,
        fill=author_colour,
        asset_data=asset_data,
        max_width=min(520 * scale, width - text_x - padding - timestamp_width - 11 * scale),
        bold=True,
        emoji_size=30 * scale,
        emoji_y_offset=1 * scale,
    )
    _draw_text(
        draw,
        (author_end + 11 * scale, 42 * scale),
        timestamp,
        size=21 * scale,
        fill=time_colour,
    )

    for kind, value, token_x, token_y, token_width in placements:
        if kind == "mention":
            draw.rounded_rectangle(
                (token_x, token_y + 3 * scale, token_x + token_width, token_y + 35 * scale),
                radius=5 * scale,
                fill=mention_background,
            )
            _draw_inline_text(
                canvas,
                draw,
                (token_x + 6 * scale, token_y),
                value,
                size=27 * scale,
                fill=mention_colour,
                asset_data=asset_data,
                max_width=max(0.0, token_width - 12 * scale),
                emoji_size=27 * scale,
                emoji_y_offset=3 * scale,
            )
        elif kind == "image":
            inline = _decode_asset(asset_data.get(value), size=34 * scale)
            if inline is not None:
                canvas.alpha_composite(inline, (int(token_x + 2 * scale), int(token_y + 3 * scale)))
            else:
                _draw_text(draw, (token_x, token_y), value, size=27 * scale, fill=text_colour)
        else:
            _draw_text(draw, (token_x, token_y), value, size=27 * scale, fill=text_colour)

    return canvas, max(109 * scale, int(y + 39 * scale),
                       int(media_top + packed.height) if packed else 0)


def _draw_connectors(canvas: Image.Image, reply_centres: list[float]) -> None:

    factor = 2
    layer = Image.new("RGBA", (90 * factor, canvas.height * factor))
    draw = ImageDraw.Draw(layer)
    colour = (113, 116, 136, 255)
    stroke = int(CONNECTOR_THICKNESS * factor)

    def line(points: list[tuple[float, float]]) -> None:
        scaled = [(x * factor, y * factor) for x, y in points]
        draw.line(scaled, fill=colour, width=stroke, joint="curve")
        radius = stroke / 2
        for x, y in (scaled[0], scaled[-1]):
            draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=colour)

    def curve(start, control, end):
        return [
            (
                (1 - t) ** 2 * start[0] + 2 * (1 - t) * t * control[0] + t * t * end[0],
                (1 - t) ** 2 * start[1] + 2 * (1 - t) * t * control[1] + t * t * end[1],
            )
            for t in (index / 16 for index in range(17))
        ]

    r = min(CONNECTOR_RADIUS, (82 - 46) / 2, (41 - 22) / 2)
    line([(46, 37), *curve((46, 22 + r), (46, 22), (46 + r, 22)), (82, 22)])
    if reply_centres:
        end = 12 + REPLY_INDENT - 8
        r = min(CONNECTOR_RADIUS, (end - 46) / 2)
        line([(46, 113), (46, reply_centres[-1] - r)])
        for y in reply_centres:
            line([*curve((46, y - r), (46, y), (46 + r, y)), (end, y)])
    canvas.alpha_composite(layer.resize((90, canvas.height), Image.Resampling.LANCZOS))


def _render_sync(
    request: AFKRenderRequest,
    asset_data: dict[str, Optional[bytes]],
) -> bytes:
    if len(request.replies) >= MAX_CONVERSATION_MESSAGES:
        raise ValueError("AFK conversation must be split before rendering")
    original, bottom = _render_message(request, asset_data)
    parts: list[tuple[Image.Image, int]] = []
    reply_centres: list[float] = []
    for reply in request.replies:
        item = AFKRenderRequest(
            reply.message_id,
            "",
            reply.author_name,
            reply.avatar_url,
            reply.content,
            reply.occurred_at,
            request.timezone_name,
            reply.entity_labels,
            media=reply.media,
        )
        assets = {**asset_data, "__avatar__": asset_data.get(f"__avatar_{reply.message_id}__")}
        image, content_bottom = _render_message(
            item,
            assets,
            width=CANVAS_WIDTH - REPLY_INDENT,
            show_channel=False,
        )
        body = image.crop((0, 37, image.width, content_bottom))
        top = bottom + REPLY_SPACING
        parts.append((body, top))
        reply_centres.append(top + 4 + 34)
        bottom = top + body.height
    if parts:
        canvas = Image.new("RGBA", (CANVAS_WIDTH, bottom + 24), (13, 11, 30, 255))
        canvas.paste(original.crop((0, 0, CANVAS_WIDTH, parts[0][1] - REPLY_SPACING)), (0, 0))
        for image, top in parts:
            canvas.paste(image, (REPLY_INDENT, top))
    else:
        canvas = original
    _draw_connectors(canvas, reply_centres)
    output = io.BytesIO()
    final = canvas.convert("RGB")
    final.save(
        output,
        format="WEBP",
        lossless=True,
        quality=100,
        method=1,
        exact=True,
    )
    return output.getvalue()


class AFKMessageRenderer:


    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._asset_inflight: dict[str, asyncio.Future[bytes]] = {}
        self._asset_lock = asyncio.Lock()
        self._emoji_cache = _ByteLRU(max_bytes=32 * 1024 * 1024, ttl_seconds=6 * 60 * 60)
        self._avatar_cache = _ByteLRU(max_bytes=64 * 1024 * 1024, ttl_seconds=10 * 60)
        self._render_cache = _ByteLRU(max_bytes=128 * 1024 * 1024, ttl_seconds=10 * 60)

    async def _get_session(self) -> aiohttp.ClientSession:
        session = self._session
        if session is not None and not session.closed:
            return session
        async with self._session_lock:
            session = self._session
            if session is None or session.closed:
                timeout = aiohttp.ClientTimeout(total=8, connect=3, sock_read=5)
                connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=600)
                session = aiohttp.ClientSession(timeout=timeout, connector=connector)
                self._session = session
            return session

    def _cache_for_url(self, url: str) -> _ByteLRU:
        return self._emoji_cache if "/emojis/" in url or "twemoji@" in url else self._avatar_cache

    async def _download(self, url: str) -> bytes:
        parsed = urlparse(url)
        if parsed.scheme != "https" or (parsed.hostname or "").lower() not in _ALLOWED_ASSET_HOSTS:
            return b""
        session = await self._get_session()
        try:
            async with session.get(url, allow_redirects=False) as response:
                if response.status != 200:
                    return b""
                content_type = str(response.headers.get("Content-Type") or "").lower()
                if not content_type.startswith("image/"):
                    return b""
                declared = int(response.headers.get("Content-Length") or 0)
                if declared > MAX_ASSET_BYTES:
                    return b""
                data = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    data.extend(chunk)
                    if len(data) > MAX_ASSET_BYTES:
                        return b""
                return bytes(data)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            return b""

    async def _asset(self, url: Optional[str]) -> Optional[bytes]:
        if not url:
            return None
        cache = self._cache_for_url(url)
        cached = cache.get(url)
        if cached is not None:
            return cached or None
        async with self._asset_lock:
            cached = cache.get(url)
            if cached is not None:
                return cached or None
            future = self._asset_inflight.get(url)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._asset_inflight[url] = future
                creator = True
            else:
                creator = False
        if not creator:
            value = await asyncio.shield(future)
            return value or None
        try:
            value = await self._download(url)
            cache.put(url, value)
            if not future.done():
                future.set_result(value)
            return value or None
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
                if not isinstance(exc, asyncio.CancelledError):
                    future.exception()
            raise
        finally:
            async with self._asset_lock:
                if self._asset_inflight.get(url) is future:
                    self._asset_inflight.pop(url, None)

    @staticmethod
    def _render_key(request: AFKRenderRequest) -> str:
        try:
            local_date = datetime.now(ZoneInfo(request.timezone_name)).date().isoformat()
        except (TypeError, ValueError, KeyError):
            local_date = "invalid-region"
        payload = {
            "v": RENDERER_VERSION,
            "id": int(request.message_id),
            "channel": request.channel_name,
            "author": request.author_name,
            "avatar": request.avatar_url,
            "content": request.content,
            "occurred": request.occurred_at.isoformat(),
            "timezone": request.timezone_name,
            "labels": list(request.entity_labels),
            "local_date": local_date,
            "media": [(m.url, m.original_url, hashlib.sha256(m.preview or b"").hexdigest())
                      for m in request.media],
            "replies": [
                {
                    "id": reply.message_id,
                    "author": reply.author_name,
                    "avatar": reply.avatar_url,
                    "content": reply.content,
                    "occurred": reply.occurred_at.isoformat(),
                    "labels": list(reply.entity_labels),
                    "media": [(m.url, m.original_url, hashlib.sha256(m.preview or b"").hexdigest())
                              for m in reply.media],
                }
                for reply in request.replies
            ],
        }
        encoded = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    async def render(self, request: AFKRenderRequest) -> bytes:
        ZoneInfo(request.timezone_name)
        key = self._render_key(request)
        cached = self._render_cache.get(key)
        if cached is not None:
            return cached
        urls: dict[str, str] = {}
        for item in (request, *request.replies):
            for value in (item.content[:4000], item.author_name):
                urls.update(_asset_urls(value))
            for _raw, label in item.entity_labels:
                urls.update(_asset_urls(label))
        urls.update(_asset_urls(request.channel_name))
        urls = dict(list(urls.items())[:64])
        avatars = {"__avatar__": request.avatar_url}
        avatars.update(
            {f"__avatar_{reply.message_id}__": reply.avatar_url for reply in request.replies}
        )
        names = [*avatars, *urls]
        values = await asyncio.gather(
            *(self._asset(url) for url in avatars.values()),
            *(self._asset(url) for url in urls.values()),
        )
        assets = dict(zip(names, values))
        webp = await asyncio.to_thread(_render_sync, request, assets)
        self._render_cache.put(key, webp)
        return webp

    async def render_many(self, requests: list[AFKRenderRequest]) -> list[bytes]:
        for request in requests:
            ZoneInfo(request.timezone_name)
        return list(await asyncio.gather(*(self.render(request) for request in requests)))

    async def close(self) -> None:
        session, self._session = self._session, None
        if session is not None and not session.closed:
            await session.close()
        self._emoji_cache.clear()
        self._avatar_cache.clear()
        self._render_cache.clear()


_renderer: Optional[AFKMessageRenderer] = None


def get_afk_message_renderer() -> AFKMessageRenderer:
    global _renderer
    if _renderer is None:
        _renderer = AFKMessageRenderer()
    return _renderer


async def close_afk_message_renderer() -> None:
    global _renderer
    renderer, _renderer = _renderer, None
    if renderer is not None:
        await renderer.close()
