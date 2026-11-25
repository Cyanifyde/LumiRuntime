from __future__ import annotations

import asyncio
import hashlib
import io
import os
import unicodedata
from collections import OrderedDict
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageStat

from .catalogue import FrameRecord
from .matting import apply_matte_patch, artwork_support_mask, load_matte_manifest

CARD_SIZE = (381, 570)


ART_BASE_SIZE = (285, 394)
ART_CENTER_X = 191.5
ART_BOTTOM = 514
GRID_COLUMNS = 4
GRID_ROWS = 3
GRID_PAGE_SIZE = GRID_COLUMNS * GRID_ROWS
RENDERER_VERSION = "frame-tester-v9-reviewed-shop-mattes"


@dataclass(frozen=True, slots=True)
class CardTextLayout:
    name_center_y: int = 95
    name_max_width: int = 306
    name_max_size: int = 52
    series_center_y: int = 474
    series_max_width: int = 304
    series_max_size: int = 34
    series_wrap_width: int = 230
    series_wrapped_max_size: int = 27
    name_tone: str = "auto"
    series_tone: str = "auto"
    stroke_width: int = 0


DEFAULT_CARD_TEXT_LAYOUT = CardTextLayout()


CARD_TEXT_LAYOUTS = {
    "infinity void": CardTextLayout(name_tone="light", series_tone="light", stroke_width=2),
    "abandoned church": CardTextLayout(
        name_center_y=91,
        name_max_size=48,
        series_center_y=478,
        series_wrapped_max_size=24,
        name_tone="dark",
        series_tone="dark",
    ),
    "spellbook": CardTextLayout(
        name_center_y=109,
        name_max_size=53,
        series_center_y=460,
        series_max_size=32,
        name_tone="light",
        series_tone="light",
    ),
    "lighthouse storm": CardTextLayout(
        name_center_y=83,
        series_center_y=463,
        series_max_size=35,
        name_tone="light",
        series_tone="light",
    ),
    "island grotto": CardTextLayout(
        name_center_y=94,
        name_max_width=202,
        series_center_y=483,
        series_wrap_width=225,
        name_tone="dark",
        series_tone="dark",
    ),
    "chopsticks": CardTextLayout(
        name_center_y=94,
        name_max_width=230,
        series_center_y=475,
        series_wrap_width=210,
        name_tone="dark",
        series_tone="dark",
    ),
    "yakuza": CardTextLayout(
        name_center_y=100,
        name_max_width=239,
        series_center_y=472,
        name_tone="dark",
        series_tone="dark",
    ),
    "snowlands": CardTextLayout(
        name_center_y=96,
        name_max_width=260,
        series_center_y=477,
        name_tone="dark",
        series_tone="dark",
    ),
}

AMARANTH_FONT_PATHS = (
    "/usr/share/fonts/truetype/amaranth/Amaranth-Bold.ttf",
    "/usr/local/share/fonts/Amaranth-Bold.ttf",
    "C:/Windows/Fonts/Amaranth-Bold.ttf",
)
CARD_FONT_FALLBACK_PATHS = (
    "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
)


class FrameRenderer:
    def __init__(
        self,
        frame_root: Path,
        *,
        composite_cache_size: int = 96,
        grid_cache_size: int = 16,
        card_font_path: Path | None = None,
        card_font_data: bytes | None = None,
    ) -> None:
        self._frame_root = frame_root
        self._matte_patches = load_matte_manifest(frame_root.parent)
        if frame_root.resolve() == (Path(__file__).with_name("assets") / "frames").resolve():
            if not self._matte_patches:
                raise ValueError("bundled frame matte manifest is missing")
        self._frames: dict[str, Image.Image] = {}
        self._art_support: dict[str, Image.Image | None] = {}
        self._composites: OrderedDict[str, bytes] = OrderedDict()
        self._grids: OrderedDict[str, bytes] = OrderedDict()
        self._composite_cache_size = composite_cache_size
        self._grid_cache_size = grid_cache_size
        self._inflight: dict[str, asyncio.Task[bytes]] = {}
        self._lock = asyncio.Lock()
        self._small_font = _load_font(16)
        self._cost_font = _load_font(14)
        self._card_font_data = _load_card_font_data(card_font_path, card_font_data)
        self._card_fonts: dict[int, ImageFont.FreeTypeFont] = {}
        probe = self._card_font(16)
        family, style = probe.getname()
        self._card_font_family = f"{family} {style}".strip()
        if os.environ.get("LUMI_GROUP") == "cards" and family.casefold() != "amaranth":
            raise RuntimeError("Cards requires the bundled Amaranth font")

    @property
    def card_font_family(self) -> str:
        return self._card_font_family

    async def composite(
        self,
        artwork: Image.Image,
        artwork_hash: str,
        frame: FrameRecord,
        *,
        character_name: str = "",
        series_name: str = "",
    ) -> bytes:
        character_name = _label_text(character_name)
        series_name = _label_text(series_name)
        key = _digest_key(
            RENDERER_VERSION,
            artwork_hash,
            frame.sha256,
            str(frame.image_scale),
            str(frame.image_offset_x),
            str(frame.image_offset_y),
            character_name,
            series_name,
        )
        return await self._coalesced(
            key,
            self._composites,
            self._composite_cache_size,
            lambda: asyncio.to_thread(
                self._render_composite,
                artwork.copy(),
                frame,
                character_name,
                series_name,
            ),
        )

    async def grid(
        self,
        artwork: Image.Image,
        artwork_hash: str,
        frames: tuple[FrameRecord, ...],
        *,
        character_name: str = "",
        series_name: str = "",
    ) -> bytes:
        if not frames or len(frames) > GRID_PAGE_SIZE:
            raise ValueError("grid requires 1 to 12 frames")
        character_name = _label_text(character_name)
        series_name = _label_text(series_name)
        key = _digest_key(
            RENDERER_VERSION,
            "grid",
            artwork_hash,
            character_name,
            series_name,
            *(
                _digest_key(
                    frame.identifier,
                    frame.name,
                    frame.sha256,
                    str(frame.image_scale),
                    str(frame.image_offset_x),
                    str(frame.image_offset_y),
                    frame.cost.cache_key,
                )
                for frame in frames
            ),
        )

        async def render() -> bytes:
            composites = await asyncio.gather(
                *(
                    self.composite(
                        artwork,
                        artwork_hash,
                        frame,
                        character_name=character_name,
                        series_name=series_name,
                    )
                    for frame in frames
                )
            )
            return await asyncio.to_thread(self._render_grid, composites, frames)

        return await self._coalesced(
            key,
            self._grids,
            self._grid_cache_size,
            render,
        )

    async def _coalesced(
        self,
        key: str,
        cache: OrderedDict[str, bytes],
        maximum: int,
        factory,
    ) -> bytes:
        async with self._lock:
            cached = cache.get(key)
            if cached is not None:
                cache.move_to_end(key)
                return cached
            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(factory())
                self._inflight[key] = task
        try:
            data = await task
        finally:
            async with self._lock:
                if self._inflight.get(key) is task:
                    self._inflight.pop(key, None)
        async with self._lock:
            cache[key] = data
            cache.move_to_end(key)
            while len(cache) > maximum:
                cache.popitem(last=False)
        return data

    def _render_composite(
        self,
        artwork: Image.Image,
        frame: FrameRecord,
        character_name: str,
        series_name: str,
    ) -> bytes:
        frame_image = self._frame(frame)
        canvas = Image.new("RGBA", CARD_SIZE, (0, 0, 0, 0))
        box = Image.new("RGBA", ART_BASE_SIZE, (0, 0, 0, 0))
        fitted = _contain(artwork, ART_BASE_SIZE)
        box.alpha_composite(
            fitted,
            (
                (ART_BASE_SIZE[0] - fitted.width) // 2,
                (ART_BASE_SIZE[1] - fitted.height) // 2,
            ),
        )

        scaled_size = (
            max(1, round(ART_BASE_SIZE[0] * frame.image_scale)),
            max(1, round(ART_BASE_SIZE[1] * frame.image_scale)),
        )
        if box.size != scaled_size:
            box = box.resize(scaled_size, Image.Resampling.LANCZOS)

        left = round(
            ART_CENTER_X
            - box.width / 2
            + box.width * frame.image_offset_x / 100
        )
        top = round(
            ART_BOTTOM
            - box.height
            + box.height * frame.image_offset_y / 100
        )
        _alpha_composite_clipped(canvas, box, left, top)
        support = self._art_support.get(frame.identifier)
        if support is not None:
            canvas.putalpha(ImageChops.multiply(canvas.getchannel("A"), support.convert("L")))
        canvas.alpha_composite(frame_image)
        self._draw_card_text(canvas, frame, character_name, series_name)
        return _webp(canvas)

    def _draw_card_text(
        self,
        canvas: Image.Image,
        frame: FrameRecord,
        character_name: str,
        series_name: str,
    ) -> None:
        draw = ImageDraw.Draw(canvas)
        layout = CARD_TEXT_LAYOUTS.get(frame.name.casefold(), DEFAULT_CARD_TEXT_LAYOUT)
        name_sample_box = (34, 66, 347, 142)
        series_sample_box = (30, 423, 351, 510)
        if character_name:
            font, shown = self._fit_single_line(
                character_name,
                maximum_width=layout.name_max_width,
                maximum_size=layout.name_max_size,
            )
            self._draw_text_line(
                canvas,
                draw,
                shown,
                font,
                center_y=layout.name_center_y,
                sample_box=name_sample_box,
                tone=layout.name_tone,
                stroke_width=layout.stroke_width,
            )
        if series_name:
            font, lines = self._fit_series(
                series_name,
                maximum_width=layout.series_max_width,
                maximum_size=layout.series_max_size,
                wrapped_width=layout.series_wrap_width,
                wrapped_maximum_size=layout.series_wrapped_max_size,
            )
            line_step = max(18, round(font.size * 0.9))
            first_center = layout.series_center_y - line_step * (len(lines) - 1) / 2
            for index, line in enumerate(lines):
                self._draw_text_line(
                    canvas,
                    draw,
                    line,
                    font,
                    center_y=round(first_center + index * line_step),
                    sample_box=series_sample_box,
                    tone=layout.series_tone,
                    stroke_width=layout.stroke_width,
                )

    def _fit_single_line(
        self,
        text: str,
        *,
        maximum_width: int,
        maximum_size: int,
    ) -> tuple[ImageFont.FreeTypeFont, str]:
        for size in range(maximum_size, 15, -1):
            font = self._card_font(size)
            if _text_width(text, font) <= maximum_width:
                return font, text
        font = self._card_font(16)
        return font, _ellipsize(text, font, maximum_width)

    def _fit_series(
        self,
        text: str,
        *,
        maximum_width: int,
        maximum_size: int,
        wrapped_width: int,
        wrapped_maximum_size: int,
    ) -> tuple[ImageFont.FreeTypeFont, tuple[str, ...]]:
        one_line_font = self._card_font(maximum_size)
        if _text_width(text, one_line_font) <= maximum_width:
            return one_line_font, (text,)
        wrapped_width = min(maximum_width, wrapped_width)
        for size in range(wrapped_maximum_size, 15, -1):
            font = self._card_font(size)
            lines, complete = _wrap_card_text(text, font, wrapped_width, maximum_lines=2)
            if complete:
                return font, lines
        font = self._card_font(16)
        lines, _ = _wrap_card_text(text, font, wrapped_width, maximum_lines=2)
        return font, lines

    def _draw_text_line(
        self,
        canvas: Image.Image,
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.FreeTypeFont,
        *,
        center_y: int,
        sample_box: tuple[int, int, int, int],
        tone: str,
        stroke_width: int = 0,
    ) -> None:
        bounds = draw.textbbox((0, 0), text, font=font)
        width = bounds[2] - bounds[0]
        height = bounds[3] - bounds[1]
        x = (CARD_SIZE[0] - width) / 2 - bounds[0]
        y = center_y - height / 2 - bounds[1]
        fill = _text_colour(canvas, sample_box, tone=tone)
        draw.text(
            (x, y),
            text,
            font=font,
            fill=fill,
            stroke_width=stroke_width,
            stroke_fill=(0, 0, 0, 255),
        )

    def _card_font(self, size: int) -> ImageFont.FreeTypeFont:
        cached = self._card_fonts.get(size)
        if cached is None:
            cached = ImageFont.truetype(io.BytesIO(self._card_font_data), size)
            self._card_fonts[size] = cached
        return cached

    def _frame(self, frame: FrameRecord) -> Image.Image:
        cached = self._frames.get(frame.identifier)
        if cached is not None:
            return cached
        path = (self._frame_root / frame.file_name).resolve()
        root = self._frame_root.resolve()
        if path.parent != root or path.is_symlink() or not path.is_file():
            raise ValueError("frame asset is unavailable")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != frame.sha256:
            raise ValueError("frame asset hash mismatch")
        with Image.open(io.BytesIO(data)) as source:
            if source.format != "PNG" or source.size != CARD_SIZE:
                raise ValueError("frame asset has invalid dimensions")
            source.load()
            image = source.convert("RGBA")
        self._art_support[frame.identifier] = artwork_support_mask(image)
        if self._matte_patches:
            matte = self._matte_patches.get(frame.file_name)
            if matte is None or matte["source_sha256"] != frame.sha256:
                raise ValueError("frame matte is not bound to this source")
            if matte["file_name"] is not None:
                image = apply_matte_patch(
                    image,
                    self._frame_root.parent / "matte-patches" / matte["file_name"],
                    matte["sha256"],
                )


        image = image.filter(_nullbat_neutral_lut())
        self._frames[frame.identifier] = image
        return image

    def _render_grid(
        self,
        composites: tuple[bytes, ...] | list[bytes],
        frames: tuple[FrameRecord, ...],
    ) -> bytes:
        cell_width = 198
        cell_height = 354
        card_width = 174
        card_height = round(card_width * CARD_SIZE[1] / CARD_SIZE[0])
        margin = 14
        grid = Image.new(
            "RGB",
            (
                margin * 2 + GRID_COLUMNS * cell_width,
                margin * 2 + GRID_ROWS * cell_height,
            ),
            (11, 11, 28),
        )
        draw = ImageDraw.Draw(grid)
        for index, (data, frame) in enumerate(zip(composites, frames, strict=True)):
            column = index % GRID_COLUMNS
            row = index // GRID_COLUMNS
            x = margin + column * cell_width
            y = margin + row * cell_height
            with Image.open(io.BytesIO(data)) as source:
                preview = source.convert("RGBA").resize(
                    (card_width, card_height), Image.Resampling.LANCZOS
                )
            card_x = x + (cell_width - card_width) // 2
            background = Image.new("RGBA", preview.size, (46, 49, 54, 255))
            background.alpha_composite(preview)
            grid.paste(background.convert("RGB"), (card_x, y))
            label = f"{index + 1}. {frame.name}"
            lines = _wrap_label(draw, label, self._small_font, cell_width - 12, 2)
            text_y = y + card_height + 8
            for line in lines:
                bounds = draw.textbbox((0, 0), line, font=self._small_font)
                width = bounds[2] - bounds[0]
                draw.text(
                    (x + (cell_width - width) / 2, text_y),
                    line,
                    fill=(238, 238, 244),
                    font=self._small_font,
                )
                text_y += 20
            cost_lines = _wrap_label(
                draw,
                frame.cost.compact,
                self._cost_font,
                cell_width - 12,
                2,
            )
            text_y += 2
            for line in cost_lines:
                bounds = draw.textbbox((0, 0), line, font=self._cost_font)
                width = bounds[2] - bounds[0]
                draw.text(
                    (x + (cell_width - width) / 2, text_y),
                    line,
                    fill=(164, 166, 184),
                    font=self._cost_font,
                )
                text_y += 18
        return _webp(grid)


def _contain(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    scale = min(size[0] / image.width, size[1] / image.height)
    dimensions = (
        max(1, round(image.width * scale)),
        max(1, round(image.height * scale)),
    )
    return image.convert("RGBA").resize(dimensions, Image.Resampling.LANCZOS)


def _alpha_composite_clipped(
    destination: Image.Image,
    source: Image.Image,
    left: int,
    top: int,
) -> None:
    destination_box = (
        max(0, left),
        max(0, top),
        min(destination.width, left + source.width),
        min(destination.height, top + source.height),
    )
    if destination_box[0] >= destination_box[2] or destination_box[1] >= destination_box[3]:
        return
    source_box = (
        destination_box[0] - left,
        destination_box[1] - top,
        destination_box[2] - left,
        destination_box[3] - top,
    )
    destination.alpha_composite(
        source.crop(source_box),
        (destination_box[0], destination_box[1]),
    )


def _webp(image: Image.Image) -> bytes:
    output = io.BytesIO()
    image.save(output, format="WEBP", lossless=True, method=1, exact=True)
    return output.getvalue()


def _digest_key(*values: str) -> str:
    digest = hashlib.sha256()
    for value in values:
        digest.update(value.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _neutral_nullbat_colour(
    red: float,
    green: float,
    blue: float,
) -> tuple[float, float, float]:
    red_byte = round(red * 255)
    green_byte = round(green * 255)
    blue_byte = round(blue * 255)
    red_blue_range = red_byte - blue_byte
    if (
        red_byte - green_byte >= 50
        and green_byte >= blue_byte
        and 4 * (green_byte - blue_byte) <= 3 * red_blue_range
        and 255 * red_blue_range >= 60 * red_byte
    ):
        grey = (54 * red_byte + 183 * green_byte + 19 * blue_byte + 128) // 256
        normalized = grey / 255
        return normalized, normalized, normalized
    return red, green, blue


@lru_cache(maxsize=1)
def _nullbat_neutral_lut() -> ImageFilter.Color3DLUT:
    return ImageFilter.Color3DLUT.generate(33, _neutral_nullbat_colour)


def _load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = (
        "/usr/share/fonts/opentype/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
    )
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _load_card_font_data(path: Path | None, data: bytes | None) -> bytes:
    if data:
        return bytes(data)
    candidates = ((str(path),) if path is not None else ()) + AMARANTH_FONT_PATHS
    candidates += CARD_FONT_FALLBACK_PATHS
    for candidate in candidates:
        try:
            payload = Path(candidate).read_bytes()
            ImageFont.truetype(io.BytesIO(payload), 16)
            return payload
        except (OSError, ValueError):
            continue
    raise RuntimeError("no usable card text font is installed")


def _label_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).split())[:200]


def _text_width(text: str, font: ImageFont.FreeTypeFont) -> int:
    bounds = font.getbbox(text)
    return bounds[2] - bounds[0]


def _ellipsize(text: str, font: ImageFont.FreeTypeFont, maximum_width: int) -> str:
    if _text_width(text, font) <= maximum_width:
        return text
    shown = text
    while shown and _text_width(f"{shown.rstrip()}…", font) > maximum_width:
        shown = shown[:-1]
    return f"{shown.rstrip()}…" if shown else "…"


def _wrap_card_text(
    text: str,
    font: ImageFont.FreeTypeFont,
    maximum_width: int,
    *,
    maximum_lines: int,
) -> tuple[tuple[str, ...], bool]:
    words = text.split()
    lines: list[str] = []
    current = ""
    while words:
        word = words[0]
        candidate = f"{current} {word}".strip()
        if current and _text_width(candidate, font) > maximum_width:
            lines.append(current)
            current = ""
            if len(lines) == maximum_lines:
                remainder = " ".join(words)
                lines[-1] = _ellipsize(f"{lines[-1]} {remainder}", font, maximum_width)
                return tuple(lines), False
            continue
        words.pop(0)
        if not current and _text_width(word, font) > maximum_width:
            current = _ellipsize(word, font, maximum_width)
        else:
            current = candidate
    if current:
        lines.append(current)
    return tuple(lines or ("",)), True


def _text_colour(
    canvas: Image.Image,
    sample_box: tuple[int, int, int, int],
    *,
    tone: str = "auto",
) -> tuple[int, int, int, int]:
    if tone == "dark":
        return (0, 0, 0, 255)
    if tone == "light":
        return (255, 255, 255, 255)
    luminance = _sample_luminance(canvas, sample_box)
    if luminance >= 145:
        return (0, 0, 0, 255)
    return (255, 255, 255, 255)


def _sample_luminance(
    canvas: Image.Image,
    sample_box: tuple[int, int, int, int],
) -> float:
    region = canvas.crop(sample_box).convert("RGBA")
    background = Image.new("RGBA", region.size, (46, 49, 54, 255))
    background.alpha_composite(region)
    red, green, blue = ImageStat.Stat(background.convert("RGB")).mean
    return 0.2126 * red + 0.7152 * green + 0.0722 * blue


def _wrap_label(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    maximum_width: int,
    maximum_lines: int,
) -> tuple[str, ...]:
    words = text.split()
    lines: list[str] = []
    current = ""
    while words and len(lines) < maximum_lines:
        word = words.pop(0)
        candidate = f"{current} {word}".strip()
        width = draw.textbbox((0, 0), candidate, font=font)[2]
        if current and width > maximum_width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current and len(lines) < maximum_lines:
        lines.append(current)
    if words and lines:
        value = lines[-1]
        while value and draw.textbbox((0, 0), f"{value}…", font=font)[2] > maximum_width:
            value = value[:-1]
        lines[-1] = f"{value.rstrip()}…"
    return tuple(lines)
