from __future__ import annotations

import asyncio
import hashlib
import math
from collections import OrderedDict
from functools import lru_cache
from html import escape
from io import BytesIO
from itertools import pairwise
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw, ImageFont

from .models import COLOURS, DAY_MS, GAMES, LABELS, DailyCount, date_label

WIDTH, HEIGHT = 736, 254
RENDERER_VERSION = "drop-count-pillow-2"
SUPERSAMPLE = 3
PLOT_SIZE = (348, 120)
PLOT_FRAME = (64, 12, 332, 75)
BACKGROUND, FOREGROUND, MUTED, BORDER = "#1e1f22", "#f2f3f5", "#b5bac1", "#3b3d44"


def _scale(maximum: int) -> tuple[int, tuple[int, ...]]:
    raw = max(1, maximum * 1.08 / 4)
    magnitude = 10 ** math.floor(math.log10(raw))
    step = next(magnitude * value for value in (1, 2, 5, 10) if magnitude * value >= raw)
    high = max(step, math.ceil(maximum * 1.08 / step) * step)
    return high, tuple(range(0, high + 1, step))


def _text(x, y, value, *, anchor="start", size=12, fill=MUTED, extra=""):
    return (
        f'<text x="{x:.3f}" y="{y:.3f}" text-anchor="{anchor}" '
        f'font-size="{size}" fill="{fill}" {extra}>{escape(str(value))}</text>'
    )


def _plot_data(rows, games, *, running):
    series = {game: [] for game in games}
    for game in games:
        total = 0
        for row in rows:
            total += row.count(game)
            series[game].append(total if running else row.count(game))
    high, ticks = _scale(max((max(values) for values in series.values()), default=0))
    return series, high, ticks


def _tick_indices(count):
    return tuple(
        dict.fromkeys(round(i * (count - 1) / min(3, count - 1)) for i in range(min(4, count)))
    )


def _date_layout(rows, *, running):

    left, _, right, _ = PLOT_FRAME
    count = len(rows)
    indices = _tick_indices(count)
    step = (right - left) / (count - 0.12)
    bandwidth, offset = step * 0.76, step * 0.12
    labels, centres = [], []
    for index in indices:
        x = (
            left + index / (count - 1) * (right - left)
            if running
            else left + offset + index * step + bandwidth / 2
        )
        anchor = "start" if index == indices[0] else "end" if index == indices[-1] else "middle"
        label = date_label(rows[index].day_ms)
        half = _font(12).getlength(label) / (2 * SUPERSAMPLE)
        centres.append(x + (half if anchor == "start" else -half if anchor == "end" else 0))
        labels.append((x, label, anchor))
    positions = []
    for segment, (first, last) in enumerate(pairwise(indices)):
        positions.extend(
            centres[segment]
            + (index - first) / (last - first) * (centres[segment + 1] - centres[segment])
            for index in range(first, last)
        )
    positions.append(centres[-1])

    gap = min(b - a for a, b in pairwise(positions))
    return tuple(positions), tuple(labels), min(bandwidth, gap * 0.98)


def _plot(rows: tuple[DailyCount, ...], games: tuple[str, ...], *, running: bool) -> str:
    width, height = PLOT_SIZE
    left, top, right, bottom = PLOT_FRAME
    series, high, ticks = _plot_data(rows, games, running=running)

    def y(amount):
        return bottom - amount / high * (bottom - top)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="{"Running total" if running else "Daily drops"}">',
        '<defs><clipPath id="clip-' + str(int(running)) + '">'
        f'<rect x="{left}" y="{top}" width="{right - left}" height="{bottom - top}"/>'
        "</clipPath></defs>",
    ]
    for tick in ticks:
        at = y(tick)
        parts.append(
            f'<path d="M{left},{at:.3f}H{right}" stroke="{BORDER}" '
            'stroke-width="1" opacity="0.55"/>'
        )
        parts.append(_text(left - 9, at + 4, tick, anchor="end"))
    parts.append(
        f'<rect data-chart-frame="" x="{left}" y="{top}" width="{right - left}" '
        f'height="{bottom - top}" fill="none" stroke="{BORDER}" stroke-width="1"/>'
    )
    positions, labels, bandwidth = _date_layout(rows, running=running)
    for x, label, anchor in labels:
        parts.append(_text(x, bottom + 17, label, anchor=anchor))
    parts.extend(
        (
            _text((left + right) / 2, height - 4, "Date", anchor="middle"),
            f'<text class="axis-title" data-axis="y" '
            f'transform="translate(15,{(top + bottom) / 2}) rotate(-90)" text-anchor="middle" '
            f'font-size="12" fill="{MUTED}">{"Total drops" if running else "Drops"}</text>',
            '<g clip-path="url(#clip-' + str(int(running)) + ')">',
        )
    )
    if running:
        for game in games:
            path = " ".join(
                ("M" if index == 0 else "L") + f"{positions[index]:.3f},{y(value):.3f}"
                for index, value in enumerate(series[game])
            )
            parts.append(
                f'<path data-series="{game}" d="{path}" fill="none" '
                f'stroke="{COLOURS[game]}" stroke-width="2" stroke-linejoin="round"/>'
            )
    else:
        inner_step = bandwidth / (len(games) + 0.12)
        bar_width = inner_step * 0.88
        for index, centre in enumerate(positions):
            for position, game in enumerate(games):
                at = centre - bandwidth / 2 + inner_step * (position + 0.12)
                value = series[game][index]
                parts.append(
                    f'<rect data-series="{game}" x="{at:.3f}" y="{y(value):.3f}" '
                    f'width="{bar_width:.3f}" height="{bottom - y(value):.3f}" '
                    f'fill="{COLOURS[game]}"/>'
                )
    parts.append("</g></svg>")
    return "".join(parts)


def _validate_chart(rows, game):
    if game not in ("all", *GAMES) or len(rows) not in (7, 14, 28):
        raise ValueError("Invalid chart selection.")
    if any(
        row.day_ms != rows[0].day_ms + index * DAY_MS
        or row.day_ms % DAY_MS
        or any(type(row.count(key)) is not int or row.count(key) < 0 for key in GAMES)
        for index, row in enumerate(rows)
    ):
        raise ValueError("Invalid chart counts.")
    return GAMES if game == "all" else (game,)


def chart_html(rows: tuple[DailyCount, ...], game: str = "all") -> str:
    games = _validate_chart(rows, game)
    totals = "".join(
        f'<span class="game"><i style="background:{COLOURS[key]}"></i>'
        f"{LABELS[key]} <strong>{sum(row.count(key) for row in rows):,}</strong></span>"
        for key in games
    )
    plots = "".join(
        "<figure><h3>" + title + "</h3>" + _plot(rows, games, running=running) + "</figure>"
        for running, title in ((False, "Daily drops"), (True, "Running total"))
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'">
<title>Drop counts</title><style>
*{{box-sizing:border-box}}html,body{{margin:0;width:{WIDTH}px;height:{HEIGHT}px;overflow:hidden}}
body{{background:{BACKGROUND};color:{FOREGROUND};font:14px/1.5 Arial,'Liberation Sans',sans-serif}}
main{{padding:20px}}header{{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:4px}}
h2{{margin:0;font-size:20px;font-weight:500;line-height:32px}}
.date{{color:{MUTED};font-size:12px}}.games{{display:flex;gap:20px;margin-bottom:6px;line-height:21px}}
.game{{display:inline-flex;align-items:center;gap:7px}}
.game i{{width:9px;height:9px;border-radius:2px}}
strong{{font-weight:500;font-variant-numeric:tabular-nums}}
.plots{{display:grid;grid-template-columns:348px 348px}}
figure{{margin:0}}h3{{margin:0 16px 3px 64px;font-size:16px;font-weight:500;line-height:28px}}
svg{{display:block;pointer-events:none}}text{{font-family:Arial,'Liberation Sans',sans-serif}}
</style></head><body><main aria-label="Drop counts">
<header><h2>Drop counts</h2><span class="date">
{date_label(rows[0].day_ms)}&ndash;{date_label(rows[-1].day_ms)} · UTC</span></header>
<div class="games">{totals}</div><div class="plots">{plots}</div>
</main></body></html>"""


@lru_cache(maxsize=8)
def _font(size):


    for path in (
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ):
        if Path(path).is_file():
            return ImageFont.truetype(path, size * SUPERSAMPLE)
    return ImageFont.load_default(size=size * SUPERSAMPLE)


def _draw_text(draw, xy, text, *, size=12, fill=MUTED, anchor="ls"):
    draw.text(
        tuple(value * SUPERSAMPLE for value in xy),
        str(text),
        font=_font(size),
        fill=fill,
        anchor=anchor,
    )


def _draw_plot(canvas, rows, games, origin, *, running):
    scale = SUPERSAMPLE
    draw = ImageDraw.Draw(canvas)
    ox, oy = origin
    left, top, right, bottom = PLOT_FRAME
    series, high, ticks = _plot_data(rows, games, running=running)

    def point(x, y):
        return ((ox + x) * scale, (oy + y) * scale)

    def y(amount):
        return bottom - amount / high * (bottom - top)

    border, background = ImageColor.getrgb(BORDER), ImageColor.getrgb(BACKGROUND)
    grid = tuple(round(a * 0.55 + b * 0.45) for a, b in zip(border, background, strict=True))
    for tick in ticks:
        draw.line((point(left, y(tick)), point(right, y(tick))), fill=grid, width=scale)
        _draw_text(draw, (ox + left - 9, oy + y(tick) + 4), tick, anchor="rs")
    draw.rectangle((point(left, top), point(right, bottom)), outline=BORDER, width=scale)
    positions, labels, bandwidth = _date_layout(rows, running=running)
    for x, label, anchor in labels:
        _draw_text(
            draw,
            (ox + x, oy + bottom + 17),
            label,
            anchor={"start": "ls", "end": "rs", "middle": "ms"}[anchor],
        )
    _draw_text(draw, (ox + (left + right) / 2, oy + PLOT_SIZE[1] - 4), "Date", anchor="ms")
    label = "Total drops" if running else "Drops"
    bounds = _font(12).getbbox(label)
    vertical = Image.new("RGBA", (bounds[2] - bounds[0], bounds[3] - bounds[1]))
    ImageDraw.Draw(vertical).text((-bounds[0], -bounds[1]), label, font=_font(12), fill=MUTED)
    vertical = vertical.transpose(Image.Transpose.ROTATE_90)
    canvas.paste(
        vertical,
        (
            round((ox + 15) * scale - vertical.width),
            round((oy + (top + bottom) / 2) * scale - vertical.height / 2),
        ),
        vertical,
    )
    layer = Image.new("RGBA", canvas.size)
    data = ImageDraw.Draw(layer)
    if running:
        for game in games:
            points = [point(positions[index], y(value)) for index, value in enumerate(series[game])]
            data.line(points, fill=COLOURS[game], width=2 * scale, joint="curve")
    else:
        inner = bandwidth / (len(games) + 0.12)
        for index, centre in enumerate(positions):
            for position, game in enumerate(games):
                value = series[game][index]
                if value:
                    x = centre - bandwidth / 2 + inner * (position + 0.12)
                    data.rectangle(
                        (point(x, y(value)), point(x + inner * 0.88, bottom)), fill=COLOURS[game]
                    )

    clip = tuple(round(value) for value in (*point(left, top), *point(right, bottom)))
    layer = layer.crop(clip)
    canvas.paste(layer, clip[:2], layer)


def render_chart(rows: tuple[DailyCount, ...], game: str = "all") -> bytes:

    games = _validate_chart(rows, game)
    scale = SUPERSAMPLE
    canvas = Image.new("RGB", (WIDTH * scale, HEIGHT * scale), BACKGROUND)
    draw = ImageDraw.Draw(canvas)
    _draw_text(draw, (20, 43), "Drop counts", size=20, fill=FOREGROUND)
    _draw_text(
        draw,
        (WIDTH - 20, 43),
        f"{date_label(rows[0].day_ms)}\u2013{date_label(rows[-1].day_ms)} · UTC",
        anchor="rs",
    )
    x = 20.0
    for key in games:
        draw.rounded_rectangle(
            (x * scale, 62 * scale, (x + 9) * scale, 71 * scale),
            radius=2 * scale,
            fill=COLOURS[key],
        )
        label = f"{LABELS[key]} {sum(row.count(key) for row in rows):,}"
        _draw_text(draw, (x + 16, 72), label, size=14, fill=FOREGROUND)
        x += 16 + draw.textlength(label, font=_font(14)) / scale + 20
    for running, title in ((False, "Daily drops"), (True, "Running total")):
        ox = 20 + int(running) * PLOT_SIZE[0]
        _draw_text(draw, (ox + PLOT_FRAME[0], 102), title, size=16, fill=FOREGROUND)
        _draw_plot(canvas, rows, games, (ox, 114), running=running)
    canvas = canvas.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS)
    output = BytesIO()
    canvas.save(output, "PNG", compress_level=3)
    return output.getvalue()


class PillowChartRenderer:


    def __init__(self):
        self.lock = asyncio.Lock()
        self.cache: OrderedDict[str, bytes] = OrderedDict()

    async def render(self, rows: tuple[DailyCount, ...], game: str = "all") -> bytes:
        _validate_chart(rows, game)
        key = hashlib.sha256((RENDERER_VERSION + game + repr(rows)).encode()).hexdigest()
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            task = asyncio.create_task(self._rasterize(rows, game))
            try:
                data = await asyncio.shield(task)
            except asyncio.CancelledError:


                await asyncio.gather(task, return_exceptions=True)
                raise
            self.cache[key] = data
            while len(self.cache) > 32:
                self.cache.popitem(last=False)
            return data

    async def _rasterize(self, rows, game):
        data = await asyncio.to_thread(render_chart, rows, game)
        self._validate_png(data)
        return data

    @staticmethod
    def _validate_png(data: bytes) -> None:
        if not 0 < len(data) <= 2 * 1024 * 1024:
            raise OSError("Invalid chart output.")
        with Image.open(BytesIO(data)) as image:
            if image.format != "PNG" or image.size != (WIDTH, HEIGHT):
                raise OSError("Invalid chart dimensions.")
            image.verify()

    async def close(self):
        async with self.lock:
            self.cache.clear()
