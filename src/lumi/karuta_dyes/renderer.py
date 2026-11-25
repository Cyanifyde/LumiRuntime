from __future__ import annotations

import base64
import io
import math
from functools import lru_cache
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFont

WIDTH = 1120
RENDER_VERSION = "static-dye-png-v5-measured-purity"
PAD = 32
BACKGROUND = "#14161b"
IMAGE_BACKGROUND = "#24282f"
FOREGROUND = "#edf0f5"
MUTED = "#a9b1c0"
GRID = "#343944"
TRACK = "#2b303a"
BODY_COLOUR = "#91c3b9"
GLOW_COLOUR = "#b8a5ed"
OTHER_HUE_COLOUR = "#d89f82"


@lru_cache(maxsize=96)
def font(size, bold=False):
    filename = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    paths = [
        Path(__file__).parent / "assets" / "fonts" / filename,
        Path("C:/Windows/Fonts/seguisb.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf"),
        Path("/usr/share/fonts/truetype/liberation2")
        / ("LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf"),
        Path("/usr/share/fonts/truetype/liberation")
        / ("LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf"),
        Path("/usr/share/fonts/truetype/noto")
        / ("NotoSans-Bold.ttf" if bold else "NotoSans-Regular.ttf"),
        Path("/usr/share/fonts/truetype/dejavu") / filename,
        filename,
    ]
    for path in paths:
        try:
            return ImageFont.truetype(str(path), int(size))
        except OSError:
            continue
    return ImageFont.load_default(size=int(size))


def percentage(value, approximate=False):
    if value is None:
        return "—"
    if not approximate and 0 < value < 0.1:
        return "<0.1%"
    return ("≈" if approximate else "") + f"{value:.1f}%"


def purity_values(record):

    core = record["regions"]["core"].get("perceptual", {})
    glow = (record.get("glow") or {}).get("perceptual", {})
    return core.get("purity"), glow.get("purity")


def mix_values(appearance):
    values = sorted(
        (dict(v) for v in appearance.get("families", []) if v["share"] > 0),
        key=lambda v: -v["share"],
    )
    result = values[:5]


    remainder = max(0.0, 100 - sum(v["share"] for v in result)) if values else 0
    if remainder > 0.005:
        result.append({"name": "Other", "share": remainder, "hex": MUTED})
    return result


def source_image(record):
    prefix = "data:image/png;base64,"
    encoded = record.get("image", "")
    if not encoded.startswith(prefix) or len(encoded) > 1_400_000:
        raise ValueError("Expected a bounded original PNG in the analysed record")
    raw = base64.b64decode(encoded[len(prefix) :], validate=True)
    with Image.open(io.BytesIO(raw)) as original:
        if original.format != "PNG" or original.size not in ((127, 108), (127, 113)):
            raise ValueError("Unsupported original dye image")
        return original.convert("RGBA")


class Canvas:
    def __init__(self, height, scale):
        self.scale = scale
        self.image = Image.new("RGB", (WIDTH * scale, height * scale), BACKGROUND)
        self.draw = ImageDraw.Draw(self.image)

    def box(self, bounds):
        return tuple(round(v * self.scale) for v in bounds)

    def text(self, xy, value, size=22, colour=FOREGROUND, bold=False, anchor="lt"):
        self.draw.text(
            tuple(round(v * self.scale) for v in xy),
            str(value),
            font=font(size * self.scale, bold),
            fill=colour,
            anchor=anchor,
        )

    def line(self, points, colour=GRID, width=1):
        self.draw.line(
            [tuple(round(v * self.scale) for v in p) for p in points],
            fill=colour,
            width=max(1, round(width * self.scale)),
        )

    def rectangle(self, bounds, colour):
        self.draw.rectangle(self.box(bounds), fill=colour)

    def rule(self, y):
        self.line([(PAD, y), (WIDTH - PAD, y)])

    def finish(self):
        if self.scale == 1:
            return self.image
        return self.image.resize((WIDTH, self.image.height // self.scale), Image.Resampling.LANCZOS)


def title_lines(value, width=WIDTH - 2 * PAD):

    probe = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    face = font(36, True)
    lines = []
    for paragraph in str(value).split("\n"):
        current = ""
        for character in paragraph:
            if current and probe.textlength(current + character, font=face) > width:
                boundary = current.rfind(" ")
                if boundary > 0:
                    lines.append(current[: boundary + 1])
                    current = current[boundary + 1 :]
                else:
                    lines.append(current)
                    current = ""
            current += character
        lines.append(current)
    return lines


def ring(canvas, center, value, label, colour):
    x, y = center
    radius = 72
    bounds = canvas.box((x - radius, y - radius, x + radius, y + radius))
    canvas.draw.arc(bounds, 0, 360, fill=TRACK, width=12 * canvas.scale)
    if value is not None and value > 0:
        canvas.draw.arc(
            bounds,
            -90,
            -90 + 360 * min(100, max(0, value)) / 100,
            fill=colour,
            width=12 * canvas.scale,
        )
    canvas.text((x, y), percentage(value, True), 30, bold=True, anchor="mm")
    canvas.text((x, y + 96), label, 21, MUTED, anchor="mt")


def measured_purity(canvas, center, core):

    value = core.get("purity", {}).get("20")
    x, y = center
    radius = 72
    bounds = canvas.box((x - radius, y - radius, x + radius, y + radius))
    canvas.draw.ellipse(bounds, fill=TRACK if value is None else OTHER_HUE_COLOUR)
    if value is not None and value > 0:
        canvas.draw.pieslice(bounds, -90, -90 + 360 * min(100, value) / 100, fill=BODY_COLOUR)
    canvas.draw.ellipse(
        canvas.box((x - radius + 12, y - radius + 12, x + radius - 12, y + radius - 12)),
        fill=BACKGROUND,
    )
    canvas.text((x, y), percentage(value), 30, bold=True, anchor="mm")
    canvas.text((x, y + 96), "Measured purity", 21, MUTED, anchor="mt")
    canvas.text((x, y + 127), "Colour-weighted · ±20°", 16, MUTED, anchor="mt")
    for xx, label, colour in (
        (x - 91, "Within", BODY_COLOUR),
        (x + 12, "Outside", OTHER_HUE_COLOUR),
    ):
        canvas.rectangle((xx, y + 158, xx + 10, y + 168), colour)
        canvas.text((xx + 17, y + 154), label, 16, MUTED)


@lru_cache(maxsize=1)
def hue_colours():

    from .engine import oklab_to_rgb

    hues = np.deg2rad(np.arange(72) * 5 + 2.5)
    labs = np.c_[np.full(72, 0.72), 0.14 * np.cos(hues), 0.14 * np.sin(hues)]
    return [tuple(v) for v in np.round(oklab_to_rgb(labs) * 255).astype(np.uint8).tolist()]


def chart_geometry(x, y, width, height=220):
    return x + 48, y + 16, x + width - 8, y + height - 42


def spectrum(canvas, x, y, width, appearance):
    left, top, right, bottom = chart_geometry(x, y, width)
    if not appearance:
        canvas.text(((left + right) / 2, (top + bottom) / 2), "—", 24, MUTED, anchor="mm")
        return
    values = appearance.get("spectrum", [0] * 72)
    peak = max(values, default=0)
    maximum = min(100, math.ceil(peak * 1.15)) if peak else 100
    maximum = max(1, maximum)
    for value in (0, maximum / 2, maximum):
        yy = bottom - (bottom - top) * value / maximum
        canvas.line([(left, yy), (right, yy)])
        label = f"{value:.1f}" if maximum < 10 else f"{value:g}"
        canvas.text((left - 9, yy), label, 18, MUTED, anchor="rm")
    canvas.text((left, top - 18), "Area %", 17, MUTED)
    step = (right - left) / 72
    for i, value in enumerate(values):
        if value > 0:
            xx = left + i * step
            yy = bottom - (bottom - top) * value / maximum
            canvas.rectangle((xx, yy, xx + step - 0.7, bottom), hue_colours()[i])
        canvas.rectangle(
            (left + i * step, bottom + 2, left + (i + 1) * step + 0.3, bottom + 7), hue_colours()[i]
        )
    for tick in (0, 90, 180, 270, 360):
        xx = left + (right - left) * tick / 360
        canvas.text(
            (xx, bottom + 17),
            f"{tick}°",
            18,
            MUTED,
            anchor="lt" if tick == 0 else "rt" if tick == 360 else "mt",
        )
    if not peak:
        canvas.text(((left + right) / 2, (top + bottom) / 2), "Neutral", 24, MUTED, anchor="mm")


def colour_mix(canvas, x, y, width, appearance):
    values = mix_values(appearance)
    if not values:
        canvas.text((x, y + 50), "—", 26, MUTED)
        return
    for index, value in enumerate(values):
        yy = y + 14 + index * 31
        canvas.text((x, yy), value["name"], 20, MUTED)
        left, right = x + 116, x + width - 82
        canvas.rectangle((left, yy + 5, right, yy + 18), TRACK)
        if value["share"] > 0:
            canvas.rectangle(
                (left, yy + 5, left + (right - left) * min(100, value["share"]) / 100, yy + 18),
                value["hex"],
            )
        canvas.text((x + width, yy), percentage(value["share"]), 20, anchor="rt")


def shades(canvas, y, metrics, x=PAD, width=510):
    canvas.text((x, y), "Shades", 24, bold=True)
    palette = metrics.get("palette", [])
    step = width / max(1, len(palette))
    for i, item in enumerate(palette):
        xx = x + i * step
        canvas.rectangle((xx, y + 40, xx + step - 12, y + 68), item["hex"])
        canvas.text((xx, y + 78), item["hex"], 18, MUTED)
        canvas.text((xx, y + 103), percentage(item["share"]), 18, MUTED)
    ramp = metrics.get("ramp", [])
    if ramp:
        colours = np.array([[int(colour[i : i + 2], 16) for i in (1, 3, 5)] for colour in ramp])
        pixels_wide = round(width * canvas.scale)
        source = np.linspace(0, len(colours) - 1, pixels_wide)
        pixels = np.stack(
            [np.interp(source, np.arange(len(colours)), colours[:, c]) for c in range(3)], axis=1
        )
        strip = Image.fromarray(np.round(pixels).astype(np.uint8)[None]).resize(
            (pixels_wide, 17 * canvas.scale)
        )
        canvas.image.paste(strip, (round(x * canvas.scale), (y + 143) * canvas.scale))
        canvas.text((x, y + 171), "Dark", 18, MUTED)
        canvas.text((x + width, y + 171), "Light", 18, MUTED, anchor="rt")


def brightness_chart(canvas, x, y, width, body):
    canvas.text((x, y), "Brightness distribution", 24, bold=True)
    left, top, right, bottom = chart_geometry(x, y + 40, width, height=180)
    if not body or body.get("median") is None:
        canvas.text(((left + right) / 2, (top + bottom) / 2), "—", 24, MUTED, anchor="mm")
        return
    values = body["histogram"]
    maximum = max(1, min(100, math.ceil(max(values) * 1.15)))

    def yy(value):
        return bottom - (bottom - top) * value / maximum

    for value in (0, maximum / 2, maximum):
        canvas.line([(left, yy(value)), (right, yy(value))])
        canvas.text((left - 9, yy(value)), f"{value:g}", 18, MUTED, anchor="rm")
    canvas.text((left, top - 18), "Area %", 17, MUTED)
    step = (right - left) / 20
    for i, value in enumerate(values):
        if value > 0:
            xx = left + i * step
            canvas.rectangle((xx, yy(value), xx + step - 1.2, bottom), BODY_COLOUR)
    for tick in (0, 25, 50, 75, 100):
        xx = left + (right - left) * tick / 100
        label = "0 · dark" if tick == 0 else "100 · light" if tick == 100 else str(tick)
        canvas.text(
            (xx, bottom + 15),
            label,
            17,
            MUTED,
            anchor="lt" if tick == 0 else "rt" if tick == 100 else "mt",
        )
    interval = f"90% range · {body['p05']:.0f}\N{EN DASH}{body['p95']:.0f}"
    canvas.text((left, bottom + 49), interval, 18, MUTED)
    canvas.text((right, bottom + 49), f"Median {body['median']:.0f}", 18, MUTED, anchor="rt")


def glow_fade(canvas, x, y, width, glow):
    left, top, right, bottom = chart_geometry(x, y, width)
    profile = glow.get("added", {}).get("profile", [])
    maximum = max((point["distance"] for point in profile), default=1)

    def xx(value):
        return left + (right - left) * (value - 1) / max(1, maximum - 1)

    def yy(value):
        return bottom - (bottom - top) * value / 100

    for value in (0, 50, 100):
        canvas.line([(left, yy(value)), (right, yy(value))])
        canvas.text((left - 9, yy(value)), str(value), 18, MUTED, anchor="rm")
    canvas.text((left, top - 18), "Extra opacity %", 17, MUTED)
    if not profile:
        canvas.text(((left + right) / 2, (top + bottom) / 2), "—", 24, MUTED, anchor="mm")
    for key, colour in (("mean", MUTED), ("p90", GLOW_COLOUR)):
        points = [(xx(point["distance"]), yy(point[key])) for point in profile]
        if len(points) > 1:
            canvas.line(points, colour, 2.5)
    for tick in sorted({1, max(1, round(maximum / 2)), maximum}):
        canvas.text(
            (xx(tick), bottom + 17),
            str(tick),
            18,
            MUTED,
            anchor="lt" if tick == 1 else "rt" if tick == maximum else "mt",
        )
    canvas.text((right, bottom + 41), "Distance · px", 17, MUTED, anchor="rt")
    canvas.text((right, top + 7), "90th percentile", 17, GLOW_COLOUR, anchor="rt")
    canvas.text((right, top + 31), "Mean", 17, MUTED, anchor="rt")


def render_dye(record, *, scale=2):

    if not isinstance(scale, int) or not 1 <= scale <= 4:
        raise ValueError("Supersampling must be an integer from 1 to 4")
    original = source_image(record)
    whole, glow = record["regions"]["whole"], record.get("glow")
    lines = title_lines(record.get("name", "Dye"))
    hero_top = 32 + len(lines) * 44 + 54
    chart_top = hero_top + (390 if glow else 296)
    shade_top = chart_top + 270
    glow_top = shade_top + 282
    height = glow_top + 292 if glow else shade_top + 276
    canvas = Canvas(height, scale)
    for i, line in enumerate(lines):
        canvas.text((PAD, PAD + i * 44), line, 36, bold=True)
    canvas.text(
        (PAD, PAD + len(lines) * 44 + 9),
        str(record.get("code", "")) + " · " + ("Mystic" if record.get("mystic") else "Dye"),
        22,
        MUTED,
    )


    target = (260, round(260 * original.height / original.width))
    canvas.rectangle((PAD, hero_top, PAD + target[0], hero_top + target[1]), IMAGE_BACKGROUND)
    shown = original.resize((target[0] * scale, target[1] * scale), Image.Resampling.LANCZOS)
    canvas.image.paste(shown, (PAD * scale, hero_top * scale), shown)
    body_value, glow_value = purity_values(record)
    ring(canvas, (426, hero_top + 101), body_value, "Purity", BODY_COLOUR)
    measured_purity(canvas, (650, hero_top + 101), record["regions"]["core"])
    if glow:
        ring(canvas, (874, hero_top + 101), glow_value, "Halo purity", GLOW_COLOUR)

    stats = [
        ("Lightness", f"{whole['lightness']:.1f}"),
        ("Neutral area", percentage(whole.get("appearance", {}).get("neutral_share"))),
    ]
    if glow:
        added = glow.get("added", {})
        reach = (
            ("≥" if added.get("clipped") else "") + str(added["reach10"]) + " px"
            if added.get("reach10") is not None
            else "—"
        )
        stats += [("Extra opacity", percentage(added.get("opacity_p95"))), ("Glow reach", reach)]
    for index, (label, value) in enumerate(stats):
        x, y = (80 + index * 260, hero_top + 283) if glow else (812 + index * 145, hero_top + 31)
        canvas.text((x, y), value, 28, bold=True)
        canvas.text((x, y + 40), label, 19, MUTED)

    canvas.rule(chart_top - 22)
    canvas.text((PAD, chart_top), "Colour spectrum", 24, bold=True)
    spectrum(canvas, PAD, chart_top + 40, 620, whole.get("appearance", {}))
    canvas.text((714, chart_top), "Colour mix", 24, bold=True)
    colour_mix(canvas, 714, chart_top + 40, WIDTH - PAD - 714, whole.get("appearance", {}))
    shades(canvas, shade_top, whole)
    brightness_chart(canvas, 596, shade_top, 492, whole.get("brightness"))
    if glow:
        canvas.rule(glow_top - 20)
        canvas.text((PAD, glow_top), "Outer halo spectrum", 24, bold=True)
        spectrum(canvas, PAD, glow_top + 42, 510, glow.get("appearance", {}))
        canvas.text((596, glow_top), "Added glow fade", 24, bold=True)
        glow_fade(canvas, 596, glow_top + 42, 492, glow)
    return canvas.finish()
