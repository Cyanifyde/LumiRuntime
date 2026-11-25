from __future__ import annotations

import hashlib
import io
import json
import re
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageFilter

SHOP_BACKGROUND = (46, 49, 54)
CARD_SIZE = (381, 570)
MATTE_VERSION = "shop-openings-v1"


ARTWORK_OPENINGS: dict[str, tuple[tuple[int, int], ...]] = {


    "infinity void": ((26, 34), (354, 34), (354, 539), (26, 539)),
    "blood thorn": ((35, 52), (345, 52), (345, 518), (35, 518)),
    "dreadstone": ((37, 145), (344, 145), (344, 428), (37, 428)),
    "hexweave": (
        (36, 73), (55, 53), (325, 53), (345, 73),
        (345, 497), (325, 517), (55, 517), (36, 497),
    ),
    "shadow needlework": (
        (35, 70), (56, 52), (324, 52), (345, 70),
        (345, 499), (325, 519), (55, 519), (35, 499),
    ),
    "spectral sigils": ((34, 54), (347, 54), (347, 518), (34, 518)),
}


def recover_shop_opening(image: Image.Image, name: str) -> Image.Image:

    polygon = ARTWORK_OPENINGS.get(name.casefold())
    if polygon is None:
        return image
    if image.size != CARD_SIZE:
        raise ValueError("shop overlay has invalid dimensions")
    image = image.convert("RGBA")
    flattened = Image.new("RGBA", image.size, (*SHOP_BACKGROUND, 255))
    flattened.alpha_composite(image)
    opening = Image.new("L", image.size)
    ImageDraw.Draw(opening).polygon(polygon, fill=255)
    recovered = bytearray(image.tobytes())
    rgb = flattened.tobytes()
    for index, selected in enumerate(opening.tobytes()):
        if selected:
            offset = index * 4
            recovered[offset:offset + 4] = _recover_colour(tuple(rgb[offset:offset + 3]))
    return Image.frombytes("RGBA", image.size, bytes(recovered))


def recover_shop_fades(image: Image.Image) -> Image.Image:

    if image.size != CARD_SIZE:
        raise ValueError("shop overlay has invalid dimensions")
    image = image.convert("RGBA")
    flattened = Image.new("RGBA", image.size, (*SHOP_BACKGROUND, 255))
    flattened.alpha_composite(image)
    pixels = flattened.tobytes()
    gradients = [
        ImageChops.subtract(channel.filter(ImageFilter.MaxFilter(3)),
                            channel.filter(ImageFilter.MinFilter(3)))
        for channel in flattened.convert("RGB").split()
    ]
    gradient = ImageChops.lighter(ImageChops.lighter(*gradients[:2]), gradients[2])
    connected = gradient.point([0 if value <= 20 else 255 for value in range(256)])
    alpha = image.getchannel("A")
    found = False
    for y in range(150, 431, 35):
        for x in range(85, 297, 35):
            seed = (x, y)
            if alpha.getpixel(seed) < 128 and connected.getpixel(seed) == 0:
                ImageDraw.floodfill(connected, seed, 128, thresh=0)
                found = True
    if not found:
        return image
    interior = connected.point([255 if value == 128 else 0 for value in range(256)])


    interior = interior.filter(ImageFilter.GaussianBlur(1.5))
    recovered = bytearray(image.tobytes())
    for index, selected in enumerate(interior.tobytes()):
        if selected:
            offset = index * 4
            rgb = tuple(pixels[offset:offset + 3])
            colour = _recover_colour(rgb)

            if colour[3] < recovered[offset + 3]:
                if selected != 255:
                    opacity = round(recovered[offset + 3] +
                                    (colour[3] - recovered[offset + 3]) * selected / 255)
                    colour = _colour_at_opacity(rgb, opacity)
                recovered[offset:offset + 4] = colour
    return Image.frombytes("RGBA", image.size, bytes(recovered))


@lru_cache(maxsize=65_536)
def _colour_at_opacity(rgb: tuple[int, ...], alpha: int) -> bytes:
    if alpha == 0:
        return bytes(4)
    opacity = alpha / 255
    foreground = tuple(
        max(0, min(255, round((value - (1 - opacity) * background) / opacity)))
        for value, background in zip(rgb, SHOP_BACKGROUND, strict=True)
    )
    return bytes((*foreground, alpha))


@lru_cache(maxsize=65_536)
def _recover_colour(rgb: tuple[int, ...]) -> bytes:
    opacity = max(
        (background - value) / background if value < background
        else (value - background) / (255 - background)
        for value, background in zip(rgb, SHOP_BACKGROUND, strict=True)
    )
    alpha = round(opacity * 255)
    if alpha == 0:
        return bytes(4)
    foreground = tuple(
        max(0, min(255, round((value - (1 - opacity) * background) / opacity)))
        for value, background in zip(rgb, SHOP_BACKGROUND, strict=True)
    )
    return bytes((*foreground, alpha))


def shop_jpeg_overlay(image: Image.Image, name: str) -> Image.Image:

    if name.casefold() not in ARTWORK_OPENINGS or image.size != CARD_SIZE:
        raise ValueError("unreviewed shop frame")
    image = image.convert("RGBA")
    difference = ImageChops.difference(
        image.convert("RGB"), Image.new("RGB", image.size, SHOP_BACKGROUND)
    )
    red, green, blue = difference.split()
    largest = ImageChops.lighter(ImageChops.lighter(red, green), blue)
    near_background = largest.point([255 if value <= 3 else 0 for value in range(256)])
    connected = ImageChops.invert(near_background)
    for seed in ((0, 0), (380, 0), (0, 569), (380, 569)):
        if connected.getpixel(seed) == 0:
            ImageDraw.floodfill(connected, seed, 128, thresh=0)
    exterior = connected.point([255 if value == 128 else 0 for value in range(256)])

    fringe = exterior.filter(ImageFilter.MaxFilter(3))
    clear = ImageChops.multiply(fringe, near_background)
    keep = Image.composite(Image.new("RGBA", image.size), image, clear)
    return recover_shop_opening(keep, name)


def encode_matte_patch(before: Image.Image, after: Image.Image) -> bytes | None:

    old, new = before.convert("RGBA").tobytes(), after.convert("RGBA").tobytes()
    patch = bytearray(len(new))
    changed = False
    for offset in range(0, len(new), 4):
        if new[offset + 3] < old[offset + 3]:
            patch[offset:offset + 3] = new[offset:offset + 3]
            patch[offset + 3] = new[offset + 3] + 1
            changed = True
    if not changed:
        return None
    output = io.BytesIO()
    Image.frombytes("RGBA", before.size, bytes(patch)).save(output, format="PNG", optimize=True)
    return output.getvalue()


def load_matte_manifest(root: Path) -> dict[str, dict[str, str | None]]:
    path = root / "matte-patches.json"
    if not path.exists():
        return {}
    if path.is_symlink() or path.stat().st_size > 1024 * 1024:
        raise ValueError("invalid frame matte manifest")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(raw, dict) or set(raw) != {"version", "frames"}
            or raw["version"] != MATTE_VERSION or not isinstance(raw["frames"], dict)
            or not 1 <= len(raw["frames"]) <= 1000):
        raise ValueError("invalid frame matte manifest")
    for name, row in raw["frames"].items():
        if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}\.png", name)
                or not isinstance(row, dict)
                or set(row) != {"source_sha256", "file_name", "sha256"}
                or not isinstance(row["source_sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", row["source_sha256"])):
            raise ValueError("invalid frame matte binding")
        if row["file_name"] is None and row["sha256"] is None:
            continue
        if (not isinstance(row["file_name"], str) or row["file_name"] != name
                or not isinstance(row["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", row["sha256"])):
            raise ValueError("invalid frame matte patch")
    return raw["frames"]


def apply_matte_patch(image: Image.Image, path: Path, digest: str) -> Image.Image:
    if path.is_symlink() or not path.is_file():
        raise ValueError("frame matte patch is unavailable")
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError("frame matte patch hash mismatch")
    with Image.open(io.BytesIO(data)) as source:
        if source.format != "PNG" or source.size != CARD_SIZE or source.mode != "RGBA":
            raise ValueError("invalid frame matte patch image")
        source.load()
        patch = source.copy()
    encoded_alpha = patch.getchannel("A")
    mask = encoded_alpha.point([0] + [255] * 255)
    patch.putalpha(encoded_alpha.point([0, *range(255)]))
    return Image.composite(patch, image, mask)


def artwork_support_mask(frame: Image.Image) -> Image.Image | None:

    wall = frame.convert("RGBA").getchannel("A").point([0] + [255] * 255)
    padded = Image.new("L", (frame.width + 2, frame.height + 2))
    padded.paste(wall, (1, 1))
    ImageDraw.floodfill(padded, (0, 0), 128, thresh=0)
    outside = padded.crop((1, 1, frame.width + 1, frame.height + 1))
    if outside.getpixel((frame.width // 2, frame.height // 2)) == 128:
        return None
    return outside.point([0 if value == 128 else 255 for value in range(256)]).convert(
        "1", dither=Image.Dither.NONE
    )
