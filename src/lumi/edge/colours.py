from __future__ import annotations

from collections.abc import Iterable, Mapping
from functools import lru_cache
from math import sqrt
from types import MappingProxyType

LUMI_COLOUR = 0x5865F2
Lab = tuple[float, float, float]


def _linear(channel: int) -> float:
    value = channel / 255
    return value / 12.92 if value <= 0.04045 else ((value + 0.055) / 1.055) ** 2.4


def oklab(colour: int) -> Lab:

    red, green, blue = (_linear((colour >> shift) & 255) for shift in (16, 8, 0))
    light = (0.4122214708 * red + 0.5363325363 * green + 0.0514459929 * blue) ** (1 / 3)
    medium = (0.2119034982 * red + 0.6806995451 * green + 0.1073969566 * blue) ** (1 / 3)
    short = (0.0883024619 * red + 0.2817188376 * green + 0.6299787005 * blue) ** (1 / 3)
    return (
        0.2104542553 * light + 0.7936177850 * medium - 0.0040720468 * short,
        1.9779984951 * light - 2.4285922050 * medium + 0.4505937099 * short,
        0.0259040371 * light + 0.7827717662 * medium - 0.8086757660 * short,
    )


def _distance(first: Lab, second: Lab) -> float:
    light, red, blue = first[0] - second[0], first[1] - second[1], first[2] - second[2]
    return light * light + red * red + blue * blue


def separation(first: int, second: int) -> float:
    return sqrt(_distance(oklab(first), oklab(second)))


@lru_cache(maxsize=1)
def _candidates() -> tuple[tuple[int, Lab], ...]:
    colours = []


    for red in range(0, 256, 17):
        for green in range(0, 256, 17):
            for blue in range(0, 256, 17):
                colour = (red << 16) | (green << 8) | blue
                lab = oklab(colour)
                luminance = sum(
                    weight * _linear(value)
                    for weight, value in zip(
                        (0.2126, 0.7152, 0.0722), (red, green, blue), strict=True
                    )
                )
                if (
                    0.58 <= lab[0] <= 0.84
                    and lab[1] ** 2 + lab[2] ** 2 >= 0.10**2
                    and 0.14 <= luminance <= 0.475
                    and colour != LUMI_COLOUR
                ):
                    colours.append((colour, lab))
    return tuple(colours)


def _farthest(nearest: list[float]) -> int:

    return max(range(len(nearest)), key=nearest.__getitem__)


@lru_cache(maxsize=128)
def _palette(count: int) -> tuple[int, ...]:

    if count == 0:
        return ()
    candidates = _candidates()
    if count < 0 or count > len(candidates):
        raise ValueError("module palette capacity exceeded")
    brand = oklab(LUMI_COLOUR)
    nearest = [_distance(lab, brand) for _, lab in candidates]
    chosen: list[int] = []
    for _ in range(count):
        index = _farthest(nearest)
        chosen.append(index)
        selected = candidates[index][1]
        nearest = [
            min(old, _distance(lab, selected))
            for old, (_, lab) in zip(nearest, candidates, strict=True)
        ]
    for _ in range(3):
        changed = False
        for position, current in enumerate(chosen):
            others = [
                brand,
                *(candidates[index][1] for i, index in enumerate(chosen) if i != position),
            ]
            scores = [min(_distance(lab, other) for other in others) for _, lab in candidates]
            replacement = _farthest(scores)
            if scores[replacement] > scores[current] + 1e-12:
                chosen[position] = replacement
                changed = True
        if not changed:
            break
    return tuple(candidates[index][0] for index in chosen)


@lru_cache(maxsize=128)
def _assigned(keys: tuple[str, ...], branded: frozenset[str]) -> Mapping[str, int]:
    ordinary = tuple(key for key in keys if key not in branded)
    result = dict(zip(ordinary, _palette(len(ordinary)), strict=True))
    result.update(dict.fromkeys(branded, LUMI_COLOUR))
    return MappingProxyType(result)


def module_colours(modules: Iterable[str], branded: Iterable[str] = ()) -> Mapping[str, int]:

    keys = set(modules)
    return _assigned(tuple(sorted(keys)), frozenset(branded) & keys)


def module_colour(module: str, modules: Iterable[str]) -> int:
    return module_colours(modules).get(module, LUMI_COLOUR)
