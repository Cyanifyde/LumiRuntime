from __future__ import annotations

import math
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Tile:
    index: int
    x: float
    y: float
    width: float
    height: float


@dataclass(frozen=True)
class Mosaic:
    width: float
    height: float
    tiles: tuple[Tile, ...]


def pack(sizes: tuple[tuple[int, int], ...], available: int, gap: int = 4) -> Mosaic:
    if not sizes or len(sizes) > 10 or available < 1 or any(min(s) <= 0 for s in sizes):
        raise ValueError("Use one to ten bounded images.")

    def quality(node):
        return sum(math.log(max(0.001, t.width / sizes[t.index][0])) for t in node.tiles)

    def join(a, b, horizontal):
        edge = min(a.height, b.height) if horizontal else min(a.width, b.width)
        sa = edge / (a.height if horizontal else a.width)
        sb = edge / (b.height if horizontal else b.width)
        w = a.width * sa + b.width * sb + gap if horizontal else edge
        h = edge if horizontal else a.height * sa + b.height * sb + gap

        def moved(t, scale, x=0, y=0):
            return Tile(
                t.index, t.x * scale + x, t.y * scale + y, t.width * scale, t.height * scale
            )

        return Mosaic(
            w,
            h,
            (
                *(moved(t, sa) for t in a.tiles),
                *(
                    moved(
                        t,
                        sb,
                        a.width * sa + gap if horizontal else 0,
                        0 if horizontal else a.height * sa + gap,
                    )
                    for t in b.tiles
                ),
            ),
        )

    @lru_cache(maxsize=55)
    def solve(start, end):
        if end - start == 1:
            w, h = sizes[start]
            return (Mosaic(w, h, (Tile(start, 0, 0, w, h),)),)
        bins = {}
        for split in range(start + 1, end):
            for a in solve(start, split):
                for b in solve(split, end):
                    for horizontal in (True, False):
                        node = join(a, b, horizontal)
                        bucket = round(math.log2(node.width / node.height) * 4)
                        score = quality(node)
                        if bucket not in bins or score > bins[bucket][0]:
                            bins[bucket] = (score, node)
        return tuple(value[1] for value in bins.values())

    best, best_score = None, -math.inf
    for node in solve(0, len(sizes)):
        scale = min(1, available / node.width)
        w, h = node.width * scale, node.height * scale
        retained = quality(node) / len(sizes) + math.log(scale)
        score = (
            retained
            + 1.1 * w / available
            - 0.12 * math.log1p(h / available)
            - 0.25 * max(0, h / w - 1)
        )
        if score > best_score:
            best_score = score
            best = Mosaic(
                w,
                h,
                tuple(
                    Tile(t.index, t.x * scale, t.y * scale, t.width * scale, t.height * scale)
                    for t in node.tiles
                ),
            )
    return best
