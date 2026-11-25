from __future__ import annotations

import secrets
from functools import lru_cache
from pathlib import Path

LOADING_ASSETS = ("loading.gif", "loading-tide.gif", "loading-petal.gif")


@lru_cache(maxsize=3)
def loading_animation(asset: str = "loading.gif") -> bytes:

    if asset not in LOADING_ASSETS:
        raise ValueError("unknown bundled loading animation")
    data = (Path(__file__).parent / "assets" / asset).read_bytes()
    if len(data) >= 8 * 1024 or not data.startswith(b"GIF89a") or data[6:10] != b"\x40\x00\x40\x00":
        raise ValueError("invalid bundled loading animation")
    return data


def random_loading_animation() -> bytes:

    return loading_animation(secrets.choice(LOADING_ASSETS))
