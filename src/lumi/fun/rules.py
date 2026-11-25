from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit


RESERVED = frozenset(
    {
        "fun",
        "help",
        "lumi",
        "ping",
        "block",
        "unblock",
        "restart",
        "drain",
        "maintenance",
        "settings",
        "modules",
        "eventdrop",
    }
)
NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
SLUG = re.compile(r"^/gifs/[A-Za-z0-9_-]{1,200}/?$")
TARGET = re.compile(r"^(?:<@!?(\d{17,20})>|(\d{17,20}))$")


def stable_id(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def category_name(value: str, conflicts=()) -> str:
    name = value.strip().casefold()
    if not NAME.fullmatch(name):
        raise ValueError("Use 1-32 letters, numbers, - or _.")
    if name in RESERVED or name in conflicts:
        raise ValueError("Command already exists.")
    return name


def gif_url(value: str) -> tuple[str, str]:

    original = value.strip()
    if len(original) > 350 or any(ord(c) < 33 for c in original):
        raise ValueError("Invalid URL")
    try:
        parsed = urlsplit(original)
        if (
            parsed.scheme != "https"
            or parsed.netloc.casefold() not in {"klipy.com", "www.klipy.com"}
            or not SLUG.fullmatch(parsed.path)
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Use a KLIPY GIF link")
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Use a KLIPY GIF link") from exc
    return original, "https://klipy.com" + parsed.path.rstrip("/")


def url_batch(text: str) -> list[str]:
    if len(text) > 4000:
        raise ValueError("Maximum 4,000 characters.")
    values = [part.strip() for part in re.split(r"[,\r\n]+", text) if part.strip()]
    if not values or len(values) > 25:
        raise ValueError("Add 1-25 URLs.")
    return values
