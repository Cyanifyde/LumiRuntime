from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass

from .query import collection_metrics


def clean_text(value: str, maximum: int = 240) -> str:
    text = unicodedata.normalize("NFKC", value)
    text = "".join(
        char for char in text if unicodedata.category(char) not in {"Cf", "Cc"} or char.isspace()
    )
    return " ".join(text.strip(" *`_").split())[:maximum]


def identity(character: str, series: str) -> str:
    character, series = clean_text(character), clean_text(series)
    if not character:
        raise ValueError("incomplete character identity")
    return hashlib.sha256((character.casefold() + "\0" + series.casefold()).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Card:
    code: str
    character: str
    series: str
    edition: int
    print_num: int
    quality: int | None = None
    wishlist: int | None = None
    effort: int | None = None

    def __post_init__(self):
        if not 3 <= len(self.code) <= 16 or any(
            c not in "abcdefghijklmnopqrstuvwxyz0123456789" for c in self.code
        ):
            raise ValueError("invalid card code")
        if type(self.edition) is not int or not 1 <= self.edition <= 100:
            raise ValueError("invalid edition")
        if type(self.print_num) is not int or not 0 <= self.print_num <= 2_147_483_647:
            raise ValueError("invalid print")
        identity(self.character, self.series)
        if not self.series:
            raise ValueError("missing card series")
        for value, maximum in (
            (self.quality, 4),
            (self.wishlist, 2_147_483_647),
            (self.effort, 2_147_483_647),
        ):
            if value is not None and (type(value) is not int or not 0 <= value <= maximum):
                raise ValueError("invalid card facts")

    @property
    def identity(self) -> str:
        return identity(self.character, self.series)

    @classmethod
    def from_dict(cls, row):
        metrics = row.get("metrics") or {}
        if isinstance(metrics, str):
            metrics = collection_metrics(metrics)
        quality = row.get("quality")
        if row.get("stars") is not None:
            quality = row["stars"].count("★") + row["stars"].count("⭐")
        return cls(
            str(row["code"]).casefold(),
            clean_text(row["character"]),
            clean_text(row["series"]),
            row["edition"],
            row["print_num"],
            quality,
            row.get("wishlist", metrics.get("wishlist")),
            row.get("effort", metrics.get("effort")),
        )


@dataclass(frozen=True, slots=True)
class Blob:
    sha256: str
    extension: str
    mime: str
    size: int
    width: int
    height: int

    def __post_init__(self):
        if (
            len(self.sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.sha256)
            or {"jpg": "image/jpeg", "png": "image/png", "webp": "image/webp"}.get(self.extension)
            != self.mime
            or any(type(v) is not int or v <= 0 for v in (self.size, self.width, self.height))
            or self.size > 10 * 1024 * 1024
            or max(self.width, self.height) > 8192
            or self.width * self.height > 16_000_000
        ):
            raise ValueError("invalid image metadata")


@dataclass(frozen=True, slots=True, order=True)
class Stamp:
    at_ms: int
    order: int = 0

    def __post_init__(self):
        if (
            type(self.at_ms) is not int
            or type(self.order) is not int
            or min(self.at_ms, self.order) < 0
        ):
            raise ValueError("invalid observation clock")


def complete_name(value: str) -> bool:

    return bool(
        isinstance(value, str)
        and 0 < len(value.strip(" *`_")) < 240
        and "…" not in value
        and "..." not in value
        and "(alias of " not in value.casefold()
        and " · " not in value
        and not any(fragment in value for fragment in ("<t:", "<@", "◈", "\n", "\r"))
        and not clean_text(value)
        .casefold()
        .startswith(
            (
                "dropped on ",
                "dropped in ",
                "owned by ",
                "grabbed by ",
                "framed with ",
                "dyed with ",
                "morphed by ",
                "sketched by ",
                "affection rating",
            )
        )
        and clean_text(value)
    )
