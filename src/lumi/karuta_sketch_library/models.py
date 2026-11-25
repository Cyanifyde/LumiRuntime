from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass

from lumi.karuta_collections.models import Blob

CODE = re.compile(r"[a-z0-9]{3,16}\Z")
HASH = re.compile(r"[a-f0-9]{64}\Z")
IMAGE_SIZE = (274, 405)


def search_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


@dataclass(frozen=True, slots=True)
class SketchRecord:
    code: str
    character: str
    series: str
    edition: int
    print_num: int
    ink: int
    blob: Blob
    source_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.code, str)
            or not CODE.fullmatch(self.code)
            or not isinstance(self.source_sha256, str)
            or not HASH.fullmatch(self.source_sha256)
        ):
            raise ValueError("invalid sketch identity")
        if not all(
            isinstance(s, str)
            and s.strip()
            and len(s) <= 240
            and not any(c in s for c in "\r\n\x00")
            for s in (self.character, self.series)
        ):
            raise ValueError("invalid sketch names")
        if any(type(n) is not int for n in (self.edition, self.print_num, self.ink)):
            raise ValueError("invalid sketch numbers")
        if not 1 <= self.edition <= 100 or not 1 <= self.print_num <= 2_147_483_647:
            raise ValueError("invalid sketch edition or print")
        if not 0 <= self.ink <= 2_147_483_647:
            raise ValueError("invalid sketch ink")
        if self.blob.mime != "image/webp":
            raise ValueError("invalid sketch image")

    def to_dict(self) -> dict:
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> SketchRecord:
        return cls(**{**raw, "blob": Blob(**raw["blob"])})


@dataclass(frozen=True, slots=True)
class CardInfo:


    code: str
    character: str
    series: str
    edition: int
    print_num: int
    sketched: bool
    ink: int | None
    created_ms: int
    message_id: int
    revision_ms: int
    event_order: int
    image_key: str = ""

    def __post_init__(self):
        from lumi.karuta_collections.models import Card

        Card(self.code, self.character, self.series, self.edition, self.print_num)
        if type(self.sketched) is not bool or (
            self.ink is not None
            and (type(self.ink) is not int or not 0 <= self.ink <= 2_147_483_647)
        ):
            raise ValueError("invalid card info")
        if (
            any(type(v) is not int or not 0 <= v <= 18_446_744_073_709_551_615 for v in self.stamp)
            or min(self.created_ms, self.message_id) <= 0
            or self.revision_ms < self.created_ms
        ):
            raise ValueError("invalid observation clock")
        if self.image_key and not HASH.fullmatch(self.image_key):
            raise ValueError("invalid image key")

    @property
    def stamp(self):

        return self.created_ms, self.message_id, self.revision_ms, self.event_order

    @property
    def signature(self):

        return hashlib.sha256(
            json.dumps(
                [
                    self.code,
                    self.character,
                    self.series,
                    self.edition,
                    self.print_num,
                    self.sketched,
                    self.ink,
                    *self.stamp[:3],
                    self.image_key,
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class LibraryView:
    count: int
    offset: int
    records: tuple[SketchRecord, ...]
    revision: int
    anchor_found: bool = True
