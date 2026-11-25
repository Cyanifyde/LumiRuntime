from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

CATEGORY_ORDER = ("Standard", "Special", "Carousel", "Seasonal", "Other")
EXPECTED_FRAME_COUNT = 463
_IDENTIFIER = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_FILE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}\.png$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BIT_NAME = re.compile(r"^[A-Za-z][A-Za-z ]{0,49}$")
_COST_SOURCES = {"karuta-market", "nullbat", "karuta-market+nullbat", "karuta", "cardcompanion"}


@dataclass(frozen=True, slots=True)
class BitMaterial:
    name: str
    amount: int


@dataclass(frozen=True, slots=True)
class FrameCost:
    kind: str
    source: str
    as_of: str
    materials: tuple[BitMaterial, ...] = ()
    ticket_low: float | None = None
    ticket_high: float | None = None

    @property
    def cache_key(self) -> str:
        if self.kind == "bits":
            values = ",".join(f"{item.amount}:{item.name}" for item in self.materials)
        elif self.kind == "tickets":
            values = f"{_format_number(self.ticket_low)}:{_format_number(self.ticket_high)}"
        else:
            values = "unknown"
        return f"{self.kind}:{values}:{self.source}:{self.as_of}"

    @property
    def compact(self) -> str:
        if self.kind == "bits":
            values = " + ".join(
                f"{_format_compact_amount(item.amount)} {item.name}" for item in self.materials
            )
            return f"{values} bits"
        if self.kind == "tickets":
            low = _format_number(self.ticket_low)
            high = _format_number(self.ticket_high)
            value = low if low == high else f"{low}\N{EN DASH}{high}"
            return f"~{value} tickets"
        return "Price unavailable"

    @property
    def detail(self) -> str:
        if self.kind == "bits":
            return " + ".join(f"{item.amount:,} {item.name} Bits" for item in self.materials)
        return self.compact


UNKNOWN_FRAME_COST = FrameCost(
    kind="unknown",
    source="karuta-market+nullbat",
    as_of="2026-10-06",
)


@dataclass(frozen=True, slots=True)
class FrameRecord:
    identifier: str
    name: str
    category: str
    category_order: int
    order: int
    requirements: str
    transparent: bool
    file_name: str
    sha256: str
    image_scale: float
    image_offset_x: float
    image_offset_y: float
    source: str
    cost: FrameCost = UNKNOWN_FRAME_COST


class FrameCatalogue:
    def __init__(self, records: tuple[FrameRecord, ...], asset_root: Path) -> None:
        self.records = records
        self.asset_root = asset_root
        self._by_id = {record.identifier: record for record in records}

    @classmethod
    def bundled(cls) -> FrameCatalogue:
        asset_root = Path(__file__).with_name("assets")
        return cls.load(asset_root / "catalogue.json", asset_root / "frames")

    @classmethod
    def load(cls, catalogue_path: Path, frame_root: Path) -> FrameCatalogue:
        raw = json.loads(catalogue_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("frame catalogue must be a list")
        records: list[FrameRecord] = []
        identifiers: set[str] = set()
        file_names: set[str] = set()
        names: set[tuple[str, str]] = set()
        expected_keys = {
            "id",
            "name",
            "category",
            "category_order",
            "order",
            "requirements",
            "transparent",
            "file_name",
            "hash",
            "image_scale",
            "image_offset_x",
            "image_offset_y",
            "source",
            "cost",
        }
        for item in raw:
            if not isinstance(item, dict) or set(item) != expected_keys:
                raise ValueError("frame catalogue has an invalid record shape")
            record = cls._record(item)
            if record.identifier in identifiers or record.file_name in file_names:
                raise ValueError("frame catalogue contains a duplicate identity")
            normalized_name = (record.category, normalize_search(record.name))
            if normalized_name in names:
                raise ValueError("frame catalogue contains a duplicate category name")
            identifiers.add(record.identifier)
            file_names.add(record.file_name)
            names.add(normalized_name)
            records.append(record)
        records.sort(key=lambda item: (item.category_order, item.order, item.name.casefold()))
        if len(records) != EXPECTED_FRAME_COUNT:
            raise ValueError(
                f"frame catalogue contains {len(records)} records; expected {EXPECTED_FRAME_COUNT}"
            )
        expected_orders = {name: index + 1 for index, name in enumerate(CATEGORY_ORDER)}
        for record in records:
            if expected_orders.get(record.category) != record.category_order:
                raise ValueError("frame category ordering is invalid")


            if (record.cost.kind == "bits" and record.category != "Standard") or (
                record.category == "Standard"
                and record.cost.kind != "bits"
                and not (record.cost.kind == "tickets" and record.cost.source == "cardcompanion")
            ):
                raise ValueError("frame cost kind does not match its category")
            path = frame_root / record.file_name
            if path.parent != frame_root or not path.is_file() or path.is_symlink():
                raise ValueError(f"missing bundled frame: {record.file_name}")
        for category in CATEGORY_ORDER:
            orders = [record.order for record in records if record.category == category]
            if orders != list(range(1, len(orders) + 1)):
                raise ValueError("frame ordering is invalid")
        return cls(tuple(records), frame_root)

    @staticmethod
    def _record(item: dict[str, Any]) -> FrameRecord:
        identifier = str(item["id"]).casefold()
        name = str(item["name"]).strip()
        category = str(item["category"]).strip()
        file_name = str(item["file_name"])
        sha256 = str(item["hash"]).casefold()
        source = str(item["source"])
        if not _IDENTIFIER.fullmatch(identifier):
            raise ValueError("invalid frame ID")
        if not name or len(name) > 100:
            raise ValueError("invalid frame name")
        if category not in CATEGORY_ORDER:
            raise ValueError("invalid frame category")
        if not _FILE_NAME.fullmatch(file_name) or "/" in file_name or "\\" in file_name:
            raise ValueError("invalid frame file name")
        if not _HASH.fullmatch(sha256):
            raise ValueError("invalid frame hash")
        if source not in {"nullbat", "karuta"}:
            raise ValueError("invalid frame source")
        cost = _frame_cost(item["cost"])
        requirements = str(item["requirements"] or "").strip()
        if len(requirements) > 500:
            raise ValueError("frame requirements are too long")
        if not isinstance(item["transparent"], bool):
            raise ValueError("invalid frame transparency")
        if not isinstance(item["category_order"], int) or not isinstance(item["order"], int):
            raise ValueError("invalid frame ordering")
        if int(item["order"]) < 1:
            raise ValueError("invalid frame ordering")
        scale = _number(item["image_scale"], default=1.0)
        offset_x = _number(item["image_offset_x"], default=0.0)
        offset_y = _number(item["image_offset_y"], default=0.0)
        if not 0.25 <= scale <= 4 or not -200 <= offset_x <= 200 or not -200 <= offset_y <= 200:
            raise ValueError("invalid frame image transform")
        return FrameRecord(
            identifier=identifier,
            name=name,
            category=category,
            category_order=int(item["category_order"]),
            order=int(item["order"]),
            requirements=requirements,
            transparent=item["transparent"],
            file_name=file_name,
            sha256=sha256,
            image_scale=scale,
            image_offset_x=offset_x,
            image_offset_y=offset_y,
            source=source,
            cost=cost,
        )

    def get(self, identifier: str) -> FrameRecord | None:
        return self._by_id.get(identifier.casefold())

    def filter(self, *, category: str = "All", query: str = "") -> tuple[FrameRecord, ...]:
        normalized_query = normalize_search(query)
        return tuple(
            record
            for record in self.records
            if (category == "All" or record.category == category)
            and (not normalized_query or normalized_query in normalize_search(record.name))
        )

    def verify_assets(self) -> None:
        for record in self.records:
            data = (self.asset_root / record.file_name).read_bytes()
            if hashlib.sha256(data).hexdigest() != record.sha256:
                raise ValueError(f"bundled frame hash mismatch: {record.file_name}")


def normalize_search(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _number(value: Any, *, default: float) -> float:
    if value is None:
        return default
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError("non-finite frame transform")
    return converted


def _frame_cost(value: Any) -> FrameCost:
    if not isinstance(value, dict):
        raise ValueError("invalid frame cost")
    kind = value.get("kind")
    source = value.get("source")
    as_of = value.get("as_of")
    if source not in _COST_SOURCES or not isinstance(as_of, str) or not _DATE.fullmatch(as_of):
        raise ValueError("invalid frame cost provenance")
    try:
        date.fromisoformat(as_of)
    except ValueError as exc:
        raise ValueError("invalid frame cost date") from exc
    if kind == "bits":
        if set(value) != {"kind", "materials", "source", "as_of"}:
            raise ValueError("invalid bit cost shape")
        raw_materials = value["materials"]
        if not isinstance(raw_materials, list) or len(raw_materials) != 2:
            raise ValueError("invalid bit cost materials")
        materials: list[BitMaterial] = []
        names: set[str] = set()
        for material in raw_materials:
            if not isinstance(material, dict) or set(material) != {"name", "amount"}:
                raise ValueError("invalid bit cost material")
            name = material["name"]
            amount = material["amount"]
            if (
                not isinstance(name, str)
                or not _BIT_NAME.fullmatch(name)
                or not isinstance(amount, int)
                or isinstance(amount, bool)
                or not 1 <= amount <= 1_000_000
                or name.casefold() in names
            ):
                raise ValueError("invalid bit cost material")
            names.add(name.casefold())
            materials.append(BitMaterial(name=name, amount=amount))
        if source != "nullbat":
            raise ValueError("invalid bit cost source")
        return FrameCost(
            kind="bits",
            source=source,
            as_of=as_of,
            materials=tuple(materials),
        )
    if kind == "tickets":
        if set(value) != {"kind", "low", "high", "source", "as_of"}:
            raise ValueError("invalid ticket cost shape")
        low = _cost_number(value["low"])
        high = _cost_number(value["high"])
        if low > high:
            raise ValueError("invalid ticket cost range")
        return FrameCost(
            kind="tickets",
            source=source,
            as_of=as_of,
            ticket_low=low,
            ticket_high=high,
        )
    if kind == "unknown":
        if set(value) != {"kind", "source", "as_of"}:
            raise ValueError("invalid unknown cost shape")
        return FrameCost(kind="unknown", source=source, as_of=as_of)
    raise ValueError("invalid frame cost kind")


def _cost_number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("invalid ticket cost")
    converted = float(value)
    if not math.isfinite(converted) or not 0 < converted <= 1_000_000:
        raise ValueError("invalid ticket cost")
    return converted


def _format_number(value: float | None) -> str:
    if value is None:
        raise ValueError("missing cost value")
    return f"{value:g}"


def _format_compact_amount(value: int) -> str:
    if value >= 1_000 and value % 100 == 0:
        return f"{value / 1_000:g}k"
    return f"{value:,}"
