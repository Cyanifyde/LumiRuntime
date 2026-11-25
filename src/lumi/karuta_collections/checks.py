from __future__ import annotations

import csv
import io
from collections import Counter

from .models import complete_name
from .models import Card, clean_text

MAX_CSV_BYTES = 25 * 1024 * 1024
MAX_CARDS = 100_000
ALIASES = {
    "code": "code",
    "cardcode": "code",
    "number": "print_num",
    "print": "print_num",
    "printnum": "print_num",
    "printnumber": "print_num",
    "edition": "edition",
    "editionnumber": "edition",
    "character": "character",
    "charactername": "character",
    "series": "series",
    "seriesname": "series",
    "quality": "quality",
    "condition": "quality",
    "wishlists": "wishlist",
    "wishlist": "wishlist",
    "wishlistcount": "wishlist",
    "workereffort": "effort",
    "workeffort": "effort",
    "effort": "effort",
}
REQUIRED = {"code", "print_num", "edition", "character", "series", "quality"}
QUALITY = {"damaged": 0, "poor": 1, "good": 2, "excellent": 3, "mint": 4}


class ImportError(ValueError):
    pass


def number(value, maximum, *, prefix="", optional=False):
    if optional and value == "":
        return None
    value = str(value).strip().removeprefix(prefix).replace(",", "")
    if not value.isascii() or not value.isdecimal() or not 0 <= int(value) <= maximum:
        raise ValueError("invalid number")
    return int(value)


def parse_csv(data: bytes):
    if not data or len(data) > MAX_CSV_BYTES or b"\x00" in data:
        raise ImportError("Invalid CSV.")
    try:
        reader = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""), strict=True)
        headers = next(reader)
        keys = [
            "".join(
                c for c in value.strip().casefold() if c in "abcdefghijklmnopqrstuvwxyz0123456789"
            )
            for value in headers
        ]
        mapped = [ALIASES.get(key, key) for key in keys]
        if (
            not 1 <= len(keys) <= 128
            or any(not key for key in keys)
            or len(set(mapped)) != len(mapped)
            or not REQUIRED <= set(mapped)
        ):
            raise ImportError("Invalid CSV columns.")
        records, codes = [], set()
        for row in reader:
            if not row or all(value == "" for value in row):
                continue
            if len(records) >= MAX_CARDS:
                raise ImportError("CSV exceeds 100,000 cards.")
            if len(row) != len(headers) or any(len(value) > 4096 for value in row):
                raise ImportError(f"Invalid CSV (row {reader.line_num:,}).")
            raw = dict(zip(mapped, row, strict=True))
            try:
                quality = QUALITY.get(raw["quality"].strip().casefold())
                if quality is None:
                    quality = number(raw["quality"], 4)
                record = dict(
                    code=raw["code"].strip().casefold(),
                    character=clean_text(raw["character"]),
                    series=clean_text(raw["series"]),
                    edition=number(raw["edition"], 100, prefix="E"),
                    print_num=number(raw["print_num"], 2_147_483_647, prefix="#"),
                    quality=quality,
                    wishlist=number(raw.get("wishlist", ""), 2_147_483_647, optional=True),
                    effort=number(raw.get("effort", ""), 2_147_483_647, optional=True),
                    extra={
                        header: value if value != "" else None
                        for header, value in zip(headers, row, strict=True)
                    },
                )
                Card(
                    **{
                        key: record[key]
                        for key in (
                            "code",
                            "character",
                            "series",
                            "edition",
                            "print_num",
                            "quality",
                            "wishlist",
                            "effort",
                        )
                    }
                )
                if not complete_name(record["character"]) or not complete_name(record["series"]):
                    raise ValueError("incomplete identity")
            except ValueError as error:
                raise ImportError(f"Invalid CSV (row {reader.line_num:,}).") from error
            if record["code"] in codes:
                raise ImportError(f"Duplicate card: {record['code']}.")
            codes.add(record["code"])
            records.append(record)
        if not records:
            raise ImportError("CSV is empty.")
        return records
    except (UnicodeError, csv.Error, StopIteration) as error:
        raise ImportError("Invalid CSV.") from error


def identity_key(record):
    character = record.get("character", "")
    series = record.get("series", "")
    if not complete_name(character) or not complete_name(series):
        return None
    return clean_text(character).casefold(), clean_text(series).casefold()


def sort_key(record):
    key = identity_key(record) or ("", "")
    return key[1], key[0], record["edition"], record["print_num"], record["code"]


def distinct(records):
    return list({record["code"]: record for record in records}.values())


def duplicates(records):
    records = distinct(records)
    counts = Counter((identity_key(row), row["edition"]) for row in records if identity_key(row))
    return sorted(
        (
            row
            for row in records
            if identity_key(row) and counts[identity_key(row), row["edition"]] >= 2
        ),
        key=sort_key,
    )


def matches(records, wishes):
    keys = {identity_key(row) for row in wishes if identity_key(row)}
    return sorted((row for row in distinct(records) if identity_key(row) in keys), key=sort_key)


def escape(value):
    value = str(value).replace("\\", "\\\\")
    for character in "`*_~|":
        value = value.replace(character, "\\" + character)
    return value


def dupe_row(card):
    metrics = []
    if card.get("wishlist") is not None:
        metrics.append(f"`♡{card['wishlist']:,}`")
    if card.get("effort") is not None:
        metrics.append(f"`✧{card['effort']:,}`")
    metrics.append(f"**`{card['code']}`**")
    if card.get("quality") is not None:
        metrics.append("`" + "★" * card["quality"] + "☆" * (4 - card["quality"]) + "`")
    metrics.extend((f"`#{card['print_num']:,}`", f"`◈{card['edition']}`"))
    return "└ " + " · ".join(metrics)


def pages(records, *, wishlist=False):

    output, lines, active = [], [], None
    for card in sorted(records, key=sort_key):
        key = identity_key(card)
        heading = f"**{escape(card['series'])}** · **{escape(card['character'])}**"
        row = (
            f"`{card['code']}` · `◈{card['edition']}` · `#{card['print_num']:,}`"
            if wishlist
            else dupe_row(card)
        )
        additions = [row] if active == key else [heading, row]
        candidate = "\n".join([*lines, *additions])
        if lines and (
            len(candidate.encode("utf-16-le")) // 2 > 1800
            or len(lines) + len(additions) > 28
            or len(candidate.encode()) > 2400
        ):
            output.append("\n".join(lines))
            lines = []
            active = None
            additions = [heading, row]
        lines.extend(additions)
        active = key
    if lines:
        output.append("\n".join(lines))
    return output
