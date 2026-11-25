from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .models import search_text

MAX_QUERY = 1000
ALIASES = {
    "c": "character",
    "character": "character",
    "s": "series",
    "series": "series",
    "e": "edition",
    "edition": "edition",
    "p": "print",
    "print": "print",
    "ink": "ink",
}
TAGS = {*ALIASES, "o", "o0", "o1"}
HEAD = re.compile(r"([a-z][a-z0-9]*)\s*(<=|>=|:|=|<|>)\s*", re.IGNORECASE)
NUMBER_TEXT = r"\d+(?:\.\d+)?[kK]?"
NUMBER = re.compile(NUMBER_TEXT + r"\Z")
LEADING_BOUND = re.compile(
    rf"({NUMBER_TEXT})\s*(<=|>=|=|<|>)\s*(edition|print|ink|e|p)(?![a-z0-9])",
    re.IGNORECASE,
)
FIRST_AMOUNT = re.compile(rf"\s*({NUMBER_TEXT})\s*")
NEXT_BOUND = re.compile(rf"\s*(<=|>=|=|<|>)\s*({NUMBER_TEXT})\s*")
REVERSED_OPERATOR = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "="}


class FilterError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Predicate:
    field: str
    operator: str
    value: int


@dataclass(frozen=True, slots=True)
class SketchQuery:
    character: str = ""
    series: str = ""
    editions: tuple[int, ...] = ()
    predicates: tuple[Predicate, ...] = ()
    order: str = "ink"
    descending: bool = True


def _head(raw: str, position: int):
    leading = LEADING_BOUND.match(raw, position)
    if leading:
        return (
            leading.end(),
            leading[3].casefold(),
            REVERSED_OPERATOR[leading[2]],
            leading[1],
        )
    match = HEAD.match(raw, position)
    return (match.end(), match[1].casefold(), match[2], "") if match else None


def _tokens(raw: str) -> list[tuple[str, str, str]]:
    if len(raw) > MAX_QUERY or "\n" in raw or "\r" in raw or "\x00" in raw:
        raise FilterError("Search is too long or contains an invalid character.")
    raw = raw.strip()
    if not raw:
        return []
    first = _head(raw, 0)
    if first is None:
        raise FilterError("Use a filter, for example c:Kishibe or ink>=20k.")
    if first[1] not in TAGS:
        raise FilterError(f"Unknown filter: {first[1]}.")
    starts, quote, escaped, skip_until = [], "", False, 0
    for i, char in enumerate(raw):
        if i < skip_until:
            continue
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'"}:

            if i == 0 or raw[i - 1].isspace() or raw[i - 1] in ":=":
                quote = char
            continue
        if i == 0 or raw[i - 1].isspace():
            head = _head(raw, i)
            if head and (head[1] in TAGS or head[1] == "code"):
                starts.append((i, *head))

                skip_until = head[0]
    if quote:
        raise FilterError("Unclosed quotation mark.")
    result = []
    for n, (_, end, tag, op, leading_amount) in enumerate(starts):
        value = raw[end : starts[n + 1][0] if n + 1 < len(starts) else len(raw)].strip()
        if leading_amount:
            if value and value[0] not in "<>=":
                raise FilterError(f"Invalid {ALIASES[tag]} comparison.")
            value = leading_amount + value
        if tag == "code":
            raise FilterError("Unknown filter: code.")
        if not value:
            raise FilterError(f"Enter a value for {tag}.")
        if value[0] in {'"', "'"}:
            if len(value) < 2 or value[-1] != value[0]:
                raise FilterError("A quoted value must end before the next filter.")
            mark = value[0]
            value = value[1:-1].replace("\\" + mark, mark).replace("\\\\", "\\")
        if not value.strip():
            raise FilterError(f"Enter a value for {tag}.")
        result.append((tag, op, value))
    if len(result) > 24:
        raise FilterError("Too many filters.")
    return result


def _number(raw: str, field: str) -> int:
    if not NUMBER.fullmatch(raw.strip()):
        raise FilterError(f"Invalid {field} amount.")
    raw = raw.strip().casefold()
    try:
        value = Decimal(raw[:-1]) * 1000 if raw.endswith("k") else Decimal(raw)
    except InvalidOperation as exc:
        raise FilterError(f"Invalid {field} amount.") from exc
    maximum, minimum = (
        (100, 1) if field == "edition" else (2_147_483_647, 0 if field == "ink" else 1)
    )
    if value != value.to_integral_value() or not minimum <= value <= maximum:
        raise FilterError(
            f"{field.capitalize()} must be a whole number from {minimum} to {maximum}."
        )
    return int(value)


def _bounds(field: str, operator: str, value: str) -> list[Predicate]:
    first = FIRST_AMOUNT.match(value)
    if first is None:
        raise FilterError(f"Invalid {field} amount.")
    predicates = [Predicate(field, "=" if operator == ":" else operator, _number(first[1], field))]
    position = first.end()
    while position < len(value):
        match = NEXT_BOUND.match(value, position)
        if match is None:
            raise FilterError(f"Invalid {field} comparison.")
        predicates.append(Predicate(field, match[1], _number(match[2], field)))
        if len(predicates) > 24:
            raise FilterError("Too many comparisons.")
        position = match.end()
    return predicates


def parse_query(raw: str) -> SketchQuery:
    names, editions, predicates = {}, (), []
    order, descending, has_order = "ink", True, False
    for tag, operator, value in _tokens(raw):
        if tag in {"o", "o0", "o1"}:
            if operator not in {":", "="}:
                raise FilterError("Use o0:ink or o1:ink to order results.")
            if has_order:
                raise FilterError("Use only one ordering tag.")
            order = ALIASES.get(value.casefold().strip(), "")
            if not order:
                raise FilterError("Order by ink, print, edition, character or series.")
            descending = tag == "o1" or (tag == "o" and order in {"ink", "edition"})
            has_order = True
            continue
        field = ALIASES[tag]
        if field in {"character", "series"}:
            if operator not in {":", "="}:
                raise FilterError(f"Use {tag}:name or {tag}=name.")
            if field in names:
                raise FilterError(f"Use only one {field} filter.")
            if len(value) > 240:
                raise FilterError(f"{field.capitalize()} is too long.")
            names[field] = search_text(value)
            continue
        if "," in value:
            if field != "edition" or operator not in {":", "="} or editions:
                raise FilterError(
                    "Comma-separated values are only supported for one edition filter."
                )
            parts = value.split(",")
            if len(parts) > 100:
                raise FilterError("Too many editions.")
            editions = tuple(sorted({_number(part, field) for part in parts}))
        elif ".." in value:
            if operator not in {":", "="} or value.count("..") != 1:
                raise FilterError("Use an inclusive range, for example ink:20k..30k.")
            low, high = (_number(part, field) for part in value.split(".."))
            if low > high:
                raise FilterError("Range minimum must not exceed its maximum.")
            predicates.extend((Predicate(field, ">=", low), Predicate(field, "<=", high)))
        else:
            predicates.extend(_bounds(field, operator, value))

    for field in ("edition", "print", "ink"):
        low, high = (1 if field != "ink" else 0), (100 if field == "edition" else 2_147_483_647)
        for p in predicates:
            if p.field != field:
                continue
            if p.operator in {"=", ">", ">="}:
                low = max(low, p.value + int(p.operator == ">"))
            if p.operator in {"=", "<", "<="}:
                high = min(high, p.value - int(p.operator == "<"))
        if low > high or (
            field == "edition" and editions and not any(low <= n <= high for n in editions)
        ):
            raise FilterError(f"The {field} filters contradict each other.")
    return SketchQuery(
        names.get("character", ""),
        names.get("series", ""),
        editions,
        tuple(predicates),
        order,
        descending,
    )


def matches(query: SketchQuery, record) -> bool:
    for field in ("character", "series"):
        value = getattr(query, field)
        if value and value != search_text(getattr(record, field)):
            return False
    if query.editions and record.edition not in query.editions:
        return False
    for p in query.predicates:
        actual = getattr(record, "print_num" if p.field == "print" else p.field)
        if not {
            "=": actual == p.value,
            "<": actual < p.value,
            "<=": actual <= p.value,
            ">": actual > p.value,
            ">=": actual >= p.value,
        }[p.operator]:
            return False
    return True


def ordered(query: SketchQuery, records: list) -> list:

    rows = sorted((r for r in records if matches(query, r)), key=lambda r: r.code)
    attr = "print_num" if query.order == "print" else query.order
    return sorted(
        rows,
        key=lambda r: (
            search_text(getattr(r, attr))
            if query.order in {"character", "series"}
            else getattr(r, attr)
        ),
        reverse=query.descending,
    )
