from __future__ import annotations

import re
from dataclasses import dataclass

MIN_WAIT = 60_000
MIN_REPEAT = 900_000
MAX_WAIT = 365 * 86_400_000
MAX_TEXT = 1000
UNITS = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
ALIASES = {
    **dict.fromkeys(("s", "sec", "secs", "second", "seconds"), "s"),
    **dict.fromkeys(("m", "min", "mins", "minute", "minutes"), "m"),
    **dict.fromkeys(("h", "hs", "hr", "hrs", "hour", "hours"), "h"),
    **dict.fromkeys(("d", "ds", "day", "days"), "d"),
    **dict.fromkeys(("w", "ws", "wk", "wks", "week", "weeks"), "w"),
}
TOKEN = re.compile(
    r"([0-9]+)\s*(" + "|".join(sorted(ALIASES, key=len, reverse=True)) + r")(?![a-z])",
    re.IGNORECASE,
)
SEPARATOR = re.compile(r"\s*(?:,\s*(?:and\s+)?|and\s+)?", re.IGNORECASE)
IDENTIFIER = re.compile(r"[a-f0-9]{8}")


class ReminderError(ValueError):
    pass


class ReminderSyntaxError(ReminderError):
    pass


@dataclass(frozen=True, slots=True)
class Command:
    kind: str
    identifier: str = ""
    duration: int = 0
    repeating: bool = False
    text: str = ""
    page: int = 1
    due_ms: int = 0


def validate_duration(total: int, repeating: bool) -> int:
    if type(total) is not int or total > MAX_WAIT:
        raise ReminderError("Use a duration between 1 minute and 365 days.")
    if total < (MIN_REPEAT if repeating else MIN_WAIT):
        raise ReminderError(
            "Repeats must be at least 15 minutes apart."
            if repeating
            else "Reminders must be at least 1 minute away."
        )
    return total


def validate_text(text: str) -> str:
    if (
        not 1 <= len(text.strip()) <= MAX_TEXT
        or len(text) > MAX_TEXT
        or any((ord(c) < 32 and c not in "\n\t") or 0xD800 <= ord(c) <= 0xDFFF for c in text)
    ):
        raise ReminderError("Reminder text must be 1–1,000 characters.")
    return text


def _duration_prefix(raw: str) -> tuple[int, int]:
    position, total, seen = 0, 0, set()
    token = TOKEN.match(raw)
    if token is None:
        raise ReminderSyntaxError("Use a duration such as 20m, 2hs or 1h30m.")
    while token:
        unit = ALIASES[token[2].lower()]
        if unit in seen:
            raise ReminderError("Use a duration such as 20m, 2h or 1h30m.")
        seen.add(unit)
        total += int(token[1]) * UNITS[unit]
        position = token.end()
        separator = SEPARATOR.match(raw, position)
        token = TOKEN.match(raw, separator.end()) if separator else None
    return total, position


def duration(raw: str, repeating: bool) -> int:
    total, end = _duration_prefix(raw.strip())
    if end != len(raw.strip()):
        raise ReminderError("Use a duration between 1 minute and 365 days.")
    return validate_duration(total, repeating)


def command_head(raw: str, *, preserve_body: bool = False) -> tuple[str, str, str]:

    raw = raw.lstrip() if preserve_body else raw.strip()
    if not raw:
        return "list", "", ""
    if len(raw) > 1200:
        raise ReminderError("Reminder text must be 1–1,000 characters.")
    if any((ord(c) < 32 and c not in "\n\t") or 0xD800 <= ord(c) <= 0xDFFF for c in raw):
        raise ReminderError("Invalid reminder text.")
    words = raw.split(maxsplit=1)
    kind = words[0].casefold()
    rest = words[1] if len(words) == 2 else ""
    if kind == "help":
        if rest:
            raise ReminderError("Use remindme help.")
        return kind, "", ""
    if kind == "list":
        if rest and (not rest.isascii() or not rest.isdigit() or len(rest) > 6 or int(rest) < 1):
            raise ReminderError("Use remindme list or remindme list 2.")
        return kind, "", rest
    identifier = ""
    if kind in {"edit", "cancel"}:
        parts = rest.split(maxsplit=1)
        identifier = parts[0].lower() if parts else ""
        if not IDENTIFIER.fullmatch(identifier):
            raise ReminderError("Use the eight-character reminder ID.")
        rest = parts[1] if len(parts) == 2 else ""
        if kind == "cancel":
            if rest:
                raise ReminderError("Use remindme cancel <id>.")
            return kind, identifier, ""
        raw = rest
    else:
        kind = "create"
    if not raw:
        raise ReminderError(
            "Include a duration and reminder text, such as remindme 2h check cards."
        )
    return kind, identifier, raw


def parse(raw: str) -> Command:
    kind, identifier, raw = command_head(raw)
    if kind in {"help", "cancel", "list"}:
        return Command(kind, identifier, page=int(raw) if raw else 1)
    parts = raw.split(maxsplit=1)
    repeating = bool(parts and parts[0].casefold() == "every")
    if repeating or (parts and parts[0].casefold() == "in"):
        raw = parts[1] if len(parts) == 2 else ""
    if not raw:
        raise ReminderError(
            "Include a duration and reminder text, such as remindme 2h check cards."
        )
    if re.match(r"-\s*\d", raw):
        raise ReminderError("Use a duration between 1 minute and 365 days.")
    wait, end = _duration_prefix(raw)

    if end < len(raw) and not raw[end].isspace():
        raise ReminderSyntaxError("Use a duration such as 20m, 2hs or 1h30m.")
    wait = validate_duration(wait, repeating)
    text = raw[end:].strip()
    if not text:
        raise ReminderError(
            "Include a duration and reminder text, such as remindme 2h check cards."
        )
    text = validate_text(text)
    return Command(kind, identifier, wait, repeating, text)


def pretty_duration(milliseconds: int) -> str:
    pieces = []
    for unit in ("w", "d", "h", "m", "s"):
        amount, milliseconds = divmod(milliseconds, UNITS[unit])
        if amount:
            pieces.append(f"{amount}{unit}")
    return "".join(pieces)
