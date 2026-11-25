from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import msgspec

from lumi.ai.validation import AIValidationError, validate_value
from lumi.contracts import AIProfileSpec

from .commands import MAX_WAIT, Command, ReminderError, validate_duration, validate_text

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["ready", "clarify", "timezone"]},
        "delay_seconds": {"type": "integer", "minimum": 0, "maximum": MAX_WAIT // 1000},
        "local_time": {"type": "string", "maxLength": 19},
        "interval_seconds": {"type": "integer", "minimum": 0, "maximum": MAX_WAIT // 1000},
        "time_text": {"type": "string", "maxLength": 1200},
        "time_side": {"type": "string", "enum": ["start", "end"]},
    },
    "required": [
        "status",
        "delay_seconds",
        "local_time",
        "interval_seconds",
        "time_text",
        "time_side",
    ],
    "additionalProperties": False,
}

REMINDER_PROFILE = AIProfileSpec(
    key="reminders.parse.v2",
    version="2.0.0",
    model="gemma4:31b",
    system_prompt=(
        "REDACTED"
    ),
    input_schema={
        "type": "object",
        "properties": {
            "request": {"type": "string", "minLength": 1, "maxLength": 1200},
            "now_utc": {"type": "string", "minLength": 1, "maxLength": 40},
            "now_local": {"type": "string", "maxLength": 40},
            "timezone": {"type": "string", "maxLength": 100},
        },
        "required": ["request", "now_utc", "now_local", "timezone"],
        "additionalProperties": False,
    },
    response_mode="json",
    response_schema=RESPONSE_SCHEMA,
    repair_prompt=(
        "REDACTED"
    ),
    deadline_ms=55_000,
    max_attempts=2,
    temperature=0.0,
    thinking=False,
    max_output_tokens=700,
)


class TimezoneRequired(ReminderError):
    pass


class Interpretation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    status: Literal["ready", "clarify", "timezone"]
    delay_seconds: int
    local_time: str
    interval_seconds: int
    time_text: str
    time_side: Literal["start", "end"]


def saved_zone(name: str | None) -> ZoneInfo | None:
    if not name:
        return None
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None


def model_input(raw: str, now: int, zone: ZoneInfo | None) -> dict[str, str]:
    clock = datetime.fromtimestamp(now / 1000, UTC)
    return {
        "request": raw,
        "now_utc": clock.isoformat(),
        "now_local": clock.astimezone(zone).isoformat() if zone else "",
        "timezone": zone.key if zone else "",
    }


def local_due(raw: str, zone: ZoneInfo | None) -> int:
    if zone is None:
        raise TimezoneRequired()
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}", raw):
        raise ReminderError("Couldn't read that time. Use an exact date and time.")
    try:
        local = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ReminderError("Couldn't read that time. Use an exact date and time.") from exc
    candidates = set()
    for fold in (0, 1):
        utc = local.replace(tzinfo=zone, fold=fold).astimezone(UTC)
        if utc.astimezone(zone).replace(tzinfo=None) == local:
            candidates.add(int(utc.timestamp() * 1000))
    if len(candidates) != 1:
        raise ReminderError("That time is skipped or repeated by the clock change. Choose another.")
    return candidates.pop()


def original_notification(raw: str, time_text: str, time_side: str) -> str:

    error = "Couldn't match the timing exactly. Use a time followed by the full reminder text."
    if not time_text or time_text != time_text.strip() or len(time_text) >= len(raw):
        raise ReminderError(error)
    if time_side == "start":
        if not raw.startswith(time_text) or not raw[len(time_text)].isspace():
            raise ReminderError(error)
        text = raw[len(time_text) + 1 :]
        assert raw == time_text + raw[len(time_text)] + text
    elif time_side == "end":

        source = raw.rstrip()
        boundary = len(source) - len(time_text)
        if boundary <= 0 or not source.endswith(time_text) or not source[boundary - 1].isspace():
            raise ReminderError(error)
        text = source[: boundary - 1]
        assert source == text + source[boundary - 1] + time_text
    else:
        raise ReminderError(error)

    return validate_text(text)


def interpret(value, *, kind: str, identifier: str, raw: str, now: int, zone: ZoneInfo | None):
    try:
        checked = validate_value(value, RESPONSE_SCHEMA)
        result = msgspec.convert(checked, type=Interpretation, strict=True)
    except (AIValidationError, msgspec.ValidationError, TypeError, ValueError) as exc:
        raise ReminderError("Couldn't read that reminder. Use a time and reminder text.") from exc
    if result.status == "timezone":
        raise TimezoneRequired()
    if result.status != "ready":
        raise ReminderError("Please give an unambiguous time and reminder text.")
    text = original_notification(raw, result.time_text, result.time_side)
    if result.local_time:
        if result.delay_seconds:
            raise ReminderError("Couldn't read that time. Please rephrase it.")
        due = local_due(result.local_time, zone)
    else:
        due = now + result.delay_seconds * 1000
    wait = validate_duration(due - now, False)
    interval = result.interval_seconds * 1000
    if interval:
        validate_duration(interval, True)
    return Command(kind, identifier, interval or wait, bool(interval), text, due_ms=due)
