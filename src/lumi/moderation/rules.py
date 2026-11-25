from __future__ import annotations

import hashlib
import json
import re
import unicodedata

MAX_REASON = 400
SESSION_PREFIX = "lumi:moderation:session:"
SESSION_TTL = 900_000
PERMISSIONS = {
    "kick": "kick_members",
    "warn": "moderate_members",
    "warnings": "moderate_members",
    "unwarn": "moderate_members",
    "timeout": "moderate_members",
    "untimeout": "moderate_members",
    "unban": "ban_members",
    "purge": "manage_messages",
    "rename": "manage_nicknames",
    "role": "manage_roles",
}
UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
TOKEN = re.compile(r"([0-9]{1,8})([smhdw])", re.I)


class ModerationError(ValueError):
    pass


def reason(value):
    value = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(
        "".join(
            c for c in value if c.isspace() or unicodedata.category(c) not in {"Cc", "Cf", "Cs"}
        ).split()
    )[:MAX_REASON]


def duration(value):
    position, total, seen = 0, 0, set()
    for match in TOKEN.finditer(value):
        unit = match[2].lower()
        if match.start() != position or unit in seen:
            raise ModerationError("Use a duration such as 20m or 1h30m.")
        seen.add(unit)
        total += int(match[1]) * UNITS[unit]
        position = match.end()
    if not value or position != len(value) or not 60 <= total <= 28 * 86400:
        raise ModerationError("Duration: 1 minute to 28 days.")
    return total


def identifier(guild, message, command):
    return hashlib.sha256(json.dumps([guild, message, command]).encode()).hexdigest()
