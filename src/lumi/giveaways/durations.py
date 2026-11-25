import re

from .models import GiveawayError

UNITS = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
ALIASES = {
    **dict.fromkeys(("s", "sec", "secs", "second", "seconds"), "s"),
    **dict.fromkeys(("m", "min", "mins", "minute", "minutes"), "m"),
    **dict.fromkeys(("h", "hs", "hr", "hrs", "hour", "hours"), "h"),
    **dict.fromkeys(("d", "ds", "day", "days"), "d"),
    **dict.fromkeys(("w", "ws", "wk", "wks", "week", "weeks"), "w"),
}
TOKEN = re.compile(
    r"([0-9]+)\s*(" + "|".join(sorted(ALIASES, key=len, reverse=True)) + r")(?![a-z])", re.I
)
SEPARATOR = re.compile(r"\s*(?:,\s*(?:and\s+)?|and\s+)?", re.I)


def duration(raw, *, minimum=60_000, maximum=30 * 86_400_000):
    raw = str(raw).strip()
    if minimum == 0 and raw in {"", "0"}:
        return 0
    position, total, seen = 0, 0, set()
    token = TOKEN.match(raw)
    if not token or len(raw) > 100:
        raise GiveawayError("Use a duration such as 20m, 2hs or 1h30m.")
    while token:
        unit = ALIASES[token[2].lower()]
        if unit in seen or len(token[1]) > 12:
            raise GiveawayError("Use a duration such as 20m, 2hs or 1h30m.")
        seen.add(unit)
        total += int(token[1]) * UNITS[unit]
        position = token.end()
        token = TOKEN.match(raw, SEPARATOR.match(raw, position).end())
    if position != len(raw) or not minimum <= total <= maximum:
        raise GiveawayError("Duration is outside the allowed range.")
    return total
