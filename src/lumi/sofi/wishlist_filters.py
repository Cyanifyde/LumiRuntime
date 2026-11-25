from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from .wishlist_store import WishlistEntry

HEAD = re.compile(r"(?i)(?<!\S)([a-z][a-z_]*|3d|#)\s*(==|!=|<=|>=|[=:<>])\s*")
IDENTITY = re.compile(r"(?:<@!?(\d{17,20})>|(\d{17,20}))")
ALIASES = {


    **dict.fromkeys(("n", "number", "print", "p", "#", "g", "gen", "generation"), "g"),
    **dict.fromkeys(("c", "character", "name"), "n"),
    **dict.fromkeys(("s", "series"), "s"),
    **dict.fromkeys(("e", "edition", "v", "ver", "version"), "v"),
    **dict.fromkeys(("w", "wl", "wishlist", "wishlists"), "wl"),
    **dict.fromkeys(("t", "tag"), "t"),
    **dict.fromkeys(("level", "lvl"), "lvl"),
    **dict.fromkeys(("m", "morph", "morphed"), "m"),
    **dict.fromkeys(("f", "framed"), "f"),
    **dict.fromkeys(("3d", "threed"), "3d"),
    **dict.fromkeys(("fr", "frame"), "fr"),
    **dict.fromkeys(("glo", "glow"), "glow"),
    **dict.fromkeys(("lay", "layer"), "layer"),
    **dict.fromkeys(("bg", "background"), "bg"),
    **dict.fromkeys(("el", "element"), "e"),
    **dict.fromkeys(("cat", "category"), "cat"),
    **dict.fromkeys(("ev", "event"), "ev"),
    **dict.fromkeys(("atk", "attack"), "atk"),
    **dict.fromkeys(("def", "defense", "defence"), "def"),
    **dict.fromkeys(("spd", "speed"), "spd"),
    **dict.fromkeys(("hp", "health"), "hp"),
    **dict.fromkeys(("ts", "totalstats"), "ts"),
    **dict.fromkeys(("bs", "basestats"), "bs"),
    **dict.fromkeys(("sm", "smp", "spm", "specialmorph"), "sm"),
    **dict.fromkeys(("dmg", "damage", "damaged"), "dmg"),
    **dict.fromkeys(("server", "ser"), "ser"),
    **dict.fromkeys(("grabber", "grabbed", "gr"), "grabbed"),
    **dict.fromkeys(("dropper", "dropped", "dr"), "dropped"),
    **{
        field: field
        for field in (
            "code",
            "page",
            "date",
            "combo",
            "gif",
            "glowed",
            "layered",
            "font",
            "ff",
            "fc",
            "bgd",
            "scratched",
            "locked",
        )
    },
}
NUMERIC = frozenset(("g", "v", "wl", "lvl", "atk", "def", "spd", "hp", "ts", "bs", "page"))
BOOLEANS = frozenset(
    (
        "m",
        "f",
        "3d",
        "combo",
        "gif",
        "glowed",
        "layered",
        "ff",
        "fc",
        "bgd",
        "scratched",
        "locked",
        "dmg",
    )
)
SNOWFLAKES = frozenset(("ser", "grabbed", "dropped"))
TRUE = frozenset(("true", "t", "yes", "y", "1"))
FALSE = frozenset(("false", "f", "no", "n", "0"))
DIRECTIONS = {"a": "a", "asc": "a", "ascending": "a", "d": "d", "desc": "d", "descending": "d"}
NATIVE_KEYS = frozenset(ALIASES) | frozenset(ALIASES.values()) | {"d", "o", "order"}


class FilterError(ValueError):
    pass


@dataclass(frozen=True)
class CollectionFilters:
    clauses: tuple[str, ...] = ()
    sort_field: str | None = None
    direction: str | None = None

    @property
    def stages(self) -> tuple[str, ...]:


        result = self.clauses
        if self.direction:


            result += (f"order {self.sort_field or 'g'}>=0 o={self.direction}",)
        return result

    @property
    def native(self) -> str:
        return " / ".join(self.stages)


def _safe_text(raw: str) -> str:
    if (
        not raw
        or len(raw) > 240
        or any(ord(char) < 32 or ord(char) == 127 for char in raw)
        or " /" in raw
        or any(match[1].casefold() in NATIVE_KEYS for match in HEAD.finditer(" " + raw))
    ):
        raise FilterError("Value cannot be represented safely in an sc filter.")
    return raw


def _value(field: str, op: str, raw: str) -> str:
    if field in NUMERIC:
        if not re.fullmatch(r"[0-9]{1,10}(?:,[0-9]{1,10})?", raw):
            raise FilterError("Use a number or min,max range.")
        values = [int(part) for part in raw.split(",")]
        if max(values) > 2_147_483_647 or (field == "page" and min(values) < 1):
            raise FilterError("Number out of range.")
        if len(values) == 2 and (op != "=" or values[0] > values[1]):
            raise FilterError("Use an ascending range with : or =.")
        return ",".join(str(part) for part in values)
    if field == "date":
        dates = raw.split(",")
        try:
            parsed = [datetime.strptime(value, "%d/%m/%Y") for value in dates]
        except ValueError as exc:
            raise FilterError("Use DD/MM/YYYY.") from exc
        if len(parsed) > 2 or (len(parsed) == 2 and (op != "=" or parsed[0] > parsed[1])):
            raise FilterError("Use an ascending date range with : or =.")
        return ",".join(value.strftime("%d/%m/%Y") for value in parsed)
    if field in SNOWFLAKES:
        match = IDENTITY.fullmatch(raw)
        if match is None or op not in {"=", "!="}:
            raise FilterError("Use a user/server ID.")
        return str(int(match[1] or match[2]))
    if field in BOOLEANS or (field in {"ev", "sm"} and raw.casefold() in TRUE | FALSE):
        if raw.casefold() not in TRUE | FALSE or op not in {"=", "!="}:
            raise FilterError("Use true or false.")
        return "true" if raw.casefold() in TRUE else "false"
    if op not in {"=", "==", "!="}:
        raise FilterError("Text filters use :, =, == or !=.")
    return _safe_text(raw)


def parse_filters(text: str) -> CollectionFilters:
    if len(text) > 1000 or any(ord(char) < 32 and char != "\t" for char in text):
        raise FilterError("Filters are too long or contain a new line.")
    text = text.strip()
    if not text:
        return CollectionFilters()
    matches = list(HEAD.finditer(text))
    if not matches or text[: matches[0].start()].strip() or len(matches) > 24:
        raise FilterError("Use field:value filters.")
    clauses: list[str] = []
    sort_field = direction = None
    for index, match in enumerate(matches):
        key, op = match[1].casefold(), match[2]
        raw = text[
            match.end() : matches[index + 1].start() if index + 1 < len(matches) else len(text)
        ].strip()
        op = "=" if op == ":" else op
        if key in {"o", "order"}:
            if direction is not None or op != "=":
                raise FilterError("Use one order filter.")
            words = raw.casefold().split()
            if len(words) == 1 and words[0] in DIRECTIONS:
                direction = DIRECTIONS[words[0]]
                continue
            if not 1 <= len(words) <= 2:
                raise FilterError("Use o:n, o:wl or o:<stat>.")
            sort_field = ALIASES.get(words[0])
            if sort_field in {"n", "s"}:
                raise FilterError("Use Sofi's Name or Series sort menu.")
            if sort_field not in NUMERIC - {"page"}:
                raise FilterError("Use o:n, o:wl or o:<stat>.")
            direction = "a" if sort_field in {"g", "v"} else "d"
            if len(words) == 2:
                if words[1] not in DIRECTIONS:
                    raise FilterError("Use asc or desc.")
                direction = DIRECTIONS[words[1]]
            continue
        field = ALIASES.get(key)
        if field is None:
            raise FilterError(f"Unsupported filter: {key}.")
        if op == "==" and (
            field in NUMERIC | BOOLEANS | SNOWFLAKES | {"date"}
            or (field in {"ev", "sm"} and raw.casefold() in TRUE | FALSE)
        ):
            op = "="
        if len(raw) >= 2 and raw[0] in "\"'" and raw[-1] == raw[0]:
            raw = raw[1:-1]
        clauses.append(field + op + _value(field, op, raw))
    return CollectionFilters(tuple(clauses), sort_field, direction)


def collection_commands(
    target_id: int, entries: tuple[WishlistEntry, ...], filters: CollectionFilters
) -> tuple[str, ...]:
    if not 10**16 <= target_id < 2**64 or not 0 < len(entries) <= 100:
        raise FilterError("Invalid collection target or wishlist.")
    prefix = f"sc {target_id} "


    tail = filters.stages or ("order g>=0 o=a",)
    if len(tail) + 2 > 5:
        raise FilterError("Too many filter stages.")
    wanted: list[tuple[str, str]] = []
    for entry in entries:
        name, series = _safe_text(entry.name), _safe_text(entry.series)
        if (name, series) not in wanted:
            wanted.append((name, series))

    def render(batch: list[tuple[str, str]]) -> str:


        series = dict.fromkeys(series for _name, series in batch)
        names = dict.fromkeys(name for name, _series in batch)
        stages = (
            " ".join(f"s=={value}" for value in series),
            " ".join(f"n=={value}" for value in names),
            *tail,
        )
        return " / ".join(stages)

    batches: list[str] = []
    chunk: list[tuple[str, str]] = []
    for entry in wanted:
        if len(render([*chunk, entry])) > 1000:
            if not chunk:
                raise FilterError("Filters are too long for one wishlist entry.")
            batches.append(prefix + render(chunk))
            chunk = []
        chunk.append(entry)
        if len(render(chunk)) > 1000:
            raise FilterError("Filters are too long for one wishlist entry.")
    if chunk:
        batches.append(prefix + render(chunk))
    return tuple(batches)
