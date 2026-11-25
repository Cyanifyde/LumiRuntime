from __future__ import annotations

import hashlib
import re
import unicodedata

FIELDS = {
    "code": "code",
    "series": "series",
    "s": "series",
    "character": "character",
    "c": "character",
    "tag": "tag",
    "t": "tag",
    "number": "print",
    "print": "print",
    "p": "print",
    "#": "print",
    "edition": "edition",
    "e": "edition",
    "quality": "quality",
    "q": "quality",
    "server": "server",
    "wishlist": "wishlist",
    "wishlists": "wishlist",
    "wl": "wishlist",
    "w": "wishlist",
    "effort": "effort",
    "eff": "effort",
    "ef": "effort",
    "level": "level",
    "ascensions": "ascensions",
    "technique": "technique",
    "framed": "framed",
    "f": "framed",
    "dyed": "dyed",
    "d": "dyed",
    "morphed": "morphed",
    "m": "morphed",
    "trimmed": "trimmed",
    "trim": "trimmed",
    "sketched": "sketched",
    "sk": "sketched",
    "grabbed": "grabbed",
    "g": "grabbed",
    "gr": "grabbed",
    "grabber": "grabbed",
    "dropped": "dropped",
    "dr": "dropped",
    "dropper": "dropped",
    "injured": "injured",
    "i": "injured",
    "fighter": "fighter",
}
SORTS = {
    name: field
    for name, field in FIELDS.items()
    if field
    in {
        "code",
        "series",
        "character",
        "tag",
        "print",
        "edition",
        "quality",
        "wishlist",
        "effort",
        "level",
        "ascensions",
        "injured",
    }
}
SORTS.update(date="date", d="date", n="print")
NUMBERS = frozenset(
    {"print", "edition", "quality", "server", "wishlist", "effort", "level", "ascensions"}
)
BOOLEANS = frozenset(
    {"framed", "dyed", "morphed", "trimmed", "sketched", "grabbed", "dropped", "injured", "fighter"}
)
HEAD = re.compile(
    r"(?i)(?<!\S)(?:(?P<sort>sort[ \t]+by)[ \t]+|"
    r"(?P<field>[a-z][a-z_]*|\#)[ \t]*(?P<op>!=|<=|>=|[=:<>]|\bis\b)[ \t]*)"
)
DIRECTIONS = {
    "ascending": "asc",
    "asc": "asc",
    "a": "asc",
    "descending": "desc",
    "desc": "desc",
    "d": "desc",
    "reverse": "reverse",
    "r": "reverse",
}


def collection_metrics(value: str) -> dict[str, int | None]:
    if len(value) > 160:
        raise ValueError("collection metrics exceed limit")
    result = {"wishlist": None, "effort": None}
    for symbol, key in (("♡", "wishlist"), ("✧", "effort")):
        matches = re.findall(
            rf"{symbol}([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]{{1,10}})(?![0-9,])", value
        )
        if matches:
            amounts = {int(raw.replace(",", "")) for raw in matches}
            if len(amounts) != 1 or max(amounts) > 2_147_483_647:
                raise ValueError("conflicting collection metrics")
            result[key] = amounts.pop()
    return result


def _value(field: str, raw: str):
    raw = raw.strip().strip('"').strip("'").strip()
    if field == "server":
        if not re.fullmatch(r"[0-9]{17,20}", raw) or int(raw) >= 2**64:
            raise ValueError("unknown server identifier")
        return int(raw)
    if field in NUMBERS:
        if field == "quality" and raw.casefold() in {
            "damaged",
            "poor",
            "good",
            "excellent",
            "mint",
        }:
            return ["damaged", "poor", "good", "excellent", "mint"].index(raw.casefold())
        if not re.fullmatch(r"(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]{1,10})", raw):
            raise ValueError("unknown numeric KC syntax")
        value = int(raw.replace(",", ""))
        if value > 2_147_483_647:
            raise ValueError("KC number exceeds limit")
        return value
    if field in BOOLEANS:
        if raw.casefold() not in {"true", "t", "1", "yes", "y", "false", "f", "0", "no", "n"}:
            raise ValueError("unknown boolean KC syntax")
        return raw.casefold() in {"true", "t", "1", "yes", "y"}
    if not raw or len(raw) > 240:
        raise ValueError("KC text exceeds limit")
    return raw.casefold()


def collection_query(command: object) -> dict:

    result = dict(
        version=1,
        recognized=False,
        target=None,
        filters=[],
        order="date",
        direction=None,
        absence_safe=False,
        stable_order=True,
        key="",
    )
    if not isinstance(command, str) or len(command) > 1200:
        return result
    command = unicodedata.normalize("NFKC", command).strip()
    result["key"] = hashlib.sha256(command.encode()).hexdigest()
    if "\n" in command or "\r" in command:
        return result
    tokens = command.split(maxsplit=1)
    if not tokens or tokens[0].casefold() not in {"kc", "kcollection", "k!c", "k!collection"}:
        return result
    text = tokens[1].strip() if len(tokens) == 2 else ""
    owner = re.match(r"^(?:<@!?(\d{17,20})>|(\d{17,20}))(?:\s+|$)", text)
    if owner:
        result["target"] = int(owner.group(1) or owner.group(2))
        text = text[owner.end() :].strip()
    matches = list(HEAD.finditer(text))
    if len(matches) > 24 or (matches and text[: matches[0].start()].strip()):
        return result
    if not matches:
        if text.casefold() in DIRECTIONS:
            result["direction"] = DIRECTIONS[text.casefold()]
        elif text:
            return result
    try:
        has_order = False
        for index, match in enumerate(matches):
            raw = text[
                match.end() : matches[index + 1].start() if index + 1 < len(matches) else len(text)
            ]
            if match.group("sort") or match.group("field").casefold() in {"o", "order"}:
                if match.group("field") and match.group("op").casefold() not in {":", "=", "is"}:
                    raise ValueError("unknown KC order operator")
                values = raw.strip().casefold().split()
                if has_order or not 1 <= len(values) <= 2 or values[0] not in SORTS:
                    raise ValueError("unknown KC order")
                result["order"] = SORTS[values[0]]
                if len(values) == 2:
                    result["direction"] = DIRECTIONS[values[1]]
                has_order = True
                continue
            field = FIELDS[match.group("field").casefold()]
            operator = match.group("op").casefold()
            if operator in {":", "is"}:
                operator = "="
            value = _value(field, raw)
            result["filters"].append(dict(field=field, op=operator, value=value))
        result["recognized"] = True
        result["absence_safe"] = all(
            term["field"] in {"print", "edition"}
            or (term["field"] == "code" and term["op"] == "=" and term["value"].isalnum())
            for term in result["filters"]
        )
        result["stable_order"] = result["order"] in {"date", "code", "edition", "print"}
    except (KeyError, ValueError):
        result.update(recognized=False, absence_safe=False, stable_order=False)
    return result


REPLY_FACTS = (
    "same_channel_reply_content",
    "reply_author_id",
    "reply_author_bot",
    "reply_webhook_id",
    "reply_created_at_ms",
    "reply_edited_at_ms",
    "reply_guild_id",
    "reply_channel_id",
)


def recent_human_reply(event: dict) -> bool:
    created = event.get("created_at_ms") or 0
    reply_created = event.get("reply_created_at_ms") or 0

    return bool(
        event.get("reply_author_bot") is False
        and event.get("reply_webhook_id") is None
        and event.get("reply_edited_at_ms") == 0
        and 0 < reply_created <= created <= reply_created + 30_000
        and event.get("reply_guild_id") == event.get("guild_id")
        and event.get("reply_channel_id") == event.get("channel_id")
    )


def collection_query_context(event: dict) -> dict:
    query = collection_query(event.get("same_channel_reply_content"))
    query["source_verified"] = recent_human_reply(event)
    return query


def wishlist_query_context(event: dict) -> dict:
    pattern = r"(?i)^\s*(?:kwl|kwishlist|k!wl|k!wishlist)(?:\s+(?:<@!?)?(\d{17,20})>?)?\s*$"
    match = re.fullmatch(pattern, event.get("same_channel_reply_content") or "")
    verified = recent_human_reply(event)
    return dict(
        verified=verified and match is not None,
        unfiltered=match is not None,
        target=int(match[1]) if match and match[1] else event.get("reply_author_id"),
    )


SPREADSHEET_URL = r"https://d1t3mbh1ebsw2a\.cloudfront\.net/[a-f0-9]{32}\.csv"


def spreadsheet_source(event: dict) -> dict | None:

    if (
        event.get("author_id") != -2
        or event.get("webhook_id") is not None
        or not event.get("guild_id")
        or not recent_human_reply(event)
        or not re.fullmatch(
            r"(?i)\s*k!?spreadsheet\s*",
            event.get("same_channel_reply_content", event.get("reply_content")) or "",
        )
        or "Collection Spreadsheet" not in event.get("embed_titles", ())
    ):
        return None
    descriptions = event.get("embed_descriptions", ())
    for description in descriptions:
        owner = re.match(r"^<@!?(\d{17,20})>, your new spreadsheet", description)
        urls = re.findall(SPREADSHEET_URL, description)
        if (
            owner
            and int(owner[1]) == event.get("reply_author_id")
            and len(urls) == 1
            and "Spreadsheet generated." in description
            and event.get("reference_message_id")
        ):
            return dict(
                version=1,
                owner=int(owner[1]),
                url=urls[0],
                command=int(event["reference_message_id"]),
                source=int(event["message_id"]),
                created=int(event["created_at_ms"]),
            )
    return None
