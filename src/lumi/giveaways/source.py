from __future__ import annotations

import re
from dataclasses import dataclass

from lumi.contracts import EmbedSourceQuery

from .models import BOTS, FRESH_MS, GiveawayError, image_url, source_fingerprint
from .protocol import card_prizes


@dataclass(frozen=True, slots=True)
class SourceObservation:


    query: EmbedSourceQuery
    job: str
    guild: int
    channel: int
    message: int
    proof: str
    valid_until: int

OWNER = re.compile(
    r"(?mi)^[ \t]*(?:this card is owned by|owned by)[ \t]*"
    r"(?:\*\*:\*\*[ \t]*)?<@!?(\d{17,20})>[ \t]*$"
)
KARUTA = re.compile(
    r"(?m)^[*`]*([A-Za-z0-9]{1,32})[*`]*[ \t]*·[ \t]*[*`]*[★☆⭐]{1,5}[*`]*"
    r"[ \t]*·[ \t]*[*`]*#([\d,]{1,12})[*`]*[ \t]*·[ \t]*[*`]*[◈❖](\d{1,3})[*`]*"
    r"[ \t]*·[ \t]*([^\r\n·]{1,240})[ \t]*·[ \t]*\*\*([^\r\n]{1,250})\*\*[ \t]*$"
)
GLITCH = re.compile(
    r"(?m)^`([A-Za-z0-9]{1,32})`[ \t]*⿻[ \t]*#([\d,]{1,12})[ \t]*⿻[ \t]*"
    r"\*\*([^\r\n]{1,250})\*\*[ \t]*⿻[ \t]*\*([^\r\n]{1,240})\*[ \t]*$"
)
SOFI = re.compile(
    r"\A### \*\*([^\r\n]{1,250})\*\*[ \t]*\n\*\*([^\r\n]{1,240})\*\*\n+"
    r"\*\*([A-Za-z0-9]{1,32})\*\*[ \t]*•[ \t]*<a?:[A-Za-z0-9_]{2,32}:\d{17,20}>"
    r"[ \t]*•[ \t]*`V(\d{1,3})`[ \t]*•[ \t]*`G-([\d,]{1,12}|\?|—|-)`[ \t]*\n"
)


def parse_view(*, author, webhook, embeds, message, created, edited, now):
    game = BOTS.get(author)
    if game is None or webhook or not 0 <= now - created <= FRESH_MS:
        raise GiveawayError("Reply to a fresh game-bot card view.")
    if len(embeds) != 1:
        raise GiveawayError("Unrecognised card view.")
    embed = embeds[0]
    text = str(embed.get("description") or "").replace("\r\n", "\n")
    if len(text) > 8000:
        raise GiveawayError("Unrecognised card view.")
    owners = OWNER.findall(text)
    if len(owners) != 1:
        raise GiveawayError("Card owner could not be verified.")
    values = dict(
        game=game,
        owner=int(owners[0]),
        message=message,
        created=created,
        edited=edited,
        image_url=image_url(str(embed.get("image", {}).get("url") or "")),
    )
    if game == "karuta":
        found = KARUTA.search(text) if embed.get("title") == "Card Details" else None
        if found:
            code, number, edition, series, character = found.groups()
            values.update(
                code=code.lower(),
                print=int(number.replace(",", "")),
                edition=int(edition),
                series=series.strip(),
                character=character,
            )
            if values["print"] <= 0 or not 1 <= values["edition"] <= 100:
                found = None
    elif game == "glitch":
        found = GLITCH.search(text) if embed.get("title") == "View info" else None
        if found:
            code, number, character, series = found.groups()
            values.update(
                code=code.lower(),
                print=int(number.replace(",", "")),
                character=character,
                series=series,
            )
            if values["print"] <= 0:
                found = None
    else:
        found = SOFI.search(text)
        if found:
            character, series, code, version, generation = found.groups()
            values.update(
                code=code.upper(),
                character=character,
                series=series,
                version=int(version),
                generation=int(generation.replace(",", "")) if generation[0].isdigit() else None,
            )
    if not found or any(
        values.get(key, 0) > 2_147_483_647
        for key in ("print", "edition", "version")
        if values.get(key) is not None
    ):
        raise GiveawayError("Unrecognised card view.")
    if values.get("generation", 0) is not None and values.get("generation", 0) > 2_147_483_647:
        raise GiveawayError("Unrecognised card view.")
    return values


def observed_view(view, guild, channel, now, *, message=None):
    if (
        not isinstance(view, dict) or view.get("guild") != guild or view.get("channel") != channel
        or (message is not None and view.get("message") != message)
        or any(type(view.get(key)) is not int for key in ("guild", "channel", "author", "message", "created", "edited"))
    ):
        raise GiveawayError("Card view unavailable.")
    embeds = view.get("embeds")
    if not isinstance(embeds, list) or any(
        not isinstance(embed, dict) or not isinstance(embed.get("image", {}), dict)
        for embed in embeds
    ):
        raise GiveawayError("Card view unavailable.")
    try:
        return parse_view(
            author=view["author"], webhook=view.get("webhook"), embeds=embeds,
            message=view["message"], created=view["created"], edited=view["edited"], now=now,
        )
    except GiveawayError:
        raise
    except ValueError as exc:
        raise GiveawayError("Card view unavailable.") from exc


def verify_views(views, guild, host, spec, now, *, previous=None):
    cards = card_prizes(spec, previous=previous)
    if not isinstance(views, (list, tuple)) or len(views) != len(cards):
        raise GiveawayError("Card view unavailable.")
    for view, prize in zip(views, cards, strict=True):
        source = observed_view(view, guild, prize["source_channel"], now, message=prize["card"]["message"])
        metadata = {key: value for key, value in source.items() if key != "image_url"}
        if source["owner"] != host or metadata != prize["card"] or source_fingerprint(source) != prize["proof"]:
            raise GiveawayError("Card details changed. Add a fresh view.")
