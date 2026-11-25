from __future__ import annotations

import asyncio
import hashlib
import io

from aiohttp import ClientError
from PIL import Image

from lumi.contracts import CaptureSpec, FieldExtraction, MatcherExpr, PassiveMatcherSpec
from lumi.frame_tester.artwork import ArtworkLoader, validate_artwork_url
from lumi.karuta_collections.artifacts import inspect_image
from lumi.karuta_collections.models import clean_text
from lumi.karuta_collections.module import heading

from .models import CardInfo

BOT = -2
KCI_COMMAND = (
    r"(?i)^\s*(?:kci|k!ci|kcardinfo|k!cardinfo)"
    r"(?:[ \t]+(?P<requested_code>[a-z0-9]{3,16}))?\s*$"
)
CARD_HEADER = (
    r"(?m)^[`*]{1,3}(?P<code>[A-Za-z0-9]{3,16})[`*]{1,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}[★☆⭐]{1,5}[`*]{0,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}#(?P<print_num>[\d,]{1,12})[ \t\x{00a0}]*[`*]{0,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}[◈❖](?P<edition>\d{1,3})[`*]{0,3}[ \t]*·[ \t]*"
    r"(?P<series>[^\n·]{1,240}?)[ \t]*·[ \t]*(?P<character>[^\n]{1,250})[ \t]*$[\s\S]*$"
)
SKETCHED = r"(?mi)^[\s\S]*?^[ \t]*Sketched by[ \t]+[^\r\n]{1,250}[ \t]*$[\s\S]*$"
AMOUNT = r"(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]{1,10})"
INKWELL = (
    r"(?mi)^[\s\S]*?^[ \t]*Inkwell is at[ \t]+[*`]*(?P<ink>" + AMOUNT + r")[*`]*"
    r"(?:[ \t]*/[ \t]*[*`]*(?P<capacity>" + AMOUNT + r")[*`]*)?[ \t]*$[\s\S]*$"
)


def passive_routes(module: str, ttl_ms: int):
    routes = []
    for event in ("message_create", "message_edit"):
        for sketched in (False, True):
            marker = MatcherExpr("regex", "embed_descriptions", SKETCHED)
            extractions = [
                FieldExtraction(
                    "reply_content",
                    KCI_COMMAND,
                    (CaptureSpec("requested_code", required=False, maximum=16),),
                ),
                FieldExtraction(
                    "embed_descriptions",
                    CARD_HEADER,
                    (
                        CaptureSpec("code", maximum=16),
                        CaptureSpec(
                            "print_num", "compact_integer", minimum=1, maximum=2_147_483_647
                        ),
                        CaptureSpec("edition", "integer", minimum=1, maximum=100),
                        CaptureSpec("series", "normalized_text", maximum=240),
                        CaptureSpec("character", "normalized_text", maximum=250),
                    ),
                ),
            ]
            if sketched:
                extractions.append(
                    FieldExtraction(
                        "embed_descriptions",
                        INKWELL,
                        (
                            CaptureSpec("ink", "compact_integer", minimum=0, maximum=2_147_483_647),
                            CaptureSpec(
                                "capacity",
                                "compact_integer",
                                required=False,
                                minimum=0,
                                maximum=2_147_483_647,
                            ),
                        ),
                    )
                )
            routes.append(
                PassiveMatcherSpec(
                    route=f"{module}.{'sketched' if sketched else 'card'}_{event}",
                    handler=f"{module}.observe",
                    event=event,
                    bot_ids=(BOT,),
                    requires_embeds=True,
                    subject_field="requester_id",
                    subject_required=True,
                    hydrate=event == "message_edit",
                    watch_ttl_ms=ttl_ms,
                    deadline_ms=60_000,
                    expression=MatcherExpr(
                        "all",
                        children=(
                            MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
                            heading("Card Details"),
                            MatcherExpr("regex", "reply_content", KCI_COMMAND),
                            marker if sketched else MatcherExpr("not", children=(marker,)),
                        ),
                    ),
                    extractions=tuple(extractions),
                    context_fields=(
                        "primary_image_url",
                        "source_revision_ms",
                        "event_order",
                        "requester_id",
                        "webhook_id",
                    ),
                )
            )
    return tuple(routes)


def observation(request, module: str) -> tuple[CardInfo, str]:
    if request.route not in {r.route for r in passive_routes(module, 900_000)}:
        raise ValueError("invalid observation route")
    event = request.event
    if (
        int(event.get("author_id") or 0) != BOT
        or event.get("webhook_id")
        or min(int(event.get(k) or 0) for k in ("guild_id", "channel_id", "actor_id")) <= 0
        or int(event.get("requester_id") or 0) != int(event["actor_id"])
    ):
        raise ValueError("untrusted observation")
    return card_info(request, sketched=request.route.startswith(module + ".sketched_"))


def card_info(request, *, sketched: bool) -> tuple[CardInfo, str]:

    event, fields = request.event, request.arguments
    code = str(fields["code"]).casefold()
    if fields.get("requested_code") and str(fields["requested_code"]).casefold() != code:
        raise ValueError("card command mismatch")
    ink = fields.get("ink") if sketched else None
    if sketched and (
        type(ink) is not int or (fields.get("capacity") is not None and ink > fields["capacity"])
    ):
        raise ValueError("invalid inkwell")
    url = str(event.get("primary_image_url") or "") if sketched else ""
    info = CardInfo(
        code,
        clean_text(fields["character"]),
        clean_text(fields["series"]),
        fields["edition"],
        fields["print_num"],
        sketched,
        ink,
        int(event.get("created_at_ms") or 0),
        int(event.get("message_id") or 0),
        int(event.get("source_revision_ms") or event.get("created_at_ms") or 0),
        int(event.get("event_order") or 0),
        hashlib.sha256(url.encode()).hexdigest() if url else "",
    )
    return info, url


def lossless_webp(data: bytes) -> bytes:
    try:
        inspect_image(data)
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        raise ValueError("sketch dimensions exceeded") from exc
    with Image.open(io.BytesIO(data)) as source:
        rgba = source.convert("RGBA")
        pixels, size = rgba.tobytes(), rgba.size
        extras = {k: source.info[k] for k in ("icc_profile", "exif") if source.info.get(k)}
    target = io.BytesIO()
    rgba.save(target, "WEBP", lossless=True, exact=True, method=4, **extras)
    encoded = target.getvalue()
    inspect_image(encoded)
    with Image.open(io.BytesIO(encoded)) as decoded:
        if decoded.size != size or decoded.convert("RGBA").tobytes() != pixels:
            raise ValueError("sketch conversion changed pixels")
    return encoded


async def fetch_webp(loader: ArtworkLoader, url: str) -> tuple[bytes, str]:
    validate_artwork_url(url)
    try:
        source = await loader.fetch(url)
    except ClientError as exc:
        raise OSError("sketch image unavailable") from exc
    converted = await asyncio.to_thread(lossless_webp, source.data)
    return converted, hashlib.sha256(source.data).hexdigest()
