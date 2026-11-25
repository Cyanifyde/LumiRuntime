from __future__ import annotations

import logging

from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts import (
    CaptureSpec,
    FieldExtraction,
    HelpSpec,
    Manifest,
    MatcherExpr,
    PassiveMatcherSpec,
    ResultEnvelope,
)

from .scanner import AMOUNT, card_info

MODULE = "karuta_sketch_feed"
GUILD = -9
CHANNEL = -10
WEBHOOK = -11
TITLES = {"new": "New Sketch has been added to our database", "updated": "Updated Sketch"}
HEADER = (
    r"(?m)^[`*]{1,3}(?P<code>[A-Za-z0-9]{3,16})[`*]{1,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}#(?P<print_num>[\d,]{1,12})[ \t\x{00a0}]*[`*]{0,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}[◈❖](?P<edition>\d{1,3})[`*]{0,3}[ \t]*·[ \t]*"
    r"(?P<series>[^\n·]{1,240}?)[ \t]*·[ \t]*(?P<character>[^\n]{1,250})[ \t]*$[\s\S]*$"
)
DROP = r"(?:🩸|<a?:[A-Za-z0-9_]{1,32}:\d{17,20}>)"
INK_PREFIX = r"(?mi)^[\s\S]*?^Ink:[ \t]*" + DROP + r"[ \t]*[`*]*"
INK_SUFFIX = r"[`*]*[ \t]*$[\s\S]*$"
INK_NEW = INK_PREFIX + r"(?P<ink>" + AMOUNT + r")" + INK_SUFFIX
INK_UPDATED = (
    INK_PREFIX
    + r"(?P<previous_ink>"
    + AMOUNT
    + r")[`*]*[ \t]*→[ \t]*"
    + DROP
    + r"[ \t]*[`*]*(?P<ink>"
    + AMOUNT
    + r")"
    + INK_SUFFIX
)
LOGGER = logging.getLogger(__name__)


def routes():
    return tuple(
        PassiveMatcherSpec(
            route=f"{MODULE}.{kind}_{event}",
            handler=MODULE + ".observe",
            event=event,
            bot_ids=(WEBHOOK,),
            requires_embeds=True,
            hydrate=event == "message_edit",
            watch_ttl_ms=900_000,
            deadline_ms=60_000,
            expression=MatcherExpr(
                "all",
                children=(
                    MatcherExpr("exact", "guild_id", str(GUILD)),
                    MatcherExpr("exact", "channel_id", str(CHANNEL)),
                    MatcherExpr("exact", "webhook_id", str(WEBHOOK)),
                    MatcherExpr("exact", "embed_titles", title),
                    MatcherExpr("prefix", "primary_image_url", "https://"),
                ),
            ),
            extractions=(
                FieldExtraction(
                    "embed_descriptions",
                    HEADER,
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
                FieldExtraction(
                    "embed_descriptions",
                    INK_NEW if kind == "new" else INK_UPDATED,
                    (
                        CaptureSpec("ink", "compact_integer", minimum=0, maximum=2_147_483_647),
                        *(
                            (
                                CaptureSpec(
                                    "previous_ink",
                                    "compact_integer",
                                    minimum=0,
                                    maximum=2_147_483_647,
                                ),
                            )
                            if kind == "updated"
                            else ()
                        ),
                    ),
                ),
            ),
            context_fields=("primary_image_url", "source_revision_ms", "event_order", "webhook_id"),
        )
        for event in ("message_create", "message_edit")
        for kind, title in TITLES.items()
    )


def observation(request):
    event = request.event
    if request.route not in {r.route for r in routes()} or (
        int(event.get("guild_id") or 0),
        int(event.get("channel_id") or 0),
        int(event.get("author_id") or 0),
        int(event.get("webhook_id") or 0),
        int(event.get("actor_id") or 0),
    ) != (GUILD, CHANNEL, WEBHOOK, WEBHOOK, WEBHOOK):
        raise ValueError("untrusted sketch feed")
    return card_info(request, sketched=True)


class KarutaSketchFeedModule:


    def __init__(self, library):
        self.library = library

    @property
    def manifest(self):
        return Manifest(
            schema=2,
            module=MODULE,
            group="cards",
            generation=self.library.generation,
            job_stream="lumi:jobs:cards",
            display_name="Karuta Sketch Feed",
            summary="Sync sketch cards from the configured webhook feed.",
            help=HelpSpec(
                "Automatic feed sync.",
                "Update the sketch library from its configured webhook.",
                hidden=True,
            ),
            passive=routes(),
            version="1.0.1",
        )

    async def handle(self, request, job_id, generation):
        if (
            generation == self.library.generation
            and request.handler == MODULE + ".observe"
            and self.library.clock()
            < int(request.event.get("_expires_at_ms", self.library.clock() + 1))
        ):
            try:
                info, url = observation(request)
                await self.library.learn_observation(
                    info, url, request.event, update_catalogue=False
                )
            except (OSError, ValueError, KeyError, TypeError, RedisError, MySQLError, TimeoutError):
                LOGGER.warning("sketch_feed_observation_unavailable")
        return ResultEnvelope(
            1,
            job_id,
            MODULE,
            "cards",
            self.library.generation,
            "ok",
            completed_at_ms=self.library.clock(),
        )

    async def close(self):
        pass
