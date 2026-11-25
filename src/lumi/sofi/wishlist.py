from __future__ import annotations

import logging
import re
import time

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    DiscordIntent,
    FieldExtraction,
    HelpSpec,
    Manifest,
    MatcherExpr,
    PassiveMatcherSpec,
    PresentationPage,
    RepeatedExtraction,
    ResultEnvelope,
    TypedRequest,
)

from .module import SOFI_BOT_ID
from .wishlist_filters import FilterError, collection_commands, parse_filters
from .wishlist_store import (
    MySQLWishlistStore,
    WishlistEntry,
    WishlistObservation,
    WishlistPage,
)

LOGGER = logging.getLogger(__name__)
MODULE = "sofi_wishlist"
TITLE = (
    r"(?i)^SOFI:\s*WISHLIST\s*\((?P<owner_label>[^\n]{1,100})\)\s*"
    r"\[(?P<total>[0-9]{1,3})/(?P<capacity>[0-9]{1,3})\]$"
)
PAGE = r"^Page:\s*(?P<page>[0-9]{1,3})/(?P<page_count>[0-9]{1,3})\s*$"
SOURCE = (
    r"(?i)^\s*s(?:wl|wishlist)(?:\s+(?:<@!?(?P<mentioned_owner>[0-9]{17,20})>"
    r"|(?P<owner_id>[0-9]{17,20})))?\s*$"
)
ROW = (
    r"(?m)^`(?P<position>[0-9]{1,3})\]`\s*•\s*`[^0-9`\n]*"
    r"(?P<wishes>(?:[0-9]{1,9}|[0-9]{1,3}(?:,[0-9]{3}){1,2}))\s*`\s*•\s*\*\*"
    r"(?P<name>[^\n]{1,240}?)\*\*\s*•\s*\*(?P<series>[^\n]{1,240})\*\s*$"
)
TARGET = re.compile(r"^(?:<@!?(\d{17,20})>|(\d{17,20}))(?:\s+(.*))?$", re.DOTALL)


class SofiWishlistModule:
    def __init__(self, *, generation: int, store: MySQLWishlistStore) -> None:
        self._generation = generation
        self._store = store

    async def close(self) -> None:
        pass

    @property
    def manifest(self) -> Manifest:
        passive = tuple(
            PassiveMatcherSpec(
                route=f"{MODULE}.observe_{event}",
                handler=f"{MODULE}.observe",
                event=event,
                bot_ids=(SOFI_BOT_ID,),
                requires_embeds=True,
                expression=MatcherExpr(
                    "all",
                    children=(
                        MatcherExpr("regex", "embed_titles", TITLE),
                        MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
                        MatcherExpr("exact", "reply_edited_at_ms", "0"),
                    ),
                ),
                extractions=(
                    FieldExtraction(
                        "embed_titles",
                        TITLE,
                        (
                            CaptureSpec("owner_label", maximum=100),
                            CaptureSpec("total", "integer", minimum=0, maximum=100),
                            CaptureSpec("capacity", "integer", minimum=1, maximum=100),
                        ),
                    ),
                    FieldExtraction(
                        "embeds.footer.text",
                        PAGE,
                        (
                            CaptureSpec("page", "integer", minimum=1, maximum=100),
                            CaptureSpec("page_count", "integer", minimum=1, maximum=100),
                        ),
                    ),
                    FieldExtraction(
                        "reply_content",
                        SOURCE,
                        (
                            CaptureSpec("mentioned_owner", "snowflake", required=False),
                            CaptureSpec("owner_id", "snowflake", required=False),
                        ),
                    ),
                ),
                repeated=(
                    RepeatedExtraction(
                        "embed_descriptions",
                        ROW,
                        100,
                        (
                            CaptureSpec("position", "integer", minimum=1, maximum=100),
                            CaptureSpec(
                                "wishes", "compact_integer", minimum=0, maximum=2_147_483_647
                            ),
                            CaptureSpec("name", maximum=240),
                            CaptureSpec("series", maximum=240),
                        ),
                    ),
                ),
                subject_field="reply_author_id",
                subject_required=True,
                reference_author_max_age_ms=30_000,
                hydrate=event == "message_edit",
                watch_ttl_ms=900_000,
                context_fields=("reply_author_id", "event_order"),
            )
            for event in ("message_create", "message_edit")
        )
        return Manifest(
            schema=2,
            module=MODULE,
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Sofi Wishlists",
            summary="Saved wishlists and collection searches.",
            version="1.0.6",
            presentation_mode="legacy",
            commands=(
                CommandSpec(
                    root="swl",
                    handler=f"{MODULE}.search",
                    grammar=r"^(?:\s+(?P<query>[\s\S]+))?\s*$",
                    captures=(CaptureSpec("query", required=False, maximum=1200),),
                    help=HelpSpec(
                        ".swl <@user|ID> [filters]\n.swl <ID> n<10 ev:hal26 o:n",
                        "Build SC searches for your saved Sofi wishlist.",
                    ),
                    acl_key="swl",
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
            passive=passive,
        )

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        del generation
        intent = None
        error = None
        try:
            if request.handler == f"{MODULE}.observe":
                await self._observe(request)
            elif request.handler == f"{MODULE}.search":
                intent = await self._search(request)
            else:
                raise ValueError("unknown wishlist route")
        except (ValueError, KeyError, TypeError):
            if request.handler == f"{MODULE}.search":
                intent = self._simple("Unavailable.")
        except Exception:
            LOGGER.exception("sofi_wishlist_request_failed")
            error = "handler_failed"
            if request.handler == f"{MODULE}.search":
                intent = self._simple("Unavailable.")
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module=MODULE,
            group="cards",
            generation=self._generation,
            status="error" if error else "ok",
            intents=(intent,) if intent else (),
            error_code=error,
            completed_at_ms=int(time.time() * 1000),
        )

    async def _observe(self, request: TypedRequest) -> None:
        event, args = request.event, request.arguments
        if event.get("dispatch") != "passive" or event.get("author_id") != SOFI_BOT_ID:
            return
        observer = int(event["reply_author_id"])
        if observer != event.get("actor_id"):
            return
        owner = int(args.get("mentioned_owner") or args.get("owner_id") or observer)
        created = int(event["created_at_ms"])
        edited = int(event.get("edited_at_ms") or 0)
        order = int(event.get("event_order") or max(created, edited) * 1000)
        entries = tuple(WishlistEntry(**item) for item in args["embed_descriptions"])
        observation = WishlistObservation(
            int(event["guild_id"]),
            owner,
            args["owner_label"],
            observer,
            int(event["channel_id"]),
            int(event["message_id"]),
            created,
            args["total"],
            args["capacity"],
            args["page_count"],
            WishlistPage(args["page"], order, entries),
        )
        await self._store.observe(observation)

    async def _search(self, request: TypedRequest) -> DiscordIntent:
        query = str(request.arguments.get("query") or "").strip()
        prefix = str(request.event.get("prefix") or ".")
        usage = f"{prefix}swl <user ID> [filters]"
        if not query:
            return self._simple(f"`{usage}`")
        if query.casefold() == "help":
            return self._simple(
                f"`{usage}`\n`{prefix}swl -7 o:n n<10 ev:hal26`\n"
                "`n` → gen · `e` → version · `element` → element"
            )
        target = TARGET.fullmatch(query)
        if target is None:
            return self._simple(f"`{usage}`")
        try:
            filters = parse_filters(target[3] or "")
        except FilterError as exc:
            return self._simple(str(exc))
        state = await self._store.wishlist(
            int(request.event["guild_id"]), int(request.event["actor_id"])
        )
        if state is None:
            return self._simple("Run Sofi's `swl` first.")
        if not state.complete:
            return self._simple("View all `swl` pages.")
        if not state.entries:
            return self._simple("Wishlist empty.")
        try:
            commands = collection_commands(int(target[1] or target[2]), state.entries, filters)
        except FilterError as exc:
            return self._simple(str(exc))
        descriptions: list[str] = []
        for command in commands:


            descriptions.append(re.sub(r"([\\`*_~|\[\]()])", r"\\\1", command))


        fits_reply = len(descriptions) <= 10 and sum(map(len, descriptions)) <= 6000
        return DiscordIntent(
            kind="send" if fits_reply else "paginator",
            pages=tuple(PresentationPage(description=value) for value in descriptions),
        )

    @staticmethod
    def _simple(text: str) -> DiscordIntent:
        return DiscordIntent(kind="send", pages=(PresentationPage(description=text),))
