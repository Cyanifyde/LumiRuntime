from __future__ import annotations

import asyncio
import logging
import time
import unicodedata
import weakref

import msgspec
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from lumi.contracts import (
    CaptureSpec,
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
    decode,
    encode,
)
from lumi.karuta_collections.query import collection_metrics

LOGGER = logging.getLogger(__name__)
KARUTA_BOT_ID = -2
MODULE = "karuta_worker_helper"
PAIR_WINDOW_MS = 30_000
STATE_TTL = 120
DASH = "[\u2013\u2212-]"


SLOT = (
    r"(?P<slot>[A-E🇦-🇪]|<a?:[A-Ea-e]:\d{17,20}>|"
    r"<a?:regional_indicator_[a-e]:\d{17,20}>)"
)
SLOT_PREFIX = rf"(?mi)^[ \t]*[*`]{{0,2}}{SLOT}[*`]{{0,2}}[ \t]+"
EFFORT = r"(?P<effort>\d{1,3}(?:,\d{3})+|\d{1,7})"
BOARD_ROW = (
    SLOT_PREFIX
    + r"(?:(?P<name>[^\n·]{1,200}?)\s*·\s*[*`]{0,3}"
    + EFFORT
    + r"[*`]{0,3}[ \t]+Effort[ \t]*·[ \t]*[*`]{0,3}"
    r"(?P<state>Healthy|Injured)[*`]{0,3}(?:[^\n]{0,100})|"
    r"[*`]{0,3}(?P<empty>Empty|None|No worker(?: assigned)?|"
    r"This (?:job )?slot is empty)[*`]{0,3}[.!]?)[ \t]*$"
)
COLLECTION_ROW = (
    r"(?m)^(?P<metrics>[^\r\n]{0,160}?)[`*]{1,3}[ \t\x{00a0}]*"
    r"(?P<code>[A-Za-z0-9]{1,16})[ \t\x{00a0}]*[`*]{1,3}"
    r"[ \t]*·[ \t]*[`*]{0,3}[★☆⭐]{1,4}[`*]{0,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}#[\d,]{1,12}[ \t\x{00a0}]*[`*]{0,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}[◈❖]\d{1,3}[`*]{0,3}[ \t]*·[ \t]*"
    r"[^\r\n·]{1,240}?[ \t]*·[ \t]*(?P<name>[^\r\n]{1,256})[ \t]*$"
)
PAGE = (
    r"(?is)^[\s\S]*?Showing[ \t]+cards[ \t]+(?P<start>[\d,]{1,9})"
    rf"[ \t]*{DASH}[ \t]*(?P<end>[\d,]{{1,9}})[ \t]+of[ \t]+"
    r"(?P<total>[\d,]{1,9})\b[\s\S]*$"
)
NODE_ROW = (
    r"(?mi)^[ \t]*(?P<position>[1-9][0-9]{0,2})\.[ \t]+[*`]{1,3}"
    r"(?P<node>[a-z][a-z0-9_-]{0,31})[*`]{1,3}[ \t]*·[ \t]*[*`]{0,3}"
    r"(?P<tax>100|[0-9]{1,2})%[*`]{0,3}[ \t]+tax[ \t]*·[ \t]*"
    r"(?:[*`]{0,3}\d{1,4}%[*`]{0,3}[ \t]+power[ \t]*·[ \t]*)?"
    r"[*`]{0,3}[\d,]{1,12}[*`]{0,3}[ \t]+workers?"
    r"(?:[ \t]*·[ \t]*<@!?\d{17,20}>)?[ \t]*$"
)
NODE_PAGE = (
    r"(?is)^[\s\S]*?Showing[ \t]+nodes[ \t]+(?P<start>[1-9][0-9]{0,2})"
    rf"[ \t]*{DASH}[ \t]*(?P<end>[1-9][0-9]{{0,2}})[ \t]+of[ \t]+"
    r"(?P<total>[1-9][0-9]{0,2})\b[\s\S]*$"
)
NODE_COMMAND = r"(?i)^[ \t]*(?:kn|knodes|k!n|k!nodes)(?:[ \t]+[^\r\n]{1,500})?[ \t]*$"


class BoardWorker(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    slot: str
    name: str
    effort: int
    injured: bool


class Candidate(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    code: str
    name: str
    effort: int


class Observation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    message_id: int
    created_at_ms: int
    event_order: int
    workers: tuple[BoardWorker, ...] = ()
    cards: tuple[Candidate, ...] = ()


class PairingState(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    board: Observation | None = None
    collection: Observation | None = None


def normalized_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", value)
    value = "".join(
        char for char in value if unicodedata.category(char) not in {"Cf", "Cc"} or char.isspace()
    )
    return " ".join(value.strip(" *`_").split()).casefold()


def slot_letter(value: str) -> str:
    if value.startswith("<"):
        value = value.split(":")[1].removeprefix("regional_indicator_")
    if len(value) == 1 and "🇦" <= value <= "🇪":
        value = chr(ord("A") + ord(value) - ord("🇦"))
    value = value.upper()
    if value not in ("A", "B", "C", "D", "E"):
        raise ValueError("invalid slot")
    return value


def observation_stamp(request: TypedRequest, kind: str) -> tuple[int, int, int]:
    created_at_ms = int(request.event["created_at_ms"])
    event_order = request.event.get("event_order")
    if event_order is None and request.route == f"{MODULE}.{kind}_message_create":


        event_order = created_at_ms * 1000
    if type(event_order) is not int or event_order <= 0:
        raise ValueError("missing event order")
    return int(request.event["message_id"]), created_at_ms, event_order


def replacement_lines(board: Observation, collection: Observation) -> tuple[str, ...]:
    injured = sorted(worker.slot for worker in board.workers if worker.injured)
    if not injured:
        return ()


    occupied = {(normalized_name(worker.name), worker.effort) for worker in board.workers}
    candidates = sorted(
        (
            card
            for card in collection.cards
            if (normalized_name(card.name), card.effort) not in occupied
        ),
        key=lambda card: -card.effort,
    )
    if len(candidates) < len(injured):
        return ("Not enough eligible cards on this page.",)
    return tuple(
        f"kjw {slot.lower()} {card.code}" for slot, card in zip(injured, candidates, strict=False)
    )


class KarutaWorkerHelperModule:
    def __init__(self, *, generation: int, cache_redis: Redis) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    async def close(self) -> None:

        self._locks.clear()

    @property
    def manifest(self) -> Manifest:
        routes = []
        owner = CaptureSpec("owner", "snowflake")
        for event in ("message_create", "message_edit"):
            for kind, title, header in (
                ("board", "Job Board", "Showing board of"),
                ("collection", "Card Collection", "Cards owned by"),
            ):
                owner_pattern = rf"(?is)^[\s\S]*?{header}[ \t*]+<@!?(?P<owner>\d{{17,20}})>[\s\S]*$"
                title_pattern = (
                    r"(?i)^(?:[^A-Za-z0-9\n]{0,16}|<a?:[A-Za-z0-9_]{1,32}:\d{17,20}>\s*)?"
                    + title
                    + r"$"
                )
                conditions = [
                    MatcherExpr(
                        "any",
                        children=(
                            MatcherExpr("regex", "embed_titles", title_pattern),
                            MatcherExpr("regex", "embed_author_name", title_pattern),
                        ),
                    )
                ]
                extractions = [FieldExtraction("embed_descriptions", owner_pattern, (owner,))]
                if kind == "board":
                    conditions.append(MatcherExpr("regex", "embed_descriptions", BOARD_ROW))
                    repeated = (
                        RepeatedExtraction(
                            "embed_descriptions",
                            BOARD_ROW,
                            6,
                            (
                                CaptureSpec("slot", maximum=80),
                                CaptureSpec("name", required=False, maximum=200),
                                CaptureSpec(
                                    "effort", "compact_integer", required=False, maximum=9_999_999
                                ),
                                CaptureSpec("state", required=False, maximum=7),
                                CaptureSpec("empty", required=False, maximum=30),
                            ),
                        ),
                        RepeatedExtraction(
                            "message_document", SLOT_PREFIX, 6, (CaptureSpec("slot", maximum=80),)
                        ),
                    )
                else:
                    conditions.append(MatcherExpr("regex", "embed_descriptions", COLLECTION_ROW))
                    extractions.append(
                        FieldExtraction(
                            "message_document",
                            PAGE,
                            (
                                CaptureSpec(
                                    "start", "compact_integer", minimum=1, maximum=9_999_999
                                ),
                                CaptureSpec("end", "compact_integer", minimum=1, maximum=9_999_999),
                                CaptureSpec(
                                    "total", "compact_integer", minimum=1, maximum=9_999_999
                                ),
                            ),
                        )
                    )
                    repeated = (
                        RepeatedExtraction(
                            "embed_descriptions",
                            COLLECTION_ROW,
                            11,
                            (
                                CaptureSpec("metrics", maximum=160),
                                CaptureSpec("code", maximum=16),
                                CaptureSpec("name", maximum=256),
                            ),
                        ),
                    )
                routes.append(
                    PassiveMatcherSpec(
                        route=f"{MODULE}.{kind}_{event}",
                        handler=f"{MODULE}.{kind}",
                        event=event,
                        bot_ids=(KARUTA_BOT_ID,),
                        requires_embeds=True,
                        expression=MatcherExpr("all", children=tuple(conditions)),
                        extractions=tuple(extractions),
                        repeated=repeated,
                        subject_capture="owner",
                        subject_required=True,
                        hydrate=event == "message_edit",
                        watch_ttl_ms=STATE_TTL * 1000,
                        context_fields=("event_order",),
                        presentation_mode="legacy",
                    )
                )
            title_pattern = (
                r"(?i)^(?:[^A-Za-z0-9\n]{0,16}|"
                r"<a?:[A-Za-z0-9_]{1,32}:\d{17,20}>\s*)?Nodes Overview$"
            )
            routes.append(
                PassiveMatcherSpec(
                    route=f"{MODULE}.nodes_{event}",
                    handler=f"{MODULE}.nodes",
                    event=event,
                    bot_ids=(KARUTA_BOT_ID,),
                    requires_embeds=True,
                    expression=MatcherExpr(
                        "all",
                        children=(
                            MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
                            MatcherExpr(
                                "any",
                                children=(
                                    MatcherExpr("regex", "embed_titles", title_pattern),
                                    MatcherExpr("regex", "embed_author_name", title_pattern),
                                ),
                            ),
                            MatcherExpr("regex", "reply_content", NODE_COMMAND),
                            MatcherExpr("regex", "embed_descriptions", NODE_ROW),
                        ),
                    ),
                    extractions=(
                        FieldExtraction(
                            "message_document",
                            NODE_PAGE,
                            tuple(
                                CaptureSpec(key, "integer", minimum=1, maximum=999)
                                for key in ("start", "end", "total")
                            ),
                        ),
                    ),
                    repeated=(
                        RepeatedExtraction(
                            "embed_descriptions",
                            NODE_ROW,
                            11,
                            (
                                CaptureSpec("position", "integer", minimum=1, maximum=999),
                                CaptureSpec("node", maximum=32),
                                CaptureSpec("tax", "integer", minimum=0, maximum=100),
                            ),
                        ),
                    ),
                    subject_field="reply_author_id",
                    subject_required=True,
                    reference_author_max_age_ms=30_000,
                    hydrate=event == "message_edit",
                    watch_ttl_ms=STATE_TTL * 1000,
                    context_fields=("event_order", "reply_author_id"),
                    presentation_mode="legacy",
                )
            )
        return Manifest(
            schema=2,
            module=MODULE,
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Karuta Worker Helper",
            summary="Suggest injured-worker replacements and low-tax Karuta nodes.",
            help=HelpSpec(
                f"`kjb` + `kc o:eff` within {PAIR_WINDOW_MS // 1000}s, in either order.\n"
                "`kn` for node suggestions.",
                "Get injured-worker replacements and low-tax node recommendations.",
            ),
            presentation_mode="legacy",
            passive=tuple(routes),
            version="1.2.1",
        )

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        intent = None
        error = None
        if generation != self._generation:
            error = "stale_generation"
        else:
            try:
                intent = (
                    await self._nodes(request)
                    if request.handler == f"{MODULE}.nodes"
                    else await self._observe(request)
                )
            except RedisError:
                LOGGER.warning("karuta_worker_helper_cache_unavailable")
            except (ValueError, TypeError, KeyError, msgspec.DecodeError):

                pass
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module=MODULE,
            group="cards",
            generation=self._generation,
            status="error" if error else "ok",
            error_code=error,
            intents=(intent,) if intent is not None else (),
            completed_at_ms=int(time.time() * 1000),
        )

    @staticmethod
    def _observation(request: TypedRequest) -> tuple[str, Observation]:
        kind = request.handler.removeprefix(f"{MODULE}.")
        if kind not in {"board", "collection"}:
            raise ValueError("invalid handler")
        rows = request.arguments.get("embed_descriptions")
        if not isinstance(rows, (list, tuple)) or not rows:
            raise ValueError("missing rows")
        workers = []
        cards = []
        if kind == "board":
            prefixes = request.arguments.get("message_document")
            if not isinstance(prefixes, (list, tuple)) or not len(rows) == len(prefixes) <= 5:
                raise ValueError("incomplete board")
            slots = set()
            for row in rows:
                slot = slot_letter(row["slot"])
                if slot in slots:
                    raise ValueError("duplicate slot")
                slots.add(slot)
                if row.get("empty"):
                    continue
                state = row["state"].casefold()
                name = normalized_name(row["name"])
                effort = row["effort"]
                if state not in {"healthy", "injured"} or not name or not 0 <= effort <= 9_999_999:
                    raise ValueError("invalid worker")
                workers.append(BoardWorker(slot, name, effort, state == "injured"))
        else:
            start, end, total = (request.arguments.get(key) for key in ("start", "end", "total"))
            if (
                not all(type(value) is int for value in (start, end, total))
                or not 1 <= start <= end <= total
                or not len(rows) == end - start + 1 <= 10
            ):
                raise ValueError("incomplete collection page")
            codes = set()
            for row in rows:
                code = row["code"].casefold()
                name = normalized_name(row["name"])
                metrics = row["metrics"]
                effort = (collection_metrics(metrics) if isinstance(metrics, str) else metrics)["effort"]
                if (
                    not 1 <= len(code) <= 16
                    or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789" for char in code)
                    or code in codes
                    or not name
                    or type(effort) is not int
                    or not 0 <= effort <= 9_999_999
                ):
                    raise ValueError("invalid candidate")
                codes.add(code)
                cards.append(Candidate(code, name, effort))
        message_id, created_at_ms, event_order = observation_stamp(request, kind)
        return kind, Observation(
            message_id,
            created_at_ms,
            event_order,
            tuple(workers),
            tuple(cards),
        )

    async def _nodes(self, request: TypedRequest) -> DiscordIntent | None:
        owner = int(request.event["reply_author_id"])
        guild = int(request.event["guild_id"])
        channel = int(request.event["channel_id"])
        message_id, created_at_ms, _ = observation_stamp(request, "nodes")
        now = int(time.time() * 1000)
        if (
            int(request.event["author_id"]) != KARUTA_BOT_ID
            or int(request.event["actor_id"]) != owner
            or not all(10**16 <= value < 10**20 for value in (owner, guild, channel))
            or message_id <= 0
            or not 0 <= now - created_at_ms <= STATE_TTL * 1000
        ):
            return None
        start, end, total = (request.arguments.get(key) for key in ("start", "end", "total"))
        rows = request.arguments.get("embed_descriptions")
        if (
            not all(type(value) is int for value in (start, end, total))
            or not 1 <= start <= end <= total
            or not isinstance(rows, (list, tuple))
            or not len(rows) == end - start + 1 <= 10
        ):
            return None
        seen = set()
        choices = []
        for position, row in enumerate(rows, start=start):
            if not isinstance(row, dict) or not isinstance(row.get("node"), str):
                return None
            node, tax = row["node"].casefold(), row["tax"]
            if (
                type(row["position"]) is not int
                or row["position"] != position
                or not 1 <= len(node) <= 32
                or node[0] not in "abcdefghijklmnopqrstuvwxyz"
                or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for char in node)
                or node in seen
                or type(tax) is not int
                or not 0 <= tax <= 100
            ):
                return None
            seen.add(node)
            choices.append((node, tax))


        node, _ = min(choices, key=lambda choice: choice[1])
        key = f"lumi:{MODULE}:nodes:{guild}:{channel}:{owner}:{message_id}"
        if not await self._cache.set(key, "claimed", nx=True, ex=STATE_TTL):
            return None
        return DiscordIntent(
            kind="send",
            pages=(PresentationPage(description=f"kjn abcde {node}"),),
            presentation_mode="legacy",
        )

    async def _observe(self, request: TypedRequest) -> DiscordIntent | None:
        owner = int(request.arguments["owner"])
        guild = int(request.event["guild_id"])
        channel = int(request.event["channel_id"])
        if (
            int(request.event["author_id"]) != KARUTA_BOT_ID
            or int(request.event["actor_id"]) != owner
            or not all(10**16 <= value < 10**20 for value in (owner, guild, channel))
        ):
            return None
        kind, observation = self._observation(request)
        now = int(time.time() * 1000)
        if (
            observation.message_id <= 0
            or observation.event_order <= 0
            or not 0 <= now - observation.created_at_ms <= STATE_TTL * 1000
        ):
            return None
        key = f"lumi:{MODULE}:state:{guild}:{channel}:{owner}"
        async with self._locks.setdefault(key, asyncio.Lock()):
            for _ in range(8):
                try:
                    async with self._cache.pipeline(transaction=True) as pipeline:
                        await pipeline.watch(key)
                        raw = await pipeline.get(key)
                        state = decode(raw, PairingState) if raw is not None else PairingState()
                        previous = getattr(state, kind)
                        if previous is not None:
                            if (observation.created_at_ms, observation.message_id) < (
                                previous.created_at_ms,
                                previous.message_id,
                            ):
                                return None
                            if observation.message_id == previous.message_id and (
                                observation.created_at_ms != previous.created_at_ms
                                or observation.event_order <= previous.event_order
                            ):
                                return None
                        state = msgspec.structs.replace(state, **{kind: observation})
                        board, collection = state.board, state.collection
                        paired = (
                            board is not None
                            and collection is not None
                            and abs(board.created_at_ms - collection.created_at_ms)
                            <= PAIR_WINDOW_MS
                            and now - min(board.created_at_ms, collection.created_at_ms)
                            <= STATE_TTL * 1000
                        )
                        pipeline.multi()
                        pipeline.set(key, encode(state), ex=STATE_TTL)
                        if paired:
                            pair_key = (
                                f"lumi:{MODULE}:pair:{guild}:{channel}:{owner}:"
                                f"{board.message_id}:{collection.message_id}"
                            )
                            pipeline.set(pair_key, "claimed", nx=True, ex=STATE_TTL)
                        results = await pipeline.execute()
                    if not paired or not results[-1]:
                        return None
                    lines = replacement_lines(board, collection)
                    return (
                        DiscordIntent(
                            kind="send",
                            pages=tuple(PresentationPage(description=line) for line in lines),
                            presentation_mode="legacy",
                        )
                        if lines
                        else None
                    )
                except WatchError:
                    continue
        return None
