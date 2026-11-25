from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import time
from dataclasses import asdict
from pathlib import Path

from aiohttp import ClientError
from asyncmy.errors import MySQLError
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
    ReactionOfferSpec,
    RepeatedExtraction,
    ResultEnvelope,
    ScheduledCallbackSpec,
    TemporaryAttachment,
)
from lumi.frame_tester.artwork import ArtworkLoader
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from . import check_routes
from .artifacts import ArtifactStore
from .check_ui import CheckUI
from .inventory import CollectionPage
from .inventory_store import MemoryInventoryStore
from .models import Card, Stamp, clean_text, identity
from .query import REPLY_FACTS, collection_query_context, spreadsheet_source, wishlist_query_context
from .renderer import CollageRenderer

LOGGER = logging.getLogger(__name__)
MODULE = "karuta_collections"
BOT = -2
TTL = 900
MAX_ACTIVE_VIEWS = 128
PREFIX = "lumi:karuta-collections:"
LOOKUP = (
    r"(?is)^[\s\S]*?Character[ \t]*·[ \t]*(?P<character>[^\r\n]{1,244})[ \t]*\n"
    r"Series[ \t]*·[ \t]*(?P<series>[^\r\n]{1,244})[\s\S]*?"
    r"Showing\s+edition\s+(?P<edition>\d{1,3})\s+of\s+(?P<edition_count>\d{1,3})\b[\s\S]*$"
)
KLU_COMMAND = r"(?i)^\s*(?:klu|klookup|k!lu|k!lookup)(?:\s+[^\r\n]{1,1000})?\s*$"
PAGE = (
    r"(?is)^[\s\S]*?Showing\s+cards\s+(?P<start>[\d,]{1,7})\s*[\x{2013}\x{2212}-]\s*"
    r"(?P<end>[\d,]{1,7})\s+of\s+(?P<total>[\d,]{1,7})\b"
    r"(?:[ \t]+\((?P<matched_total>[\d,]{1,7})[ \t]+total\))?[\s\S]*$"
)
COLLECTION_OWNER = (
    r"(?is)^[ \t]*Cards owned by[ \t*`]+<@!?(?P<collection_owner>\d{17,20})>[^\r\n]*[\s\S]*$"
)
COLLECTION = (
    r"(?is)^[\s\S]*?Cards owned by[^\r\n]{1,250}[\s\S]*?Showing\s+cards\s+"
    r"[\d,]{1,7}\s*[\x{2013}\x{2212}-]\s*[\d,]{1,7}\s+of\s+[\d,]{1,7}\b[\s\S]*$"
)
COLLECTION_OFFER = (
    r"(?is)^[\s\S]*?Cards owned by[ \t*`]+<@!?\d{17,20}>[^\r\n]*[\s\S]*?"
    r"Showing\s+cards\s+[1-9][\d,]{0,6}\s*[\x{2013}\x{2212}-]\s*"
    r"[1-9][\d,]{0,6}\s+of\s+[1-9][\d,]{0,6}\b[\s\S]*$"
)
ROW = (
    r"(?m)^(?P<metrics>[^\n]{0,160}?)[`*]{0,3}[ \t\x{00a0}]*"
    r"(?P<code>[A-Za-z0-9]{3,16})[ \t\x{00a0}]*[`*]{1,3}"
    r"[ \t]*·[ \t]*[`*]{0,3}(?P<stars>[★☆⭐]{1,4})[`*]{0,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}#(?P<print_num>[\d,]{1,12})[ \t\x{00a0}]*[`*]{0,3}[ \t]*·[ \t]*"
    r"[`*]{0,3}[◈❖](?P<edition>\d{1,3})[`*]{0,3}[ \t]*·[ \t]*"
    r"(?P<series>[^\n·]{1,240}?)[ \t]*·[ \t]*(?P<character>[^\n]{1,250})[ \t]*$"
)


def heading(name):
    pattern = (
        r"(?i)^(?:[^A-Za-z0-9\n]{0,16}|"
        r"<a?:[A-Za-z0-9_]{1,32}:\d{17,20}>\s*)?" + name + r"$"
    )
    return MatcherExpr(
        "any",
        children=(
            MatcherExpr("regex", "embed_titles", pattern),
            MatcherExpr("regex", "embed_author_name", pattern),
        ),
    )


class KarutaCollectionsModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        cache_redis,
        store,
        artifact_root,
        temp_root,
        actions=None,
        scheduler=None,
        clock=None,
        loader=None,
        inventory_store=None,
    ):
        self.generation = generation
        self.cache = cache_redis
        self.store = store
        self.artifacts = ArtifactStore(Path(artifact_root))
        self.temp_root = Path(temp_root)
        self.actions = actions or EdgeActionClient(
            queue_redis, module=MODULE, group="cards", generation=generation
        )
        self.scheduler = scheduler or ScheduledCallbackClient(
            queue_redis, module=MODULE, group="cards", generation=generation
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.loader = loader or ArtworkLoader()
        self.renderer = CollageRenderer()
        self.maintenance = None
        self._draining = False
        self._maintenance_busy = False
        self._maintenance_wake = asyncio.Event()
        self._maintenance_wake.set()
        self.inventory = inventory_store or MemoryInventoryStore(self.store)
        self.checks = CheckUI(self)

    @property
    def manifest(self):
        passive = []
        for event in ("message_create", "message_edit"):
            common = (MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),)

            if event == "message_create":
                passive.append(
                    PassiveMatcherSpec(
                        route=f"{MODULE}.lookup_pending",
                        handler=f"{MODULE}.lookup_pending",
                        event=event,
                        bot_ids=(BOT,),
                        requires_embeds=True,
                        watch_ttl_ms=TTL * 1000,
                        subject_field="requester_id",
                        subject_required=True,
                        expression=MatcherExpr(
                            "all",
                            children=(
                                *common,
                                heading("Character Results"),
                                MatcherExpr("regex", "reply_content", KLU_COMMAND),
                            ),
                        ),
                        context_fields=("requester_id", "webhook_id"),
                    )
                )
            passive.append(
                PassiveMatcherSpec(
                    route=f"{MODULE}.lookup_{event}",
                    handler=f"{MODULE}.lookup",
                    event=event,
                    bot_ids=(BOT,),
                    requires_embeds=True,
                    hydrate=event == "message_edit",
                    watch_ttl_ms=TTL * 1000,
                    subject_field="requester_id",
                    subject_required=True,
                    expression=MatcherExpr(
                        "all",
                        children=(
                            *common,
                            heading("Character Lookup"),
                            MatcherExpr("regex", "reply_content", KLU_COMMAND),
                            MatcherExpr("exists", "primary_image_url"),
                        ),
                    ),
                    extractions=(
                        FieldExtraction(
                            "message_document",
                            LOOKUP,
                            (
                                CaptureSpec("character", "normalized_text", maximum=244),
                                CaptureSpec("series", "normalized_text", maximum=244),
                                CaptureSpec("edition", "integer", minimum=1, maximum=100),
                                CaptureSpec("edition_count", "integer", minimum=1, maximum=100),
                            ),
                        ),
                    ),
                    context_fields=(
                        "primary_image_url",
                        "source_revision_ms",
                        "event_order",
                        "requester_id",
                        "webhook_id",
                    ),
                )
            )
            passive.append(
                PassiveMatcherSpec(
                    route=f"{MODULE}.collection_{event}",
                    handler=f"{MODULE}.collection",
                    event=event,
                    bot_ids=(BOT,),
                    requires_embeds=True,
                    hydrate=event == "message_edit",
                    watch_ttl_ms=TTL * 1000,
                    subject_capture="collection_owner",
                    subject_required=True,
                    expression=MatcherExpr(
                        "all",
                        children=(
                            *common,
                            heading("Card Collection"),
                            MatcherExpr("regex", "message_document", COLLECTION),
                        ),
                    ),
                    extractions=(
                        FieldExtraction(
                            "embed_descriptions",
                            COLLECTION_OWNER,
                            (CaptureSpec("collection_owner", "snowflake"),),
                        ),
                        FieldExtraction(
                            "message_document",
                            PAGE,
                            (
                                CaptureSpec("start", "compact_integer", minimum=0, maximum=999999),
                                CaptureSpec("end", "compact_integer", minimum=0, maximum=999999),
                                CaptureSpec("total", "compact_integer", minimum=0, maximum=999999),
                                CaptureSpec(
                                    "matched_total",
                                    "compact_integer",
                                    required=False,
                                    minimum=0,
                                    maximum=999999,
                                ),
                            ),
                        ),
                    ),
                    repeated=(
                        RepeatedExtraction(
                            "embed_descriptions",
                            ROW,
                            11,
                            (
                                CaptureSpec("code", maximum=16),
                                CaptureSpec("metrics", maximum=160),
                                CaptureSpec("stars", maximum=4),
                                CaptureSpec("print_num", "compact_integer", maximum=2_147_483_647),
                                CaptureSpec("edition", "integer", minimum=1, maximum=100),
                                CaptureSpec("series", "normalized_text", maximum=240),
                                CaptureSpec("character", "normalized_text", maximum=250),
                            ),
                        ),
                    ),
                    context_fields=(
                        "source_revision_ms",
                        "event_order",
                        "requester_id",
                        "webhook_id",
                        *REPLY_FACTS,
                    ),
                )
            )
        passive.append(
            PassiveMatcherSpec(
                route=f"{MODULE}.delete",
                handler=f"{MODULE}.delete",
                event="message_delete",
                expression=MatcherExpr("exists", "message_id"),
                context_fields=("event_order",),
            )
        )
        passive.extend(check_routes.wishlist_routes(heading))
        passive.extend(check_routes.spreadsheet_routes(heading))
        return Manifest(
            schema=2,
            module=MODULE,
            group="cards",
            generation=self.generation,
            job_stream="lumi:jobs:cards",
            display_name="Karuta Collections",
            summary="Save cards, compare wishlists and view collection collages.",
            help=HelpSpec(
                "React 🖼️ on your Karuta collection.",
                "View your collection as a card collage.",
            ),
            commands=check_routes.commands(),
            components=check_routes.controls(),
            passive=tuple(passive),
            reaction_offers=(
                ReactionOfferSpec(
                    key="collection",
                    callback=f"{MODULE}.open",
                    pattern=COLLECTION_OFFER,
                    emoji="🖼️",
                    bot_ids=(BOT,),
                    requires_embeds=True,
                    audience_fields=("requester_id",),
                    require_owner=True,
                    remove_after_dispatch=False,
                    context_fields=("requester_id",),
                    presentation_mode="v2",
                    deadline_ms=60_000,
                ),
            ),
            scheduled=(
                ScheduledCallbackSpec(
                    route=f"{MODULE}.refresh",
                    handler=f"{MODULE}.refresh",
                    deadline_ms=60_000,
                    presentation_mode="v2",
                ),
            ),
            capabilities=("send_message", "edit_message", "resolve_members"),
            version="1.4.3",
        )

    @staticmethod
    def source_key(guild, source):
        return PREFIX + f"source:{guild}:{source}"

    async def close(self):


        if self.maintenance:
            self.maintenance.cancel()
            await asyncio.gather(self.maintenance, return_exceptions=True)
        await self.loader.close()
        await self.store.close()

    async def start(self):

        self.maintenance = asyncio.create_task(self._maintain())


    def begin_drain(self):
        self._draining = True
        if wake := getattr(self, "_maintenance_wake", None):
            wake.clear()


    def resume(self):
        self._draining = False
        if wake := getattr(self, "_maintenance_wake", None):
            wake.set()


    def drain_ready(self):
        return not self._maintenance_busy

    async def _maintain(self):

        await asyncio.sleep(5)
        await self._maintenance_wake.wait()
        self._maintenance_busy = True
        try:
            await self.resume_sessions()
        except (RedisError, ActionUnavailable, TimeoutError):
            LOGGER.warning("karuta_collection_resume_unavailable")
        finally:
            self._maintenance_busy = False
        while True:
            if self._draining:
                await asyncio.sleep(0.05)
                continue
            self._maintenance_busy = True
            try:
                await self.prune_attachments()
            except (RedisError, OSError, ValueError):
                LOGGER.warning("karuta_collection_cleanup_unavailable")
            finally:
                self._maintenance_busy = False
            await asyncio.sleep(30)

    async def prune_attachments(self):
        for raw in await self.cache.zrangebyscore(
            PREFIX + "attachments", "-inf", self.clock(), start=0, num=128
        ):
            reference = raw.decode() if isinstance(raw, bytes) else raw
            if len(reference) != 64 or any(c not in "0123456789abcdef" for c in reference):
                raise ValueError("invalid temporary reference")
            path = self.temp_root / reference
            if self.temp_root.resolve() not in path.resolve().parents or path.is_symlink():
                raise ValueError("unsafe temporary reference")
            await asyncio.to_thread(path.unlink, missing_ok=True)
            await self.cache.zrem(PREFIX + "attachments", raw)

    async def resume_sessions(self):
        for raw in await self.cache.zrangebyscore(
            PREFIX + "active", self.clock(), "+inf", start=0, num=MAX_ACTIVE_VIEWS
        ):
            await self._schedule(raw.decode() if isinstance(raw, bytes) else raw)

    async def _json(self, key):
        raw = await self.cache.get(key)
        return json.loads(raw) if raw else None

    async def handle(self, request, job_id, generation):
        result = ResultEnvelope(1, job_id, MODULE, "cards", self.generation, "ok")
        if (
            generation != self.generation
            or int(request.event.get("_expires_at_ms", self.clock() + 1)) <= self.clock()
        ):
            return result
        try:
            suffix = request.handler.rpartition(".")[2]
            if suffix == "spreadsheet_pending":
                return result
            if suffix == "spreadsheet":
                event = request.event
                source = spreadsheet_source(event) or {}
                if (
                    source.get("version") == 1 and source.get("owner") == event.get("actor_id")
                    and source.get("owner") == request.arguments.get("owner")
                    and source.get("url") == request.arguments.get("export_url")
                    and source.get("source") == event.get("message_id")
                    and source.get("created") == event.get("created_at_ms")
                    and event.get("author_id") == BOT and event.get("webhook_id") is None
                ):
                    lock = self.checks.locks.setdefault(
                        "spreadsheet:" + str(source["source"]), asyncio.Lock()
                    )
                    async with lock:
                        _text, ok = await self.checks.import_csv(
                            request, source=source["source"], created=source["created"],
                            export_url=source["url"],
                        )
                    if not ok:
                        LOGGER.warning("karuta_spreadsheet_import_unavailable")
                return result
            if suffix in {"kdupes", "wlcheck", "kcimport", "check_control", "import_upload"}:
                try:
                    intent = await self.checks.handle(request, job_id)
                except (
                    ValueError,
                    OSError,
                    RedisError,
                    MySQLError,
                    ActionUnavailable,
                    TimeoutError,
                ):
                    LOGGER.warning("karuta_check_unavailable", exc_info=True)
                    intent = (
                        self.checks.simple(
                            "Unavailable.", mode="legacy" if suffix == "kdupes" else "v2",
                            kind="edit" if suffix == "import_upload" else "send",
                        )
                        if suffix != "check_control"
                        else None
                    )
                return ResultEnvelope(
                    1,
                    job_id,
                    MODULE,
                    "cards",
                    self.generation,
                    "ok",
                    intents=(intent,) if intent else (),
                )
            if suffix == "wishlist":
                await self._wishlist(request)
                return result


            if suffix == "lookup_pending":

                return result
            if suffix == "delete":
                await self._delete(request)
            elif suffix == "collection":
                await self._observe(request)
            elif await self.store.ready():
                if suffix == "lookup":
                    await self._learn(request)
                elif suffix == "open":
                    await self._open(request, job_id)
                elif suffix == "refresh":
                    await self._refresh(request.arguments.get("source_key", ""), request, job_id)
        except (
            RedisError,
            MySQLError,
            ClientError,
            ActionUnavailable,
            ValueError,
            KeyError,
            TypeError,
            OSError,
            TimeoutError,
        ):
            LOGGER.warning("karuta_collection_unavailable", exc_info=True)
        return result

    def _origin(self, request):
        event = request.event
        guild, channel, source = (
            int(event.get(k) or 0) for k in ("guild_id", "channel_id", "message_id")
        )
        if min(guild, channel, source) <= 0:
            raise ValueError("invalid source context")
        return guild, channel, source

    async def _wishlist(self, request):
        _guild, _channel, source = self._origin(request)
        event, args = request.event, request.arguments
        owner = int(args["owner"])
        if (
            event.get("author_id") != BOT
            or event.get("webhook_id") is not None
            or event.get("actor_id") != owner
        ):
            raise ValueError("untrusted wishlist")
        entries = [
            dict(character=clean_text(row["character"]), series=clean_text(row["series"]))
            for row in args.get("embed_descriptions", ())
        ]
        total = args["capacity"] - args["available"]
        if total < 0 or (args.get("total") is not None and args["total"] != total):
            raise ValueError("inconsistent wishlist total")
        query = wishlist_query_context(event)
        await self.inventory.accounts.observe_wishlist(
            dict(
                owner=owner,
                source=source,
                created=int(event["created_at_ms"]),
                stamp=max(
                    int(event.get("source_revision_ms") or event["created_at_ms"]) * 1000,
                    int(event.get("event_order") or 0),
                ),
                start=args.get("start") or 0,
                end=args.get("end") or 0,
                total=total,
                entries=entries,
                verified=query.get("verified") is True and query.get("target") == owner,
                unfiltered=query.get("unfiltered") is True,
            )
        )

    def _proven(self, request):
        guild, channel, source = self._origin(request)
        if request.event.get("author_id") != BOT or request.event.get("webhook_id") is not None:
            raise ValueError("untrusted source")
        owner = int(request.event.get("requester_id") or 0)
        if owner <= 0 or request.event.get("actor_id") != owner:
            raise ValueError("unresolved requester")
        stamp = Stamp(
            int(request.event.get("source_revision_ms") or request.event.get("created_at_ms") or 0),
            int(request.event.get("event_order") or 0),
        )
        if not stamp.at_ms:
            raise ValueError("missing source clock")
        return guild, channel, source, owner, stamp

    async def _learn(self, request):
        _guild, _channel, _source, _owner, stamp = self._proven(request)
        row = request.arguments
        character, series = clean_text(row["character"]), clean_text(row["series"])
        if not character or not series:
            raise ValueError("incomplete lookup identity")
        edition, count = row["edition"], row["edition_count"]
        if type(edition) is not int or type(count) is not int or not 1 <= edition <= count <= 100:
            raise ValueError("invalid lookup edition")

        artwork = await self.loader.fetch(request.event["primary_image_url"])
        blob = await self.artifacts.store(artwork.data)
        if await self.store.learn(character, series, edition, count, blob, stamp):
            await self.artwork_changed(identity(character, series))


    async def artwork_changed(self, key):
        dependency = PREFIX + "uses:" + key
        for raw in await self.cache.smembers(dependency):
            await self._schedule(raw.decode() if isinstance(raw, bytes) else raw)

    async def _observe(self, request):
        guild, channel, source = self._origin(request)
        event = request.event
        collection_owner = int(request.arguments.get("collection_owner") or 0)
        owner = int(event.get("requester_id") or 0)
        if (
            event.get("author_id") != BOT
            or event.get("webhook_id") is not None
            or collection_owner <= 0
            or event.get("actor_id") != collection_owner
        ):
            raise ValueError("untrusted collection owner")
        stamp = Stamp(
            int(event.get("source_revision_ms") or event.get("created_at_ms") or 0),
            int(event.get("event_order") or 0),
        )
        start, end, total = (request.arguments[k] for k in ("start", "end", "total"))
        cards = tuple(
            Card.from_dict(row) for row in request.arguments.get("embed_descriptions", ())
        )
        page = CollectionPage(
            collection_owner,
            owner,
            source,
            guild,
            channel,
            int(event.get("created_at_ms") or 0),
            stamp,
            start,
            end,
            total,
            request.arguments.get("matched_total") or total,
            cards,
            collection_query_context(event),
        )
        names = await self.inventory.observe(page)
        if names is None:
            return
        if cards and owner > 0:
            await self._save_view_page(page)


    async def _save_view_page(self, page: CollectionPage):
        cards, stamp = page.cards, page.stamp
        key = self.source_key(page.guild, page.source)
        source_hash = hashlib.sha256(
            json.dumps(
                [page.start, page.end, page.visible, [asdict(card) for card in cards]], sort_keys=True
            ).encode()
        ).hexdigest()
        async with self.cache.pipeline() as pipe:
            for _ in range(8):
                try:
                    await pipe.watch(key)
                    old = await self._json(key)
                    if old and (old["deleted"] or (stamp.at_ms, stamp.order) <= tuple(old["stamp"])):
                        return
                    if old and old["owner"] != page.requester:
                        raise ValueError("source requester changed")
                    record = dict(
                        guild=page.guild,
                        channel=page.channel,
                        source=page.source,
                        owner=page.requester,
                        collection_owner=page.owner,
                        stamp=[stamp.at_ms, stamp.order],
                        hash=source_hash,
                        rows=[asdict(card) for card in cards],
                        deleted=False,
                    )
                    pipe.multi()
                    pipe.set(key, json.dumps(record), ex=TTL * 2)
                    await pipe.execute()
                    break
                except WatchError:
                    continue
            else:
                raise ValueError("source update contention")
        if old is None or old["hash"] != source_hash:
            await self._schedule(key)

    async def _delete(self, request):
        guild, channel, source = self._origin(request)
        key = self.source_key(guild, source)
        lock = self.cache.lock(key + ":lock", timeout=60, blocking_timeout=5)
        async with lock:
            record = await self._json(key)
            view = await self._json(key + ":view")
            origin = record or view
            if not origin or origin["channel"] != channel:
                return
            record = record or {"channel": channel}
            record["deleted"] = True
            await self.cache.set(key, json.dumps(record), ex=TTL * 2)
            await self.cache.delete(key + ":view")
            await self.cache.zrem(PREFIX + "active", key)

    async def _schedule(self, key):
        view = await self._json(key + ":view")
        if not view or view["expires"] <= self.clock():
            return
        revision = await self.cache.incr(key + ":revision")
        await self.cache.expire(key + ":revision", TTL * 2)
        await self.scheduler.publish(
            operation="upsert",
            schedule_key=MODULE + ":" + view["token"],
            route=f"{MODULE}.refresh",
            revision=revision,
            guild_id=view["guild"],
            channel_id=view["channel"],
            actor_id=view["owner"],
            due_at_ms=self.clock() + 50,
            expires_at_ms=min(view["expires"], self.clock() + 300_000),
            arguments={"source_key": key},
        )

    async def _open(self, request, job_id):
        guild, channel, source = self._origin(request)
        owner = int(request.event.get("offer_owner_id") or 0)
        if (
            owner <= 0
            or request.event.get("actor_id") != owner
            or request.event.get("requester_id") != owner
        ):
            return
        key = self.source_key(guild, source)
        lock = self.cache.lock(key + ":lock", timeout=60, blocking_timeout=5)
        async with lock:
            record = await self._json(key)
            if record and (
                record["deleted"] or record["owner"] != owner or record["channel"] != channel
            ):
                return
            view = await self._json(key + ":view")
            if view and view["expires"] <= self.clock():
                return
            if not view:
                await self.cache.zremrangebyscore(PREFIX + "active", "-inf", self.clock())
                if await self.cache.zcard(PREFIX + "active") >= MAX_ACTIVE_VIEWS:
                    return
                view = dict(
                    token=secrets.token_hex(8),
                    guild=guild,
                    channel=channel,
                    source=source,
                    owner=owner,
                    expires=self.clock() + TTL * 1000,
                    message=None,
                    applied=None,
                    delivery_sequence=0,
                    pending=None,
                )
                await self.cache.set(key + ":view", json.dumps(view), ex=TTL)
                await self.cache.zadd(PREFIX + "active", {key: view["expires"]})


        await self._refresh(key, request, job_id)

    async def _refresh(self, key, request, job_id):
        if not isinstance(key, str) or not key.startswith(PREFIX + "source:"):
            return
        lock = self.cache.lock(key + ":lock", timeout=60, blocking_timeout=5)
        async with lock:
            source, view = await self._json(key), await self._json(key + ":view")
            if not source or not view or source["deleted"] or view["expires"] <= self.clock():
                return
            if (source["owner"], source["channel"]) != (view["owner"], view["channel"]):
                return
            event = request.event
            if (event.get("actor_id"), event.get("guild_id"), event.get("channel_id")) != (
                view["owner"],
                view["guild"],
                view["channel"],
            ):
                return
            member = await self.actions.request(
                job_id=job_id,
                kind="resolve_members",
                guild_id=view["guild"],
                channel_id=view["channel"],
                member_ids=(view["owner"],),
                idempotency_key=f"kc-member:{view['token']}:{job_id}",
            )
            people = member.data.get("members", ()) if member.ok else ()
            if not any(
                person["id"] == view["owner"]
                and not person.get("bot")
                and not person.get("blocked")
                and person.get("allowed", False)
                for person in people
            ):
                return
            cards = tuple(Card.from_dict(row) for row in source["rows"])
            resolved = await self.store.resolved_images(cards)

            images = []
            for canonical, blob in resolved:
                dependency = PREFIX + "uses:" + canonical
                await self.cache.sadd(dependency, key)
                await self.cache.expire(dependency, TTL * 2)
                try:
                    images.append(await self.artifacts.read(blob) if blob else None)
                except (OSError, ValueError):
                    images.append(None)
            render_hash, data = await self.renderer.render(tuple(images))
            latest = await self._json(key)
            if (
                not latest
                or latest["deleted"]
                or latest["hash"] != source["hash"]
                or not await lock.owned()
            ):
                await self._schedule(key)
                return
            if view["applied"] == render_hash and view["message"]:
                return
            if view.get("pending") != render_hash:
                view["delivery_sequence"] = view.get("delivery_sequence", 0) + 1
                view["pending"] = render_hash
                await self.cache.set(
                    key + ":view",
                    json.dumps(view),
                    ex=max(1, (view["expires"] - self.clock()) // 1000),
                )
            reference = secrets.token_hex(32)
            self.temp_root.mkdir(parents=True, exist_ok=True)
            path = self.temp_root / reference
            await asyncio.to_thread(path.write_bytes, data)
            path.chmod(0o640)
            await self.cache.zadd(PREFIX + "attachments", {reference: self.clock() + 120_000})
            attachment = TemporaryAttachment(
                "tmp:" + reference,
                "collection-" + render_hash[:16] + ".webp",
                hashlib.sha256(data).hexdigest(),
                len(data),
                self.clock() + 120_000,
            )
            intent = DiscordIntent(
                kind="send" if not view["message"] else "edit",
                pages=(PresentationPage(file_name=attachment.file_name),),
                attachments=(attachment,),
                session_id=view["token"],
                presentation_mode="v2",
            )
            response = await self.actions.request(
                job_id=job_id,
                kind="send_message" if not view["message"] else "edit_message",
                guild_id=view["guild"],
                channel_id=view["channel"],
                message_id=view["message"],
                intent=intent,
                idempotency_key=f"kc:{view['token']}:"
                + ("open" if not view["message"] else str(view["delivery_sequence"])),
            )
            if not response.ok or not await lock.owned():
                return
            view["message"] = view["message"] or response.data.get("message_id")
            if not view["message"]:
                return
            view["applied"] = render_hash
            view["pending"] = None
            remaining = max(1, (view["expires"] - self.clock()) // 1000)
            await self.cache.set(key + ":view", json.dumps(view), ex=remaining)
            latest = await self._json(key)
            if latest and latest["hash"] != source["hash"]:
                await self._schedule(key)
