from __future__ import annotations

from lumi.transport import response_state_key
from lumi.ui_keys import ui_key

import asyncio
import contextlib
import hashlib
import logging
import secrets
import time
from pathlib import Path

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import LockError, RedisError, ResponseError

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
    TemporaryAttachment,
    TypedRequest,
    decode,
    encode,
)
from lumi.worker.actions import ActionUnavailable, EdgeActionClient

from .models import BOT_IDS, DAY_MS, GAMES, LABELS, RANGES, DropRecord, day_start
from .observations import detail_from_request
from .patterns import passive_routes
from .renderer import PillowChartRenderer
from .statistics import METRICS, VIEWS, DropDetail
from .stats_renderer import PAGE_SIZE, StatsRenderer
from .stats_store import MemoryStatsStore

LOGGER = logging.getLogger(__name__)
MODULE = "drop_counts"
PREFIX = "lumi:drop-counts:"
STREAM = PREFIX + "observations"
CONSUMERS = "drop-count-writers"
SESSION_MS = 900_000


class ChartSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner: int
    guild: int
    channel: int
    end_ms: int
    expires_ms: int
    game: str = "all"
    days: int = 7
    revision: int = 0
    last_job: str = ""
    pending: bool = False
    previous: tuple[str, int, int, int] | None = None
    view: str = "counts"
    target: int = 0
    target_label: str = ""
    metric: str = "drops"
    page: int = 0
    previous_extra: tuple[str, int, str, int] | None = None


class DropCountsModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        cache_redis,
        store,
        temp_root,
        renderer=None,
        actions=None,
        clock=None,
        stats_store=None,
    ):
        self.generation, self.queue, self.cache = generation, queue_redis, cache_redis
        self.store, self.temp_root = store, Path(temp_root)
        self.renderer = renderer or PillowChartRenderer()
        self.stats = stats_store or MemoryStatsStore()
        self.stats_renderer = StatsRenderer(getattr(self.renderer, "lock", None))
        self.report_cache = {}

        self.actions = actions or EdgeActionClient(
            queue_redis, module=MODULE, group="cards", generation=generation
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.consumer = f"{generation}-{secrets.token_hex(8)}"
        self.task = None
        self.draining = False
        self.writing = False

    @property
    def manifest(self):
        def control(action, input_kind="none"):
            return ComponentSpec(
                MODULE,
                action,
                MODULE + ".component",
                grammar=r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$",
                captures=(CaptureSpec("session"), CaptureSpec("revision", "integer", minimum=0)),
                input_kind=input_kind,
                acl_key="drops",
                public=True,
                session_bound=True,
                deadline_ms=60_000,
            )

        return Manifest(
            schema=2,
            module=MODULE,
            group="cards",
            generation=self.generation,
            job_stream="lumi:jobs:cards",
            display_name="Drop Counts",
            summary="Server-wide drop counts, card statistics and player rankings.",
            commands=(
                CommandSpec(
                    root="drops",
                    aliases=("dropstats",),
                    handler=MODULE + ".open",
                    grammar=(
                        r"(?i)^\s*(?:(?P<view>stats|user|lb)(?:\s+(?:<@!?(?P<mention>\d{17,20})>|(?P<user_id>\d{17,20})))?\s*)?"
                        r"(?P<game>all|glitch|karuta|sofi|help)?\s*$"
                    ),
                    captures=(
                        CaptureSpec("view", "normalized_text", required=False, maximum=5),
                        CaptureSpec("mention", "snowflake", required=False),
                        CaptureSpec("user_id", "snowflake", required=False),
                        CaptureSpec("game", "normalized_text", required=False, maximum=6),
                    ),
                    help=HelpSpec(
                        ".drops stats [game]\n.drops user [@user|ID] [game]\n.drops lb [game]",
                        "Drop counts and rankings for Karuta, Sofi, Glitch or all games."
                    ),
                    acl_key="drops",
                    public=True,
                    deadline_ms=60_000,
                    context_fields=("prefix", "entity_labels", "author_name"),
                ),
            ),
            passive=passive_routes(),
            components=(
                control("game", "string_values"),
                control("range", "string_values"),
                control("previous"),
                control("next"),
                control("latest"),
                control("refresh"),
                control("view", "string_values"),
                control("metric", "string_values"),
            ),
            capabilities=("edit_message",),
            version="2.0.2",
        )

    async def start(self):
        self.temp_root.mkdir(parents=True, exist_ok=True)
        await self.stats.activate(self.clock())
        await self._ensure_stream()
        self.task = asyncio.create_task(self._writer(), name="drop-count-writer")

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

        await self.renderer.close()
        await self.stats_renderer.close()
        await self.store.close()

    def begin_drain(self):
        self.draining = True

    def resume(self):
        self.draining = False

    def drain_ready(self):
        return not self.writing

    async def _ensure_stream(self):
        try:
            await self.queue.xgroup_create(STREAM, CONSUMERS, id="0-0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _write_batch(self, entries):
        identifiers, records, details = [], [], []
        for identifier, fields in entries:
            identifiers.append(identifier)
            payload = fields.get(b"drop") or fields.get("drop")
            try:
                record = decode(payload, DropRecord) if payload and len(payload) <= 512 else None
            except (TypeError, ValueError, msgspec.DecodeError):
                record = None
            if record and record.valid():
                records.append(record)
                payload = fields.get(b"detail") or fields.get("detail")
                try:
                    detail = (
                        decode(payload, DropDetail) if payload and len(payload) <= 16_384 else None
                    )
                except (TypeError, ValueError, msgspec.DecodeError):
                    detail = None
                if detail and detail.valid() and detail.source == record:
                    details.append(detail)

        await self.store.record(tuple(records))
        for detail in details:
            try:
                await self.stats.record(detail)
            except ValueError:
                LOGGER.warning("drop_detail_rejected")
                continue

        if details:
            self.report_cache.clear()
        if identifiers:
            pipeline = self.queue.pipeline(transaction=True)
            pipeline.xack(STREAM, CONSUMERS, *identifiers)
            pipeline.xdel(STREAM, *identifiers)
            await pipeline.execute()

    async def _writer(self):
        failures = 0
        while True:
            if self.draining:
                await asyncio.sleep(0.2)
                continue
            try:
                reclaimed = await self.queue.xautoclaim(
                    STREAM,
                    CONSUMERS,
                    self.consumer,
                    min_idle_time=30_000,
                    start_id="0-0",
                    count=100,
                )
                entries = reclaimed[1]
                if not entries:
                    batches = await self.queue.xreadgroup(
                        CONSUMERS, self.consumer, {STREAM: ">"}, count=100, block=2000
                    )
                    entries = batches[0][1] if batches else ()
                if self.draining:

                    continue
                if entries:
                    self.writing = True
                    try:
                        await self._write_batch(entries)
                    finally:
                        self.writing = False
                failures = 0


                await asyncio.sleep(0)
            except (OSError, RedisError, MySQLError, TimeoutError):
                failures += 1
                LOGGER.warning("drop_count_writer_unavailable")
                await asyncio.sleep(min(30, 2 ** min(failures, 5)))

    async def handle(self, request: TypedRequest, job_id: str, generation: int):
        if generation != self.generation or self.clock() >= int(
            request.event.get("_expires_at_ms", self.clock() + 1)
        ):
            return self._result(job_id)
        if request.handler.startswith(MODULE + ".observe_"):
            await self._observe(request)
            return self._result(job_id)
        try:
            if request.handler == MODULE + ".open":
                return self._result(job_id, await self._open(request))
            if request.handler == MODULE + ".component":
                return self._result(job_id, await self._component(request, job_id))
        except (
            OSError,
            ValueError,
            RedisError,
            MySQLError,
            ActionUnavailable,
            TimeoutError,
        ) as exc:
            LOGGER.warning("drop_counts_view_unavailable error_type=%s", type(exc).__name__)
            return self._result(job_id, self._notice(request, "Unavailable."))
        return self._result(job_id)

    async def _observe(self, request):
        game = request.handler.removeprefix(MODULE + ".observe_")
        event = request.event
        if (
            game not in GAMES
            or event.get("author_id") != BOT_IDS[game]
            or event.get("author_bot") is not True
            or event.get("webhook_id") is not None
            or event.get("dispatch") != "passive"
        ):
            return
        try:
            record = DropRecord(
                int(event.get("guild_id") or 0),
                int(event.get("channel_id") or 0),
                int(event.get("message_id") or 0),
                game,
            )
        except (ValueError, TypeError):
            return
        if not record.valid() or record.occurred_at_ms > self.clock() + 60_000:
            return


        if self.stats.cutoff_ms is None:
            await self.stats.activate(self.clock())
        fields = {"drop": encode(record)}
        if record.occurred_at_ms >= self.stats.cutoff_ms:
            try:
                detail = detail_from_request(record, request)
                fields["detail"] = encode(detail)

            except (ValueError, TypeError, KeyError):
                LOGGER.warning("drop_detail_rejected")
        await self.queue.xadd(STREAM, fields)

    async def _open(self, request):
        game = str(request.arguments.get("game") or "all").strip().casefold()
        view = str(request.arguments.get("view") or "counts").casefold()
        if game == "help":
            prefix = str(request.event.get("prefix") or ".")
            return self._notice(
                request,
                f"`{prefix}drops [game]` · graphs\n"
                f"`{prefix}drops stats [game]` · server\n"
                f"`{prefix}drops user [@user|id] [game]` · player\n"
                f"`{prefix}drops lb [game]` · rankings\n"
                "Glitch · Karuta · Sofi. UTC. New drops only; all tracked channels.",
            )
        if game not in ("all", *GAMES):
            raise ValueError("Invalid game.")
        if view not in VIEWS or (
            view != "user"
            and (request.arguments.get("mention") or request.arguments.get("user_id"))
        ):
            raise ValueError("Invalid player selection.")
        owner, guild, channel = (
            int(request.event.get(key) or 0) for key in ("actor_id", "guild_id", "channel_id")
        )
        if min(owner, guild, channel) <= 0:
            raise ValueError("Server context required.")
        target = int(request.arguments.get("mention") or request.arguments.get("user_id") or owner)
        labels = dict(request.event.get("entity_labels", ()))
        label = (
            labels.get(f"<@{target}>")
            or labels.get(f"<@!{target}>")
            or (request.event.get("author_name") if target == owner else "")
            or ""
        )
        session = ChartSession(
            secrets.token_hex(8),
            owner,
            guild,
            channel,
            day_start(self.clock()) + DAY_MS,
            self.clock() + SESSION_MS,
            game=game,
            view=view,
            target=target,
            target_label=str(label).removeprefix("@")[:80],
        )
        data, first_day = await self._chart(session)
        intent = await self._intent(session, data, first_day, "send")
        await self._save(session)
        return intent

    async def _save(self, session):
        remaining = session.expires_ms - self.clock()
        if remaining <= 0:
            raise ValueError("Expired session.")
        pipeline = self.cache.pipeline(transaction=True)
        pipeline.set(PREFIX + "session:" + session.token, encode(session), px=remaining)
        pipeline.set(ui_key("owner", session.token), str(session.owner), px=remaining)
        await pipeline.execute()

    async def _component(self, request, job_id):
        token = request.arguments.get("session", "")
        if (
            not isinstance(token, str)
            or len(token) != 16
            or any(c not in "0123456789abcdef" for c in token)
        ):
            return self._notice(request, "Expired.")
        lock = self.cache.lock(PREFIX + "lock:" + token, timeout=45, blocking_timeout=2)
        if not await lock.acquire():
            return self._notice(request, "Busy. Try again.")
        try:
            raw = await self.cache.get(PREFIX + "session:" + token)
            session = decode(raw, ChartSession) if raw else None
            bound = await self.cache.get(ui_key("message", token))
            if (
                session is None
                or session.expires_ms <= self.clock()
                or session.owner != request.event.get("actor_id")
                or session.guild != request.event.get("guild_id")
                or session.channel != request.event.get("channel_id")
                or bound is None
                or int(bound) != request.event.get("message_id")
            ):
                return self._notice(request, "Expired.")
            if not session.pending and session.last_job == job_id:
                return None
            if session.pending:


                response = await self._deliver(session, job_id, int(bound), lock)
                if not response or not response.ok:
                    if response and getattr(response, "error_code", "") != "ambiguous":
                        await self._rollback(session, lock)
                    return self._notice(request, "Unavailable.")
                await self._save(
                    msgspec.structs.replace(
                        session, pending=False, previous=None, previous_extra=None
                    )
                )
                return (
                    None
                    if session.last_job == job_id
                    else self._notice(request, "Updated. Try again.")
                )
            if request.arguments.get("revision") != session.revision:
                return self._notice(request, "Updated. Try again.")
            changed = await self._change(session, request)
            changed = msgspec.structs.replace(
                changed,
                revision=session.revision + 1,
                last_job=job_id,
                pending=True,
                previous=(session.game, session.days, session.end_ms, session.revision),
                previous_extra=(session.view, session.target, session.metric, session.page),
            )
            data, first = await self._chart(changed)
            if not await lock.owned() or changed.expires_ms <= self.clock():
                return self._notice(request, "Expired.")
            await self._save(changed)
            response = await self._deliver(changed, job_id, int(bound), lock, data, first)
            if response and response.ok and await lock.owned():
                await self._save(
                    msgspec.structs.replace(
                        changed, pending=False, previous=None, previous_extra=None
                    )
                )
                return None
            if response and getattr(response, "error_code", "") != "ambiguous":
                await self._rollback(changed, lock)
            return self._notice(request, "Unavailable.")
        finally:
            with contextlib.suppress(LockError):
                await lock.release()

    async def _rollback(self, session, lock):
        if session.previous is None or not await lock.owned():
            return
        game, days, end, revision = session.previous
        view, target, metric, page = session.previous_extra or (
            session.view,
            session.target,
            session.metric,
            session.page,
        )
        await self._save(
            msgspec.structs.replace(
                session,
                game=game,
                days=days,
                end_ms=end,
                revision=revision,
                pending=False,
                previous=None,
                previous_extra=None,
                view=view,
                target=target,
                metric=metric,
                page=page,
            )
        )

    async def _change(self, session, request):
        action = request.route.rpartition(".")[2]
        if action in {"game", "range", "view", "metric"}:
            values = request.arguments.get("values")
            options = {
                "game": ("all", *GAMES),
                "range": tuple(map(str, RANGES)) + (("0",) if session.view != "counts" else ()),
                "view": tuple(VIEWS),
                "metric": tuple(METRICS) if session.game != "all" else ("drops", "cards"),
            }[action]
            if (
                not isinstance(values, (tuple, list))
                or len(values) != 1
                or values[0] not in options
            ):
                raise ValueError("Invalid selection.")
            field = "days" if action == "range" else action
            changes = {field: int(values[0]) if action == "range" else values[0], "page": 0}
            if action == "view" and values[0] == "counts" and session.days == 0:
                changes["days"] = 7
            if action == "view" and values[0] == "user" and not session.target:
                changes["target"] = session.owner
            if action == "game" and values[0] == "all" and session.metric not in {"drops", "cards"}:
                changes["metric"] = "drops"
            return msgspec.structs.replace(session, **changes)
        if action == "latest":
            return msgspec.structs.replace(session, end_ms=day_start(self.clock()) + DAY_MS, page=0)
        if action in {"previous", "next"}:
            if session.view == "lb":
                report = await self._report(session)
                pages = max(
                    1, (len(report.leaderboard(session.metric)) + PAGE_SIZE - 1) // PAGE_SIZE
                )
                return msgspec.structs.replace(
                    session,
                    page=max(0, min(pages - 1, session.page + (-1 if action == "previous" else 1))),
                )
            if session.days == 0:
                return session
            first = (
                await self.store.first_day(session.guild)
                if session.view == "counts"
                else day_start(self.stats.cutoff_ms or self.clock())
            )
            if action == "previous" and (
                first is None or session.end_ms - session.days * DAY_MS <= first
            ):
                return session
            end = session.end_ms + (-1 if action == "previous" else 1) * session.days * DAY_MS
            return msgspec.structs.replace(
                session, end_ms=min(end, day_start(self.clock()) + DAY_MS)
            )
        if action == "refresh":
            self.report_cache.clear()
            return session
        raise ValueError("Invalid action.")

    async def _chart(self, session):
        if session.view != "counts":
            report = await self._report(session)
            data = await self.stats_renderer.render(
                report,
                view=session.view,
                game=session.game,
                target=session.target or session.owner,
                metric=session.metric,
                page=session.page,
                label=session.target_label,
            )
            pages = (
                max(1, (len(report.leaderboard(session.metric)) + PAGE_SIZE - 1) // PAGE_SIZE)
                if session.view == "lb"
                else day_start(report.cutoff_ms)
            )
            return data, pages
        rows = await self.store.chart(
            session.guild, session.end_ms - session.days * DAY_MS, session.days
        )
        first = await self.store.first_day(session.guild)
        return await self.renderer.render(rows, session.game), first

    async def _report(self, session):
        if self.stats.cutoff_ms is None:
            await self.stats.activate(self.clock())
        start = max(
            self.stats.cutoff_ms, session.end_ms - session.days * DAY_MS if session.days else 0
        )
        key = (
            session.guild,
            start,
            session.end_ms,
            session.game,
            (session.target or session.owner) if session.view == "user" else None,
            session.view == "lb",
        )
        cached = self.report_cache.get(key)
        if cached and cached[0] > self.clock():
            return cached[1]
        async with asyncio.timeout(30):
            report = await self.stats.report(*key[:5], leaderboard=key[5])
        self.report_cache[key] = (self.clock() + 5000, report)
        while len(self.report_cache) > 32:
            del self.report_cache[next(iter(self.report_cache))]
        return report

    async def _deliver(self, session, job_id, message_id, lock, data=None, first=None):
        if data is None:
            data, first = await self._chart(session)
        if not await lock.owned() or self.clock() >= session.expires_ms:
            return False
        intent = await self._intent(session, data, first, "edit")
        response = await self.actions.request(
            job_id=job_id,
            kind="edit_message",
            guild_id=session.guild,
            channel_id=session.channel,
            message_id=message_id,
            intent=intent,
            idempotency_key=f"drop-counts:{session.token}:{session.revision}:{session.last_job}",
            wait_seconds=20,
        )
        if response.ok:
            key = response_state_key(job_id, "cards")
            state = await self.cache.get(key)
            if state in {f"queued:{message_id}", f"queued:{message_id}".encode()}:
                await self.cache.set(key, "final", ex=180)
        return response

    async def _intent(self, session, data, first, kind):
        reference = secrets.token_hex(32)
        path = self.temp_root / reference
        await asyncio.to_thread(path.write_bytes, data)
        path.chmod(0o640)
        digest = hashlib.sha256(data).hexdigest()
        attachment = TemporaryAttachment(
            "tmp:" + reference,
            "drop-counts-" + digest[:16] + ".png",
            digest,
            len(data),
            self.clock() + 120_000,
        )
        payload = f"{session.token}~{session.revision}"

        def button(action, label, disabled=False):
            return PresentationControl(
                "button", MODULE, action, payload, label=label, disabled=disabled
            )

        controls = [
            PresentationControl(
                "string_select",
                MODULE,
                "view",
                payload,
                placeholder="View",
                options=tuple(
                    PresentationOption(label, view, default=session.view == view)
                    for view, label in VIEWS.items()
                ),
            ),
            PresentationControl(
                "string_select",
                MODULE,
                "game",
                payload,
                placeholder="Game",
                options=tuple(
                    PresentationOption(
                        "All games" if game == "all" else LABELS[game],
                        game,
                        default=session.game == game,
                    )
                    for game in ("all", *GAMES)
                ),
            ),
            PresentationControl(
                "string_select",
                MODULE,
                "range",
                payload,
                placeholder="Range",
                options=tuple(
                    PresentationOption(
                        f"{days} days" if days else "All time",
                        str(days),
                        default=session.days == days,
                    )
                    for days in (*RANGES, *((0,) if session.view != "counts" else ()))
                ),
            ),
        ]
        if session.view == "lb":
            controls.append(
                PresentationControl(
                    "string_select",
                    MODULE,
                    "metric",
                    payload,
                    placeholder="Metric",
                    options=tuple(
                        PresentationOption(label, metric, default=session.metric == metric)
                        for metric, label in METRICS.items()
                        if session.game != "all" or metric in {"drops", "cards"}
                    ),
                )
            )
        controls.extend(
            (
                button(
                    "previous",
                    "Previous",
                    session.page == 0
                    if session.view == "lb"
                    else session.days == 0
                    or first is None
                    or session.end_ms - session.days * DAY_MS <= first,
                ),
                button(
                    "latest",
                    "First" if session.view == "lb" else "Latest",
                    session.page == 0
                    if session.view == "lb"
                    else session.end_ms >= day_start(self.clock()) + DAY_MS,
                ),
                button(
                    "next",
                    "Next",
                    session.page >= first - 1
                    if session.view == "lb"
                    else session.days == 0 or session.end_ms >= day_start(self.clock()) + DAY_MS,
                ),
                button("refresh", "Refresh"),
            )
        )
        return DiscordIntent(
            kind,
            pages=(PresentationPage(file_name=attachment.file_name, controls=tuple(controls)),),
            attachments=(attachment,),
            session_id=session.token,
            presentation_mode="v2",
        )

    @staticmethod
    def _notice(request, text):
        return DiscordIntent(
            "ephemeral" if request.event.get("dispatch") == "component" else "send",
            pages=(PresentationPage(description=text),),
        )

    def _result(self, job_id, intent=None):
        return ResultEnvelope(
            1,
            job_id,
            MODULE,
            "cards",
            self.generation,
            "ok",
            intents=(intent,) if intent else (),
            completed_at_ms=self.clock(),
        )
