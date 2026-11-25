from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import time
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import discord
import msgspec
from redis.exceptions import RedisError

from lumi.contracts import CommandSpec, HelpSpec, JobEnvelope, PresentationPage
from lumi.transport import response_state_key

from lumi.shared.broker import QueueUnavailable, RedisBroker
from .control_state import read_block_receipt
from .message_fetches import UNKNOWN_MESSAGE, fetch_message, known_message
from .pending import random_loading_animation
from .registry import module_colour

if TYPE_CHECKING:
    from .bot import LumiEdge
    from .matcher import MatchedRoute

LOGGER = logging.getLogger(__name__)
NATIVE_HELP = {
    "help": HelpSpec(".help [search]", "Find commands and tools by name, module or keyword."),
    "lumi": HelpSpec(".lumi", "About Lumi and its invite link."),
    "ping": HelpSpec(".ping", "Show Gateway latency and worker status."),
}
NATIVE_COMMANDS = {
    root: CommandSpec(
        root=root,
        handler=f"edge.{root}",
        grammar="^.*$",
        guild_only=False,
        help=NATIVE_HELP.get(root),
    )
    for root in ("help", "lumi", "ping", "block", "unblock", "restart", "drain", "maintenance")
}
OWNER_NATIVE_COMMANDS = frozenset({"block", "unblock", "restart", "drain", "maintenance"})
RECORDS_KEY = "lumi:command-loading:pending"
GRACE_SECONDS = 900


CLAIM_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('SET', KEYS[3], 'queueing', 'EX', ARGV[3])
return 1
"""


class LoadingRecord(msgspec.Struct):
    job: JobEnvelope
    source_id: int
    message_id: int = 0
    attachment_id: int = 0
    phase: str = "posting"


def response_ttl(job: JobEnvelope) -> int:
    remaining = getattr(job, "expires_at_ms", 0) - int(time.time() * 1000)
    return max(0, (remaining + 999) // 1000) + GRACE_SECONDS


class CommandLoading:
    def __init__(self, edge: LumiEdge) -> None:
        self.edge = edge
        self.records: dict[str, LoadingRecord] = {}
        self.sources: dict[int, str] = {}
        self.completed_sources: dict[int, discord.Message] = {}
        self.retry_at: dict[str, float] = {}
        self.retired_locks: set[str] = set()
        self.recovering = False
        self.ready = asyncio.Event()

    def arm_recovery(self) -> None:
        self.recovering = True

    async def wait_recovered(self) -> None:
        if self.recovering:
            await self.ready.wait()

    @staticmethod
    async def _cache(operation: Awaitable[Any]) -> Any:


        async with asyncio.timeout(2):
            return await operation

    def for_source(self, source_id: int) -> LoadingRecord | None:
        return self.records.get(self.sources.get(source_id, ""))

    def manages(self, job_id: str) -> bool:
        return job_id in self.records

    async def block_notice_allowed(self, job: JobEnvelope) -> bool:
        record = self.records.get(job.job_id)
        return bool(
            record and record.message_id and record.job == job and record.phase == "submitting"
            and job.request.event.get("_actor_admission") is True
            and await read_block_receipt(self.edge.queue_redis, job)
        )

    async def deliver_block_notice(self, job, intent) -> bool:

        record = self.records.get(job.job_id)
        if not record or not record.message_id or record.job != job:
            return False
        if (
            intent.kind != "send" or intent.session_id or intent.controls or intent.mention_actor
            or intent.attachments or intent.attachment_ref
            or (
                (not intent.content or bool(intent.pages)) if intent.presentation_mode == "plain"
                else (len(intent.pages) != 1 or not intent.pages[0].description or intent.pages[0]
                      != PresentationPage(description=intent.pages[0].description))
            )
        ):
            return False
        pending = self.edge._pending_messages.get(job.job_id)
        if pending is None:
            pending = await self._message(record, inspect=False)
        if pending is None or pending.id != record.message_id:
            return False
        await self.delivering(job)
        try:
            await pending.edit(**self.edge._intent_kwargs(intent, job, for_edit=True))
        except discord.NotFound as exc:
            if exc.code != UNKNOWN_MESSAGE:
                raise

        return True

    def _remember(self, record: LoadingRecord) -> None:
        self.records[record.job.job_id] = record
        self.sources[record.source_id] = record.job.job_id
        self.edge._pending_jobs[record.job.job_id] = record.job

    async def _save(self, record: LoadingRecord) -> None:
        await self._cache(
            self.edge.cache_redis.hset(RECORDS_KEY, record.job.job_id, msgspec.json.encode(record))
        )

    def _kwargs(self, job: JobEnvelope) -> dict[str, Any]:
        accent = module_colour(self.edge.registry.snapshot, job.module)
        mode = self.edge._job_presentation_mode(job)
        modes = self.edge._job_presentation_modes(job)
        kwargs: dict[str, Any] = {
            "file": discord.File(io.BytesIO(random_loading_animation()), "lumi-loading.gif"),
            "allowed_mentions": discord.AllowedMentions.none(),
        }
        if mode == "v2" and len(modes) <= 1:
            view = discord.ui.LayoutView(timeout=None)
            view.add_item(
                self.edge.layouts.container(
                    PresentationPage(thumbnail_url="attachment://lumi-loading.gif"),
                    accent_color=accent,
                )
            )
            kwargs["view"] = view
        else:


            kwargs["embed"] = self.edge.layouts.legacy_embed(
                PresentationPage(thumbnail_url="attachment://lumi-loading.gif"),
                accent_color=accent,
            )
        return kwargs

    async def begin(self, route: MatchedRoute, message: discord.Message) -> bool:

        if not route.loading:
            return True
        if self.for_source(message.id) is not None:
            return False
        job = RedisBroker.prepare(
            route,
            actor_id=message.author.id,
            guild_id=message.guild.id if message.guild else None,
            channel_id=message.channel.id,
        )
        record = LoadingRecord(job, message.id)
        claimed = await self._cache(
            self.edge.cache_redis.eval(
                CLAIM_LUA,
                3,
                f"lumi:command-loading:source:{message.id}",
                RECORDS_KEY,
                response_state_key(job.job_id, job.group),
                job.job_id,
                msgspec.json.encode(record),
                response_ttl(job),
            )
        )
        if not claimed:
            return False
        self._remember(record)
        async with self.edge._response_locks.setdefault(job.job_id, asyncio.Lock()):
            kwargs = self._kwargs(job)
            try:
                async with asyncio.timeout(route.deadline_ms / 1000):
                    pending = await message.reply(
                        **kwargs, mention_author=False, nonce=job.job_id[:25]
                    )
                record.message_id = pending.id
                record.attachment_id = next(
                    (attachment.id for attachment in pending.attachments), 0
                )
                record.phase = "bound"
                self.edge._pending_messages[job.job_id] = pending
                pipeline = self.edge.cache_redis.pipeline(transaction=True)
                pipeline.hset(RECORDS_KEY, job.job_id, msgspec.json.encode(record))
                pipeline.set(
                    response_state_key(job.job_id, job.group),
                    f"queued:{pending.id}",
                    ex=response_ttl(job),
                )
                await self._cache(pipeline.execute())
                return True
            except (discord.HTTPException, RedisError, TimeoutError) as error:

                await self.finish(job, text="Unavailable. Try again.")
                if (
                    not record.message_id
                    and isinstance(error, discord.HTTPException)
                    and error.status in {400, 403}
                ):


                    with contextlib.suppress(discord.HTTPException):
                        await message.reply(
                            "Unavailable. Try again.",
                            mention_author=False,
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                return False
            finally:
                kwargs["file"].close()

    async def submit(self, route: MatchedRoute, message: discord.Message) -> bool:
        record = self.for_source(message.id)
        if record is None:
            if not await self.begin(route, message):
                return False
            record = self.for_source(message.id)
        if record is None:
            raise QueueUnavailable("command response is unavailable")
        if record.phase != "bound":
            return False
        original = record.job
        if (original.module, original.generation, original.request.handler) != (
            route.module,
            route.generation,
            route.request.handler,
        ):
            raise QueueUnavailable("command registration changed")
        async with self.edge._response_locks.setdefault(original.job_id, asyncio.Lock()):
            if self.records.get(original.job_id) is not record:
                return False
            if not self.edge._pending_allowed(original):
                raise QueueUnavailable("command expired")


            record.job = RedisBroker.prepare(
                route,
                actor_id=original.actor_id,
                guild_id=original.guild_id,
                channel_id=original.channel_id,
                now_ms=original.admitted_at_ms,
                job_id=original.job_id,
            )
            record.phase = "submitting"
            self.edge._pending_jobs[original.job_id] = record.job
            await self._save(record)
            await self.edge.broker.admit(record.job, stream=route.stream)
        return True

    async def reply(
        self, source: discord.Message, kwargs: dict[str, Any]
    ) -> discord.Message | None:

        record = self.for_source(source.id)
        if record is None:
            previous = self.completed_sources.get(source.id)
            if previous is not None:
                try:
                    sent = await previous.edit(content=None, embeds=[], attachments=[], **kwargs)
                except discord.NotFound as exc:
                    if exc.code != UNKNOWN_MESSAGE:
                        raise
                    sent = await source.reply(**kwargs, mention_author=False)
                self.edge._bind_view(kwargs, sent)
                self.completed_sources[source.id] = sent
                return sent
            return None
        pending = self.edge._pending_messages.get(record.job.job_id)
        async with self.edge._response_locks.setdefault(record.job.job_id, asyncio.Lock()):
            if self.records.get(record.job.job_id) is not record:
                return pending or await self._message(record, inspect=False)
            record.phase = "delivering"
            await self._save(record)
            if pending is None:
                pending = await self._message(record, inspect=False)
            if pending is None:
                sent = await source.reply(**kwargs, mention_author=False)
            else:
                try:
                    sent = await pending.edit(content=None, embeds=[], attachments=[], **kwargs)
                except discord.NotFound as exc:
                    if exc.code != UNKNOWN_MESSAGE:
                        raise
                    sent = await source.reply(**kwargs, mention_author=False)
            self.edge._bind_view(kwargs, sent)
            await self.finish(record.job, applied=True)
            self.completed_sources[source.id] = sent
            return sent

    async def delivering(self, job: JobEnvelope) -> None:
        record = self.records.get(job.job_id)
        if record is not None:
            record.phase = "delivering"
            await self._save(record)

    async def _message(self, record: LoadingRecord, *, inspect=True) -> discord.Message | discord.PartialMessage | None:
        channel = self.edge.get_channel(record.job.channel_id or 0)
        if channel is None and record.job.channel_id:
            channel = await self.edge.fetch_channel(record.job.channel_id)
        if channel is None:
            return None
        if record.message_id:
            if not inspect:
                return known_message(self.edge, channel, record.message_id)
            try:
                return await fetch_message(self.edge, channel, record.message_id)
            except discord.NotFound:
                return None


        async for message in channel.history(
            limit=100, after=discord.Object(id=record.source_id), oldest_first=True
        ):
            reference = getattr(message, "reference", None)
            if (
                self.edge.user is not None
                and message.author.id == self.edge.user.id
                and (
                    getattr(message, "nonce", None) == record.job.job_id[:25]
                    or (reference and reference.message_id == record.source_id)
                )
                and self._still_loading(record, message)
            ):
                record.message_id = message.id
                record.attachment_id = next(
                    (a.id for a in message.attachments if a.filename == "lumi-loading.gif"), 0
                )
                return message
        return None

    @staticmethod
    def _still_loading(record: LoadingRecord, message: discord.Message) -> bool:
        if any(
            a.id == record.attachment_id
            if record.attachment_id
            else a.filename == "lumi-loading.gif"
            for a in message.attachments
        ):
            return True
        if (
            record.attachment_id
            or getattr(message, "content", "")
            or getattr(message, "components", ())
        ):
            return False
        embeds = getattr(message, "embeds", ())
        if len(embeds) != 1 or embeds[0].title or embeds[0].description or embeds[0].image.url:
            return False
        try:
            image = urlsplit(str(embeds[0].thumbnail.url or ""))
        except ValueError:
            return False
        return (
            image.scheme == "https"
            and image.hostname in {"cdn.discordapp.com", "media.discordapp.net"}
            and image.path.startswith("/attachments/")
            and image.path.endswith("/lumi-loading.gif")
        )

    async def finish(
        self,
        job: JobEnvelope,
        *,
        applied: bool = False,
        silent: bool = False,
        text: str = "Unavailable.",
    ) -> bool:
        record = self.records.get(job.job_id)
        if record is None:
            return False


        with contextlib.suppress(RedisError, TimeoutError):
            await self._cache(
                self.edge.cache_redis.set(
                    response_state_key(job.job_id, job.group), "final", ex=response_ttl(job)
                )
            )
        if not applied:
            cached_safe = record.phase in {"bound", "submitting"}
            record.phase = "interrupted"
            with contextlib.suppress(RedisError, TimeoutError):
                await self._save(record)
            try:
                message = self.edge._pending_messages.get(job.job_id) if cached_safe else None
                if message is None:
                    message = await self._message(record)
                if message is not None and self._still_loading(record, message):
                    if self.edge.user is None or message.author.id != self.edge.user.id:
                        raise ValueError("command response is not owned by Edge")
                    if silent or self.edge.blocklist.contains(job.actor_id):
                        await message.delete()
                    else:
                        accent = module_colour(self.edge.registry.snapshot, job.module)
                        await message.edit(
                            content=None,
                            embeds=[],
                            attachments=[],
                            view=self.edge.layouts.build(
                                PresentationPage(description=text), accent_color=accent
                            ),
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
            except discord.NotFound:
                pass
            except (discord.HTTPException, RedisError, TimeoutError):
                self.retry_at[job.job_id] = time.monotonic() + 5
                return True
        try:
            await self._cache(self.edge.cache_redis.hdel(RECORDS_KEY, job.job_id))
        except (RedisError, TimeoutError):
            record.phase = "delivered" if applied else "interrupted"
            self.retry_at[job.job_id] = time.monotonic() + 5
            return True
        self.records.pop(job.job_id, None)
        self.sources.pop(record.source_id, None)
        self.retry_at.pop(job.job_id, None)
        self.edge._pending_messages.pop(job.job_id, None)
        self.edge._pending_jobs.pop(job.job_id, None)
        self.retired_locks.add(job.job_id)
        return True

    async def settle_unsubmitted(self, source_id: int) -> None:
        record = self.for_source(source_id)
        if record is not None and record.phase in {"posting", "bound"}:
            async with self.edge._response_locks.setdefault(record.job.job_id, asyncio.Lock()):
                await self.finish(record.job, silent=True)

    async def sweep(self) -> None:
        for record in tuple(self.records.values()):
            job = record.job
            if self.retry_at.get(job.job_id, 0) > time.monotonic():
                continue
            interrupted = record.phase in {"interrupted", "delivered"}
            if not interrupted:
                if self.edge._pending_allowed(job):
                    continue
                if self.edge._pending_allowed(job, allow_block=True) and await self.block_notice_allowed(job):
                    continue
            async with self.edge._response_locks.setdefault(job.job_id, asyncio.Lock()):
                await self.finish(job, applied=record.phase == "delivered")
        for job_id in tuple(self.retired_locks):
            lock = self.edge._response_locks.get(job_id)
            if lock is not None and (lock.locked() or getattr(lock, "_waiters", None)):
                continue
            self.edge._response_locks.pop(job_id, None)
            self.retired_locks.discard(job_id)

    async def recover(self) -> None:

        rows = await self._cache(self.edge.cache_redis.hgetall(RECORDS_KEY))
        recovered = []
        pipeline = self.edge.cache_redis.pipeline(transaction=True)
        for key, raw in rows.items():
            try:
                record = msgspec.json.decode(raw, type=LoadingRecord)
            except msgspec.DecodeError:
                LOGGER.warning("invalid_command_loading_record")
                pipeline.hdel(RECORDS_KEY, key)
                continue
            self._remember(record)
            recovered.append(record)
            pipeline.set(
                response_state_key(record.job.job_id, record.job.group),
                "final",
                ex=response_ttl(record.job),
            )
        await self._cache(pipeline.execute())
        self.ready.set()
        self.recovering = False
        for record in recovered:
            async with self.edge._response_locks.setdefault(record.job.job_id, asyncio.Lock()):
                await self.finish(record.job, applied=record.phase == "delivered")
