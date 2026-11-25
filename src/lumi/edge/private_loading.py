from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import re
import time
from typing import Any

import discord
from redis.exceptions import RedisError

from lumi.contracts import JobEnvelope, PresentationPage, PrivateFeedbackSpec
from lumi.shared.private_feedback import (
    RESPONSE_LIFETIME_MS,
    PrivateFeedbackSnapshot,
    decode_feedback,
)

from .pending import random_loading_animation
from .private_responses import (
    TIMEOUT_TEXT,
    PrivateBinding,
    PrivateResponseStore,
    PrivateResponseUnavailable,
)
from .registry import module_colour

LOGGER = logging.getLogger(__name__)


class InteractionLogFilter(logging.Filter):


    pattern = re.compile(r"(/(?:webhooks|interactions)/\d+/)[^/\s?]+")

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.pattern.sub(r"\1[redacted]", record.getMessage())
        record.args = ()
        return True


for _name in ("discord.http", "discord.webhook.async_"):
    logging.getLogger(_name).addFilter(InteractionLogFilter())


class PrivateLoading:


    def __init__(self, edge, store: PrivateResponseStore, *, clock=None) -> None:
        self.edge, self.store = edge, store
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.records: dict[str, PrivateBinding] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.retry_at: dict[str, int] = {}
        self.failures: dict[str, int] = {}
        self.unsaved: set[str] = set()
        self._tasks: set[asyncio.Task] = set()
        self._last_read_error = -30_000

    async def recover(self) -> None:
        self.records = {record.job: record for record in await self.store.load(self.clock())}

    def uses_projection(self, job_id: str) -> bool:
        record = self.records.get(job_id)
        return bool(record and record.projection_key())

    async def bind(
        self, job: JobEnvelope, interaction, feedback: PrivateFeedbackSpec | None = None
    ) -> None:
        record = PrivateBinding(
            interaction=str(interaction.id),
            application=interaction.application_id,
            token=interaction.token,
            job=job.job_id,
            module=job.module,
            generation=job.generation,
            guild=job.guild_id or 0,
            channel=job.channel_id or 0,
            message=int(job.request.event.get("message_id") or 0),
            actor=job.actor_id,
            expires=int(interaction.created_at.timestamp() * 1000) + RESPONSE_LIFETIME_MS,
            deadline=job.expires_at_ms,
            group=job.group,
            component=interaction.data["custom_id"] if feedback is not None else "",
            timeout_text=feedback.timeout_text if feedback is not None else TIMEOUT_TEXT,
        )
        self.records[job.job_id] = record


        await self._save(record)

    async def _save(self, record: PrivateBinding) -> bool:
        try:
            async with asyncio.timeout(2):
                await self.store.save(record)
            self.unsaved.discard(record.job)
            return True
        except (TimeoutError, RedisError, PrivateResponseUnavailable):
            self.unsaved.add(record.job)
            LOGGER.warning("private_response_storage_unavailable")
            return False

    def animate(self, job: JobEnvelope) -> None:
        self._start(self._animate(job), "private-component-loading")

    def _start(self, coroutine, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        ingress = getattr(self.edge, "private_ingress", None)
        if ingress is not None:
            ingress.gate.watch(task)

    async def _edit(self, record: PrivateBinding, **kwargs: Any):
        ingress = getattr(self.edge, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(record.actor) as permitted:
                if not permitted:
                    return None
                return await self._ordinary_edit(record, **kwargs)
        return await self._ordinary_edit(record, **kwargs)

    async def _ordinary_edit(self, record: PrivateBinding, **kwargs: Any):
        webhook = discord.Webhook.partial(record.application, record.token, client=self.edge)


        return await webhook.edit_message("@original", **kwargs)

    def _embed(self, record: PrivateBinding, **page) -> discord.Embed:
        accent = module_colour(self.edge.registry.snapshot, record.module)
        return self.edge.layouts.legacy_embed(PresentationPage(**page), accent_color=accent)

    async def _animate(self, job: JobEnvelope) -> None:
        async with self.locks.setdefault(job.job_id, asyncio.Lock()):
            record = self.records.get(job.job_id)
            if record is None or record.shown or self.clock() >= record.deadline:
                return
            animation = discord.File(io.BytesIO(random_loading_animation()), "lumi-loading.gif")
            try:
                async with asyncio.timeout(10):
                    await self._edit(
                        record,
                        content=None,
                        embed=self._embed(record, thumbnail_url="attachment://lumi-loading.gif"),
                        attachments=[animation],
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
            except Exception:


                pass
            finally:
                animation.close()

    async def _forget(self, record: PrivateBinding) -> None:


        await self.store.delete(record.interaction)
        self.records.pop(record.job, None)
        self.retry_at.pop(record.job, None)
        self.failures.pop(record.job, None)
        self.unsaved.discard(record.job)

    @staticmethod
    def _matching_update(record: PrivateBinding, snapshot: PrivateFeedbackSnapshot | None):
        if (
            snapshot is None
            or snapshot.generation < max(record.generation, record.seen_generation)
            or snapshot.revision < record.revision
        ):
            return None
        update = snapshot.updates.get(record.interaction)
        if update and update.actor == record.actor and update.component == record.component:
            return update
        return None

    async def sweep(self) -> None:
        ingress = getattr(self.edge, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(0) as permitted:
                if permitted:
                    await self._ordinary_sweep()
            return
        await self._ordinary_sweep()

    async def _ordinary_sweep(self) -> None:
        records = tuple(self.records.values())
        now = self.clock()
        keys = sorted({
            key for record in records
            if now < record.expires and (key := record.projection_key()) is not None
        })
        snapshots = {}
        if keys:
            try:
                async with asyncio.timeout(2):
                    for offset in range(0, len(keys), 16):
                        batch = keys[offset:offset + 16]
                        values = await self.edge.queue_redis.mget(batch)
                        snapshots.update(zip(batch, map(decode_feedback, values), strict=True))
            except Exception:

                if self.clock() - self._last_read_error >= 30_000:
                    LOGGER.warning("private_feedback_unavailable")
                    self._last_read_error = self.clock()
        for record in records:
            ingress = getattr(self.edge, "private_ingress", None)
            if ingress is not None and ingress.blocked(record.actor):
                continue
            if self.retry_at.get(record.job, 0) > self.clock():
                continue
            lock = self.locks.setdefault(record.job, asyncio.Lock())
            if lock.locked():
                continue


            await lock.acquire()
            self._start(
                self._deliver(record, snapshots.get(record.projection_key()), lock),
                "private-response-feedback",
            )

    async def _deliver(self, record: PrivateBinding, snapshot: PrivateFeedbackSnapshot | None, lock) -> None:
        ingress = getattr(self.edge, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(record.actor) as permitted:
                if not permitted:
                    lock.release()
                    return
                return await self._ordinary_deliver(record, snapshot, lock)
        return await self._ordinary_deliver(record, snapshot, lock)

    async def _ordinary_deliver(self, record: PrivateBinding, snapshot: PrivateFeedbackSnapshot | None, lock) -> None:
        try:
            if self.records.get(record.job) is not record:
                return
            now = self.clock()
            if now >= record.expires:
                await self._forget(record)
                return
            update = self._matching_update(record, snapshot)
            if update:
                text = update.text
                changed = (
                    record.seen_generation != snapshot.generation
                    or record.revision != snapshot.revision
                    or record.shown != text
                )
                record.seen_generation = snapshot.generation
                record.revision = snapshot.revision
                final = snapshot.final
            elif now >= record.deadline and not record.shown:
                text, final, changed = record.timeout_text, not record.component, True
            else:
                if record.job in self.unsaved:
                    await self._save(record)
                return
            if record.shown != text:
                async with asyncio.timeout(10):
                    await self._edit(
                        record,
                        content=None,
                        embed=self._embed(record, description=text),
                        attachments=[],
                        view=None,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                record.shown = text
            if final:
                await self._forget(record)
            elif changed or record.job in self.unsaved:
                await self._save(record)
            self.failures.pop(record.job, None)
            self.retry_at.pop(record.job, None)
        except (discord.NotFound, discord.Forbidden):
            with contextlib.suppress(RedisError):
                await self._forget(record)
        except Exception:
            attempts = self.failures.get(record.job, 0) + 1
            self.failures[record.job] = attempts
            self.retry_at[record.job] = self.clock() + min(30_000, 500 * 2 ** min(attempts, 6))
            LOGGER.warning("private_response_feedback_retry")
        finally:
            lock.release()
            if record.job not in self.records:
                self.locks.pop(record.job, None)

    async def result(self, job: JobEnvelope, intent) -> None:

        try:
            async with self.locks.setdefault(job.job_id, asyncio.Lock()):
                record = self.records.get(job.job_id)
                if record is None or record.projection_key():
                    return
                if intent is None:
                    webhook = discord.Webhook.partial(
                        record.application, record.token, client=self.edge
                    )
                    await webhook.delete_message("@original")
                else:
                    from .attachment_delivery import kwargs_for

                    async with kwargs_for(self.edge, intent, job, for_edit=True) as kwargs:
                        sent = await self._edit(record, **kwargs)
                    self.edge._bind_view(kwargs, sent)
                    if intent.session_id:
                        await self.edge._bind_ui_session(
                            session_id=intent.session_id,
                            owner_id=job.actor_id,
                            message_id=sent.id,
                            guild_id=job.guild_id,
                            channel_id=job.channel_id,
                        )
                await self._forget(record)
        except Exception:
            LOGGER.warning("private_component_result_unavailable")

    async def run(self) -> None:
        while not self.edge._closing:
            started = time.monotonic()
            await self.sweep()
            await asyncio.sleep(max(0.01, 0.25 - (time.monotonic() - started)))

    async def close(self) -> None:
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
