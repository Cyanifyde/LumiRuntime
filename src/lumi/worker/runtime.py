from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import time
from collections.abc import Iterable
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from lumi.ai import AIProfileAnnouncer
from lumi.contracts import (
    AIProfileSpec,
    DiscordIntent,
    JobEnvelope,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
    decode,
)
from lumi.shared.attachment_payloads import AttachmentUnavailable
from lumi.shared.broker import RedisBroker
from lumi.storage.checkpoint import barrier

from .announcer import ManifestAnnouncer
from .attachment_delivery import ACTIVE_DELIVERY, AttachmentDelivery
from .module import WorkerModule

LOGGER = logging.getLogger(__name__)


class WorkerRuntime:
    def __init__(
        self,
        *,
        group: str,
        generation: int,
        redis: Redis,
        modules: Iterable[WorkerModule],
        control_socket: str,
        internal_services: Iterable[Any] = (),
        attachment_delivery: AttachmentDelivery | None = None,
    ) -> None:
        self.group = group
        self.generation = generation
        self.redis = redis
        self.modules = tuple(modules)
        self.internal_services = tuple(internal_services)
        self.control_socket = control_socket
        self.stream = f"lumi:jobs:{group}"
        self.consumer_group = f"lumi-workers:{group}"
        self.consumer = f"g{generation}-{os.getpid()}"
        self._handlers = {
            command.handler: module
            for module in self.modules
            for command in module.manifest.commands
        }
        self._handlers.update(
            {
                passive.handler: module
                for module in self.modules
                for passive in module.manifest.passive
            }
        )
        self._handlers.update(
            {
                offer.callback: module
                for module in self.modules
                for offer in module.manifest.reaction_offers
            }
        )
        self._handlers.update(
            {
                component.handler: module
                for module in self.modules
                for component in module.manifest.components
            }
        )
        self._handlers.update(
            {
                scheduled.handler: module
                for module in self.modules
                for scheduled in module.manifest.scheduled
            }
        )
        self._handlers.update(
            {
                module.manifest.guild_commands.handler: module
                for module in self.modules
                if module.manifest.guild_commands is not None
            }
        )
        self._accepting = True
        self._stopping = False
        self._active: set[asyncio.Task[None]] = set()
        self._fetching = 0
        self._handover_jobs: dict[asyncio.Task, JobEnvelope] = {}
        self._drain_ready_sent = False
        self._drain_signal_revision = 0
        self._announcer = ManifestAnnouncer(redis)
        self._profile_announcer = AIProfileAnnouncer(redis)
        self._broker = RedisBroker(redis)
        self.attachment_delivery = attachment_delivery

    async def run(self) -> None:
        token = ACTIVE_DELIVERY.set(self.attachment_delivery)
        try:
            await self._run()
        finally:
            ACTIVE_DELIVERY.reset(token)

    async def _run(self) -> None:
        await self._broker.start()
        await self._ensure_group()
        for service in (*self.modules, *self.internal_services):
            start = getattr(service, "start", None)
            if start is not None:
                await start()
        for module in self.modules:
            await self._announcer.announce(module.manifest)
            await self._announce_profiles(module)
        loop = asyncio.get_running_loop()
        with contextlib.suppress(NotImplementedError):
            if drain_signal := getattr(signal, "SIGUSR1", None):
                loop.add_signal_handler(drain_signal, self.begin_drain)
            if resume_signal := getattr(signal, "SIGUSR2", None):
                loop.add_signal_handler(resume_signal, self.resume)
            loop.add_signal_handler(signal.SIGTERM, self.stop)
        revision = self._drain_signal_revision
        ready = await self._notify_supervisor("ready")
        if os.getenv("LUMI_WORKER_SUPERVISED") == "1" and (
            not isinstance(ready, dict) or type(ready.get("drain_requested")) is not bool
        ):
            for service in (*self.modules, *self.internal_services):
                await service.close()
            raise RuntimeError("Worker startup control was not confirmed.")
        if (
            isinstance(ready, dict) and ready.get("drain_requested") is True
            and revision == self._drain_signal_revision and not self._stopping
        ):
            self.begin_drain()
        heartbeat = asyncio.create_task(self._heartbeat(), name="worker-heartbeat")
        reclaim = asyncio.create_task(self._reclaim_loop(), name="worker-reclaim")
        try:
            await self._consume_loop()
        finally:
            self._accepting = False
            if self._active:
                await asyncio.gather(*self._active, return_exceptions=True)
            heartbeat.cancel()
            reclaim.cancel()
            await asyncio.gather(heartbeat, reclaim, return_exceptions=True)
            for service in (*self.modules, *self.internal_services):
                await service.close()

    def begin_drain(self) -> None:
        self._drain_signal_revision += 1
        self._accepting = False
        for service in (*self.modules, *self.internal_services):
            hook = getattr(service, "begin_drain", None)
            if hook is not None:
                hook()

    def resume(self) -> None:
        self._drain_signal_revision += 1
        barrier().resume()
        self._drain_ready_sent = False
        for service in (*self.modules, *self.internal_services):
            hook = getattr(service, "resume", None)
            if hook is not None:
                hook()
        self._accepting = True

    def stop(self) -> None:
        barrier().resume()
        self._stopping = True
        self._accepting = False

    async def _consume_loop(self) -> None:
        while not self._stopping:
            if not self._accepting:
                if not self._active and self._services_drain_ready() and not self._drain_ready_sent:
                    if await barrier().freeze():
                        await self._notify_supervisor("drain_ready")
                        self._drain_ready_sent = True
                await asyncio.sleep(0.05)
                continue
            async with self._fetching_jobs():
                entries = await self.redis.xreadgroup(
                    self.consumer_group,
                    self.consumer,
                    {self.stream: ">"},
                    count=8,
                    block=1_000,
                )
                if not entries:
                    continue
                for _, messages in entries:
                    for identifier, fields in messages:
                        payload = fields.get(b"job") or fields.get("job")
                        if payload is None:
                            await self.redis.xack(self.stream, self.consumer_group, identifier)
                            continue
                        task = asyncio.create_task(
                            self._execute(identifier, payload),
                            name=f"job:{identifier!r}",
                        )
                        self._active.add(task)
                        task.add_done_callback(self._active.discard)

    async def _execute(self, identifier: str | bytes, payload: bytes) -> None:
        upload_context = ACTIVE_DELIVERY.set(self.attachment_delivery)
        try:
            job = decode(payload, JobEnvelope)
            if await self._cancel_privacy_work(job, identifier):
                return
            for service in self.internal_services:
                guard = getattr(service, "blocked", None)
                if guard is not None:
                    while (
                        guard(job)
                        and job.expires_at_ms > int(time.time() * 1000)
                        and not self._stopping
                    ):
                        await service.wait(job)
            if await self._cancel_privacy_work(job, identifier):
                return
            task = asyncio.current_task()
            self._handover_jobs[task] = job
            now_ms = int(time.time() * 1000)
            if job.expires_at_ms <= now_ms:
                result = self._failure(job, "expired")
            else:
                module = self._handlers.get(job.request.handler)
                if module is None:
                    result = self._failure(job, "unknown_handler")
                else:
                    try:
                        event = dict(job.request.event)
                        event["_admitted_at_ms"] = job.admitted_at_ms
                        event["_expires_at_ms"] = job.expires_at_ms
                        event["_job_generation"] = job.generation
                        request = TypedRequest(
                            route=job.request.route,
                            handler=job.request.handler,
                            arguments=job.request.arguments,
                            event=event,
                        )
                        result = await module.handle(request, job.job_id, self.generation)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        LOGGER.exception(
                            "worker_handler_failed",
                            extra={"module_name": job.module, "handler": job.request.handler},
                        )
                        result = self._failure(job, "handler_failed")
            if self.attachment_delivery is not None:
                try:
                    for intent in result.intents:
                        await self.attachment_delivery.stage(
                            job.job_id, intent, deadline=job.expires_at_ms,
                        )
                except AttachmentUnavailable:
                    result = self._failure(job, "attachment_unavailable")
            await self._broker.publish_result(result)
            await self.redis.xack(self.stream, self.consumer_group, identifier)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("worker_job_failed_before_terminal_result")
        finally:
            ACTIVE_DELIVERY.reset(upload_context)
            self._handover_jobs.pop(asyncio.current_task(), None)

    async def _cancel_privacy_work(self, job, identifier):
        for service in self.internal_services:
            reject = getattr(service, "privacy_reject", None)
            if reject is not None and await reject(job):
                await self._broker.release(job.job_id)
                await self.redis.xack(self.stream, self.consumer_group, identifier)
                await self.redis.xdel(self.stream, identifier)
                return True
        return False

    @contextlib.asynccontextmanager
    async def _fetching_jobs(self):
        self._fetching += 1
        try:
            yield
        finally:
            self._fetching -= 1

    def _failure(self, job: JobEnvelope, code: str) -> ResultEnvelope:
        if job.request.event.get("dispatch") == "channel":
            return ResultEnvelope(
                1,
                job.job_id,
                job.module,
                self.group,
                self.generation,
                "error",
                intents=(),
                error_code=code,
                completed_at_ms=int(time.time() * 1000),
            )
        plain = job.request.event.get("_presentation_mode") == "plain"
        return ResultEnvelope(
            schema=1,
            job_id=job.job_id,
            module=job.module,
            group=self.group,
            generation=self.generation,
            status="error",
            intents=(
                DiscordIntent(
                    kind="send",
                    content="Unavailable. Try again." if plain else None,
                    pages=(PresentationPage(description="Unavailable. Try again."),)
                    if not plain
                    else (),
                ),
            ),
            error_code=code,
            completed_at_ms=int(time.time() * 1000),
        )

    async def _ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(
                self.stream,
                self.consumer_group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(10)
            for module in self.modules:
                await self._announcer.announce(module.manifest, kind="heartbeat")
                await self._announce_profiles(module, kind="heartbeat")

    async def _announce_profiles(self, module: WorkerModule, *, kind: str = "ready") -> None:
        profiles: tuple[AIProfileSpec, ...] = tuple(getattr(module, "ai_profiles", ()))
        for profile in profiles:
            await self._profile_announcer.announce(
                profile,
                module=module.manifest.module,
                group=self.group,
                generation=self.generation,
                kind=kind,
            )

    def _services_drain_ready(self) -> bool:
        if getattr(self, "_fetching", 0):
            return False
        for service in (*self.modules, *self.internal_services):
            hook = getattr(service, "drain_ready", None)
            if hook is not None and not bool(hook()):
                return False
        return True

    async def _reclaim_loop(self) -> None:
        cursor = "0-0"
        while True:
            await asyncio.sleep(5)
            if not self._accepting:
                continue
            async with self._fetching_jobs():
                response = await self.redis.xautoclaim(
                    self.stream,
                    self.consumer_group,
                    self.consumer,
                    min_idle_time=30_000,
                    start_id=cursor,
                    count=8,
                )
                cursor = (
                    response[0].decode() if isinstance(response[0], bytes) else str(response[0])
                )
                for identifier, fields in response[1]:
                    payload = fields.get(b"job") or fields.get("job")
                    if payload is None:
                        await self.redis.xack(self.stream, self.consumer_group, identifier)
                        continue
                    task = asyncio.create_task(self._execute(identifier, payload))
                    self._active.add(task)
                    task.add_done_callback(self._active.discard)

    async def _notify_supervisor(self, event: str) -> dict | None:
        import json

        try:
            async with asyncio.timeout(5):
                reader, writer = await asyncio.open_unix_connection(self.control_socket)
                try:
                    writer.write(json.dumps({
                        "op": "worker_event", "event": event, "generation": self.generation,
                        "checkpoint_protocol": 2,
                    }).encode() + b"\n")
                    await writer.drain()
                    reply = json.loads(await reader.readline())
                    return reply if isinstance(reply, dict) and reply.get("ok") is True else None
                finally:
                    writer.close()
                    await writer.wait_closed()
        except (OSError, ValueError):
            LOGGER.warning("supervisor_notification_failed", extra={"event": event})
            return None
