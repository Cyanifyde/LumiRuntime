from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

import aiohttp
import msgspec
from aiohttp import web
from redis.asyncio import Redis

from lumi.contracts import (
    AICallRequest,
    AICallResult,
    AIProfileAnnouncement,
    decode,
    encode,
)
from lumi.transport import incoming, require_module_owner, require_origin, stream_scope

from .profiles import AIProfileAnnouncer, profile_hash
from .validation import (
    AIValidationError,
    ValidatedProfile,
    convert_capture,
    validate_profile_announcement,
    validate_value,
)

LOGGER = logging.getLogger(__name__)
CALL_ID = re.compile(r"^[a-f0-9]{32}$")
MAX_PROVIDER_RESPONSE = 128 * 1024
MAX_PROVIDER_REQUEST = 16 * 1024
PROVIDER_URL = "https://ollama.com/api/chat"


@dataclass(slots=True)
class _PendingCall:
    request: AICallRequest
    profile: ValidatedProfile
    future: asyncio.Future[AICallResult]
    cancelled: bool = False


class _RetryableProviderError(RuntimeError):
    pass


class _ProviderError(RuntimeError):
    pass


class AIWorkerService:


    def __init__(
        self,
        *,
        redis: Redis,
        generation: int,
        key_file: Path = Path("/run/secrets/ollama_api_key"),
        provider_url: str = PROVIDER_URL,
        host: str = "0.0.0.0",
        port: int = 8080,
    ) -> None:
        self._redis = redis
        self._generation = generation
        self._key_file = key_file
        self._provider_url = provider_url
        self._host = host
        self._port = port
        self._profiles: MappingProxyType[str, ValidatedProfile] = MappingProxyType({})
        self._profile_lock = asyncio.Lock()
        self._accepting = True
        self._closing = False
        self._api_key = ""
        self._session: aiohttp.ClientSession | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.BaseSite | None = None
        self._profile_task: asyncio.Task[None] | None = None
        self._dispatch_task: asyncio.Task[None] | None = None
        self._queue_event = asyncio.Event()
        self._queue_lock = asyncio.Lock()
        self._queues: dict[str, dict[str, deque[_PendingCall]]] = {}
        self._module_order: deque[str] = deque()
        self._actor_order: dict[str, deque[str]] = {}
        self._outstanding: defaultdict[tuple[str, str], int] = defaultdict(int)
        self._active: _PendingCall | None = None

    async def start(self) -> None:
        try:
            self._api_key = (
                await asyncio.to_thread(self._key_file.read_text, encoding="utf-8")
            ).strip()
        except (FileNotFoundError, OSError):
            self._api_key = ""
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=1, ttl_dns_cache=300)
        )
        await self._load_profiles()
        app = web.Application(client_max_size=64 * 1024)
        app.router.add_post("/v1/calls", self._handle_http)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()
        self._profile_task = asyncio.create_task(
            self._consume_profiles(), name="ai-profile-consumer"
        )
        self._dispatch_task = asyncio.create_task(self._dispatch(), name="ai-call-dispatcher")

    def begin_drain(self) -> None:
        self._accepting = False

    def resume(self) -> None:
        self._accepting = True

    def drain_ready(self) -> bool:
        return self._active is None and not any(
            queue for actors in self._queues.values() for queue in actors.values()
        )

    async def close(self) -> None:
        self._accepting = False
        self._closing = True
        self._queue_event.set()
        if self._runner is not None:
            await self._runner.cleanup()
        for task in (self._profile_task, self._dispatch_task):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._profile_task, self._dispatch_task) if task),
            return_exceptions=True,
        )
        async with self._queue_lock:
            for actors in self._queues.values():
                for queue in actors.values():
                    for pending in queue:
                        if not pending.future.done():
                            pending.future.set_result(self._result(pending.request, "unavailable"))
            self._queues.clear()
            self._module_order.clear()
            self._actor_order.clear()
            self._outstanding.clear()
        if self._session is not None:
            await self._session.close()
        self._session = None

    async def _handle_http(self, request: web.Request) -> web.Response:
        call: AICallRequest | None = None
        try:
            body = await request.read()
            call = decode(body, AICallRequest)
            failure, profile = await self._validate_call(call)
            if failure is not None or profile is None:
                result = self._result(call, failure or "invalid_request")
            else:
                result = await self._enqueue(call, profile)
        except (ValueError, msgspec.DecodeError):
            result = AICallResult(
                schema=1,
                call_id=call.call_id if call is not None else "",
                status="invalid_request",
            )
        return web.Response(body=encode(result), content_type="application/json")

    async def _validate_call(
        self, call: AICallRequest
    ) -> tuple[str | None, ValidatedProfile | None]:
        now_ms = int(time.time() * 1_000)
        if (
            call.schema != 1
            or not CALL_ID.fullmatch(call.call_id)
            or call.expires_at_ms <= now_ms
            or call.expires_at_ms > now_ms + 900_000
            or (call.actor_id is not None and call.actor_id <= 0)
        ):
            return "invalid_request", None
        if not self._accepting:
            return "draining", None
        profile = self._profiles.get(call.profile_key)
        if profile is None:
            return "profile_unavailable", None
        owner = profile.announcement
        if (
            owner.module != call.caller_module
            or owner.group != call.caller_group
            or owner.generation != call.caller_generation
            or owner.profile_hash != call.profile_hash
        ):
            return "profile_mismatch", None
        current_generation = await self._redis.get(f"lumi:generation:{call.caller_group}")
        if current_generation is None or int(current_generation) != call.caller_generation:
            return "stale_generation", None
        try:
            validate_value(call.input, owner.profile.input_schema)
        except AIValidationError:
            return "invalid_input", None
        return None, profile

    async def _enqueue(self, call: AICallRequest, profile: ValidatedProfile) -> AICallResult:
        actor = str(call.actor_id) if call.actor_id is not None else "service"
        bucket = (call.caller_module, actor)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[AICallResult] = loop.create_future()
        pending = _PendingCall(call, profile, future)
        async with self._queue_lock:
            if self._outstanding[bucket] >= 2:
                return self._result(call, "busy")
            self._outstanding[bucket] += 1
            module_queues = self._queues.setdefault(call.caller_module, {})
            queue = module_queues.get(actor)
            if queue is None:
                queue = module_queues[actor] = deque()
                self._actor_order.setdefault(call.caller_module, deque()).append(actor)
            if call.caller_module not in self._module_order:
                self._module_order.append(call.caller_module)
            queue.append(pending)
            self._queue_event.set()
        try:
            return await future
        except asyncio.CancelledError:
            pending.cancelled = True
            if not future.done():
                future.cancel()
            raise

    async def _dispatch(self) -> None:
        while not self._closing:
            await self._queue_event.wait()
            pending = await self._take_next()
            if pending is None:
                self._queue_event.clear()
                continue
            call = pending.request
            actor = str(call.actor_id) if call.actor_id is not None else "service"
            bucket = (call.caller_module, actor)
            self._active = pending
            try:
                if pending.cancelled or pending.future.cancelled():
                    continue
                if call.expires_at_ms <= int(time.time() * 1_000):
                    result = self._result(call, "expired")
                else:
                    result = await self._run_call(pending)
                if not pending.future.done():
                    pending.future.set_result(result)
            finally:
                self._active = None
                async with self._queue_lock:
                    self._outstanding[bucket] -= 1
                    if self._outstanding[bucket] <= 0:
                        self._outstanding.pop(bucket, None)

    async def _take_next(self) -> _PendingCall | None:
        async with self._queue_lock:
            while self._module_order:
                module = self._module_order.popleft()
                actors = self._queues.get(module)
                actor_order = self._actor_order.get(module)
                if not actors or not actor_order:
                    self._queues.pop(module, None)
                    self._actor_order.pop(module, None)
                    continue
                actor = actor_order.popleft()
                queue = actors.get(actor)
                if not queue:
                    actors.pop(actor, None)
                    continue
                pending = queue.popleft()
                if queue:
                    actor_order.append(actor)
                else:
                    actors.pop(actor, None)
                if actors:
                    self._module_order.append(module)
                else:
                    self._queues.pop(module, None)
                    self._actor_order.pop(module, None)
                if not self._module_order:
                    self._queue_event.clear()
                return pending
            self._queue_event.clear()
            return None

    async def _run_call(self, pending: _PendingCall) -> AICallResult:
        call = pending.request
        spec = pending.profile.announcement.profile
        if not self._api_key or self._session is None:
            return self._result(call, "unavailable")
        started = time.monotonic()
        provider_deadline_ms = min(
            call.expires_at_ms,
            int(time.time() * 1_000) + spec.deadline_ms,
        )
        user_content = json.dumps(
            call.input, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        messages: list[dict[str, str]] = [
            {"role": "system", "content": spec.system_prompt},
            {"role": "user", "content": user_content},
        ]
        last_status = "unavailable"
        for attempt in range(spec.max_attempts):
            remaining = provider_deadline_ms - int(time.time() * 1_000)
            if remaining <= 0:
                return self._result(call, "expired")
            try:
                raw, prompt_tokens, output_tokens = await self._provider_call(
                    spec, messages, remaining
                )
                parsed = self._parse_output(pending.profile, raw)
                if parsed is not None:
                    outcome, captures, value = parsed
                    return AICallResult(
                        schema=1,
                        call_id=call.call_id,
                        status="ok",
                        outcome=outcome,
                        captures=captures,
                        value=value,
                        elapsed_ms=int((time.monotonic() - started) * 1_000),
                        prompt_tokens=prompt_tokens,
                        output_tokens=output_tokens,
                    )
                last_status = "malformed"
                if attempt + 1 < spec.max_attempts and spec.repair_prompt:
                    messages.extend(
                        (
                            {"role": "assistant", "content": raw},
                            {"role": "user", "content": spec.repair_prompt},
                        )
                    )
            except _RetryableProviderError:
                last_status = "unavailable"
            except _ProviderError:
                return self._result(call, "unavailable")
            if attempt + 1 < spec.max_attempts:
                delay = min(
                    0.25,
                    max(0.0, (provider_deadline_ms / 1_000) - time.time()),
                )
                if delay:
                    await asyncio.sleep(delay)
        return AICallResult(
            schema=1,
            call_id=call.call_id,
            status=last_status,
            elapsed_ms=int((time.monotonic() - started) * 1_000),
        )

    async def _provider_call(
        self,
        spec: Any,
        messages: list[dict[str, str]],
        remaining_ms: int,
    ) -> tuple[str, int | None, int | None]:
        assert self._session is not None
        payload = json.dumps(
            {
                "model": spec.model,
                "messages": messages,
                "stream": False,
                "think": spec.thinking,
                "options": {
                    "temperature": spec.temperature,
                    "num_predict": spec.max_output_tokens,
                },
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) > MAX_PROVIDER_REQUEST:
            raise _ProviderError("request_too_large")
        timeout = aiohttp.ClientTimeout(
            total=min(30.0, remaining_ms / 1_000), connect=5, sock_read=25
        )
        try:
            async with self._session.post(
                self._provider_url,
                data=payload,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                allow_redirects=False,
                timeout=timeout,
            ) as response:
                buffer = bytearray()
                async for chunk in response.content.iter_chunked(8_192):
                    buffer.extend(chunk)
                    if len(buffer) > MAX_PROVIDER_RESPONSE:
                        raise _ProviderError("response_too_large")
                body = bytes(buffer)
                if response.status == 429 or response.status >= 500:
                    raise _RetryableProviderError("provider_retryable")
                if response.status != 200:
                    raise _ProviderError("provider_rejected")
        except (TimeoutError, aiohttp.ClientError) as exc:
            raise _RetryableProviderError("provider_transport") from exc
        try:
            value = json.loads(body)
            message = value["message"]
            content = message["content"]
            if not isinstance(content, str):
                raise TypeError
            prompt_tokens = value.get("prompt_eval_count")
            output_tokens = value.get("eval_count")
            return (
                content,
                prompt_tokens if isinstance(prompt_tokens, int) else None,
                output_tokens if isinstance(output_tokens, int) else None,
            )
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise _RetryableProviderError("provider_shape") from exc

    @staticmethod
    def _parse_output(
        profile: ValidatedProfile, raw: str
    ) -> tuple[str | None, dict[str, Any], Any] | None:
        spec = profile.announcement.profile
        if spec.response_mode == "json":
            try:


                fenced = re.fullmatch(r"\s*```(?:json)?[ \t]*\r?\n([\s\S]*?)\r?\n```\s*", raw)
                value = json.loads(fenced[1] if fenced else raw)
                assert spec.response_schema is not None
                return None, {}, validate_value(value, spec.response_schema)
            except (json.JSONDecodeError, AIValidationError):
                return None
        normalized = raw.strip().replace("\r\n", "\n").replace("\r", "\n")
        for variant in profile.variants:
            match = variant.pattern.fullmatch(normalized)
            if match is None:
                continue
            try:
                captures = {
                    capture.name: convert_capture(match.group(capture.name), capture)
                    for capture in variant.spec.captures
                }
            except (AIValidationError, ValueError):
                return None
            return variant.spec.outcome, captures, None
        return None

    async def _load_profiles(self) -> None:
        for snapshot, stream in zip(
            incoming(AIProfileAnnouncer.CURRENT), incoming(AIProfileAnnouncer.STREAM), strict=True
        ):
            current = await self._redis.hgetall(snapshot)
            with stream_scope(AIProfileAnnouncer.STREAM, stream):
                for payload in current.values():
                    with contextlib.suppress(Exception):
                        await self._apply_profile(decode(payload, AIProfileAnnouncement))

    async def _consume_profiles(self) -> None:
        cursors = {stream: "0-0" for stream in incoming(AIProfileAnnouncer.STREAM)}
        while not self._closing:
            try:
                entries = await self._redis.xread(cursors, count=128, block=5_000)
                for source, rows in entries:
                    source = source.decode() if isinstance(source, bytes) else source
                    for identifier, fields in rows:
                        cursors[source] = (
                            identifier.decode()
                            if isinstance(identifier, bytes)
                            else str(identifier)
                        )
                        payload = fields.get(b"announcement") or fields.get("announcement")
                        if payload is None:
                            continue
                        try:
                            with stream_scope(AIProfileAnnouncer.STREAM, source):
                                await self._apply_profile(decode(payload, AIProfileAnnouncement))
                        except Exception:
                            LOGGER.warning("ai_profile_rejected")
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("ai_profile_consumer_failed")
                await asyncio.sleep(1)

    async def _apply_profile(self, announcement: AIProfileAnnouncement) -> None:
        require_origin(AIProfileAnnouncer.STREAM, announcement.group)
        require_module_owner(announcement.module, announcement.group)
        validated = await asyncio.to_thread(validate_profile_announcement, announcement)
        calculated_hash = await asyncio.to_thread(profile_hash, announcement.profile)
        if calculated_hash != announcement.profile_hash:
            raise AIValidationError("profile hash mismatch")
        current_generation = await self._redis.get(f"lumi:generation:{announcement.group}")
        if current_generation is None or int(current_generation) != announcement.generation:
            raise AIValidationError("stale profile generation")
        async with self._profile_lock:
            existing = self._profiles.get(announcement.profile.key)
            if existing is not None:
                owner = existing.announcement
                if owner.module != announcement.module or owner.group != announcement.group:
                    raise AIValidationError("profile namespace conflict")
                if announcement.generation < owner.generation:
                    raise AIValidationError("stale profile replacement")
            updated = dict(self._profiles)
            if announcement.kind == "retire":
                updated.pop(announcement.profile.key, None)
            else:
                updated[announcement.profile.key] = validated
            self._profiles = MappingProxyType(updated)

    @staticmethod
    def _result(call: AICallRequest, status: str) -> AICallResult:
        return AICallResult(schema=1, call_id=call.call_id, status=status)
