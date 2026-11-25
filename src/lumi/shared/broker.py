from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from lumi.contracts import JobEnvelope, ResultEnvelope, TypedRequest, encode
from lumi.contracts.runtime_control import ACTOR_ADMISSIONS, ActorAdmission
from lumi.transport import outbound

from .access_receipts import admitted_request
from .request_scope import memo_read

if TYPE_CHECKING:
    from lumi.edge.matcher import MatchedRoute

ADMIT_LUA = """
local total = tonumber(redis.call('GET', KEYS[1]) or '0')
local worker = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '0')
local actor = tonumber(redis.call('HGET', KEYS[3], ARGV[2]) or '0')
local total_limit = tonumber(ARGV[3])
if total >= total_limit then return {err='GLOBAL_FULL'} end
if worker >= tonumber(ARGV[4]) then return {err='WORKER_FULL'} end
if actor >= tonumber(ARGV[5]) then return {err='USER_FULL'} end
redis.call('INCR', KEYS[1])
redis.call('HINCRBY', KEYS[2], ARGV[1], 1)
redis.call('HINCRBY', KEYS[3], ARGV[2], 1)
redis.call('HSET', KEYS[5], ARGV[6], ARGV[1] .. '|' .. ARGV[2])
redis.call('ZADD', KEYS[6], ARGV[7], ARGV[6])
redis.call('HSET', KEYS[7], ARGV[6], ARGV[8])
local stream_id = redis.call('XADD', KEYS[4], '*', 'job', ARGV[8])
if #KEYS > 7 then redis.call('SET', KEYS[8], ARGV[9], 'PX', ARGV[10]) end
return stream_id
"""

RELEASE_LUA = """
local meta = redis.call('HGET', KEYS[4], ARGV[1])
if not meta then return 0 end
local divider = string.find(meta, '|', 1, true)
local worker = string.sub(meta, 1, divider - 1)
local actor = string.sub(meta, divider + 1)
local total = tonumber(redis.call('GET', KEYS[1]) or '0')
if total > 0 then redis.call('DECR', KEYS[1]) end
local worker_count = tonumber(redis.call('HGET', KEYS[2], worker) or '0')
if worker_count > 1 then
  redis.call('HINCRBY', KEYS[2], worker, -1)
else
  redis.call('HDEL', KEYS[2], worker)
end
local actor_count = tonumber(redis.call('HGET', KEYS[3], actor) or '0')
if actor_count > 1 then
  redis.call('HINCRBY', KEYS[3], actor, -1)
else
  redis.call('HDEL', KEYS[3], actor)
end
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('DEL', KEYS[7])
return 1
"""


class QueueFull(RuntimeError):
    pass


class QueueUnavailable(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SubmittedJob:
    envelope: JobEnvelope
    stream_id: str


class RedisBroker:
    TOTAL_KEY = "lumi:pending:total"
    WORKER_KEY = "lumi:pending:workers"
    USER_KEY = "lumi:pending:users"
    META_KEY = "lumi:pending:meta"
    EXPIRY_KEY = "lumi:pending:expiry"
    JOBS_KEY = "lumi:pending:jobs"
    RESULTS_STREAM = "lumi:results"

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.available = True
        self._probe_failures = 0
        self._probe_successes = 0
        self._admit_script: Any = None
        self._release_script: Any = None

    async def start(self) -> None:
        self._admit_script = self.redis.register_script(ADMIT_LUA)
        self._release_script = self.redis.register_script(RELEASE_LUA)
        await self.probe()

    async def submit(
        self,
        route: MatchedRoute,
        *,
        actor_id: int,
        guild_id: int | None,
        channel_id: int | None,
        now_ms: int | None = None,
    ) -> SubmittedJob:
        envelope = self.prepare(
            route, actor_id=actor_id, guild_id=guild_id, channel_id=channel_id, now_ms=now_ms
        )
        return await self.admit(envelope, stream=route.stream)

    @staticmethod
    def prepare(
        route: MatchedRoute,
        *,
        actor_id: int,
        guild_id: int | None,
        channel_id: int | None,
        now_ms: int | None = None,
        job_id: str | None = None,
    ) -> JobEnvelope:

        now_ms = now_ms or int(time.time() * 1000)
        event = dict(route.request.event)
        event["_presentation_mode"] = route.presentation_mode
        event["_presentation_modes"] = list(route.presentation_modes)
        event["_acl_key"] = route.acl_key
        event["_public"] = route.public
        if route.private_loading:
            event["_private_loading"] = True
        request = TypedRequest(
            route=route.request.route,
            handler=route.request.handler,
            arguments=route.request.arguments,
            event=event,
        )
        return JobEnvelope(
            schema=1,
            job_id=job_id or uuid.uuid4().hex,
            module=route.module,
            group=route.group,
            generation=route.generation,
            admitted_at_ms=now_ms,
            expires_at_ms=now_ms + route.deadline_ms,
            guild_id=guild_id,
            channel_id=channel_id,
            actor_id=actor_id,
            queue_class=route.queue_class,
            request=request,
            guild_sources=route.guild_sources,
        )

    async def admit(self, envelope: JobEnvelope, *, stream: str) -> SubmittedJob:

        if not self.available:
            raise QueueUnavailable("queue is unavailable")
        total_limit = 1000 if envelope.queue_class in {"control", "security"} else 950
        keys = [
            self.TOTAL_KEY, self.WORKER_KEY, self.USER_KEY, stream, self.META_KEY,
            self.EXPIRY_KEY, self.JOBS_KEY,
        ]
        extra = []
        now = time.time_ns() // 1_000_000
        if (
            envelope.request.event.get("_actor_admission") is True
            and envelope.request.event.get("dispatch") == "command"
            and envelope.actor_id > 0 and envelope.expires_at_ms > now
            and admitted_request(
                envelope.request, module=envelope.module, group=envelope.group,
                generation=envelope.generation, actor=envelope.actor_id,
                guild=envelope.guild_id, channel=envelope.channel_id,
            )
        ):
            keys.append(ACTOR_ADMISSIONS + envelope.job_id)
            extra = [encode(ActorAdmission(
                envelope.job_id, envelope.actor_id, envelope.group, envelope.module,
                envelope.request.route, envelope.generation, envelope.expires_at_ms,
            )), envelope.expires_at_ms - now]
        try:
            stream_id = await self._admit_script(
                keys=keys,
                args=[
                    envelope.group,
                    str(envelope.actor_id)
                    if envelope.actor_id
                    else f"service:{envelope.module}:{envelope.guild_id}",
                    total_limit,
                    250,
                    5,
                    envelope.job_id,
                    envelope.expires_at_ms,
                    encode(envelope),
                    *extra,
                ],
            )
        except ResponseError as exc:
            if str(exc) in {"GLOBAL_FULL", "WORKER_FULL", "USER_FULL"}:
                raise QueueFull(str(exc)) from exc
            raise
        if isinstance(stream_id, bytes):
            stream_id = stream_id.decode()
        return SubmittedJob(envelope, str(stream_id))

    async def release(self, job_id: str) -> bool:
        released = await self._release_script(
            keys=[
                self.TOTAL_KEY,
                self.WORKER_KEY,
                self.USER_KEY,
                self.META_KEY,
                self.EXPIRY_KEY,
                self.JOBS_KEY,
                ACTOR_ADMISSIONS + job_id,
            ],
            args=[job_id],
        )
        return bool(released)

    async def get_job(self, job_id: str) -> JobEnvelope | None:
        return await memo_read("job", (self, job_id), lambda: self._read_job(job_id))

    async def _read_job(self, job_id: str) -> JobEnvelope | None:
        payload = await self.redis.hget(self.JOBS_KEY, job_id)
        if payload is None:
            return None
        from lumi.contracts import decode

        return decode(payload, JobEnvelope)

    async def pending_by_group(self) -> dict[str, int]:
        values = await self.redis.hgetall(self.WORKER_KEY)
        return {
            (key.decode() if isinstance(key, bytes) else str(key)): int(value)
            for key, value in values.items()
        }

    async def sweep_expired(self, *, now_ms: int | None = None, limit: int = 500) -> int:
        now_ms = now_ms or int(time.time() * 1000)
        identifiers = await self.redis.zrangebyscore(self.EXPIRY_KEY, 0, now_ms, start=0, num=limit)
        count = 0
        for identifier in identifiers:
            job_id = identifier.decode() if isinstance(identifier, bytes) else str(identifier)
            count += int(await self.release(job_id))
        return count

    async def publish_result(self, result: ResultEnvelope) -> str:
        identifier = await self.redis.xadd(
            outbound(self.RESULTS_STREAM, result.group), {"result": encode(result)}
        )
        return identifier.decode() if isinstance(identifier, bytes) else str(identifier)

    async def probe(self) -> bool:
        try:
            await asyncio.wait_for(self.redis.ping(), timeout=1.0)
        except Exception:
            self._probe_successes = 0
            self._probe_failures += 1
            if self._probe_failures >= 3:
                self.available = False
            return False
        self._probe_failures = 0
        self._probe_successes += 1
        if self._probe_successes >= 2:
            self.available = True
        return True
