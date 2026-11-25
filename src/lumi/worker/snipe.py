from __future__ import annotations

import time
from collections.abc import Callable
from urllib.parse import urlsplit

import msgspec
from redis.asyncio import Redis

from lumi.contracts import (
    CommandSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    MatcherExpr,
    PassiveMatcherSpec,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
)

SNIPE_TTL_MS = 60_000
MAX_SNIPES = 5_000
MAX_SNIPE_DESCRIPTION = 4_096


class SnipeData(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    content: str
    author_name: str
    author_avatar_url: str | None
    image_url: str | None
    event_kind: str
    recorded_at_ms: int


_RECORD_SCRIPT = """
local previous = redis.call('GET', KEYS[3])
if previous and tonumber(previous) >= tonumber(ARGV[2]) then
    return 0
end

redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
redis.call('SET', KEYS[3], ARGV[2], 'PX', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], KEYS[1])

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[5])
for _, member in ipairs(expired) do
    if redis.call('EXISTS', member) == 0 then
        redis.call('ZREM', KEYS[2], member)
        redis.call('DEL', member .. ':order')
    end
end

local excess = redis.call('ZCARD', KEYS[2]) - tonumber(ARGV[6])
if excess > 0 then
    local oldest = redis.call('ZRANGE', KEYS[2], 0, excess - 1)
    for _, member in ipairs(oldest) do
        redis.call('DEL', member)
        redis.call('DEL', member .. ':order')
        redis.call('ZREM', KEYS[2], member)
    end
end
return 1
"""


class SnipeStore:
    INDEX_KEY = "lumi:snipe:index"

    def __init__(
        self,
        redis: Redis,
        *,
        clock: Callable[[], float] = time.time,
        max_snipes: int = MAX_SNIPES,
    ) -> None:
        self._redis = redis
        self._clock = clock
        self._max_snipes = max_snipes

    @staticmethod
    def _key(guild_id: int, channel_id: int) -> str:
        return f"lumi:snipe:{guild_id}:{channel_id}"

    async def record(
        self,
        guild_id: int,
        channel_id: int,
        data: SnipeData,
        *,
        event_order: int,
    ) -> bool:
        key = self._key(guild_id, channel_id)
        now_ms = int(self._clock() * 1_000)
        result = await self._redis.eval(
            _RECORD_SCRIPT,
            3,
            key,
            self.INDEX_KEY,
            f"{key}:order",
            msgspec.json.encode(data),
            str(event_order),
            str(SNIPE_TTL_MS),
            str(now_ms),
            str(now_ms - SNIPE_TTL_MS),
            str(self._max_snipes),
        )
        return int(result) == 1

    async def get(self, guild_id: int, channel_id: int) -> SnipeData | None:
        key = self._key(guild_id, channel_id)
        raw = await self._redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.encode()
        data = msgspec.json.decode(raw, type=SnipeData)
        if int(self._clock() * 1_000) - data.recorded_at_ms <= SNIPE_TTL_MS:
            return data
        pipeline = self._redis.pipeline(transaction=False)
        pipeline.delete(key, f"{key}:order")
        pipeline.zrem(self.INDEX_KEY, key)
        await pipeline.execute()
        return None


def _safe_https_url(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or len(text) > 2_048:
        return None
    parsed = urlsplit(text)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        return None
    return text


def snipe_from_snapshot(
    snapshot: object,
    *,
    event_kind: str,
    recorded_at_ms: int,
) -> SnipeData | None:
    if not isinstance(snapshot, dict):
        return None
    parts = []
    for field in ("message_content", "embed_content", "attachment_content"):
        value = snapshot.get(field)
        if isinstance(value, str) and (value := value.strip()):
            parts.append(value)
    content = "\n".join(parts).strip()
    image_url = _safe_https_url(snapshot.get("image_url"))
    if not content and not image_url:
        return None
    if not content:
        content = "[Image]"
    if len(content) > MAX_SNIPE_DESCRIPTION:
        content = content[: MAX_SNIPE_DESCRIPTION - 1].rstrip() + "…"
    author_name = str(snapshot.get("author_name") or "Unknown user")[:256]
    return SnipeData(
        content=content,
        author_name=author_name,
        author_avatar_url=_safe_https_url(snapshot.get("author_avatar_url")),
        image_url=image_url,
        event_kind=event_kind,
        recorded_at_ms=recorded_at_ms,
    )


class SnipeModule:
    def __init__(
        self,
        *,
        generation: int,
        cache_redis: Redis,
        group: str = "community",
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._generation = generation
        self._group = group
        self._clock = clock
        self._store = SnipeStore(cache_redis, clock=clock)

    @property
    def manifest(self) -> Manifest:
        snapshot_match = MatcherExpr("exists", field="message_snapshot")
        return Manifest(
            schema=2,
            module="snipe",
            group=self._group,
            generation=self._generation,
            job_stream=f"lumi:jobs:{self._group}",
            display_name="Snipe",
            summary="Recent deleted and edited messages.",
            presentation_mode="v2",
            version="1.0.1",
            commands=(
                CommandSpec(
                    root="snipe",
                    handler="snipe.show",
                    grammar=r"^$",
                    help=HelpSpec(".snipe", "Show this channel's last deleted or edited message."),
                    acl_key="snipe",
                    deadline_ms=5_000,
                    public=True,
                    presentation_mode="legacy",
                ),
            ),
            passive=(
                PassiveMatcherSpec(
                    route="snipe.message_edited",
                    handler="snipe.capture_edit",
                    event="message_edit",
                    expression=snapshot_match,
                    deadline_ms=10_000,
                    context_fields=("message_snapshot", "event_order"),
                ),
                PassiveMatcherSpec(
                    route="snipe.message_deleted",
                    handler="snipe.capture_delete",
                    event="message_delete",
                    expression=snapshot_match,
                    deadline_ms=10_000,
                    context_fields=("message_snapshot", "event_order"),
                ),
            ),
        )

    async def handle(
        self,
        request: TypedRequest,
        job_id: str,
        generation: int,
    ) -> ResultEnvelope:
        del generation
        if request.handler == "snipe.show":
            return await self._show(request, job_id)
        if request.handler in {"snipe.capture_edit", "snipe.capture_delete"}:
            return await self._capture(request, job_id)
        return self._result(job_id, error_code="unknown_handler")

    async def _capture(self, request: TypedRequest, job_id: str) -> ResultEnvelope:
        guild_id = int(request.event.get("guild_id") or 0)
        channel_id = int(request.event.get("channel_id") or 0)
        event_order = int(request.event.get("event_order") or 0)
        if guild_id <= 0 or channel_id <= 0 or event_order <= 0:
            return self._result(job_id, error_code="invalid_event")
        kind = "edited" if request.handler == "snipe.capture_edit" else "deleted"
        now_ms = int(self._clock() * 1_000)
        data = snipe_from_snapshot(
            request.event.get("message_snapshot"),
            event_kind=kind,
            recorded_at_ms=now_ms,
        )
        if data is None:
            return self._result(job_id)
        try:
            await self._store.record(
                guild_id,
                channel_id,
                data,
                event_order=event_order,
            )
        except Exception:
            return self._result(job_id, error_code="cache")
        return self._result(job_id)

    async def _show(self, request: TypedRequest, job_id: str) -> ResultEnvelope:
        guild_id = int(request.event.get("guild_id") or 0)
        channel_id = int(request.event.get("channel_id") or 0)
        try:
            data = await self._store.get(guild_id, channel_id)
        except Exception:
            return self._result(
                job_id,
                intent=self._send("Unavailable."),
                error_code="cache",
            )
        if data is None:
            return self._result(
                job_id,
                intent=self._send("There's nothing to snipe!"),
            )
        footer = "Message before edit" if data.event_kind == "edited" else "Deleted message"
        return self._result(
            job_id,
            intent=DiscordIntent(
                kind="send",
                pages=(
                    PresentationPage(
                        description=data.content,
                        author_name=data.author_name,
                        author_icon_url=data.author_avatar_url,
                        image_url=data.image_url,
                        footer=footer,
                    ),
                ),
                presentation_mode="legacy",
            ),
        )

    @staticmethod
    def _send(text: str) -> DiscordIntent:
        return DiscordIntent(
            kind="send",
            pages=(PresentationPage(description=text),),
            presentation_mode="legacy",
        )

    def _result(
        self,
        job_id: str,
        *,
        intent: DiscordIntent | None = None,
        error_code: str | None = None,
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="snipe",
            group=self._group,
            generation=self._generation,
            status="ok" if error_code is None else "error",
            intents=(intent,) if intent is not None else (),
            error_code=error_code,
            completed_at_ms=int(time.time() * 1_000),
        )

    async def close(self) -> None:
        return None
