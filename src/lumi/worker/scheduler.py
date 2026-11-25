from __future__ import annotations

from redis.asyncio import Redis

from lumi.contracts import ScheduledDispatch, encode
from lumi.transport import outbound


class ScheduledCallbackClient:
    STREAM = "lumi:scheduled-control"

    def __init__(self, redis: Redis, *, module: str, group: str, generation: int) -> None:
        self._redis = redis
        self._module = module
        self._group = group
        self._generation = generation

    async def publish(
        self,
        *,
        operation: str,
        schedule_key: str,
        route: str,
        revision: int,
        guild_id: int,
        channel_id: int | None,
        actor_id: int,
        due_at_ms: int,
        expires_at_ms: int,
        arguments: dict[str, object] | None = None,
    ) -> None:
        request = ScheduledDispatch(
            schema=1,
            operation=operation,
            schedule_key=schedule_key,
            module=self._module,
            group=self._group,
            generation=self._generation,
            route=route,
            revision=revision,
            guild_id=guild_id,
            channel_id=channel_id,
            actor_id=actor_id,
            due_at_ms=due_at_ms,
            expires_at_ms=expires_at_ms,
            arguments=arguments or {},
        )
        await self._redis.xadd(outbound(self.STREAM, self._group), {"schedule": encode(request)})
