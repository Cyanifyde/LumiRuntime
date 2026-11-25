from __future__ import annotations

import asyncio
import os
from pathlib import Path

from redis.asyncio import Redis

from lumi.contracts.observation_state import CUTOVER as OBSERVATIONS_READY
from lumi.contracts.observation_state import CUTOVER_VERSION as OBSERVATIONS_VERSION
from lumi.diagnostics import configure_diagnostics
from lumi.runtime_secrets import redis_url, worker_lifecycle_key

from .bot import LumiEdge
from .capture_spool import CUTOVER, CUTOVER_VERSION
from .control_state import ProjectedBlocklist, RedisControl
from .gateway_relay import GatewayRelays
from .operator_controls import RuntimeControlPlane
from .channel_fence import RedisChannelFence
from .channel_gate import ChannelGate
from lumi.shared.discord_write_journal import DiscordWriteJournal
from .private_loading import PrivateLoading
from .private_responses import PrivateResponseStore
from .registry import EdgeRouteRegistry
from .worker_lifecycle import RedisWorkerLifecycle


async def _run() -> None:
    configure_diagnostics(level=os.getenv("LUMI_LOG_LEVEL", "INFO"))
    token_path = Path(os.environ["LUMI_DISCORD_TOKEN_FILE"])
    token = (await asyncio.to_thread(token_path.read_text, encoding="utf-8")).strip()
    if not token:
        raise RuntimeError("Discord token file is empty")
    owner_id = int(os.getenv("LUMI_OWNER_ID", "-6"))
    registry = EdgeRouteRegistry()
    queue_redis = Redis.from_url(redis_url("queue"))
    cache_redis = Redis.from_url(redis_url("cache"))
    projection = RedisControl(queue_redis, owner_id)
    lifecycle_path = os.getenv("LUMI_WORKER_LIFECYCLE_KEY_FILE")
    control = RuntimeControlPlane(
        registry=registry,
        store=projection,
        lifecycle=RedisWorkerLifecycle(
            queue_redis, worker_lifecycle_key(lifecycle_path) if lifecycle_path else None
        ),
    )
    edge = LumiEdge(
        owner_id=owner_id,
        default_prefix=os.getenv("LUMI_DEFAULT_PREFIX", "."),
        blocklist=ProjectedBlocklist(projection),
        registry=registry,
        queue_redis=queue_redis,
        cache_redis=cache_redis,
        control=control,
    )
    projection.on_change = control.apply_projection
    edge.control_projection = projection
    edge.handover = ChannelGate(DiscordWriteJournal(queue_redis), registry, barrier=RedisChannelFence(queue_redis))
    edge.private_loading = PrivateLoading(edge, PrivateResponseStore(queue_redis))
    edge.gateway_relays = GatewayRelays(edge)
    try:
        if os.getenv("LUMI_REQUIRE_CAPTURE_CUTOVER") == "1" and (
            await queue_redis.get(CUTOVER) != CUTOVER_VERSION
        ):
            raise RuntimeError("Gateway capture journal cutover is not confirmed.")
        if os.getenv("LUMI_REQUIRE_OBSERVATION_CUTOVER") == "1" and (
            await queue_redis.get(OBSERVATIONS_READY) != OBSERVATIONS_VERSION
        ):
            raise RuntimeError("Discord observation cutover is not confirmed.")
        await projection.wait_ready()
        await edge.start(token)
    finally:
        await queue_redis.aclose()
        await cache_redis.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
