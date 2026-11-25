from __future__ import annotations

import asyncio
import logging
import time

from redis.asyncio import Redis

from lumi.contracts import AcceptedCatalog, CatalogCommand, CatalogModule, encode
from lumi.shared.quiescence import tracked_read

from .registry import EdgeRouteRegistry, module_colour

LOGGER = logging.getLogger(__name__)


class AcceptedCatalogPublisher:


    CURRENT = "lumi:catalog:current"
    STREAM = "lumi:catalog:updates"

    def __init__(self, redis: Redis, registry: EdgeRouteRegistry) -> None:
        self.redis = redis
        self.registry = registry
        self._published_revision = -1
        self._fingerprint: bytes | None = None
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        while not self._stopped.is_set():
            await self._publish_current()
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=0.5)
            except TimeoutError:
                pass

    @tracked_read
    async def _publish_current(self):
        revision = self.registry.snapshot.revision
        if revision != self._published_revision:
            try:
                await self._publish(revision)
            except Exception:
                LOGGER.exception("accepted_catalog_publish_failed")
            else:
                self._published_revision = revision

    async def _publish(self, revision: int) -> None:
        registry = self.registry.snapshot
        modules: list[CatalogModule] = []
        for key, registered in sorted(registry.modules.items()):
            manifest = registered.manifest
            commands = tuple(
                CatalogCommand(
                    root=command.root,
                    acl_key=command.acl_key,
                    usage=command.help.usage if command.help else "",
                    summary=command.help.summary if command.help else "",
                    public=command.public,
                    guild_only=command.guild_only,
                )
                for command in manifest.commands

                if command.configure_in_guild
            )
            modules.append(
                CatalogModule(
                    key=key,
                    group=manifest.group,
                    display_name=manifest.display_name,
                    summary=manifest.summary,
                    accent_color=module_colour(registry, key),
                    presentation_mode=manifest.presentation_mode,
                    runtime_state=registered.runtime_state,
                    commands=commands,
                )
            )
        module_tuple = tuple(modules)
        fingerprint = encode(AcceptedCatalog(1, 0, module_tuple, 0))
        if fingerprint == self._fingerprint:
            return
        catalog_revision = int(await self.redis.incr("lumi:catalog:revision"))
        catalog = AcceptedCatalog(
            schema=1,
            revision=catalog_revision,
            modules=module_tuple,
            published_at_ms=int(time.time() * 1000),
        )
        payload = encode(catalog)
        pipeline = self.redis.pipeline(transaction=True)
        pipeline.hset(self.CURRENT, "snapshot", payload)
        pipeline.xadd(self.STREAM, {"catalog": payload}, maxlen=512, approximate=True)
        await pipeline.execute()
        self._fingerprint = fingerprint
