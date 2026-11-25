from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from lumi.contracts import AcceptedCatalog, decode

from .store import SettingsStore

LOGGER = logging.getLogger(__name__)


class SettingsCatalog:
    CURRENT = "lumi:catalog:current"
    STREAM = "lumi:catalog:updates"

    def __init__(self, redis: Redis, store: SettingsStore) -> None:
        self.redis = redis
        self.store = store
        self.current = AcceptedCatalog(1, 0, (), 0)
        self._stopped = asyncio.Event()

    @property
    def online_modules(self):
        return tuple(
            module
            for module in self.current.modules
            if module.runtime_state == "online" and module.key != "settings"
        )

    def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        cursor: str | bytes | None = None
        while not self._stopped.is_set():
            try:
                if cursor is None:
                    latest = await self.redis.xrevrange(self.STREAM, count=1)
                    cursor = latest[0][0] if latest else b"0-0"
                    payload = await self.redis.hget(self.CURRENT, "snapshot")
                    if payload:
                        await self._apply(payload)
                rows = await self.redis.xread({self.STREAM: cursor}, count=16, block=5_000)
                for _, entries in rows:
                    for identifier, fields in entries:
                        cursor = identifier
                        payload = fields.get(b"catalog") or fields.get("catalog")
                        if payload:
                            await self._apply(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("settings_catalog_consumer_failed")
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=1)
                except TimeoutError:
                    pass

    async def _apply(self, payload: bytes) -> None:
        try:
            catalog = decode(payload, AcceptedCatalog)
            if catalog.schema != 1 or catalog.revision <= self.current.revision:
                return
            commands = {
                module.key: tuple(command.acl_key for command in module.commands)
                for module in catalog.modules
                if module.key != "settings"
            }
            self.current = catalog
            for guild_id in await self.store.guild_ids():
                try:
                    await self.store.sync_catalog(guild_id, commands)
                except Exception:
                    LOGGER.exception(
                        "settings_catalog_guild_sync_failed", extra={"guild_id": guild_id}
                    )
        except Exception:
            LOGGER.exception("settings_catalog_rejected")
