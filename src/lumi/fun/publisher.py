from __future__ import annotations

import asyncio
import logging

from lumi.contracts import FunCatalogueSnapshot, GuildCommandDefinition, GuildCommandSnapshot, HelpSpec, decode
from lumi.shared.guild_catalogues import catalogue_keys, publish_catalogue

from .rules import RESERVED

LOGGER = logging.getLogger(__name__)
CURRENT, STREAM = catalogue_keys("community")
PERFORM_GRAMMAR = r"^(?:\s+(?P<target>.{1,100}))?$"


def command_catalogue(snapshot: FunCatalogueSnapshot) -> GuildCommandSnapshot:
    return GuildCommandSnapshot(
        1, "fun", "community", snapshot.guild_id, snapshot.revision,
        tuple(
            GuildCommandDefinition(
                category.name,
                HelpSpec(
                    "." + category.name + (" <@member | ID>" if category.mode == "targeted" else ""),
                    category.name.replace("_", " ").capitalize() + " GIF.",
                ),
                dict(category_id=category.id, category_name=category.name, mode=category.mode,
                     catalogue_revision=snapshot.revision),
            )
            for category in snapshot.categories if category.name not in RESERVED
        ),
    )


class CataloguePublisher:
    def __init__(self, store, queue) -> None:
        self.store, self.queue = store, queue
        self.stopped = asyncio.Event()
        self.lock = asyncio.Lock()
        self.known: set[int] = set()
        self.draining = False
        self.busy = False

    async def publish(self, payload: bytes) -> None:
        snapshot = decode(payload, FunCatalogueSnapshot)
        await publish_catalogue(self.queue, command_catalogue(snapshot))

    async def restore(self) -> None:
        for payload in await self.store.snapshots():
            await self.publish(payload)

    async def flush(self) -> None:
        async with self.lock:
            for guild, revision, payload in await self.store.pending():
                await self.publish(payload)
                await self.store.published(guild, revision)

    async def run(self) -> None:
        while not self.stopped.is_set():
            if self.draining:
                await asyncio.sleep(0.05)
                continue
            self.busy = True
            try:

                for raw in await self.queue.hkeys("lumi:policy:current"):
                    guild = int(raw)
                    if guild not in self.known:
                        await self.store.read(guild)
                        self.known.add(guild)
                await self.flush()
            except Exception:
                LOGGER.exception("fun_catalogue_publish_failed")
            finally:
                self.busy = False
            try:
                await asyncio.wait_for(self.stopped.wait(), timeout=1)
            except TimeoutError:
                pass

    def stop(self) -> None:
        self.stopped.set()
