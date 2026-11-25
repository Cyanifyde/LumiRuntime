from __future__ import annotations

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlparse

from lumi.storage.checkpoint import create_pool

from .engine import EventError, new_event

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS halloween_events (
        guild_id BIGINT UNSIGNED PRIMARY KEY,
        revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        document LONGTEXT NOT NULL CHECK (JSON_VALID(document)),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS halloween_archives (
        guild_id BIGINT UNSIGNED NOT NULL, run_id CHAR(16) NOT NULL,
        document LONGTEXT NOT NULL CHECK (JSON_VALID(document)),
        archived_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, run_id)
    ) ENGINE=InnoDB""",
)


def serialize(state: dict) -> str:
    document = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    if len(document.encode()) > 4 * 1024 * 1024:
        raise EventError("Event storage is full.")
    return document


class MySQLEventStore:


    def __init__(self, dsn: str, publisher=None) -> None:
        self.publisher = publisher
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("invalid event database DSN")
        self._settings = dict(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            db=parsed.path.removeprefix("/"),
            autocommit=True,
            minsize=1,
            maxsize=4,
        )
        self._pool = None

    async def connect(self) -> None:
        self._pool = await create_pool(**self._settings)

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    async def migrate(self) -> None:
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            for statement in SCHEMA:
                await cursor.execute(statement)

    @asynccontextmanager
    async def transaction(self, guild: int):
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute(
                    "INSERT INTO halloween_events (guild_id, document) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE guild_id=halloween_events.guild_id",
                    (guild, serialize(new_event())),
                )
                await cursor.execute(
                    "SELECT document FROM halloween_events WHERE guild_id=%s FOR UPDATE", (guild,)
                )
                state = json.loads((await cursor.fetchone())[0])
                old = copy.deepcopy(state)
                yield state
                if state != old:
                    if state["run"] != old["run"]:
                        await cursor.execute(
                            "INSERT INTO halloween_archives (guild_id, run_id, document) "
                            "VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE guild_id=halloween_archives.guild_id",
                            (guild, old["run"], serialize(old)),
                        )
                    state["revision"] = old["revision"] + 1
                    await cursor.execute(
                        "UPDATE halloween_events SET revision=%s, document=%s WHERE guild_id=%s",
                        (state["revision"], serialize(state), guild),
                    )
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise
            if self.publisher:
                await self.publisher(guild, state)

    async def active_guilds(self) -> list[int]:
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT guild_id FROM halloween_events")
            return [int(row[0]) for row in await cursor.fetchall()]

    async def read(self, guild: int) -> dict | None:
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT document FROM halloween_events WHERE guild_id=%s", (guild,)
            )
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def claim_feedback(self, guilds: list[int]) -> dict[int, list[dict]]:

        result = {}
        for offset in range(0, len(guilds), 100):
            batch = guilds[offset : offset + 100]
            placeholders = ",".join("%s" for _ in batch)
            async with self._pool.acquire() as connection, connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT guild_id, JSON_EXTRACT(document, '$.drop.feedback'), "
                    "JSON_EXTRACT(document, '$.last_drop.feedback') FROM halloween_events "
                    f"WHERE guild_id IN ({placeholders})",
                    tuple(batch),
                )
                for guild, current, previous in await cursor.fetchall():
                    result[int(guild)] = [json.loads(raw) for raw in (current, previous) if raw]
        return result


class MemoryEventStore:


    def __init__(self, publisher=None) -> None:
        self.publisher = publisher
        self.states: dict[int, dict] = {}
        self.archives: dict[tuple[int, str], dict] = {}
        self._locks: dict[int, asyncio.Lock] = {}

    @asynccontextmanager
    async def transaction(self, guild: int):
        async with self._locks.setdefault(guild, asyncio.Lock()):
            old = self.states.setdefault(guild, new_event())
            state = copy.deepcopy(old)
            yield state
            serialize(state)
            if state != old:
                if state["run"] != old["run"]:
                    self.archives[guild, old["run"]] = copy.deepcopy(old)
                state["revision"] = old["revision"] + 1
                self.states[guild] = state
            if self.publisher:
                await self.publisher(guild, state)

    async def active_guilds(self) -> list[int]:
        return list(self.states)

    async def read(self, guild: int) -> dict | None:
        async with self._locks.setdefault(guild, asyncio.Lock()):
            return copy.deepcopy(self.states.get(guild))

    async def claim_feedback(self, guilds: list[int]) -> dict[int, list[dict]]:
        return {
            guild: [
                copy.deepcopy(drop["feedback"])
                for drop in (self.states[guild].get("drop"), self.states[guild].get("last_drop"))
                if drop and drop.get("feedback")
            ]
            for guild in guilds
            if guild in self.states
        }
