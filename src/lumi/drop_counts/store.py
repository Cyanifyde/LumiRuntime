from __future__ import annotations

from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool

from .models import DAY_MS, DropRecord, day_start, series_rows

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS game_drop_counts (
      guild_id BIGINT UNSIGNED NOT NULL,
      channel_id BIGINT UNSIGNED NOT NULL,
      message_id BIGINT UNSIGNED NOT NULL,
      game ENUM('glitch','karuta','sofi') NOT NULL,
      occurred_at_ms BIGINT UNSIGNED NOT NULL,
      recorded_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      PRIMARY KEY (guild_id,message_id),
      KEY ix_drop_counts_range (guild_id,channel_id,occurred_at_ms,game)
    ) ENGINE=InnoDB""",
)


class MySQLDropCountStore:
    def __init__(self, dsn: str) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("Drop count DSN must use mysql:// or mariadb://")
        self.settings = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "db": parsed.path.removeprefix("/"),
            "autocommit": False,
            "minsize": 1,
            "maxsize": 3,
        }
        self.pool = None

    async def connect(self) -> None:
        self.pool = await create_pool(**self.settings)

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def migrate(self) -> None:
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for statement in SCHEMA:
                    await cursor.execute(statement)
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise

    async def record(self, records: tuple[DropRecord, ...]) -> None:
        if not records:
            return
        if len(records) > 100 or any(not item.valid() for item in records):
            raise ValueError("Invalid drop batch.")
        rows = tuple(
            (r.guild_id, r.channel_id, r.message_id, r.game, r.occurred_at_ms) for r in records
        )
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:


                await cursor.executemany(
                    "INSERT INTO game_drop_counts "
                    "(guild_id,channel_id,message_id,game,occurred_at_ms) VALUES (%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE message_id=VALUES(message_id)",
                    rows,
                )
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise

    async def chart(self, guild_id: int, start_ms: int, days: int):

        series_rows(start_ms, days, {})
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT occurred_at_ms DIV 86400000,game,COUNT(*) FROM game_drop_counts "
                    "WHERE guild_id=%s "
                    "AND occurred_at_ms>=%s AND occurred_at_ms<%s "
                    "GROUP BY occurred_at_ms DIV 86400000,game",
                    (guild_id, start_ms, start_ms + days * DAY_MS),
                )
                rows = await cursor.fetchall()
            finally:

                await connection.rollback()
        counts = {(int(day) * DAY_MS, game): int(count) for day, game, count in rows}
        return series_rows(start_ms, days, counts)

    async def first_day(self, guild_id: int) -> int | None:
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT MIN(occurred_at_ms) FROM game_drop_counts WHERE guild_id=%s",
                    (guild_id,),
                )
                row = await cursor.fetchone()
            finally:
                await connection.rollback()
        return day_start(int(row[0])) if row and row[0] is not None else None


class MemoryDropCountStore:


    def __init__(self):
        self.records: dict[tuple[int, int], DropRecord] = {}

    async def record(self, records: tuple[DropRecord, ...]) -> None:
        if len(records) > 100 or any(not item.valid() for item in records):
            raise ValueError("Invalid drop batch.")
        for record in records:
            self.records.setdefault((record.guild_id, record.message_id), record)

    async def chart(self, guild_id: int, start_ms: int, days: int):
        counts: dict[tuple[int, str], int] = {}
        for record in self.records.values():
            if record.guild_id != guild_id:
                continue
            key = day_start(record.occurred_at_ms), record.game
            counts[key] = counts.get(key, 0) + 1
        return series_rows(start_ms, days, counts)

    async def first_day(self, guild_id: int):
        days = [
            day_start(r.occurred_at_ms) for r in self.records.values() if r.guild_id == guild_id
        ]
        return min(days) if days else None

    async def close(self):
        pass
