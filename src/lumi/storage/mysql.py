from __future__ import annotations

from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS global_blocks (
        user_id BIGINT UNSIGNED PRIMARY KEY,
        blocked_by BIGINT UNSIGNED NOT NULL,
        blocked_at TIMESTAMP(6) NULL DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS global_temporary_blocks (
        user_id BIGINT UNSIGNED PRIMARY KEY,
        expires_at_ms BIGINT UNSIGNED NOT NULL,
        source_module VARCHAR(64) NOT NULL,
        created_at TIMESTAMP(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
        INDEX idx_global_temporary_blocks_expiry (expires_at_ms)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS runtime_settings (
        singleton TINYINT UNSIGNED PRIMARY KEY,
        maintenance BOOLEAN NOT NULL DEFAULT FALSE,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        CONSTRAINT runtime_settings_singleton CHECK (singleton = 1)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS runtime_drains (
        group_name VARCHAR(64) PRIMARY KEY,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
)


class MySQLControlStore:
    def __init__(self, dsn: str) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("control DSN must use mysql:// or mariadb://")
        self._settings = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "db": parsed.path.removeprefix("/"),
            "autocommit": True,
            "minsize": 1,
            "maxsize": 4,
        }
        self._pool: asyncmy.Pool | None = None

    async def connect(self) -> None:
        self._pool = await create_pool(**self._settings)

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    def _require_pool(self) -> asyncmy.Pool:
        if self._pool is None:
            raise RuntimeError("control store is not connected")
        return self._pool

    @property
    def pool(self) -> asyncmy.Pool:
        return self._require_pool()

    async def migrate(self) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            for statement in SCHEMA:
                await cursor.execute(statement)
            await cursor.execute(
                "INSERT IGNORE INTO runtime_settings (singleton, maintenance) VALUES (1, FALSE)"
            )

    async def load_blocks(self) -> frozenset[int]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT user_id FROM global_blocks")
            return frozenset(int(row[0]) for row in await cursor.fetchall())

    async def add_block(self, user_id: int, actor_id: int) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "INSERT IGNORE INTO global_blocks (user_id, blocked_by) VALUES (%s, %s)",
                (user_id, actor_id),
            )
            return cursor.rowcount == 1

    async def load_temporary_blocks(self) -> dict[int, int]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT user_id, expires_at_ms FROM global_temporary_blocks "
                "WHERE expires_at_ms > CAST("
                "UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS UNSIGNED)"
            )
            return {int(row[0]): int(row[1]) for row in await cursor.fetchall()}

    async def add_temporary_block(
        self, user_id: int, expires_at_ms: int, source_module: str
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO global_temporary_blocks
                    (user_id, expires_at_ms, source_module)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    expires_at_ms = GREATEST(expires_at_ms, VALUES(expires_at_ms)),
                    source_module = VALUES(source_module)
                """,
                (user_id, expires_at_ms, source_module),
            )
            return True

    async def remove_block(self, user_id: int) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute("DELETE FROM global_blocks WHERE user_id = %s", (user_id,))
                changed = cursor.rowcount > 0
                await cursor.execute(
                    "DELETE FROM global_temporary_blocks WHERE user_id = %s",
                    (user_id,),
                )
                changed = changed or cursor.rowcount > 0
                await connection.commit()
                return changed
            except Exception:
                await connection.rollback()
                raise

    async def delete_expired_temporary_blocks(self, now_ms: int) -> int:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM global_temporary_blocks WHERE expires_at_ms <= %s",
                (now_ms,),
            )
            return int(cursor.rowcount)

    async def get_maintenance(self) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT maintenance FROM runtime_settings WHERE singleton = 1")
            row = await cursor.fetchone()
            return bool(row[0])

    async def set_maintenance(self, enabled: bool) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE runtime_settings SET maintenance = %s WHERE singleton = 1",
                (enabled,),
            )

    async def load_drains(self) -> frozenset[str]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT group_name FROM runtime_drains")
            return frozenset(str(row[0]) for row in await cursor.fetchall())

    async def set_drain(self, group: str, enabled: bool) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            if enabled:
                await cursor.execute(
                    "INSERT IGNORE INTO runtime_drains (group_name) VALUES (%s)", (group,)
                )
            else:
                await cursor.execute("DELETE FROM runtime_drains WHERE group_name = %s", (group,))
