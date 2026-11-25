from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.contracts import AFKActiveSnapshot, encode
from lumi.storage.checkpoint import create_pool, read_cursor

from .archive import ArchiveFiles
from .archive_store import SCHEMA as ARCHIVE_SCHEMA
from .archive_store import ArchiveStore
from .global_store import GLOBAL_SCHEMA, GlobalAFKStore
from .history import HISTORY_SCHEMA, AFKHistoryStore

RETENTION_HOURS = 48

AFK_SCHEMA = (
    HISTORY_SCHEMA,
    *GLOBAL_SCHEMA,
    """
    CREATE TABLE IF NOT EXISTS afk_status (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        reason VARCHAR(1900) NULL,
        source_channel_id BIGINT UNSIGNED NOT NULL,
        started_at DATETIME(6) NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id),
        KEY ix_afk_status_guild (guild_id, user_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS afk_preferences (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        delivery_mode ENUM('dm', 'channel', 'none') NOT NULL DEFAULT 'dm',
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS afk_regions (
        user_id BIGINT UNSIGNED PRIMARY KEY,
        timezone_name VARCHAR(100) NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS afk_missed_interactions (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        author_id BIGINT UNSIGNED NOT NULL,
        author_name VARCHAR(100) NOT NULL,
        author_avatar_url VARCHAR(2048) NULL,
        channel_id BIGINT UNSIGNED NOT NULL,
        channel_name VARCHAR(100) NOT NULL,
        message_id BIGINT UNSIGNED NOT NULL,
        mention_kind ENUM('direct', 'reply', 'role', 'everyone', 'here') NOT NULL,
        content TEXT NOT NULL,
        entity_labels_json JSON NOT NULL,
        occurred_at DATETIME(6) NOT NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        UNIQUE KEY uq_afk_interaction_recipient (guild_id, user_id, message_id),
        KEY ix_afk_interaction_window (guild_id, user_id, occurred_at, id),
        KEY ix_afk_interaction_expiry (occurred_at)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS afk_active_revisions (
        guild_id BIGINT UNSIGNED PRIMARY KEY,
        revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS afk_interaction_replies (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        channel_id BIGINT UNSIGNED NOT NULL,
        message_id BIGINT UNSIGNED NOT NULL,
        original_message_id BIGINT UNSIGNED NOT NULL,
        occurred_at DATETIME(6) NOT NULL,
        PRIMARY KEY (guild_id, user_id, channel_id, message_id),
        KEY ix_afk_reply_expiry (occurred_at)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS afk_publication_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        revision BIGINT UNSIGNED NOT NULL,
        payload MEDIUMBLOB NOT NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        published_at TIMESTAMP(6) NULL,
        UNIQUE KEY uq_afk_publication_revision (guild_id, revision),
        KEY ix_afk_publication_pending (published_at, id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS afk_migration_runs (
        source_digest CHAR(64) PRIMARY KEY,
        source_label VARCHAR(100) NOT NULL,
        preferences_count INT UNSIGNED NOT NULL,
        regions_count INT UNSIGNED NOT NULL,
        guilds_count INT UNSIGNED NOT NULL,
        details_json JSON NOT NULL,
        completed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
    *ARCHIVE_SCHEMA,
)


@dataclass(frozen=True, slots=True)
class AFKState:
    guild_id: int
    user_id: int
    reason: str | None
    source_channel_id: int
    started_at: datetime
    global_scope: bool = False
    history_since: datetime | None = None


@dataclass(frozen=True, slots=True)
class AFKInteraction:
    guild_id: int
    user_id: int
    author_id: int
    author_name: str
    author_avatar_url: str | None
    channel_id: int
    channel_name: str
    message_id: int
    mention_kind: str
    content: str
    entity_labels: tuple[tuple[str, str], ...]
    occurred_at: datetime
    reply_to_message_id: int | None = None


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _mysql_utc(value: datetime) -> datetime:
    return _aware_utc(value).replace(tzinfo=None)


class MySQLAFKStore(AFKHistoryStore, GlobalAFKStore):
    def __init__(self, dsn: str, *, archive_root: Path | None = None) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("AFK DSN must use mysql:// or mariadb://")
        self._settings = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "db": parsed.path.removeprefix("/"),
            "autocommit": False,
            "minsize": 1,
            "maxsize": 8,
        }
        self._pool: asyncmy.Pool | None = None
        self.archive = ArchiveStore(self, ArchiveFiles(archive_root) if archive_root else None)

    async def connect(self) -> None:
        self._pool = await create_pool(**self._settings)

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    def _require_pool(self) -> asyncmy.Pool:
        if self._pool is None:
            raise RuntimeError("AFK store is not connected")
        return self._pool

    async def migrate(self) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for statement in AFK_SCHEMA:
                    await cursor.execute(statement)


                for table, columns in (
                    ("afk_status", "user_id,started_at"),
                    ("afk_missed_interactions", "user_id,guild_id"),
                    ("afk_archived_interactions", "user_id,guild_id"),
                ):
                    await cursor.execute(
                        f"SHOW INDEX FROM {table} WHERE Key_name='ix_afk_global_user'"
                    )
                    if not await cursor.fetchone():
                        await cursor.execute(
                            f"ALTER TABLE {table} ADD INDEX ix_afk_global_user({columns}), "
                            "ALGORITHM=INPLACE, LOCK=NONE"
                        )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def set_afk(
        self,
        guild_id: int,
        user_id: int,
        reason: str | None,
        source_channel_id: int,
        started_at: datetime,
    ) -> AFKState:
        clean_reason = (reason or "").strip() or None
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                scope = await self._lock_scope(cursor, user_id)
                if scope.global_status:
                    state = await self._put_global(
                        cursor,
                        guild_id,
                        user_id,
                        clean_reason,
                        source_channel_id,
                        _mysql_utc(started_at),
                    )
                    await connection.commit()
                    return state


                await cursor.execute(
                    "INSERT INTO afk_return_windows(guild_id,user_id,started_at) "
                    "SELECT guild_id,user_id,started_at FROM afk_status "
                    "WHERE guild_id=%s AND user_id=%s "
                    "ON DUPLICATE KEY UPDATE started_at=LEAST("
                    "afk_return_windows.started_at,VALUES(started_at))",
                    (guild_id, user_id),
                )
                await cursor.execute(
                    """
                    INSERT INTO afk_status
                        (guild_id, user_id, reason, source_channel_id, started_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE reason = VALUES(reason),
                        source_channel_id = VALUES(source_channel_id),
                        started_at = VALUES(started_at)
                    """,
                    (
                        guild_id,
                        user_id,
                        clean_reason,
                        source_channel_id,
                        _mysql_utc(started_at),
                    ),
                )
                await self._publish_active(cursor, guild_id)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise
        return AFKState(guild_id, user_id, clean_reason, source_channel_id, started_at)

    async def clear_afk(
        self, guild_id: int, user_id: int, *, returned_at: datetime | None = None
    ) -> AFKState | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                scope = await self._lock_scope(cursor, user_id)
                if scope.global_status:
                    state = await self._clear_global(
                        cursor,
                        guild_id,
                        user_id,
                        _mysql_utc(returned_at) if returned_at is not None else None,
                    )
                    await connection.commit()
                    return state
                await cursor.execute(
                    """
                    SELECT guild_id, user_id, reason, source_channel_id, started_at
                    FROM afk_status WHERE guild_id = %s AND user_id = %s FOR UPDATE
                    """,
                    (guild_id, user_id),
                )
                row = await cursor.fetchone()
                if row is None or (returned_at is not None and row[4] > _mysql_utc(returned_at)):
                    await connection.commit()
                    return None
                await cursor.execute(
                    "INSERT INTO afk_return_windows VALUES(%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE started_at=LEAST(started_at,VALUES(started_at))",
                    (guild_id, user_id, row[4]),
                )
                await cursor.execute(
                    "DELETE FROM afk_status WHERE guild_id = %s AND user_id = %s",
                    (guild_id, user_id),
                )
                await self._publish_active(cursor, guild_id)
                await connection.commit()
                return self._state(row)
            except Exception:
                await connection.rollback()
                raise

    async def statuses(self, guild_id: int, user_ids: tuple[int, ...]) -> tuple[AFKState, ...]:
        identifiers = tuple(dict.fromkeys(int(value) for value in user_ids if int(value) > 0))
        if not identifiers:
            return ()
        placeholders = ",".join("%s" for _ in identifiers)
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                f"""
                SELECT guild_id, user_id, reason, source_channel_id, started_at
                FROM afk_status
                WHERE guild_id = %s AND user_id IN ({placeholders})
                ORDER BY user_id
                """,
                (guild_id, *identifiers),
            )
            local = {int(row[1]): self._state(row) for row in await cursor.fetchall()}
            await cursor.execute(
                f"SELECT user_id,origin_guild_id,reason,source_channel_id,started_at,history_since "
                f"FROM afk_global_status WHERE user_id IN ({placeholders})",
                identifiers,
            )
            for row in await cursor.fetchall():
                local[int(row[0])] = self._global_state(row, guild_id)
            return tuple(local[user] for user in sorted(local))

    async def get_status(self, guild_id: int, user_id: int) -> AFKState | None:
        rows = await self.statuses(guild_id, (user_id,))
        return rows[0] if rows else None

    async def set_mode(self, guild_id: int, user_id: int, mode: str) -> None:
        if mode not in {"dm", "channel", "none"}:
            raise ValueError("invalid AFK delivery mode")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO afk_preferences (guild_id, user_id, delivery_mode)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE delivery_mode = VALUES(delivery_mode)
                """,
                (guild_id, user_id, mode),
            )
            await connection.commit()

    async def get_mode(self, guild_id: int, user_id: int) -> str:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                """
                SELECT delivery_mode FROM afk_preferences
                WHERE guild_id = %s AND user_id = %s
                """,
                (guild_id, user_id),
            )
            row = await cursor.fetchone()
            return str(row[0]) if row else "dm"

    async def set_region(self, user_id: int, timezone_name: str) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO afk_regions (user_id, timezone_name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE timezone_name = VALUES(timezone_name)
                """,
                (user_id, timezone_name),
            )
            await connection.commit()

    async def clear_region(self, user_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("DELETE FROM afk_regions WHERE user_id = %s", (user_id,))
            await connection.commit()

    async def get_region(self, user_id: int) -> str | None:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT timezone_name FROM afk_regions WHERE user_id = %s", (user_id,)
            )
            row = await cursor.fetchone()
            return str(row[0]) if row else None

    async def import_legacy(
        self,
        source_digest: str,
        source_label: str,
        preferences: tuple[tuple[int, int, str], ...],
        regions: tuple[tuple[int, str], ...],
        guild_count: int,
        details: dict[str, object],
    ) -> bool:

        if len(source_digest) != 64:
            raise ValueError("invalid AFK migration digest")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                if preferences:
                    await cursor.executemany(
                        """
                        INSERT IGNORE INTO afk_preferences
                            (guild_id, user_id, delivery_mode)
                        VALUES (%s, %s, %s)
                        """,
                        preferences,
                    )
                if regions:
                    await cursor.executemany(
                        """
                        INSERT IGNORE INTO afk_regions (user_id, timezone_name)
                        VALUES (%s, %s)
                        """,
                        regions,
                    )
                await cursor.execute(
                    """
                    INSERT IGNORE INTO afk_migration_runs
                        (source_digest, source_label, preferences_count, regions_count,
                         guilds_count, details_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        source_digest,
                        source_label[:100],
                        len(preferences),
                        len(regions),
                        guild_count,
                        json.dumps(details, sort_keys=True, separators=(",", ":")),
                    ),
                )
                imported = cursor.rowcount == 1
                await connection.commit()
                return imported
            except Exception:
                await connection.rollback()
                raise

    async def record_interactions(
        self,
        *,
        guild_id: int,
        recipients: tuple[tuple[int, str], ...],
        author_id: int,
        author_name: str,
        author_avatar_url: str | None,
        channel_id: int,
        channel_name: str,
        message_id: int,
        content: str,
        entity_labels: tuple[tuple[str, str], ...],
        occurred_at: datetime,
        reply_roots: tuple[tuple[int, int], ...] = (),
    ) -> int:
        rows = tuple(
            (
                guild_id,
                user_id,
                author_id,
                author_name[:100],
                (author_avatar_url or "")[:2048] or None,
                channel_id,
                channel_name[:100],
                message_id,
                kind,
                content,
                json.dumps(entity_labels, ensure_ascii=False, separators=(",", ":")),
                _mysql_utc(occurred_at),
            )
            for user_id, kind in recipients
            if user_id > 0 and user_id != author_id
        )
        if not rows:
            return 0
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                return await self._record_interactions(
                    connection,
                    cursor,
                    rows,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    message_id=message_id,
                    occurred_at=occurred_at,
                    reply_roots=reply_roots,
                )
            except Exception:
                await connection.rollback()
                raise

    async def _record_interactions(
        self,
        connection: object,
        cursor: asyncmy.cursors.Cursor,
        rows: tuple,
        *,
        guild_id: int,
        channel_id: int,
        message_id: int,
        occurred_at: datetime,
        reply_roots: tuple[tuple[int, int], ...],
    ) -> int:

        rows = await self._history_rows(cursor, rows, guild_id, occurred_at)
        if not rows:
            await connection.commit()
            return 0
        await cursor.execute(
            "SELECT user_id FROM afk_archived_interactions WHERE guild_id=%s AND message_id=%s",
            (guild_id, message_id),
        )
        archived = {int(row[0]) for row in await cursor.fetchall()}
        rows = tuple(row for row in rows if row[1] not in archived)
        if not rows:
            await connection.commit()
            return 0
        await cursor.executemany(
            """
                INSERT INTO afk_missed_interactions
                    (guild_id, user_id, author_id, author_name, author_avatar_url,
                     channel_id, channel_name, message_id, mention_kind, content,
                     entity_labels_json, occurred_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE author_id = VALUES(author_id),
                    author_name = VALUES(author_name),
                    author_avatar_url = VALUES(author_avatar_url),
                    channel_id = VALUES(channel_id), channel_name = VALUES(channel_name),
                    mention_kind = VALUES(mention_kind), content = VALUES(content),
                    entity_labels_json = VALUES(entity_labels_json),
                    occurred_at = VALUES(occurred_at)
                """,
            rows,
        )
        changed = max(0, int(cursor.rowcount or 0))
        recipients = {row[1] for row in rows}
        for user_id, original_id in dict(reply_roots).items():
            if user_id not in recipients or not 0 < original_id < message_id:
                continue


            await cursor.execute(
                """
                INSERT IGNORE INTO afk_interaction_replies
                    (guild_id, user_id, channel_id, message_id, original_message_id, occurred_at)
                SELECT guild_id, user_id, channel_id, %s, message_id, %s
                FROM afk_missed_interactions
                WHERE guild_id = %s AND user_id = %s AND channel_id = %s AND message_id = %s
                  AND occurred_at <= %s
                UNION ALL
                SELECT guild_id,user_id,channel_id,%s,message_id,%s
                FROM afk_archived_interactions
                WHERE guild_id=%s AND user_id=%s AND channel_id=%s
                  AND message_id=%s AND occurred_at<=%s
                """,
                (
                    message_id,
                    _mysql_utc(occurred_at),
                    guild_id,
                    user_id,
                    channel_id,
                    original_id,
                    _mysql_utc(occurred_at),
                    message_id,
                    _mysql_utc(occurred_at),
                    guild_id,
                    user_id,
                    channel_id,
                    original_id,
                    _mysql_utc(occurred_at),
                ),
            )
        await connection.commit()
        return changed

    async def read_cutoff(self) -> datetime:


        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute("SELECT UTC_TIMESTAMP(6)")
                return _aware_utc((await cursor.fetchone())[0])
            finally:
                await connection.rollback()

    async def interactions(
        self,
        guild_id: int,
        user_id: int,
        *,
        since: datetime | None = None,
        retained: bool = False,
        unread_before: datetime | None = None,
    ) -> tuple[AFKInteraction, ...]:
        if retained:
            await self.archive.lease(guild_id, user_id)
            since = _aware_utc(
                await self.archive.window(
                    guild_id, user_id, _mysql_utc(since or datetime(1970, 1, 1, tzinfo=UTC))
                )
            )
        parameters: list[object] = [guild_id, user_id]
        since_clause = ""
        if since is not None:
            since_clause = " AND m.occurred_at >= %s"
            parameters.append(_mysql_utc(since))
        unread_clause = ""
        if unread_before is not None:
            unread_clause = (
                "AND NOT EXISTS (SELECT 1 FROM afk_interaction_reads rd "
                "WHERE rd.guild_id=m.guild_id AND rd.user_id=m.user_id "
                "AND rd.message_id=m.message_id AND rd.read_at<=%s)"
            )
            parameters.append(_mysql_utc(unread_before))
        pool = self._require_pool()
        age_clause = (
            ""
            if retained
            else (
                f"AND m.occurred_at >= DATE_SUB(UTC_TIMESTAMP(6), INTERVAL {RETENTION_HOURS} HOUR)"
            )
        )
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    f"""
                SELECT m.guild_id, m.user_id, m.author_id, m.author_name, m.author_avatar_url,
                    m.channel_id, m.channel_name, m.message_id, m.mention_kind, m.content,
                    m.entity_labels_json, m.occurred_at, r.original_message_id
                FROM afk_missed_interactions m
                LEFT JOIN afk_interaction_replies r
                  ON r.guild_id = m.guild_id AND r.user_id = m.user_id
                  AND r.channel_id = m.channel_id AND r.message_id = m.message_id
                WHERE m.guild_id = %s AND m.user_id = %s
                  {age_clause}
                  {since_clause}
                  {unread_clause}
                ORDER BY m.occurred_at DESC, m.id DESC
                """,
                    tuple(parameters),
                )
                rows = [self._interaction(row) for row in await cursor.fetchall()]
            finally:
                await connection.rollback()
        if retained:
            cold = await self.archive.read(
                guild_id, user_id, _mysql_utc(since), unread_before=unread_before
            )
            rows = list({item.message_id: item for item in (*cold, *rows)}.values())
        return tuple(
            sorted(rows, key=lambda item: (item.occurred_at, item.message_id), reverse=True)
        )

    async def media_sources(self, guild_id, channel_id, message_id, author_id):

        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT user_id,content FROM afk_missed_interactions "
                    "WHERE guild_id=%s AND channel_id=%s AND message_id=%s AND author_id=%s",
                    (guild_id, channel_id, message_id, author_id),
                )
                return tuple(await cursor.fetchall())
            finally:
                await connection.rollback()

    async def prune(self) -> int:

        await self.prune_selections()
        return int(await self.archive.move_one())

    async def stage_recap(self, token, guild, user, interactions):
        await self.archive.stage_sources(token, user, interactions)

    async def confirm_recap(self, token):
        return await self.archive.confirm(token)

    async def pending_recaps(self):
        return await self.archive.pending()

    async def recap_lease(self, guild, user):
        return await self.archive.lease(guild, user)

    async def active_snapshots(self) -> tuple[AFKActiveSnapshot, ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT guild_id, revision FROM afk_active_revisions ORDER BY guild_id"
            )
            revisions = tuple((int(row[0]), int(row[1])) for row in await cursor.fetchall())
            snapshots: list[AFKActiveSnapshot] = []
            for guild_id, revision in revisions:
                await cursor.execute(
                    "SELECT user_id FROM afk_global_status ORDER BY user_id"
                    if guild_id == 0
                    else "SELECT user_id FROM afk_status WHERE guild_id = %s ORDER BY user_id",
                    () if guild_id == 0 else (guild_id,),
                )
                snapshots.append(
                    AFKActiveSnapshot(
                        2 if guild_id == 0 else 1,
                        guild_id,
                        revision,
                        tuple(int(row[0]) for row in await cursor.fetchall()),
                        int(time.time() * 1000),
                    )
                )
            return tuple(snapshots)

    async def pending_publications(self, limit: int = 100) -> tuple[tuple[int, bytes], ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                """
                SELECT id, payload FROM afk_publication_outbox
                WHERE published_at IS NULL ORDER BY id LIMIT %s
                """,
                (limit,),
            )
            return tuple((int(row[0]), bytes(row[1])) for row in await cursor.fetchall())

    async def mark_publication_sent(self, outbox_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE afk_publication_outbox SET published_at = CURRENT_TIMESTAMP(6)
                WHERE id = %s AND published_at IS NULL
                """,
                (outbox_id,),
            )
            await connection.commit()

    async def _publish_active(self, cursor: asyncmy.cursors.Cursor, guild_id: int) -> None:
        await cursor.execute(
            """
            INSERT INTO afk_active_revisions (guild_id, revision) VALUES (%s, 1)
            ON DUPLICATE KEY UPDATE revision = revision + 1
            """,
            (guild_id,),
        )
        await cursor.execute(
            "SELECT revision FROM afk_active_revisions WHERE guild_id = %s", (guild_id,)
        )
        revision = int((await cursor.fetchone())[0])
        await cursor.execute(
            "SELECT user_id FROM afk_global_status ORDER BY user_id"
            if guild_id == 0
            else "SELECT user_id FROM afk_status WHERE guild_id = %s ORDER BY user_id",
            () if guild_id == 0 else (guild_id,),
        )
        snapshot = AFKActiveSnapshot(
            2 if guild_id == 0 else 1,
            guild_id,
            revision,
            tuple(int(row[0]) for row in await cursor.fetchall()),
            int(time.time() * 1000),
        )
        await cursor.execute(
            """
            INSERT INTO afk_publication_outbox (guild_id, revision, payload)
            VALUES (%s, %s, %s)
            """,
            (guild_id, revision, encode(snapshot)),
        )

    @staticmethod
    def _state(row: tuple[object, ...]) -> AFKState:
        return AFKState(
            int(row[0]),
            int(row[1]),
            str(row[2]) if row[2] is not None else None,
            int(row[3]),
            _aware_utc(row[4]),
        )

    @staticmethod
    def _interaction(row: tuple[object, ...]) -> AFKInteraction:
        raw_labels = row[10]
        if isinstance(raw_labels, bytes):
            raw_labels = raw_labels.decode("utf-8")
        labels = json.loads(str(raw_labels or "[]"))
        return AFKInteraction(
            int(row[0]),
            int(row[1]),
            int(row[2]),
            str(row[3] or "Someone"),
            str(row[4]) if row[4] else None,
            int(row[5]),
            str(row[6] or "channel"),
            int(row[7]),
            str(row[8]),
            str(row[9] or ""),
            tuple((str(key), str(value)) for key, value in labels),
            _aware_utc(row[11]),
            int(row[12]) if len(row) > 12 and row[12] is not None else None,
        )
