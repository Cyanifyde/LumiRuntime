from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool, read_cursor

SOFI_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS sofi_tracker_subscriptions (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        drop_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        series_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        delivery_mode ENUM('dm', 'channel') NOT NULL DEFAULT 'dm',
        channel_id BIGINT UNSIGNED NULL,
        modified_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS sofi_tracker_timers (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop', 'series') NOT NULL,
        revision BIGINT UNSIGNED NOT NULL,
        active BOOLEAN NOT NULL,
        due_at DATETIME(6) NULL,
        source_message_id BIGINT UNSIGNED NULL,
        source_at DATETIME(6) NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id, timer_kind),
        KEY ix_sofi_timer_due (active, due_at)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS sofi_schedule_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop', 'series') NOT NULL,
        revision BIGINT UNSIGNED NOT NULL,
        operation ENUM('upsert', 'cancel') NOT NULL,
        due_at DATETIME(6) NOT NULL,
        channel_id BIGINT UNSIGNED NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        published_at TIMESTAMP(6) NULL,
        UNIQUE KEY uq_sofi_schedule_revision (guild_id, user_id, timer_kind, revision),
        KEY ix_sofi_schedule_pending (published_at, id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS pogdrop_feeds (
        provider_key VARCHAR(64) NOT NULL,
        guild_id BIGINT UNSIGNED NOT NULL,
        channel_id BIGINT UNSIGNED NULL,
        wishlist_threshold INT UNSIGNED NOT NULL DEFAULT 100,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        needs_repair BOOLEAN NOT NULL DEFAULT FALSE,
        revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
        modified_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (provider_key, guild_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS pogdrop_delivery_receipts (
        provider_key VARCHAR(64) NOT NULL,
        source_message_id BIGINT UNSIGNED NOT NULL,
        guild_id BIGINT UNSIGNED NOT NULL,
        destination_channel_id BIGINT UNSIGNED NOT NULL,
        status ENUM('reserved', 'sent', 'failed', 'uncertain', 'skipped') NOT NULL,
        destination_message_id BIGINT UNSIGNED NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (provider_key, source_message_id),
        KEY ix_pogdrop_receipt_expiry (created_at)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS sofi_migration_runs (
        source_digest CHAR(64) PRIMARY KEY,
        source_label VARCHAR(120) NOT NULL,
        subscription_count INT UNSIGNED NOT NULL,
        feed_count INT UNSIGNED NOT NULL,
        details_json JSON NOT NULL,
        policy_applied BOOLEAN NOT NULL DEFAULT FALSE,
        completed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
    """
    ALTER TABLE sofi_migration_runs
        ADD COLUMN IF NOT EXISTS policy_applied BOOLEAN NOT NULL DEFAULT FALSE
    """,
)


@dataclass(frozen=True, slots=True)
class SofiSubscription:
    guild_id: int
    user_id: int
    drop_enabled: bool
    series_enabled: bool
    delivery_mode: str
    channel_id: int | None


@dataclass(frozen=True, slots=True)
class SofiTimer:
    guild_id: int
    user_id: int
    kind: str
    revision: int
    active: bool
    due_at_ms: int | None
    source_message_id: int | None
    source_at_ms: int | None


@dataclass(frozen=True, slots=True)
class PogDropFeed:
    provider: str
    guild_id: int
    channel_id: int | None
    threshold: int
    enabled: bool
    needs_repair: bool
    revision: int


@dataclass(frozen=True, slots=True)
class ScheduleOutboxItem:
    identifier: int
    guild_id: int
    user_id: int
    kind: str
    revision: int
    operation: str
    due_at_ms: int
    channel_id: int | None


def _mysql_datetime(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1_000, tz=UTC).replace(tzinfo=None)


def _epoch_ms(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.timestamp() * 1_000)


def _timer_reconciliation(
    *,
    active: bool,
    current_due_ms: int | None,
    previous_source_ms: int,
    previous_source_id: int,
    due_at_ms: int,
    source_at_ms: int,
    source_message_id: int,
    tolerance_ms: int,
) -> str:
    if source_at_ms < previous_source_ms or (
        source_at_ms == previous_source_ms and source_message_id < previous_source_id
    ):
        return "stale"
    if (
        not active
        and source_at_ms == previous_source_ms
        and source_message_id == previous_source_id
    ):
        return "stale"
    if (
        active
        and current_due_ms is not None
        and abs(current_due_ms - due_at_ms) <= tolerance_ms
    ):
        return "retain"
    return "replace"


class MySQLSofiStore:
    def __init__(self, dsn: str) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("Sofi DSN must use mysql:// or mariadb://")
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

    async def connect(self) -> None:
        self._pool = await create_pool(**self._settings)

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    def _require_pool(self) -> asyncmy.Pool:
        if self._pool is None:
            raise RuntimeError("Sofi store is not connected")
        return self._pool

    async def migrate(self) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for statement in SOFI_SCHEMA:
                    await cursor.execute(statement)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def subscription(self, guild_id: int, user_id: int) -> SofiSubscription:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT drop_enabled, series_enabled, delivery_mode, channel_id "
                "FROM sofi_tracker_subscriptions WHERE guild_id=%s AND user_id=%s",
                (guild_id, user_id),
            )
            row = await cursor.fetchone()
        if row is None:
            return SofiSubscription(guild_id, user_id, False, False, "dm", None)
        return SofiSubscription(guild_id, user_id, bool(row[0]), bool(row[1]), str(row[2]), row[3])

    async def save_subscription(
        self,
        subscription: SofiSubscription,
        *,
        modified_by: int,
        cancel_kind: str | None = None,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    INSERT INTO sofi_tracker_subscriptions
                        (guild_id, user_id, drop_enabled, series_enabled, delivery_mode,
                         channel_id, modified_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE drop_enabled=VALUES(drop_enabled),
                        series_enabled=VALUES(series_enabled), delivery_mode=VALUES(delivery_mode),
                        channel_id=VALUES(channel_id), modified_by=VALUES(modified_by)
                    """,
                    (
                        subscription.guild_id,
                        subscription.user_id,
                        subscription.drop_enabled,
                        subscription.series_enabled,
                        subscription.delivery_mode,
                        subscription.channel_id,
                        modified_by,
                    ),
                )
                if cancel_kind is not None:
                    await self._cancel_timer(
                        cursor, subscription.guild_id, subscription.user_id, cancel_kind
                    )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def reconcile_timer(
        self,
        *,
        guild_id: int,
        user_id: int,
        kind: str,
        due_at_ms: int,
        source_message_id: int,
        source_at_ms: int,
        tolerance_ms: int = 20_000,
    ) -> SofiTimer | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT drop_enabled, series_enabled, channel_id FROM "
                    "sofi_tracker_subscriptions WHERE guild_id=%s AND user_id=%s FOR UPDATE",
                    (guild_id, user_id),
                )
                subscription = await cursor.fetchone()
                enabled_index = 0 if kind == "drop" else 1
                if subscription is None or not bool(subscription[enabled_index]):
                    await connection.rollback()
                    return None
                await cursor.execute(
                    "SELECT revision, active, due_at, source_message_id, source_at "
                    "FROM sofi_tracker_timers WHERE guild_id=%s AND user_id=%s "
                    "AND timer_kind=%s FOR UPDATE",
                    (guild_id, user_id, kind),
                )
                row = await cursor.fetchone()
                if row is not None:
                    previous_source_ms = _epoch_ms(row[4]) or 0
                    previous_source_id = int(row[3] or 0)
                    decision = _timer_reconciliation(
                        active=bool(row[1]),
                        current_due_ms=_epoch_ms(row[2]),
                        previous_source_ms=previous_source_ms,
                        previous_source_id=previous_source_id,
                        due_at_ms=due_at_ms,
                        source_at_ms=source_at_ms,
                        source_message_id=source_message_id,
                        tolerance_ms=tolerance_ms,
                    )
                    if decision == "stale":
                        await connection.rollback()
                        return self._timer(guild_id, user_id, kind, row)
                    current_due_ms = _epoch_ms(row[2])
                    if decision == "retain":
                        await cursor.execute(
                            "UPDATE sofi_tracker_timers SET source_message_id=%s, source_at=%s "
                            "WHERE guild_id=%s AND user_id=%s AND timer_kind=%s",
                            (
                                source_message_id,
                                _mysql_datetime(source_at_ms),
                                guild_id,
                                user_id,
                                kind,
                            ),
                        )
                        await connection.commit()
                        return SofiTimer(
                            guild_id,
                            user_id,
                            kind,
                            int(row[0]),
                            True,
                            current_due_ms,
                            source_message_id,
                            source_at_ms,
                        )
                    revision = int(row[0]) + 1
                else:
                    revision = 1
                due = _mysql_datetime(due_at_ms)
                source_at = _mysql_datetime(source_at_ms)
                await cursor.execute(
                    """
                    INSERT INTO sofi_tracker_timers
                        (guild_id,user_id,timer_kind,revision,active,due_at,
                         source_message_id,source_at)
                    VALUES (%s,%s,%s,%s,TRUE,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE revision=VALUES(revision), active=TRUE,
                        due_at=VALUES(due_at), source_message_id=VALUES(source_message_id),
                        source_at=VALUES(source_at)
                    """,
                    (guild_id, user_id, kind, revision, due, source_message_id, source_at),
                )
                await self._insert_schedule_outbox(
                    cursor,
                    guild_id,
                    user_id,
                    kind,
                    revision,
                    "upsert",
                    due,
                    subscription[2],
                )
                await connection.commit()
                return SofiTimer(
                    guild_id,
                    user_id,
                    kind,
                    revision,
                    True,
                    due_at_ms,
                    source_message_id,
                    source_at_ms,
                )
            except Exception:
                await connection.rollback()
                raise

    async def timers(self, guild_id: int, user_id: int) -> tuple[SofiTimer, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT timer_kind, revision, active, due_at, source_message_id, source_at "
                "FROM sofi_tracker_timers WHERE guild_id=%s AND user_id=%s",
                (guild_id, user_id),
            )
            rows = await cursor.fetchall()
        return tuple(
            SofiTimer(
                guild_id,
                user_id,
                str(row[0]),
                int(row[1]),
                bool(row[2]),
                _epoch_ms(row[3]),
                row[4],
                _epoch_ms(row[5]),
            )
            for row in rows
        )

    async def active_timers(self) -> tuple[tuple[SofiTimer, int | None], ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT t.guild_id,t.user_id,t.timer_kind,t.revision,t.active,t.due_at,"
                "t.source_message_id,t.source_at,s.channel_id FROM sofi_tracker_timers t "
                "JOIN sofi_tracker_subscriptions s USING (guild_id,user_id) "
                "WHERE t.active=TRUE AND ((t.timer_kind='drop' AND s.drop_enabled=TRUE) "
                "OR (t.timer_kind='series' AND s.series_enabled=TRUE))"
            )
            rows = await cursor.fetchall()
        return tuple(
            (
                SofiTimer(
                    int(row[0]),
                    int(row[1]),
                    str(row[2]),
                    int(row[3]),
                    True,
                    _epoch_ms(row[5]),
                    row[6],
                    _epoch_ms(row[7]),
                ),
                row[8],
            )
            for row in rows
        )

    async def scheduled_subscription(
        self, guild_id: int, user_id: int, kind: str, revision: int
    ) -> SofiSubscription | None:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT t.active,t.revision,s.drop_enabled,s.series_enabled,s.delivery_mode,"
                "s.channel_id FROM sofi_tracker_timers t JOIN sofi_tracker_subscriptions s "
                "USING (guild_id,user_id) WHERE t.guild_id=%s AND t.user_id=%s "
                "AND t.timer_kind=%s",
                (guild_id, user_id, kind),
            )
            row = await cursor.fetchone()
        enabled = bool(row[2] if kind == "drop" else row[3]) if row else False
        if row is None or not bool(row[0]) or int(row[1]) != revision or not enabled:
            return None
        return SofiSubscription(guild_id, user_id, bool(row[2]), bool(row[3]), str(row[4]), row[5])

    async def finish_timer(self, guild_id: int, user_id: int, kind: str, revision: int) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE sofi_tracker_timers SET active=FALSE WHERE guild_id=%s AND user_id=%s "
                "AND timer_kind=%s AND revision=%s AND active=TRUE",
                (guild_id, user_id, kind, revision),
            )
            changed = cursor.rowcount == 1
            await connection.commit()
        return changed

    async def pending_schedule_outbox(self, limit: int = 100) -> tuple[ScheduleOutboxItem, ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT id,guild_id,user_id,timer_kind,revision,operation,due_at,channel_id "
                "FROM sofi_schedule_outbox WHERE published_at IS NULL ORDER BY id LIMIT %s",
                (limit,),
            )
            rows = await cursor.fetchall()
        return tuple(
            ScheduleOutboxItem(
                int(row[0]),
                int(row[1]),
                int(row[2]),
                str(row[3]),
                int(row[4]),
                str(row[5]),
                _epoch_ms(row[6]) or 0,
                row[7],
            )
            for row in rows
        )

    async def mark_schedule_published(self, identifier: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE sofi_schedule_outbox SET published_at=CURRENT_TIMESTAMP(6) WHERE id=%s",
                (identifier,),
            )
            await connection.commit()

    async def feed(self, guild_id: int, provider: str = "sofi") -> PogDropFeed:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT channel_id,wishlist_threshold,enabled,needs_repair,revision FROM "
                "pogdrop_feeds WHERE provider_key=%s AND guild_id=%s",
                (provider, guild_id),
            )
            row = await cursor.fetchone()
        if row is None:
            return PogDropFeed(provider, guild_id, None, 100, False, False, 0)
        return PogDropFeed(
            provider, guild_id, row[0], int(row[1]), bool(row[2]), bool(row[3]), int(row[4])
        )

    async def save_feed(self, feed: PogDropFeed, *, modified_by: int) -> PogDropFeed:
        pool = self._require_pool()
        revision = max(1, feed.revision + 1)
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO pogdrop_feeds
                    (provider_key,guild_id,channel_id,wishlist_threshold,enabled,
                     needs_repair,revision,modified_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE channel_id=VALUES(channel_id),
                    wishlist_threshold=VALUES(wishlist_threshold), enabled=VALUES(enabled),
                    needs_repair=VALUES(needs_repair), revision=revision+1,
                    modified_by=VALUES(modified_by)
                """,
                (
                    feed.provider,
                    feed.guild_id,
                    feed.channel_id,
                    feed.threshold,
                    feed.enabled,
                    feed.needs_repair,
                    revision,
                    modified_by,
                ),
            )
            await connection.commit()
        return await self.feed(feed.guild_id, feed.provider)

    async def mark_feed_repair(self, guild_id: int, provider: str = "sofi") -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE pogdrop_feeds SET enabled=FALSE,needs_repair=TRUE,revision=revision+1 "
                "WHERE provider_key=%s AND guild_id=%s",
                (provider, guild_id),
            )
            await connection.commit()

    async def reserve_delivery(
        self,
        provider: str,
        source_message_id: int,
        guild_id: int,
        channel_id: int,
        feed_revision: int,
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "INSERT IGNORE INTO pogdrop_delivery_receipts "
                "(provider_key,source_message_id,guild_id,destination_channel_id,status) "
                "SELECT %s,%s,%s,%s,'reserved' FROM pogdrop_feeds WHERE provider_key=%s "
                "AND guild_id=%s AND channel_id=%s AND revision=%s AND enabled=TRUE "
                "AND needs_repair=FALSE",
                (
                    provider,
                    source_message_id,
                    guild_id,
                    channel_id,
                    provider,
                    guild_id,
                    channel_id,
                    feed_revision,
                ),
            )
            changed = cursor.rowcount == 1
            await connection.commit()
        return changed

    async def finish_delivery(
        self,
        provider: str,
        source_message_id: int,
        status: str,
        destination_message_id: int | None = None,
    ) -> None:
        if status not in {"sent", "failed", "uncertain", "skipped"}:
            raise ValueError("invalid delivery status")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE pogdrop_delivery_receipts SET status=%s,destination_message_id=%s "
                "WHERE provider_key=%s AND source_message_id=%s",
                (status, destination_message_id, provider, source_message_id),
            )
            await connection.commit()

    async def prune_receipts(self) -> int:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM pogdrop_delivery_receipts WHERE created_at < "
                "CURRENT_TIMESTAMP(6) - INTERVAL 7 DAY"
            )
            count = int(cursor.rowcount)
            await connection.commit()
        return count

    async def import_legacy(
        self,
        *,
        digest: str,
        source: str,
        subscriptions: tuple[SofiSubscription, ...],
        feed: PogDropFeed,
        details: dict[str, object],
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await cursor.execute(
                    "SELECT 1 FROM sofi_migration_runs WHERE source_digest=%s",
                    (digest,),
                )
                if await cursor.fetchone() is not None:
                    await connection.rollback()
                    return False
                for item in subscriptions:
                    await cursor.execute(
                        """
                        INSERT IGNORE INTO sofi_tracker_subscriptions
                            (guild_id,user_id,drop_enabled,series_enabled,delivery_mode,
                             channel_id,modified_by)
                        VALUES (%s,%s,TRUE,TRUE,%s,%s,0)
                        """,
                        (item.guild_id, item.user_id, item.delivery_mode, item.channel_id),
                    )
                await cursor.execute(
                    """
                    INSERT IGNORE INTO pogdrop_feeds
                        (provider_key,guild_id,channel_id,wishlist_threshold,enabled,
                         needs_repair,revision,modified_by)
                    VALUES (%s,%s,%s,%s,%s,%s,1,0)
                    """,
                    (
                        feed.provider,
                        feed.guild_id,
                        feed.channel_id,
                        feed.threshold,
                        feed.enabled,
                        feed.needs_repair,
                    ),
                )
                await cursor.execute(
                    "INSERT INTO sofi_migration_runs "
                    "(source_digest,source_label,subscription_count,feed_count,details_json) "
                    "VALUES (%s,%s,%s,1,%s)",
                    (
                        digest,
                        source[:120],
                        len(subscriptions),
                        json.dumps(details, sort_keys=True, separators=(",", ":")),
                    ),
                )
                await connection.commit()
                return True
            except Exception:
                await connection.rollback()
                raise

    async def migration_policy_pending(self, digest: str) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT policy_applied FROM sofi_migration_runs WHERE source_digest=%s",
                (digest,),
            )
            row = await cursor.fetchone()
        return bool(row is not None and not bool(row[0]))

    async def mark_migration_policy_applied(self, digest: str) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE sofi_migration_runs SET policy_applied=TRUE WHERE source_digest=%s",
                (digest,),
            )
            await connection.commit()

    @staticmethod
    def _timer(guild_id: int, user_id: int, kind: str, row: tuple[object, ...]) -> SofiTimer:
        return SofiTimer(
            guild_id,
            user_id,
            kind,
            int(row[0]),
            bool(row[1]),
            _epoch_ms(row[2]),
            row[3],
            _epoch_ms(row[4]),
        )

    async def _cancel_timer(
        self, cursor: asyncmy.cursors.Cursor, guild_id: int, user_id: int, kind: str
    ) -> None:
        await cursor.execute(
            "SELECT revision,due_at FROM sofi_tracker_timers WHERE guild_id=%s AND user_id=%s "
            "AND timer_kind=%s FOR UPDATE",
            (guild_id, user_id, kind),
        )
        row = await cursor.fetchone()
        revision = int(row[0]) + 1 if row else 1
        due = row[1] if row and row[1] is not None else datetime.now(UTC).replace(tzinfo=None)
        await cursor.execute(
            """
            INSERT INTO sofi_tracker_timers
                (guild_id,user_id,timer_kind,revision,active,due_at)
            VALUES (%s,%s,%s,%s,FALSE,NULL)
            ON DUPLICATE KEY UPDATE revision=VALUES(revision),active=FALSE,due_at=NULL,
                source_message_id=NULL,source_at=NULL
            """,
            (guild_id, user_id, kind, revision),
        )
        await self._insert_schedule_outbox(
            cursor, guild_id, user_id, kind, revision, "cancel", due, None
        )

    @staticmethod
    async def _insert_schedule_outbox(
        cursor: asyncmy.cursors.Cursor,
        guild_id: int,
        user_id: int,
        kind: str,
        revision: int,
        operation: str,
        due_at: datetime,
        channel_id: int | None,
    ) -> None:
        await cursor.execute(
            "INSERT INTO sofi_schedule_outbox "
            "(guild_id,user_id,timer_kind,revision,operation,due_at,channel_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (guild_id, user_id, kind, revision, operation, due_at, channel_id),
        )
