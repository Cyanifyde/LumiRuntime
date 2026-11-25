from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool, read_cursor

KARUTA_TRACKER_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS karuta_tracker_subscriptions (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        drop_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        pickup_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        delivery_mode ENUM('dm', 'channel') NOT NULL DEFAULT 'dm',
        channel_id BIGINT UNSIGNED NULL,
        modified_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS karuta_tracker_profiles (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop', 'pickup') NOT NULL,
        cooldown_seconds SMALLINT UNSIGNED NOT NULL,
        learned_from ENUM('interval', 'cooldown') NOT NULL,
        observed_at DATETIME(6) NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id, timer_kind)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS karuta_tracker_observations (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop', 'pickup') NOT NULL,
        source_message_id BIGINT UNSIGNED NOT NULL,
        source_at DATETIME(6) NOT NULL,
        source_order BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id, timer_kind),
        UNIQUE KEY uq_karuta_observation_message (guild_id, timer_kind, source_message_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS karuta_tracker_timers (
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop', 'pickup') NOT NULL,
        revision BIGINT UNSIGNED NOT NULL,
        active BOOLEAN NOT NULL,
        due_at DATETIME(6) NULL,
        source_message_id BIGINT UNSIGNED NULL,
        source_at DATETIME(6) NULL,
        source_order BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, user_id, timer_kind),
        KEY ix_karuta_timer_due (active, due_at)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS karuta_schedule_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop', 'pickup') NOT NULL,
        revision BIGINT UNSIGNED NOT NULL,
        operation ENUM('upsert', 'cancel') NOT NULL,
        due_at DATETIME(6) NOT NULL,
        channel_id BIGINT UNSIGNED NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        published_at TIMESTAMP(6) NULL,
        UNIQUE KEY uq_karuta_schedule_revision (guild_id, user_id, timer_kind, revision),
        KEY ix_karuta_schedule_pending (published_at, id)
    ) ENGINE=InnoDB
    """,
)


@dataclass(frozen=True, slots=True)
class KarutaSubscription:
    guild_id: int
    user_id: int
    drop_enabled: bool
    pickup_enabled: bool
    delivery_mode: str
    channel_id: int | None


@dataclass(frozen=True, slots=True)
class KarutaTimer:
    guild_id: int
    user_id: int
    kind: str
    revision: int
    active: bool
    due_at_ms: int | None
    source_message_id: int | None
    source_at_ms: int | None
    source_order: int = 0


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


@dataclass(frozen=True, slots=True)
class WaitResolution:
    due_at_ms: int
    learned_cooldown_ms: int | None


def _mysql_datetime(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1_000, tz=UTC).replace(tzinfo=None)


def _epoch_ms(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.timestamp() * 1_000)


def _event_key(
    source_at_ms: int, source_order: int, source_message_id: int
) -> tuple[int, int, int]:
    return source_at_ms, source_order, source_message_id


def _inferred_cooldown(
    *, interval_ms: int | None, current_ms: int | None, normal_ms: int, half_ms: int
) -> int | None:
    if interval_ms is not None and 0 <= interval_ms < normal_ms:
        return half_ms
    return current_ms


def _resolve_wait(
    *,
    action_at_ms: int | None,
    current_due_ms: int | None,
    reported_at_ms: int,
    remaining_ms: int,
    rounding_ms: int,
    normal_ms: int,
    half_ms: int,
) -> WaitResolution:
    lower = reported_at_ms + remaining_ms
    upper = lower + max(0, rounding_ms)
    candidates: list[tuple[int, int]] = []
    if action_at_ms is not None:
        for cooldown in (normal_ms, half_ms):
            due = action_at_ms + cooldown
            if lower <= due <= upper:
                candidates.append((cooldown, due))
    if len(candidates) == 1:
        cooldown, due = candidates[0]
        return WaitResolution(due, cooldown)
    if current_due_ms is not None and lower <= current_due_ms <= upper:
        learned = normal_ms if remaining_ms > half_ms else None
        return WaitResolution(current_due_ms, learned)
    learned = normal_ms if remaining_ms > half_ms else None
    return WaitResolution(upper, learned)


class MySQLKarutaTrackerStore:
    def __init__(self, dsn: str) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("Karuta tracker DSN must use mysql:// or mariadb://")
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
            raise RuntimeError("Karuta tracker store is not connected")
        return self._pool

    async def migrate(self) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for statement in KARUTA_TRACKER_SCHEMA:
                    await cursor.execute(statement)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def subscription(self, guild_id: int, user_id: int) -> KarutaSubscription:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT drop_enabled,pickup_enabled,delivery_mode,channel_id "
                "FROM karuta_tracker_subscriptions WHERE guild_id=%s AND user_id=%s",
                (guild_id, user_id),
            )
            row = await cursor.fetchone()
        if row is None:
            return KarutaSubscription(guild_id, user_id, False, False, "dm", None)
        return KarutaSubscription(
            guild_id, user_id, bool(row[0]), bool(row[1]), str(row[2]), row[3]
        )

    async def save_subscription(
        self,
        subscription: KarutaSubscription,
        *,
        modified_by: int,
        cancel_kind: str | None = None,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    INSERT INTO karuta_tracker_subscriptions
                        (guild_id,user_id,drop_enabled,pickup_enabled,delivery_mode,
                         channel_id,modified_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE drop_enabled=VALUES(drop_enabled),
                        pickup_enabled=VALUES(pickup_enabled),
                        delivery_mode=VALUES(delivery_mode),channel_id=VALUES(channel_id),
                        modified_by=VALUES(modified_by)
                    """,
                    (
                        subscription.guild_id,
                        subscription.user_id,
                        subscription.drop_enabled,
                        subscription.pickup_enabled,
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

    async def record_success(
        self,
        *,
        guild_id: int,
        user_id: int,
        kind: str,
        source_message_id: int,
        source_at_ms: int,
        source_order: int,
        normal_ms: int,
        half_ms: int,
    ) -> KarutaTimer | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                subscription = await self._locked_subscription(cursor, guild_id, user_id, kind)
                if subscription is None:
                    await connection.rollback()
                    return None
                await cursor.execute(
                    "SELECT source_message_id,source_at,source_order FROM "
                    "karuta_tracker_observations WHERE guild_id=%s AND user_id=%s "
                    "AND timer_kind=%s FOR UPDATE",
                    (guild_id, user_id, kind),
                )
                observation = await cursor.fetchone()
                interval_ms: int | None = None
                if observation is not None:
                    previous_id = int(observation[0])
                    previous_at = _epoch_ms(observation[1]) or 0
                    previous_order = int(observation[2] or 0)
                    if previous_id == source_message_id or _event_key(
                        source_at_ms, source_order, source_message_id
                    ) <= _event_key(previous_at, previous_order, previous_id):
                        await connection.rollback()
                        return None
                    interval_ms = source_at_ms - previous_at

                timer_row = await self._locked_timer(cursor, guild_id, user_id, kind)
                if timer_row is not None:
                    timer_id = int(timer_row[3] or 0)
                    timer_at = _epoch_ms(timer_row[4]) or 0
                    timer_order = int(timer_row[5] or 0)
                    if timer_id == source_message_id or _event_key(
                        source_at_ms, source_order, source_message_id
                    ) <= _event_key(timer_at, timer_order, timer_id):
                        await connection.rollback()
                        return None

                await cursor.execute(
                    "SELECT cooldown_seconds FROM karuta_tracker_profiles "
                    "WHERE guild_id=%s AND user_id=%s AND timer_kind=%s FOR UPDATE",
                    (guild_id, user_id, kind),
                )
                profile = await cursor.fetchone()
                current_ms = int(profile[0]) * 1_000 if profile else None
                learned_ms = _inferred_cooldown(
                    interval_ms=interval_ms,
                    current_ms=current_ms,
                    normal_ms=normal_ms,
                    half_ms=half_ms,
                )
                if learned_ms is not None and learned_ms != current_ms:
                    await self._upsert_profile(
                        cursor,
                        guild_id,
                        user_id,
                        kind,
                        learned_ms,
                        "interval",
                        source_at_ms,
                    )
                cooldown_ms = learned_ms or normal_ms
                await cursor.execute(
                    """
                    INSERT INTO karuta_tracker_observations
                        (guild_id,user_id,timer_kind,source_message_id,source_at,source_order)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE source_message_id=VALUES(source_message_id),
                        source_at=VALUES(source_at),source_order=VALUES(source_order)
                    """,
                    (
                        guild_id,
                        user_id,
                        kind,
                        source_message_id,
                        _mysql_datetime(source_at_ms),
                        source_order,
                    ),
                )
                timer = await self._replace_timer(
                    cursor,
                    guild_id=guild_id,
                    user_id=user_id,
                    kind=kind,
                    timer_row=timer_row,
                    due_at_ms=source_at_ms + cooldown_ms,
                    source_message_id=source_message_id,
                    source_at_ms=source_at_ms,
                    source_order=source_order,
                    channel_id=subscription[2],
                )
                await connection.commit()
                return timer
            except Exception:
                await connection.rollback()
                raise

    async def record_wait(
        self,
        *,
        guild_id: int,
        user_id: int,
        kind: str,
        source_message_id: int,
        source_at_ms: int,
        source_order: int,
        remaining_ms: int,
        rounding_ms: int,
        normal_ms: int,
        half_ms: int,
    ) -> KarutaTimer | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                subscription = await self._locked_subscription(cursor, guild_id, user_id, kind)
                if subscription is None:
                    await connection.rollback()
                    return None
                timer_row = await self._locked_timer(cursor, guild_id, user_id, kind)
                if timer_row is not None:
                    previous_id = int(timer_row[3] or 0)
                    previous_at = _epoch_ms(timer_row[4]) or 0
                    previous_order = int(timer_row[5] or 0)
                    if previous_id == source_message_id or _event_key(
                        source_at_ms, source_order, source_message_id
                    ) <= _event_key(previous_at, previous_order, previous_id):
                        await connection.rollback()
                        return None
                await cursor.execute(
                    "SELECT source_at FROM karuta_tracker_observations "
                    "WHERE guild_id=%s AND user_id=%s AND timer_kind=%s FOR UPDATE",
                    (guild_id, user_id, kind),
                )
                observation = await cursor.fetchone()
                action_at_ms = _epoch_ms(observation[0]) if observation else None
                current_due_ms = _epoch_ms(timer_row[2]) if timer_row else None
                resolution = _resolve_wait(
                    action_at_ms=action_at_ms,
                    current_due_ms=current_due_ms,
                    reported_at_ms=source_at_ms,
                    remaining_ms=remaining_ms,
                    rounding_ms=rounding_ms,
                    normal_ms=normal_ms,
                    half_ms=half_ms,
                )
                if resolution.learned_cooldown_ms is not None:
                    await self._upsert_profile(
                        cursor,
                        guild_id,
                        user_id,
                        kind,
                        resolution.learned_cooldown_ms,
                        "cooldown",
                        source_at_ms,
                    )
                if (
                    timer_row is not None
                    and bool(timer_row[1])
                    and current_due_ms == resolution.due_at_ms
                ):
                    await cursor.execute(
                        "UPDATE karuta_tracker_timers SET source_message_id=%s,source_at=%s,"
                        "source_order=%s WHERE guild_id=%s AND user_id=%s AND timer_kind=%s",
                        (
                            source_message_id,
                            _mysql_datetime(source_at_ms),
                            source_order,
                            guild_id,
                            user_id,
                            kind,
                        ),
                    )
                    await connection.commit()
                    return None
                timer = await self._replace_timer(
                    cursor,
                    guild_id=guild_id,
                    user_id=user_id,
                    kind=kind,
                    timer_row=timer_row,
                    due_at_ms=resolution.due_at_ms,
                    source_message_id=source_message_id,
                    source_at_ms=source_at_ms,
                    source_order=source_order,
                    channel_id=subscription[2],
                )
                await connection.commit()
                return timer
            except Exception:
                await connection.rollback()
                raise

    async def timers(self, guild_id: int, user_id: int) -> tuple[KarutaTimer, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT timer_kind,revision,active,due_at,source_message_id,source_at,"
                "source_order FROM karuta_tracker_timers WHERE guild_id=%s AND user_id=%s",
                (guild_id, user_id),
            )
            rows = await cursor.fetchall()
        return tuple(
            KarutaTimer(
                guild_id,
                user_id,
                str(row[0]),
                int(row[1]),
                bool(row[2]),
                _epoch_ms(row[3]),
                row[4],
                _epoch_ms(row[5]),
                int(row[6] or 0),
            )
            for row in rows
        )

    async def active_timers(self) -> tuple[tuple[KarutaTimer, int | None], ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT t.guild_id,t.user_id,t.timer_kind,t.revision,t.due_at,"
                "t.source_message_id,t.source_at,t.source_order,s.channel_id "
                "FROM karuta_tracker_timers t JOIN karuta_tracker_subscriptions s "
                "USING (guild_id,user_id) WHERE t.active=TRUE AND "
                "((t.timer_kind='drop' AND s.drop_enabled=TRUE) OR "
                "(t.timer_kind='pickup' AND s.pickup_enabled=TRUE))"
            )
            rows = await cursor.fetchall()
        return tuple(
            (
                KarutaTimer(
                    int(row[0]),
                    int(row[1]),
                    str(row[2]),
                    int(row[3]),
                    True,
                    _epoch_ms(row[4]),
                    row[5],
                    _epoch_ms(row[6]),
                    int(row[7] or 0),
                ),
                row[8],
            )
            for row in rows
        )

    async def scheduled_subscription(
        self, guild_id: int, user_id: int, kind: str, revision: int
    ) -> KarutaSubscription | None:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT t.active,t.revision,s.drop_enabled,s.pickup_enabled,s.delivery_mode,"
                "s.channel_id FROM karuta_tracker_timers t JOIN "
                "karuta_tracker_subscriptions s USING (guild_id,user_id) "
                "WHERE t.guild_id=%s AND t.user_id=%s AND t.timer_kind=%s",
                (guild_id, user_id, kind),
            )
            row = await cursor.fetchone()
        enabled = bool(row[2] if kind == "drop" else row[3]) if row else False
        if row is None or not bool(row[0]) or int(row[1]) != revision or not enabled:
            return None
        return KarutaSubscription(
            guild_id, user_id, bool(row[2]), bool(row[3]), str(row[4]), row[5]
        )

    async def finish_timer(self, guild_id: int, user_id: int, kind: str, revision: int) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE karuta_tracker_timers SET active=FALSE WHERE guild_id=%s "
                "AND user_id=%s AND timer_kind=%s AND revision=%s AND active=TRUE",
                (guild_id, user_id, kind, revision),
            )
            changed = cursor.rowcount == 1
            await connection.commit()
        return changed

    async def pending_schedule_outbox(self, limit: int = 100) -> tuple[ScheduleOutboxItem, ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT id,guild_id,user_id,timer_kind,revision,operation,due_at,channel_id "
                "FROM karuta_schedule_outbox WHERE published_at IS NULL ORDER BY id LIMIT %s",
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
                "UPDATE karuta_schedule_outbox SET published_at=CURRENT_TIMESTAMP(6) WHERE id=%s",
                (identifier,),
            )
            await connection.commit()

    @staticmethod
    async def _locked_subscription(
        cursor: asyncmy.cursors.Cursor, guild_id: int, user_id: int, kind: str
    ) -> tuple[object, ...] | None:
        await cursor.execute(
            "SELECT drop_enabled,pickup_enabled,channel_id FROM "
            "karuta_tracker_subscriptions WHERE guild_id=%s AND user_id=%s FOR UPDATE",
            (guild_id, user_id),
        )
        row = await cursor.fetchone()
        if row is None or not bool(row[0] if kind == "drop" else row[1]):
            return None
        return row

    @staticmethod
    async def _locked_timer(
        cursor: asyncmy.cursors.Cursor, guild_id: int, user_id: int, kind: str
    ) -> tuple[object, ...] | None:
        await cursor.execute(
            "SELECT revision,active,due_at,source_message_id,source_at,source_order "
            "FROM karuta_tracker_timers WHERE guild_id=%s AND user_id=%s "
            "AND timer_kind=%s FOR UPDATE",
            (guild_id, user_id, kind),
        )
        return await cursor.fetchone()

    @staticmethod
    async def _upsert_profile(
        cursor: asyncmy.cursors.Cursor,
        guild_id: int,
        user_id: int,
        kind: str,
        cooldown_ms: int,
        learned_from: str,
        observed_at_ms: int,
    ) -> None:
        await cursor.execute(
            """
            INSERT INTO karuta_tracker_profiles
                (guild_id,user_id,timer_kind,cooldown_seconds,learned_from,observed_at)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE cooldown_seconds=VALUES(cooldown_seconds),
                learned_from=VALUES(learned_from),observed_at=VALUES(observed_at)
            """,
            (
                guild_id,
                user_id,
                kind,
                cooldown_ms // 1_000,
                learned_from,
                _mysql_datetime(observed_at_ms),
            ),
        )

    async def _replace_timer(
        self,
        cursor: asyncmy.cursors.Cursor,
        *,
        guild_id: int,
        user_id: int,
        kind: str,
        timer_row: tuple[object, ...] | None,
        due_at_ms: int,
        source_message_id: int,
        source_at_ms: int,
        source_order: int,
        channel_id: int | None,
    ) -> KarutaTimer:
        revision = int(timer_row[0]) + 1 if timer_row else 1
        due = _mysql_datetime(due_at_ms)
        await cursor.execute(
            """
            INSERT INTO karuta_tracker_timers
                (guild_id,user_id,timer_kind,revision,active,due_at,source_message_id,
                 source_at,source_order)
            VALUES (%s,%s,%s,%s,TRUE,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE revision=VALUES(revision),active=TRUE,
                due_at=VALUES(due_at),source_message_id=VALUES(source_message_id),
                source_at=VALUES(source_at),source_order=VALUES(source_order)
            """,
            (
                guild_id,
                user_id,
                kind,
                revision,
                due,
                source_message_id,
                _mysql_datetime(source_at_ms),
                source_order,
            ),
        )
        await self._insert_schedule_outbox(
            cursor, guild_id, user_id, kind, revision, "upsert", due, channel_id
        )
        return KarutaTimer(
            guild_id,
            user_id,
            kind,
            revision,
            True,
            due_at_ms,
            source_message_id,
            source_at_ms,
            source_order,
        )

    async def _cancel_timer(
        self, cursor: asyncmy.cursors.Cursor, guild_id: int, user_id: int, kind: str
    ) -> None:
        await cursor.execute(
            "SELECT revision,due_at FROM karuta_tracker_timers WHERE guild_id=%s "
            "AND user_id=%s AND timer_kind=%s FOR UPDATE",
            (guild_id, user_id, kind),
        )
        row = await cursor.fetchone()
        revision = int(row[0]) + 1 if row else 1
        due = row[1] if row and row[1] is not None else datetime.now(UTC).replace(tzinfo=None)
        await cursor.execute(
            """
            INSERT INTO karuta_tracker_timers
                (guild_id,user_id,timer_kind,revision,active,due_at)
            VALUES (%s,%s,%s,%s,FALSE,NULL)
            ON DUPLICATE KEY UPDATE revision=VALUES(revision),active=FALSE,due_at=NULL,
                source_message_id=NULL,source_at=NULL,source_order=0
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
            "INSERT INTO karuta_schedule_outbox "
            "(guild_id,user_id,timer_kind,revision,operation,due_at,channel_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (guild_id, user_id, kind, revision, operation, due_at, channel_id),
        )
