from __future__ import annotations

import json
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool

COOLDOWNS = {"drop": 20 * 60_000, "grab": 10 * 60_000}
DELIVERY_WINDOW_MS = 5 * 60_000
SCHEMA = (
    """CREATE TABLE IF NOT EXISTS glitch_tracker_subscriptions (
        guild_id BIGINT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL,
        drop_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        grab_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        delivery_mode ENUM('dm','channel') NOT NULL DEFAULT 'dm',
        channel_id BIGINT UNSIGNED NULL, modified_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6), PRIMARY KEY (guild_id,user_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS glitch_tracker_timers (
        guild_id BIGINT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop','grab') NOT NULL, revision BIGINT UNSIGNED NOT NULL,
        active BOOLEAN NOT NULL, due_at DATETIME(6) NULL,
        source_message_id BIGINT UNSIGNED NULL, source_at DATETIME(6) NULL,
        source_order BIGINT UNSIGNED NOT NULL DEFAULT 0,
        basis ENUM('success','reported','ready','disabled') NOT NULL DEFAULT 'success',
        origin_message_id BIGINT UNSIGNED NULL, command_id BIGINT UNSIGNED NULL,
        source_channel_id BIGINT UNSIGNED NULL,
        extra_commands LONGTEXT NOT NULL CHECK (JSON_VALID(extra_commands)),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6), PRIMARY KEY (guild_id,user_id,timer_kind),
        KEY ix_glitch_timer_due (active,due_at)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS glitch_schedule_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL, user_id BIGINT UNSIGNED NOT NULL,
        timer_kind ENUM('drop','grab') NOT NULL, revision BIGINT UNSIGNED NOT NULL,
        operation ENUM('upsert','cancel') NOT NULL, due_at DATETIME(6) NOT NULL,
        channel_id BIGINT UNSIGNED NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        published_at TIMESTAMP(6) NULL,
        UNIQUE KEY uq_glitch_schedule_revision (guild_id,user_id,timer_kind,revision),
        KEY ix_glitch_schedule_pending (published_at,id)
    ) ENGINE=InnoDB""",
)
TIMER_FIELDS = (
    "revision,active,due_at,source_message_id,source_at,source_order,basis,"
    "origin_message_id,command_id,source_channel_id,extra_commands"
)


@dataclass(frozen=True, slots=True)
class GlitchSubscription:
    guild_id: int
    user_id: int
    drop_enabled: bool = False
    grab_enabled: bool = False
    delivery_mode: str = "dm"
    channel_id: int | None = None


@dataclass(frozen=True, slots=True)
class GlitchTimer:
    guild_id: int
    user_id: int
    kind: str
    revision: int = 0
    active: bool = False
    due_at_ms: int | None = None
    source_message_id: int = 0
    source_at_ms: int = 0
    source_order: int = 0
    basis: str = "success"
    origin_message_id: int = 0
    command_id: int | None = None
    source_channel_id: int | None = None
    extra_commands: tuple[tuple[int, int, int], ...] = ()


@dataclass(frozen=True, slots=True)
class Observation:
    action: str
    message_id: int
    at_ms: int
    order: int
    channel_id: int
    command_id: int | None = None
    due_at_ms: int | None = None
    rounding_ms: int = 0
    related_message_id: int | None = None


@dataclass(frozen=True, slots=True)
class ScheduleItem:
    identifier: int
    guild_id: int
    user_id: int
    kind: str
    revision: int
    operation: str
    due_at_ms: int
    channel_id: int | None


def transition(timer: GlitchTimer, event: Observation, now: int) -> GlitchTimer:

    if timer.kind not in COOLDOWNS or event.action not in {"success", "reported", "ready", "extra"}:
        raise ValueError("invalid Glitch observation")
    if min(event.message_id, event.at_ms, event.channel_id) <= 0 or event.order < 0:
        raise ValueError("invalid Glitch source")
    if event.action == "extra":
        if not event.command_id and not event.related_message_id:
            return timer
        linked = (
            event.channel_id,
            -event.related_message_id if event.related_message_id else event.command_id,
        )
        receipts = tuple(item for item in timer.extra_commands if now - item[2] < 40 * 60_000)
        if not any(item[:2] == linked for item in receipts):
            receipts = (*receipts, (*linked, event.at_ms))[-64:]
        updated = replace(timer, extra_commands=receipts)
        if (
            timer.active
            and timer.basis == "success"
            and (
                timer.source_channel_id,
                -timer.origin_message_id if event.related_message_id else timer.command_id,
            )
            == linked
        ):
            updated = replace(updated, active=False, revision=timer.revision + 1)
        return updated
    key = (event.at_ms, event.message_id, event.order)
    previous = (timer.source_at_ms, timer.source_message_id, timer.source_order)
    if key <= previous or (
        event.action == "success"
        and event.message_id in {timer.source_message_id, timer.origin_message_id}
    ):
        return timer
    if event.action == "success" and any(
        item[:2]
        in {
            (event.channel_id, event.command_id),
            (event.channel_id, -event.message_id),
        }
        for item in timer.extra_commands
    ):
        return timer
    updated = replace(
        timer,
        source_message_id=event.message_id,
        source_at_ms=event.at_ms,
        source_order=event.order,
    )
    if (
        event.action == "success"
        and timer.active
        and timer.due_at_ms
        and timer.due_at_ms > event.at_ms
    ):

        return updated
    due = event.at_ms + COOLDOWNS[timer.kind] if event.action == "success" else event.due_at_ms
    if event.action == "ready":
        due = event.at_ms
    if due is None or due <= 0:
        raise ValueError("missing Glitch cooldown")
    active = due > event.at_ms and due + DELIVERY_WINDOW_MS > now
    if (
        timer.active == active
        and timer.due_at_ms is not None
        and abs(timer.due_at_ms - due) <= event.rounding_ms
    ):
        return updated
    return replace(
        updated,
        revision=timer.revision + 1,
        active=active,
        due_at_ms=due,
        basis=event.action,
        origin_message_id=event.message_id,
        command_id=event.command_id,
        source_channel_id=event.channel_id,
    )


def _date(value: int | None) -> datetime | None:
    return datetime.fromtimestamp(value / 1000, UTC).replace(tzinfo=None) if value else None


def _ms(value: datetime | None) -> int | None:
    return int(value.replace(tzinfo=UTC).timestamp() * 1000) if value else None


def _timer(guild: int, user: int, kind: str, row) -> GlitchTimer:
    if row is None:
        return GlitchTimer(guild, user, kind)
    return GlitchTimer(
        guild,
        user,
        kind,
        int(row[0]),
        bool(row[1]),
        _ms(row[2]),
        int(row[3] or 0),
        _ms(row[4]) or 0,
        int(row[5]),
        str(row[6]),
        int(row[7] or 0),
        row[8],
        row[9],
        tuple(tuple(item) for item in json.loads(row[10])),
    )


class MySQLGlitchTrackerStore:
    def __init__(self, dsn: str, *, clock=None):
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("invalid Glitch database DSN")
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
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    async def connect(self):
        self._pool = await create_pool(**self._settings)

    async def close(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    async def migrate(self):
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            for statement in SCHEMA:
                await cursor.execute(statement)

    async def subscription(self, guild: int, user: int) -> GlitchSubscription:
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT drop_enabled,grab_enabled,delivery_mode,channel_id FROM "
                "glitch_tracker_subscriptions WHERE guild_id=%s AND user_id=%s",
                (guild, user),
            )
            row = await cursor.fetchone()
        return (
            GlitchSubscription(guild, user, bool(row[0]), bool(row[1]), str(row[2]), row[3])
            if row
            else GlitchSubscription(guild, user)
        )

    async def save_subscription(
        self, state: GlitchSubscription, *, modified_by: int, cancel_kind: str | None = None
    ):
        if modified_by != state.user_id or state.delivery_mode not in {"dm", "channel"}:
            raise ValueError("invalid Glitch subscription")
        if cancel_kind is not None and cancel_kind not in COOLDOWNS:
            raise ValueError("invalid Glitch timer kind")
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute(
                    "SELECT delivery_mode,channel_id FROM glitch_tracker_subscriptions "
                    "WHERE guild_id=%s AND user_id=%s FOR UPDATE",
                    (state.guild_id, state.user_id),
                )
                old = await cursor.fetchone()
                await cursor.execute(
                    "INSERT INTO glitch_tracker_subscriptions "
                    "(guild_id,user_id,drop_enabled,grab_enabled,delivery_mode,"
                    "channel_id,modified_by) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                    "drop_enabled=VALUES(drop_enabled),grab_enabled=VALUES(grab_enabled),"
                    "delivery_mode=VALUES(delivery_mode),channel_id=VALUES(channel_id),"
                    "modified_by=VALUES(modified_by)",
                    (
                        state.guild_id,
                        state.user_id,
                        state.drop_enabled,
                        state.grab_enabled,
                        state.delivery_mode,
                        state.channel_id,
                        modified_by,
                    ),
                )
                moved = old is not None and tuple(old) != (state.delivery_mode, state.channel_id)
                for kind in COOLDOWNS:
                    timer = await self._locked_timer(cursor, state.guild_id, state.user_id, kind)
                    if not timer.active:
                        continue
                    if kind == cancel_kind:
                        updated = replace(
                            timer, active=False, revision=timer.revision + 1, basis="disabled"
                        )
                    elif moved:
                        updated = replace(timer, revision=timer.revision + 1)
                    else:
                        continue
                    await self._write_timer(cursor, updated)
                    await self._outbox(cursor, updated, state.channel_id)
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise

    async def record(self, guild: int, user: int, kind: str, event: Observation):
        if kind not in COOLDOWNS:
            raise ValueError("invalid Glitch timer kind")
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute(
                    "SELECT drop_enabled,grab_enabled,channel_id FROM glitch_tracker_subscriptions "
                    "WHERE guild_id=%s AND user_id=%s FOR UPDATE",
                    (guild, user),
                )
                subscription = await cursor.fetchone()
                if subscription is None or not bool(subscription[0 if kind == "drop" else 1]):
                    await connection.rollback()
                    return None
                before = await self._locked_timer(cursor, guild, user, kind)
                after = transition(before, event, self.clock())
                if after != before:
                    await self._write_timer(cursor, after)
                    if after.revision != before.revision:
                        await self._outbox(cursor, after, subscription[2])
                await connection.commit()
                return after
            except BaseException:
                await connection.rollback()
                raise

    async def _locked_timer(self, cursor, guild, user, kind):
        await cursor.execute(
            f"SELECT {TIMER_FIELDS} FROM glitch_tracker_timers WHERE guild_id=%s "
            "AND user_id=%s AND timer_kind=%s FOR UPDATE",
            (guild, user, kind),
        )
        return _timer(guild, user, kind, await cursor.fetchone())

    @staticmethod
    async def _write_timer(cursor, timer):
        await cursor.execute(
            "INSERT INTO glitch_tracker_timers (guild_id,user_id,timer_kind,"
            + TIMER_FIELDS
            + ") VALUES ("
            + ",".join(["%s"] * 14)
            + ") "
            "ON DUPLICATE KEY UPDATE "
            + ",".join(field + "=VALUES(" + field + ")" for field in TIMER_FIELDS.split(",")),
            (
                timer.guild_id,
                timer.user_id,
                timer.kind,
                timer.revision,
                timer.active,
                _date(timer.due_at_ms),
                timer.source_message_id or None,
                _date(timer.source_at_ms),
                timer.source_order,
                timer.basis,
                timer.origin_message_id or None,
                timer.command_id,
                timer.source_channel_id,
                json.dumps(timer.extra_commands),
            ),
        )

    async def _outbox(self, cursor, timer, channel):
        await cursor.execute(
            "INSERT INTO glitch_schedule_outbox "
            "(guild_id,user_id,timer_kind,revision,operation,due_at,channel_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                timer.guild_id,
                timer.user_id,
                timer.kind,
                timer.revision,
                "upsert" if timer.active else "cancel",
                _date(timer.due_at_ms or self.clock()),
                channel,
            ),
        )

    async def timers(self, guild: int, user: int) -> tuple[GlitchTimer, ...]:
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                f"SELECT timer_kind,{TIMER_FIELDS} FROM glitch_tracker_timers "
                "WHERE guild_id=%s AND user_id=%s",
                (guild, user),
            )
            rows = await cursor.fetchall()
        return tuple(_timer(guild, user, str(row[0]), row[1:]) for row in rows)

    async def active_timers(self):
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT t.guild_id,t.user_id,t.timer_kind,"
                + ",".join("t." + field for field in TIMER_FIELDS.split(","))
                + ",s.channel_id FROM glitch_tracker_timers t JOIN glitch_tracker_subscriptions s "
                "USING(guild_id,user_id) WHERE t.active=TRUE AND "
                "((t.timer_kind='drop' AND s.drop_enabled) OR "
                "(t.timer_kind='grab' AND s.grab_enabled))"
            )
            rows = await cursor.fetchall()
        return tuple(
            (_timer(int(row[0]), int(row[1]), str(row[2]), row[3:-1]), row[-1]) for row in rows
        )

    async def scheduled_subscription(self, guild: int, user: int, kind: str, revision: int):
        if kind not in COOLDOWNS:
            return None
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT t.active,t.revision,t.due_at,s.drop_enabled,s.grab_enabled,"
                "s.delivery_mode,s.channel_id FROM glitch_tracker_timers t JOIN "
                "glitch_tracker_subscriptions s USING(guild_id,user_id) WHERE t.guild_id=%s "
                "AND t.user_id=%s AND t.timer_kind=%s",
                (guild, user, kind),
            )
            row = await cursor.fetchone()
        now = self.clock()
        due = _ms(row[2]) if row else None
        if (
            row is None
            or not row[0]
            or int(row[1]) != revision
            or due is None
            or not due <= now < due + DELIVERY_WINDOW_MS
            or not row[3 if kind == "drop" else 4]
        ):
            return None
        return GlitchSubscription(guild, user, bool(row[3]), bool(row[4]), str(row[5]), row[6])

    async def finish_timer(self, guild: int, user: int, kind: str, revision: int):
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE glitch_tracker_timers SET active=FALSE WHERE guild_id=%s "
                "AND user_id=%s AND timer_kind=%s AND revision=%s AND active=TRUE",
                (guild, user, kind, revision),
            )
            return cursor.rowcount == 1

    async def pending_schedule_outbox(self, limit: int = 100):
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT id,guild_id,user_id,timer_kind,revision,operation,due_at,channel_id "
                "FROM glitch_schedule_outbox WHERE published_at IS NULL ORDER BY id LIMIT %s",
                (min(100, max(1, limit)),),
            )
            rows = await cursor.fetchall()
        return tuple(
            ScheduleItem(
                int(row[0]),
                int(row[1]),
                int(row[2]),
                str(row[3]),
                int(row[4]),
                str(row[5]),
                _ms(row[6]) or 0,
                row[7],
            )
            for row in rows
        )

    async def mark_schedule_published(self, identifier: int):
        async with self._pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE glitch_schedule_outbox SET published_at=CURRENT_TIMESTAMP(6) WHERE id=%s",
                (identifier,),
            )
