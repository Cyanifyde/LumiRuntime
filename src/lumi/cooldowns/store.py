from __future__ import annotations

import asyncio
import json
import secrets
import time
import weakref
from datetime import UTC, datetime

from asyncmy.errors import IntegrityError, OperationalError

from .models import (
    DELIVERY_MS,
    MAX_RECIPIENTS,
    TERMINAL,
    TRACKERS,
    WINDOW_MS,
    Batch,
    Occurrence,
    Recipient,
)

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS card_cooldown_batches (
        id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
        tracker VARCHAR(32) NOT NULL, guild_id BIGINT UNSIGNED NOT NULL,
        channel_id BIGINT UNSIGNED NOT NULL, timer_kind VARCHAR(16) NOT NULL,
        revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
        opened_at_ms BIGINT UNSIGNED NOT NULL, flush_at_ms BIGINT UNSIGNED NOT NULL,
        expires_at_ms BIGINT UNSIGNED NOT NULL, open_slot TINYINT UNSIGNED NULL,
        item_count SMALLINT UNSIGNED NOT NULL DEFAULT 0,
        state ENUM('open','ready','sending','sent','skipped','failed','uncertain') NOT NULL,
        outcome LONGTEXT NOT NULL CHECK (JSON_VALID(outcome)),
        dispatch_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        next_dispatch_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
        attempt_at_ms BIGINT UNSIGNED NULL, settled BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        UNIQUE KEY uq_card_cooldown_open (tracker,guild_id,channel_id,timer_kind,open_slot),
        KEY ix_card_cooldown_due (state,next_dispatch_ms),
        KEY ix_card_cooldown_retention (settled,created_at)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS card_cooldown_batch_items (
        tracker VARCHAR(32) NOT NULL, guild_id BIGINT UNSIGNED NOT NULL,
        user_id BIGINT UNSIGNED NOT NULL, timer_kind VARCHAR(16) NOT NULL,
        timer_revision BIGINT UNSIGNED NOT NULL,
        batch_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        due_at_ms BIGINT UNSIGNED NOT NULL, expires_at_ms BIGINT UNSIGNED NOT NULL,
        delivery_mode ENUM('channel','dm') NOT NULL,
        state ENUM('pending','sent','skipped','failed','uncertain') NOT NULL DEFAULT 'pending',
        PRIMARY KEY (tracker,guild_id,user_id,timer_kind,timer_revision),
        KEY ix_card_cooldown_members (batch_id,user_id)
    ) ENGINE=InnoDB""",
)
BATCH_FIELDS = (
    "id,tracker,guild_id,channel_id,timer_kind,revision,opened_at_ms,flush_at_ms,"
    "expires_at_ms,state,outcome"
)
ITEM_KEY = "tracker=%s AND guild_id=%s AND user_id=%s AND timer_kind=%s AND timer_revision=%s"


def milliseconds(value):
    return round(value.replace(tzinfo=UTC).timestamp() * 1000) if value else None


def date(value):
    return datetime.fromtimestamp(value / 1000, UTC).replace(tzinfo=None)


def batch_row(row):
    return Batch(*row[:-1], json.loads(row[-1])) if row else None


def source_tables(tracker, kind):
    _acl, kinds = TRACKERS[tracker]
    return tracker + "_timers", tracker + "_subscriptions", kinds[kind]


class MySQLCooldownStore:


    def __init__(self, owner, *, clock=None):
        self.owner = owner
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._locks = weakref.WeakValueDictionary()

    @property
    def pool(self):
        pool = getattr(self.owner, "_pool", None)
        if pool is None:
            raise RuntimeError("cooldown metadata unavailable")
        return pool

    async def migrate(self):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            for statement in SCHEMA:
                await cursor.execute(statement)
            await connection.commit()

    async def _source(self, cursor, tracker, guild, user, kind, *, lock=False):
        timers, subscriptions, enabled = source_tables(tracker, kind)
        await cursor.execute(
            f"SELECT t.revision,t.due_at,s.channel_id,s.delivery_mode FROM {timers} t "
            f"JOIN {subscriptions} s USING(guild_id,user_id) WHERE t.guild_id=%s "
            f"AND t.user_id=%s AND t.timer_kind=%s AND t.active=TRUE AND s.{enabled}=TRUE"
            + (" FOR UPDATE" if lock else ""),
            (guild, user, kind),
        )
        row = await cursor.fetchone()
        if row is None or not row[1] or not row[2]:
            return None
        return Occurrence(
            tracker, guild, user, kind, int(row[0]), milliseconds(row[1]), int(row[2]), row[3]
        )

    async def enroll(self, tracker, guild, user, kind, revision, mode):
        if (
            tracker not in TRACKERS
            or kind not in TRACKERS[tracker][1]
            or mode not in {"channel", "dm"}
            or min(guild, user, revision) <= 0
        ):
            raise ValueError("invalid cooldown occurrence")
        lock = self._locks.setdefault((tracker, guild, kind), asyncio.Lock())
        async with lock:
            for attempt in range(4):
                try:
                    return await self._enroll(tracker, guild, user, kind, revision, mode)
                except (IntegrityError, OperationalError) as error:
                    if error.args[0] not in {1062, 1205, 1213} or attempt == 3:
                        raise
                    await asyncio.sleep(0.01 * (attempt + 1))

    async def _enroll(self, tracker, guild, user, kind, revision, mode):
        now = self.clock()
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                source = await self._source(cursor, tracker, guild, user, kind, lock=True)
                if (
                    source is None
                    or source.revision != revision
                    or source.mode != mode
                    or not source.due_at_ms <= now < source.expires_at_ms
                ):
                    await connection.rollback()
                    return None
                await cursor.execute(
                    "SELECT batch_id FROM card_cooldown_batch_items WHERE " + ITEM_KEY, source.key
                )
                existing = await cursor.fetchone()
                if existing:
                    await connection.rollback()
                    return existing[0]
                group = (tracker, guild, source.channel_id, kind)
                await cursor.execute(
                    "SELECT " + BATCH_FIELDS + " FROM card_cooldown_batches WHERE tracker=%s "
                    "AND guild_id=%s AND channel_id=%s AND timer_kind=%s AND "
                    "open_slot=1 FOR UPDATE",
                    group,
                )
                batch = batch_row(await cursor.fetchone())
                if batch and not (
                    batch.opened_at_ms <= source.due_at_ms < batch.flush_at_ms
                    and now < batch.flush_at_ms
                ):
                    await cursor.execute(
                        "UPDATE card_cooldown_batches SET open_slot=NULL WHERE id=%s", (batch.id,)
                    )
                    batch = None
                if batch is None:


                    await cursor.execute(
                        "SELECT opened_at_ms,flush_at_ms FROM "
                        "card_cooldown_batches WHERE tracker=%s "
                        "AND guild_id=%s AND channel_id=%s AND timer_kind=%s AND state='open' "
                        "AND opened_at_ms<=%s AND flush_at_ms>%s AND flush_at_ms>%s "
                        "ORDER BY opened_at_ms LIMIT 1 FOR UPDATE",
                        (*group, source.due_at_ms, source.due_at_ms, now),
                    )
                    window = await cursor.fetchone()
                    opened, flush = window or (source.due_at_ms, source.due_at_ms + WINDOW_MS)
                    batch = Batch(
                        secrets.token_hex(16),
                        tracker,
                        guild,
                        source.channel_id,
                        kind,
                        1,
                        opened,
                        flush,
                        source.expires_at_ms,
                    )
                    await cursor.execute(
                        "INSERT INTO card_cooldown_batches (" + BATCH_FIELDS + ",open_slot) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            batch.id,
                            *group,
                            1,
                            opened,
                            flush,
                            source.expires_at_ms,
                            "open",
                            "{}",
                            1 if now < flush else None,
                        ),
                    )
                await cursor.execute(
                    "INSERT INTO card_cooldown_batch_items (tracker,guild_id,user_id,timer_kind,"
                    "timer_revision,batch_id,due_at_ms,expires_at_ms,delivery_mode) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (*source.key, batch.id, source.due_at_ms, source.expires_at_ms, mode),
                )
                await cursor.execute(
                    "UPDATE card_cooldown_batches SET item_count=item_count+1,"
                    "open_slot=IF(item_count>=%s,NULL,open_slot),"
                    "expires_at_ms=GREATEST(expires_at_ms,%s) "
                    "WHERE id=%s",
                    (MAX_RECIPIENTS, source.expires_at_ms, batch.id),
                )
                await connection.commit()
                return batch.id
            except BaseException:
                await connection.rollback()
                raise

    async def get(self, identifier):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT " + BATCH_FIELDS + " FROM card_cooldown_batches WHERE id=%s", (identifier,)
            )
            batch = batch_row(await cursor.fetchone())
            await connection.rollback()
            return batch

    async def eligible(self, batch):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            recipients = await self._eligible(cursor, batch, self.clock())
            await connection.rollback()
            return recipients

    async def _eligible(self, cursor, batch, now, *, lock=False):
        timers, subscriptions, enabled = source_tables(batch.tracker, batch.kind)
        await cursor.execute(
            "SELECT i.user_id,i.expires_at_ms,i.delivery_mode,i.timer_revision "
            "FROM card_cooldown_batch_items i "
            f"JOIN {timers} t ON t.guild_id=i.guild_id AND t.user_id=i.user_id "
            f"AND t.timer_kind=i.timer_kind "
            f"JOIN {subscriptions} s ON s.guild_id=i.guild_id AND s.user_id=i.user_id "
            "WHERE i.batch_id=%s AND i.state='pending' AND t.active=TRUE AND "
            "t.revision=i.timer_revision "
            f"AND s.{enabled}=TRUE AND s.channel_id=%s AND s.delivery_mode=i.delivery_mode "
            "AND i.due_at_ms<=%s AND i.expires_at_ms>%s "
            "AND t.due_at=TIMESTAMPADD(MICROSECOND,i.due_at_ms*1000,'1970-01-01 00:00:00') "
            "ORDER BY i.due_at_ms,i.user_id" + (" FOR UPDATE" if lock else ""),
            (batch.id, batch.channel_id, now, now),
        )
        return tuple(
            Recipient(int(row[0]), int(row[1]), str(row[2]), int(row[3]))
            for row in await cursor.fetchall()
        )

    async def seal(self, identifier):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE card_cooldown_batches SET state='ready',open_slot=NULL WHERE id=%s "
                "AND state='open' AND flush_at_ms<=%s",
                (identifier, self.clock()),
            )
            await connection.commit()
        return await self.get(identifier)

    async def begin_send(self, batch, permitted):

        identities = {(item.user_id, item.revision, item.mode) for item in permitted}
        if len(identities) > MAX_RECIPIENTS:
            raise ValueError("too many cooldown recipients")
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute(
                    "SELECT " + BATCH_FIELDS + " FROM card_cooldown_batches WHERE id=%s FOR UPDATE",
                    (batch.id,),
                )
                current = batch_row(await cursor.fetchone())
                now = self.clock()
                if (
                    current != batch or current.state != "ready"
                    or not current.flush_at_ms <= now < current.expires_at_ms
                ):
                    await connection.rollback()
                    return None
                candidates = await self._eligible(cursor, current, now, lock=True) if identities else ()
                included = tuple({
                    item.user_id: item for item in candidates
                    if (item.user_id, item.revision, item.mode) in identities
                    and item.expires_at_ms > self.clock()
                }.values())
                users = [item.user_id for item in included]
                await cursor.execute(
                    "UPDATE card_cooldown_batches SET state=%s,attempt_at_ms=%s,outcome=%s WHERE id=%s",
                    ("sending" if users else "skipped", self.clock(), json.dumps({"recipient_ids": users}), batch.id),
                )
                await connection.commit()
                return included
            except BaseException:
                await connection.rollback()
                raise

    async def sending(self, limit=100):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT " + BATCH_FIELDS + ",attempt_at_ms FROM card_cooldown_batches "
                "WHERE state='sending' ORDER BY attempt_at_ms,id LIMIT %s", (min(100, limit),),
            )
            rows = [(batch_row(row[:-1]), int(row[-1] or 0)) for row in await cursor.fetchall()]
            await connection.rollback()
            return rows

    async def record_result(self, batch, state, data):
        if state not in TERMINAL:
            raise ValueError("invalid cooldown outcome")
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE card_cooldown_batches SET state=%s,outcome=%s,open_slot=NULL "
                "WHERE id=%s AND revision=%s AND state='sending'",
                (state, json.dumps(data), batch.id, batch.revision),
            )
            await connection.commit()

    async def schedules(self, limit=100):
        now = self.clock()
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute(
                    "SELECT " + BATCH_FIELDS + ",dispatch_revision FROM card_cooldown_batches "
                    "WHERE state IN ('open','ready') AND next_dispatch_ms<=%s AND expires_at_ms>%s "
                    "ORDER BY flush_at_ms LIMIT %s FOR UPDATE",
                    (now, now, min(100, limit)),
                )
                result = []
                for row in await cursor.fetchall():
                    batch, dispatch = batch_row(row[:-1]), int(row[-1]) + 1
                    await cursor.execute(
                        "UPDATE card_cooldown_batches SET "
                        "next_dispatch_ms=%s,dispatch_revision=%s WHERE id=%s",
                        (now + 10_000, dispatch, batch.id),
                    )
                    result.append((batch, dispatch))
                await connection.commit()
                return result
            except BaseException:
                await connection.rollback()
                raise

    async def settle(self):
        now = self.clock()
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute(
                    "UPDATE card_cooldown_batches SET state='skipped',open_slot=NULL,outcome='{}' "
                    "WHERE state IN ('open','ready') AND expires_at_ms<=%s",
                    (now,),
                )
                await cursor.execute(
                    "SELECT " + BATCH_FIELDS + " FROM card_cooldown_batches WHERE settled=FALSE "
                    "AND state IN ('sent','skipped','failed','uncertain') LIMIT 100 FOR UPDATE"
                )
                for row in await cursor.fetchall():
                    batch = batch_row(row)
                    recipients = set((batch.outcome or {}).get("recipient_ids", ()))
                    await cursor.execute(
                        "SELECT user_id,timer_revision FROM "
                        "card_cooldown_batch_items WHERE batch_id=%s",
                        (batch.id,),
                    )
                    for user, revision in await cursor.fetchall():
                        status = batch.state if int(user) in recipients else "skipped"
                        await cursor.execute(
                            "UPDATE card_cooldown_batch_items SET state=%s WHERE " + ITEM_KEY,
                            (status, batch.tracker, batch.guild_id, user, batch.kind, revision),
                        )
                        timers, _subscriptions, _enabled = source_tables(batch.tracker, batch.kind)
                        await cursor.execute(
                            f"UPDATE {timers} SET active=FALSE WHERE guild_id=%s AND user_id=%s "
                            "AND timer_kind=%s AND revision=%s AND active=TRUE",
                            (batch.guild_id, user, batch.kind, revision),
                        )
                    await cursor.execute(
                        "UPDATE card_cooldown_batches SET settled=TRUE WHERE id=%s", (batch.id,)
                    )
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise

    async def candidates(self, tracker, kind, mode, after=0, limit=100):
        timers, subscriptions, enabled = source_tables(tracker, kind)
        now = self.clock()
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                f"SELECT t.guild_id,t.user_id,t.revision FROM {timers} t JOIN {subscriptions} s "
                f"USING(guild_id,user_id) WHERE t.timer_kind=%s AND t.active=TRUE "
                f"AND s.{enabled}=TRUE "
                "AND s.delivery_mode=%s AND s.channel_id IS NOT NULL AND "
                "t.due_at<=%s AND t.due_at>%s "
                "AND t.user_id>%s AND NOT EXISTS (SELECT 1 FROM card_cooldown_batch_items i "
                "WHERE i.tracker=%s AND i.guild_id=t.guild_id AND i.user_id=t.user_id "
                "AND i.timer_kind=t.timer_kind AND i.timer_revision=t.revision) "
                "ORDER BY t.user_id LIMIT %s",
                (kind, mode, date(now), date(now - DELIVERY_MS), after, tracker, min(100, limit)),
            )
            result = tuple(await cursor.fetchall())
            await connection.rollback()
            return result

    async def cleanup(self):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await connection.begin()
            try:
                await cursor.execute(
                    "SELECT id FROM card_cooldown_batches WHERE settled=TRUE "
                    "AND created_at<UTC_TIMESTAMP()-INTERVAL 7 DAY LIMIT 100 FOR UPDATE"
                )
                for (identifier,) in await cursor.fetchall():
                    await cursor.execute(
                        "DELETE FROM card_cooldown_batch_items WHERE batch_id=%s", (identifier,)
                    )
                    await cursor.execute(
                        "DELETE FROM card_cooldown_batches WHERE id=%s", (identifier,)
                    )
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise
