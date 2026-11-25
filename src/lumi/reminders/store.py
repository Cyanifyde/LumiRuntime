from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import secrets
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool

from .reactions import SCHEMA as REACTION_SCHEMA
from .reactions import ReactionReminders

WINDOW = 300_000
CHECK_LEASE = 60_000
SEND_LEASE = 60_000
RETENTION = 30 * 86_400_000
TERMINAL = frozenset({"sent", "failed", "ambiguous", "cancelled"})
BACKOFF = (5_000, 30_000, 120_000, 300_000)

SCHEMA = (
    *REACTION_SCHEMA,
    """CREATE TABLE IF NOT EXISTS reminder_guild_locks (
    guild_id BIGINT UNSIGNED PRIMARY KEY) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS reminder_records (
    id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
    channel_id BIGINT UNSIGNED NOT NULL,status VARCHAR(16) NOT NULL,
    next_ms BIGINT UNSIGNED NOT NULL,closed_ms BIGINT UNSIGNED NULL,document JSON NOT NULL,
    PRIMARY KEY(guild_id,id),INDEX by_due(status,next_ms),
    INDEX by_owner(guild_id,user_id,channel_id,status),
    INDEX by_closed(closed_ms)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS reminder_occurrences (
    id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    reminder_id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    guild_id BIGINT UNSIGNED NOT NULL,state VARCHAR(16) NOT NULL,
    recover_ms BIGINT UNSIGNED NOT NULL,finished_ms BIGINT UNSIGNED NULL,
    document JSON NOT NULL,INDEX by_recovery(state,recover_ms),
    INDEX by_reminder(reminder_id),INDEX by_finished(finished_ms)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS reminder_commands (
    guild_id BIGINT UNSIGNED NOT NULL,message_id BIGINT UNSIGNED NOT NULL,
    created_ms BIGINT UNSIGNED NOT NULL,document JSON NOT NULL,
    PRIMARY KEY(guild_id,message_id),INDEX by_created(created_ms)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS reminder_schedule_outbox (
    id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,created_ms BIGINT UNSIGNED NOT NULL,
    operation VARCHAR(8) NOT NULL,published BOOLEAN NOT NULL DEFAULT 0,document JSON NOT NULL,
    INDEX pending(published,created_ms)) ENGINE=InnoDB""",
)


def future_due(record: dict, now: int) -> int:
    interval = record["interval_ms"]
    return record["anchor_ms"] + ((now - record["anchor_ms"]) // interval + 1) * interval


async def _outbox(tx, occurrence, now, operation="upsert"):
    entry = dict(
        id=f"{occurrence['id']}:{occurrence['dispatch_revision']}:{operation}",
        guild_id=occurrence["guild_id"],
        created_ms=now,
        published=False,
        operation=operation,
        occurrence_id=occurrence["id"],
        revision=occurrence["dispatch_revision"],
        user_id=occurrence["user_id"],
        channel_id=occurrence["channel_id"],
        due_ms=now,
        expires_ms=min(now + WINDOW, occurrence.get("delivery_deadline_ms", now + WINDOW)),
    )
    if occurrence.get("source_author_id"):
        entry["source_author_id"] = occurrence["source_author_id"]
    await tx.put_outbox(entry)


async def _finish(tx, record, occurrence, now, outcome, error=None, message_id=None):
    if occurrence["state"] in TERMINAL:
        return False
    occurrence.update(
        state=outcome, finished_ms=now, error=error, message_id=message_id, recover_ms=0
    )
    await tx.put_occurrence(occurrence)
    if (
        record["revision"] != occurrence["reminder_revision"]
        or record["current"] != occurrence["id"]
    ):
        return True
    record.update(current=None, error=error, updated_ms=now)
    if record["status"] == "active":
        if outcome == "failed":
            record["status"] = "paused" if record["interval_ms"] else "failed"
            record["closed_ms"] = None if record["interval_ms"] else now
        elif record["interval_ms"]:
            record["next_ms"] = max(
                future_due(record, now), occurrence["scheduled_ms"] + record["interval_ms"]
            )
            if record.get("reaction"):
                record["delivery_deadline_ms"] = record["next_ms"] + WINDOW
        else:
            record.update(status="completed" if outcome == "sent" else "failed", closed_ms=now)
    record["wake_ms"] = record["next_ms"]
    await tx.put_record(record)
    return True


async def _cancel_pending(tx, record, now):
    if not record["current"]:
        return
    occurrence = await tx.occurrence(record["current"])
    if occurrence and occurrence["state"] not in (*TERMINAL, "sending"):
        occurrence.update(
            state="cancelled",
            finished_ms=now,
            dispatch_revision=occurrence["dispatch_revision"] + 1,
            recover_ms=0,
        )
        await tx.put_occurrence(occurrence)
        await _outbox(tx, occurrence, now, "cancel")


class ReminderStore(ReactionReminders):


    async def command_receipt(self, guild, message):
        async with self.transaction(guild) as tx:
            return await tx.receipt(message)

    async def command(self, command, *, guild, user, channel, message, now):
        async with self.transaction(guild) as tx:
            receipt = await tx.receipt(message)
            if receipt:
                return receipt
            record = await tx.record(command.identifier) if command.identifier else None
            if command.kind != "create" and (
                not record
                or record["guild_id"] != guild
                or record["user_id"] != user
                or record["status"] in {"completed", "cancelled"}
            ):
                receipt = {"error": "Reminder not found."}
            elif (
                command.kind == "create"
                or (command.kind == "edit" and record["status"] == "failed")
            ) and (await tx.count(user) >= 20 or await tx.count() >= 1000):
                receipt = {"error": "Reminder limit reached."}
            elif command.kind == "cancel":
                await _cancel_pending(tx, record, now)
                record.update(
                    status="cancelled",
                    current=None,
                    closed_ms=now,
                    updated_ms=now,
                    revision=record["revision"] + 1,
                )
                await tx.put_record(record)
                receipt = {"kind": "cancel", "id": record["id"]}
            else:
                if record is None:
                    for _ in range(8):
                        identifier = secrets.token_hex(4)
                        if not await tx.record(identifier):
                            break
                    else:
                        raise RuntimeError("reminder identifier unavailable")
                    record = dict(
                        id=identifier,
                        guild_id=guild,
                        user_id=user,
                        channel_id=channel,
                        revision=0,
                        current=None,
                        created_ms=now,
                    )
                due = command.due_ms or now + command.duration
                if due <= now:
                    return {"error": "That time has passed. Choose a later time."}
                await _cancel_pending(tx, record, now)
                record.update(
                    revision=record["revision"] + 1,
                    text=command.text,
                    interval_ms=command.duration if command.repeating else None,
                    anchor_ms=due - command.duration,
                    next_ms=due,
                    status="active",
                    wake_ms=due,
                    closed_ms=None,
                    current=None,
                    error=None,
                    updated_ms=now,
                )
                if record.get("reaction"):
                    record["delivery_deadline_ms"] = due + WINDOW
                await tx.put_record(record)
                receipt = dict(
                    kind=command.kind,
                    id=record["id"],
                    due_ms=record["next_ms"],
                    interval_ms=record["interval_ms"],
                )
            await tx.put_receipt(message, now, receipt)
            return receipt

    async def reconcile(self, identifier, guild, now):
        async with self.transaction(guild) as tx:
            record = await tx.record(identifier)
            if not record or record["guild_id"] != guild or record["status"] != "active":
                return
            occurrence = await tx.occurrence(record["current"]) if record["current"] else None
            if occurrence is None:
                if record["next_ms"] > now:
                    return
                scheduled = record["next_ms"]
                if record["interval_ms"]:
                    scheduled = future_due(record, now) - record["interval_ms"]
                key = hashlib.sha256(
                    f"{guild}:{identifier}:{record['revision']}:{scheduled}".encode()
                ).hexdigest()[:32]
                occurrence = dict(
                    id=key,
                    reminder_id=identifier,
                    guild_id=guild,
                    **(
                        {"source_author_id": record["source_author_id"]}
                        if record.get("source_author_id")
                        else {}
                    ),
                    **(
                        {"delivery_deadline_ms": record["delivery_deadline_ms"]}
                        if record.get("reaction")
                        else {}
                    ),
                    user_id=record["user_id"],
                    channel_id=record["channel_id"],
                    reminder_revision=record["revision"],
                    scheduled_ms=scheduled,
                    state="ready",
                    dispatch_revision=0,
                    recover_ms=now,
                    finished_ms=None,
                    failures=0,
                    send_attempt=0,
                )
                record["current"] = key
                await tx.put_record(record)
            if (
                record.get("reaction")
                and record["delivery_deadline_ms"] <= now
                and occurrence["state"] != "sending"
            ):
                await _finish(tx, record, occurrence, now, "failed", "expired")
                return
            if occurrence["recover_ms"] > now or occurrence["state"] in TERMINAL:
                return
            if occurrence["state"] == "sending":
                await _finish(tx, record, occurrence, now, "ambiguous", "delivery_uncertain")
                return
            occurrence.update(
                state="queued",
                dispatch_revision=occurrence["dispatch_revision"] + 1,
                recover_ms=now + WINDOW,
            )
            record["wake_ms"] = occurrence["recover_ms"]
            await tx.put_record(record)
            await tx.put_occurrence(occurrence)
            await _outbox(tx, occurrence, now)

    async def recover_orphan(
        self,
        identifier,
        guild,
        now,
        *,
        outcome="ambiguous",
        error="delivery_uncertain",
        message_id=None,
    ):
        async with self.transaction(guild) as tx:
            occurrence = await tx.occurrence(identifier)
            if not occurrence or occurrence["state"] != "sending" or occurrence["recover_ms"] > now:
                return
            record = await tx.record(occurrence["reminder_id"])
            if record:
                await _finish(tx, record, occurrence, now, outcome, error, message_id)

    async def claim(self, identifier, revision, *, guild, user, channel, now):
        async with self.transaction(guild) as tx:
            occurrence = await tx.occurrence(identifier)
            if (
                not occurrence
                or occurrence["state"] != "queued"
                or occurrence["dispatch_revision"] != revision
            ):
                return None
            record = await tx.record(occurrence["reminder_id"])
            if not self._current(record, occurrence, guild, user, channel):
                return None
            if (
                occurrence["recover_ms"] <= now
                or record.get("delivery_deadline_ms", now + 1) <= now
            ):
                return None
            occurrence.update(state="checking", recover_ms=now + CHECK_LEASE)
            await tx.put_occurrence(occurrence)
            record["wake_ms"] = occurrence["recover_ms"]
            await tx.put_record(record)
            return copy.deepcopy(record)

    @staticmethod
    def _current(record, occurrence, guild, user, channel):
        return bool(
            record
            and record["status"] == "active"
            and record["guild_id"] == guild
            and record["user_id"] == user
            and record["channel_id"] == channel
            and occurrence["guild_id"] == guild
            and occurrence["user_id"] == user
            and occurrence["channel_id"] == channel
            and record["current"] == occurrence["id"]
            and record["revision"] == occurrence["reminder_revision"]
        )

    async def begin_send(self, identifier, revision, guild, now):
        async with self.transaction(guild) as tx:
            occurrence = await tx.occurrence(identifier)
            if (
                not occurrence
                or occurrence["state"] != "checking"
                or occurrence["dispatch_revision"] != revision
            ):
                return False
            record = await tx.record(occurrence["reminder_id"])
            if not self._current(
                record, occurrence, guild, occurrence["user_id"], occurrence["channel_id"]
            ):
                return False
            if (
                occurrence["recover_ms"] <= now
                or record.get("delivery_deadline_ms", now + 1) <= now
            ):
                return False
            occurrence.update(
                state="sending",
                recover_ms=now + SEND_LEASE,
                send_attempt=occurrence["send_attempt"] + 1,
            )
            await tx.put_occurrence(occurrence)
            record["wake_ms"] = occurrence["recover_ms"]
            await tx.put_record(record)
            return occurrence["send_attempt"]

    async def finish(
        self, identifier, revision, guild, now, *, outcome, error=None, message_id=None
    ):
        async with self.transaction(guild) as tx:
            occurrence = await tx.occurrence(identifier)
            if (
                not occurrence
                or occurrence["dispatch_revision"] != revision
                or occurrence["state"] in TERMINAL
            ):
                return False
            record = await tx.record(occurrence["reminder_id"])
            if not record:
                return False
            return await _finish(tx, record, occurrence, now, outcome, error, message_id)

    async def retry(self, identifier, revision, guild, now):
        async with self.transaction(guild) as tx:
            occurrence = await tx.occurrence(identifier)
            if (
                not occurrence
                or occurrence["guild_id"] != guild
                or occurrence["dispatch_revision"] != revision
                or occurrence["state"] in TERMINAL
            ):
                return
            record = await tx.record(occurrence["reminder_id"])
            if not self._current(
                record, occurrence, guild, occurrence["user_id"], occurrence["channel_id"]
            ):
                occurrence.update(state="cancelled", finished_ms=now, recover_ms=0)
                await tx.put_occurrence(occurrence)
                return
            failures = occurrence["failures"]
            occurrence.update(
                state="ready",
                recover_ms=now + BACKOFF[min(failures, len(BACKOFF) - 1)],
                failures=failures + 1,
            )
            await tx.put_occurrence(occurrence)
            if (
                record
                and record["current"] == identifier
                and record["revision"] == occurrence["reminder_revision"]
            ):
                record["wake_ms"] = occurrence["recover_ms"]
                await tx.put_record(record)


class MemoryTransaction:
    def __init__(self, store, guild):
        self.store, self.guild = store, guild

    async def record(self, identifier):
        row = self.store.records.get((self.guild, identifier))
        return copy.deepcopy(row) if row else None

    async def occurrence(self, identifier):
        row = self.store.occurrences.get(identifier)
        return copy.deepcopy(row) if row else None

    async def count(self, user=None):
        return sum(
            row["guild_id"] == self.guild
            and row["status"] in {"active", "paused"}
            and (user is None or row["user_id"] == user)
            for row in self.store.records.values()
        )

    async def receipt(self, message):
        receipt = copy.deepcopy(self.store.commands.get((self.guild, message)))
        if receipt:
            receipt.pop("receipt_ms", None)
        return receipt

    async def put_receipt(self, message, now, receipt):
        self.store.commands[self.guild, message] = {**receipt, "receipt_ms": now}

    async def put_record(self, record):
        self.store.records[record["guild_id"], record["id"]] = copy.deepcopy(record)

    async def put_occurrence(self, occurrence):
        self.store.occurrences[occurrence["id"]] = copy.deepcopy(occurrence)

    async def put_outbox(self, entry):
        self.store.outbox[entry["id"]] = copy.deepcopy(entry)

    async def state(self, key):
        return copy.deepcopy(self.store.reaction_state.get((self.guild, key)))

    async def pending_prompt_count(self, user, now):
        return sum(
            guild == self.guild
            and row.get("status") == "pending"
            and row.get("expires_at_ms", 0) > now
            and (user is None or row.get("owner_id") == user)
            for (guild, _), row in self.store.reaction_state.items()
        )

    async def put_state(self, key, user, author, kind, document):
        self.store.reaction_state[self.guild, key] = copy.deepcopy(document)


class MemoryReminderStore(ReminderStore):
    def __init__(self):
        self.records, self.occurrences, self.commands, self.outbox = {}, {}, {}, {}
        self.reaction_state = {}
        self.lock = asyncio.Lock()

    @asynccontextmanager
    async def transaction(self, guild):
        async with self.lock:
            snapshot = copy.deepcopy(
                (self.records, self.occurrences, self.commands, self.outbox, self.reaction_state)
            )
            try:
                yield MemoryTransaction(self, guild)
            except BaseException:
                self.records, self.occurrences, self.commands, self.outbox, self.reaction_state = (
                    snapshot
                )
                raise

    async def candidates(self, now, guilds, limit=100):
        rows = sorted(
            (
                r
                for r in self.records.values()
                if r["guild_id"] in guilds and r["status"] == "active" and r["wake_ms"] <= now
            ),
            key=lambda r: (r["wake_ms"], r["id"]),
        )
        return [(r["id"], r["guild_id"]) for r in rows[:limit]]

    async def orphans(self, now, limit=100):
        return copy.deepcopy(
            [
                r
                for r in self.occurrences.values()
                if r["state"] == "sending" and r["recover_ms"] <= now
            ][:limit]
        )

    async def pending(self, guilds, limit=100):
        return copy.deepcopy(
            [
                r
                for r in self.outbox.values()
                if not r["published"] and (r["guild_id"] in guilds or r["operation"] == "cancel")
            ][:limit]
        )

    async def published(self, identifier):
        self.outbox[identifier]["published"] = True

    async def current_outbox(self, entry):
        occurrence = self.occurrences.get(entry["occurrence_id"])
        return bool(
            occurrence
            and occurrence["dispatch_revision"] == entry["revision"]
            and occurrence["state"] == ("cancelled" if entry["operation"] == "cancel" else "queued")
        )

    async def listing(self, guild, user, channel):
        return copy.deepcopy(
            sorted(
                (
                    r
                    for r in self.records.values()
                    if (r["guild_id"], r["user_id"], r["channel_id"]) == (guild, user, channel)
                    and r["status"] not in {"completed", "cancelled"}
                ),
                key=lambda r: (r["next_ms"], r["id"]),
            )
        )

    async def list_page(self, guild, user, channel, page):
        rows = await self.listing(guild, user, channel)
        return rows[(page - 1) * 5 : page * 5], len(rows)

    async def cleanup(self, now):
        cutoff = now - RETENTION
        self.records = {
            k: r
            for k, r in self.records.items()
            if r.get("closed_ms") is None or r["closed_ms"] > cutoff
        }
        self.occurrences = {
            k: r
            for k, r in self.occurrences.items()
            if r.get("finished_ms") is None or r["finished_ms"] > cutoff
        }
        self.commands = {k: r for k, r in self.commands.items() if r["receipt_ms"] > cutoff}
        self.outbox = {k: r for k, r in self.outbox.items() if r["created_ms"] > cutoff}

    async def close(self):
        pass


class MySQLTransaction:
    def __init__(self, cursor, guild):
        self.cursor, self.guild = cursor, guild

    async def state(self, key):
        await self.cursor.execute(
            "SELECT document FROM reminder_reaction_state WHERE state_key=%s AND guild_id=%s",
            (key, self.guild),
        )
        row = await self.cursor.fetchone()
        return json.loads(row[0]) if row else None

    async def pending_prompt_count(self, user, now):
        sql = (
            "SELECT COUNT(*) FROM reminder_reaction_state WHERE guild_id=%s AND kind='prompt' "
            "AND JSON_UNQUOTE(JSON_EXTRACT(document,'$.status'))='pending' "
            "AND CAST(JSON_UNQUOTE(JSON_EXTRACT(document,'$.expires_at_ms')) AS UNSIGNED)>%s"
        )
        args = [self.guild, now]
        if user is not None:
            sql += " AND user_id=%s"
            args.append(user)
        await self.cursor.execute(sql, args)
        return (await self.cursor.fetchone())[0]

    async def put_state(self, key, user, author, kind, document):
        await self.cursor.execute(
            "INSERT INTO reminder_reaction_state VALUES(%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE document=VALUES(document)",
            (key, self.guild, user, author, kind, json.dumps(document, ensure_ascii=False)),
        )

    async def record(self, identifier):
        await self.cursor.execute(
            "SELECT document FROM reminder_records WHERE guild_id=%s AND id=%s",
            (self.guild, identifier),
        )
        row = await self.cursor.fetchone()
        return json.loads(row[0]) if row else None

    async def occurrence(self, identifier):
        await self.cursor.execute(
            "SELECT document FROM reminder_occurrences WHERE id=%s", (identifier,)
        )
        row = await self.cursor.fetchone()
        return json.loads(row[0]) if row else None

    async def count(self, user=None):
        sql = (
            "SELECT COUNT(*) FROM reminder_records "
            "WHERE guild_id=%s AND status IN ('active','paused')"
        )
        args = (self.guild,)
        if user is not None:
            sql += " AND user_id=%s"
            args += (user,)
        await self.cursor.execute(sql, args)
        return (await self.cursor.fetchone())[0]

    async def receipt(self, message):
        await self.cursor.execute(
            "SELECT document FROM reminder_commands WHERE guild_id=%s AND message_id=%s",
            (self.guild, message),
        )
        row = await self.cursor.fetchone()
        receipt = json.loads(row[0]) if row else None
        if receipt:
            receipt.pop("receipt_ms", None)
        return receipt

    async def put_receipt(self, message, now, receipt):
        await self.cursor.execute(
            "INSERT INTO reminder_commands VALUES (%s,%s,%s,%s)",
            (self.guild, message, now, json.dumps({**receipt, "receipt_ms": now})),
        )

    async def put_record(self, record):
        await self.cursor.execute(
            """INSERT INTO reminder_records VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE status=VALUES(status),next_ms=VALUES(next_ms),
            closed_ms=VALUES(closed_ms),document=VALUES(document)""",
            (
                record["id"],
                record["guild_id"],
                record["user_id"],
                record["channel_id"],
                record["status"],
                record["wake_ms"],
                record["closed_ms"],
                json.dumps(record),
            ),
        )

    async def put_occurrence(self, occurrence):
        await self.cursor.execute(
            """INSERT INTO reminder_occurrences VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE state=VALUES(state),recover_ms=VALUES(recover_ms),
            finished_ms=VALUES(finished_ms),document=VALUES(document)""",
            (
                occurrence["id"],
                occurrence["reminder_id"],
                occurrence["guild_id"],
                occurrence["state"],
                occurrence["recover_ms"],
                occurrence["finished_ms"],
                json.dumps(occurrence),
            ),
        )

    async def put_outbox(self, entry):
        await self.cursor.execute(
            "INSERT INTO reminder_schedule_outbox VALUES (%s,%s,%s,%s,0,%s)",
            (
                entry["id"],
                entry["guild_id"],
                entry["created_ms"],
                entry["operation"],
                json.dumps(entry),
            ),
        )


class MySQLReminderStore(ReminderStore):
    def __init__(self, dsn):
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("invalid reminder DSN")
        self.settings = dict(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            db=parsed.path.removeprefix("/"),
            autocommit=False,
            charset="utf8mb4",
            minsize=1,
            maxsize=4,
        )
        self.pool = None

    async def connect(self):
        self.pool = await create_pool(**self.settings)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def migrate(self):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            for statement in SCHEMA:
                await q.execute(statement)
            await conn.commit()

    @asynccontextmanager
    async def transaction(self, guild):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await conn.begin()
                await q.execute(
                    "INSERT INTO reminder_guild_locks VALUES (%s) "
                    "ON DUPLICATE KEY UPDATE guild_id=VALUES(guild_id)",
                    (guild,),
                )
                await q.execute(
                    "SELECT guild_id FROM reminder_guild_locks WHERE guild_id=%s FOR UPDATE",
                    (guild,),
                )
                await q.fetchone()
                yield MySQLTransaction(q, guild)
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def _read(self, sql, args=()):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(sql, args)
            rows = await q.fetchall()
            await conn.rollback()
            return rows

    async def candidates(self, now, guilds, limit=100):
        if not guilds:
            return []
        return await self._read(
            "SELECT id,guild_id FROM reminder_records WHERE guild_id IN ("
            + ",".join(["%s"] * len(guilds))
            + ") AND status='active' AND next_ms<=%s ORDER BY next_ms,id LIMIT %s",
            (*sorted(guilds), now, limit),
        )

    async def orphans(self, now, limit=100):
        return [
            json.loads(r[0])
            for r in await self._read(
                "SELECT document FROM reminder_occurrences WHERE state='sending' "
                "AND recover_ms<=%s ORDER BY recover_ms LIMIT %s",
                (now, limit),
            )
        ]

    async def pending(self, guilds, limit=100):
        condition = "guild_id IN (" + ",".join(["%s"] * len(guilds)) + ") OR " if guilds else ""
        return [
            json.loads(r[0])
            for r in await self._read(
                "SELECT document FROM reminder_schedule_outbox WHERE published=0 AND ("
                + condition
                + "operation='cancel') ORDER BY created_ms,id LIMIT %s",
                (*sorted(guilds), limit),
            )
        ]

    async def published(self, identifier):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "UPDATE reminder_schedule_outbox SET published=1 WHERE id=%s", (identifier,)
            )
            await conn.commit()

    async def current_outbox(self, entry):
        rows = await self._read(
            "SELECT document FROM reminder_occurrences WHERE id=%s", (entry["occurrence_id"],)
        )
        if not rows:
            return False
        occurrence = json.loads(rows[0][0])
        return occurrence["dispatch_revision"] == entry["revision"] and occurrence["state"] == (
            "cancelled" if entry["operation"] == "cancel" else "queued"
        )

    async def listing(self, guild, user, channel):
        return [
            json.loads(r[0])
            for r in await self._read(
                "SELECT document FROM reminder_records WHERE guild_id=%s AND user_id=%s "
                "AND channel_id=%s AND status NOT IN ('completed','cancelled') ORDER BY next_ms,id",
                (guild, user, channel),
            )
        ]

    async def list_page(self, guild, user, channel, page):
        predicate = (
            " FROM reminder_records WHERE guild_id=%s AND user_id=%s AND channel_id=%s "
            "AND status NOT IN ('completed','cancelled')"
        )
        args = (guild, user, channel)
        total = (await self._read("SELECT COUNT(*)" + predicate, args))[0][0]
        if page > (total + 4) // 5:
            return [], total
        rows = await self._read(
            "SELECT document" + predicate + " ORDER BY next_ms,id LIMIT 5 OFFSET %s",
            (*args, (page - 1) * 5),
        )
        return [json.loads(row[0]) for row in rows], total

    async def cleanup(self, now):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                for table, condition in (
                    ("reminder_records", "closed_ms<=%s"),
                    ("reminder_occurrences", "finished_ms<=%s"),
                    ("reminder_commands", "created_ms<=%s"),
                    ("reminder_schedule_outbox", "created_ms<=%s"),
                ):
                    await q.execute(
                        "DELETE FROM " + table + " WHERE " + condition + " LIMIT 5000",
                        (now - RETENTION,),
                    )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
