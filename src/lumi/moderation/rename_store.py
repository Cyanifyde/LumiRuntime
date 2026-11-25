from __future__ import annotations

import asyncio
import copy
import json
import secrets
from collections import Counter
from contextlib import asynccontextmanager

from .rename import RUNNABLE, TERMINAL, summarize

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS moderation_rename_batches (
      id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      guild_id BIGINT UNSIGNED NOT NULL, source_key CHAR(64) CHARACTER SET ascii NOT NULL UNIQUE,
      source_id BIGINT UNSIGNED NOT NULL, state VARCHAR(20) NOT NULL,
      next_ms BIGINT UNSIGNED NOT NULL, updated_ms BIGINT UNSIGNED NOT NULL,
      revision BIGINT UNSIGNED NOT NULL, document JSON NOT NULL,
      KEY rename_due(state,next_ms), KEY rename_guild(guild_id,source_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""",
    """CREATE TABLE IF NOT EXISTS moderation_rename_members (
      batch_id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      member_id BIGINT UNSIGNED NOT NULL, state VARCHAR(20) NOT NULL,
      revision BIGINT UNSIGNED NOT NULL, document JSON NOT NULL,
      PRIMARY KEY(batch_id,member_id), KEY rename_work(batch_id,state,member_id),
      FOREIGN KEY(batch_id) REFERENCES moderation_rename_batches(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""",
    """CREATE TABLE IF NOT EXISTS moderation_rename_receipts (
      source_key CHAR(64) CHARACTER SET ascii PRIMARY KEY,
      batch_id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      FOREIGN KEY(batch_id) REFERENCES moderation_rename_batches(id) ON DELETE CASCADE
    ) ENGINE=InnoDB""",
)


def new_batch(record):
    return dict(
        **copy.deepcopy(record),
        id=secrets.token_hex(4),
        revision=1,
        state="listing",
        mode="apply",
        approved=False,
        cursor=0,
        listed=False,
        token=secrets.token_hex(8),
        expires=0,
        message=0,
        error="",
        next=record["created"],
        updated=record["created"],
        retry=False,
    )


def member_record(batch, member, joined):
    return dict(batch=batch, member=member, joined=joined, state="pending", revision=1)


def work_states(mode):

    return (
        ("apply_sending", "undo_sending", "restore_failed", "applied")
        if mode == "undo"
        else (
            "apply_sending",
            "pending",
        )
    )


class MemoryRenameStore:
    def __init__(self):
        self.jobs, self.members, self.receipts = {}, {}, {}
        self.lock, self.execution_lock = asyncio.Lock(), asyncio.Lock()

    @asynccontextmanager
    async def exclusive(self):
        async with self.execution_lock:
            yield True

    async def create(self, record):
        async with self.lock:
            rows = list(self.jobs.values())
            for row in rows:
                if row["key"] == record["key"]:
                    return copy.deepcopy(row), "existing"
            if any(r["guild"] == record["guild"] and r["state"] not in TERMINAL for r in rows):
                return None, "busy"
            if sum(r["state"] not in TERMINAL for r in rows) >= 32:
                return None, "full"
            row = new_batch(record)
            self.jobs[row["id"]] = row
            return copy.deepcopy(row), "created"

    async def get(self, guild, code=None):
        async with self.lock:
            rows = [
                r
                for r in self.jobs.values()
                if r["guild"] == guild and (not code or r["id"] == code)
            ]
            return (
                copy.deepcopy(max(rows, key=lambda r: (r["state"] not in TERMINAL, r["source"])))
                if rows
                else None
            )

    async def ready(self, now):
        async with self.lock:
            return copy.deepcopy(
                sorted(
                    (r for r in self.jobs.values() if r["state"] in RUNNABLE and r["next"] <= now),
                    key=lambda r: (r["next"], r["id"]),
                )[:32]
            )

    async def next_due(self):
        async with self.lock:
            return min(
                (r["next"] for r in self.jobs.values() if r["state"] in RUNNABLE), default=None
            )

    async def update(self, row, changes, now, receipt=None, members=()):
        async with self.lock:
            old = self.jobs.get(row["id"])
            if receipt in self.receipts:
                return copy.deepcopy(old) if self.receipts[receipt] == row["id"] else None
            if not old or old["revision"] != row["revision"]:
                return None
            for member in members:
                self.members.setdefault((row["id"], member["member"]), copy.deepcopy(member))
            old.update(copy.deepcopy(changes), revision=old["revision"] + 1, updated=now)
            if receipt:
                self.receipts[receipt] = row["id"]
            return copy.deepcopy(old)

    async def next_member(self, row):
        async with self.lock:
            states = work_states(row["mode"])
            rows = [
                m
                for (batch, _), m in self.members.items()
                if batch == row["id"] and m["state"] in states
            ]
            return (
                copy.deepcopy(min(rows, key=lambda m: (states.index(m["state"]), m["member"])))
                if rows
                else None
            )

    async def save_member(self, member, changes):
        async with self.lock:
            old = self.members.get((member["batch"], member["member"]))
            if not old or old["revision"] != member["revision"]:
                return None
            if "original" in old and changes.get("original", old["original"]) != old["original"]:
                raise ValueError("Original nickname is immutable")
            old.update(copy.deepcopy(changes), revision=old["revision"] + 1)
            return copy.deepcopy(old)

    async def counts(self, row):
        async with self.lock:
            return summarize(
                Counter(m["state"] for (batch, _), m in self.members.items() if batch == row["id"])
            )

    async def cleanup(self, now):
        async with self.lock:
            expired = {
                key
                for key, r in self.jobs.items()
                if r["state"] in TERMINAL and r["updated"] < now - 30 * 86400_000
            }
            for key in expired:
                self.jobs.pop(key)
            self.members = {k: v for k, v in self.members.items() if k[0] not in expired}
            self.receipts = {k: v for k, v in self.receipts.items() if v not in expired}


class MySQLRenameStore:
    def __init__(self, owner):
        self.owner = owner

    @property
    def pool(self):
        return self.owner.pool

    @asynccontextmanager
    async def exclusive(self):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            acquired = False
            try:
                await cursor.execute("SELECT GET_LOCK('lumi:rename:executor',0)")
                acquired = (await cursor.fetchone())[0] == 1
                yield acquired
            finally:
                await conn.rollback()
                if acquired:
                    await cursor.execute("SELECT RELEASE_LOCK('lumi:rename:executor')")
                    await cursor.fetchone()

    async def create(self, record):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            acquired = False
            try:
                await cursor.execute("SELECT GET_LOCK('lumi:rename:admission',5)")
                acquired = (await cursor.fetchone())[0] == 1
                if not acquired:
                    raise TimeoutError("Nickname admission unavailable")
                await cursor.execute(
                    "SELECT document FROM moderation_rename_batches WHERE source_key=%s",
                    (record["key"],),
                )
                found = await cursor.fetchone()
                if found:
                    return json.loads(found[0]), "existing"
                await cursor.execute(
                    "SELECT guild_id FROM moderation_rename_batches "
                    "WHERE state NOT IN ('done','expired','failed') FOR UPDATE"
                )
                rows = await cursor.fetchall()
                if any(r[0] == record["guild"] for r in rows):
                    return None, "busy"
                if len(rows) >= 32:
                    return None, "full"
                row = new_batch(record)
                await cursor.execute(
                    "INSERT INTO moderation_rename_batches VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        row["id"],
                        row["guild"],
                        row["key"],
                        row["source"],
                        row["state"],
                        row["next"],
                        row["updated"],
                        row["revision"],
                        json.dumps(row),
                    ),
                )
                await conn.commit()
                return row, "created"
            finally:
                await conn.rollback()
                if acquired:
                    await cursor.execute("SELECT RELEASE_LOCK('lumi:rename:admission')")
                    await cursor.fetchone()

    async def get(self, guild, code=None):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM moderation_rename_batches WHERE guild_id=%s "
                    + ("AND id=%s " if code else "")
                    + "ORDER BY (state NOT IN ('done','expired','failed')) DESC,"
                    "source_id DESC LIMIT 1",
                    (guild, code) if code else (guild,),
                )
                row = await cursor.fetchone()
                return json.loads(row[0]) if row else None
            finally:
                await conn.rollback()

    async def ready(self, now):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM moderation_rename_batches WHERE state IN "
                    "('listing','queued','running','undoing','confirm','undo_confirm') "
                    "AND next_ms<=%s ORDER BY next_ms,id LIMIT 32",
                    (now,),
                )
                return [json.loads(r[0]) for r in await cursor.fetchall()]
            finally:
                await conn.rollback()

    async def next_due(self):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT MIN(next_ms) FROM moderation_rename_batches WHERE state IN "
                    "('listing','queued','running','undoing','confirm','undo_confirm')"
                )
                return (await cursor.fetchone())[0]
            finally:
                await conn.rollback()

    async def update(self, row, changes, now, receipt=None, members=()):
        if len(members) > 1000:
            raise ValueError("Unbounded nickname member page")
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM moderation_rename_batches "
                    "WHERE id=%s AND guild_id=%s FOR UPDATE",
                    (row["id"], row["guild"]),
                )
                raw = await cursor.fetchone()
                if not raw:
                    return None
                old = json.loads(raw[0])
                if receipt:
                    await cursor.execute(
                        "SELECT batch_id FROM moderation_rename_receipts WHERE source_key=%s",
                        (receipt,),
                    )
                    previous = await cursor.fetchone()
                    if previous:
                        return old if previous[0] == row["id"] else None
                if old["revision"] != row["revision"]:
                    return None
                for member in members:
                    await cursor.execute(
                        "INSERT IGNORE INTO moderation_rename_members VALUES (%s,%s,%s,%s,%s)",
                        (
                            row["id"],
                            member["member"],
                            member["state"],
                            member["revision"],
                            json.dumps(member),
                        ),
                    )
                old.update(changes, revision=old["revision"] + 1, updated=now)
                await cursor.execute(
                    "UPDATE moderation_rename_batches SET state=%s,next_ms=%s,"
                    "updated_ms=%s,revision=%s,document=%s WHERE id=%s",
                    (old["state"], old["next"], now, old["revision"], json.dumps(old), row["id"]),
                )
                if receipt:
                    await cursor.execute(
                        "INSERT INTO moderation_rename_receipts VALUES (%s,%s)",
                        (receipt, row["id"]),
                    )
                await conn.commit()
                return old
            finally:
                await conn.rollback()

    async def next_member(self, row):
        states = work_states(row["mode"])
        placeholders = ",".join("%s" for _ in states)
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM moderation_rename_members "
                    "WHERE batch_id=%s AND state IN ("
                    + placeholders
                    + ") ORDER BY FIELD(state,"
                    + placeholders
                    + "),member_id LIMIT 1",
                    (row["id"], *states, *states),
                )
                found = await cursor.fetchone()
                return json.loads(found[0]) if found else None
            finally:
                await conn.rollback()

    async def save_member(self, member, changes):
        if (
            "original" in member
            and changes.get("original", member["original"]) != member["original"]
        ):
            raise ValueError("Original nickname is immutable")
        updated = dict(member, **changes, revision=member["revision"] + 1)
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "UPDATE moderation_rename_members SET state=%s,revision=%s,document=%s "
                    "WHERE batch_id=%s AND member_id=%s AND revision=%s",
                    (
                        updated["state"],
                        updated["revision"],
                        json.dumps(updated),
                        member["batch"],
                        member["member"],
                        member["revision"],
                    ),
                )
                if cursor.rowcount != 1:
                    return None
                await conn.commit()
                return updated
            finally:
                await conn.rollback()

    async def counts(self, row):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT state,COUNT(*) FROM moderation_rename_members "
                    "WHERE batch_id=%s GROUP BY state",
                    (row["id"],),
                )
                return summarize(dict(await cursor.fetchall()))
            finally:
                await conn.rollback()

    async def cleanup(self, now):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "DELETE FROM moderation_rename_batches "
                    "WHERE state IN ('done','expired','failed') AND updated_ms<%s",
                    (now - 30 * 86400_000,),
                )
                await conn.commit()
            finally:
                await conn.rollback()
