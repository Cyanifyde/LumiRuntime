from __future__ import annotations

import asyncio
import copy
import json
import secrets
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool

from .purge_store import SCHEMA as PURGE_SCHEMA
from .rename_store import SCHEMA as RENAME_SCHEMA

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS moderation_operations (
      id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      guild_id BIGINT UNSIGNED NOT NULL, created_ms BIGINT UNSIGNED NOT NULL,
      document JSON NOT NULL, INDEX moderation_operations_retention(created_ms)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS moderation_warnings (
      guild_id BIGINT UNSIGNED NOT NULL,
      id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      user_id BIGINT UNSIGNED NOT NULL, created_ms BIGINT UNSIGNED NOT NULL,
      document JSON NOT NULL, PRIMARY KEY(guild_id,id),
      INDEX moderation_warning_history(guild_id,user_id,created_ms)
    ) ENGINE=InnoDB""",
    *PURGE_SCHEMA,
    *RENAME_SCHEMA,
)


def completed(record, status, **data):
    record.update(status="done", result=dict(status=status, data=data))
    return record


def warning(record, identifier):
    request = record["request"]
    return dict(
        id=identifier,
        guild=record["guild"],
        user=request["target"],
        moderator=request["actor"],
        reason=request["reason"],
        created=record["created"],
        revoked=None,
    )


def paginate(rows, page, total):
    pages = max(1, (total + 4) // 5)
    return dict(warnings=rows, page=min(page, pages), pages=pages, total=total)


class MemoryStore:
    def __init__(self):
        self.operations, self.warnings = {}, {}
        self.lock = asyncio.Lock()

    async def begin(self, record):
        async with self.lock:
            new = record["id"] not in self.operations
            if new:
                self.operations[record["id"]] = copy.deepcopy(record)
            return copy.deepcopy(self.operations[record["id"]]), new

    async def save(self, record):
        async with self.lock:
            self.operations[record["id"]] = copy.deepcopy(record)

    async def operation(self, guild, key):
        async with self.lock:
            row = self.operations.get(key)
            return copy.deepcopy(row) if row and row["guild"] == guild else None

    async def pending_roles(self):
        async with self.lock:
            return sorted(
                [
                    copy.deepcopy(r)
                    for r in self.operations.values()
                    if r.get("type") == "roles" and r["status"] != "done"
                ],
                key=lambda r: (r["created"], r["id"]),
            )[:100]

    async def warn(self, record):
        async with self.lock:
            if record["id"] in self.operations:
                return copy.deepcopy(self.operations[record["id"]])
            while (record["guild"], identifier := secrets.token_hex(4)) in self.warnings:
                pass
            self.warnings[record["guild"], identifier] = warning(record, identifier)
            completed(record, "warned", warning_id=identifier)
            self.operations[record["id"]] = copy.deepcopy(record)
            return copy.deepcopy(record)

    async def revoke(self, record):
        async with self.lock:
            if record["id"] in self.operations:
                return copy.deepcopy(self.operations[record["id"]])
            row = self.warnings.get((record["guild"], record["request"]["warning_id"]))
            status = (
                "warning_missing"
                if row is None
                else "already_revoked"
                if row["revoked"]
                else "revoked"
            )
            if status == "revoked":
                row["revoked"] = dict(
                    actor=record["request"]["actor"],
                    reason=record["request"]["reason"],
                    at=record["created"],
                )
            completed(record, status, warning_id=record["request"]["warning_id"])
            self.operations[record["id"]] = copy.deepcopy(record)
            return copy.deepcopy(record)

    async def history(self, guild, user, page):
        async with self.lock:
            rows = sorted(
                (r for r in self.warnings.values() if (r["guild"], r["user"]) == (guild, user)),
                key=lambda r: (r["created"], r["id"]),
                reverse=True,
            )
            page = min(page, max(1, (len(rows) + 4) // 5))
            return paginate(copy.deepcopy(rows[(page - 1) * 5 : page * 5]), page, len(rows))

    async def cleanup(self, now):
        async with self.lock:
            self.operations = {
                k: v for k, v in self.operations.items() if v["created"] >= now - 30 * 86400_000
            }

    async def close(self):
        pass


class MySQLStore:
    def __init__(self, dsn):
        p = urlparse(dsn)
        if p.scheme not in {"mysql", "mariadb"}:
            raise ValueError("invalid moderation DSN")
        self.settings = dict(
            host=p.hostname or "localhost",
            port=p.port or 3306,
            user=unquote(p.username or ""),
            password=unquote(p.password or ""),
            db=p.path.lstrip("/"),
            charset="utf8mb4",
            autocommit=False,
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
        async with self.pool.acquire() as c, c.cursor() as q:
            for statement in SCHEMA:
                await q.execute(statement)
            await c.commit()

    async def _begin(self, q, record):


        await q.execute("SELECT GET_LOCK(%s,5)", ("moderation:" + record["id"][:48],))
        if (await q.fetchone())[0] != 1:
            raise TimeoutError("moderation operation busy")
        await q.execute(
            "SELECT document FROM moderation_operations WHERE id=%s FOR UPDATE", (record["id"],)
        )
        row = await q.fetchone()
        if row:
            return json.loads(row[0]), False
        await q.execute(
            "INSERT INTO moderation_operations VALUES (%s,%s,%s,%s)",
            (record["id"], record["guild"], record["created"], json.dumps(record)),
        )
        return copy.deepcopy(record), True

    async def _release(self, c, q, record):
        await c.rollback()
        await q.execute("SELECT RELEASE_LOCK(%s)", ("moderation:" + record["id"][:48],))
        await q.fetchone()

    async def _save(self, q, record):
        await q.execute(
            "UPDATE moderation_operations SET document=%s WHERE id=%s AND guild_id=%s",
            (json.dumps(record), record["id"], record["guild"]),
        )

    async def begin(self, record):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                row, new = await self._begin(q, record)
                await c.commit()
                return row, new
            finally:
                await self._release(c, q, record)

    async def save(self, record):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await self._save(q, record)
                await c.commit()
            finally:
                await c.rollback()

    async def operation(self, guild, key):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT document FROM moderation_operations WHERE id=%s AND guild_id=%s",
                    (key, guild),
                )
                row = await q.fetchone()
                return json.loads(row[0]) if row else None
            finally:
                await c.rollback()

    async def pending_roles(self):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT document FROM moderation_operations "
                    "WHERE JSON_UNQUOTE(JSON_EXTRACT(document,'$.type'))='roles' "
                    "AND JSON_UNQUOTE(JSON_EXTRACT(document,'$.status'))<>'done' "
                    "ORDER BY created_ms,id LIMIT 100"
                )
                return [json.loads(r[0]) for r in await q.fetchall()]
            finally:
                await c.rollback()

    async def warn(self, record):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                row, new = await self._begin(q, record)
                if not new:
                    return row
                for _ in range(10):
                    identifier = secrets.token_hex(4)
                    value = warning(record, identifier)
                    await q.execute(
                        "INSERT IGNORE INTO moderation_warnings VALUES (%s,%s,%s,%s,%s)",
                        (
                            record["guild"],
                            identifier,
                            value["user"],
                            value["created"],
                            json.dumps(value),
                        ),
                    )
                    if q.rowcount == 1:
                        break
                else:
                    raise RuntimeError("warning ID unavailable")
                completed(record, "warned", warning_id=identifier)
                await self._save(q, record)
                await c.commit()
                return record
            finally:
                await self._release(c, q, record)

    async def revoke(self, record):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                row, new = await self._begin(q, record)
                if not new:
                    return row
                identifier = record["request"]["warning_id"]
                await q.execute(
                    "SELECT document FROM moderation_warnings "
                    "WHERE guild_id=%s AND id=%s FOR UPDATE",
                    (record["guild"], identifier),
                )
                raw = await q.fetchone()
                value = json.loads(raw[0]) if raw else None
                status = (
                    "warning_missing"
                    if value is None
                    else "already_revoked"
                    if value["revoked"]
                    else "revoked"
                )
                if status == "revoked":
                    value["revoked"] = dict(
                        actor=record["request"]["actor"],
                        reason=record["request"]["reason"],
                        at=record["created"],
                    )
                    await q.execute(
                        "UPDATE moderation_warnings SET document=%s WHERE guild_id=%s AND id=%s",
                        (json.dumps(value), record["guild"], identifier),
                    )
                completed(record, status, warning_id=identifier)
                await self._save(q, record)
                await c.commit()
                return record
            finally:
                await self._release(c, q, record)

    async def history(self, guild, user, page):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT COUNT(*) FROM moderation_warnings WHERE guild_id=%s AND user_id=%s",
                    (guild, user),
                )
                total = (await q.fetchone())[0]
                page = min(page, max(1, (total + 4) // 5))
                await q.execute(
                    "SELECT document FROM moderation_warnings WHERE guild_id=%s AND user_id=%s "
                    "ORDER BY created_ms DESC,id DESC LIMIT 5 OFFSET %s",
                    (guild, user, (page - 1) * 5),
                )
                return paginate([json.loads(r[0]) for r in await q.fetchall()], page, total)
            finally:
                await c.rollback()

    async def cleanup(self, now):
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute(
                "DELETE FROM moderation_operations WHERE created_ms<%s LIMIT 1000",
                (now - 30 * 86400_000,),
            )
            await c.commit()
