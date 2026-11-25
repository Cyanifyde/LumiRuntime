from __future__ import annotations

import asyncio
import copy
import json
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool

from .models import RETENTION, fresh, validate

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS imageguard_config (
      guild_id BIGINT UNSIGNED PRIMARY KEY, document JSON NOT NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS imageguard_incidents (
      id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      guild_id BIGINT UNSIGNED NOT NULL, created_ms BIGINT UNSIGNED NOT NULL,
      terminal_ms BIGINT UNSIGNED NOT NULL DEFAULT 0, document JSON NOT NULL,
      INDEX imageguard_guild_terminal(guild_id,terminal_ms), INDEX imageguard_retention(created_ms)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS imageguard_stats (
      guild_id BIGINT UNSIGNED PRIMARY KEY, scanned BIGINT UNSIGNED NOT NULL DEFAULT 0,
      skipped BIGINT UNSIGNED NOT NULL DEFAULT 0, errors BIGINT UNSIGNED NOT NULL DEFAULT 0,
      matched BIGINT UNSIGNED NOT NULL DEFAULT 0, health VARCHAR(32) NOT NULL DEFAULT ''
    ) ENGINE=InnoDB""",
)


class Store:
    async def release_terminal(self, identifier):
        def update(row):
            row["terminal_ms"] = 0
            return row

        await self._incident(identifier, update)

    async def change(self, guild, operation, expected=None):
        def mutate(doc):
            if expected is not None and doc["revision"] != expected:
                from .models import GuardError

                raise GuardError("Settings changed. Refresh and try again.")
            result = operation(doc)
            validate(doc)
            doc["revision"] += 1
            return result

        return await self._config(guild, mutate)

    async def snapshot(self, guild):
        return await self._config(guild, None)

    async def open_incident(self, record):
        return await self._incident(record["id"], lambda old: old or record)

    async def incident(self, identifier):
        return await self._incident(identifier, None)

    async def update_incident(self, identifier, operation):
        return await self._incident(identifier, operation)

    async def claim(self, identifier, owner, now, *, recovery=False):
        def update(row):
            if (
                row
                and (
                    (recovery and not row.get("reconciled"))
                    or (not recovery and row.get("status") != "done")
                )
                and row.get("lease_until", 0) <= now
            ):
                row.update(lease_owner=owner, lease_until=now + 120_000)
            return row

        row = await self._incident(identifier, update)
        return row if row and row.get("lease_owner") == owner else None

    async def step(self, identifier, owner, name, state=None, **details):
        def update(row):
            if row and row.get("lease_owner") == owner:
                steps = row.setdefault("steps", {})
                if state is not None:
                    attempt = steps.get(name, {}).get("attempt") if state == "uncertain" else None
                    update = {"attempt": attempt} if attempt else {}
                    steps[name] = {"state": state, **update, **details}
            return row

        row = await self._incident(identifier, update)
        return (row or {}).get("steps", {}).get(name, {})

    async def finish(self, identifier, owner, outcome):
        def update(row):
            if row and row.get("lease_owner") == owner:
                row.update(status="done", outcome=outcome, lease_until=0)
            return row

        return await self._incident(identifier, update)


class MemoryStore(Store):
    def __init__(self):
        self.configs, self.incidents, self.stats = {}, {}, {}
        self.lock = asyncio.Lock()

    async def _config(self, guild, fn):
        async with self.lock:
            doc = copy.deepcopy(self.configs.get(guild, fresh()))
            if fn is None:
                return doc
            result = fn(doc)
            self.configs[guild] = doc
            return copy.deepcopy(result)

    async def _incident(self, key, fn):
        async with self.lock:
            row = copy.deepcopy(self.incidents.get(key))
            if fn is not None:
                row = fn(row)
                if row:
                    self.incidents[key] = row
            return copy.deepcopy(row)

    async def reserve_terminal(self, identifier, guild, now):
        async with self.lock:
            row = self.incidents[identifier]
            if row.get("terminal_ms"):
                return True
            if (
                sum(
                    r["guild"] == guild and r.get("terminal_ms", 0) > now - 3_600_000
                    for r in self.incidents.values()
                )
                >= 5
            ):
                return False
            row["terminal_ms"] = now
            return True

    async def metric(self, guild, kind, health=None):
        row = self.stats.setdefault(guild, {})
        row[kind] = row.get(kind, 0) + 1
        if health is not None:
            row["health"] = health

    async def status(self, guild):
        return copy.deepcopy(self.stats.get(guild, {}))

    async def references(self):
        return {i["sha256"] for c in self.configs.values() for i in c["images"]}

    async def recoverable(self, now):
        return [
            copy.deepcopy(r)
            for r in self.incidents.values()
            if r["created"] < now - 120_000
            and not r.get("reconciled")
            and (
                r.get("status") != "done" or "uncertain" in r.get("outcome", "")
                or r.get("steps", {}).get("evidence_update", {}).get("state") == "uncertain"
            )
        ][:100]

    async def cleanup(self, now):
        self.incidents = {
            k: v for k, v in self.incidents.items() if v["created"] >= now - RETENTION
        }

    async def close(self):
        pass


class MySQLStore(Store):
    def __init__(self, dsn):
        p = urlparse(dsn)
        if p.scheme not in {"mysql", "mariadb"}:
            raise ValueError("invalid ImageGuard DSN")
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

    async def _config(self, guild, fn):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                if fn is not None:
                    await q.execute(
                        "INSERT IGNORE INTO imageguard_config VALUES (%s,%s)",
                        (guild, json.dumps(fresh())),
                    )
                await q.execute(
                    "SELECT document FROM imageguard_config WHERE guild_id=%s"
                    + (" FOR UPDATE" if fn else ""),
                    (guild,),
                )
                row = await q.fetchone()
                doc = json.loads(row[0]) if row else fresh()
                if fn is None:
                    return doc
                result = fn(doc)
                await q.execute(
                    "UPDATE imageguard_config SET document=%s WHERE guild_id=%s",
                    (json.dumps(doc), guild),
                )
                await c.commit()
                return result
            finally:
                await c.rollback()

    async def _incident(self, key, fn):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:

                await q.execute("SELECT GET_LOCK(%s,5)", ("imageguard:" + key,))
                if (await q.fetchone())[0] != 1:
                    raise TimeoutError("ImageGuard incident busy")
                await q.execute(
                    "SELECT document FROM imageguard_incidents WHERE id=%s FOR UPDATE", (key,)
                )
                row = await q.fetchone()
                record = json.loads(row[0]) if row else None
                if fn is not None:
                    record = fn(record)
                    if record:
                        await q.execute(
                            """INSERT INTO imageguard_incidents VALUES (%s,%s,%s,%s,%s)
                          ON DUPLICATE KEY UPDATE document=VALUES(document),
                          terminal_ms=VALUES(terminal_ms)""",
                            (
                                key,
                                record["guild"],
                                record["created"],
                                record.get("terminal_ms", 0),
                                json.dumps(record),
                            ),
                        )
                await c.commit()
                return record
            finally:
                await c.rollback()
                await q.execute("SELECT RELEASE_LOCK(%s)", ("imageguard:" + key,))
                await q.fetchone()

    async def reserve_terminal(self, identifier, guild, now):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT guild_id FROM imageguard_config WHERE guild_id=%s FOR UPDATE", (guild,)
                )
                if not await q.fetchone():
                    return False
                await q.execute(
                    "SELECT terminal_ms,document FROM imageguard_incidents "
                    "WHERE id=%s AND guild_id=%s FOR UPDATE",
                    (identifier, guild),
                )
                row = await q.fetchone()
                if not row:
                    return False
                if row[0]:
                    return True
                await q.execute(
                    "SELECT COUNT(*) FROM imageguard_incidents "
                    "WHERE guild_id=%s AND terminal_ms>%s",
                    (guild, now - 3_600_000),
                )
                if (await q.fetchone())[0] >= 5:
                    return False
                record = json.loads(row[1])
                record["terminal_ms"] = now
                await q.execute(
                    "UPDATE imageguard_incidents SET terminal_ms=%s,document=%s WHERE id=%s",
                    (now, json.dumps(record), identifier),
                )
                await c.commit()
                return True
            finally:
                await c.rollback()

    async def metric(self, guild, kind, health=None):
        if kind not in {"scanned", "skipped", "errors", "matched"}:
            raise ValueError("invalid metric")
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute("INSERT IGNORE INTO imageguard_stats (guild_id) VALUES (%s)", (guild,))
            await q.execute(
                f"UPDATE imageguard_stats SET {kind}={kind}+1"
                + (",health=%s" if health is not None else "")
                + " WHERE guild_id=%s",
                (health, guild) if health is not None else (guild,),
            )
            await c.commit()

    async def status(self, guild):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT scanned,skipped,errors,matched,health FROM imageguard_stats "
                    "WHERE guild_id=%s",
                    (guild,),
                )
                row = await q.fetchone()
                return (
                    dict(
                        zip(("scanned", "skipped", "errors", "matched", "health"), row, strict=True)
                    )
                    if row
                    else {}
                )
            finally:
                await c.rollback()

    async def cleanup(self, now):
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute(
                "DELETE FROM imageguard_incidents WHERE created_ms<%s LIMIT 1000",
                (now - RETENTION,),
            )
            await c.commit()

    async def references(self):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute("SELECT document FROM imageguard_config")
                return {
                    i["sha256"] for row in await q.fetchall() for i in json.loads(row[0])["images"]
                }
            finally:
                await c.rollback()

    async def recoverable(self, now):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    """SELECT document FROM imageguard_incidents WHERE created_ms<%s
                  AND COALESCE(JSON_VALUE(document,'$.reconciled'),'false')<>'true'
                  AND (JSON_VALUE(document,'$.status')<>'done' OR
                       JSON_VALUE(document,'$.outcome') LIKE '%%uncertain%%' OR
                       JSON_VALUE(document,'$.steps.evidence_update.state')='uncertain')
                  ORDER BY created_ms LIMIT 100""",
                    (now - 120_000,),
                )
                return [json.loads(r[0]) for r in await q.fetchall()]
            finally:
                await c.rollback()
