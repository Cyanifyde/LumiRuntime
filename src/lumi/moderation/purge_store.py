from __future__ import annotations

import asyncio
import copy
import json
import secrets
from contextlib import asynccontextmanager

ACTIVE = ("confirm", "queued", "running", "paused", "uncertain", "handover", "cancelling")
SCHEMA = (
    """CREATE TABLE IF NOT EXISTS moderation_purge_clock (
      singleton TINYINT UNSIGNED PRIMARY KEY, next_ms BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS moderation_purge_jobs (
      id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      source_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
      guild_id BIGINT UNSIGNED NOT NULL, channel_id BIGINT UNSIGNED NOT NULL,
      state VARCHAR(16) NOT NULL, created_ms BIGINT UNSIGNED NOT NULL,
      updated_ms BIGINT UNSIGNED NOT NULL, next_ms BIGINT UNSIGNED NOT NULL,
      revision BIGINT UNSIGNED NOT NULL, lease_token CHAR(32) NOT NULL DEFAULT '',
      lease_until_ms BIGINT UNSIGNED NOT NULL DEFAULT 0, document JSON NOT NULL,
      INDEX purge_due(state,next_ms), INDEX purge_channel(guild_id,channel_id,state)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS moderation_purge_duplicates (
      job_id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      keeper_id BIGINT UNSIGNED NOT NULL, PRIMARY KEY(job_id,digest),
      FOREIGN KEY(job_id)
      REFERENCES moderation_purge_jobs(id) ON DELETE CASCADE
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS moderation_channel_handovers (
      id CHAR(8) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      guild_id BIGINT UNSIGNED NOT NULL, old_channel BIGINT UNSIGNED NOT NULL,
      new_channel BIGINT UNSIGNED NOT NULL DEFAULT 0,
      phase VARCHAR(24) NOT NULL, revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
      document JSON NOT NULL, INDEX handover_channels(guild_id,old_channel,new_channel)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS moderation_handover_workers (
      worker_group VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      generation BIGINT UNSIGNED NOT NULL, protocol SMALLINT UNSIGNED NOT NULL,
      seen_ms BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS moderation_channel_writes (
      id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      guild_id BIGINT UNSIGNED NOT NULL,source_channel BIGINT UNSIGNED NOT NULL,
      destination BIGINT UNSIGNED NOT NULL,state VARCHAR(16) NOT NULL,
      receipt_key VARCHAR(256) NOT NULL,updated_ms BIGINT UNSIGNED NOT NULL,
      INDEX channel_writes(guild_id,source_channel,destination,state)
    ) ENGINE=InnoDB""",
)


def initial(record):
    return {
        **copy.deepcopy(record),
        "id": secrets.token_hex(4),
        "revision": 1,
        "lease_token": "",
        "lease_until": 0,
        "next": record["created"],
        "updated": record["created"],
    }


def admit(records, record):
    for value in records:
        if value["key"] == record["key"]:
            return copy.deepcopy(value), "existing"
    active = [r for r in records if r["state"] in ACTIVE]
    if any(
        r["guild"] == record["guild"] and record["channel"] in {r["channel"], r.get("replacement")}
        for r in active
    ):
        return None, "busy"
    if record.get("mode") == "reset" and any(
        r["guild"] == record["guild"] and r.get("mode") == "reset" for r in active
    ):
        return None, "busy"
    if len(active) >= 32 or sum(r["guild"] == record["guild"] for r in active) >= 4:
        return None, "full"
    return initial(record), "created"


class MemoryPurgeStore:
    def __init__(self):
        self.jobs, self.duplicates, self.handovers = {}, {}, {}
        self.lock = asyncio.Lock()
        self.execution_lock = asyncio.Lock()
        self.next_mutation = 0

    async def pace(self, now, *, complete=False):
        async with self.lock:
            if complete or self.next_mutation <= now:
                self.next_mutation = max(self.next_mutation, now + 1000)
                return 0
            return self.next_mutation

    async def defer(self, until):
        async with self.lock:
            self.next_mutation = max(self.next_mutation, until)

    async def renew(self, row, now):
        async with self.lock:
            current = self.jobs[row["id"]]
            if (current["revision"], current["lease_token"]) != (
                row["revision"],
                row["lease_token"],
            ):
                return False
            current["lease_until"] = row["lease_until"] = now + 120000
            return True

    @asynccontextmanager
    async def exclusive(self):
        async with self.execution_lock:
            yield True

    async def create(self, record):
        async with self.lock:
            value, status = admit(self.jobs.values(), record)
            if status == "created":
                while value["id"] in self.jobs:
                    value["id"] = secrets.token_hex(4)
                self.jobs[value["id"]] = copy.deepcopy(value)
            return value, status

    async def get(self, guild, channel, code=None):
        async with self.lock:
            candidates = [
                r
                for r in self.jobs.values()
                if r["guild"] == guild
                and channel in {r["channel"], r.get("replacement")}
                and (code is None or r["id"] == code)
            ]
            return (
                copy.deepcopy(max(candidates, key=lambda r: r["created"])) if candidates else None
            )

    async def ready(self, now, *, mode=None):
        async with self.lock:
            for row in self.jobs.values():
                if (mode is None or mode == row["mode"]) and row["state"] == "confirm" and row["expires"] <= now:
                    row.update(state="expired", revision=row["revision"] + 1, updated=now)
            return [
                copy.deepcopy(r)
                for r in sorted(
                    self.jobs.values(),
                    key=lambda r: (r["next"], r.get("served", 0), r["created"]),
                )
                if r["state"] in {"queued", "running", "handover", "cancelling"}
                and (mode is None or mode == r["mode"])
                and r["next"] <= now
                and r["lease_until"] <= now
            ][:32]

    async def claim(self, row, now):
        async with self.lock:
            saved = self.jobs[row["id"]]
            if saved["revision"] != row["revision"] or saved["lease_until"] > now:
                return None
            saved.update(lease_token=secrets.token_hex(16), lease_until=now + 120000)
            return copy.deepcopy(saved)

    async def save(self, row, now, *, release=False):
        async with self.lock:
            old = self.jobs[row["id"]]
            if (
                old["revision"] != row["revision"]
                or old["lease_token"] != row["lease_token"]
                or old["lease_until"] <= now
            ):
                return False
            row.update(revision=row["revision"] + 1, updated=now)
            if release:
                row.update(lease_token="", lease_until=0)
            self.jobs[row["id"]] = copy.deepcopy(row)
            return True

    async def control(self, row, state, now, *, requester=None, prepare_handover=None):
        async with self.lock:
            old = self.jobs.get(row["id"])
            if not old or old["revision"] != row["revision"] or old["state"] not in ACTIVE:
                return None
            if requester is not None and requester != old["actor"]:
                return None
            updated = dict(
                old,
                state=state,
                revision=old["revision"] + 1,
                updated=now,
                next=now,
                lease_token="",
                lease_until=0,
            )
            if prepare_handover is not None and not await prepare_handover.prepare(updated, now):
                raise ValueError("Cannot prepare the reset")
            self.jobs[row["id"]] = updated
            return copy.deepcopy(updated)

    async def recover_unprepared(self, handovers, now):
        async with self.lock:
            for row in self.jobs.values():
                if row.get("mode") == "reset" and row["state"] in {"queued", "cancelling"}:
                    await handovers.prepare(row, now)

    async def duplicate(self, job, digest, keeper=None):
        async with self.lock:
            if keeper is not None:
                self.duplicates[job, digest] = keeper
            return self.duplicates.get((job, digest))

    async def keepers(self, job, digests):
        if len(digests) > 100:
            raise ValueError("Unbounded duplicate lookup")
        async with self.lock:
            return {
                digest: self.duplicates[job, digest]
                for digest in digests
                if (job, digest) in self.duplicates
            }

    async def remember(self, job, updates):
        if len(updates) > 100:
            raise ValueError("Unbounded duplicate checkpoint")
        async with self.lock:
            self.duplicates.update({(job, digest): keeper for digest, keeper in updates.items()})

    async def cleanup(self, now, *, mode=None):
        async with self.lock:
            for key, row in list(self.jobs.items()):
                if ((mode is None or row["mode"] == mode) and row["state"] not in ACTIVE
                        and row["updated"] < now - 30 * 86400000):
                    self.jobs.pop(key)
                    self.duplicates = {k: v for k, v in self.duplicates.items() if k[0] != key}


class MySQLPurgeStore:
    def __init__(self, owner):
        self.owner = owner

    @property
    def pool(self):
        return self.owner.pool

    async def defer(self, until):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "INSERT INTO moderation_purge_clock VALUES (1,%s) "
                    "ON DUPLICATE KEY UPDATE next_ms=GREATEST(next_ms,VALUES(next_ms))",
                    (until,),
                )
                await c.commit()
            finally:
                await c.rollback()

    async def pace(self, now, *, complete=False):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute("INSERT IGNORE INTO moderation_purge_clock VALUES (1,0)")
                await q.execute(
                    "SELECT next_ms FROM moderation_purge_clock WHERE singleton=1 FOR UPDATE"
                )
                due = int((await q.fetchone())[0])
                if complete or due <= now:
                    await q.execute(
                        "UPDATE moderation_purge_clock SET next_ms=GREATEST(next_ms,%s) "
                        "WHERE singleton=1",
                        (now + 1000,),
                    )
                    await c.commit()
                    return 0
                return due
            finally:
                await c.rollback()

    async def renew(self, row, now):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "UPDATE moderation_purge_jobs SET lease_until_ms=%s "
                    "WHERE id=%s AND revision=%s AND lease_token=%s",
                    (now + 120000, row["id"], row["revision"], row["lease_token"]),
                )
                valid = q.rowcount > 0
                await c.commit()
                if valid:
                    row["lease_until"] = now + 120000
                return valid
            finally:
                await c.rollback()

    @asynccontextmanager
    async def exclusive(self):

        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute("SELECT GET_LOCK('lumi:purge:executor',0)")
                acquired = (await q.fetchone())[0] == 1
                yield acquired
            finally:
                await c.rollback()
                await q.execute("SELECT RELEASE_LOCK('lumi:purge:executor')")
                await q.fetchone()

    async def migrate(self):
        async with self.pool.acquire() as c, c.cursor() as q:
            for sql in SCHEMA:
                await q.execute(sql)
            await c.commit()

    @staticmethod
    def read(raw):
        if not raw:
            return None
        row = json.loads(raw[0])
        row.update(
            revision=int(raw[1]),
            lease_token=raw[2],
            lease_until=int(raw[3]),
            state=raw[4],
            next=int(raw[5]),
            updated=int(raw[6]),
        )
        return row

    SELECT = (
        "SELECT document,revision,lease_token,lease_until_ms,state,next_ms,updated_ms "
        "FROM moderation_purge_jobs "
    )

    async def create(self, record):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute("SELECT GET_LOCK('lumi:purge:admission',5)")
                if (await q.fetchone())[0] != 1:
                    raise TimeoutError("purge admission busy")
                await q.execute(self.SELECT + "WHERE source_key=%s FOR UPDATE", (record["key"],))
                existing = self.read(await q.fetchone())
                if existing:
                    return existing, "existing"
                await q.execute(
                    self.SELECT
                    + "WHERE state IN ("
                    + ",".join("%s" for _ in ACTIVE)
                    + ") FOR UPDATE",
                    ACTIVE,
                )
                records = [self.read(raw) for raw in await q.fetchall()]
                row, status = admit(records, record)
                if status != "created":
                    return row, status
                for _ in range(10):
                    await q.execute(
                        "INSERT IGNORE INTO moderation_purge_jobs "
                        "(id,source_key,guild_id,channel_id,state,created_ms,updated_ms,"
                        "next_ms,revision,document) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s)",
                        (
                            row["id"],
                            row["key"],
                            row["guild"],
                            row["channel"],
                            row["state"],
                            row["created"],
                            row["updated"],
                            row["next"],
                            json.dumps(row),
                        ),
                    )
                    if q.rowcount:
                        await c.commit()
                        return row, "created"
                    row["id"] = secrets.token_hex(4)
                raise RuntimeError("purge ID unavailable")
            finally:
                await c.rollback()
                await q.execute("SELECT RELEASE_LOCK('lumi:purge:admission')")
                await q.fetchone()

    async def get(self, guild, channel, code=None):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    self.SELECT + "WHERE guild_id=%s AND (channel_id=%s OR "
                    "JSON_VALUE(document,'$.replacement')=%s) "
                    + ("AND id=%s " if code else "")
                    + "ORDER BY created_ms DESC LIMIT 1",
                    (guild, channel, channel, code) if code else (guild, channel, channel),
                )
                return self.read(await q.fetchone())
            finally:
                await c.rollback()

    async def ready(self, now, *, mode=None):
        scope = " AND JSON_VALUE(document,'$.mode')=%s" if mode else ""
        arguments = (now, now, mode) if mode else (now, now)
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "UPDATE moderation_purge_jobs SET state='expired',revision=revision+1,"
                    "updated_ms=%s "
                    "WHERE state='confirm' AND JSON_VALUE(document,'$.expires')<=%s" + scope,
                    arguments,
                )
                await c.commit()
                await q.execute(
                    self.SELECT + "WHERE state IN ('queued','running','handover','cancelling') "
                    "AND next_ms<=%s AND lease_until_ms<=%s" + scope + " ORDER BY next_ms,"
                    "CAST(COALESCE(JSON_VALUE(document,'$.served'),0) AS UNSIGNED),"
                    "created_ms LIMIT 32",
                    arguments,
                )
                return [self.read(raw) for raw in await q.fetchall()]
            finally:
                await c.rollback()

    async def claim(self, row, now):
        token = secrets.token_hex(16)
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "UPDATE moderation_purge_jobs SET lease_token=%s,lease_until_ms=%s "
                    "WHERE id=%s AND revision=%s AND lease_until_ms<=%s "
                    "AND state IN ('queued','running','handover','cancelling')",
                    (token, now + 120000, row["id"], row["revision"], now),
                )
                if not q.rowcount:
                    return None
                await c.commit()
                return {**row, "lease_token": token, "lease_until": now + 120000}
            finally:
                await c.rollback()

    async def save(self, row, now, *, release=False):
        updated = {**row, "revision": row["revision"] + 1, "updated": now}
        if release:
            updated.update(lease_token="", lease_until=0)
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "UPDATE moderation_purge_jobs SET document=%s,state=%s,revision=%s,"
                    "updated_ms=%s,"
                    "next_ms=%s,lease_token=%s,lease_until_ms=%s WHERE id=%s AND revision=%s "
                    "AND lease_token=%s AND lease_until_ms>%s",
                    (
                        json.dumps(updated),
                        updated["state"],
                        updated["revision"],
                        now,
                        updated["next"],
                        updated["lease_token"],
                        updated["lease_until"],
                        row["id"],
                        row["revision"],
                        row["lease_token"],
                        now,
                    ),
                )
                if not q.rowcount:
                    return False
                await c.commit()
                row.update(updated)
                return True
            finally:
                await c.rollback()

    async def control(self, row, state, now, *, requester=None, prepare_handover=None):
        if requester is not None and requester != row["actor"]:
            return None
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "UPDATE moderation_purge_jobs SET state=%s,revision=revision+1,updated_ms=%s,"
                    "next_ms=%s,lease_token='',lease_until_ms=0 WHERE id=%s AND revision=%s "
                    "AND state IN (" + ",".join("%s" for _ in ACTIVE) + ")",
                    (state, now, now, row["id"], row["revision"], *ACTIVE),
                )
                if not q.rowcount:
                    return None
                if prepare_handover is not None and not await prepare_handover.prepare(
                    q, row["id"], row["revision"] + 1, now,
                ):
                    raise ValueError("Cannot prepare the reset")
                await c.commit()
                return {
                    **row,
                    "state": state,
                    "revision": row["revision"] + 1,
                    "updated": now,
                    "next": now,
                    "lease_token": "",
                    "lease_until": 0,
                }
            finally:
                await c.rollback()

    async def recover_unprepared(self, handovers, now):

        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT job.id,job.revision FROM moderation_purge_jobs AS job "
                    f"LEFT JOIN `{handovers.table}` AS gate ON gate.id=job.id "
                    "WHERE gate.id IS NULL AND job.state IN ('queued','cancelling') "
                    "AND JSON_VALUE(job.document,'$.mode')='reset' "
                    "ORDER BY job.id LIMIT 32"
                )
                for code, revision in await q.fetchall():
                    await handovers.prepare(q, code, int(revision), now)
                await c.commit()
            finally:
                await c.rollback()

    async def duplicate(self, job, digest, keeper=None):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                if keeper is not None:
                    await q.execute(
                        "INSERT INTO moderation_purge_duplicates VALUES (%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE keeper_id=VALUES(keeper_id)",
                        (job, digest, keeper),
                    )
                    await c.commit()
                await q.execute(
                    "SELECT keeper_id FROM moderation_purge_duplicates "
                    "WHERE job_id=%s AND digest=%s",
                    (job, digest),
                )
                value = await q.fetchone()
                return int(value[0]) if value else None
            finally:
                await c.rollback()

    async def keepers(self, job, digests):
        if len(digests) > 100:
            raise ValueError("Unbounded duplicate lookup")
        if not digests:
            return {}
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT digest,keeper_id FROM moderation_purge_duplicates WHERE job_id=%s "
                    "AND digest IN (" + ",".join("%s" for _ in digests) + ")",
                    (job, *sorted(digests)),
                )
                return {digest: int(keeper) for digest, keeper in await q.fetchall()}
            finally:
                await c.rollback()

    async def remember(self, job, updates):
        if len(updates) > 100:
            raise ValueError("Unbounded duplicate checkpoint")
        if not updates:
            return
        values = tuple(
            value for digest, keeper in sorted(updates.items()) for value in (job, digest, keeper)
        )
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "INSERT INTO moderation_purge_duplicates (job_id,digest,keeper_id) VALUES "
                    + ",".join("(%s,%s,%s)" for _ in updates)
                    + " ON DUPLICATE KEY UPDATE keeper_id=VALUES(keeper_id)",
                    values,
                )
                await c.commit()
            finally:
                await c.rollback()

    async def cleanup(self, now, *, mode=None):
        scope = " AND JSON_VALUE(document,'$.mode')=%s" if mode else ""
        cutoff = now - 30 * 86400000
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute(
                "DELETE FROM moderation_purge_jobs WHERE state NOT IN ("
                + ",".join("%s" for _ in ACTIVE)
                + ") AND updated_ms<%s" + scope + " LIMIT 1000",
                (*ACTIVE, cutoff, mode) if mode else (*ACTIVE, cutoff),
            )
            await c.commit()
