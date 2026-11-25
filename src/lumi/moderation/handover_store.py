from __future__ import annotations

import asyncio
import copy
import hashlib
import json

from lumi.contracts.channel_fence import CRITICAL, DEFAULT_TRANSITION, TERMINAL, blocked
from lumi.contracts.handover_reference import PARTICIPANTS
from lumi.shared.action_receipts import action_receipt_key
from lumi.storage.handover_policy import CLEANUP, RETENTION_MS, retention_statements

PROTOCOL = 5
HEARTBEAT_TTL_MS = 15_000
HEARTBEAT_SKEW_MS = 1000
GROUPS = PARTICIPANTS
def waiting_record(job, now):
    return dict(
        id=job["id"], guild=job["guild"], old=job["channel"], new=0, phase="waiting",
        started=now, actor=job["actor"], settings=copy.deepcopy(job["settings"]),
        epoch=0, acks={}, plans={},
    )


class MemoryHandoverStore:
    def __init__(self):
        self.rows, self.workers, self.deliveries = {}, {}, {}
        self.lock = asyncio.Lock()

    async def begin(self, row):
        async with self.lock:
            self.rows.setdefault(row["id"], copy.deepcopy(row))
            return copy.deepcopy(self.rows[row["id"]])

    async def prepare(self, job, now):
        if job.get("mode") != "reset" or job["state"] not in {"queued", "cancelling"} or not isinstance(job.get("settings"), dict):
            return False
        await self.begin(waiting_record(job, now))
        return True

    async def get(self, code):
        async with self.lock:
            return copy.deepcopy(self.rows.get(code))

    async def mutate(self, code, change):
        async with self.lock:
            row = copy.deepcopy(self.rows[code])
            change(row)
            row["revision"] = row.get("revision", 0) + 1
            self.rows[code] = row
            return copy.deepcopy(row)

    async def index(self, guild=None):
        async with self.lock:
            return [
                {key: row.get(key, 0) for key in ("id", "guild", "old", "new", "phase", "started")}
                for row in self.rows.values()
                if guild is None or row["guild"] == guild
            ]

    async def heartbeat(self, group, generation, now):
        async with self.lock:
            if generation >= self.workers.get(group, (0, 0, 0))[0]:
                self.workers[group] = (generation, PROTOCOL, now)

    async def ready(self, now):
        async with self.lock:
            return {
                g: n
                for g, (n, p, seen) in self.workers.items()
                if p == PROTOCOL and now - HEARTBEAT_TTL_MS <= seen <= now + HEARTBEAT_SKEW_MS
            }

    async def begin_action(self, action, job, now):
        code = hashlib.sha256((action.module + ":" + action.idempotency_key).encode()).hexdigest()
        async with self.lock:
            if code in self.deliveries:
                return None
            self.deliveries[code] = dict(
                id=code,
                guild=job.guild_id,
                source=job.channel_id or 0,
                destination=action.channel_id or 0,
                state="prepared",
                receipt_key=action_receipt_key(action),
                updated=now,
            )
            return code

    async def finish_action(self, code, uncertain, now):
        async with self.lock:
            self.deliveries[code].update(state="uncertain" if uncertain else "settled", updated=now)

    async def pending_actions(self, guild, channel):
        async with self.lock:
            return copy.deepcopy(
                [
                    r
                    for r in self.deliveries.values()
                    if r["guild"] == guild
                    and channel in {r["source"], r["destination"]}
                    and r["state"] != "settled"
                ]
            )

    async def cleanup(self, now):
        cutoff = now - RETENTION_MS
        async with self.lock:
            for key in [key for key, row in self.rows.items()
                        if row["phase"] in TERMINAL and 0 < row.get("completed", cutoff) < cutoff][:1000]:
                del self.rows[key]
            for key in [key for key, row in self.deliveries.items()
                        if row["state"] == "settled" and row["updated"] < cutoff][:1000]:
                del self.deliveries[key]


class MySQLHandoverStore:
    def __init__(self, owner, *, group=None):
        self.owner = owner
        self.group = group
        self.table = "moderation_channel_handovers"
        self.heartbeat_table = "moderation_handover_workers"
        self.procedure = None
        if group is not None:
            from lumi.storage.handover_policy import names

            self.table, self.heartbeat_table, self.procedure = names(group)

    @property
    def pool(self):
        return self.owner.pool

    async def prepare(self, q, code, revision, now):

        from lumi.storage.handover_policy import STAGE, stage_insert

        if self.group is None:
            await q.execute(stage_insert("%s", "%s", "%s"), (now, code, revision))
        elif self.group == "security":
            await q.execute(f"CALL `{STAGE}`(%s,%s,%s)", (code, revision, now))
            while await q.nextset():
                pass
        else:
            raise ValueError("Only the reset owner can prepare a handover")
        await q.execute(f"SELECT id FROM `{self.table}` WHERE id=%s", (code,))
        return bool(await q.fetchone())

    async def get(self, code):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(f"SELECT document FROM `{self.table}` WHERE id=%s", (code,))
                raw = await q.fetchone()
                return json.loads(raw[0]) if raw else None
            finally:
                await c.rollback()

    async def mutate(self, code, change, *, reset_lease=None):
        if self.group is not None:
            if reset_lease is not None:
                raise ValueError("A participant cannot mutate the reset journal")
            return await self._mutate_projection(code, change)
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                if reset_lease is not None:
                    revision, token, now = reset_lease
                    await q.execute(
                        "SELECT id FROM moderation_purge_jobs WHERE id=%s AND revision=%s AND lease_token=%s "
                        "AND lease_until_ms>%s AND state IN ('queued','handover','cancelling','running') "
                        "AND JSON_VALUE(document,'$.mode')='reset' FOR UPDATE", (code, revision, token, now()),
                    )
                    if not await q.fetchone():
                        return None
                await q.execute(
                    "SELECT document FROM moderation_channel_handovers WHERE id=%s FOR UPDATE",
                    (code,),
                )
                row = json.loads((await q.fetchone())[0])
                change(row)
                row["revision"] = row.get("revision", 0) + 1
                await self.save(q, row)
                await c.commit()
                return row
            finally:
                await c.rollback()

    async def _mutate_projection(self, code, change):
        before = await self.get(code)
        if before is None:
            raise ValueError("Handover unavailable")
        row = copy.deepcopy(before)
        change(row)
        fields = {"acks": "ack", "plans": "plans", "errors": "error"}
        changes = {}
        for field, parameter in fields.items():
            old = (before.get(field) or {}).get(self.group)
            new = (row.get(field) or {}).get(self.group)
            if new != old:
                changes[parameter] = new
        if not changes:
            return before
        document = json.dumps(changes, separators=(",", ":"))
        if len(document.encode()) > 16 * 1024 * 1024:
            raise ValueError("Reset journal too large")
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    f"CALL `{self.procedure}`(%s,%s,%s,%s)",
                    (code, before["phase"], before.get("epoch", 0), document),
                )
                while await q.nextset():
                    pass
                await c.commit()
            finally:
                await c.rollback()
        return await self.get(code)

    @staticmethod
    async def save(q, row):
        document = json.dumps(row, separators=(",", ":"))
        if len(document.encode()) > 16 * 1024 * 1024:
            raise ValueError("Reset journal too large")
        await q.execute(
            "UPDATE moderation_channel_handovers SET "
            "new_channel=%s,phase=%s,revision=%s,document=%s WHERE id=%s",
            (row.get("new", 0), row["phase"], row.get("revision", 1), document, row["id"]),
        )

    async def index(self, guild=None):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT "
                    "id,guild_id,old_channel,new_channel,phase,JSON_VALUE(document,'$.started') "
                    f"FROM `{self.table}` " + ("WHERE guild_id=%s" if guild is not None else ""),
                    (guild,) if guild is not None else (),
                )
                return [
                    dict(
                        id=code,
                        guild=int(g),
                        old=int(old),
                        new=int(new),
                        phase=phase,
                        started=int(started),
                    )
                    for code, g, old, new, phase, started in await q.fetchall()
                ]
            finally:
                await c.rollback()

    async def heartbeat(self, group, generation, now):
        if group not in GROUPS:
            raise ValueError("Unknown handover participant")
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    f"INSERT INTO `{self.heartbeat_table}` VALUES (%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "protocol=IF(VALUES(generation)>=generation,VALUES(protocol),protocol),"
                    "seen_ms=IF(VALUES(generation)>=generation,VALUES(seen_ms),seen_ms),"
                    "generation=GREATEST(generation,VALUES(generation))",
                    (group, generation, PROTOCOL, now),
                )
                await c.commit()
            finally:
                await c.rollback()

    async def cleanup(self, now):

        if self.group not in (None, "security"):
            raise ValueError("Only Security owns transition retention")
        cutoff = now - RETENTION_MS
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                if self.group == "security":

                    await q.execute(f"CALL `{CLEANUP}`()")
                    while await q.nextset():
                        pass
                else:
                    for statement in retention_statements("%s"):
                        await q.execute(statement, (cutoff,))
                await c.commit()
            finally:
                await c.rollback()

    async def ready(self, now):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                return await self.read_workers(q, now)
            finally:
                await c.rollback()

    @staticmethod
    async def read_workers(q, now):
        await q.execute(
            "SELECT worker_group,generation FROM moderation_handover_workers "
            "WHERE protocol=%s AND seen_ms BETWEEN %s AND %s",
            (PROTOCOL, now - HEARTBEAT_TTL_MS, now + HEARTBEAT_SKEW_MS),
        )
        return {str(group): int(generation) for group, generation in await q.fetchall()}
