from __future__ import annotations

import asyncio

from lumi.contracts import decode, encode

from .statistics import CardDetail, DropDetail, Report, merge_detail, positive_number

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS game_drop_stats_activation (
      id TINYINT UNSIGNED PRIMARY KEY, cutoff_ms BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS game_drop_stats_sources (
      guild_id BIGINT UNSIGNED NOT NULL, message_id BIGINT UNSIGNED NOT NULL,
      game VARCHAR(8) CHARACTER SET ascii NOT NULL,
      occurred_at_ms BIGINT UNSIGNED NOT NULL, dropper_id BIGINT UNSIGNED NULL,
      revision_ms BIGINT UNSIGNED NOT NULL, source_order BIGINT UNSIGNED NOT NULL,
      document JSON NOT NULL,
      PRIMARY KEY(guild_id,message_id),
      KEY ix_drop_stats_range(guild_id,game,occurred_at_ms),
      KEY ix_drop_stats_player(guild_id,dropper_id,occurred_at_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS game_drop_stats_cards (
      guild_id BIGINT UNSIGNED NOT NULL, message_id BIGINT UNSIGNED NOT NULL,
      position TINYINT UNSIGNED NOT NULL, document JSON NOT NULL,
      PRIMARY KEY(guild_id,message_id,position)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
)


def decoded(value):
    return decode(value.encode() if isinstance(value, str) else value, DropDetail)


def karuta_update(detail, row, records):
    if detail.source.game != "karuta" or detail.image_ref != row["ref"]:
        return None
    revision, order = max((row["at_ms"], row["order"]), (detail.revision_ms, detail.order))
    cards = tuple(
        CardDetail(
            r["position"],
            positive_number(r.get("print_num")),
            positive_number(r.get("edition"), 100),
            (r.get("character") or None) if r.get("verified_identity") else None,
            (r.get("series") or None) if r.get("verified_identity") else None,
        )
        for r in records
    )
    update = DropDetail(
        detail.source,
        revision,
        order,
        detail.dropper,
        detail.kind,
        cards,
        detail.slots,
        detail.image_ref,
    )
    return update if update.valid() else None


class MemoryStatsStore:
    def __init__(self):
        self.cutoff_ms = None
        self.records = {}
        self.lock = asyncio.Lock()

    async def activate(self, now):
        if self.cutoff_ms is None:
            self.cutoff_ms = now
        return self.cutoff_ms

    async def record(self, detail):
        if not detail.valid():
            raise ValueError("Invalid detailed drop.")
        if self.cutoff_ms is None or detail.source.occurred_at_ms < self.cutoff_ms:
            return False
        key = detail.source.guild_id, detail.source.message_id
        async with self.lock:
            if key in self.records and self.records[key].source != detail.source:
                raise ValueError("Conflicting drop identity.")
            self.records[key] = merge_detail(self.records.get(key), detail)
        return True

    async def learn_karuta(self, row, records):
        async with self.lock:
            key = row["guild"], row["message"]
            previous = self.records.get(key)
            update = karuta_update(previous, row, records) if previous else None
            if update:
                self.records[key] = merge_detail(previous, update)

    async def report(self, guild, start, end, game="all", user=None, *, leaderboard=False):
        result = Report(self.cutoff_ms or end, start, end)
        for detail in tuple(self.records.values()):
            source = detail.source
            if (
                source.guild_id == guild
                and start <= source.occurred_at_ms < end
                and (game == "all" or source.game == game)
                and (user is None or detail.dropper == user)
            ):
                result.add(detail, leaderboard=leaderboard)
        return result


class MySQLStatsStore:
    def __init__(self, counts):
        self.counts = counts
        self.cutoff_ms = None

    @property
    def pool(self):
        return self.counts.pool

    async def migrate(self):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                for statement in SCHEMA:
                    await cursor.execute(statement)
                await conn.commit()
            finally:
                await conn.rollback()

    async def activate(self, now):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "INSERT INTO game_drop_stats_activation VALUES (1,%s) "
                    "ON DUPLICATE KEY UPDATE id=game_drop_stats_activation.id",
                    (now,),
                )
                await cursor.execute("SELECT cutoff_ms FROM game_drop_stats_activation WHERE id=1")
                self.cutoff_ms = int((await cursor.fetchone())[0])
                await conn.commit()
            finally:
                await conn.rollback()
        return self.cutoff_ms

    async def _save(self, cursor, detail):
        source = detail.source
        document = encode(detail).decode()
        await cursor.execute(
            "INSERT INTO game_drop_stats_sources "
            "(guild_id,message_id,game,occurred_at_ms,dropper_id,revision_ms,"
            "source_order,document) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
            "dropper_id=VALUES(dropper_id),revision_ms=VALUES(revision_ms),"
            "source_order=VALUES(source_order),document=VALUES(document)",
            (
                source.guild_id,
                source.message_id,
                source.game,
                source.occurred_at_ms,
                detail.dropper,
                detail.revision_ms,
                detail.order,
                document,
            ),
        )
        if detail.cards:
            await cursor.executemany(
                "INSERT INTO game_drop_stats_cards VALUES (%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE document=VALUES(document)",
                [
                    (source.guild_id, source.message_id, c.position, encode(c).decode())
                    for c in detail.cards
                ],
            )

    async def record(self, detail):
        if not detail.valid():
            raise ValueError("Invalid detailed drop.")
        if self.cutoff_ms is None or detail.source.occurred_at_ms < self.cutoff_ms:
            return False
        source = detail.source
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:

                await cursor.execute(
                    "INSERT INTO game_drop_stats_sources VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE message_id=VALUES(message_id)",
                    (
                        source.guild_id,
                        source.message_id,
                        source.game,
                        source.occurred_at_ms,
                        detail.dropper,
                        detail.revision_ms,
                        detail.order,
                        encode(detail).decode(),
                    ),
                )
                await cursor.execute(
                    "SELECT document FROM game_drop_stats_sources "
                    "WHERE guild_id=%s AND message_id=%s FOR UPDATE",
                    (source.guild_id, source.message_id),
                )
                previous = decoded((await cursor.fetchone())[0])
                if previous.source != source:
                    raise ValueError("Conflicting drop identity.")
                await self._save(cursor, merge_detail(previous, detail))
                await conn.commit()
            finally:
                await conn.rollback()
        return True

    async def learn_karuta(self, row, records):
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM game_drop_stats_sources "
                    "WHERE guild_id=%s AND message_id=%s FOR UPDATE",
                    (row["guild"], row["message"]),
                )
                found = await cursor.fetchone()
                previous = decoded(found[0]) if found else None
                update = karuta_update(previous, row, records) if previous else None
                if update:
                    await self._save(cursor, merge_detail(previous, update))
                await conn.commit()
            finally:
                await conn.rollback()

    async def report(self, guild, start, end, game="all", user=None, *, leaderboard=False):
        result = Report(self.cutoff_ms or end, start, end)
        where = "guild_id=%s AND occurred_at_ms>=%s AND occurred_at_ms<%s"
        args = [guild, start, end]
        if game != "all":
            where += " AND game=%s"
            args.append(game)
        if user is not None:
            where += " AND dropper_id=%s"
            args.append(user)

        last = 0
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            try:
                while True:
                    await cursor.execute(
                        "SELECT message_id,document FROM game_drop_stats_sources WHERE "
                        + where
                        + " AND message_id>%s ORDER BY message_id LIMIT 1000",
                        (*args, last),
                    )
                    batch = await cursor.fetchall()
                    if not batch:
                        break
                    for message, document in batch:
                        result.add(decoded(document), leaderboard=leaderboard)
                        last = int(message)
                    await asyncio.sleep(0)
            finally:
                await conn.rollback()
        return result
