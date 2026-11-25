from __future__ import annotations

import asyncio

from lumi.karuta_collections.models import Blob, identity
from lumi.karuta_collections.store import MySQLCatalogueStore

from .filters import SketchQuery, ordered
from .models import CardInfo, LibraryView, SketchRecord, search_text

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS karuta_sketch_library_imports (
      snapshot_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      status ENUM('importing','complete') NOT NULL, record_count INT UNSIGNED NOT NULL,
      completed_at TIMESTAMP(6) NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_sketch_library_images (
      sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      size_bytes INT UNSIGNED NOT NULL, width INT UNSIGNED NOT NULL, height INT UNSIGNED NOT NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_sketch_library_cards (
      snapshot_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      code VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      character_name VARCHAR(240) NOT NULL, series_name VARCHAR(240) NOT NULL,
      character_search VARCHAR(480) COLLATE utf8mb4_bin NOT NULL,
      series_search VARCHAR(480) COLLATE utf8mb4_bin NOT NULL,
      edition SMALLINT UNSIGNED NOT NULL, print_num INT UNSIGNED NOT NULL,
      ink INT UNSIGNED NOT NULL,
      image_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      source_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      PRIMARY KEY(snapshot_sha,code), KEY ink_order(snapshot_sha,ink,code),
      KEY print_order(snapshot_sha,print_num,code), KEY edition_order(snapshot_sha,edition,code),
      FOREIGN KEY(snapshot_sha) REFERENCES karuta_sketch_library_imports(snapshot_sha),
      FOREIGN KEY(image_sha) REFERENCES karuta_sketch_library_images(sha256)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS karuta_sketch_library_state (
      id TINYINT UNSIGNED PRIMARY KEY, revision BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_sketch_library_observations (
      code VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      character_name VARCHAR(240) NOT NULL, series_name VARCHAR(240) NOT NULL,
      edition SMALLINT UNSIGNED NOT NULL, print_num INT UNSIGNED NOT NULL,
      sketched BOOLEAN NOT NULL, ink INT UNSIGNED NULL,
      created_ms BIGINT UNSIGNED NOT NULL, message_id BIGINT UNSIGNED NOT NULL,
      revision_ms BIGINT UNSIGNED NOT NULL, event_order BIGINT UNSIGNED NOT NULL,
      image_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS karuta_sketch_library_live (
      code VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      character_name VARCHAR(240) NOT NULL, series_name VARCHAR(240) NOT NULL,
      character_search VARCHAR(480) COLLATE utf8mb4_bin NOT NULL,
      series_search VARCHAR(480) COLLATE utf8mb4_bin NOT NULL,
      edition SMALLINT UNSIGNED NOT NULL, print_num INT UNSIGNED NOT NULL,
      ink INT UNSIGNED NOT NULL,
      image_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      source_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      observation_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      FOREIGN KEY(image_sha) REFERENCES karuta_sketch_library_images(sha256)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
)
COLUMNS = {
    "character": "c.character_search",
    "series": "c.series_search",
    "ink": "c.ink",
    "edition": "c.edition",
    "print": "c.print_num",
}
EFFECTIVE = """(SELECT b.* FROM karuta_sketch_library_cards b
  WHERE b.snapshot_sha=%s AND NOT EXISTS
    (SELECT 1 FROM karuta_sketch_library_live l WHERE l.code=b.code)
  UNION ALL SELECT %s,l.code,l.character_name,l.series_name,l.character_search,l.series_search,
    l.edition,l.print_num,l.ink,l.image_sha,l.source_sha FROM karuta_sketch_library_live l) c"""
RECORD_COLUMNS = """c.code,c.character_name,c.series_name,c.edition,c.print_num,c.ink,
  i.sha256,i.size_bytes,i.width,i.height,c.source_sha"""
INFO_COLUMNS = (
    "code,character_name,series_name,edition,print_num,sketched,ink,"
    "created_ms,message_id,revision_ms,event_order,image_key"
)


def _info(row):
    return CardInfo(*row[:5], bool(row[5]), *row[6:])


def _record(row):
    return SketchRecord(*row[:6], Blob(row[6], "webp", "image/webp", *map(int, row[7:10])), row[10])


def _same_card(info, record):
    return info.sketched and (
        info.code,
        info.character,
        info.series,
        info.edition,
        info.print_num,
        info.ink,
    ) == (
        record.code,
        record.character,
        record.series,
        record.edition,
        record.print_num,
        record.ink,
    )


def sql_filter(snapshot: str, query: SketchQuery) -> tuple[str, list]:
    clauses, args = ["c.snapshot_sha=%s"], [snapshot]
    for field in ("character", "series"):
        value = getattr(query, field)
        if value:

            clauses.append(f"{COLUMNS[field]} = %s")
            args.append(value)
    if query.editions:
        clauses.append("c.edition IN (" + ",".join("%s" for _ in query.editions) + ")")
        args.extend(query.editions)
    for p in query.predicates:
        if p.operator not in {"=", "<", "<=", ">", ">="} or p.field not in COLUMNS:
            raise ValueError("invalid predicate")
        clauses.append(f"{COLUMNS[p.field]} {p.operator} %s")
        args.append(p.value)
    return " AND ".join(clauses), args


class MemorySketchStore:
    def __init__(self, records=(), snapshot="a" * 64):
        self.records, self.snapshot = list(records), snapshot
        self.observations, self.live, self.published = {}, {}, {}
        self.revision, self.lock = 0, asyncio.Lock()

    def _effective(self):
        return list({**{r.code: r for r in self.records}, **self.live}.values())

    async def current(self):
        return self.snapshot, len(self._effective())

    async def count(self, snapshot, query):
        return (await self.view(snapshot, query, 0)).count

    async def page(self, snapshot, query, offset, limit=6):
        return (await self.view(snapshot, query, offset, limit, clamp=False)).records

    async def view(self, snapshot, query, offset, limit=6, *, code="", delta=0, clamp=True):
        async with self.lock:
            rows = ordered(query, self._effective()) if snapshot == self.snapshot else []
            found = True
            if code:
                index = next((i for i, r in enumerate(rows) if r.code == code), None)
                found = index is not None
                if found:
                    offset = max(0, min(len(rows) - 1, index + delta))
            if clamp:
                offset = min(offset, max(0, (len(rows) - 1) // limit * limit))
            return LibraryView(
                len(rows), offset, tuple(rows[offset : offset + limit]), self.revision, found
            )

    async def observe(self, info: CardInfo, *, update_catalogue=True):
        async with self.lock:
            old = self.observations.get(info.code)
            if old and (
                info.stamp < old.stamp
                or (info.stamp == old.stamp and info.signature != old.signature)
            ):
                return False
            self.observations[info.code] = info
            return True

    async def is_published(self, info):
        return self.published.get(info.code) == info.signature

    async def publish(self, info, record):
        if not _same_card(info, record):
            raise ValueError("sketch observation mismatch")
        async with self.lock:
            if self.observations.get(info.code) != info:
                return False
            if self.live.get(info.code) != record:
                self.revision += 1
            self.live[info.code], self.published[info.code] = record, info.signature
            return True

    async def close(self):
        pass


class MySQLSketchStore(MySQLCatalogueStore):
    async def migrate(self):
        await super().migrate()
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            for statement in SCHEMA:
                await cursor.execute(statement)
            await cursor.execute("INSERT IGNORE INTO karuta_sketch_library_state VALUES (1,0)")
            await connection.commit()

    async def current(self) -> tuple[str, int]:
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("""SELECT snapshot_sha,record_count
              FROM karuta_sketch_library_imports
              WHERE status='complete' ORDER BY completed_at DESC,snapshot_sha LIMIT 1""")
            row = await cursor.fetchone()
            snapshot = row[0] if row else "0" * 64
            try:
                view = await self._view(cursor, snapshot, SketchQuery(), 0, 1)
                return snapshot, view.count
            finally:
                await connection.rollback()

    async def count(self, snapshot: str, query: SketchQuery) -> int:
        return (await self.view(snapshot, query, 0, 1)).count

    async def page(self, snapshot: str, query: SketchQuery, offset: int, limit: int = 6):
        return (await self.view(snapshot, query, offset, limit, clamp=False)).records

    async def view(self, snapshot, query, offset, limit=6, *, code="", delta=0, clamp=True):
        if (
            not 0 <= offset <= 1_000_000
            or not 1 <= limit <= 6
            or query.order not in COLUMNS
            or delta not in {-1, 0, 1}
        ):
            raise ValueError("invalid page")
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                return await self._view(
                    cursor, snapshot, query, offset, limit, code=code, delta=delta, clamp=clamp
                )
            finally:
                await connection.rollback()

    async def _view(self, cursor, snapshot, query, offset, limit, *, code="", delta=0, clamp=True):

        await cursor.execute("SELECT revision FROM karuta_sketch_library_state WHERE id=1")
        revision = int((await cursor.fetchone())[0])
        where, args = sql_filter(snapshot, query)
        parameters = [snapshot, snapshot, *args]
        await cursor.execute("SELECT COUNT(*) FROM " + EFFECTIVE + " WHERE " + where, parameters)
        count = int((await cursor.fetchone())[0])
        direction = "DESC" if query.descending else "ASC"
        order = f"{COLUMNS[query.order]} {direction},c.code ASC"
        found = True
        if code:
            await cursor.execute(
                "SELECT position FROM (SELECT c.code,ROW_NUMBER() OVER (ORDER BY "
                + order
                + ")-1 AS position FROM "
                + EFFECTIVE
                + " WHERE "
                + where
                + ") ranked WHERE code=%s",
                [*parameters, code],
            )
            anchor = await cursor.fetchone()
            found = anchor is not None
            if found:
                offset = max(0, min(count - 1, int(anchor[0]) + delta))
        if clamp:
            offset = min(offset, max(0, (count - 1) // limit * limit))
        await cursor.execute(
            "SELECT "
            + RECORD_COLUMNS
            + " FROM "
            + EFFECTIVE
            + " JOIN karuta_sketch_library_images i ON i.sha256=c.image_sha WHERE "
            + where
            + " ORDER BY "
            + order
            + " LIMIT %s OFFSET %s",
            [*parameters, limit, offset],
        )
        return LibraryView(
            count, offset, tuple(_record(r) for r in await cursor.fetchall()), revision, found
        )

    async def observe(self, info: CardInfo, *, update_catalogue=True):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:

                await cursor.execute(
                    "SELECT revision FROM karuta_sketch_library_state WHERE id=1 FOR UPDATE"
                )
                await cursor.fetchone()
                await cursor.execute(
                    "SELECT "
                    + INFO_COLUMNS
                    + " FROM karuta_sketch_library_observations WHERE code=%s",
                    (info.code,),
                )
                row = await cursor.fetchone()
                old = _info(row) if row else None
                if old and (
                    info.stamp < old.stamp
                    or (info.stamp == old.stamp and info.signature != old.signature)
                ):
                    await connection.rollback()
                    return False
                await cursor.execute(
                    "REPLACE INTO karuta_sketch_library_observations ("
                    + INFO_COLUMNS
                    + ") VALUES ("
                    + ",".join(["%s"] * 12)
                    + ")",
                    (
                        info.code,
                        info.character,
                        info.series,
                        info.edition,
                        info.print_num,
                        info.sketched,
                        info.ink,
                        *info.stamp,
                        info.image_key,
                    ),
                )
                if update_catalogue:
                    await self._catalogue(cursor, info)
                await connection.commit()
                return True
            except BaseException:
                await connection.rollback()
                raise

    @staticmethod
    async def _catalogue(cursor, info):
        key = identity(info.character, info.series)
        await cursor.execute(
            """INSERT INTO karuta_library_characters
          VALUES (%s,%s,%s,NULL,%s,%s) ON DUPLICATE KEY UPDATE
          character_name=IF(VALUES(last_seen_ms)>last_seen_ms,VALUES(character_name),character_name),
          series_name=IF(VALUES(last_seen_ms)>last_seen_ms,VALUES(series_name),series_name),
          first_seen_ms=LEAST(first_seen_ms,VALUES(first_seen_ms)),
          last_seen_ms=GREATEST(last_seen_ms,VALUES(last_seen_ms))""",
            (key, info.character, info.series, info.created_ms, info.created_ms),
        )

        await cursor.execute(
            """INSERT INTO karuta_library_cards VALUES
          (%s,%s,%s,%s,JSON_OBJECT('sketched',%s),100,%s,%s) ON DUPLICATE KEY UPDATE
          identity_key=IF(VALUES(last_seen_ms)>=last_seen_ms,VALUES(identity_key),identity_key),
          edition=IF(VALUES(last_seen_ms)>=last_seen_ms,VALUES(edition),edition),
          print_num=IF(VALUES(last_seen_ms)>=last_seen_ms,VALUES(print_num),print_num),
          facts_json=IF(VALUES(last_seen_ms)>=last_seen_ms,
            JSON_SET(facts_json,'$.sketched',%s),facts_json),
          confidence=IF(VALUES(last_seen_ms)>=last_seen_ms,GREATEST(confidence,100),confidence),
          first_seen_ms=LEAST(first_seen_ms,VALUES(first_seen_ms)),
          last_seen_ms=GREATEST(last_seen_ms,VALUES(last_seen_ms))""",
            (
                info.code,
                key,
                info.edition,
                info.print_num,
                info.sketched,
                info.created_ms,
                info.created_ms,
                info.sketched,
            ),
        )

    async def is_published(self, info):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT observation_sha FROM karuta_sketch_library_live WHERE code=%s",
                    (info.code,),
                )
                row = await cursor.fetchone()
                return bool(row and row[0] == info.signature)
            finally:
                await connection.rollback()

    async def publish(self, info, record):
        if not _same_card(info, record):
            raise ValueError("sketch observation mismatch")
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT revision FROM karuta_sketch_library_state WHERE id=1 FOR UPDATE"
                )
                await cursor.fetchone()
                await cursor.execute(
                    "SELECT "
                    + INFO_COLUMNS
                    + " FROM karuta_sketch_library_observations WHERE code=%s",
                    (info.code,),
                )
                row = await cursor.fetchone()
                if not row or _info(row) != info:
                    await connection.rollback()
                    return False
                await cursor.execute(
                    "SELECT "
                    + RECORD_COLUMNS
                    + " FROM karuta_sketch_library_live c JOIN karuta_sketch_library_images i"
                    + " ON i.sha256=c.image_sha WHERE c.code=%s",
                    (info.code,),
                )
                previous = await cursor.fetchone()
                changed = previous is None or _record(previous) != record
                await cursor.execute(
                    "INSERT INTO karuta_sketch_library_images VALUES (%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE sha256=VALUES(sha256)",
                    (record.blob.sha256, record.blob.size, record.blob.width, record.blob.height),
                )
                await cursor.execute(
                    """REPLACE INTO karuta_sketch_library_live VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        record.code,
                        record.character,
                        record.series,
                        search_text(record.character),
                        search_text(record.series),
                        record.edition,
                        record.print_num,
                        record.ink,
                        record.blob.sha256,
                        record.source_sha256,
                        info.signature,
                    ),
                )
                if changed:
                    await cursor.execute(
                        "UPDATE karuta_sketch_library_state SET revision=revision+1 WHERE id=1"
                    )
                await connection.commit()
                return True
            except BaseException:
                await connection.rollback()
                raise

    async def import_records(self, snapshot: str, records: list[SketchRecord]) -> None:
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT status,record_count FROM karuta_sketch_library_imports "
                "WHERE snapshot_sha=%s",
                (snapshot,),
            )
            old = await cursor.fetchone()
            if old and old[0] == "complete":
                if int(old[1]) != len(records):
                    raise ValueError("completed import count changed")
                await connection.rollback()
                return
            await cursor.execute(
                """INSERT INTO karuta_sketch_library_imports(snapshot_sha,status,record_count)
              VALUES (%s,'importing',%s)
              ON DUPLICATE KEY UPDATE record_count=VALUES(record_count)""",
                (snapshot, len(records)),
            )
            await connection.commit()
            for start in range(0, len(records), 500):
                batch = records[start : start + 500]
                await cursor.executemany(
                    """INSERT INTO karuta_sketch_library_images VALUES (%s,%s,%s,%s)
                  ON DUPLICATE KEY UPDATE sha256=VALUES(sha256)""",
                    [(r.blob.sha256, r.blob.size, r.blob.width, r.blob.height) for r in batch],
                )
                await cursor.executemany(
                    """INSERT INTO karuta_sketch_library_cards VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE code=VALUES(code)""",
                    [
                        (
                            snapshot,
                            r.code,
                            r.character,
                            r.series,
                            search_text(r.character),
                            search_text(r.series),
                            r.edition,
                            r.print_num,
                            r.ink,
                            r.blob.sha256,
                            r.source_sha256,
                        )
                        for r in batch
                    ],
                )
                await connection.commit()
            await cursor.execute(
                "SELECT COUNT(*) FROM karuta_sketch_library_cards WHERE snapshot_sha=%s",
                (snapshot,),
            )
            if int((await cursor.fetchone())[0]) != len(records):
                raise ValueError("import count differs")

            await cursor.execute(
                """UPDATE karuta_sketch_library_imports SET status='complete',
              completed_at=CURRENT_TIMESTAMP(6) WHERE snapshot_sha=%s""",
                (snapshot,),
            )
            await connection.commit()
