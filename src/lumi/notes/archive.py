from __future__ import annotations

import asyncio
import json
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from lumi.afk.archive import MAX_RECORD, SEGMENT, TARGET_CHUNK, ArchiveFiles, ArchiveFull

HOT_MS = 48 * 3_600_000
SCHEMA = (
    """CREATE TABLE IF NOT EXISTS personal_notes_archive_lock (
    id TINYINT PRIMARY KEY) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS personal_notes_archive_chunks (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,segment CHAR(38) NOT NULL,
    byte_offset BIGINT UNSIGNED NOT NULL,byte_length INT NOT NULL,
    raw_size INT NOT NULL,sha256 CHAR(64) NOT NULL,record_count INT NOT NULL,
    INDEX segments(segment)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS personal_notes_archive_entries (
    note_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,author_id BIGINT UNSIGNED NOT NULL,
    chunk_id BIGINT UNSIGNED NOT NULL,record_index SMALLINT NOT NULL,
    INDEX chunks(chunk_id),INDEX owner(user_id),INDEX author(author_id)) ENGINE=InnoDB""",
)


def chunks(records):
    batch, size = [], 2
    for record in records:
        length = len(json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()) + 1
        if length > MAX_RECORD:
            raise ValueError("Note text exceeds its archive bound.")
        if batch and size + length > TARGET_CHUNK:
            yield batch
            batch, size = [], 2
        batch.append(record)
        size += length
    if batch:
        yield batch


async def file_io(function, *args):

    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def check_root(files):
    files.root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if files.root.is_symlink() or not files.root.is_dir():
        raise ValueError("Unsafe notes archive.")


def archive_names(files):
    check_root(files)
    names = set()
    for path in files.root.iterdir():
        if path.is_symlink() or not path.is_file() or not SEGMENT.fullmatch(path.name):
            raise ValueError("Unclassified notes archive file.")
        names.add(path.name)
    return names


class NotesArchive:
    def __init__(self, store, root: Path):
        self.store, self.files = store, ArchiveFiles(root)

    @asynccontextmanager
    async def transaction(self):
        check_root(self.files)
        async with self.store.pool.acquire() as connection, connection.cursor() as q:
            try:
                await connection.begin()
                await q.execute(
                    "INSERT INTO personal_notes_archive_lock VALUES(1) "
                    "ON DUPLICATE KEY UPDATE id=personal_notes_archive_lock.id"
                )
                await q.execute("SELECT id FROM personal_notes_archive_lock WHERE id=1 FOR UPDATE")
                await q.fetchone()
                yield q
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise

    async def _chunk(self, q, records, segment):
        if not segment:
            segment = secrets.token_hex(16) + ".lz4c"
        try:
            offset, length, raw, digest = await file_io(self.files.append, segment, records)
        except ArchiveFull:
            segment = secrets.token_hex(16) + ".lz4c"
            offset, length, raw, digest = await file_io(self.files.append, segment, records)
        await q.execute(
            "INSERT INTO personal_notes_archive_chunks"
            "(segment,byte_offset,byte_length,raw_size,sha256,record_count) "
            "VALUES(%s,%s,%s,%s,%s,%s)",
            (segment, offset, length, raw, digest, len(records)),
        )
        identifier = q.lastrowid
        for index, record in enumerate(records):
            await q.execute(
                "INSERT INTO personal_notes_archive_entries VALUES(%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE chunk_id=VALUES(chunk_id),"
                "record_index=VALUES(record_index)",
                (record["id"], record["user_id"], record["author_id"], identifier, index),
            )
        return segment

    async def _append(self, q, records):

        await q.execute(
            "SELECT id,segment,byte_offset,byte_length,raw_size,sha256 "
            "FROM personal_notes_archive_chunks ORDER BY id DESC LIMIT 1"
        )
        tail = await q.fetchone()
        segment = tail[1] if tail else None
        if tail and tail[4] < TARGET_CHUNK:
            previous = await file_io(self.files.read, *tail[1:])
            records = [*previous, *records]
            await q.execute(
                "DELETE FROM personal_notes_archive_entries WHERE chunk_id=%s", (tail[0],)
            )
            await q.execute("DELETE FROM personal_notes_archive_chunks WHERE id=%s", (tail[0],))

            segment = None
        for batch in chunks(records):
            segment = await self._chunk(q, batch, segment)

    async def archive_old(self, now, limit=1000):
        from .store import SQLTransaction

        count = 0
        async with self.transaction() as q:
            await q.execute(
                "SELECT document FROM personal_reaction_snapshots WHERE kind='note' "
                "AND cold=0 AND saved_ms<%s ORDER BY saved_ms,id LIMIT %s FOR UPDATE",
                (now - HOT_MS, limit),
            )
            rows = [json.loads(row[0]) for row in await q.fetchall()]
            if not rows:
                return 0
            records = [
                {
                    "id": row["id"],
                    "user_id": row["user_id"],
                    "author_id": row["author_id"],
                    "content": row["document"]["content"],
                    "entity_labels": row["document"].get("entity_labels", []),
                }
                for row in rows
            ]
            await self._append(q, records)
            for row in rows:
                row["document"]["content"] = ""
                row["document"]["entity_labels"] = []
                row["cold"] = True
                await SQLTransaction(q, row["user_id"]).put_snapshot(row)
                count += 1
        await self.collect_orphans()
        return count

    async def text(self, identifier, *, cache=None):

        async with self.transaction() as q:
            await q.execute(
                "SELECT c.id,c.segment,c.byte_offset,c.byte_length,c.raw_size,c.sha256,"
                "e.record_index FROM personal_notes_archive_entries e "
                "JOIN personal_notes_archive_chunks c ON c.id=e.chunk_id WHERE e.note_id=%s",
                (identifier,),
            )
            row = await q.fetchone()
            if not row:
                raise ValueError("Archived note is unavailable.")
            chunk, segment, offset, length, raw, digest, index = row
            records = cache.get(chunk) if cache is not None else None
            if records is None:
                records = await file_io(self.files.read, segment, offset, length, raw, digest)
                if cache is not None:
                    cache.clear()
                    cache[chunk] = records
        record = records[index]
        if record["id"] != identifier:
            raise ValueError("Archived note identity mismatch.")
        return record

    async def hydrate(self, row, *, cache=None):
        if row and row.get("cold"):
            text = await self.text(row["id"], cache=cache)
            row["document"].update(content=text["content"], entity_labels=text["entity_labels"])
        return row

    async def matching_ids(self, identifiers, query):
        from .models import search_text

        matched, cache = set(), {}
        query = search_text(query)


        for start in range(0, len(identifiers), 500):
            batch = identifiers[start : start + 500]
            if not batch:
                continue
            async with self.transaction() as q:
                await q.execute(
                    "SELECT e.note_id,e.record_index,c.id,c.segment,c.byte_offset,c.byte_length,"
                    "c.raw_size,c.sha256 FROM personal_notes_archive_entries e "
                    "JOIN personal_notes_archive_chunks c ON c.id=e.chunk_id "
                    "WHERE e.note_id IN (" + ",".join(["%s"] * len(batch)) + ") "
                    "ORDER BY c.id,e.record_index",
                    batch,
                )
                for (
                    identifier,
                    index,
                    chunk,
                    segment,
                    offset,
                    length,
                    raw,
                    digest,
                ) in await q.fetchall():
                    if chunk not in cache:
                        cache.clear()
                        cache[chunk] = await file_io(
                            self.files.read, segment, offset, length, raw, digest
                        )
                    record = cache[chunk][index]
                    if record["id"] != identifier:
                        raise ValueError("Archived note identity mismatch.")
                    if query in search_text(record["content"]):
                        matched.add(identifier)
        return matched

    async def _rewrite(self, q, segments, transform):
        obsolete, count = [], 0
        for segment in sorted(set(segments)):
            await q.execute(
                "SELECT id,byte_offset,byte_length,raw_size,sha256 "
                "FROM personal_notes_archive_chunks "
                "WHERE segment=%s ORDER BY byte_offset",
                (segment,),
            )
            entries = await q.fetchall()
            new_segment = None
            for chunk, offset, length, raw, digest in entries:
                records = await file_io(self.files.read, segment, offset, length, raw, digest)
                kept = []
                for record in records:
                    updated = transform(record)
                    count += int(updated != record)
                    if updated is not None:
                        kept.append(updated)
                await q.execute(
                    "DELETE FROM personal_notes_archive_entries WHERE chunk_id=%s", (chunk,)
                )
                await q.execute("DELETE FROM personal_notes_archive_chunks WHERE id=%s", (chunk,))
                for batch in chunks(kept):
                    new_segment = await self._chunk(q, batch, new_segment)
            obsolete.append(segment)
        return obsolete, count

    async def delete(self, user, identifiers):
        selected, count, obsolete = set(identifiers), 0, []
        if not selected:
            return 0
        async with self.transaction() as q:
            owned = set()
            segments = set()
            for offset in range(0, len(selected), 500):
                batch = sorted(selected)[offset : offset + 500]
                slots = ",".join(["%s"] * len(batch))
                await q.execute(
                    "SELECT id FROM personal_reaction_snapshots WHERE user_id=%s "
                    f"AND kind='note' AND id IN ({slots}) FOR UPDATE",
                    (user, *batch),
                )
                owned.update(row[0] for row in await q.fetchall())
                await q.execute(
                    "SELECT DISTINCT c.segment FROM personal_notes_archive_entries e "
                    "JOIN personal_notes_archive_chunks c ON c.id=e.chunk_id "
                    f"WHERE e.user_id=%s AND e.note_id IN ({slots})",
                    (user, *batch),
                )
                segments.update(row[0] for row in await q.fetchall())
            obsolete, _ = await self._rewrite(
                q, segments, lambda record: None if record["id"] in owned else record
            )
            for offset in range(0, len(owned), 500):
                batch = sorted(owned)[offset : offset + 500]
                await q.execute(
                    "DELETE FROM personal_reaction_snapshots "
                    "WHERE user_id=%s AND kind='note' AND id IN ("
                    + ",".join(["%s"] * len(batch))
                    + ")",
                    (user, *batch),
                )
                count += q.rowcount
        for segment in obsolete:
            self.files.remove(segment)
        return count

    async def collect_orphans(self):

        async with self.transaction() as q:
            candidates = archive_names(self.files)
            await q.execute(
                "SELECT segment,MAX(byte_offset+byte_length),SUM(byte_length) "
                "FROM personal_notes_archive_chunks GROUP BY segment"
            )
            referenced = {row[0]: (int(row[1]), int(row[2])) for row in await q.fetchall()}
            repairs = []
            for segment, (end, covered) in referenced.items():
                path = self.files.path(segment)
                if path.stat().st_size != end or end != covered:
                    repairs.append(segment)
            await self._rewrite(q, repairs, lambda record: record)
            await q.execute("SELECT DISTINCT segment FROM personal_notes_archive_chunks")
            live = {row[0] for row in await q.fetchall()}
        if self.files.root.exists():
            for segment in candidates - live:
                self.files.remove(segment)


class MemoryNotesArchive:


    def __init__(self, store, root):
        self.store, self.files, self.index = store, ArchiveFiles(root), {}

    async def archive_old(self, now, limit=1000):
        async with self.store.lock:
            check_root(self.files)
            rows = [
                row
                for row in self.store.rows.values()
                if row["kind"] == "note" and not row.get("cold") and row["saved_ms"] < now - HOT_MS
            ][:limit]
            records = [
                {
                    "id": row["id"],
                    "user_id": row["user_id"],
                    "author_id": row["author_id"],
                    "content": row["document"]["content"],
                    "entity_labels": row["document"].get("entity_labels", []),
                }
                for row in rows
            ]
            if not records:
                return 0

            if self.index:
                tail = next(reversed(self.index.values()))
                if tail[3] < TARGET_CHUNK:
                    previous = await file_io(self.files.read, *tail[:5])
                    records = [*previous, *records]
                    for record in previous:
                        self.index.pop(record["id"])
                    self.files.remove(tail[0])
            for batch in chunks(records):
                segment = secrets.token_hex(16) + ".lz4c"
                details = await file_io(self.files.append, segment, batch)
                for index, record in enumerate(batch):
                    self.index[record["id"]] = (segment, *details, index)
            for row in rows:
                row["document"].update(content="", entity_labels=[])
                row["cold"] = True
            return len(rows)

    async def text(self, identifier, *, cache=None):
        segment, offset, length, raw, digest, index = self.index[identifier]
        records = cache.get(segment) if cache is not None else None
        if records is None:
            records = await file_io(self.files.read, segment, offset, length, raw, digest)
            if cache is not None:
                cache.clear()
                cache[segment] = records
        return records[index]

    async def hydrate(self, row, *, cache=None):
        if row and row.get("cold"):
            record = await self.text(row["id"], cache=cache)
            row["document"].update(content=record["content"], entity_labels=record["entity_labels"])
        return row

    async def delete(self, user, identifiers):
        async with self.store.lock:
            owned = {
                identifier
                for identifier in identifiers
                if identifier in self.store.rows
                and self.store.rows[identifier]["user_id"] == user
                and self.store.rows[identifier]["kind"] == "note"
            }
            affected = {self.index[i][0] for i in owned if i in self.index}
            for segment in affected:
                identifiers_here = [i for i, entry in self.index.items() if entry[0] == segment]
                records = [await self.text(i) for i in identifiers_here if i not in owned]
                for identifier in identifiers_here:
                    self.index.pop(identifier)
                for batch in chunks(records):
                    new = secrets.token_hex(16) + ".lz4c"
                    details = await file_io(self.files.append, new, batch)
                    for index, record in enumerate(batch):
                        self.index[record["id"]] = (new, *details, index)
                self.files.remove(segment)
            for identifier in owned:
                self.store.rows.pop(identifier)
            return len(owned)

    async def collect_orphans(self):
        async with self.store.lock:
            for name in archive_names(self.files) - {entry[0] for entry in self.index.values()}:
                self.files.remove(name)
