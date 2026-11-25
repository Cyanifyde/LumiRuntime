from __future__ import annotations

import asyncio
import json
import secrets
from datetime import UTC, datetime, timedelta

from .archive import (
    MAX_CHUNK,
    MAX_RECORD,
    MAX_SEGMENT,
    TARGET_CHUNK,
    ArchiveFull,
    ArchiveUnavailable,
)

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS afk_archive_segments (
      name CHAR(37) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      guild_id BIGINT UNSIGNED NOT NULL, bytes BIGINT UNSIGNED NOT NULL,
      sealed BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      KEY ix_afk_segment_writer(guild_id,sealed,created_at)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_archive_chunks (
      id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      segment CHAR(37) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      byte_offset BIGINT UNSIGNED NOT NULL,byte_length INT UNSIGNED NOT NULL,
      raw_size INT UNSIGNED NOT NULL,sha256 CHAR(64) CHARACTER SET ascii NOT NULL,
      record_count INT UNSIGNED NOT NULL,KEY ix_afk_chunk_segment(segment)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_archived_interactions (
      id BIGINT UNSIGNED PRIMARY KEY, guild_id BIGINT UNSIGNED NOT NULL,
      user_id BIGINT UNSIGNED NOT NULL, channel_id BIGINT UNSIGNED NOT NULL,
      message_id BIGINT UNSIGNED NOT NULL, occurred_at DATETIME(6) NOT NULL,
      chunk CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      record_index INT UNSIGNED NOT NULL,
      UNIQUE KEY uq_afk_archive_recipient(guild_id,user_id,message_id),
      KEY ix_afk_archive_window(guild_id,user_id,occurred_at,message_id),
      KEY ix_afk_archive_chunk(chunk),KEY ix_afk_archive_source(guild_id,message_id,user_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_interaction_reads (
      guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
      message_id BIGINT UNSIGNED NOT NULL,read_at DATETIME(6) NOT NULL,
      PRIMARY KEY(guild_id,user_id,message_id),KEY ix_afk_read_time(read_at)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_recap_leases (
      guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
      keep_until DATETIME(6) NOT NULL,PRIMARY KEY(guild_id,user_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_recap_deliveries (
      token CHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      part INT UNSIGNED NOT NULL,guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
      message_ids JSON NOT NULL,created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      PRIMARY KEY(token,part),KEY ix_afk_delivery_pending(created_at)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_return_windows (
      guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
      started_at DATETIME(6) NOT NULL,PRIMARY KEY(guild_id,user_id)
    ) ENGINE=InnoDB""",
)


class ArchiveStore:
    def __init__(self, store, files):
        self.store, self.files = store, files
        self._receipt_after = ""

    async def lease(self, guild, user):
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "INSERT INTO afk_recap_leases VALUES(%s,%s,"
                    "DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 15 MINUTE)) "
                    "ON DUPLICATE KEY UPDATE keep_until=GREATEST(keep_until,VALUES(keep_until))",
                    (guild, user),
                )
                await c.commit()
            except BaseException:
                await c.rollback()
                raise

    async def stage(self, token, guild, user, ids):
        unique = list(dict.fromkeys(ids))
        if not unique:
            return
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.executemany(
                    "INSERT IGNORE INTO afk_recap_deliveries"
                    "(token,part,guild_id,user_id,message_ids) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    [
                        (token, n // 1000, guild, user, json.dumps(unique[n : n + 1000]))
                        for n in range(0, len(unique), 1000)
                    ],
                )
                await c.commit()
            except BaseException:
                await c.rollback()
                raise

    async def stage_sources(self, token, user, interactions):

        grouped = {}
        for item in interactions:
            grouped.setdefault(item.guild_id, set()).add(item.message_id)
        rows = []
        for guild, ids in sorted(grouped.items()):
            ids = sorted(ids)
            for n in range(0, len(ids), 1000):
                rows.append((token, len(rows), guild, user, json.dumps(ids[n : n + 1000])))
        if not rows:
            return
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.executemany(
                    "INSERT IGNORE INTO afk_recap_deliveries"
                    "(token,part,guild_id,user_id,message_ids) VALUES(%s,%s,%s,%s,%s)",
                    rows,
                )
                await c.commit()
            except BaseException:
                await c.rollback()
                raise

    async def pending(self):
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            try:


                await q.execute(
                    "DELETE FROM afk_recap_deliveries WHERE created_at<"
                    "DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 30 MINUTE) LIMIT 128"
                )
                await c.commit()
                await q.execute(
                    "SELECT DISTINCT token FROM afk_recap_deliveries "
                    "WHERE token>%s ORDER BY token LIMIT 64",
                    (self._receipt_after,),
                )
                tokens = [r[0] for r in await q.fetchall()]
                if not tokens and self._receipt_after:
                    await q.execute(
                        "SELECT DISTINCT token FROM afk_recap_deliveries ORDER BY token LIMIT 64"
                    )
                    tokens = [r[0] for r in await q.fetchall()]
                self._receipt_after = tokens[-1] if tokens else ""
                return tokens
            finally:
                await c.rollback()

    async def confirm(self, token):
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            try:
                await c.begin()
                await q.execute(
                    "SELECT guild_id,user_id,message_ids FROM afk_recap_deliveries "
                    "WHERE token=%s FOR UPDATE",
                    (token,),
                )
                rows = await q.fetchall()
                for guild, user, raw in rows:


                    await q.executemany(
                        "INSERT INTO afk_interaction_reads VALUES(%s,%s,%s,UTC_TIMESTAMP(6)) "
                        "ON DUPLICATE KEY UPDATE read_at=read_at",
                        [(guild, user, mid) for mid in json.loads(raw)],
                    )
                await q.execute("DELETE FROM afk_recap_deliveries WHERE token=%s", (token,))
                await c.commit()
                return len(rows)
            except BaseException:
                await c.rollback()
                raise

    async def window(self, guild, user, since):
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT started_at FROM afk_return_windows WHERE guild_id=%s AND user_id=%s",
                    (guild, user),
                )
                row = await q.fetchone()
                return min(since, row[0]) if row else since
            finally:
                await c.rollback()

    async def read(self, guild, user, since, *, unread_before=None):
        if self.files is None:
            raise ArchiveUnavailable("AFK archive storage is not configured")
        parameters = [guild, user, since]
        unread_clause = ""
        if unread_before is not None:
            unread_clause = (
                "AND NOT EXISTS (SELECT 1 FROM afk_interaction_reads rd "
                "WHERE rd.guild_id=i.guild_id AND rd.user_id=i.user_id "
                "AND rd.message_id=i.message_id AND rd.read_at<=%s) "
            )
            cutoff = (
                unread_before
                if unread_before.tzinfo is None
                else unread_before.astimezone(UTC).replace(tzinfo=None)
            )
            parameters.append(cutoff)
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT i.channel_id,i.message_id,i.occurred_at,c.id,c.segment,c.byte_offset,"
                    "c.byte_length,c.raw_size,c.sha256,i.record_index "
                    "FROM afk_archived_interactions i JOIN afk_archive_chunks c ON c.id=i.chunk "
                    "WHERE i.guild_id=%s AND i.user_id=%s AND i.occurred_at>=%s "
                    + unread_clause
                    + "ORDER BY i.occurred_at DESC,i.message_id DESC",
                    tuple(parameters),
                )
                rows = await q.fetchall()
            finally:
                await c.rollback()

        def decode():
            values, last_chunk, items = [], None, []
            for channel, message, occurred, cid, name, offset, size, raw_size, sha, index in rows:
                if cid != last_chunk:
                    items = self.files.read(name, int(offset), int(size), int(raw_size), sha)
                    last_chunk = cid
                if not isinstance(items, list) or not 0 <= index < len(items):
                    raise ArchiveUnavailable("Invalid AFK chunk record index")
                value = list(items[index])
                if len(value) != 13 or (value[0], value[1], value[5], value[7]) != (
                    guild,
                    user,
                    channel,
                    message,
                ):
                    raise ArchiveUnavailable("AFK archive scope mismatch")
                value[11] = datetime.fromisoformat(value[11]).replace(tzinfo=None)
                if value[11] != occurred:
                    raise ArchiveUnavailable("AFK archive timestamp mismatch")
                values.append(self.store._interaction(tuple(value)))
            return values

        return await asyncio.to_thread(decode)

    @staticmethod
    def prepare_chunk(rows, now):
        records, archived, consumed, size = [], [], [], 2
        for row in rows:
            if row[14] is not None and (row[15] is None or row[15] <= now):
                consumed.append(row)
                continue
            value = list(row[1:14])
            value[11] = row[12].isoformat()
            length = len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())
            if length > MAX_RECORD:
                raise ArchiveUnavailable("AFK record exceeds its bound")
            if size + length + 1 > MAX_CHUNK:
                break
            archived.append(row)
            records.append(value)
            size += length + int(len(records) > 1)
            if size >= TARGET_CHUNK:
                break


        if archived and size < TARGET_CHUNK and rows[0][12] > now - timedelta(hours=48, minutes=10):
            records, archived = [], []
        return records, archived, consumed

    async def _writer_lock(self, q):
        await q.execute("SELECT GET_LOCK('lumi:afk:archive',0)")
        return (await q.fetchone())[0] == 1

    async def _writer_release(self, c, q):
        await c.rollback()
        await q.execute("SELECT RELEASE_LOCK('lumi:afk:archive')")
        await q.fetchone()
        await c.rollback()

    @staticmethod
    async def _file_io(operation, *args):


        task = asyncio.create_task(asyncio.to_thread(operation, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await task
            finally:
                raise

    async def move_one(self):

        if self.files is None:
            raise ArchiveUnavailable("AFK archive storage is not configured")
        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            locked = False
            try:
                locked = await self._writer_lock(q)
                if not locked:
                    return False
                await c.begin()
                await q.execute(
                    "SELECT guild_id FROM afk_missed_interactions "
                    "WHERE occurred_at<DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 48 HOUR) "
                    "ORDER BY occurred_at,id LIMIT 1"
                )
                first = await q.fetchone()
                if not first:
                    return False
                await q.execute(
                    "SELECT m.id,m.guild_id,m.user_id,m.author_id,m.author_name,"
                    "m.author_avatar_url,"
                    "m.channel_id,m.channel_name,m.message_id,m.mention_kind,m.content,"
                    "m.entity_labels_json,m.occurred_at,r.original_message_id,"
                    "rd.read_at,l.keep_until "
                    "FROM afk_missed_interactions m LEFT JOIN afk_interaction_replies r "
                    "ON r.guild_id=m.guild_id AND r.user_id=m.user_id "
                    "AND r.channel_id=m.channel_id AND r.message_id=m.message_id "
                    "LEFT JOIN afk_interaction_reads rd ON rd.guild_id=m.guild_id "
                    "AND rd.user_id=m.user_id AND rd.message_id=m.message_id "
                    "LEFT JOIN afk_recap_leases l "
                    "ON l.guild_id=m.guild_id AND l.user_id=m.user_id "
                    "WHERE m.guild_id=%s "
                    "AND m.occurred_at<DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 48 HOUR) "
                    "ORDER BY m.occurred_at,m.id LIMIT 1024 FOR UPDATE",
                    (first[0],),
                )
                rows = await q.fetchall()
                records, archived, consumed = self.prepare_chunk(
                    rows, datetime.now(UTC).replace(tzinfo=None)
                )
                if records:
                    await q.execute(
                        "SELECT name FROM afk_archive_segments WHERE guild_id=%s "
                        "AND sealed=FALSE AND bytes<%s "
                        "ORDER BY created_at DESC LIMIT 1 FOR UPDATE",
                        (first[0], MAX_SEGMENT - MAX_CHUNK - 4096),
                    )
                    segment = await q.fetchone()
                    name = segment[0] if segment else secrets.token_hex(16) + ".lz4c"
                    if segment is None:
                        await q.execute(
                            "INSERT INTO afk_archive_segments(name,guild_id,bytes) VALUES(%s,%s,0)",
                            (name, first[0]),
                        )
                    try:
                        offset, size, raw_size, sha = await self._file_io(
                            self.files.append, name, records
                        )
                    except ArchiveFull:
                        await q.execute(
                            "UPDATE afk_archive_segments SET sealed=TRUE WHERE name=%s", (name,)
                        )
                        await c.commit()
                        return False
                    chunk = secrets.token_hex(16)
                    await q.execute(
                        "INSERT INTO afk_archive_chunks VALUES(%s,%s,%s,%s,%s,%s,%s)",
                        (chunk, name, offset, size, raw_size, sha, len(records)),
                    )
                    await q.executemany(
                        "INSERT INTO afk_archived_interactions VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                        [
                            (r[0], r[1], r[2], r[6], r[8], r[12], chunk, index)
                            for index, r in enumerate(archived)
                        ],
                    )
                    await q.execute(
                        "UPDATE afk_archive_segments SET bytes=%s WHERE name=%s",
                        (offset + size, name),
                    )
                deleting = [r[0] for r in (*archived, *consumed)]
                if not deleting:
                    return False
                for table, alias in (
                    ("afk_interaction_replies", "r"),
                    ("afk_interaction_reads", "rd"),
                ):
                    ids = deleting if alias == "r" else [r[0] for r in consumed]
                    if ids:
                        marks = ",".join(["%s"] * len(ids))
                        await q.execute(
                            f"DELETE {alias} FROM {table} {alias} "
                            "JOIN afk_missed_interactions m "
                            f"ON {alias}.guild_id=m.guild_id AND {alias}.user_id=m.user_id "
                            f"AND {alias}.message_id=m.message_id WHERE m.id IN ({marks})",
                            tuple(ids),
                        )
                marks = ",".join(["%s"] * len(deleting))
                await q.execute(
                    f"DELETE FROM afk_missed_interactions WHERE id IN ({marks})", tuple(deleting)
                )
                await c.commit()
                return True
            except BaseException:
                await c.rollback()
                raise
            finally:
                if locked:
                    await self._writer_release(c, q)
                else:
                    await c.rollback()

    async def cleanup(self):

        async with self.store._require_pool().acquire() as c, c.cursor() as q:
            locked = False
            try:
                locked = await self._writer_lock(q)
                if not locked:
                    return
                await c.begin()
                await q.execute(
                    "SELECT i.id FROM afk_archived_interactions i "
                    "JOIN afk_interaction_reads r ON r.guild_id=i.guild_id "
                    "AND r.user_id=i.user_id AND r.message_id=i.message_id "
                    "LEFT JOIN afk_recap_leases l ON l.guild_id=i.guild_id AND l.user_id=i.user_id "
                    "WHERE l.keep_until IS NULL OR l.keep_until<=UTC_TIMESTAMP(6) "
                    "ORDER BY r.read_at LIMIT 512 FOR UPDATE"
                )
                ids = tuple(r[0] for r in await q.fetchall())
                if ids:
                    marks = ",".join(["%s"] * len(ids))
                    await q.execute(
                        "DELETE r FROM afk_interaction_reads r "
                        "JOIN afk_archived_interactions i ON r.guild_id=i.guild_id "
                        "AND r.user_id=i.user_id AND r.message_id=i.message_id "
                        f"WHERE i.id IN ({marks})",
                        ids,
                    )
                    await q.execute(
                        f"DELETE FROM afk_archived_interactions WHERE id IN ({marks})", ids
                    )
                await q.execute(
                    "SELECT c.id FROM afk_archive_chunks c "
                    "LEFT JOIN afk_archived_interactions i ON i.chunk=c.id "
                    "WHERE i.id IS NULL LIMIT 64 FOR UPDATE"
                )
                empty_chunks = tuple(row[0] for row in await q.fetchall())
                if empty_chunks:
                    marks = ",".join(["%s"] * len(empty_chunks))
                    await q.execute(
                        f"DELETE FROM afk_archive_chunks WHERE id IN ({marks})", empty_chunks
                    )
                await c.commit()
                await q.execute(
                    "SELECT s.name FROM afk_archive_segments s LEFT JOIN afk_archive_chunks c "
                    "ON c.segment=s.name WHERE c.id IS NULL LIMIT 1 FOR UPDATE"
                )
                empty = await q.fetchone()
                if empty and self.files is not None:
                    await self._file_io(self.files.remove, empty[0])
                    await q.execute("DELETE FROM afk_archive_segments WHERE name=%s", (empty[0],))
                await q.execute(
                    "DELETE w FROM afk_return_windows w WHERE NOT EXISTS ("
                    "SELECT 1 FROM afk_missed_interactions m LEFT JOIN afk_interaction_reads r "
                    "ON r.guild_id=m.guild_id AND r.user_id=m.user_id "
                    "AND r.message_id=m.message_id WHERE m.guild_id=w.guild_id "
                    "AND m.user_id=w.user_id AND m.occurred_at>=w.started_at "
                    "AND r.message_id IS NULL) AND NOT EXISTS ("
                    "SELECT 1 FROM afk_archived_interactions i LEFT JOIN afk_interaction_reads r "
                    "ON r.guild_id=i.guild_id AND r.user_id=i.user_id "
                    "AND r.message_id=i.message_id WHERE i.guild_id=w.guild_id "
                    "AND i.user_id=w.user_id AND i.occurred_at>=w.started_at "
                    "AND r.message_id IS NULL)"
                )
                await c.commit()
            except BaseException:
                await c.rollback()
                raise
            finally:
                if locked:
                    await self._writer_release(c, q)
                else:
                    await c.rollback()
