from __future__ import annotations

import asyncio
import copy
import json
import secrets
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlparse

import asyncmy
import msgspec

from lumi.storage.checkpoint import create_pool

from .archive import SCHEMA as ARCHIVE_SCHEMA
from .models import SavedMessage, ShortcutSnapshot, search_text, source_key

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS personal_reaction_users (
    user_id BIGINT UNSIGNED PRIMARY KEY, revision BIGINT UNSIGNED NOT NULL,
    document JSON NOT NULL) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS personal_reaction_outbox (
    user_id BIGINT UNSIGNED NOT NULL,revision BIGINT UNSIGNED NOT NULL,
    document JSON NOT NULL,published BOOLEAN NOT NULL DEFAULT 0,
    PRIMARY KEY(user_id,revision),INDEX pending(published)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS personal_reaction_mutations (
    user_id BIGINT UNSIGNED NOT NULL,author_id BIGINT UNSIGNED NOT NULL,
    activation CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_ms BIGINT UNSIGNED NOT NULL,PRIMARY KEY(user_id,activation),
    INDEX expiry(created_ms)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS personal_reaction_snapshots (
    id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,author_id BIGINT UNSIGNED NOT NULL,
    guild_id BIGINT UNSIGNED NOT NULL,channel_id BIGINT UNSIGNED NOT NULL,
    message_id BIGINT UNSIGNED NOT NULL,kind VARCHAR(8) NOT NULL,
    author_name VARCHAR(256) NOT NULL,search_copy TEXT NOT NULL,card_count INT NOT NULL,
    saved_ms BIGINT UNSIGNED NOT NULL,media_pending BOOLEAN NOT NULL,
    cold BOOLEAN NOT NULL DEFAULT 0,
    document JSON NOT NULL,
    UNIQUE KEY source(kind,user_id,guild_id,message_id),
    INDEX notebook(user_id,kind,saved_ms,id),INDEX media(media_pending),
    INDEX archival(kind,cold,saved_ms)) ENGINE=InnoDB""",
    *ARCHIVE_SCHEMA,
)


class NotesStore:


    async def shortcuts(self, user):
        async with self.transaction(user) as tx:
            return await tx.preferences()

    async def configure(self, user, change):
        async with self.transaction(user) as tx:
            current = await tx.preferences()
            updated = change(current)
            updated = msgspec.structs.replace(updated, revision=current.revision + 1)
            await tx.put_preferences(updated)
            return updated

    async def save(self, user, message: SavedMessage, now, *, kind="note", activation=None):
        if kind == "afk":

            message = msgspec.structs.replace(message, content="", entity_labels=())
        async with self.transaction(user) as tx:
            existing = await tx.source(kind, message.guild_id, message.message_id)
            if activation:
                if await tx.seen(activation):
                    return existing
                await tx.remember(activation, message.author_id, now)
            if existing:
                return existing
            identifier = (
                source_key(kind, user, message.guild_id, message.message_id)
                if kind == "afk"
                else secrets.token_hex(16)
            )
            row = {
                "id": identifier,
                "user_id": user,
                "author_id": message.author_id,
                "guild_id": message.guild_id,
                "channel_id": message.channel_id,
                "message_id": message.message_id,
                "kind": kind,
                "saved_ms": now,
                "media_pending": bool(message.media),
                "document": msgspec.to_builtins(message),
            }
            await tx.put_snapshot(row)
            return row

    async def enrich_afk_media(self, user, message, now):

        async with self.transaction(user) as tx:
            current = await tx.source("afk", message.guild_id, message.message_id)
            if current is None:
                message = msgspec.structs.replace(message, content="", entity_labels=())
                current = {
                    "id": source_key("afk", user, message.guild_id, message.message_id),
                    "user_id": user,
                    "author_id": message.author_id,
                    "guild_id": message.guild_id,
                    "channel_id": message.channel_id,
                    "message_id": message.message_id,
                    "kind": "afk",
                    "saved_ms": now,
                    "media_pending": True,
                    "document": msgspec.to_builtins(message),
                }
                await tx.put_snapshot(current)
                return current
            if (
                current["author_id"] != message.author_id
                or current["channel_id"] != message.channel_id
            ):
                return None
            media = list(current["document"].get("media", ()))
            existing = {item.get("original_url") or item["url"] for item in media}
            additions = [
                msgspec.to_builtins(item)
                for item in message.media
                if (item.original_url or item.url) not in existing
            ]
            if not additions or len(media) + len(additions) > 40:
                return current
            current["document"]["media"] = media + additions
            current["media_pending"] = True
            await tx.put_snapshot(current)
            return current

    async def media_ready(self, row, message):
        async with self.transaction(row["user_id"]) as tx:
            current = await tx.snapshot(row["id"])

            if current is None or not current["media_pending"]:
                return False
            if tuple(current["document"].get("media", ())) != tuple(
                row["document"].get("media", ())
            ):
                return False
            current["document"]["media"] = msgspec.to_builtins(message.media)
            current["media_pending"] = False
            await tx.put_snapshot(current)
            return True

    async def find(self, user, *, query="", authors=(), guild=0):
        rows = await self.snapshots(user, kind="note")
        query = search_text(query)
        return [
            row
            for row in rows
            if (not guild or row["guild_id"] == guild)
            and (not authors or row["author_id"] in authors)
            and (not query or query in search_text(row["document"]["content"]))
        ]

    async def delete(self, user, identifiers):
        count = 0
        async with self.transaction(user) as tx:
            for identifier in set(identifiers):
                row = await tx.snapshot(identifier)
                if row and row["user_id"] == user and row["kind"] == "note":
                    await tx.delete_snapshot(identifier)
                    count += 1
        return count

    async def card_page(self, user, *, query="", authors=(), page=0):
        rows = await self.find(user, query=query, authors=authors)
        cards = [
            (row, start)
            for row in rows
            for start in range(0, max(1, len(row["document"].get("media", ()))), 10)
        ]
        return cards[page * 5 : page * 5 + 5], len(cards)

    async def author_page(self, user, *, query="", page=0):
        rows = await self.find(user)
        people = {}
        for row in rows:
            name = row["document"]["author_name"]
            if (
                not query
                or search_text(query) in search_text(name)
                or query == str(row["author_id"])
            ):
                people.setdefault(row["author_id"], name)
        values = sorted(people.items(), key=lambda item: (search_text(item[1]), item[0]))
        return values[page * 25 : page * 25 + 25], len(values)

    async def matching_ids(self, user, *, query="", before=None, guild=0):
        return [
            row["id"]
            for row in await self.find(user, query=query, guild=guild)
            if before is None or row["saved_ms"] < before
        ]

    async def cleanup_receipts(self, now):
        raise NotImplementedError

    async def afk_counts(self, user, guild, message_ids):
        result = {}
        for message in message_ids:
            row = await self.snapshot(source_key("afk", user, guild, message))
            if row:
                result[message] = max(1, (len(row["document"].get("media", ())) + 9) // 10)
        return result

    async def afk_messages(self, user, guild, message_ids):
        result = {}
        for message in message_ids:
            row = await self.snapshot(source_key("afk", user, guild, message))
            if row:
                result[message] = msgspec.convert(row["document"], type=SavedMessage)
        return result


class MemoryTransaction:
    def __init__(self, store, user):
        self.store, self.user = store, user

    async def preferences(self):
        return self.store.preferences.get(self.user, ShortcutSnapshot(self.user))

    async def put_preferences(self, snapshot):
        self.store.preferences[self.user] = snapshot
        self.store.outbox[self.user, snapshot.revision] = snapshot

    async def snapshot(self, identifier):
        return copy.deepcopy(self.store.rows.get(identifier))

    async def source(self, kind, guild, message):
        return next(
            (
                copy.deepcopy(row)
                for row in self.store.rows.values()
                if (row["kind"], row["user_id"], row["guild_id"], row["message_id"])
                == (kind, self.user, guild, message)
            ),
            None,
        )

    async def seen(self, activation):
        return (self.user, activation) in self.store.receipts

    async def remember(self, activation, author, now):
        self.store.receipts[self.user, activation] = (author, now)

    async def put_snapshot(self, row):
        self.store.rows[row["id"]] = copy.deepcopy(row)

    async def delete_snapshot(self, identifier):
        self.store.rows.pop(identifier, None)


class MemoryNotesStore(NotesStore):
    def __init__(self, *, archive_root=None):
        self.preferences, self.rows, self.outbox = {}, {}, {}
        self.receipts = {}
        self.lock = asyncio.Lock()
        from .archive import MemoryNotesArchive

        self.archive = MemoryNotesArchive(self, archive_root) if archive_root else None

    @asynccontextmanager
    async def transaction(self, user):
        async with self.lock:
            backup = copy.deepcopy((self.preferences, self.rows, self.outbox, self.receipts))
            try:
                yield MemoryTransaction(self, user)
            except BaseException:
                self.preferences, self.rows, self.outbox, self.receipts = backup
                raise

    async def snapshots(self, user, *, kind="note"):
        rows = sorted(
            (
                copy.deepcopy(r)
                for r in self.rows.values()
                if r["user_id"] == user and r["kind"] == kind
            ),
            key=lambda r: (r["saved_ms"], r["id"]),
            reverse=True,
        )
        if self.archive:
            cache = {}
            return [await self.archive.hydrate(row, cache=cache) for row in rows]
        return rows

    async def snapshot(self, identifier):
        row = copy.deepcopy(self.rows.get(identifier))
        return await self.archive.hydrate(row) if self.archive else row

    async def delete(self, user, identifiers):
        if self.archive:
            return await self.archive.delete(user, identifiers)
        return await super().delete(user, identifiers)

    async def pending_media(self, limit=8):
        return [copy.deepcopy(r) for r in self.rows.values() if r["media_pending"]][:limit]

    async def publications(self, *, all_users=False):
        return list(self.preferences.values() if all_users else self.outbox.values())

    async def published(self, snapshot):
        self.outbox.pop((snapshot.user_id, snapshot.revision), None)

    async def cleanup_receipts(self, now):
        async with self.lock:
            self.receipts = {
                key: row for key, row in self.receipts.items() if row[1] > now - 30 * 86_400_000
            }

    async def close(self):
        pass


class SQLTransaction:
    def __init__(self, cursor, user):
        self.q, self.user = cursor, user

    async def preferences(self):
        await self.q.execute(
            "SELECT document FROM personal_reaction_users WHERE user_id=%s", (self.user,)
        )
        row = await self.q.fetchone()
        return msgspec.json.decode(row[0], type=ShortcutSnapshot)

    async def put_preferences(self, snapshot):
        payload = msgspec.json.encode(snapshot).decode()
        await self.q.execute(
            "UPDATE personal_reaction_users SET revision=%s,document=%s WHERE user_id=%s",
            (snapshot.revision, payload, self.user),
        )
        await self.q.execute(
            "INSERT INTO personal_reaction_outbox(user_id,revision,document) VALUES(%s,%s,%s)",
            (self.user, snapshot.revision, payload),
        )

    async def snapshot(self, identifier):
        await self.q.execute(
            "SELECT document FROM personal_reaction_snapshots "
            "WHERE id=%s AND user_id=%s FOR UPDATE",
            (identifier, self.user),
        )
        row = await self.q.fetchone()
        return json.loads(row[0]) if row else None

    async def source(self, kind, guild, message):
        await self.q.execute(
            "SELECT document FROM personal_reaction_snapshots "
            "WHERE kind=%s AND user_id=%s AND guild_id=%s AND message_id=%s FOR UPDATE",
            (kind, self.user, guild, message),
        )
        row = await self.q.fetchone()
        return json.loads(row[0]) if row else None

    async def seen(self, activation):
        await self.q.execute(
            "SELECT 1 FROM personal_reaction_mutations WHERE user_id=%s AND activation=%s",
            (self.user, activation),
        )
        return bool(await self.q.fetchone())

    async def remember(self, activation, author, now):
        await self.q.execute(
            "INSERT INTO personal_reaction_mutations VALUES(%s,%s,%s,%s)",
            (self.user, author, activation, now),
        )

    async def put_snapshot(self, row):
        await self.q.execute(
            """INSERT INTO personal_reaction_snapshots
            (id,user_id,author_id,guild_id,channel_id,message_id,kind,saved_ms,media_pending,
             author_name,search_copy,card_count,cold,document)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
            media_pending=VALUES(media_pending),search_copy=VALUES(search_copy),
            cold=VALUES(cold),document=VALUES(document)""",
            (
                *[
                    row[k]
                    for k in (
                        "id",
                        "user_id",
                        "author_id",
                        "guild_id",
                        "channel_id",
                        "message_id",
                        "kind",
                        "saved_ms",
                        "media_pending",
                    )
                ],
                row["document"]["author_name"],
                search_text(row["document"]["content"]),
                max(1, (len(row["document"].get("media", ())) + 9) // 10),
                bool(row.get("cold")),
                json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            ),
        )

    async def delete_snapshot(self, identifier):
        await self.q.execute(
            "DELETE FROM personal_reaction_snapshots WHERE id=%s AND user_id=%s AND kind='note'",
            (identifier, self.user),
        )


class MySQLNotesStore(NotesStore):
    def __init__(self, dsn, *, archive_root=None):
        parsed = urlparse(dsn)
        self.settings = dict(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            db=parsed.path.removeprefix("/"),
            autocommit=False,
            minsize=1,
            maxsize=6,
            charset="utf8mb4",
        )
        self.pool = None
        from .archive import NotesArchive

        self.archive = NotesArchive(self, archive_root) if archive_root else None

    async def connect(self):
        self.pool = await create_pool(**self.settings)

    async def migrate(self):
        async with self.pool.acquire() as c, c.cursor() as q:
            for sql in SCHEMA:
                await q.execute(sql)
            await c.commit()

    @asynccontextmanager
    async def transaction(self, user):
        async with self.pool.acquire() as c, c.cursor() as q:
            try:
                await c.begin()
                await q.execute(
                    "INSERT IGNORE INTO personal_reaction_users(user_id,revision,document) "
                    "VALUES(%s,0,%s)",
                    (user, msgspec.json.encode(ShortcutSnapshot(user)).decode()),
                )
                await q.execute(
                    "SELECT revision FROM personal_reaction_users WHERE user_id=%s FOR UPDATE",
                    (user,),
                )
                await q.fetchone()
                yield SQLTransaction(q, user)
                await c.commit()
            except BaseException:
                await c.rollback()
                raise

    async def _read(self, sql, args=()):
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute(sql, args)
            rows = await q.fetchall()
            await c.rollback()
            return rows

    async def snapshots(self, user, *, kind="note"):
        rows = await self._read(
            "SELECT document FROM personal_reaction_snapshots "
            "WHERE user_id=%s AND kind=%s ORDER BY saved_ms DESC,id DESC",
            (user, kind),
        )
        result, cache = [], {}
        for (raw,) in rows:
            row = json.loads(raw)
            result.append(await self.archive.hydrate(row, cache=cache) if self.archive else row)
        return result

    @staticmethod
    def _filter(user, query="", authors=(), guild=0, before=None):
        sql, args = "user_id=%s AND kind='note'", [user]
        if query:
            sql += " AND (LOCATE(%s,search_copy)>0 OR cold=1)"
            args.append(search_text(query))
        if authors:
            sql += " AND author_id IN (" + ",".join(["%s"] * len(authors)) + ")"
            args.extend(authors)
        if guild:
            sql += " AND guild_id=%s"
            args.append(guild)
        if before is not None:
            sql += " AND saved_ms<%s"
            args.append(before)
        return sql, args

    async def card_page(self, user, *, query="", authors=(), page=0):
        where, args = self._filter(user, query, authors)
        if query:
            positions = await self._matching_index(user, query=query, authors=authors)
            total = sum(row[1] for row in positions)
            selected, cursor = [], 0
            for identifier, count in positions:
                if cursor + count > page * 5 and cursor < page * 5 + 5:
                    note = await self.snapshot(identifier)
                    if note:
                        selected.extend(
                            (note, index * 10)
                            for index in range(
                                max(0, page * 5 - cursor), min(count, page * 5 + 5 - cursor)
                            )
                        )
                cursor += count
            return selected, total
        total = (
            await self._read(
                "SELECT COALESCE(SUM(card_count),0) FROM personal_reaction_snapshots "
                f"WHERE {where}",
                args,
            )
        )[0][0]

        positions = await self._read(
            f"""SELECT id,card_count,end_card FROM (
            SELECT id,card_count,SUM(card_count) OVER (ORDER BY saved_ms DESC,id DESC) end_card
            FROM personal_reaction_snapshots WHERE {where}) positions
            WHERE end_card>%s AND end_card-card_count<%s ORDER BY end_card LIMIT 5""",
            (*args, page * 5, page * 5 + 5),
        )
        cards = []
        for identifier, count, end in positions:
            row = await self.snapshot(identifier)
            if row is None:
                continue
            start = int(end) - count
            for index in range(max(0, page * 5 - start), min(count, page * 5 + 5 - start)):
                cards.append((row, index * 10))
        return cards, int(total)

    async def author_page(self, user, *, query="", page=0):
        where, args = self._filter(user)
        if query:
            where += " AND (LOCATE(%s,LOWER(author_name))>0 OR CAST(author_id AS CHAR)=%s)"
            args.extend((search_text(query), query))
        total = (
            await self._read(
                f"SELECT COUNT(DISTINCT author_id) FROM personal_reaction_snapshots WHERE {where}",
                args,
            )
        )[0][0]
        rows = await self._read(
            "SELECT author_id,MAX(author_name) name FROM personal_reaction_snapshots "
            f"WHERE {where} "
            "GROUP BY author_id ORDER BY name,author_id LIMIT 25 OFFSET %s",
            (*args, page * 25),
        )
        return list(rows), total

    async def matching_ids(self, user, *, query="", before=None, guild=0):
        if query:
            return [
                row[0]
                for row in await self._matching_index(user, query=query, before=before, guild=guild)
            ]
        where, args = self._filter(user, query, guild=guild, before=before)
        return [
            row[0]
            for row in await self._read(
                f"SELECT id FROM personal_reaction_snapshots WHERE {where}", args
            )
        ]

    async def delete(self, user, identifiers):
        if self.archive:
            return await self.archive.delete(user, identifiers)
        ids, count = sorted(set(identifiers)), 0
        async with self.transaction(user) as tx:
            for offset in range(0, len(ids), 500):
                selected = ids[offset : offset + 500]
                await tx.q.execute(
                    "DELETE FROM personal_reaction_snapshots "
                    "WHERE user_id=%s AND kind='note' AND id IN ("
                    + ",".join(["%s"] * len(selected))
                    + ")",
                    (user, *selected),
                )
                count += tx.q.rowcount
        return count

    async def snapshot(self, identifier):
        rows = await self._read(
            "SELECT document FROM personal_reaction_snapshots WHERE id=%s", (identifier,)
        )
        row = json.loads(rows[0][0]) if rows else None
        return await self.archive.hydrate(row) if self.archive else row

    async def _matching_index(self, user, *, query="", authors=(), guild=0, before=None):
        where, args = self._filter(user, query, authors, guild, before)
        rows = await self._read(
            f"SELECT id,card_count,cold FROM personal_reaction_snapshots WHERE {where} "
            "ORDER BY saved_ms DESC,id DESC",
            args,
        )
        cold_matches = (
            await self.archive.matching_ids(
                [identifier for identifier, _, cold in rows if cold], query
            )
            if query and self.archive
            else set()
        )
        return [
            (identifier, count)
            for identifier, count, cold in rows
            if not cold or not query or identifier in cold_matches
        ]

    async def afk_counts(self, user, guild, message_ids):
        if not message_ids:
            return {}
        result = {}
        ids = list(set(message_ids))
        for offset in range(0, len(ids), 500):
            batch = ids[offset : offset + 500]
            rows = await self._read(
                "SELECT message_id,card_count FROM personal_reaction_snapshots "
                "WHERE kind='afk' AND user_id=%s AND guild_id=%s AND message_id IN ("
                + ",".join(["%s"] * len(batch))
                + ")",
                (user, guild, *batch),
            )
            result.update(rows)
        return result

    async def prune_afk(self):
        async with self.pool.acquire() as connection, connection.cursor() as q:
            await q.execute("""DELETE s FROM personal_reaction_snapshots s WHERE s.kind='afk'
                AND NOT EXISTS(SELECT 1 FROM afk_missed_interactions a
                    WHERE a.guild_id=s.guild_id AND a.user_id=s.user_id
                    AND a.message_id=s.message_id)
                AND NOT EXISTS(SELECT 1 FROM afk_archived_interactions a
                    WHERE a.guild_id=s.guild_id AND a.user_id=s.user_id
                    AND a.message_id=s.message_id)""")
            await connection.commit()

    async def pending_media(self, limit=8):
        return [
            json.loads(r[0])
            for r in await self._read(
                "SELECT document FROM personal_reaction_snapshots WHERE media_pending=1 LIMIT %s",
                (limit,),
            )
        ]

    async def publications(self, *, all_users=False):
        table = (
            "personal_reaction_users" if all_users else "personal_reaction_outbox WHERE published=0"
        )
        return [
            msgspec.json.decode(r[0], type=ShortcutSnapshot)
            for r in await self._read(f"SELECT document FROM {table}")
        ]

    async def published(self, snapshot):
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute(
                "DELETE FROM personal_reaction_outbox WHERE user_id=%s AND revision<=%s",
                (snapshot.user_id, snapshot.revision),
            )
            await c.commit()

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def cleanup_receipts(self, now):
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute(
                "DELETE FROM personal_reaction_mutations WHERE created_ms<=%s LIMIT 5000",
                (now - 30 * 86_400_000,),
            )
            await c.commit()
