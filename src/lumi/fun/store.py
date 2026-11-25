from __future__ import annotations

import hashlib

import msgspec

from lumi.contracts import decode, encode
from lumi.sofi.store import MySQLSofiStore

from .catalogue import Catalogue, Mutation, mutate, starter
from .media import ResolvedGif, validate_media

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS fun_gif_media (
        source_url VARCHAR(350) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
        document JSON NOT NULL,
        resolved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS fun_catalogues (
        guild_id BIGINT UNSIGNED PRIMARY KEY, revision BIGINT UNSIGNED NOT NULL,
        document JSON NOT NULL, updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS fun_mutations (
        guild_id BIGINT UNSIGNED NOT NULL, mutation_id VARCHAR(64) NOT NULL,
        actor_id BIGINT UNSIGNED NOT NULL, fingerprint CHAR(64) NOT NULL, result JSON NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (guild_id, mutation_id),
        KEY ix_fun_mutation_expiry (created_at)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS fun_catalogue_outbox (
        guild_id BIGINT UNSIGNED NOT NULL, revision BIGINT UNSIGNED NOT NULL,
        payload JSON NOT NULL, published BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (guild_id, revision), KEY ix_fun_outbox (published, guild_id)) ENGINE=InnoDB""",
)


class FunStore(MySQLSofiStore):


    async def migrate(self) -> None:
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            for statement in SCHEMA:
                await cursor.execute(statement)
            await connection.commit()

    async def read_media(self, source: str) -> ResolvedGif | None:
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM fun_gif_media WHERE source_url=%s", (source,)
                )
                row = await cursor.fetchone()
                return validate_media(decode(row[0], ResolvedGif), source) if row else None
            finally:
                await connection.rollback()

    async def save_media(self, item: ResolvedGif) -> ResolvedGif:
        validate_media(item, item.source_url)
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "INSERT IGNORE INTO fun_gif_media (source_url,document) VALUES (%s,%s)",
                    (item.source_url, encode(item).decode()),
                )
                await cursor.execute(
                    "SELECT document FROM fun_gif_media WHERE source_url=%s FOR UPDATE",
                    (item.source_url,),
                )
                saved = validate_media(
                    decode((await cursor.fetchone())[0], ResolvedGif), item.source_url
                )
                await connection.commit()
                return saved
            except BaseException:
                await connection.rollback()
                raise

    async def _outbox(self, cursor, guild: int, state: Catalogue) -> None:
        await cursor.execute(
            "INSERT IGNORE INTO fun_catalogue_outbox "
            "(guild_id, revision, payload) VALUES (%s,%s,%s)",
            (guild, state.revision, encode(state.snapshot(guild)).decode()),
        )

    async def read(self, guild: int) -> Catalogue:
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM fun_catalogues WHERE guild_id=%s", (guild,)
                )
                row = await cursor.fetchone()
                if row:
                    await connection.rollback()
                    return decode(row[0], Catalogue)

                await connection.rollback()
                initial = starter()
                await cursor.execute(
                    "INSERT IGNORE INTO fun_catalogues "
                    "(guild_id,revision,document) VALUES (%s,0,%s)",
                    (guild, encode(initial).decode()),
                )
                inserted = cursor.rowcount
                await cursor.execute(
                    "SELECT document FROM fun_catalogues WHERE guild_id=%s", (guild,)
                )
                state = decode((await cursor.fetchone())[0], Catalogue)
                if inserted:
                    await self._outbox(cursor, guild, state)
                await connection.commit()
                return state
            except BaseException:
                await connection.rollback()
                raise

    async def change(
        self, guild: int, key: str, change: Mutation, *, moderator: bool, conflicts: frozenset[str]
    ) -> tuple[Catalogue, str]:
        fingerprint = hashlib.sha256(
            msgspec.json.encode(
                msgspec.to_builtins(
                    dict(
                        kind=change.kind,
                        actor=change.actor,
                        revision=change.revision,
                        category=change.category,
                        gif=change.gif,
                        name=change.name,
                        mode=change.mode,
                        urls=change.urls,
                    )
                )
            )
        ).hexdigest()
        await self.read(guild)
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:


                await cursor.execute(
                    "SELECT document FROM fun_catalogues WHERE guild_id=%s FOR UPDATE", (guild,)
                )
                state = decode((await cursor.fetchone())[0], Catalogue)
                await cursor.execute(
                    "SELECT actor_id,fingerprint,result FROM fun_mutations "
                    "WHERE guild_id=%s AND mutation_id=%s",
                    (guild, key),
                )
                receipt = await cursor.fetchone()
                if receipt:
                    if receipt[0] != change.actor or receipt[1] != fingerprint:
                        raise ValueError("Invalid submission.")
                    notice = msgspec.json.decode(receipt[2])["notice"]
                else:
                    previous = state.revision
                    state, notice = mutate(state, change, moderator=moderator, conflicts=conflicts)
                    if state.revision != previous:
                        await cursor.execute(
                            "UPDATE fun_catalogues SET revision=%s, document=%s WHERE guild_id=%s",
                            (state.revision, encode(state).decode(), guild),
                        )
                        await self._outbox(cursor, guild, state)
                    await cursor.execute(
                        "INSERT INTO fun_mutations "
                        "(guild_id,mutation_id,actor_id,fingerprint,result) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        (
                            guild,
                            key,
                            change.actor,
                            fingerprint,
                            msgspec.json.encode({"notice": notice}).decode(),
                        ),
                    )
                await connection.commit()
                return state, notice
            except BaseException:
                await connection.rollback()
                raise

    async def snapshots(self) -> list[bytes]:
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT guild_id,document FROM fun_catalogues")
            rows = await cursor.fetchall()
            await connection.rollback()
            return [
                encode(decode(document, Catalogue).snapshot(int(guild))) for guild, document in rows
            ]

    async def pending(self) -> list[tuple[int, int, bytes]]:
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT guild_id,revision,payload FROM fun_catalogue_outbox "
                "WHERE published=FALSE ORDER BY guild_id,revision LIMIT 100"
            )
            rows = await cursor.fetchall()
            await connection.rollback()
            return [(int(g), int(r), p.encode() if isinstance(p, str) else p) for g, r, p in rows]

    async def published(self, guild: int, revision: int) -> None:
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE fun_catalogue_outbox SET published=TRUE WHERE guild_id=%s AND revision=%s",
                (guild, revision),
            )
            await cursor.execute(
                "DELETE FROM fun_catalogue_outbox WHERE guild_id=%s "
                "AND revision<%s AND published=TRUE",
                (guild, revision),
            )
            await cursor.execute(
                "DELETE FROM fun_mutations WHERE "
                "created_at<CURRENT_TIMESTAMP - INTERVAL 7 DAY LIMIT 100"
            )
            await connection.commit()
