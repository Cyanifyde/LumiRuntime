from __future__ import annotations

import copy
import json
import secrets
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool, read_cursor

from . import expanded as v2
from .expanded_store import SCHEMA as EXPANDED_SCHEMA
from .expanded_store import ExpandedStore
from .models import ACTIVE, GiveawayError, new_record

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS giveaway_config (
    guild_id BIGINT UNSIGNED PRIMARY KEY,channel_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    required_role BIGINT UNSIGNED NOT NULL DEFAULT 0,
    host_role BIGINT UNSIGNED NOT NULL DEFAULT 0,
    revision BIGINT UNSIGNED NOT NULL DEFAULT 0,updated_by BIGINT UNSIGNED NOT NULL DEFAULT 0
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS giveaway_records (
    id CHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,host_id BIGINT UNSIGNED NOT NULL,
    channel_id BIGINT UNSIGNED NOT NULL,source_command BIGINT UNSIGNED NOT NULL,
    game VARCHAR(12) CHARACTER SET ascii NOT NULL,code VARCHAR(32) CHARACTER SET ascii NOT NULL,
    active_slot TINYINT NULL,status VARCHAR(20) NOT NULL,next_ms BIGINT UNSIGNED NOT NULL,
    document JSON NOT NULL,UNIQUE KEY command_once(guild_id,source_command),
    UNIQUE KEY active_card(guild_id,game,code,active_slot),
    INDEX due_work(next_ms),INDEX channel_records(guild_id,channel_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS giveaway_outbox (
    id VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,giveaway_id CHAR(16) NOT NULL,
    state VARCHAR(20) NOT NULL,document JSON NOT NULL,
    INDEX giveaway_delivery(giveaway_id),INDEX pending_delivery(state)
    ) ENGINE=InnoDB""",
    *EXPANDED_SCHEMA,
)


class MySQLGiveawayStore(ExpandedStore):
    def __init__(self, dsn):
        value = urlparse(dsn)
        if value.scheme not in {"mysql", "mariadb"}:
            raise ValueError("Invalid giveaway DSN")
        self.settings = dict(
            host=value.hostname,
            port=value.port or 3306,
            user=unquote(value.username or ""),
            password=unquote(value.password or ""),
            db=value.path.lstrip("/"),
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
            self.pool = None

    async def migrate(self):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            for sql in SCHEMA:
                await cursor.execute(sql)
            await cursor.execute(
                "ALTER TABLE giveaway_config ADD COLUMN IF NOT EXISTS "
                "host_role BIGINT UNSIGNED NOT NULL DEFAULT 0"
            )
            await connection.commit()

    async def config(self, guild):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT channel_id,revision,required_role,host_role FROM giveaway_config "
                "WHERE guild_id=%s",
                (guild,),
            )
            row = await cursor.fetchone()
            result = (
                dict(channel=row[0], revision=row[1], required_role=row[2], host_role=row[3])
                if row
                else dict(channel=0, revision=0, required_role=0, host_role=0)
            )
        if settings := await self.extra_settings(guild):
            result["settings"] = settings
        return result

    @staticmethod
    async def _require_config_revision(cursor, guild, expected):
        if expected is None:
            return
        if expected == 0:
            await cursor.execute("INSERT IGNORE INTO giveaway_config(guild_id) VALUES(%s)", (guild,))
        await cursor.execute(
            "SELECT revision FROM giveaway_config WHERE guild_id=%s FOR UPDATE", (guild,)
        )
        row = await cursor.fetchone()
        if not row or row[0] != expected:
            raise GiveawayError("Setup changed. Refresh it.")

    async def configure_role(self, guild, role, actor, *, host=False, expected=None):
        column = "host_role" if host else "required_role"
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await self._require_config_revision(cursor, guild, expected)
                await cursor.execute(
                    f"INSERT INTO giveaway_config(guild_id,{column},revision,updated_by) "
                    "VALUES(%s,%s,1,%s) ON DUPLICATE KEY UPDATE "
                    f"{column}=VALUES({column}),"
                    "revision=revision+1,updated_by=VALUES(updated_by)",
                    (guild, role, actor),
                )
                await connection.commit()
            finally:
                await connection.rollback()

    async def configure(self, guild, channel, actor, *, expected=None):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await self._require_config_revision(cursor, guild, expected)
                await cursor.execute(
                    "INSERT INTO giveaway_config(guild_id,channel_id,revision,updated_by) "
                    "VALUES(%s,%s,1,%s) ON DUPLICATE KEY UPDATE channel_id=VALUES(channel_id),"
                    "revision=revision+1,updated_by=VALUES(updated_by)",
                    (guild, channel, actor),
                )
                await connection.commit()
            finally:
                await connection.rollback()

    async def get(self, guild, identifier):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT document FROM giveaway_expanded WHERE guild_id=%s AND id=%s",
                (guild, identifier),
            )
            if row := await cursor.fetchone():
                return json.loads(row[0])
            await cursor.execute(
                "SELECT document FROM giveaway_records WHERE guild_id=%s AND id=%s",
                (guild, identifier),
            )
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def for_command(self, guild, command):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT document FROM giveaway_expanded "
                "WHERE guild_id=%s AND source_command=%s ORDER BY id LIMIT 1",
                (guild, command),
            )
            if row := await cursor.fetchone():
                return json.loads(row[0])
            await cursor.execute(
                "SELECT document FROM giveaway_records WHERE guild_id=%s AND source_command=%s",
                (guild, command),
            )
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def for_message(self, guild, message):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT document FROM giveaway_expanded WHERE guild_id=%s "
                "AND CAST(JSON_VALUE(document,'$.message') AS UNSIGNED)=%s",
                (guild, message),
            )
            if row := await cursor.fetchone():
                return json.loads(row[0])
            await cursor.execute(
                "SELECT document FROM giveaway_records WHERE guild_id=%s "
                "AND CAST(JSON_VALUE(document,'$.message') AS UNSIGNED)=%s",
                (guild, message),
            )
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def create(self, guild, host, command, source_channel, source, wait, image, now):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT channel_id,required_role,host_role FROM giveaway_config "
                    "WHERE guild_id=%s FOR UPDATE",
                    (guild,),
                )
                row = await cursor.fetchone()
                if not row or not row[0]:
                    raise GiveawayError("An administrator must set a giveaway channel first.")
                await cursor.execute(
                    "SELECT document FROM giveaway_records WHERE guild_id=%s AND source_command=%s",
                    (guild, command),
                )
                prior = await cursor.fetchone()
                if prior:
                    return json.loads(prior[0]), False
                if not row[2]:
                    raise GiveawayError("An administrator must set a host role first.")
                await cursor.execute(
                    "SELECT id FROM giveaway_records WHERE guild_id=%s AND game=%s "
                    "AND code=%s AND active_slot=1",
                    (guild, source["game"], source["code"].casefold()),
                )
                if await cursor.fetchone():
                    raise GiveawayError("That card already has an active giveaway.")
                await cursor.execute(
                    "SELECT giveaway_id FROM giveaway_reservations "
                    "WHERE guild_id=%s AND game=%s AND code=%s",
                    (guild, source["game"], source["code"].casefold()),
                )
                if await cursor.fetchone():
                    raise GiveawayError("That card already has an active giveaway.")
                record = new_record(
                    secrets.token_hex(8),
                    guild,
                    host,
                    row[0],
                    command,
                    source_channel,
                    source,
                    wait,
                    image,
                    now,
                )
                record["required_role"] = row[1]
                record["host_role"] = row[2]
                await cursor.execute(
                    "INSERT INTO giveaway_records(id,guild_id,host_id,channel_id,source_command,"
                    "game,code,active_slot,status,next_ms,document) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s)",
                    (
                        record["id"],
                        guild,
                        host,
                        row[0],
                        command,
                        source["game"],
                        source["code"].casefold(),
                        record["status"],
                        now,
                        json.dumps(record),
                    ),
                )
                await self._save(cursor, record)
                await connection.commit()
                return record, True
            finally:
                await connection.rollback()

    @asynccontextmanager
    async def locked(self, guild, identifier):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT document FROM giveaway_records WHERE guild_id=%s AND id=%s FOR UPDATE",
                    (guild, identifier),
                )
                legacy_row = await cursor.fetchone()
                await cursor.execute(
                    "SELECT document FROM giveaway_expanded WHERE guild_id=%s AND id=%s FOR UPDATE",
                    (guild, identifier),
                )
                expanded_row = await cursor.fetchone()
                row = expanded_row or legacy_row
                if not row:
                    raise GiveawayError("Giveaway unavailable.")
                record = json.loads(row[0])
                before = copy.deepcopy(record)
                yield record
                if record != before:
                    await self._save(cursor, record)
                await connection.commit()
            finally:
                await connection.rollback()

    @staticmethod
    async def _save(cursor, record):
        if v2.is_v2(record):
            await ExpandedStore._reserve(cursor, record)
            await cursor.execute(
                "UPDATE giveaway_expanded SET channel_id=%s,status=%s,next_ms=%s,document=%s "
                "WHERE guild_id=%s AND id=%s",
                (
                    record["channel"],
                    record["status"],
                    record["next_ms"],
                    json.dumps(record),
                    record["guild"],
                    record["id"],
                ),
            )
        else:
            await MySQLGiveawayStore._save_legacy(cursor, record)
        await MySQLGiveawayStore._save_deliveries(cursor, record)

    @staticmethod
    async def _save_legacy(cursor, record):
        await cursor.execute(
            "UPDATE giveaway_records SET channel_id=%s,active_slot=%s,status=%s,next_ms=%s,"
            "document=%s WHERE guild_id=%s AND id=%s",
            (
                record["channel"],
                1 if record["status"] in ACTIVE else None,
                record["status"],
                record["next_ms"],
                json.dumps(record),
                record["guild"],
                record["id"],
            ),
        )

    @staticmethod
    async def _save_deliveries(cursor, record):
        for key, delivery in record["deliveries"].items():
            await cursor.execute(
                "INSERT INTO giveaway_outbox(id,guild_id,giveaway_id,state,document) "
                "VALUES(%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                "state=VALUES(state),document=VALUES(document)",
                (
                    f"{record['id']}:{key}",
                    record["guild"],
                    record["id"],
                    delivery["state"],
                    json.dumps(delivery),
                ),
            )

    async def due(self, now, limit=100, *, guilds=None):
        if guilds is not None and not guilds:
            return []
        scope = ""
        arguments = [now]
        if guilds is not None:
            scope = " AND guild_id IN (" + ",".join("%s" for _ in guilds) + ")"
            arguments.extend(sorted(guilds))
        arguments.append(limit)
        async with read_cursor(self.pool) as cursor:
            await cursor.execute(
                "SELECT guild_id,id,host_id,channel_id,next_ms FROM "
                "(SELECT guild_id,id,host_id,channel_id,next_ms FROM giveaway_records "
                "UNION ALL SELECT guild_id,id,host_id,channel_id,next_ms "
                "FROM giveaway_expanded) records "
                "WHERE next_ms<=%s" + scope + " ORDER BY next_ms,id LIMIT %s",
                tuple(arguments),
            )
            return [
                dict(guild=row[0], id=row[1], host=row[2], channel=row[3], due=row[4])
                for row in await cursor.fetchall()
            ]

    async def in_channel(self, guild, channel):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT document FROM (SELECT guild_id,channel_id,status,document "
                "FROM giveaway_records UNION ALL "
                "SELECT guild_id,channel_id,status,document FROM giveaway_expanded) records "
                "WHERE guild_id=%s AND channel_id=%s "
                "AND status NOT IN ('failed','cancelled','migrated')",
                (guild, channel),
            )
            return [json.loads(row[0]) for row in await cursor.fetchall()]
