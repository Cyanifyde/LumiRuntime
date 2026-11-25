from __future__ import annotations

import json
import secrets

from . import expanded as v2
from .models import ID, GiveawayError

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS giveaway_expanded (
    id CHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,host_id BIGINT UNSIGNED NOT NULL,
    channel_id BIGINT UNSIGNED NOT NULL,source_command BIGINT UNSIGNED NOT NULL,
    draft_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL,next_ms BIGINT UNSIGNED NOT NULL,document JSON NOT NULL,
    INDEX giveaway_expanded_due(next_ms),INDEX giveaway_expanded_guild(guild_id,host_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS giveaway_reservations (
    guild_id BIGINT UNSIGNED NOT NULL,game VARCHAR(12) CHARACTER SET ascii NOT NULL,
    code VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    giveaway_id CHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY(guild_id,game,code),INDEX giveaway_reservation_owner(giveaway_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS giveaway_drafts (
    id CHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,owner_id BIGINT UNSIGNED NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,expires_ms BIGINT UNSIGNED NOT NULL,document JSON NOT NULL,
    INDEX giveaway_drafts_owner(guild_id,owner_id),INDEX giveaway_draft_expiry(expires_ms)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS giveaway_presets (
    id CHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,name VARCHAR(100) CHARACTER SET utf8mb4 NOT NULL,
    document JSON NOT NULL,INDEX giveaway_presets_guild(guild_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS giveaway_settings (
    guild_id BIGINT UNSIGNED PRIMARY KEY,document JSON NOT NULL
    ) ENGINE=InnoDB""",
)


class ExpandedStore:
    async def extra_settings(self, guild):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute("SELECT document FROM giveaway_settings WHERE guild_id=%s", (guild,))
            row = await q.fetchone()
            return json.loads(row[0]) if row else None

    async def save_settings(self, guild, settings, expected, *, clear_legacy=False):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await q.execute(
                    "SELECT revision FROM giveaway_config WHERE guild_id=%s FOR UPDATE", (guild,)
                )
                row = await q.fetchone()
                if not row or row[0] != expected:
                    raise GiveawayError("Setup changed. Refresh it.")
                await q.execute(
                    "INSERT INTO giveaway_settings(guild_id,document) VALUES(%s,%s) "
                    "ON DUPLICATE KEY UPDATE document=VALUES(document)",
                    (guild, json.dumps(settings)),
                )
                await q.execute(
                    "UPDATE giveaway_config SET revision=revision+1"
                    + (",required_role=0" if clear_legacy else "")
                    + " WHERE guild_id=%s",
                    (guild,),
                )
                await conn.commit()
            finally:
                await conn.rollback()

    async def new_draft(
        self, guild, owner, command, channel, spec, now, *, editing=None, identifier=None
    ):
        value = dict(
            id=identifier or secrets.token_hex(8),
            guild=guild,
            owner=owner,
            command=command,
            source_channel=channel,
            revision=1,
            expires=now + v2.DAY,
            spec=v2.validate_spec(spec),
            editing=editing,
        )
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "INSERT IGNORE INTO giveaway_drafts VALUES(%s,%s,%s,%s,%s,%s)",
                (value["id"], guild, owner, 1, value["expires"], json.dumps(value)),
            )
            await conn.commit()
        if identifier:
            saved = await self.draft(guild, owner, identifier, now)
            if saved["command"] != command:
                raise GiveawayError("Draft unavailable.")
            return saved
        return value

    async def draft(self, guild, owner, identifier, now):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT document FROM giveaway_drafts WHERE guild_id=%s AND owner_id=%s "
                "AND id=%s AND expires_ms>%s",
                (guild, owner, identifier, now),
            )
            row = await q.fetchone()
            if not row:
                raise GiveawayError("Draft expired.")
            return json.loads(row[0])

    async def update_draft(self, guild, owner, identifier, revision, now, change, *, receipt):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await q.execute(
                    "SELECT document FROM giveaway_drafts WHERE guild_id=%s AND owner_id=%s "
                    "AND id=%s AND expires_ms>%s FOR UPDATE",
                    (guild, owner, identifier, now),
                )
                row = await q.fetchone()
                if not row:
                    raise GiveawayError("Draft expired.")
                value = json.loads(row[0])
                if receipt in value.get("receipts", []):
                    return value
                if value["revision"] != revision or value.get("published"):
                    raise GiveawayError("Draft changed. Refresh it.")
                change(value)
                value["spec"] = v2.validate_spec(value["spec"])
                value["revision"] += 1
                value["receipts"] = [*value.get("receipts", [])[-511:], receipt]
                await q.execute(
                    "UPDATE giveaway_drafts SET revision=%s,document=%s WHERE id=%s",
                    (value["revision"], json.dumps(value), identifier),
                )
                await conn.commit()
                return value
            finally:
                await conn.rollback()

    async def drafts(self, guild, owner, now):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT document FROM giveaway_drafts WHERE guild_id=%s AND owner_id=%s "
                "AND expires_ms>%s ORDER BY expires_ms DESC LIMIT 100",
                (guild, owner, now),
            )
            return [
                v for row in await q.fetchall() if not (v := json.loads(row[0])).get("published")
            ]

    async def expire_drafts(self, now):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute("DELETE FROM giveaway_drafts WHERE expires_ms<=%s LIMIT 100", (now,))
            await conn.commit()

    async def presets(self, guild):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT id,name,document FROM giveaway_presets WHERE guild_id=%s ORDER BY name,id",
                (guild,),
            )
            return [dict(id=r[0], name=r[1], spec=json.loads(r[2])) for r in await q.fetchall()]

    async def save_preset(self, guild, name, spec, *, identifier=None):
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 100:
            raise GiveawayError("Use a preset name up to 100 characters.")
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await q.execute(
                    "SELECT revision FROM giveaway_config WHERE guild_id=%s FOR UPDATE", (guild,)
                )
                await q.fetchone()
                identifier = identifier or secrets.token_hex(8)
                await q.execute(
                    "SELECT id FROM giveaway_presets WHERE guild_id=%s AND id=%s",
                    (guild, identifier),
                )
                if await q.fetchone():
                    return identifier
                await q.execute("SELECT id FROM giveaway_presets WHERE guild_id=%s", (guild,))
                if len(await q.fetchall()) >= 25:
                    raise GiveawayError("There are already 25 presets.")
                await q.execute(
                    "INSERT INTO giveaway_presets VALUES(%s,%s,%s,%s)",
                    (identifier, guild, name, json.dumps(v2.preset(v2.validate_spec(spec)))),
                )
                await conn.commit()
                return identifier
            finally:
                await conn.rollback()

    async def delete_preset(self, guild, identifier):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "DELETE FROM giveaway_presets WHERE guild_id=%s AND id=%s", (guild, identifier)
            )
            await conn.commit()

    async def list_records(
        self, guild, *, host=None, joined=None, channels=None, offset=0, limit=6
    ):

        host_clause = " AND host_id=%s" if host else ""
        if channels is not None and not channels:
            return []
        channel_clause = (
            " AND channel_id IN (" + ",".join("%s" for _ in channels) + ")"
            if channels is not None
            else ""
        )
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT document FROM (SELECT document,host_id,source_command,channel_id "
                "FROM giveaway_records "
                "WHERE guild_id=%s AND status!='migrated' UNION ALL "
                "SELECT document,host_id,source_command,channel_id "
                "FROM giveaway_expanded WHERE guild_id=%s) g WHERE 1=1"
                + host_clause
                + channel_clause
                + (" AND JSON_EXTRACT(document,%s)='true'" if joined else "")
                + " ORDER BY source_command DESC LIMIT %s OFFSET %s",
                tuple(
                    [guild, guild]
                    + ([host] if host else [])
                    + (list(channels) if channels is not None else [])
                    + ([f'$.entries."{joined}"'] if joined else [])
                    + [limit, offset]
                ),
            )
            return [json.loads(row[0]) for row in await q.fetchall()]

    async def create_expanded(self, draft, now, *, host_role, expected_config, identifier=None):
        identifier = secrets.token_hex(8) if identifier is None else identifier
        if not ID.fullmatch(identifier):
            raise GiveawayError("Invalid giveaway ID.")
        guild = draft["guild"]
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await q.execute(
                    "SELECT channel_id,host_role,required_role,revision FROM giveaway_config "
                    "WHERE guild_id=%s FOR UPDATE",
                    (guild,),
                )
                config = await q.fetchone()
                if (
                    not config
                    or not config[0]
                    or config[1] != host_role
                    or config[3] != expected_config
                ):
                    raise GiveawayError("Setup changed. Refresh it.")
                await q.execute(
                    "SELECT document FROM giveaway_settings WHERE guild_id=%s", (guild,)
                )
                settings = await q.fetchone()
                baseline = v2.config_defaults(
                    dict(
                        required_role=config[2],
                        settings=json.loads(settings[0]) if settings else {},
                    )
                )["baseline"]
                if draft["spec"]["baseline"] != baseline:
                    raise GiveawayError("Server requirements changed. Start a fresh draft.")
                await q.execute(
                    "SELECT document FROM giveaway_expanded WHERE draft_id=%s", (draft["id"],)
                )
                row = await q.fetchone()
                if row:
                    return json.loads(row[0]), False
                await q.execute(
                    "SELECT revision,document FROM giveaway_drafts "
                    "WHERE guild_id=%s AND owner_id=%s AND id=%s "
                    "AND expires_ms>%s FOR UPDATE",
                    (guild, draft["owner"], draft["id"], now),
                )
                current = await q.fetchone()
                if not current or current[0] != draft["revision"]:
                    raise GiveawayError("Draft changed. Refresh it.")
                saved = json.loads(current[1])
                if saved != draft or saved.get("editing"):
                    raise GiveawayError("Draft changed. Refresh it.")
                record = v2.new_record(
                    identifier,
                    guild,
                    draft["owner"],
                    config[0],
                    draft["command"],
                    draft["source_channel"],
                    draft["id"],
                    draft["spec"],
                    now,
                )
                record["host_role"] = host_role


                await self._insert_expanded(q, record)
                await self._reserve(q, record)
                await self._save_deliveries(q, record)
                saved["published"] = record["id"]
                await q.execute(
                    "UPDATE giveaway_drafts SET document=%s WHERE id=%s",
                    (json.dumps(saved), draft["id"]),
                )
                await conn.commit()
                return record, True
            finally:
                await conn.rollback()

    @staticmethod
    async def _insert_expanded(q, value):
        await q.execute(
            "INSERT INTO giveaway_expanded VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                value["id"],
                value["guild"],
                value["host"],
                value["channel"],
                value["command"],
                value["draft_id"],
                value["status"],
                value["next_ms"],
                json.dumps(value),
            ),
        )

    @staticmethod
    async def _reserve(q, record):
        wanted = set()
        if record["status"] in {"publishing", "active", "drawing"}:
            wanted |= v2.card_keys(record["spec"])
            if record.get("pending_edit"):
                wanted |= v2.card_keys(record["pending_edit"]["spec"])

        for game, code in sorted(wanted):
            await q.execute(
                "SELECT giveaway_id FROM giveaway_reservations "
                "WHERE guild_id=%s AND game=%s AND code=%s",
                (record["guild"], game, code),
            )
            if existing := await q.fetchone():
                if existing[0] != record["id"]:
                    raise GiveawayError("That card already has an active giveaway.")
                continue
            await q.execute(
                "SELECT id FROM giveaway_records WHERE guild_id=%s AND game=%s AND code=%s "
                "AND active_slot=1 AND id!=%s",
                (record["guild"], game, code, record["id"]),
            )
            if await q.fetchone():
                raise GiveawayError("That card already has an active giveaway.")
            await q.execute(
                "INSERT IGNORE INTO giveaway_reservations VALUES(%s,%s,%s,%s)",
                (record["guild"], game, code, record["id"]),
            )
            await q.execute(
                "SELECT giveaway_id FROM giveaway_reservations "
                "WHERE guild_id=%s AND game=%s AND code=%s FOR UPDATE",
                (record["guild"], game, code),
            )
            if (await q.fetchone())[0] != record["id"]:
                raise GiveawayError("That card already has an active giveaway.")
        await q.execute(
            "SELECT game,code FROM giveaway_reservations WHERE giveaway_id=%s AND guild_id=%s",
            (record["id"], record["guild"]),
        )
        for game, code in await q.fetchall():
            if (game, code) not in wanted:
                await q.execute(
                    "DELETE FROM giveaway_reservations "
                    "WHERE giveaway_id=%s AND guild_id=%s AND game=%s AND code=%s",
                    (record["id"], record["guild"], game, code),
                )

    async def upgrade_expanded(self, guild, identifier, revision):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await q.execute(
                    "SELECT document FROM giveaway_records WHERE guild_id=%s AND id=%s FOR UPDATE",
                    (guild, identifier),
                )
                row = await q.fetchone()
                if not row:
                    raise GiveawayError("Giveaway unavailable.")
                old = json.loads(row[0])
                await q.execute(
                    "SELECT document FROM giveaway_expanded WHERE guild_id=%s AND id=%s FOR UPDATE",
                    (guild, identifier),
                )
                if existing := await q.fetchone():
                    return json.loads(existing[0])
                if old["revision"] != revision or old["status"] != "active":
                    raise GiveawayError("This giveaway changed.")
                value = v2.upgrade(old)
                await self._insert_expanded(q, value)
                await self._reserve(q, value)
                old.update(status="migrated", next_ms=v2.NEVER)
                await q.execute(
                    "UPDATE giveaway_records SET status='migrated',active_slot=NULL,"
                    "next_ms=%s,document=%s WHERE id=%s",
                    (v2.NEVER, json.dumps(old), identifier),
                )
                await conn.commit()
                return value
            finally:
                await conn.rollback()
