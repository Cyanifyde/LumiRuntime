from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

import asyncmy
import msgspec

from lumi.contracts import ReactionRouteSnapshot, encode
from lumi.contracts.layout_message import ATTEMPT
from lumi.storage.checkpoint import create_pool, read_cursor

CUSTOM_EMOJI_ID = re.compile(r"^<a?:[A-Za-z0-9_]{2,32}:(\d{17,20})>$")

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS verification_panels (
        panel_id CHAR(32) PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        channel_id BIGINT UNSIGNED NOT NULL,
        message_id BIGINT UNSIGNED NULL,
        body TEXT NOT NULL,
        unverified_role_id BIGINT UNSIGNED NOT NULL,
        verified_role_id BIGINT UNSIGNED NOT NULL,
        lifecycle VARCHAR(24) NOT NULL DEFAULT 'pending',
        ownership VARCHAR(24) NOT NULL DEFAULT 'lumi',
        source VARCHAR(24) NOT NULL DEFAULT 'new',
        route_revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
        updated_by BIGINT UNSIGNED NOT NULL DEFAULT 0,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        UNIQUE KEY uq_verification_message (guild_id, message_id),
        KEY ix_verification_guild (guild_id, lifecycle)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS reaction_role_panels (
        panel_id CHAR(32) PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        channel_id BIGINT UNSIGNED NOT NULL,
        message_id BIGINT UNSIGNED NULL,
        lifecycle VARCHAR(24) NOT NULL DEFAULT 'pending',
        ownership VARCHAR(24) NOT NULL DEFAULT 'lumi',
        source VARCHAR(24) NOT NULL DEFAULT 'new',
        route_revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
        applied_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_by BIGINT UNSIGNED NOT NULL DEFAULT 0,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        UNIQUE KEY uq_reaction_panel_message (guild_id, message_id),
        KEY ix_reaction_panel_guild (guild_id, lifecycle)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS reaction_role_panel_messages (
        panel_id CHAR(32) NOT NULL,
        position SMALLINT UNSIGNED NOT NULL,
        message_id BIGINT UNSIGNED NOT NULL,
        ownership VARCHAR(24) NOT NULL DEFAULT 'lumi',
        applied_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        repair_state VARCHAR(24) NOT NULL DEFAULT 'ready',
        content_hash CHAR(64) NOT NULL DEFAULT '',
        PRIMARY KEY (panel_id, position),
        UNIQUE KEY uq_reaction_part_message (message_id),
        CONSTRAINT fk_reaction_part_panel FOREIGN KEY (panel_id)
            REFERENCES reaction_role_panels(panel_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS reaction_role_message_cleanup (
        panel_id CHAR(32) NOT NULL,
        message_id BIGINT UNSIGNED NOT NULL,
        channel_id BIGINT UNSIGNED NOT NULL,
        attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0,
        last_error VARCHAR(160) NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        completed_at TIMESTAMP(6) NULL,
        PRIMARY KEY (panel_id, message_id),
        CONSTRAINT fk_reaction_cleanup_panel FOREIGN KEY (panel_id)
            REFERENCES reaction_role_panels(panel_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS reaction_role_mappings (
        mapping_id CHAR(32) PRIMARY KEY,
        panel_id CHAR(32) NOT NULL,
        emoji_key VARCHAR(160) NOT NULL,
        role_id BIGINT UNSIGNED NOT NULL,
        label VARCHAR(200) NULL,
        position SMALLINT UNSIGNED NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        updated_by BIGINT UNSIGNED NOT NULL DEFAULT 0,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        CONSTRAINT fk_reaction_mapping_panel FOREIGN KEY (panel_id)
            REFERENCES reaction_role_panels(panel_id) ON DELETE CASCADE,
        UNIQUE KEY uq_reaction_panel_emoji (panel_id, emoji_key),
        KEY ix_reaction_mapping_order (panel_id, enabled, position)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS reaction_role_applied_routes (
        panel_id CHAR(32) NOT NULL,
        mapping_id CHAR(32) NOT NULL,
        message_id BIGINT UNSIGNED NOT NULL,
        emoji_key VARCHAR(160) NOT NULL,
        role_id BIGINT UNSIGNED NOT NULL,
        position SMALLINT UNSIGNED NOT NULL,
        applied_revision BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (panel_id, mapping_id),
        UNIQUE KEY uq_reaction_applied_message_emoji (message_id, emoji_key),
        KEY ix_reaction_applied_message (message_id),
        CONSTRAINT fk_reaction_applied_panel FOREIGN KEY (panel_id)
            REFERENCES reaction_role_panels(panel_id) ON DELETE CASCADE,
        CONSTRAINT fk_reaction_applied_mapping FOREIGN KEY (mapping_id)
            REFERENCES reaction_role_mappings(mapping_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS button_role_panels (
        panel_id CHAR(32) PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        channel_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
        lifecycle VARCHAR(24) NOT NULL DEFAULT 'draft',
        desired_revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
        applied_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        pending_layout_create VARBINARY(512) NULL,
        updated_by BIGINT UNSIGNED NOT NULL DEFAULT 0,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        KEY ix_button_role_guild (guild_id, lifecycle)
    ) ENGINE=InnoDB
    """,
    """
    ALTER TABLE button_role_panels
    ADD COLUMN IF NOT EXISTS pending_layout_create VARBINARY(512) NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS button_role_panel_messages (
        panel_id CHAR(32) NOT NULL,
        position SMALLINT UNSIGNED NOT NULL,
        message_id BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (panel_id, position),
        UNIQUE KEY uq_button_role_message (message_id),
        CONSTRAINT fk_button_message_panel FOREIGN KEY (panel_id)
            REFERENCES button_role_panels(panel_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS button_role_containers (
        container_id CHAR(32) PRIMARY KEY,
        panel_id CHAR(32) NOT NULL,
        position SMALLINT UNSIGNED NOT NULL,
        clear_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        CONSTRAINT fk_button_container_panel FOREIGN KEY (panel_id)
            REFERENCES button_role_panels(panel_id) ON DELETE CASCADE,
        KEY ix_button_container_order (panel_id, position)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS button_role_assets (
        asset_id CHAR(32) PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        sha256 CHAR(64) NOT NULL,
        file_name VARCHAR(100) NOT NULL,
        media_type VARCHAR(64) NOT NULL,
        width INT UNSIGNED NOT NULL,
        height INT UNSIGNED NOT NULL,
        byte_size INT UNSIGNED NOT NULL,
        payload LONGBLOB NOT NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        UNIQUE KEY uq_button_asset_hash (guild_id, sha256)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS button_role_blocks (
        block_id CHAR(32) PRIMARY KEY,
        container_id CHAR(32) NOT NULL,
        kind VARCHAR(24) NOT NULL,
        position SMALLINT UNSIGNED NOT NULL,
        text TEXT NULL,
        heading_level TINYINT UNSIGNED NULL,
        asset_id CHAR(32) NULL,
        role_id BIGINT UNSIGNED NULL,
        description VARCHAR(1000) NULL,
        emoji_key VARCHAR(160) NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        CONSTRAINT fk_button_block_container FOREIGN KEY (container_id)
            REFERENCES button_role_containers(container_id) ON DELETE CASCADE,
        CONSTRAINT fk_button_block_asset FOREIGN KEY (asset_id)
            REFERENCES button_role_assets(asset_id) ON DELETE RESTRICT,
        KEY ix_button_block_order (container_id, position)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS panel_route_revisions (
        topic VARCHAR(64) PRIMARY KEY,
        revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS panel_publication_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        topic VARCHAR(64) NOT NULL,
        revision BIGINT UNSIGNED NOT NULL,
        payload MEDIUMBLOB NOT NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        published_at TIMESTAMP(6) NULL,
        UNIQUE KEY uq_panel_outbox_revision (topic, revision),
        KEY ix_panel_outbox_pending (published_at, id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS panel_migration_runs (
        run_id CHAR(32) PRIMARY KEY,
        source_digest CHAR(64) NOT NULL,
        verification_count INT UNSIGNED NOT NULL DEFAULT 0,
        reaction_mapping_count INT UNSIGNED NOT NULL DEFAULT 0,
        status VARCHAR(24) NOT NULL,
        details_json JSON NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        completed_at TIMESTAMP(6) NULL,
        UNIQUE KEY uq_panel_migration_digest (source_digest)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS panel_migration_items (
        module_key VARCHAR(32) NOT NULL,
        panel_id CHAR(32) NOT NULL,
        classification VARCHAR(24) NOT NULL DEFAULT 'pending',
        snapshot_json JSON NULL,
        preview_channel_id BIGINT UNSIGNED NULL,
        preview_message_id BIGINT UNSIGNED NULL,
        final_snapshot_json JSON NULL,
        converted_at TIMESTAMP(6) NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (module_key, panel_id)
    ) ENGINE=InnoDB
    """,
    """
    ALTER TABLE panel_migration_items
        ADD COLUMN IF NOT EXISTS final_snapshot_json JSON NULL AFTER preview_message_id
    """,
    """
    ALTER TABLE reaction_role_panels
        ADD COLUMN IF NOT EXISTS applied_revision BIGINT UNSIGNED NOT NULL DEFAULT 0
            AFTER route_revision
    """,
    """
    ALTER TABLE reaction_role_panel_messages
        ADD COLUMN IF NOT EXISTS content_hash CHAR(64) NOT NULL DEFAULT ''
            AFTER repair_state
    """,
    """
    INSERT IGNORE INTO reaction_role_panel_messages
        (panel_id, position, message_id, ownership, applied_revision, repair_state)
    SELECT panel_id, 0, message_id, ownership, route_revision, 'ready'
    FROM reaction_role_panels
    WHERE message_id IS NOT NULL
    """,
    """
    INSERT IGNORE INTO reaction_role_applied_routes
        (panel_id, mapping_id, message_id, emoji_key, role_id, position,
         applied_revision)
    SELECT p.panel_id, m.mapping_id, p.message_id, m.emoji_key, m.role_id,
           m.position, p.route_revision
    FROM reaction_role_panels p
    JOIN reaction_role_mappings m ON m.panel_id = p.panel_id AND m.enabled = TRUE
    WHERE p.message_id IS NOT NULL AND p.lifecycle IN ('active', 'needs_repair')
    """,
    """
    UPDATE reaction_role_panels
    SET applied_revision = route_revision
    WHERE applied_revision = 0 AND message_id IS NOT NULL
    """,
)


@dataclass(frozen=True, slots=True)
class VerificationPanel:
    panel_id: str
    guild_id: int
    channel_id: int
    message_id: int | None
    body: str
    unverified_role_id: int
    verified_role_id: int
    lifecycle: str
    ownership: str
    source: str
    route_revision: int


@dataclass(frozen=True, slots=True)
class ReactionPanel:
    panel_id: str
    guild_id: int
    channel_id: int
    message_id: int | None
    lifecycle: str
    ownership: str
    source: str
    route_revision: int
    applied_revision: int = 0


@dataclass(frozen=True, slots=True)
class ReactionPanelPart:
    panel_id: str
    position: int
    message_id: int
    ownership: str
    applied_revision: int
    repair_state: str
    content_hash: str = ""


@dataclass(frozen=True, slots=True)
class ReactionAppliedRoute:
    panel_id: str
    mapping_id: str
    message_id: int
    emoji_key: str
    role_id: int
    position: int
    applied_revision: int


@dataclass(frozen=True, slots=True)
class ReactionMapping:
    mapping_id: str
    panel_id: str
    emoji_key: str
    role_id: int
    label: str | None
    position: int
    enabled: bool


@dataclass(frozen=True, slots=True)
class ButtonRolePanel:
    panel_id: str
    guild_id: int
    channel_id: int
    lifecycle: str
    desired_revision: int
    applied_revision: int


class ButtonPanelCreation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    channel: int
    revision: int
    position: int
    previous: int
    attempt: str

    def valid(self):
        return bool(
            0 < self.channel < 2**64 and self.revision > 0 and 0 <= self.position < 100
            and 0 <= self.previous < 2**64 and ATTEMPT.fullmatch(self.attempt)
        )


@dataclass(frozen=True, slots=True)
class ButtonRoleContainer:
    container_id: str
    panel_id: str
    position: int
    clear_enabled: bool


@dataclass(frozen=True, slots=True)
class ButtonRoleBlock:
    block_id: str
    container_id: str
    kind: str
    position: int
    text: str | None = None
    heading_level: int | None = None
    asset_id: str | None = None
    role_id: int | None = None
    description: str | None = None
    emoji_key: str | None = None


@dataclass(frozen=True, slots=True)
class ButtonRoleAsset:
    asset_id: str
    guild_id: int
    sha256: str
    file_name: str
    media_type: str
    width: int
    height: int
    payload: bytes


class MySQLPanelStore:
    def __init__(self, dsn: str) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("panel DSN must use mysql:// or mariadb://")
        self._settings = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "db": parsed.path.removeprefix("/"),
            "autocommit": False,
            "minsize": 1,
            "maxsize": 4,
        }
        self._pool: asyncmy.Pool | None = None

    async def connect(self) -> None:
        self._pool = await create_pool(**self._settings)

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def initialize_runtime(self) -> None:

        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await self._bump_reaction_index(cursor)
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise

    async def migrate(self) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for statement in SCHEMA:
                    await cursor.execute(statement)
                await cursor.execute(
                    "INSERT IGNORE INTO panel_route_revisions (topic, revision) "
                    "VALUES ('reaction_roles', 0)"
                )
                await self._bump_reaction_index(cursor)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    def _require_pool(self) -> asyncmy.Pool:
        if self._pool is None:
            raise RuntimeError("panel store is not connected")
        return self._pool

    async def list_verification(self, guild_id: int) -> tuple[VerificationPanel, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, guild_id, channel_id, message_id, body,
                       unverified_role_id, verified_role_id, lifecycle, ownership,
                       source, route_revision
                FROM verification_panels
                WHERE guild_id = %s AND lifecycle <> 'deleted'
                ORDER BY created_at, panel_id
                """,
                (guild_id,),
            )
            return tuple(self._verification(row) for row in await cursor.fetchall())

    async def get_verification(self, guild_id: int, panel_id: str) -> VerificationPanel | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, guild_id, channel_id, message_id, body,
                       unverified_role_id, verified_role_id, lifecycle, ownership,
                       source, route_revision
                FROM verification_panels WHERE guild_id = %s AND panel_id = %s
                """,
                (guild_id, panel_id),
            )
            row = await cursor.fetchone()
            return self._verification(row) if row else None

    async def get_verification_by_message(
        self, guild_id: int, message_id: int
    ) -> VerificationPanel | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, guild_id, channel_id, message_id, body,
                       unverified_role_id, verified_role_id, lifecycle, ownership,
                       source, route_revision
                FROM verification_panels
                WHERE guild_id = %s AND message_id = %s
                """,
                (guild_id, message_id),
            )
            row = await cursor.fetchone()
            return self._verification(row) if row else None

    async def stage_verification(
        self,
        panel: VerificationPanel,
        actor_id: int,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    INSERT INTO verification_panels
                        (panel_id, guild_id, channel_id, message_id, body,
                         unverified_role_id, verified_role_id, lifecycle, ownership,
                         source, route_revision, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE channel_id = VALUES(channel_id),
                        body = VALUES(body), unverified_role_id = VALUES(unverified_role_id),
                        verified_role_id = VALUES(verified_role_id),
                        lifecycle = VALUES(lifecycle), ownership = VALUES(ownership),
                        route_revision = route_revision + 1, updated_by = VALUES(updated_by)
                    """,
                    (
                        panel.panel_id,
                        panel.guild_id,
                        panel.channel_id,
                        panel.message_id,
                        panel.body,
                        panel.unverified_role_id,
                        panel.verified_role_id,
                        panel.lifecycle,
                        panel.ownership,
                        panel.source,
                        panel.route_revision,
                        actor_id,
                    ),
                )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def activate_verification(
        self, guild_id: int, panel_id: str, channel_id: int, message_id: int, actor_id: int
    ) -> None:
        await self._verification_lifecycle(
            guild_id,
            panel_id,
            actor_id,
            lifecycle="active",
            channel_id=channel_id,
            message_id=message_id,
            ownership="lumi",
        )

    async def mark_verification(
        self, guild_id: int, panel_id: str, lifecycle: str, actor_id: int
    ) -> None:
        await self._verification_lifecycle(guild_id, panel_id, actor_id, lifecycle=lifecycle)

    async def mark_verification_resource_missing(
        self, guild_id: int, resource_kind: str, resource_id: int
    ) -> int:
        if resource_kind == "channel":
            predicate = "channel_id = %s"
            parameters: tuple[object, ...] = (resource_id,)
        elif resource_kind == "message":
            predicate = "message_id = %s"
            parameters = (resource_id,)
        elif resource_kind == "role":
            predicate = "(unverified_role_id = %s OR verified_role_id = %s)"
            parameters = (resource_id, resource_id)
        else:
            raise ValueError("unsupported verification resource")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    f"""
                    UPDATE verification_panels
                    SET lifecycle = 'needs_repair', route_revision = route_revision + 1,
                        updated_by = 0
                    WHERE guild_id = %s AND lifecycle NOT IN ('deleted', 'needs_repair')
                      AND {predicate}
                    """,
                    (guild_id, *parameters),
                )
                changed = int(cursor.rowcount)
                await connection.commit()
                return changed
            except Exception:
                await connection.rollback()
                raise

    async def _verification_lifecycle(
        self,
        guild_id: int,
        panel_id: str,
        actor_id: int,
        *,
        lifecycle: str,
        channel_id: int | None = None,
        message_id: int | None = None,
        ownership: str | None = None,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    UPDATE verification_panels
                    SET lifecycle = %s,
                        channel_id = COALESCE(%s, channel_id),
                        message_id = COALESCE(%s, message_id),
                        ownership = COALESCE(%s, ownership),
                        route_revision = route_revision + 1,
                        updated_by = %s
                    WHERE guild_id = %s AND panel_id = %s
                    """,
                    (
                        lifecycle,
                        channel_id,
                        message_id,
                        ownership,
                        actor_id,
                        guild_id,
                        panel_id,
                    ),
                )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def list_reaction_panels(self, guild_id: int) -> tuple[ReactionPanel, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, guild_id, channel_id, message_id, lifecycle,
                       ownership, source, route_revision, applied_revision
                FROM reaction_role_panels
                WHERE guild_id = %s AND lifecycle NOT IN ('deleted', 'detached')
                ORDER BY created_at, panel_id
                """,
                (guild_id,),
            )
            return tuple(self._reaction_panel(row) for row in await cursor.fetchall())

    async def get_reaction_panel(self, guild_id: int, panel_id: str) -> ReactionPanel | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, guild_id, channel_id, message_id, lifecycle,
                       ownership, source, route_revision, applied_revision
                FROM reaction_role_panels WHERE guild_id = %s AND panel_id = %s
                """,
                (guild_id, panel_id),
            )
            row = await cursor.fetchone()
            return self._reaction_panel(row) if row else None

    async def get_reaction_panel_by_message(
        self, guild_id: int, message_id: int
    ) -> ReactionPanel | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT p.panel_id, p.guild_id, p.channel_id, p.message_id, p.lifecycle,
                       p.ownership, p.source, p.route_revision, p.applied_revision
                FROM reaction_role_panels p
                JOIN reaction_role_panel_messages part ON part.panel_id = p.panel_id
                WHERE p.guild_id = %s AND part.message_id = %s
                  AND p.lifecycle IN ('active', 'needs_repair')
                """,
                (guild_id, message_id),
            )
            row = await cursor.fetchone()
            return self._reaction_panel(row) if row else None

    async def stage_reaction_panel(self, panel: ReactionPanel, actor_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    INSERT INTO reaction_role_panels
                        (panel_id, guild_id, channel_id, message_id, lifecycle,
                         ownership, source, route_revision, applied_revision, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE channel_id = VALUES(channel_id),
                        message_id = VALUES(message_id), lifecycle = VALUES(lifecycle),
                        ownership = VALUES(ownership), route_revision = route_revision + 1,
                        updated_by = VALUES(updated_by)
                    """,
                    (
                        panel.panel_id,
                        panel.guild_id,
                        panel.channel_id,
                        panel.message_id,
                        panel.lifecycle,
                        panel.ownership,
                        panel.source,
                        panel.route_revision,
                        panel.applied_revision,
                        actor_id,
                    ),
                )
                await self._bump_reaction_index(cursor)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def activate_reaction_panel(
        self,
        guild_id: int,
        panel_id: str,
        channel_id: int,
        message_id: int,
        ownership: str,
        actor_id: int,
    ) -> None:
        await self._set_reaction_lifecycle(
            guild_id,
            panel_id,
            "active",
            actor_id,
            channel_id=channel_id,
            message_id=message_id,
            ownership=ownership,
        )

    async def set_reaction_lifecycle(
        self, guild_id: int, panel_id: str, lifecycle: str, actor_id: int
    ) -> None:
        await self._set_reaction_lifecycle(guild_id, panel_id, lifecycle, actor_id)

    async def mark_reaction_resource_missing(
        self, guild_id: int, resource_kind: str, resource_id: int
    ) -> int:
        if resource_kind == "channel":
            join = ""
            predicate = "p.channel_id = %s"
            parameters: tuple[object, ...] = (resource_id,)
        elif resource_kind == "role":
            join = "JOIN reaction_role_applied_routes r ON r.panel_id = p.panel_id"
            predicate = "r.role_id = %s"
            parameters = (resource_id,)
        elif resource_kind == "emoji":
            join = "JOIN reaction_role_applied_routes r ON r.panel_id = p.panel_id"
            predicate = "r.emoji_key LIKE %s"
            parameters = (f"%:{resource_id}>",)
        else:
            raise ValueError("unsupported reaction-role resource")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    f"""
                    UPDATE reaction_role_panels p {join}
                    SET p.lifecycle = 'needs_repair',
                        p.route_revision = p.route_revision + 1, p.updated_by = 0
                    WHERE p.guild_id = %s
                      AND p.lifecycle NOT IN ('deleted', 'detached', 'needs_repair')
                      AND {predicate}
                    """,
                    (guild_id, *parameters),
                )
                changed = int(cursor.rowcount)
                if changed:
                    if resource_kind == "channel":
                        await cursor.execute(
                            """
                            DELETE r FROM reaction_role_applied_routes r
                            JOIN reaction_role_panels p ON p.panel_id = r.panel_id
                            WHERE p.guild_id = %s AND p.channel_id = %s
                            """,
                            (guild_id, resource_id),
                        )
                    elif resource_kind == "role":
                        await cursor.execute(
                            """
                            DELETE r FROM reaction_role_applied_routes r
                            JOIN reaction_role_panels p ON p.panel_id = r.panel_id
                            WHERE p.guild_id = %s AND r.role_id = %s
                            """,
                            (guild_id, resource_id),
                        )
                    elif resource_kind == "emoji":
                        await cursor.execute(
                            """
                            DELETE r FROM reaction_role_applied_routes r
                            JOIN reaction_role_panels p ON p.panel_id = r.panel_id
                            WHERE p.guild_id = %s AND r.emoji_key LIKE %s
                            """,
                            (guild_id, f"%:{resource_id}>"),
                        )
                    await self._bump_reaction_index(cursor)
                await connection.commit()
                return changed
            except Exception:
                await connection.rollback()
                raise

    async def _set_reaction_lifecycle(
        self,
        guild_id: int,
        panel_id: str,
        lifecycle: str,
        actor_id: int,
        *,
        channel_id: int | None = None,
        message_id: int | None = None,
        ownership: str | None = None,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    UPDATE reaction_role_panels SET lifecycle = %s,
                        channel_id = COALESCE(%s, channel_id),
                        message_id = COALESCE(%s, message_id),
                        ownership = COALESCE(%s, ownership),
                        route_revision = route_revision + 1, updated_by = %s
                    WHERE guild_id = %s AND panel_id = %s
                    """,
                    (
                        lifecycle,
                        channel_id,
                        message_id,
                        ownership,
                        actor_id,
                        guild_id,
                        panel_id,
                    ),
                )
                if message_id is not None:
                    await cursor.execute(
                        """
                        INSERT INTO reaction_role_panel_messages
                            (panel_id, position, message_id, ownership,
                             applied_revision, repair_state)
                        SELECT panel_id, 0, %s, COALESCE(%s, ownership),
                               applied_revision, 'ready'
                        FROM reaction_role_panels WHERE panel_id = %s
                        ON DUPLICATE KEY UPDATE message_id = VALUES(message_id),
                            ownership = VALUES(ownership), repair_state = 'ready'
                        """,
                        (message_id, ownership, panel_id),
                    )
                if lifecycle in {"deleted", "detached"}:
                    await cursor.execute(
                        "DELETE FROM reaction_role_applied_routes WHERE panel_id = %s",
                        (panel_id,),
                    )
                    await self._bump_reaction_index(cursor)
                elif lifecycle == "active":
                    await self._bump_reaction_index(cursor)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def list_mappings(
        self, panel_id: str, *, include_disabled: bool = False
    ) -> tuple[ReactionMapping, ...]:
        pool = self._require_pool()
        clause = "" if include_disabled else "AND enabled = TRUE"
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                f"""
                SELECT mapping_id, panel_id, emoji_key, role_id, label, position, enabled
                FROM reaction_role_mappings WHERE panel_id = %s {clause}
                ORDER BY position, mapping_id
                """,
                (panel_id,),
            )
            return tuple(self._mapping(row) for row in await cursor.fetchall())

    async def list_reaction_parts(self, panel_id: str) -> tuple[ReactionPanelPart, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, position, message_id, ownership,
                       applied_revision, repair_state, content_hash
                FROM reaction_role_panel_messages
                WHERE panel_id = %s ORDER BY position
                """,
                (panel_id,),
            )
            return tuple(
                ReactionPanelPart(
                    str(row[0]),
                    int(row[1]),
                    int(row[2]),
                    str(row[3]),
                    int(row[4]),
                    str(row[5]),
                    str(row[6]),
                )
                for row in await cursor.fetchall()
            )

    async def list_applied_reaction_routes(self, panel_id: str) -> tuple[ReactionAppliedRoute, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, mapping_id, message_id, emoji_key, role_id,
                       position, applied_revision
                FROM reaction_role_applied_routes
                WHERE panel_id = %s ORDER BY position, mapping_id
                """,
                (panel_id,),
            )
            return tuple(
                ReactionAppliedRoute(
                    str(row[0]),
                    str(row[1]),
                    int(row[2]),
                    str(row[3]),
                    int(row[4]),
                    int(row[5]),
                    int(row[6]),
                )
                for row in await cursor.fetchall()
            )

    async def save_reaction_part(
        self,
        panel_id: str,
        position: int,
        message_id: int,
        ownership: str,
        revision: int,
        content_hash: str,
        *,
        repair_state: str = "pending",
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    INSERT INTO reaction_role_panel_messages
                        (panel_id, position, message_id, ownership,
                         applied_revision, repair_state, content_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE message_id = VALUES(message_id),
                        ownership = VALUES(ownership),
                        repair_state = VALUES(repair_state),
                        content_hash = VALUES(content_hash)
                    """,
                    (
                        panel_id,
                        position,
                        message_id,
                        ownership,
                        revision,
                        repair_state,
                        content_hash,
                    ),
                )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def apply_reaction_document(
        self,
        guild_id: int,
        panel_id: str,
        message_parts: tuple[tuple[int, str, str], ...],
        expected_revision: int,
        actor_id: int,
    ) -> bool:

        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    SELECT route_revision, channel_id
                    FROM reaction_role_panels
                    WHERE guild_id = %s AND panel_id = %s FOR UPDATE
                    """,
                    (guild_id, panel_id),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise LookupError("reaction-role panel does not exist")
                if int(row[0]) != expected_revision:
                    await connection.rollback()
                    return False
                await cursor.execute(
                    """
                    SELECT mapping_id, emoji_key, role_id, position
                    FROM reaction_role_mappings
                    WHERE panel_id = %s AND enabled = TRUE
                    ORDER BY position, mapping_id
                    """,
                    (panel_id,),
                )
                mappings = await cursor.fetchall()
                if len(mappings) > 100 or len(message_parts) != max(1, (len(mappings) + 19) // 20):
                    raise ValueError("invalid reaction-role document")
                await cursor.execute(
                    "DELETE FROM reaction_role_applied_routes WHERE panel_id = %s",
                    (panel_id,),
                )
                for dense_position, mapping in enumerate(mappings):
                    message_id = int(message_parts[dense_position // 20][0])
                    await cursor.execute(
                        """
                        INSERT INTO reaction_role_applied_routes
                            (panel_id, mapping_id, message_id, emoji_key, role_id,
                             position, applied_revision)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            panel_id,
                            mapping[0],
                            message_id,
                            mapping[1],
                            mapping[2],
                            dense_position,
                            expected_revision,
                        ),
                    )
                keep_positions = tuple(range(len(message_parts)))
                await cursor.execute(
                    "SELECT position, message_id, ownership "
                    "FROM reaction_role_panel_messages WHERE panel_id = %s FOR UPDATE",
                    (panel_id,),
                )
                previous = await cursor.fetchall()
                desired_messages = {int(item[0]) for item in message_parts}
                if len(desired_messages) != len(message_parts):
                    raise ValueError("duplicate reaction-role message")
                channel_id = int(row[1])
                for position, message_id, ownership in previous:
                    if int(position) not in keep_positions and str(ownership) == "lumi":
                        await cursor.execute(
                            """
                            INSERT IGNORE INTO reaction_role_message_cleanup
                                (panel_id, message_id, channel_id)
                            VALUES (%s, %s, %s)
                            """,
                            (panel_id, message_id, channel_id),
                        )
                await cursor.execute(
                    "DELETE FROM reaction_role_panel_messages WHERE panel_id = %s",
                    (panel_id,),
                )
                for position, (message_id, ownership, content_hash) in enumerate(message_parts):
                    await cursor.execute(
                        """
                        INSERT INTO reaction_role_panel_messages
                            (panel_id, position, message_id, ownership,
                             applied_revision, repair_state, content_hash)
                        VALUES (%s, %s, %s, %s, %s, 'ready', %s)
                        """,
                        (
                            panel_id,
                            position,
                            message_id,
                            ownership,
                            expected_revision,
                            content_hash,
                        ),
                    )
                await cursor.execute(
                    """
                    UPDATE reaction_role_panels
                    SET message_id = %s, lifecycle = 'active',
                        applied_revision = %s, updated_by = %s
                    WHERE guild_id = %s AND panel_id = %s
                    """,
                    (
                        message_parts[0][0],
                        expected_revision,
                        actor_id,
                        guild_id,
                        panel_id,
                    ),
                )
                await self._bump_reaction_index(cursor)
                await connection.commit()
                return True
            except Exception:
                await connection.rollback()
                raise

    async def mark_reaction_message_missing(
        self, guild_id: int, message_id: int
    ) -> ReactionPanel | None:

        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    SELECT p.panel_id, p.guild_id, p.channel_id, p.message_id,
                           p.lifecycle, p.ownership, p.source, p.route_revision,
                           p.applied_revision
                    FROM reaction_role_panels p
                    JOIN reaction_role_panel_messages part ON part.panel_id = p.panel_id
                    WHERE p.guild_id = %s AND part.message_id = %s FOR UPDATE
                    """,
                    (guild_id, message_id),
                )
                row = await cursor.fetchone()
                if row is None:
                    await connection.rollback()
                    return None
                panel = self._reaction_panel(row)
                await cursor.execute(
                    "DELETE FROM reaction_role_applied_routes "
                    "WHERE panel_id = %s AND message_id = %s",
                    (panel.panel_id, message_id),
                )
                await cursor.execute(
                    "UPDATE reaction_role_panel_messages "
                    "SET repair_state = 'missing' "
                    "WHERE panel_id = %s AND message_id = %s",
                    (panel.panel_id, message_id),
                )
                await cursor.execute(
                    "UPDATE reaction_role_panels SET lifecycle = 'needs_repair' "
                    "WHERE panel_id = %s",
                    (panel.panel_id,),
                )
                await self._bump_reaction_index(cursor)
                await connection.commit()
                return panel
            except Exception:
                await connection.rollback()
                raise

    async def pending_reaction_cleanup(self, panel_id: str) -> tuple[tuple[int, int], ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT channel_id, message_id
                FROM reaction_role_message_cleanup
                WHERE panel_id = %s AND completed_at IS NULL
                ORDER BY created_at, message_id
                """,
                (panel_id,),
            )
            return tuple((int(row[0]), int(row[1])) for row in await cursor.fetchall())

    async def finish_reaction_cleanup(
        self, panel_id: str, message_id: int, error: str | None = None
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            if error is None:
                await cursor.execute(
                    "UPDATE reaction_role_message_cleanup "
                    "SET completed_at = CURRENT_TIMESTAMP(6), last_error = NULL "
                    "WHERE panel_id = %s AND message_id = %s",
                    (panel_id, message_id),
                )
            else:
                await cursor.execute(
                    "UPDATE reaction_role_message_cleanup "
                    "SET attempts = attempts + 1, last_error = %s "
                    "WHERE panel_id = %s AND message_id = %s",
                    (error[:160], panel_id, message_id),
                )
            await connection.commit()

    async def mapping_for_emoji(
        self, guild_id: int, message_id: int, emoji_key: str
    ) -> ReactionMapping | None:
        pool = self._require_pool()
        custom = CUSTOM_EMOJI_ID.fullmatch(emoji_key)
        custom_suffix = f"%:{custom.group(1)}>" if custom else ""
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT r.mapping_id, r.panel_id, r.emoji_key, r.role_id,
                       m.label, r.position, TRUE
                FROM reaction_role_applied_routes r
                JOIN reaction_role_panels p ON p.panel_id = r.panel_id
                JOIN reaction_role_mappings m ON m.mapping_id = r.mapping_id
                WHERE p.guild_id = %s AND r.message_id = %s
                  AND p.lifecycle IN ('active', 'needs_repair')
                  AND (r.emoji_key = %s OR (%s <> '' AND r.emoji_key LIKE %s))
                ORDER BY (r.emoji_key = %s) DESC, r.position
                LIMIT 1
                """,
                (
                    guild_id,
                    message_id,
                    emoji_key,
                    custom_suffix,
                    custom_suffix,
                    emoji_key,
                ),
            )
            row = await cursor.fetchone()
            return self._mapping(row) if row else None

    async def upsert_mapping(
        self,
        mapping: ReactionMapping,
        actor_id: int,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT panel_id FROM reaction_role_panels WHERE panel_id = %s FOR UPDATE",
                    (mapping.panel_id,),
                )
                if await cursor.fetchone() is None:
                    raise LookupError("reaction-role panel does not exist")
                await cursor.execute(
                    "SELECT COUNT(*) FROM reaction_role_mappings "
                    "WHERE panel_id = %s AND enabled = TRUE",
                    (mapping.panel_id,),
                )
                count = int((await cursor.fetchone())[0])
                await cursor.execute(
                    "SELECT mapping_id, position, enabled FROM reaction_role_mappings "
                    "WHERE panel_id = %s AND (emoji_key = %s OR mapping_id = %s) "
                    "ORDER BY (emoji_key = %s) DESC LIMIT 1 FOR UPDATE",
                    (
                        mapping.panel_id,
                        mapping.emoji_key,
                        mapping.mapping_id,
                        mapping.emoji_key,
                    ),
                )
                existing = await cursor.fetchone()
                if (existing is None or not bool(existing[2])) and count >= 100:
                    raise ValueError("Panel full.")
                await cursor.execute(
                    """
                    INSERT INTO reaction_role_mappings
                        (mapping_id, panel_id, emoji_key, role_id, label,
                         position, enabled, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON DUPLICATE KEY UPDATE role_id = VALUES(role_id),
                        emoji_key = VALUES(emoji_key), label = VALUES(label),
                        position = VALUES(position),
                        enabled = TRUE, updated_by = VALUES(updated_by)
                    """,
                    (
                        existing[0] if existing else mapping.mapping_id,
                        mapping.panel_id,
                        mapping.emoji_key,
                        mapping.role_id,
                        mapping.label,
                        int(existing[1]) if existing else mapping.position,
                        actor_id,
                    ),
                )
                await cursor.execute(
                    "UPDATE reaction_role_panels SET route_revision = route_revision + 1, "
                    "updated_by = %s WHERE panel_id = %s",
                    (actor_id, mapping.panel_id),
                )
                await cursor.execute(
                    """
                    INSERT IGNORE INTO reaction_role_applied_routes
                        (panel_id, mapping_id, message_id, emoji_key, role_id,
                         position, applied_revision)
                    SELECT p.panel_id, %s, p.message_id, %s, %s, %s,
                           p.route_revision
                    FROM reaction_role_panels p
                    WHERE p.panel_id = %s AND p.source = 'legacy'
                      AND p.message_id IS NOT NULL AND %s = 0
                    """,
                    (
                        existing[0] if existing else mapping.mapping_id,
                        mapping.emoji_key,
                        mapping.role_id,
                        int(existing[1]) if existing else mapping.position,
                        mapping.panel_id,
                        actor_id,
                    ),
                )
                if cursor.rowcount:
                    await self._bump_reaction_index(cursor)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def disable_mapping(
        self, panel_id: str, mapping_id: str, actor_id: int
    ) -> ReactionMapping | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    SELECT mapping_id, panel_id, emoji_key, role_id, label, position, enabled
                    FROM reaction_role_mappings
                    WHERE panel_id = %s AND mapping_id = %s FOR UPDATE
                    """,
                    (panel_id, mapping_id),
                )
                row = await cursor.fetchone()
                if row is None:
                    await connection.rollback()
                    return None
                await cursor.execute(
                    "UPDATE reaction_role_mappings SET enabled = FALSE, updated_by = %s "
                    "WHERE panel_id = %s AND mapping_id = %s",
                    (actor_id, panel_id, mapping_id),
                )
                await cursor.execute(
                    "UPDATE reaction_role_panels SET route_revision = route_revision + 1, "
                    "updated_by = %s WHERE panel_id = %s",
                    (actor_id, panel_id),
                )
                await self._normalize_mapping_positions(cursor, panel_id)
                await connection.commit()
                return self._mapping(row)
            except Exception:
                await connection.rollback()
                raise

    async def move_mapping(
        self, panel_id: str, mapping_id: str, direction: int, actor_id: int
    ) -> bool:
        mappings = list(await self.list_mappings(panel_id))
        index = next(
            (position for position, item in enumerate(mappings) if item.mapping_id == mapping_id),
            -1,
        )
        target = index + direction
        if index < 0 or target < 0 or target >= len(mappings):
            return False
        mappings[index], mappings[target] = mappings[target], mappings[index]
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for position, mapping in enumerate(mappings):
                    await cursor.execute(
                        "UPDATE reaction_role_mappings SET position = %s, updated_by = %s "
                        "WHERE panel_id = %s AND mapping_id = %s",
                        (position, actor_id, panel_id, mapping.mapping_id),
                    )
                await cursor.execute(
                    "UPDATE reaction_role_panels SET route_revision = route_revision + 1, "
                    "updated_by = %s WHERE panel_id = %s",
                    (actor_id, panel_id),
                )
                await connection.commit()
                return True
            except Exception:
                await connection.rollback()
                raise

    async def create_button_panel(
        self, panel_id: str, guild_id: int, actor_id: int
    ) -> ButtonRolePanel:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    INSERT INTO button_role_panels (panel_id, guild_id, updated_by)
                    VALUES (%s, %s, %s)
                    """,
                    (panel_id, guild_id, actor_id),
                )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise
        panel = await self.get_button_panel(guild_id, panel_id)
        if panel is None:
            raise RuntimeError("button panel was not created")
        return panel

    async def list_button_panels(self, guild_id: int) -> tuple[ButtonRolePanel, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, guild_id, channel_id, lifecycle,
                       desired_revision, applied_revision
                FROM button_role_panels
                WHERE guild_id = %s AND lifecycle <> 'deleted'
                ORDER BY created_at, panel_id
                """,
                (guild_id,),
            )
            return tuple(self._button_panel(row) for row in await cursor.fetchall())

    async def get_button_panel(self, guild_id: int, panel_id: str) -> ButtonRolePanel | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT panel_id, guild_id, channel_id, lifecycle,
                       desired_revision, applied_revision
                FROM button_role_panels
                WHERE guild_id = %s AND panel_id = %s
                """,
                (guild_id, panel_id),
            )
            row = await cursor.fetchone()
            return self._button_panel(row) if row else None

    async def get_button_panel_by_message(
        self, guild_id: int, message_id: int
    ) -> ButtonRolePanel | None:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                """
                SELECT p.panel_id, p.guild_id, p.channel_id, p.lifecycle,
                       p.desired_revision, p.applied_revision
                FROM button_role_panels p
                JOIN button_role_panel_messages m ON m.panel_id = p.panel_id
                WHERE p.guild_id = %s AND m.message_id = %s
                  AND p.lifecycle IN ('active', 'needs_repair')
                """,
                (guild_id, message_id),
            )
            row = await cursor.fetchone()
            return self._button_panel(row) if row else None

    async def set_button_panel_channel(
        self, guild_id: int, panel_id: str, channel_id: int, actor_id: int
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    UPDATE button_role_panels
                    SET channel_id = %s, desired_revision = desired_revision + 1,
                        updated_by = %s
                    WHERE guild_id = %s AND panel_id = %s AND lifecycle <> 'deleted'
                      AND pending_layout_create IS NULL
                    """,
                    (channel_id, actor_id, guild_id, panel_id),
                )
                moved = cursor.rowcount == 1
                if moved:
                    await cursor.execute(
                        "DELETE FROM button_role_panel_messages WHERE panel_id = %s",
                        (panel_id,),
                    )
                await connection.commit()
                return moved
            except Exception:
                await connection.rollback()
                raise

    async def set_button_panel_lifecycle(
        self, guild_id: int, panel_id: str, lifecycle: str, actor_id: int
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    UPDATE button_role_panels
                    SET lifecycle = %s, desired_revision = desired_revision + 1,
                        updated_by = %s
                    WHERE guild_id = %s AND panel_id = %s
                    """,
                    (lifecycle, actor_id, guild_id, panel_id),
                )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def list_button_messages(self, panel_id: str) -> tuple[int, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT message_id FROM button_role_panel_messages "
                "WHERE panel_id = %s ORDER BY position",
                (panel_id,),
            )
            return tuple(int(row[0]) for row in await cursor.fetchall())

    async def pending_button_creation(self, panel: ButtonRolePanel) -> ButtonPanelCreation | None:
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT pending_layout_create FROM button_role_panels "
                "WHERE guild_id = %s AND panel_id = %s",
                (panel.guild_id, panel.panel_id),
            )
            row = await cursor.fetchone()
        if row is None:
            raise LookupError("button panel does not exist")
        if row[0] is None:
            return None
        entry = msgspec.json.decode(row[0], type=ButtonPanelCreation)
        if not entry.valid():
            raise ValueError("Invalid pending layout creation")
        return entry

    async def reserve_button_creation(self, panel: ButtonRolePanel, entry: ButtonPanelCreation) -> bool:
        if not entry.valid() or (entry.channel, entry.revision) != (
            panel.channel_id, panel.desired_revision,
        ):
            return False
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "UPDATE button_role_panels SET pending_layout_create = %s "
                    "WHERE guild_id = %s AND panel_id = %s AND channel_id = %s "
                    "AND desired_revision = %s AND lifecycle <> 'deleted' "
                    "AND pending_layout_create IS NULL AND COALESCE(("
                    "SELECT message_id FROM button_role_panel_messages "
                    "WHERE panel_id = %s AND position = %s"
                    "), 0) = %s",
                    (
                        encode(entry), panel.guild_id, panel.panel_id, entry.channel, entry.revision,
                        panel.panel_id, entry.position, entry.previous,
                    ),
                )
                reserved = cursor.rowcount == 1
                await connection.commit()
                return reserved
            except BaseException:
                await connection.rollback()
                raise

    async def settle_button_creation(
        self, panel: ButtonRolePanel, entry: ButtonPanelCreation, message_id: int | None,
    ) -> bool:

        if not entry.valid() or (message_id is not None and (
            type(message_id) is not int or not 0 < message_id < 2**64
        )):
            return False
        async with self._require_pool().acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT channel_id, pending_layout_create, lifecycle FROM button_role_panels "
                    "WHERE guild_id = %s AND panel_id = %s FOR UPDATE",
                    (panel.guild_id, panel.panel_id),
                )
                row = await cursor.fetchone()
                if (
                    row is None or int(row[0]) != entry.channel or row[1] != encode(entry)
                    or row[2] == "deleted"
                ):
                    await connection.rollback()
                    return False
                if message_id is not None:
                    await cursor.execute(
                        "SELECT message_id FROM button_role_panel_messages "
                        "WHERE panel_id = %s AND position = %s FOR UPDATE",
                        (panel.panel_id, entry.position),
                    )
                    existing = await cursor.fetchone()
                    if (int(existing[0]) if existing else 0) != entry.previous:
                        await connection.rollback()
                        return False
                    if existing:
                        await cursor.execute(
                            "UPDATE button_role_panel_messages SET message_id = %s "
                            "WHERE panel_id = %s AND position = %s",
                            (message_id, panel.panel_id, entry.position),
                        )
                    else:
                        await cursor.execute(
                            "INSERT INTO button_role_panel_messages "
                            "(panel_id, position, message_id) VALUES (%s, %s, %s)",
                            (panel.panel_id, entry.position, message_id),
                        )
                await cursor.execute(
                    "UPDATE button_role_panels SET pending_layout_create = NULL "
                    "WHERE guild_id = %s AND panel_id = %s",
                    (panel.guild_id, panel.panel_id),
                )
                await connection.commit()
                return True
            except BaseException:
                await connection.rollback()
                raise

    async def apply_button_document(
        self,
        guild_id: int,
        panel_id: str,
        message_ids: tuple[int, ...],
        expected_revision: int,
        actor_id: int,
        *,
        expected_channel: int,
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT desired_revision, channel_id, pending_layout_create, lifecycle "
                    "FROM button_role_panels "
                    "WHERE guild_id = %s AND panel_id = %s FOR UPDATE",
                    (guild_id, panel_id),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise LookupError("button panel does not exist")
                if int(row[1]) != expected_channel or row[2] is not None or row[3] == "deleted":
                    await connection.rollback()
                    return False
                revision = int(row[0])
                await cursor.execute(
                    "DELETE FROM button_role_panel_messages WHERE panel_id = %s",
                    (panel_id,),
                )
                for position, message_id in enumerate(message_ids):
                    await cursor.execute(
                        "INSERT INTO button_role_panel_messages "
                        "(panel_id, position, message_id) VALUES (%s, %s, %s)",
                        (panel_id, position, message_id),
                    )
                applied = revision == expected_revision
                if applied:
                    await cursor.execute(
                        """
                        UPDATE button_role_panels
                        SET lifecycle = 'active', applied_revision = %s, updated_by = %s
                        WHERE guild_id = %s AND panel_id = %s
                        """,
                        (expected_revision, actor_id, guild_id, panel_id),
                    )
                await connection.commit()
                return applied
            except Exception:
                await connection.rollback()
                raise

    async def list_button_containers(self, panel_id: str) -> tuple[ButtonRoleContainer, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT container_id, panel_id, position, clear_enabled
                FROM button_role_containers WHERE panel_id = %s
                ORDER BY position, container_id
                """,
                (panel_id,),
            )
            return tuple(self._button_container(row) for row in await cursor.fetchall())

    async def get_button_container(
        self, panel_id: str, container_id: str
    ) -> ButtonRoleContainer | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT container_id, panel_id, position, clear_enabled
                FROM button_role_containers
                WHERE panel_id = %s AND container_id = %s
                """,
                (panel_id, container_id),
            )
            row = await cursor.fetchone()
            return self._button_container(row) if row else None

    async def get_button_container_by_id(self, container_id: str) -> ButtonRoleContainer | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT container_id, panel_id, position, clear_enabled
                FROM button_role_containers WHERE container_id = %s
                """,
                (container_id,),
            )
            row = await cursor.fetchone()
            return self._button_container(row) if row else None

    async def add_button_container(
        self, panel_id: str, container_id: str, actor_id: int
    ) -> ButtonRoleContainer:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT COUNT(*) FROM button_role_containers WHERE panel_id = %s FOR UPDATE",
                    (panel_id,),
                )
                count = int((await cursor.fetchone())[0])
                if count >= 100:
                    raise ValueError("button panel has too many containers")
                await cursor.execute(
                    "INSERT INTO button_role_containers "
                    "(container_id, panel_id, position) VALUES (%s, %s, %s)",
                    (container_id, panel_id, count),
                )
                await self._bump_button_panel(cursor, panel_id, actor_id)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise
        container = await self.get_button_container(panel_id, container_id)
        if container is None:
            raise RuntimeError("button container was not created")
        return container

    async def toggle_button_container_clear(
        self, panel_id: str, container_id: str, actor_id: int
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    UPDATE button_role_containers
                    SET clear_enabled = NOT clear_enabled
                    WHERE panel_id = %s AND container_id = %s
                    """,
                    (panel_id, container_id),
                )
                if not cursor.rowcount:
                    await connection.rollback()
                    return False
                await self._bump_button_panel(cursor, panel_id, actor_id)
                await connection.commit()
                return True
            except Exception:
                await connection.rollback()
                raise

    async def delete_button_container(
        self, panel_id: str, container_id: str, actor_id: int
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "DELETE FROM button_role_containers WHERE panel_id = %s AND container_id = %s",
                    (panel_id, container_id),
                )
                changed = bool(cursor.rowcount)
                if changed:
                    await self._normalize_container_positions(cursor, panel_id)
                    await self._bump_button_panel(cursor, panel_id, actor_id)
                await connection.commit()
                return changed
            except Exception:
                await connection.rollback()
                raise

    async def move_button_container(
        self, panel_id: str, container_id: str, direction: int, actor_id: int
    ) -> bool:
        containers = list(await self.list_button_containers(panel_id))
        index = next(
            (
                position
                for position, item in enumerate(containers)
                if item.container_id == container_id
            ),
            -1,
        )
        target = index + direction
        if index < 0 or target < 0 or target >= len(containers):
            return False
        containers[index], containers[target] = containers[target], containers[index]
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for position, container in enumerate(containers):
                    await cursor.execute(
                        "UPDATE button_role_containers SET position = %s WHERE container_id = %s",
                        (position, container.container_id),
                    )
                await self._bump_button_panel(cursor, panel_id, actor_id)
                await connection.commit()
                return True
            except Exception:
                await connection.rollback()
                raise

    async def list_button_blocks(self, container_id: str) -> tuple[ButtonRoleBlock, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT block_id, container_id, kind, position, text, heading_level,
                       asset_id, role_id, description, emoji_key
                FROM button_role_blocks WHERE container_id = %s
                ORDER BY position, block_id
                """,
                (container_id,),
            )
            return tuple(self._button_block(row) for row in await cursor.fetchall())

    async def count_button_blocks(self, panel_id: str) -> int:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT COUNT(*)
                FROM button_role_blocks b
                JOIN button_role_containers c ON c.container_id = b.container_id
                WHERE c.panel_id = %s
                """,
                (panel_id,),
            )
            return int((await cursor.fetchone())[0])

    async def get_button_block(self, block_id: str) -> ButtonRoleBlock | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT block_id, container_id, kind, position, text, heading_level,
                       asset_id, role_id, description, emoji_key
                FROM button_role_blocks WHERE block_id = %s
                """,
                (block_id,),
            )
            row = await cursor.fetchone()
            return self._button_block(row) if row else None

    async def save_button_block(self, panel_id: str, block: ButtonRoleBlock, actor_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "SELECT panel_id FROM button_role_panels WHERE panel_id = %s FOR UPDATE",
                    (panel_id,),
                )
                if await cursor.fetchone() is None:
                    raise LookupError("button panel does not exist")
                await cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM button_role_blocks b
                    JOIN button_role_containers c ON c.container_id = b.container_id
                    WHERE c.panel_id = %s
                    """,
                    (panel_id,),
                )
                count = int((await cursor.fetchone())[0])
                await cursor.execute(
                    """
                    SELECT b.position
                    FROM button_role_blocks b
                    JOIN button_role_containers c ON c.container_id = b.container_id
                    WHERE b.block_id = %s AND c.panel_id = %s
                    """,
                    (block.block_id, panel_id),
                )
                existing = await cursor.fetchone()
                if existing is None and count >= 100:
                    raise ValueError("Panel full.")
                await cursor.execute(
                    "SELECT COUNT(*) FROM button_role_blocks WHERE container_id = %s",
                    (block.container_id,),
                )
                container_count = int((await cursor.fetchone())[0])
                position = int(existing[0]) if existing else container_count
                await cursor.execute(
                    """
                    INSERT INTO button_role_blocks
                        (block_id, container_id, kind, position, text, heading_level,
                         asset_id, role_id, description, emoji_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE kind = VALUES(kind), text = VALUES(text),
                        heading_level = VALUES(heading_level), asset_id = VALUES(asset_id),
                        role_id = VALUES(role_id), description = VALUES(description),
                        emoji_key = VALUES(emoji_key)
                    """,
                    (
                        block.block_id,
                        block.container_id,
                        block.kind,
                        position,
                        block.text,
                        block.heading_level,
                        block.asset_id,
                        block.role_id,
                        block.description,
                        block.emoji_key,
                    ),
                )
                await self._bump_button_panel(cursor, panel_id, actor_id)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def delete_button_block(
        self, panel_id: str, container_id: str, block_id: str, actor_id: int
    ) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "DELETE FROM button_role_blocks WHERE container_id = %s AND block_id = %s",
                    (container_id, block_id),
                )
                changed = bool(cursor.rowcount)
                if changed:
                    await self._normalize_block_positions(cursor, container_id)
                    await self._bump_button_panel(cursor, panel_id, actor_id)
                await connection.commit()
                return changed
            except Exception:
                await connection.rollback()
                raise

    async def move_button_block(
        self,
        panel_id: str,
        container_id: str,
        block_id: str,
        direction: int,
        actor_id: int,
    ) -> bool:
        blocks = list(await self.list_button_blocks(container_id))
        index = next(
            (position for position, item in enumerate(blocks) if item.block_id == block_id),
            -1,
        )
        target = index + direction
        if index < 0 or target < 0 or target >= len(blocks):
            return False
        blocks[index], blocks[target] = blocks[target], blocks[index]
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for position, block in enumerate(blocks):
                    await cursor.execute(
                        "UPDATE button_role_blocks SET position = %s WHERE block_id = %s",
                        (position, block.block_id),
                    )
                await self._bump_button_panel(cursor, panel_id, actor_id)
                await connection.commit()
                return True
            except Exception:
                await connection.rollback()
                raise

    async def store_button_asset(self, asset: ButtonRoleAsset) -> ButtonRoleAsset:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    INSERT INTO button_role_assets
                        (asset_id, guild_id, sha256, file_name, media_type,
                         width, height, byte_size, payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE asset_id = asset_id
                    """,
                    (
                        asset.asset_id,
                        asset.guild_id,
                        asset.sha256,
                        asset.file_name,
                        asset.media_type,
                        asset.width,
                        asset.height,
                        len(asset.payload),
                        asset.payload,
                    ),
                )
                await cursor.execute(
                    """
                    SELECT asset_id, guild_id, sha256, file_name, media_type,
                           width, height, payload
                    FROM button_role_assets WHERE guild_id = %s AND sha256 = %s
                    """,
                    (asset.guild_id, asset.sha256),
                )
                row = await cursor.fetchone()
                await connection.commit()
                return self._button_asset(row)
            except Exception:
                await connection.rollback()
                raise

    async def get_button_asset(self, asset_id: str) -> ButtonRoleAsset | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT asset_id, guild_id, sha256, file_name, media_type,
                       width, height, payload
                FROM button_role_assets WHERE asset_id = %s
                """,
                (asset_id,),
            )
            row = await cursor.fetchone()
            return self._button_asset(row) if row else None

    async def button_roles_for_container(self, container_id: str) -> tuple[int, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT role_id FROM button_role_blocks "
                "WHERE container_id = %s AND kind = 'role' AND role_id IS NOT NULL "
                "ORDER BY position",
                (container_id,),
            )
            return tuple(dict.fromkeys(int(row[0]) for row in await cursor.fetchall()))

    async def mark_button_resource_missing(
        self, guild_id: int, resource_kind: str, resource_id: int
    ) -> int:
        if resource_kind == "channel":
            join = ""
            predicate = "p.channel_id = %s"
            parameters: tuple[object, ...] = (resource_id,)
        elif resource_kind == "role":
            join = (
                "JOIN button_role_containers c ON c.panel_id = p.panel_id "
                "JOIN button_role_blocks b ON b.container_id = c.container_id"
            )
            predicate = "b.kind = 'role' AND b.role_id = %s"
            parameters = (resource_id,)
        elif resource_kind == "emoji":
            join = (
                "JOIN button_role_containers c ON c.panel_id = p.panel_id "
                "JOIN button_role_blocks b ON b.container_id = c.container_id"
            )
            predicate = "b.kind = 'role' AND b.emoji_key LIKE %s"
            parameters = (f"%:{resource_id}>",)
        else:
            raise ValueError("unsupported button-role resource")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    f"""
                    UPDATE button_role_panels p {join}
                    SET p.lifecycle = 'needs_repair',
                        p.desired_revision = p.desired_revision + 1,
                        p.updated_by = 0
                    WHERE p.guild_id = %s AND p.lifecycle NOT IN ('deleted', 'draft')
                      AND {predicate}
                    """,
                    (guild_id, *parameters),
                )
                changed = int(cursor.rowcount)
                await connection.commit()
                return changed
            except Exception:
                await connection.rollback()
                raise

    @staticmethod
    async def _bump_button_panel(
        cursor: asyncmy.cursors.Cursor, panel_id: str, actor_id: int
    ) -> None:
        await cursor.execute(
            "UPDATE button_role_panels "
            "SET desired_revision = desired_revision + 1, updated_by = %s "
            "WHERE panel_id = %s",
            (actor_id, panel_id),
        )

    @staticmethod
    async def _normalize_container_positions(cursor: asyncmy.cursors.Cursor, panel_id: str) -> None:
        await cursor.execute(
            "SELECT container_id FROM button_role_containers "
            "WHERE panel_id = %s ORDER BY position, container_id",
            (panel_id,),
        )
        for position, row in enumerate(await cursor.fetchall()):
            await cursor.execute(
                "UPDATE button_role_containers SET position = %s WHERE container_id = %s",
                (position, row[0]),
            )

    @staticmethod
    async def _normalize_block_positions(cursor: asyncmy.cursors.Cursor, container_id: str) -> None:
        await cursor.execute(
            "SELECT block_id FROM button_role_blocks "
            "WHERE container_id = %s ORDER BY position, block_id",
            (container_id,),
        )
        for position, row in enumerate(await cursor.fetchall()):
            await cursor.execute(
                "UPDATE button_role_blocks SET position = %s WHERE block_id = %s",
                (position, row[0]),
            )

    @staticmethod
    async def _normalize_mapping_positions(cursor: asyncmy.cursors.Cursor, panel_id: str) -> None:
        await cursor.execute(
            "SELECT mapping_id FROM reaction_role_mappings "
            "WHERE panel_id = %s AND enabled = TRUE ORDER BY position, mapping_id",
            (panel_id,),
        )
        for position, row in enumerate(await cursor.fetchall()):
            await cursor.execute(
                "UPDATE reaction_role_mappings SET position = %s WHERE mapping_id = %s",
                (position, row[0]),
            )

    @staticmethod
    async def _bump_reaction_index(cursor: asyncmy.cursors.Cursor) -> None:
        await cursor.execute(
            "UPDATE panel_route_revisions SET revision = LAST_INSERT_ID(revision + 1) "
            "WHERE topic = 'reaction_roles'"
        )
        await cursor.execute(
            "SELECT revision FROM panel_route_revisions WHERE topic = 'reaction_roles'"
        )
        revision = int((await cursor.fetchone())[0])
        await cursor.execute(
            """
            SELECT DISTINCT r.message_id
            FROM reaction_role_applied_routes r
            JOIN reaction_role_panels p ON p.panel_id = r.panel_id
            WHERE p.lifecycle IN ('active', 'needs_repair')
            ORDER BY r.message_id
            """
        )
        message_ids = tuple(int(row[0]) for row in await cursor.fetchall())
        snapshot = ReactionRouteSnapshot(
            schema=1,
            revision=revision,
            message_ids=message_ids,
            published_at_ms=int(time.time() * 1000),
        )
        await cursor.execute(
            """
            INSERT IGNORE INTO panel_publication_outbox (topic, revision, payload)
            VALUES ('reaction_roles', %s, %s)
            """,
            (revision, encode(snapshot)),
        )

    async def pending_publications(self, limit: int = 100) -> tuple[tuple[int, bytes], ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                "SELECT id, payload FROM panel_publication_outbox "
                "WHERE published_at IS NULL ORDER BY id LIMIT %s",
                (limit,),
            )
            return tuple((int(row[0]), bytes(row[1])) for row in await cursor.fetchall())

    async def mark_publication_sent(self, outbox_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "UPDATE panel_publication_outbox SET published_at = CURRENT_TIMESTAMP(6) "
                "WHERE id = %s AND published_at IS NULL",
                (outbox_id,),
            )
            await connection.commit()

    async def migration_status(self, source_digest: str) -> str | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT status FROM panel_migration_runs WHERE source_digest = %s",
                (source_digest,),
            )
            row = await cursor.fetchone()
            return str(row[0]) if row else None

    async def begin_migration(
        self,
        run_id: str,
        source_digest: str,
        verification_count: int,
        reaction_mapping_count: int,
        details: dict[str, object],
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO panel_migration_runs
                    (run_id, source_digest, verification_count,
                     reaction_mapping_count, status, details_json)
                VALUES (%s, %s, %s, %s, 'running', %s)
                ON DUPLICATE KEY UPDATE details_json = VALUES(details_json)
                """,
                (
                    run_id,
                    source_digest,
                    verification_count,
                    reaction_mapping_count,
                    json.dumps(details, separators=(",", ":")),
                ),
            )
            await connection.commit()

    async def finish_migration(self, source_digest: str, details: dict[str, object]) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE panel_migration_runs
                SET status = 'imported', details_json = %s,
                    completed_at = CURRENT_TIMESTAMP(6)
                WHERE source_digest = %s
                """,
                (json.dumps(details, separators=(",", ":")), source_digest),
            )
            await connection.commit()

    async def record_preflight(
        self,
        module: str,
        panel_id: str,
        classification: str,
        snapshot: dict[str, object],
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO panel_migration_items
                    (module_key, panel_id, classification, snapshot_json)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE classification = VALUES(classification),
                    snapshot_json = VALUES(snapshot_json)
                """,
                (
                    module,
                    panel_id,
                    classification,
                    json.dumps(snapshot, separators=(",", ":")),
                ),
            )
            await connection.commit()

    async def migration_classification(self, module: str, panel_id: str) -> str | None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT classification FROM panel_migration_items "
                "WHERE module_key = %s AND panel_id = %s",
                (module, panel_id),
            )
            row = await cursor.fetchone()
            return str(row[0]) if row else None

    async def record_preview(
        self, module: str, panel_id: str, channel_id: int, message_id: int
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE panel_migration_items
                SET preview_channel_id = %s, preview_message_id = %s
                WHERE module_key = %s AND panel_id = %s
                """,
                (channel_id, message_id, module, panel_id),
            )
            await connection.commit()

    async def mark_converted(self, module: str, panel_id: str, snapshot: dict[str, object]) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE panel_migration_items
                SET final_snapshot_json = %s, converted_at = CURRENT_TIMESTAMP(6)
                WHERE module_key = %s AND panel_id = %s
                """,
                (json.dumps(snapshot, separators=(",", ":")), module, panel_id),
            )
            await connection.commit()

    @staticmethod
    def _verification(row: tuple[object, ...]) -> VerificationPanel:
        return VerificationPanel(
            str(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]) if row[3] is not None else None,
            str(row[4]),
            int(row[5]),
            int(row[6]),
            str(row[7]),
            str(row[8]),
            str(row[9]),
            int(row[10]),
        )

    @staticmethod
    def _reaction_panel(row: tuple[object, ...]) -> ReactionPanel:
        return ReactionPanel(
            str(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]) if row[3] is not None else None,
            str(row[4]),
            str(row[5]),
            str(row[6]),
            int(row[7]),
            int(row[8]),
        )

    @staticmethod
    def _mapping(row: tuple[object, ...]) -> ReactionMapping:
        return ReactionMapping(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            int(row[3]),
            str(row[4]) if row[4] is not None else None,
            int(row[5]),
            bool(row[6]),
        )

    @staticmethod
    def _button_panel(row: tuple[object, ...]) -> ButtonRolePanel:
        return ButtonRolePanel(
            str(row[0]),
            int(row[1]),
            int(row[2]),
            str(row[3]),
            int(row[4]),
            int(row[5]),
        )

    @staticmethod
    def _button_container(row: tuple[object, ...]) -> ButtonRoleContainer:
        return ButtonRoleContainer(
            str(row[0]),
            str(row[1]),
            int(row[2]),
            bool(row[3]),
        )

    @staticmethod
    def _button_block(row: tuple[object, ...]) -> ButtonRoleBlock:
        return ButtonRoleBlock(
            block_id=str(row[0]),
            container_id=str(row[1]),
            kind=str(row[2]),
            position=int(row[3]),
            text=str(row[4]) if row[4] is not None else None,
            heading_level=int(row[5]) if row[5] is not None else None,
            asset_id=str(row[6]) if row[6] is not None else None,
            role_id=int(row[7]) if row[7] is not None else None,
            description=str(row[8]) if row[8] is not None else None,
            emoji_key=str(row[9]) if row[9] is not None else None,
        )

    @staticmethod
    def _button_asset(row: tuple[object, ...]) -> ButtonRoleAsset:
        return ButtonRoleAsset(
            asset_id=str(row[0]),
            guild_id=int(row[1]),
            sha256=str(row[2]),
            file_name=str(row[3]),
            media_type=str(row[4]),
            width=int(row[5]),
            height=int(row[6]),
            payload=bytes(row[7]),
        )
