from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.contracts import ChannelGrant, GuildPolicySnapshot, RoleGrant, encode
from lumi.storage.checkpoint import create_pool, read_cursor

SETTINGS_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id BIGINT UNSIGNED PRIMARY KEY,
        prefix VARCHAR(5) NOT NULL,
        revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
        updated_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS guild_module_state (
        guild_id BIGINT UNSIGNED NOT NULL,
        module_key VARCHAR(64) NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        initialized BOOLEAN NOT NULL DEFAULT FALSE,
        updated_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, module_key),
        CONSTRAINT fk_module_guild FOREIGN KEY (guild_id)
            REFERENCES guild_settings(guild_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS guild_command_state (
        guild_id BIGINT UNSIGNED NOT NULL,
        module_key VARCHAR(64) NOT NULL,
        acl_key VARCHAR(128) NOT NULL,
        state ENUM('enabled', 'disabled', 'pending') NOT NULL DEFAULT 'pending',
        updated_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, module_key, acl_key),
        CONSTRAINT fk_command_guild FOREIGN KEY (guild_id)
            REFERENCES guild_settings(guild_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS guild_role_grants (
        guild_id BIGINT UNSIGNED NOT NULL,
        scope ENUM('module', 'command') NOT NULL,
        module_key VARCHAR(64) NOT NULL,
        acl_key VARCHAR(128) NOT NULL DEFAULT '',
        role_id BIGINT UNSIGNED NOT NULL,
        updated_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, scope, module_key, acl_key, role_id),
        CONSTRAINT fk_role_guild FOREIGN KEY (guild_id)
            REFERENCES guild_settings(guild_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS guild_channel_grants (
        guild_id BIGINT UNSIGNED NOT NULL,
        scope ENUM('module', 'command') NOT NULL,
        module_key VARCHAR(64) NOT NULL,
        acl_key VARCHAR(128) NOT NULL DEFAULT '',
        target_id BIGINT UNSIGNED NOT NULL,
        target_type ENUM('channel', 'category') NOT NULL,
        updated_by BIGINT UNSIGNED NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, scope, module_key, acl_key, target_id),
        CONSTRAINT fk_channel_guild FOREIGN KEY (guild_id)
            REFERENCES guild_settings(guild_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS guild_policy_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        guild_id BIGINT UNSIGNED NOT NULL,
        revision BIGINT UNSIGNED NOT NULL,
        payload MEDIUMBLOB NOT NULL,
        created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        published_at TIMESTAMP(6) NULL,
        UNIQUE KEY uq_policy_revision (guild_id, revision),
        KEY ix_policy_unpublished (published_at, id),
        CONSTRAINT fk_outbox_guild FOREIGN KEY (guild_id)
            REFERENCES guild_settings(guild_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
)


@dataclass(frozen=True, slots=True)
class ModuleSettings:
    enabled: bool
    initialized: bool
    roles: tuple[int, ...]
    channels: tuple[int, ...]
    categories: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CommandSettings:
    state: str
    roles: tuple[int, ...]
    channels: tuple[int, ...]
    categories: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class GuildSettingsState:
    guild_id: int
    prefix: str
    revision: int
    modules: dict[str, ModuleSettings]
    commands: dict[tuple[str, str], CommandSettings]


class SettingsStore(Protocol):
    async def migrate(self) -> None: ...

    async def ensure_guild(self, guild_id: int, prefix: str, actor_id: int) -> None: ...

    async def read_state(self, guild_id: int, prefix: str, actor_id: int) -> GuildSettingsState: ...

    async def sync_catalog(self, guild_id: int, commands: dict[str, tuple[str, ...]]) -> None: ...

    async def guild_ids(self) -> tuple[int, ...]: ...

    async def set_prefix(
        self, guild_id: int, prefix: str, actor_id: int
    ) -> GuildPolicySnapshot: ...

    async def set_module_enabled(
        self,
        guild_id: int,
        module: str,
        command_keys: tuple[str, ...],
        enabled: bool,
        actor_id: int,
    ) -> GuildPolicySnapshot: ...

    async def set_command_enabled(
        self, guild_id: int, module: str, acl_key: str, enabled: bool, actor_id: int
    ) -> GuildPolicySnapshot: ...

    async def set_roles(
        self,
        guild_id: int,
        scope: str,
        module: str,
        acl_key: str,
        role_ids: tuple[int, ...],
        actor_id: int,
    ) -> GuildPolicySnapshot: ...

    async def set_channels(
        self,
        guild_id: int,
        scope: str,
        module: str,
        acl_key: str,
        channels: tuple[int, ...],
        categories: tuple[int, ...],
        actor_id: int,
    ) -> GuildPolicySnapshot: ...


class MySQLSettingsStore:
    def __init__(self, dsn: str) -> None:
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("settings DSN must use mysql:// or mariadb://")
        self._settings = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "db": parsed.path.removeprefix("/"),
            "autocommit": False,
            "minsize": 1,
            "maxsize": 6,
        }
        self._pool: asyncmy.Pool | None = None

    async def connect(self) -> None:
        self._pool = await create_pool(**self._settings)

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    def _require_pool(self) -> asyncmy.Pool:
        if self._pool is None:
            raise RuntimeError("settings store is not connected")
        return self._pool

    async def migrate(self) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for statement in SETTINGS_SCHEMA:
                    await cursor.execute(statement)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def ensure_guild(self, guild_id: int, prefix: str, actor_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await cursor.execute(
                    """
                    INSERT IGNORE INTO guild_settings
                        (guild_id, prefix, revision, updated_by)
                    VALUES (%s, %s, 1, %s)
                    """,
                    (guild_id, prefix, actor_id),
                )
                if cursor.rowcount:
                    snapshot = await self._snapshot(cursor, guild_id)
                    await self._insert_outbox(cursor, snapshot)
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def import_prefix(self, guild_id: int, prefix: str) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await cursor.execute(
                    """
                    INSERT IGNORE INTO guild_settings
                        (guild_id, prefix, revision, updated_by)
                    VALUES (%s, %s, 1, 0)
                    """,
                    (guild_id, prefix),
                )
                inserted = cursor.rowcount == 1
                if inserted:
                    snapshot = await self._snapshot(cursor, guild_id)
                    await self._insert_outbox(cursor, snapshot)
                await connection.commit()
                return inserted
            except Exception:
                await connection.rollback()
                raise

    async def sync_catalog(self, guild_id: int, commands: dict[str, tuple[str, ...]]) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                for module, keys in commands.items():
                    await cursor.execute(
                        """
                        SELECT initialized FROM guild_module_state
                        WHERE guild_id = %s AND module_key = %s
                        """,
                        (guild_id, module),
                    )
                    row = await cursor.fetchone()
                    if not row or not bool(row[0]):
                        continue
                    for acl_key in keys:
                        await cursor.execute(
                            """
                            INSERT IGNORE INTO guild_command_state
                                (guild_id, module_key, acl_key, state, updated_by)
                            VALUES (%s, %s, %s, 'pending', 0)
                            """,
                            (guild_id, module, acl_key),
                        )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def read_state(self, guild_id: int, prefix: str, actor_id: int) -> GuildSettingsState:
        await self.ensure_guild(guild_id, prefix, actor_id)
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            return await self._read_state(cursor, guild_id)

    async def guild_ids(self) -> tuple[int, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute("SELECT guild_id FROM guild_settings ORDER BY guild_id")
            return tuple(int(row[0]) for row in await cursor.fetchall())

    async def set_prefix(self, guild_id: int, prefix: str, actor_id: int) -> GuildPolicySnapshot:
        return await self._mutate(
            guild_id,
            actor_id,
            "UPDATE guild_settings SET prefix = %s WHERE guild_id = %s",
            (prefix, guild_id),
        )

    async def set_module_enabled(
        self,
        guild_id: int,
        module: str,
        command_keys: tuple[str, ...],
        enabled: bool,
        actor_id: int,
    ) -> GuildPolicySnapshot:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await self._lock_guild(cursor, guild_id)
                await cursor.execute(
                    """
                    SELECT enabled, initialized FROM guild_module_state
                    WHERE guild_id = %s AND module_key = %s FOR UPDATE
                    """,
                    (guild_id, module),
                )
                row = await cursor.fetchone()
                current_enabled = bool(row[0]) if row else False
                initialized = bool(row[1]) if row else False
                if row and current_enabled == enabled and (initialized or not enabled):
                    snapshot = await self._snapshot(cursor, guild_id)
                    await connection.commit()
                    return snapshot
                if row:
                    await cursor.execute(
                        """
                        UPDATE guild_module_state
                        SET enabled = %s, initialized = %s, updated_by = %s
                        WHERE guild_id = %s AND module_key = %s
                        """,
                        (
                            enabled,
                            initialized or enabled,
                            actor_id,
                            guild_id,
                            module,
                        ),
                    )
                else:
                    await cursor.execute(
                        """
                        INSERT INTO guild_module_state
                            (guild_id, module_key, enabled, initialized, updated_by)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (guild_id, module, enabled, enabled, actor_id),
                    )
                if enabled and not initialized:
                    for acl_key in command_keys:
                        await cursor.execute(
                            """
                            INSERT IGNORE INTO guild_command_state
                                (guild_id, module_key, acl_key, state, updated_by)
                            VALUES (%s, %s, %s, 'enabled', %s)
                            """,
                            (guild_id, module, acl_key, actor_id),
                        )
                return await self._finish(connection, cursor, guild_id, actor_id)
            except Exception:
                await connection.rollback()
                raise

    async def set_command_enabled(
        self, guild_id: int, module: str, acl_key: str, enabled: bool, actor_id: int
    ) -> GuildPolicySnapshot:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await self._lock_guild(cursor, guild_id)
                await cursor.execute(
                    """
                    SELECT state FROM guild_command_state
                    WHERE guild_id = %s AND module_key = %s AND acl_key = %s FOR UPDATE
                    """,
                    (guild_id, module, acl_key),
                )
                row = await cursor.fetchone()
                next_state = "enabled" if enabled else "disabled"
                if row and row[0] == next_state:
                    snapshot = await self._snapshot(cursor, guild_id)
                    await connection.commit()
                    return snapshot
                await cursor.execute(
                    """
                    INSERT INTO guild_command_state
                        (guild_id, module_key, acl_key, state, updated_by)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE state = VALUES(state), updated_by = VALUES(updated_by)
                    """,
                    (guild_id, module, acl_key, next_state, actor_id),
                )
                return await self._finish(connection, cursor, guild_id, actor_id)
            except Exception:
                await connection.rollback()
                raise

    async def set_roles(
        self,
        guild_id: int,
        scope: str,
        module: str,
        acl_key: str,
        role_ids: tuple[int, ...],
        actor_id: int,
    ) -> GuildPolicySnapshot:
        return await self._replace_grants(
            guild_id,
            scope,
            module,
            acl_key,
            tuple(sorted(set(role_ids))),
            actor_id,
            table="guild_role_grants",
        )

    async def set_channels(
        self,
        guild_id: int,
        scope: str,
        module: str,
        acl_key: str,
        channels: tuple[int, ...],
        categories: tuple[int, ...],
        actor_id: int,
    ) -> GuildPolicySnapshot:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await self._lock_guild(cursor, guild_id)
                await cursor.execute(
                    """
                    DELETE FROM guild_channel_grants
                    WHERE guild_id = %s AND scope = %s AND module_key = %s AND acl_key = %s
                    """,
                    (guild_id, scope, module, acl_key),
                )
                for target_id, target_type in (
                    *((value, "channel") for value in sorted(set(channels))),
                    *((value, "category") for value in sorted(set(categories))),
                ):
                    await cursor.execute(
                        """
                        INSERT INTO guild_channel_grants
                            (guild_id, scope, module_key, acl_key, target_id,
                             target_type, updated_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (guild_id, scope, module, acl_key, target_id, target_type, actor_id),
                    )
                return await self._finish(connection, cursor, guild_id, actor_id)
            except Exception:
                await connection.rollback()
                raise

    async def _replace_grants(
        self,
        guild_id: int,
        scope: str,
        module: str,
        acl_key: str,
        identifiers: tuple[int, ...],
        actor_id: int,
        *,
        table: str,
    ) -> GuildPolicySnapshot:
        if table != "guild_role_grants":
            raise ValueError("unsupported grant table")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await self._lock_guild(cursor, guild_id)
                await cursor.execute(
                    """
                    DELETE FROM guild_role_grants
                    WHERE guild_id = %s AND scope = %s AND module_key = %s AND acl_key = %s
                    """,
                    (guild_id, scope, module, acl_key),
                )
                for role_id in identifiers:
                    await cursor.execute(
                        """
                        INSERT INTO guild_role_grants
                            (guild_id, scope, module_key, acl_key, role_id, updated_by)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (guild_id, scope, module, acl_key, role_id, actor_id),
                    )
                return await self._finish(connection, cursor, guild_id, actor_id)
            except Exception:
                await connection.rollback()
                raise

    async def _mutate(
        self, guild_id: int, actor_id: int, query: str, parameters: tuple[object, ...]
    ) -> GuildPolicySnapshot:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await connection.begin()
                await self._lock_guild(cursor, guild_id)
                await cursor.execute(query, parameters)
                return await self._finish(connection, cursor, guild_id, actor_id)
            except Exception:
                await connection.rollback()
                raise

    async def _lock_guild(self, cursor: asyncmy.cursors.Cursor, guild_id: int) -> None:
        await cursor.execute(
            "SELECT revision FROM guild_settings WHERE guild_id = %s FOR UPDATE", (guild_id,)
        )
        if await cursor.fetchone() is None:
            raise LookupError("guild settings do not exist")

    async def _finish(
        self,
        connection: asyncmy.Connection,
        cursor: asyncmy.cursors.Cursor,
        guild_id: int,
        actor_id: int,
    ) -> GuildPolicySnapshot:
        await cursor.execute(
            """
            UPDATE guild_settings
            SET revision = revision + 1, updated_by = %s
            WHERE guild_id = %s
            """,
            (actor_id, guild_id),
        )
        snapshot = await self._snapshot(cursor, guild_id)
        await self._insert_outbox(cursor, snapshot)
        await connection.commit()
        return snapshot

    @staticmethod
    async def _insert_outbox(
        cursor: asyncmy.cursors.Cursor, snapshot: GuildPolicySnapshot
    ) -> None:
        await cursor.execute(
            """
            INSERT IGNORE INTO guild_policy_outbox (guild_id, revision, payload)
            VALUES (%s, %s, %s)
            """,
            (snapshot.guild_id, snapshot.revision, encode(snapshot)),
        )

    @staticmethod
    async def _snapshot(cursor: asyncmy.cursors.Cursor, guild_id: int) -> GuildPolicySnapshot:
        state = await MySQLSettingsStore._read_state(cursor, guild_id)
        enabled_modules = tuple(
            sorted(module for module, settings in state.modules.items() if settings.enabled)
        )
        enabled_commands = tuple(
            sorted(
                f"{module}:{acl_key}"
                for (module, acl_key), settings in state.commands.items()
                if settings.state == "enabled"
            )
        )
        module_roles = tuple(
            RoleGrant(module, settings.roles)
            for module, settings in sorted(state.modules.items())
            if settings.roles
        )
        command_roles = tuple(
            RoleGrant(f"{module}:{acl_key}", settings.roles)
            for (module, acl_key), settings in sorted(state.commands.items())
            if settings.roles
        )
        module_channels = tuple(
            ChannelGrant(module, settings.channels, settings.categories)
            for module, settings in sorted(state.modules.items())
            if settings.channels or settings.categories
        )
        command_channels = tuple(
            ChannelGrant(f"{module}:{acl_key}", settings.channels, settings.categories)
            for (module, acl_key), settings in sorted(state.commands.items())
            if settings.channels or settings.categories
        )
        return GuildPolicySnapshot(
            schema=2,
            guild_id=guild_id,
            revision=state.revision,
            prefix=state.prefix,
            enabled_modules=enabled_modules,
            enabled_commands=enabled_commands,
            module_roles=module_roles,
            command_roles=command_roles,
            module_channels=module_channels,
            command_channels=command_channels,
            published_at_ms=int(time.time() * 1000),
        )

    @staticmethod
    async def _read_state(
        cursor: asyncmy.cursors.Cursor, guild_id: int
    ) -> GuildSettingsState:
        await cursor.execute(
            "SELECT prefix, revision FROM guild_settings WHERE guild_id = %s", (guild_id,)
        )
        guild = await cursor.fetchone()
        if guild is None:
            raise LookupError("guild settings do not exist")
        await cursor.execute(
            """
            SELECT module_key, enabled, initialized
            FROM guild_module_state WHERE guild_id = %s
            """,
            (guild_id,),
        )
        module_rows = await cursor.fetchall()
        await cursor.execute(
            """
            SELECT module_key, acl_key, state
            FROM guild_command_state WHERE guild_id = %s
            """,
            (guild_id,),
        )
        command_rows = await cursor.fetchall()
        await cursor.execute(
            """
            SELECT scope, module_key, acl_key, role_id
            FROM guild_role_grants WHERE guild_id = %s
            """,
            (guild_id,),
        )
        role_rows = await cursor.fetchall()
        await cursor.execute(
            """
            SELECT scope, module_key, acl_key, target_id, target_type
            FROM guild_channel_grants WHERE guild_id = %s
            """,
            (guild_id,),
        )
        channel_rows = await cursor.fetchall()

        module_roles: dict[str, list[int]] = {}
        command_roles: dict[tuple[str, str], list[int]] = {}
        for scope, module, acl_key, role_id in role_rows:
            if scope == "module":
                module_roles.setdefault(str(module), []).append(int(role_id))
            else:
                command_roles.setdefault((str(module), str(acl_key)), []).append(int(role_id))
        module_channels: dict[str, tuple[list[int], list[int]]] = {}
        command_channels: dict[tuple[str, str], tuple[list[int], list[int]]] = {}
        for scope, module, acl_key, target_id, target_type in channel_rows:
            target = int(target_id)
            mapping = module_channels if scope == "module" else command_channels
            key = str(module) if scope == "module" else (str(module), str(acl_key))
            channels, categories = mapping.setdefault(key, ([], []))
            (categories if target_type == "category" else channels).append(target)

        modules = {
            str(module): ModuleSettings(
                bool(enabled),
                bool(initialized),
                tuple(sorted(module_roles.get(str(module), []))),
                tuple(sorted(module_channels.get(str(module), ([], []))[0])),
                tuple(sorted(module_channels.get(str(module), ([], []))[1])),
            )
            for module, enabled, initialized in module_rows
        }
        commands = {
            (str(module), str(acl_key)): CommandSettings(
                str(state),
                tuple(sorted(command_roles.get((str(module), str(acl_key)), []))),
                tuple(sorted(command_channels.get((str(module), str(acl_key)), ([], []))[0])),
                tuple(sorted(command_channels.get((str(module), str(acl_key)), ([], []))[1])),
            )
            for module, acl_key, state in command_rows
        }
        return GuildSettingsState(
            guild_id=guild_id,
            prefix=str(guild[0]),
            revision=int(guild[1]),
            modules=modules,
            commands=commands,
        )

    async def pending_outbox(self, limit: int = 100) -> tuple[tuple[int, bytes], ...]:
        async with read_cursor(self._require_pool()) as cursor:
            await cursor.execute(
                """
                SELECT id, payload FROM guild_policy_outbox
                WHERE published_at IS NULL ORDER BY id LIMIT %s
                """,
                (limit,),
            )
            return tuple((int(row[0]), bytes(row[1])) for row in await cursor.fetchall())

    async def mark_published(self, outbox_id: int) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.cursor() as cursor:
            try:
                await cursor.execute(
                    """
                    UPDATE guild_policy_outbox SET published_at = CURRENT_TIMESTAMP(6)
                    WHERE id = %s AND published_at IS NULL
                    """,
                    (outbox_id,),
                )
                await connection.commit()
            except Exception:
                await connection.rollback()
                raise
