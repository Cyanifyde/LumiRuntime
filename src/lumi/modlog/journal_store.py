from __future__ import annotations

import json

from lumi.contracts.native_mutations import DELETE_MESSAGES, EDIT_NICKNAME
from lumi.moderation.rename_store import MySQLRenameStore
from lumi.storage.block_policy import relation as block_subjects

from .models import TTL
from .mutations import scope

PAGE_PER_SOURCE = 25
SOURCES = (
    ("moderation", "moderation_operations", "created_ms", "modlog_revision", ("done",)),
    ("purge", "moderation_purge_jobs", "updated_ms", "revision", ("done", "cancelled")),
    (
        "rename", "moderation_rename_batches", "updated_ms", "revision",
        ("ready", "done", "cancelled", "expired"),
    ),
    ("imageguard", "imageguard_incidents", "created_ms", "modlog_revision", ("done",)),
)
MAX_SUBJECTS = len(SOURCES) * PAGE_PER_SOURCE * 2


class JournalReader:
    def __init__(self, owner, *, scoped):
        self.owner = owner
        self.rename = MySQLRenameStore(owner)
        self.scoped = scoped

    async def entries(self, now, guilds):
        if not guilds:
            return []
        guilds = json.dumps(sorted(guilds))
        result = []
        async with self.owner.pool.acquire() as connection, connection.cursor() as query:
            try:
                for source, table, clock, version, states in SOURCES:
                    marker = "NULLIF(JSON_VALUE(s.document,'$.modlog_id'),'')"
                    key = (
                        f"CONCAT({marker},':',JSON_UNQUOTE(JSON_EXTRACT(s.document,'$.mode')))"
                        if source == "rename" else marker
                    )
                    revision = (
                        "COALESCE(CAST(JSON_VALUE(s.document,'$.modlog_revision') AS UNSIGNED),"
                        "s.created_ms)"
                        if version == "modlog_revision" else f"s.{version}"
                    )
                    state = (
                        "s.state" if source in {"purge", "rename"}
                        else "JSON_VALUE(s.document,'$.status')"
                    )
                    placeholders = ",".join("%s" for _ in states)
                    await query.execute(
                        f"SELECT s.document FROM {table} s "
                        "JOIN JSON_TABLE(%s,'$[*]' COLUMNS(guild_id BIGINT UNSIGNED PATH '$')) g "
                        "ON g.guild_id=s.guild_id LEFT JOIN modlog_documents j "
                        f"ON j.guild_id=s.guild_id AND j.kind='journal' AND j.id={key} "
                        f"WHERE s.{clock}>=%s AND {marker} IS NOT NULL "
                        f"AND {state} IN ({placeholders}) "
                        f"AND (j.id IS NULL OR j.revision<{revision}) "
                        f"ORDER BY s.{clock},s.id LIMIT {PAGE_PER_SOURCE}",
                        (guilds, now - TTL, *states),
                    )
                    result.extend((source, json.loads(row[0])) for row in await query.fetchall())
            finally:
                await connection.rollback()
        return result

    async def rename_counts(self, row):
        return await self.rename.counts(row)

    async def tracked_mutations(self, attempts):
        wanted = {scope(attempt) for attempt in attempts}
        if not wanted:
            return set()

        approved = set()
        async with self.owner.pool.acquire() as connection, connection.cursor() as query:
            try:
                for kind, table, channel in (
                    (DELETE_MESSAGES, "moderation_purge_jobs", "channel_id"),
                    (EDIT_NICKNAME, "moderation_rename_batches", "0"),
                ):
                    ids = sorted({
                        operation for effect, _, operation, _, _ in wanted if effect == kind
                    })
                    if not ids:
                        continue
                    placeholders = ",".join("%s" for _ in ids)
                    await query.execute(
                        f"SELECT id,guild_id,JSON_VALUE(document,'$.actor'),{channel} FROM {table} "
                        f"WHERE id IN ({placeholders}) "
                        "AND NULLIF(JSON_VALUE(document,'$.modlog_id'),'') IS NOT NULL",
                        tuple(ids),
                    )
                    for operation, guild, actor, destination in await query.fetchall():
                        identity = kind, int(guild), operation, int(actor or 0), int(destination)
                        if identity in wanted:
                            approved.add(identity)
            finally:
                await connection.rollback()
        return approved

    async def warning_subject(self, guild, code):
        async with self.owner.pool.acquire() as connection, connection.cursor() as query:
            try:
                await query.execute(
                    "SELECT user_id FROM moderation_warnings WHERE guild_id=%s AND id=%s",
                    (guild, code),
                )
                row = await query.fetchone()
                return int(row[0]) if row is not None else 0
            finally:
                await connection.rollback()

    async def blocked(self, subjects):
        if not subjects:
            return frozenset()
        if len(subjects) > MAX_SUBJECTS:
            raise ValueError("Journal subject batch exceeds source bounds.")
        relation = block_subjects(scoped=self.scoped)
        placeholders = ",".join("%s" for _ in subjects)
        async with self.owner.pool.acquire() as connection, connection.cursor() as query:
            try:
                await query.execute(
                    f"SELECT DISTINCT user_id FROM {relation} b "
                    f"WHERE b.user_id IN ({placeholders})",
                    tuple(sorted(subjects)),
                )
                return frozenset(int(row[0]) for row in await query.fetchall())
            finally:
                await connection.rollback()
