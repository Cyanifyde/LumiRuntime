from __future__ import annotations

import json
from dataclasses import dataclass

GLOBAL_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS afk_user_scopes (
      user_id BIGINT UNSIGNED PRIMARY KEY,
      global_status BOOLEAN NOT NULL DEFAULT FALSE,
      global_recaps BOOLEAN NOT NULL DEFAULT FALSE,
      revision BIGINT UNSIGNED NOT NULL DEFAULT 0
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_global_status (
      user_id BIGINT UNSIGNED PRIMARY KEY,
      origin_guild_id BIGINT UNSIGNED NOT NULL,
      reason VARCHAR(1900) NULL, source_channel_id BIGINT UNSIGNED NOT NULL,
      started_at DATETIME(6) NOT NULL, history_since DATETIME(6) NOT NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS afk_recap_selections (
      token CHAR(16) NOT NULL, part INT UNSIGNED NOT NULL,
      user_id BIGINT UNSIGNED NOT NULL, sources_json JSON NOT NULL,
      expires_at DATETIME(6) NOT NULL,
      PRIMARY KEY(token,part), KEY ix_afk_selection_owner(user_id),
      KEY ix_afk_selection_expiry(expires_at)
    ) ENGINE=InnoDB""",
)


@dataclass(frozen=True, slots=True)
class AFKScopes:
    global_status: bool = False
    global_recaps: bool = False
    revision: int = 0


class GlobalAFKStore:
    async def get_scopes(self, user_id: int) -> AFKScopes:
        async with self._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "SELECT global_status,global_recaps,revision FROM afk_user_scopes "
                    "WHERE user_id=%s",
                    (user_id,),
                )
                row = await q.fetchone()
                return AFKScopes(bool(row[0]), bool(row[1]), int(row[2])) if row else AFKScopes()
            finally:
                await c.rollback()

    async def _lock_scope(self, q, user):
        await q.execute(
            "INSERT INTO afk_user_scopes(user_id) VALUES(%s) "
            "ON DUPLICATE KEY UPDATE user_id=VALUES(user_id)",
            (user,),
        )
        await q.execute(
            "SELECT global_status,global_recaps,revision FROM afk_user_scopes "
            "WHERE user_id=%s FOR UPDATE",
            (user,),
        )
        row = await q.fetchone()
        return AFKScopes(bool(row[0]), bool(row[1]), int(row[2]))

    @staticmethod
    async def _remember_window(q, guild, user, since):
        await q.execute(
            "INSERT INTO afk_return_windows VALUES(%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE started_at=LEAST(started_at,VALUES(started_at))",
            (guild, user, since),
        )

    async def _put_global(self, q, guild, user, reason, channel, started):
        await q.execute(
            "SELECT history_since FROM afk_global_status WHERE user_id=%s FOR UPDATE", (user,)
        )
        previous = await q.fetchone()
        await q.execute("SELECT MIN(started_at) FROM afk_return_windows WHERE user_id=%s", (user,))
        window = await q.fetchone()
        since = min(
            v
            for v in (started, previous[0] if previous else None, window[0] if window else None)
            if v is not None
        )
        await q.execute(
            "INSERT INTO afk_global_status VALUES(%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE origin_guild_id=VALUES(origin_guild_id),reason=VALUES(reason),"
            "source_channel_id=VALUES(source_channel_id),started_at=VALUES(started_at),history_since=VALUES(history_since)",
            (user, guild, reason, channel, started, since),
        )
        await self._publish_active(q, 0)
        return self._global_state((user, guild, reason, channel, started, since), guild)

    async def _clear_global(self, q, guild, user, returned_at=None):
        await q.execute(
            "SELECT user_id,origin_guild_id,reason,source_channel_id,started_at,history_since "
            "FROM afk_global_status WHERE user_id=%s FOR UPDATE",
            (user,),
        )
        row = await q.fetchone()
        if row is None:
            return None
        if returned_at is not None and row[4] > returned_at:
            return None

        await q.execute(
            "SELECT DISTINCT guild_id FROM afk_missed_interactions WHERE user_id=%s "
            "UNION SELECT DISTINCT guild_id FROM afk_archived_interactions WHERE user_id=%s",
            (user, user),
        )
        guilds = {int(r[0]) for r in await q.fetchall()} | {guild, int(row[1])}
        for source in sorted(guilds):
            await self._remember_window(q, source, user, row[5])
        await q.execute("DELETE FROM afk_global_status WHERE user_id=%s", (user,))
        await self._publish_active(q, 0)
        return self._global_state(row, guild)

    @staticmethod
    def _global_state(row, guild):
        from .store import AFKState, _aware_utc

        return AFKState(
            guild, int(row[0]), row[2], int(row[3]), _aware_utc(row[4]), True, _aware_utc(row[5])
        )

    async def set_scope(self, guild, user, name, enabled, *, expected_revision=None):
        if name not in {"global_status", "global_recaps"} or type(enabled) is not bool:
            raise ValueError("invalid AFK scope")
        async with self._require_pool().acquire() as c, c.cursor() as q:
            try:
                await c.begin()
                scopes = await self._lock_scope(q, user)
                if expected_revision is not None and scopes.revision != expected_revision:
                    await c.commit()
                    return False
                if getattr(scopes, name) == enabled:
                    await c.commit()
                    return
                if name == "global_status" and enabled:
                    await q.execute(
                        "SELECT guild_id,user_id,reason,source_channel_id,started_at "
                        "FROM afk_status WHERE user_id=%s "
                        "ORDER BY started_at DESC,guild_id FOR UPDATE",
                        (user,),
                    )
                    local = await q.fetchall()
                    if local:
                        selected = next((r for r in local if int(r[0]) == guild), local[0])
                        for r in local:
                            await self._remember_window(q, int(r[0]), user, r[4])
                        await q.execute("DELETE FROM afk_status WHERE user_id=%s", (user,))
                        await self._put_global(
                            q, int(selected[0]), user, selected[2], int(selected[3]), selected[4]
                        )
                        for source in sorted({int(r[0]) for r in local}):
                            await self._publish_active(q, source)
                elif name == "global_status":
                    state = await self._clear_global(q, guild, user)
                    if state:
                        from .store import _mysql_utc

                        await q.execute(
                            "INSERT INTO afk_status"
                            "(guild_id,user_id,reason,source_channel_id,started_at) "
                            "VALUES(%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE reason=VALUES(reason),"
                            "source_channel_id=VALUES(source_channel_id),"
                            "started_at=VALUES(started_at)",
                            (
                                guild,
                                user,
                                state.reason,
                                state.source_channel_id,
                                _mysql_utc(state.started_at),
                            ),
                        )
                        await self._publish_active(q, guild)
                await q.execute(
                    f"UPDATE afk_user_scopes SET {name}=%s,revision=revision+1 WHERE user_id=%s",
                    (enabled, user),
                )
                await c.commit()
                return True
            except BaseException:
                await c.rollback()
                raise

    async def freeze_recap(self, token, user, interactions):
        sources = sorted({(item.guild_id, item.message_id) for item in interactions})
        async with self._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.executemany(
                    "INSERT INTO afk_recap_selections VALUES(%s,%s,%s,%s,"
                    "DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 15 MINUTE))",
                    [
                        (token, n // 1000, user, json.dumps(sources[n : n + 1000]))
                        for n in range(0, len(sources), 1000)
                    ],
                )
                await c.commit()
            except BaseException:
                await c.rollback()
                raise

    async def recap_selection(self, token, user):
        async with self._require_pool().acquire() as c, c.cursor() as q:
            try:
                await q.execute(
                    "UPDATE afk_recap_selections SET "
                    "expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 15 MINUTE) "
                    "WHERE token=%s AND user_id=%s AND expires_at>UTC_TIMESTAMP(6)",
                    (token, user),
                )
                await q.execute(
                    "SELECT sources_json FROM afk_recap_selections WHERE token=%s AND user_id=%s "
                    "AND expires_at>UTC_TIMESTAMP(6) ORDER BY part",
                    (token, user),
                )
                sources = frozenset(
                    (int(g), int(m)) for (raw,) in await q.fetchall() for g, m in json.loads(raw)
                )
                await c.commit()
                return sources
            except BaseException:
                await c.rollback()
                raise

    async def prune_selections(self):
        async with self._require_pool().acquire() as c, c.cursor() as q:
            await q.execute(
                "DELETE FROM afk_recap_selections WHERE expires_at<=UTC_TIMESTAMP(6) LIMIT 1000"
            )
            await c.commit()
