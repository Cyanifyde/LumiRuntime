from __future__ import annotations

import json
from datetime import UTC, datetime

from .archive import ArchiveUnavailable

HISTORY_SCHEMA = """CREATE TABLE IF NOT EXISTS afk_history_clears (
    guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
    cleared_before DATETIME(6) NOT NULL,PRIMARY KEY(guild_id,user_id),
    KEY ix_afk_history_user(user_id,guild_id)) ENGINE=InnoDB"""


class AFKHistoryStore:
    async def _history_rows(self, cursor, rows, guild, occurred_at):
        users = tuple(sorted({row[1] for row in rows}))
        await cursor.execute(
            "SELECT user_id,cleared_before FROM afk_history_clears WHERE guild_id=%s "
            "AND user_id IN (" + ",".join(["%s"] * len(users)) + ") FOR UPDATE",
            (guild, *users),
        )
        cutoffs = dict(await cursor.fetchall())
        occurred_at = (
            (occurred_at.replace(tzinfo=UTC) if occurred_at.tzinfo is None else occurred_at)
            .astimezone(UTC)
            .replace(tzinfo=None)
        )
        return tuple(row for row in rows if row[1] not in cutoffs or occurred_at > cutoffs[row[1]])

    @staticmethod
    async def _newer_history_ids(q, guild, user, ids, before):
        if not ids:
            return set()
        marks = ",".join(["%s"] * len(ids))
        clause = f" WHERE guild_id=%s AND user_id=%s AND occurred_at>%s AND message_id IN ({marks})"
        args = (guild, user, before, *ids)
        await q.execute(
            "SELECT message_id FROM afk_missed_interactions"
            + clause
            + " UNION SELECT message_id FROM afk_archived_interactions"
            + clause,
            (*args, *args),
        )
        return {int(row[0]) for row in await q.fetchall()}

    async def clear_history(self, user: int, guilds: tuple[int, ...], before: datetime) -> int:

        guilds = tuple(sorted(set(guilds)))
        if not guilds or user <= 0 or any(g <= 0 for g in guilds) or before.tzinfo is None:
            raise ValueError("Invalid AFK history scope")
        before = before.astimezone(UTC).replace(tzinfo=None)
        marks = ",".join(["%s"] * len(guilds))
        scope = f"user_id=%s AND guild_id IN ({marks})"
        args = (user, *guilds)
        async with self._require_pool().acquire() as connection, connection.cursor() as q:
            locked = False
            try:
                locked = await self.archive._writer_lock(q)
                if not locked:
                    raise ArchiveUnavailable("AFK archive is busy. Try again.")
                await connection.begin()
                await q.executemany(
                    "INSERT INTO afk_history_clears VALUES(%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "cleared_before=GREATEST(cleared_before,VALUES(cleared_before))",
                    [(guild, user, before) for guild in guilds],
                )

                await q.execute(
                    "SELECT token,part,guild_id,message_ids FROM afk_recap_deliveries "
                    f"WHERE {scope} FOR UPDATE",
                    args,
                )
                for token, part, guild, raw in await q.fetchall():
                    ids = json.loads(raw)
                    newer = await self._newer_history_ids(q, guild, user, ids, before)
                    retained = [mid for mid in ids if mid in newer]
                    if retained:
                        await q.execute(
                            "UPDATE afk_recap_deliveries SET message_ids=%s "
                            "WHERE token=%s AND part=%s",
                            (json.dumps(retained), token, part),
                        )
                    else:
                        await q.execute(
                            "DELETE FROM afk_recap_deliveries WHERE token=%s AND part=%s",
                            (token, part),
                        )


                await q.execute(
                    "SELECT token,part,sources_json FROM afk_recap_selections "
                    "WHERE user_id=%s FOR UPDATE",
                    (user,),
                )
                for token, part, raw in await q.fetchall():
                    sources = json.loads(raw)
                    retained = [source for source in sources if source[0] not in guilds]
                    for guild in guilds:
                        ids = [mid for g, mid in sources if g == guild]
                        newer = await self._newer_history_ids(q, guild, user, ids, before)
                        retained.extend([guild, mid] for mid in ids if mid in newer)
                    if retained:
                        await q.execute(
                            "UPDATE afk_recap_selections SET sources_json=%s "
                            "WHERE token=%s AND part=%s",
                            (json.dumps(retained), token, part),
                        )
                    else:
                        await q.execute(
                            "DELETE FROM afk_recap_selections WHERE token=%s AND part=%s",
                            (token, part),
                        )
                await q.execute(
                    f"DELETE r FROM afk_interaction_reads r WHERE r.{scope} "
                    "AND NOT EXISTS (SELECT 1 FROM afk_missed_interactions m "
                    "WHERE m.guild_id=r.guild_id AND m.user_id=r.user_id "
                    "AND m.message_id=r.message_id AND m.occurred_at>%s) "
                    "AND NOT EXISTS (SELECT 1 FROM afk_archived_interactions a "
                    "WHERE a.guild_id=r.guild_id AND a.user_id=r.user_id "
                    "AND a.message_id=r.message_id AND a.occurred_at>%s)",
                    (*args, before, before),
                )
                removed = 0
                for table in (
                    "afk_interaction_replies",
                    "afk_missed_interactions",
                    "afk_archived_interactions",
                ):
                    await q.execute(
                        f"DELETE FROM {table} WHERE {scope} AND occurred_at<=%s",
                        (*args, before),
                    )
                    if table != "afk_interaction_replies":
                        removed += q.rowcount
                await q.execute(
                    f"DELETE FROM afk_return_windows WHERE {scope} AND started_at<=%s",
                    (*args, before),
                )
                await connection.commit()
                return removed
            except BaseException:
                await connection.rollback()
                raise
            finally:
                if locked:
                    await self.archive._writer_release(connection, q)
