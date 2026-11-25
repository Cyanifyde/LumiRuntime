from __future__ import annotations

import asyncio
import copy
import json

from asyncmy.errors import IntegrityError

from lumi.karuta_collections.models import Blob

from .kguess import Bounds, normalize

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS games_kguess_sessions (
      id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      guild_id BIGINT UNSIGNED NOT NULL, channel_id BIGINT UNSIGNED NOT NULL,
      active_slot TINYINT NULL, revision BIGINT UNSIGNED NOT NULL,
      document LONGTEXT NOT NULL CHECK(JSON_VALID(document)),
      updated_ms BIGINT UNSIGNED NOT NULL,
      UNIQUE KEY one_channel(guild_id,channel_id,active_slot)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS games_kguess_outcomes (
      round_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      session_id CHAR(32) NOT NULL, guild_id BIGINT UNSIGNED NOT NULL,
      mode VARCHAR(12) NOT NULL, winner_id BIGINT UNSIGNED NULL,
      points INT UNSIGNED NOT NULL, elapsed_ms INT UNSIGNED NOT NULL,
      ended_ms BIGINT UNSIGNED NOT NULL, INDEX session_results(session_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS games_kguess_players (
      guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
      mode VARCHAR(12) NOT NULL,points BIGINT UNSIGNED NOT NULL DEFAULT 0,
      wins BIGINT UNSIGNED NOT NULL DEFAULT 0,
      PRIMARY KEY(guild_id,user_id,mode)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS games_kguess_aliases (
      identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      mode VARCHAR(12) NOT NULL, alias VARCHAR(240) NOT NULL,
      PRIMARY KEY(identity_key,mode,alias)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
)

TERMINAL = {"done", "cancelled"}


def document(state):
    result = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    if len(result.encode()) > 16 * 1024 * 1024:
        raise ValueError("game snapshot too large")
    return result


class MySQLGameStore:
    def __init__(self, catalogue):
        self.catalogue = catalogue

    @property
    def pool(self):
        return self.catalogue.pool

    async def migrate(self):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            for sql in SCHEMA:
                await q.execute(sql)
            await conn.commit()

    async def close(self):
        pass

    async def candidates(self, bounds: Bounds):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                """SELECT c.identity_key,c.character_name,c.series_name,w.wishlist
                FROM karuta_character_wishlists w JOIN karuta_library_characters c
                ON c.identity_key=w.identity_key WHERE w.wishlist BETWEEN %s AND %s
                AND c.character_name<>'' AND c.series_name<>'' AND EXISTS (
                  SELECT 1 FROM karuta_library_editions e JOIN karuta_library_images i
                  ON i.sha256=e.image_sha256 WHERE e.identity_key=c.identity_key)
                ORDER BY c.identity_key""",
                (bounds.low, bounds.high),
            )
            result = [
                dict(key=k, character=c, series=s, wl=w)
                for k, c, s, w in await q.fetchall()
                if normalize(c) and normalize(s)
            ]
            await conn.rollback()
            return result

    async def images(self, key):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                """SELECT e.edition,i.sha256,i.extension,i.mime,i.size_bytes,i.width,i.height
              FROM karuta_library_editions e JOIN karuta_library_images i ON i.sha256=e.image_sha256
              WHERE e.identity_key=%s ORDER BY e.edition""",
                (key,),
            )
            result = [(row[0], Blob(*row[1:])) for row in await q.fetchall()]
            await conn.rollback()
            return result

    async def aliases(self, key, mode):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT alias FROM games_kguess_aliases WHERE identity_key=%s AND mode=%s",
                (key, mode),
            )
            result = [r[0] for r in await q.fetchall()]
            await conn.rollback()
            return result

    async def create(self, state):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await q.execute(
                    "INSERT INTO games_kguess_sessions VALUES (%s,%s,%s,1,0,%s,%s)",
                    (
                        state["id"],
                        state["guild"],
                        state["channel"],
                        document(state),
                        state["updated"],
                    ),
                )
                await conn.commit()
                return True
            except IntegrityError as exc:
                await conn.rollback()
                if exc.args and exc.args[0] == 1062:
                    return False
                raise

    async def get(self, session):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute("SELECT document FROM games_kguess_sessions WHERE id=%s", (session,))
            row = await q.fetchone()
            await conn.rollback()
            return json.loads(row[0]) if row else None

    async def active(self, guild, channel):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT document FROM games_kguess_sessions "
                "WHERE guild_id=%s AND channel_id=%s AND active_slot=1",
                (guild, channel),
            )
            row = await q.fetchone()
            await conn.rollback()
            return json.loads(row[0]) if row else None

    async def active_sessions(self):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute("SELECT document FROM games_kguess_sessions WHERE active_slot=1")
            result = [json.loads(r[0]) for r in await q.fetchall()]
            await conn.rollback()
            return result

    async def save(self, state, expected):
        updated = dict(state, revision=expected + 1)
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "UPDATE games_kguess_sessions "
                "SET revision=%s,document=%s,active_slot=%s,updated_ms=%s "
                "WHERE id=%s AND revision=%s",
                (
                    expected + 1,
                    document(updated),
                    None if state["phase"] in TERMINAL else 1,
                    state["updated"],
                    state["id"],
                    expected,
                ),
            )
            ok = q.rowcount == 1
            await conn.commit()
        if ok:
            state["revision"] = expected + 1
        return ok

    async def settle(self, state, expected, outcome):
        updated = dict(state, revision=expected + 1)
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await conn.begin()
                await q.execute(
                    "SELECT revision FROM games_kguess_sessions WHERE id=%s FOR UPDATE",
                    (state["id"],),
                )
                row = await q.fetchone()
                if not row or row[0] != expected:
                    await conn.rollback()
                    return False
                await q.execute(
                    "INSERT IGNORE INTO games_kguess_outcomes VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        outcome["round"],
                        state["id"],
                        state["guild"],
                        state["mode"],
                        outcome["winner"] or None,
                        outcome["points"],
                        outcome["elapsed"],
                        outcome["ended"],
                    ),
                )
                inserted = q.rowcount == 1
                if not inserted:
                    await conn.rollback()
                    return False
                if outcome["winner"]:
                    await q.execute(
                        """INSERT INTO games_kguess_players VALUES (%s,%s,%s,%s,1)
                    ON DUPLICATE KEY UPDATE points=points+VALUES(points),wins=wins+1""",
                        (state["guild"], outcome["winner"], state["mode"], outcome["points"]),
                    )
                await q.execute(
                    "UPDATE games_kguess_sessions SET revision=%s,document=%s,updated_ms=%s "
                    "WHERE id=%s",
                    (expected + 1, document(updated), state["updated"], state["id"]),
                )
                await conn.commit()
                state["revision"] = expected + 1
                return True
            except BaseException:
                await conn.rollback()
                raise

    async def leaderboard(self, guild, mode, page=0):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT COUNT(*) FROM games_kguess_players WHERE guild_id=%s AND mode=%s",
                (guild, mode),
            )
            count = (await q.fetchone())[0]
            await q.execute(
                """SELECT user_id,points,wins,RANK() OVER (ORDER BY points DESC)
              FROM games_kguess_players WHERE guild_id=%s AND mode=%s
              ORDER BY points DESC,user_id LIMIT 10 OFFSET %s""",
                (guild, mode, page * 10),
            )
            rows = [dict(user=u, points=p, wins=w, rank=r) for u, p, w, r in await q.fetchall()]
            await conn.rollback()
            return rows, count

    async def stats(self, guild, user):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT mode,points,wins FROM games_kguess_players "
                "WHERE guild_id=%s AND user_id=%s",
                (guild, user),
            )
            rows = {m: dict(points=p, wins=w) for m, p, w in await q.fetchall()}
            await conn.rollback()
            return rows

    async def cleanup(self, now):
        cutoff = now - 30 * 86400 * 1000
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute("DELETE FROM games_kguess_outcomes WHERE ended_ms<%s", (cutoff,))
            await q.execute(
                "DELETE FROM games_kguess_sessions WHERE active_slot IS NULL AND updated_ms<%s",
                (cutoff,),
            )
            await conn.commit()


class MemoryGameStore:
    def __init__(self, pool=(), images=None, aliases=None):
        self.catalogue_rows = list(pool)
        self.image_rows, self.alias_rows = images or {}, aliases or {}
        self.sessions, self.outcomes, self.players = {}, {}, {}
        self.lock = asyncio.Lock()

    async def close(self):
        pass

    async def candidates(self, bounds):
        return copy.deepcopy(
            [
                r
                for r in self.catalogue_rows
                if bounds.contains(r["wl"])
                and self.image_rows.get(r["key"])
                and normalize(r["character"])
                and normalize(r["series"])
            ]
        )

    async def images(self, key):
        return self.image_rows.get(key, ())

    async def aliases(self, key, mode):
        return self.alias_rows.get((key, mode), [])

    async def create(self, state):
        async with self.lock:
            if await self.active(state["guild"], state["channel"]):
                return False
            self.sessions[state["id"]] = copy.deepcopy(state)
            return True

    async def get(self, key):
        return copy.deepcopy(self.sessions.get(key))

    async def active(self, guild, channel):
        return next(
            (
                copy.deepcopy(s)
                for s in self.sessions.values()
                if (s["guild"], s["channel"]) == (guild, channel) and s["phase"] not in TERMINAL
            ),
            None,
        )

    async def active_sessions(self):
        return [copy.deepcopy(s) for s in self.sessions.values() if s["phase"] not in TERMINAL]

    async def save(self, state, expected):
        async with self.lock:
            old = self.sessions.get(state["id"])
            if not old or old["revision"] != expected:
                return False
            state["revision"] = expected + 1
            self.sessions[state["id"]] = copy.deepcopy(state)
            return True

    async def settle(self, state, expected, outcome):
        async with self.lock:
            if (
                self.sessions[state["id"]]["revision"] != expected
                or outcome["round"] in self.outcomes
            ):
                return False
            self.outcomes[outcome["round"]] = copy.deepcopy(outcome)
            if outcome["winner"]:
                key = state["guild"], outcome["winner"], state["mode"]
                entry = self.players.setdefault(key, dict(points=0, wins=0))
                entry["points"] += outcome["points"]
                entry["wins"] += 1
            state["revision"] = expected + 1
            self.sessions[state["id"]] = copy.deepcopy(state)
            return True

    async def leaderboard(self, guild, mode, page=0):
        rows = [
            dict(user=u, **v) for (g, u, m), v in self.players.items() if (g, m) == (guild, mode)
        ]
        rows.sort(key=lambda r: (-r["points"], r["user"]))
        rank, last = 0, None
        for i, row in enumerate(rows, 1):
            if last != row["points"]:
                rank = i
            row["rank"] = rank
            last = row["points"]
        return rows[page * 10 : page * 10 + 10], len(rows)

    async def stats(self, guild, user):
        return {m: dict(v) for (g, u, m), v in self.players.items() if (g, u) == (guild, user)}

    async def cleanup(self, now):
        cutoff = now - 30 * 86400 * 1000
        self.sessions = {
            k: s
            for k, s in self.sessions.items()
            if s["phase"] not in TERMINAL or s["updated"] >= cutoff
        }
        self.outcomes = {k: o for k, o in self.outcomes.items() if o["ended"] >= cutoff}
