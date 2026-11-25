from __future__ import annotations

import asyncio
import json
from urllib.parse import unquote, urlparse

import asyncmy

from lumi.storage.checkpoint import create_pool

from .models import Blob, Card, Stamp, clean_text, identity

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS karuta_library_characters (
      identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      character_name VARCHAR(240) NOT NULL, series_name VARCHAR(240) NOT NULL,
      edition_count SMALLINT UNSIGNED NULL, first_seen_ms BIGINT UNSIGNED NOT NULL,
      last_seen_ms BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS karuta_library_images (
      sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      extension VARCHAR(5) NOT NULL, mime VARCHAR(32) NOT NULL,
      size_bytes INT UNSIGNED NOT NULL, width INT UNSIGNED NOT NULL, height INT UNSIGNED NOT NULL
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_library_editions (
      identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      edition SMALLINT UNSIGNED NOT NULL,
      image_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
      observed_ms BIGINT UNSIGNED NOT NULL, observed_order BIGINT UNSIGNED NOT NULL DEFAULT 0,
      source_kind VARCHAR(16) NOT NULL,
      PRIMARY KEY(identity_key,edition),
      FOREIGN KEY(identity_key) REFERENCES karuta_library_characters(identity_key),
      FOREIGN KEY(image_sha256) REFERENCES karuta_library_images(sha256)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_library_cards (
      card_code VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      edition SMALLINT UNSIGNED NULL, print_num INT UNSIGNED NULL,
      facts_json JSON NOT NULL, confidence SMALLINT UNSIGNED NOT NULL,
      first_seen_ms BIGINT UNSIGNED NOT NULL, last_seen_ms BIGINT UNSIGNED NOT NULL,
      FOREIGN KEY(identity_key) REFERENCES karuta_library_characters(identity_key)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_library_variants (
      identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      edition SMALLINT UNSIGNED NOT NULL, variant INT NOT NULL,
      image_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      observed_ms BIGINT UNSIGNED NOT NULL, source_kind VARCHAR(16) NOT NULL,
      PRIMARY KEY(identity_key,edition,variant,image_sha256),
      FOREIGN KEY(identity_key) REFERENCES karuta_library_characters(identity_key),
      FOREIGN KEY(image_sha256) REFERENCES karuta_library_images(sha256)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_library_imports (
      source_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      status ENUM('importing','complete') NOT NULL, counts_json JSON NOT NULL,
      updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB""",
)


class MemoryCatalogueStore:
    def __init__(self):
        self.characters = {}
        self.editions = {}
        self.cards = {}
        self.imports = {}
        self.variants = {}
        self.lock = asyncio.Lock()

    async def ready(self):
        return True

    async def close(self):
        pass

    async def put_character(self, character, series, first_ms, last_ms, edition_count=None):
        key = identity(character, series)
        old = self.characters.get(key)
        if not old or last_ms >= old["last_ms"]:
            self.characters[key] = dict(
                character=clean_text(character),
                series=clean_text(series),
                first_ms=min(first_ms, old["first_ms"]) if old else first_ms,
                last_ms=last_ms,
                edition_count=edition_count
                if edition_count is not None
                else (old or {}).get("edition_count"),
            )
        return key

    async def learn(self, character, series, edition, edition_count, blob, stamp, source="klu"):
        async with self.lock:
            key = identity(character, series)
            if source != "legacy":
                await self.put_character(character, series, stamp.at_ms, stamp.at_ms, edition_count)
            old = self.editions.get((key, edition))
            if old and stamp <= old[0] and not (source == "klu" and old[2] == "cdn"):
                return False
            changed = old is None or old[1] != blob
            self.editions[key, edition] = (stamp, blob, source)
            return changed

    async def images(self, cards):
        return tuple(blob for _, blob in await self.resolved_images(cards))

    async def resolved_images(self, cards):
        result = []
        for card in cards:
            canonical = self.cards.get(card.code)
            key = (
                canonical["identity"]
                if canonical
                and canonical["confidence"] >= 95
                and canonical["edition"] == card.edition
                else card.identity
            )
            result.append((key, (self.editions.get((key, card.edition)) or (None, None))[1]))
        return tuple(result)

    async def put_card(self, record):
        old = self.cards.get(record["code"])
        if not old or record["last_ms"] > old["last_ms"]:
            self.cards[record["code"]] = dict(record)

    async def put_variant(self, key, edition, variant, blob, stamp):
        self.variants[key, edition, variant, blob.sha256] = stamp

    async def put_images(self, blobs):
        pass

    async def put_variants(self, rows, blobs):
        for row in rows:
            await self.put_variant(
                row["identity"],
                row["edition"],
                row["variant"],
                blobs[row["sha256"]],
                Stamp(row["at_ms"]),
            )

    async def import_status(self, digest):
        return self.imports.get(digest)

    async def mark_import(self, digest, status, counts):
        self.imports[digest] = dict(status=status, counts=dict(counts))


class MySQLCatalogueStore:
    def __init__(self, dsn):
        parsed = urlparse(dsn)
        if parsed.scheme not in {"mysql", "mariadb"}:
            raise ValueError("invalid catalogue DSN")
        self.settings = dict(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            db=parsed.path.removeprefix("/"),
            autocommit=False,
            charset="utf8mb4",
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
            for statement in SCHEMA:
                await cursor.execute(statement)
            await connection.commit()

    async def ready(self):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT 1 FROM karuta_library_imports WHERE status='complete' LIMIT 1"
            )
            row = await cursor.fetchone()
            await connection.rollback()
            return bool(row)

    @staticmethod
    async def _character(cursor, character, series, first_ms, last_ms, edition_count=None):
        key = identity(character, series)
        await cursor.execute(
            """INSERT INTO karuta_library_characters
          (identity_key,character_name,series_name,edition_count,first_seen_ms,last_seen_ms)
          VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
          edition_count=IF(VALUES(last_seen_ms)>last_seen_ms,
          COALESCE(VALUES(edition_count),edition_count),edition_count),
          first_seen_ms=LEAST(first_seen_ms,VALUES(first_seen_ms)),
          last_seen_ms=GREATEST(last_seen_ms,VALUES(last_seen_ms))""",
            (key, clean_text(character), clean_text(series), edition_count, first_ms, last_ms),
        )
        return key

    async def put_character(self, character, series, first_ms, last_ms, edition_count=None):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            key = await self._character(cursor, character, series, first_ms, last_ms, edition_count)
            await connection.commit()
            return key

    async def learn(
        self,
        character,
        series,
        edition,
        edition_count,
        blob: Blob | None,
        stamp: Stamp,
        source="klu",
    ):
        if not 1 <= edition <= 100 or source not in {"klu", "legacy"}:
            raise ValueError("invalid edition observation")
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                key = identity(character, series)
                if source != "legacy":
                    await self._character(
                        cursor, character, series, stamp.at_ms, stamp.at_ms, edition_count
                    )
                await cursor.execute(
                    """SELECT observed_ms,observed_order,image_sha256,source_kind
                    FROM karuta_library_editions WHERE identity_key=%s AND edition=%s FOR UPDATE""",
                    (key, edition),
                )
                old = await cursor.fetchone()
                if (
                    old
                    and stamp <= Stamp(old[0], old[1])
                    and not (source == "klu" and old[3] == "cdn")
                ):
                    await connection.rollback()
                    return False
                if blob:
                    await cursor.execute(
                        "INSERT INTO karuta_library_images VALUES (%s,%s,%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE sha256=VALUES(sha256)",
                        (
                            blob.sha256,
                            blob.extension,
                            blob.mime,
                            blob.size,
                            blob.width,
                            blob.height,
                        ),
                    )
                await cursor.execute(
                    """INSERT INTO karuta_library_editions
                  (identity_key,edition,image_sha256,observed_ms,observed_order,source_kind)
                  VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                  image_sha256=COALESCE(VALUES(image_sha256),image_sha256),
                  observed_ms=VALUES(observed_ms),observed_order=VALUES(observed_order),
                  source_kind=VALUES(source_kind)""",
                    (key, edition, blob.sha256 if blob else None, stamp.at_ms, stamp.order, source),
                )
                await connection.commit()
                return old is None or (blob is not None and old[2] != blob.sha256)
            except Exception:
                await connection.rollback()
                raise

    async def images(self, cards: tuple[Card, ...]):
        return tuple(blob for _, blob in await self.resolved_images(cards))

    async def resolved_images(self, cards: tuple[Card, ...]):
        result = []
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            for card in cards:
                key = card.identity
                await cursor.execute(
                    """SELECT identity_key FROM karuta_library_cards
                    WHERE card_code=%s AND edition=%s AND confidence>=95""",
                    (card.code, card.edition),
                )
                canonical = await cursor.fetchone()
                if canonical:
                    key = canonical[0]
                await cursor.execute(
                    """SELECT i.sha256,i.extension,i.mime,i.size_bytes,i.width,i.height
                  FROM karuta_library_editions e
                  JOIN karuta_library_images i ON i.sha256=e.image_sha256
                  WHERE e.identity_key=%s AND e.edition=%s""",
                    (key, card.edition),
                )
                row = await cursor.fetchone()
                result.append((key, Blob(*row) if row else None))
            await connection.rollback()
        return tuple(result)

    async def put_card(self, record):
        facts = {k: record.get(k) for k in ("quality", "stars", "wishlist", "effort", "burn_value")}
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """INSERT INTO karuta_library_cards
              (card_code,identity_key,edition,print_num,facts_json,confidence,first_seen_ms,last_seen_ms)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
              identity_key=IF(VALUES(last_seen_ms)>last_seen_ms,VALUES(identity_key),identity_key),
              edition=IF(VALUES(last_seen_ms)>last_seen_ms,VALUES(edition),edition),
              print_num=IF(VALUES(last_seen_ms)>last_seen_ms,VALUES(print_num),print_num),
              facts_json=IF(VALUES(last_seen_ms)>last_seen_ms,VALUES(facts_json),facts_json),
              confidence=IF(VALUES(last_seen_ms)>last_seen_ms,VALUES(confidence),confidence),
              first_seen_ms=LEAST(first_seen_ms,VALUES(first_seen_ms)),
              last_seen_ms=GREATEST(last_seen_ms,VALUES(last_seen_ms))""",
                (
                    record["code"],
                    record["identity"],
                    record["edition"],
                    record["print_num"],
                    json.dumps(facts, ensure_ascii=False),
                    record["confidence"],
                    record["first_ms"],
                    record["last_ms"],
                ),
            )
            await connection.commit()

    async def put_variant(self, key, edition, variant, blob, stamp):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO karuta_library_images VALUES (%s,%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE sha256=VALUES(sha256)",
                (blob.sha256, blob.extension, blob.mime, blob.size, blob.width, blob.height),
            )
            await cursor.execute(
                """INSERT INTO karuta_library_variants VALUES (%s,%s,%s,%s,%s,'legacy')
              ON DUPLICATE KEY UPDATE observed_ms=GREATEST(observed_ms,VALUES(observed_ms))""",
                (key, edition, variant, blob.sha256, stamp.at_ms),
            )
            await connection.commit()

    async def put_images(self, blobs):
        values = [(b.sha256, b.extension, b.mime, b.size, b.width, b.height) for b in blobs]
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for start in range(0, len(values), 500):
                    await cursor.executemany(
                        "INSERT INTO karuta_library_images VALUES (%s,%s,%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE sha256=VALUES(sha256)",
                        values[start : start + 500],
                    )
                    await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def put_variants(self, rows, blobs):
        values = [
            (r["identity"], r["edition"], r["variant"], r["sha256"], r["at_ms"]) for r in rows
        ]
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            try:
                for start in range(0, len(values), 500):
                    await cursor.executemany(
                        """INSERT INTO karuta_library_variants VALUES (%s,%s,%s,%s,%s,'legacy')
                        ON DUPLICATE KEY UPDATE
                        observed_ms=GREATEST(observed_ms,VALUES(observed_ms))""",
                        values[start : start + 500],
                    )
                    await connection.commit()
            except Exception:
                await connection.rollback()
                raise

    async def import_status(self, digest):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                "SELECT status,counts_json FROM karuta_library_imports WHERE source_sha256=%s",
                (digest,),
            )
            row = await cursor.fetchone()
            await connection.rollback()
            return dict(status=row[0], counts=json.loads(row[1])) if row else None

    async def mark_import(self, digest, status, counts):
        async with self.pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                """INSERT INTO karuta_library_imports VALUES (%s,%s,%s,CURRENT_TIMESTAMP(6))
              ON DUPLICATE KEY UPDATE status=VALUES(status),counts_json=VALUES(counts_json)""",
                (digest, status, json.dumps(counts)),
            )
            await connection.commit()
