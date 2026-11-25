from __future__ import annotations

import asyncio
import json

from .accounts import SCHEMA as ACCOUNT_SCHEMA
from .accounts import MemoryAccounts, MySQLAccounts
from .models import complete_name
from .inventory import reconcile, wishlist_changes

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS karuta_inventory_control (
      id TINYINT PRIMARY KEY) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_inventory_membership (
      card_code VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      owner_id BIGINT UNSIGNED NOT NULL, listed BOOLEAN NOT NULL,
      epoch_ms BIGINT UNSIGNED NOT NULL, epoch_source BIGINT UNSIGNED NOT NULL,
      seen_ms BIGINT UNSIGNED NOT NULL, facts_json JSON NOT NULL,
      INDEX by_owner(owner_id,listed)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_inventory_queries (
      source_id BIGINT UNSIGNED PRIMARY KEY, owner_id BIGINT UNSIGNED NOT NULL,
      created_ms BIGINT UNSIGNED NOT NULL, document JSON NOT NULL) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_inventory_history (
      event_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      card_code VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      owner_id BIGINT UNSIGNED NOT NULL, observed_ms BIGINT UNSIGNED NOT NULL,
      kind VARCHAR(20) NOT NULL, document JSON NOT NULL,
      INDEX by_card(card_code,observed_ms),INDEX by_owner(owner_id,observed_ms)) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_character_wishlists (
      identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      wishlist INT UNSIGNED NOT NULL, observed_ms BIGINT UNSIGNED NOT NULL,
      source_id BIGINT UNSIGNED NOT NULL, source_order BIGINT UNSIGNED NOT NULL,
      FOREIGN KEY(identity_key) REFERENCES karuta_library_characters(identity_key))
      ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_wishlist_history (
      event_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
      identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      observed_ms BIGINT UNSIGNED NOT NULL, document JSON NOT NULL,
      INDEX by_character(identity_key,observed_ms)) ENGINE=InnoDB""",
    *ACCOUNT_SCHEMA,
)


def identities(page, records):
    result = {}
    for card in page.cards:
        old = records.get(card.code)
        verified = bool(
            old
            and old["confidence"] >= 95
            and old["edition"] == card.edition
            and old["print_num"] == card.print_num
        )
        result[card.code] = old["identity"] if verified else card.identity
    return result


def card_record(page, card, key, old):
    if old and old["last_ms"] >= page.stamp.at_ms:
        return old
    record = dict(old) if old else {}
    record.update(
        code=card.code,
        identity=key,
        edition=card.edition,
        print_num=card.print_num,
        confidence=max(old["confidence"], 90) if old and old["identity"] == key else 90,
        first_ms=min(old["first_ms"], page.created_ms) if old else page.created_ms,
        last_ms=page.stamp.at_ms,
    )
    for name in ("quality", "wishlist", "effort"):
        value = getattr(card, name)
        if value is not None:
            record[name] = value
    if (not complete_name(card.character) or not complete_name(card.series)) and (
        not old or old["identity"] != key or old["confidence"] < 95
    ):
        record["confidence"] = min(record["confidence"], 50)
    return record


class MemoryInventoryStore:
    def __init__(self, catalogue):
        self.catalogue = catalogue
        self.memberships, self.sessions, self.history = {}, {}, {}
        self.wishlists, self.wishlist_history = {}, {}
        self.lock = asyncio.Lock()
        self.accounts = MemoryAccounts(self)

    async def observe(self, page):
        async with self.lock:
            prior = self.sessions.get(page.source)
            result = reconcile(page, self.memberships, prior)
            if result is None:
                return None
            changes, events, session = result
            keys = identities(page, self.catalogue.cards)
            names = {}
            for card in page.cards:
                key = keys[card.code]
                if key not in self.catalogue.characters:
                    await self.catalogue.put_character(
                        card.character, card.series, page.created_ms, page.stamp.at_ms
                    )
                old = self.catalogue.cards.get(card.code)
                self.catalogue.cards[card.code] = card_record(page, card, key, old)
                row = self.catalogue.characters[key]
                names[card.code] = (row["character"], row["series"])
            wl, wl_events = wishlist_changes(page, self.wishlists, keys)
            self.memberships.update(changes)
            self.sessions[page.source] = session
            self.history.update({event["id"]: event for event in events})
            self.wishlists.update(wl)
            self.wishlist_history.update({event["id"]: event for event in wl_events})
            await self.accounts.native(page, prior)
            return names


class MySQLInventoryStore:
    def __init__(self, catalogue):
        self.catalogue = catalogue
        self.accounts = MySQLAccounts(self)

    async def migrate(self):
        async with self.catalogue.pool.acquire() as conn, conn.cursor() as q:
            for statement in SCHEMA:
                await q.execute(statement)
            await q.execute("INSERT IGNORE INTO karuta_inventory_control VALUES (1)")
            await conn.commit()

    async def observe(self, page):
        async with self.catalogue.pool.acquire() as conn, conn.cursor() as q:
            try:
                await conn.begin()

                await q.execute("SELECT id FROM karuta_inventory_control WHERE id=1 FOR UPDATE")
                if not await q.fetchone():
                    raise ValueError("inventory schema is unavailable")
                await q.execute(
                    "SELECT document FROM karuta_inventory_queries WHERE source_id=%s",
                    (page.source,),
                )
                raw = await q.fetchone()
                session = json.loads(raw[0]) if raw else None
                session_before = session
                await q.execute(
                    "SELECT card_code,owner_id,listed,epoch_ms,epoch_source,seen_ms,facts_json "
                    "FROM karuta_inventory_membership WHERE owner_id=%s LIMIT 100001",
                    (page.owner,),
                )
                rows = list(await q.fetchall())
                if len(rows) > 100000:
                    raise ValueError("inventory reconciliation limit exceeded")
                codes = tuple(card.code for card in page.cards)
                if codes:
                    await q.execute(
                        "SELECT card_code,owner_id,listed,epoch_ms,epoch_source,seen_ms,facts_json "
                        "FROM karuta_inventory_membership WHERE card_code IN ("
                        + ",".join(["%s"] * len(codes))
                        + ") ORDER BY card_code FOR UPDATE",
                        codes,
                    )
                    rows.extend(await q.fetchall())
                existing = {
                    r[0]: dict(
                        owner=r[1],
                        listed=bool(r[2]),
                        epoch=[r[3], r[4]],
                        seen_ms=r[5],
                        facts=json.loads(r[6]),
                    )
                    for r in rows
                }
                result = reconcile(page, existing, session)
                if result is None:
                    await conn.rollback()
                    return None
                changes, events, session = result
                records = {}
                if codes:
                    await q.execute(
                        "SELECT card_code,identity_key,edition,print_num,facts_json,confidence,"
                        "first_seen_ms,last_seen_ms FROM karuta_library_cards WHERE card_code IN ("
                        + ",".join(["%s"] * len(codes))
                        + ") ORDER BY card_code FOR UPDATE",
                        codes,
                    )
                    records = {
                        r[0]: dict(
                            code=r[0],
                            identity=r[1],
                            edition=r[2],
                            print_num=r[3],
                            confidence=r[5],
                            first_ms=r[6],
                            last_ms=r[7],
                            **json.loads(r[4]),
                        )
                        for r in await q.fetchall()
                    }
                keys = identities(page, records)
                names = {}
                for card in page.cards:
                    key = keys[card.code]
                    await q.execute(
                        "SELECT character_name,series_name FROM karuta_library_characters "
                        "WHERE identity_key=%s",
                        (key,),
                    )
                    row = await q.fetchone()
                    if row is None:
                        await self.catalogue._character(
                            q, card.character, card.series, page.created_ms, page.stamp.at_ms
                        )
                        row = (card.character, card.series)
                    names[card.code] = row
                    record = card_record(page, card, key, records.get(card.code))
                    facts = {
                        name: record[name]
                        for name in ("quality", "stars", "wishlist", "effort", "burn_value")
                        if record.get(name) is not None
                    }
                    await q.execute(
                        """INSERT INTO karuta_library_cards VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE identity_key=VALUES(identity_key),
                        edition=VALUES(edition),print_num=VALUES(print_num),
                        facts_json=VALUES(facts_json),confidence=VALUES(confidence),
                        first_seen_ms=VALUES(first_seen_ms),last_seen_ms=VALUES(last_seen_ms)""",
                        (
                            record["code"],
                            record["identity"],
                            record["edition"],
                            record["print_num"],
                            json.dumps(facts),
                            record["confidence"],
                            record["first_ms"],
                            record["last_ms"],
                        ),
                    )
                wl_records = {}
                unique_keys = tuple(set(keys.values()))
                if unique_keys:
                    await q.execute(
                        "SELECT identity_key,wishlist,observed_ms,source_id,source_order "
                        "FROM karuta_character_wishlists WHERE identity_key IN ("
                        + ",".join(["%s"] * len(unique_keys))
                        + ")",
                        unique_keys,
                    )
                    wl_records = {
                        r[0]: dict(value=r[1], stamp=[r[2], r[3], r[4]]) for r in await q.fetchall()
                    }
                wl, wl_events = wishlist_changes(page, wl_records, keys)
                for code, row in changes.items():
                    await q.execute(
                        """INSERT INTO karuta_inventory_membership VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE owner_id=VALUES(owner_id),listed=VALUES(listed),
                        epoch_ms=VALUES(epoch_ms),epoch_source=VALUES(epoch_source),
                        seen_ms=VALUES(seen_ms),facts_json=VALUES(facts_json)""",
                        (
                            code,
                            row["owner"],
                            row["listed"],
                            *row["epoch"],
                            row["seen_ms"],
                            json.dumps(row["facts"]),
                        ),
                    )
                await q.execute(
                    """INSERT INTO karuta_inventory_queries VALUES (%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE document=VALUES(document)""",
                    (page.source, page.owner, page.created_ms, json.dumps(session)),
                )
                for event in events:
                    await q.execute(
                        "INSERT IGNORE INTO karuta_inventory_history VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            event["id"],
                            event["code"],
                            page.owner,
                            page.stamp.at_ms,
                            event["kind"],
                            json.dumps(event),
                        ),
                    )
                for key, row in wl.items():
                    await q.execute(
                        """INSERT INTO karuta_character_wishlists VALUES (%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE wishlist=VALUES(wishlist),
                        observed_ms=VALUES(observed_ms),source_id=VALUES(source_id),
                        source_order=VALUES(source_order)""",
                        (key, row["value"], *row["stamp"]),
                    )
                for event in wl_events:
                    await q.execute(
                        "INSERT IGNORE INTO karuta_wishlist_history VALUES (%s,%s,%s,%s)",
                        (event["id"], event["identity"], page.stamp.at_ms, json.dumps(event)),
                    )
                await self.accounts.native(q, page, session_before)
                await conn.commit()
                return names
            except BaseException:
                await conn.rollback()
                raise
