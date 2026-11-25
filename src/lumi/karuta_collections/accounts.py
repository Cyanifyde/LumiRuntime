from __future__ import annotations

import copy
import json
import time

from .checks import MAX_CARDS, ImportError
from .inventory import reconcile
from .personal_wishlist import merge


def check_deadline(deadline, clock):
    if deadline is not None and (clock() if clock else time.time_ns() // 1_000_000) >= deadline:
        raise ImportError("Expired.")


def validate_import(owner, source, created, digest, records):
    if (
        not all(isinstance(value, int) and 0 < value < 2**64 for value in (owner, source, created))
        or len(digest) != 64
        or any(c not in "0123456789abcdef" for c in digest)
        or not 1 <= len(records) <= MAX_CARDS
        or len({row["code"] for row in records}) != len(records)
    ):
        raise ImportError("Invalid CSV.")


SCHEMA = (
    """CREATE TABLE IF NOT EXISTS karuta_personal_wishlists (
      owner_id BIGINT UNSIGNED PRIMARY KEY, document JSON NOT NULL) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_inventory_imports (
      owner_id BIGINT UNSIGNED PRIMARY KEY, source_id BIGINT UNSIGNED NOT NULL,
      created_ms BIGINT UNSIGNED NOT NULL, digest CHAR(64) NOT NULL,
      card_count INT UNSIGNED NOT NULL) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS karuta_inventory_import_cards (
      owner_id BIGINT UNSIGNED NOT NULL,
      card_code VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
      listed BOOLEAN NOT NULL, removed_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
      facts_json JSON NOT NULL, PRIMARY KEY(owner_id,card_code),
      INDEX by_owner_listed(owner_id,listed)) ENGINE=InnoDB""",
)


def canonical(record, known, characters):
    if not known or known["confidence"] < 95:
        return record
    names = characters.get(known["identity"])
    immutable = (known["edition"], known["print_num"]) == (record["edition"], record["print_num"])
    if immutable and names:
        return {**record, "character": names["character"], "series": names["series"]}
    return record


def effective(owner, meta, imported, native, known, characters):
    result = {code: row["facts"] for code, row in imported.items() if row["listed"]} if meta else {}
    cutoff = [meta["created"], meta["source"]] if meta else [0, 0]
    for code, row in native.items():
        if meta and row["epoch"] <= cutoff:
            continue
        if row["seen_ms"] <= imported.get(code, {}).get("removed_ms", 0):
            continue
        if row["owner"] == owner and row["listed"]:
            result[code] = row["facts"]
        else:
            result.pop(code, None)
    cards = []
    for code, row in result.items():
        if meta and code in imported:


            original = imported[code]["facts"]
            cards.append(
                {
                    **original,
                    **row,
                    **{
                        key: original[key]
                        for key in ("character", "series", "edition", "print_num")
                    },
                }
            )
        else:
            cards.append(canonical(row, known.get(code), characters))
    if len(cards) > MAX_CARDS:
        raise ValueError("inventory limit exceeded")
    return cards


def negative_updates(page, meta, imported, prior):
    if not meta or page.epoch <= [meta["created"], meta["source"]]:
        return {}


    existing = {
        code: dict(
            owner=page.owner,
            listed=row["listed"],
            epoch=[meta["created"], meta["source"]],
            seen_ms=max(meta["created"], row.get("removed_ms", 0)),
            facts=row["facts"],
        )
        for code, row in imported.items()
    }
    result = reconcile(page, existing, prior)
    return {code: row for code, row in (result[0] if result else {}).items() if not row["listed"]}


class MemoryAccounts:
    def __init__(self, inventory):
        self.inventory = inventory
        self.imports, self.cards, self.wishes = {}, {}, {}

    async def import_status(self, owner, source, created):
        async with self.inventory.lock:
            old = self.imports.get(owner)
            return (
                old["count"]
                if old and [created, source] <= [old["created"], old["source"]]
                else None
            )

    async def import_cards(
        self, owner, source, created, digest, records, *, deadline=None, clock=None
    ):
        validate_import(owner, source, created, digest, records)
        async with self.inventory.lock:
            check_deadline(deadline, clock)
            old = self.imports.get(owner)
            if old and [created, source] <= [old["created"], old["source"]]:
                return old["count"]
            replacement = {
                row["code"]: dict(facts=copy.deepcopy(row), listed=True, removed_ms=0)
                for row in records
            }
            check_deadline(deadline, clock)
            self.cards[owner] = replacement
            self.imports[owner] = dict(
                source=source, created=created, digest=digest, count=len(records)
            )
            return len(records)

    async def account(self, owner):
        async with self.inventory.lock:
            meta = self.imports.get(owner)
            native = self.inventory.memberships
            cards = effective(
                owner,
                meta,
                self.cards.get(owner, {}),
                native,
                self.inventory.catalogue.cards,
                self.inventory.catalogue.characters,
            )
            known = (
                meta is not None
                or any(row["owner"] == owner for row in native.values())
                or any(row["owner"] == owner for row in self.inventory.sessions.values())
            )
            return dict(known=known, cards=cards, imported=meta is not None)

    async def native(self, page, prior):
        rows = self.cards.get(page.owner, {})
        for code in negative_updates(page, self.imports.get(page.owner), rows, prior):
            rows[code].update(listed=False, removed_ms=page.stamp.at_ms)

    async def wishlist(self, owner):
        return copy.deepcopy(self.wishes.get(owner))

    async def observe_wishlist(self, value):
        async with self.inventory.lock:
            self.wishes[value["owner"]] = merge(self.wishes.get(value["owner"]), value)


class MySQLAccounts:
    def __init__(self, inventory):
        self.inventory = inventory

    @property
    def pool(self):
        return self.inventory.catalogue.pool

    async def lock(self, q):
        await q.execute("SELECT id FROM karuta_inventory_control WHERE id=1 FOR UPDATE")
        if not await q.fetchone():
            raise ValueError("inventory unavailable")

    async def _meta(self, q, owner):
        await q.execute(
            "SELECT source_id,created_ms,digest,card_count FROM karuta_inventory_imports "
            "WHERE owner_id=%s",
            (owner,),
        )
        row = await q.fetchone()
        return dict(source=row[0], created=row[1], digest=row[2], count=row[3]) if row else None

    async def import_status(self, owner, source, created):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            old = await self._meta(q, owner)
            await conn.rollback()
            return (
                old["count"]
                if old and [created, source] <= [old["created"], old["source"]]
                else None
            )

    async def _imported(self, q, owner, meta=None):
        meta = meta or await self._meta(q, owner)
        if meta is None:
            return None, {}
        await q.execute(
            "SELECT card_code,listed,removed_ms,facts_json FROM karuta_inventory_import_cards "
            "WHERE owner_id=%s",
            (owner,),
        )
        rows = await q.fetchall()
        if len(rows) > MAX_CARDS:
            raise ValueError("inventory limit exceeded")
        return meta, {
            code: dict(listed=bool(listed), removed_ms=removed, facts=json.loads(facts))
            for code, listed, removed, facts in rows
        }

    async def _canonical(self, q, codes):
        known, names = {}, {}
        codes = list(codes)
        for offset in range(0, len(codes), 500):
            chunk = codes[offset : offset + 500]
            await q.execute(
                "SELECT c.card_code,c.identity_key,c.edition,c.print_num,c.confidence,"
                "k.character_name,k.series_name "
                "FROM karuta_library_cards c JOIN karuta_library_characters k USING(identity_key) "
                "WHERE c.card_code IN (" + ",".join(["%s"] * len(chunk)) + ")",
                chunk,
            )
            for code, key, edition, print_num, confidence, character, series in await q.fetchall():
                known[code] = dict(
                    identity=key, edition=edition, print_num=print_num, confidence=confidence
                )
                names[key] = dict(character=character, series=series)
        return known, names

    async def import_cards(
        self, owner, source, created, digest, records, *, deadline=None, clock=None
    ):
        validate_import(owner, source, created, digest, records)
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await conn.begin()
                await self.lock(q)
                check_deadline(deadline, clock)
                old = await self._meta(q, owner)
                if old and [created, source] <= [old["created"], old["source"]]:
                    await conn.rollback()
                    return old["count"]
                await q.execute(
                    "DELETE FROM karuta_inventory_import_cards WHERE owner_id=%s", (owner,)
                )
                for offset in range(0, len(records), 250):
                    check_deadline(deadline, clock)
                    chunk = records[offset : offset + 250]
                    await q.execute(
                        "INSERT INTO karuta_inventory_import_cards "
                        "(owner_id,card_code,listed,removed_ms,facts_json) VALUES "
                        + ",".join(["(%s,%s,TRUE,0,%s)"] * len(chunk)),
                        tuple(
                            value
                            for row in chunk
                            for value in (owner, row["code"], json.dumps(row))
                        ),
                    )
                await q.execute(
                    "INSERT INTO karuta_inventory_imports VALUES (%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE source_id=VALUES(source_id),"
                    "created_ms=VALUES(created_ms),"
                    "digest=VALUES(digest),card_count=VALUES(card_count)",
                    (owner, source, created, digest, len(records)),
                )

                await q.execute(
                    "INSERT IGNORE INTO karuta_inventory_history VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        digest_key(owner, source),
                        "csv",
                        owner,
                        created,
                        "csv_import",
                        json.dumps(
                            dict(
                                source=source,
                                digest=digest,
                                count=len(records),
                                trust="owner_provided",
                            )
                        ),
                    ),
                )
                check_deadline(deadline, clock)
                await conn.commit()
                return len(records)
            except BaseException:
                await conn.rollback()
                raise

    async def account(self, owner):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await conn.begin()
                meta, imported = await self._imported(q, owner)
                await q.execute(
                    "SELECT card_code,owner_id,listed,epoch_ms,epoch_source,seen_ms,facts_json "
                    "FROM karuta_inventory_membership "
                    "WHERE owner_id=%s LIMIT 100001",
                    (owner,),
                )
                rows = list(await q.fetchall())
                own_known = bool(rows)
                if meta:
                    await q.execute(
                        "SELECT m.card_code,m.owner_id,m.listed,m.epoch_ms,m.epoch_source,"
                        "m.seen_ms,m.facts_json FROM karuta_inventory_import_cards i "
                        "JOIN karuta_inventory_membership m USING(card_code) "
                        "WHERE i.owner_id=%s",
                        (owner,),
                    )
                    rows.extend(await q.fetchall())
                native = {
                    r[0]: dict(
                        owner=r[1],
                        listed=bool(r[2]),
                        epoch=[r[3], r[4]],
                        seen_ms=r[5],
                        facts=json.loads(r[6]),
                    )
                    for r in rows
                }
                if len(native) > MAX_CARDS:
                    raise ValueError("inventory limit exceeded")
                known, names = await self._canonical(q, set(native) - set(imported))
                await q.execute(
                    "SELECT source_id FROM karuta_inventory_queries WHERE owner_id=%s LIMIT 1",
                    (owner,),
                )
                observed = bool(await q.fetchone())
                result = dict(
                    known=meta is not None or own_known or observed,
                    cards=effective(owner, meta, imported, native, known, names),
                    imported=meta is not None,
                )
                await conn.rollback()
                return result
            except BaseException:
                await conn.rollback()
                raise

    async def native(self, q, page, prior):
        meta = await self._meta(q, page.owner)
        if not meta or page.epoch <= [meta["created"], meta["source"]]:
            return
        meta, imported = await self._imported(q, page.owner, meta)
        for code in negative_updates(page, meta, imported, prior):
            await q.execute(
                "UPDATE karuta_inventory_import_cards SET listed=FALSE,removed_ms=%s "
                "WHERE owner_id=%s AND card_code=%s",
                (page.stamp.at_ms, page.owner, code),
            )

    async def wishlist(self, owner):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            await q.execute(
                "SELECT document FROM karuta_personal_wishlists WHERE owner_id=%s", (owner,)
            )
            row = await q.fetchone()
            await conn.rollback()
            return json.loads(row[0]) if row else None

    async def observe_wishlist(self, value):
        async with self.pool.acquire() as conn, conn.cursor() as q:
            try:
                await conn.begin()
                await self.lock(q)
                await q.execute(
                    "SELECT document FROM karuta_personal_wishlists WHERE owner_id=%s",
                    (value["owner"],),
                )
                row = await q.fetchone()
                updated = merge(json.loads(row[0]) if row else None, value)
                await q.execute(
                    "INSERT INTO karuta_personal_wishlists VALUES (%s,%s) "
                    "ON DUPLICATE KEY UPDATE document=VALUES(document)",
                    (value["owner"], json.dumps(updated)),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise


def digest_key(owner, source):
    import hashlib

    return hashlib.sha256(f"csv:{owner}:{source}".encode()).hexdigest()
