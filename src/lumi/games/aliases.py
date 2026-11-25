from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from .kguess import normalize


def resolve(catalogue, records):
    identities = defaultdict(set)
    owners = defaultdict(set)
    for key, character, _series in catalogue:
        identities[normalize(character)].add(key)
        owners[normalize(character)].add(key)
    candidates = []
    for record in records:
        keys = identities.get(normalize(record["name"]), set())
        if len(keys) != 1:
            continue
        key = next(iter(keys))
        for alias in record.get("aliases", ()):
            if isinstance(alias, str) and 0 < len(alias) <= 240 and normalize(alias):
                owners[normalize(alias)].add(key)
                candidates.append((key, "character", alias))
    return sorted({row for row in candidates if owners[normalize(row[2])] == {row[0]}})


async def seed(store):
    records = json.loads(
        Path(__file__)
        .with_name("assets")
        .joinpath("character_aliases.json")
        .read_text(encoding="utf-8")
    )
    async with store.pool.acquire() as conn, conn.cursor() as q:
        await q.execute(
            "SELECT identity_key,character_name,series_name FROM karuta_library_characters"
        )
        rows = resolve(await q.fetchall(), records)
        if rows:
            await q.executemany(
                "INSERT INTO games_kguess_aliases VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE identity_key=games_kguess_aliases.identity_key",
                rows,
            )
        await conn.commit()
    return len(rows)
