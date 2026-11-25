async def giveaway_tables(q):
    tables = []
    for table in ("giveaway_records", "giveaway_expanded"):
        await q.execute(f"SHOW TABLES LIKE '{table}'")
        if await q.fetchone():
            tables.append(table)
    return tables
