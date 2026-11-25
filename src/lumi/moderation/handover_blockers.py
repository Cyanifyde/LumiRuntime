import json

from .handover_sql import giveaway_tables


async def cards(q, guild, old):
    for table in await giveaway_tables(q):
        await q.execute(
            f"SELECT document FROM {table} WHERE guild_id=%s AND channel_id=%s "
            "AND status!='migrated'", (guild, old),
        )
        for raw in await q.fetchall():
            giveaway = json.loads(raw[0])
            if (
                giveaway.get("pending_edit")
                or giveaway.get("cancel_pending")
                or giveaway["status"] in {"publishing", "drawing"}
                or any(
                    d["state"] in {"pending", "sending", "ambiguous"}
                    for d in giveaway["deliveries"].values()
                )
            ):
                return "giveaway_delivery"
    await q.execute(
        "SELECT COUNT(*) FROM pogdrop_delivery_receipts WHERE guild_id=%s "
        "AND destination_channel_id=%s AND status IN ('reserved','uncertain')",
        (guild, old),
    )
    return "feed_delivery" if (await q.fetchone())[0] else None


async def community(q, guild, old):
    await q.execute("SELECT document FROM halloween_events WHERE guild_id=%s", (guild,))
    raw = await q.fetchone()
    if raw:
        state = json.loads(raw[0])
        if state.get("drop") or state.get("outbox"):
            return "event_drop"
        displays = [
            *state.get("standings_parts", []), *state.get("contribution_panels", []),
            state.get("contribution_portal") or {}, state.get("prize_prompt") or {},
        ]
        if any(
            p.get("channel") == old and p.get("status") in {"sending", "ambiguous"}
            for p in displays
        ):
            return "event_display"
    await q.execute(
        "SELECT COUNT(*) FROM reminder_occurrences WHERE guild_id=%s "
        "AND state IN ('checking','sending','ambiguous') AND "
        "JSON_VALUE(document,'$.channel_id')=%s", (guild, old),
    )
    return "reminder_delivery" if (await q.fetchone())[0] else None


async def security(q, guild, old):
    await q.execute(
        "SELECT COUNT(*) FROM imageguard_incidents WHERE guild_id=%s "
        "AND JSON_VALUE(document,'$.status')<>'done'", (guild,),
    )
    if (await q.fetchone())[0]:
        return "image_incident"
    await q.execute(
        "SELECT COUNT(*) FROM moderation_operations WHERE guild_id=%s "
        "AND JSON_VALUE(document,'$.request.channel')=%s "
        "AND JSON_VALUE(document,'$.status') IN ('sending','uncertain')",
        (guild, old),
    )
    if (await q.fetchone())[0]:
        return "moderation_action"
    await q.execute("SHOW TABLES LIKE 'modlog_documents'")
    if await q.fetchone():
        await q.execute(
            "SELECT COUNT(*) FROM modlog_documents WHERE guild_id=%s "
            "AND kind='outbox' AND state IN ('sending','uncertain') "
            "AND JSON_VALUE(document,'$.destination')=%s", (guild, old),
        )
        if (await q.fetchone())[0]:
            return "modlog_delivery"
    return None


CHECKS = {"community": community, "security": security, "cards": cards}


async def inspect_group(owner, group, guild, old):
    if group == "platform":
        return None
    check = CHECKS[group]
    async with owner.pool.acquire() as c, c.cursor() as q:
        try:
            return await check(q, guild, old)
        finally:
            await c.rollback()
