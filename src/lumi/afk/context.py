def combine_last_spoke(data):
    facts = data.get("last_spoke")
    if not isinstance(facts, list):
        raise ValueError("AFK activity unavailable")
    seen = set()
    latest = (0, 0)
    for row in facts:
        if not isinstance(row, dict):
            raise ValueError("AFK activity unavailable")
        guild, occurred, message = (
            row.get(key) for key in ("guild_id", "occurred_at_ms", "message_id")
        )
        if (
            any(type(value) is not int or value <= 0 for value in (guild, occurred, message))
            or guild in seen
        ):
            raise ValueError("AFK activity unavailable")
        seen.add(guild)
        latest = max(latest, (occurred, message))
    if not seen <= {source["id"] for source in data["guilds"]}:
        raise ValueError("AFK activity unavailable")
    return latest
