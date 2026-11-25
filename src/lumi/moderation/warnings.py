from __future__ import annotations

import re

from .operation_tags import tag_operation
from .rules import identifier, reason


async def apply_warning(store, queue, request, clock):

    name = request.handler.rpartition(".")[2]
    event, args = request.event, request.arguments
    guild, actor, channel, source = (event.get(k) for k in (
        "guild_id", "actor_id", "channel_id", "message_id",
    ))
    if (
        name not in {"warn", "unwarn"}
        or any(type(v) is not int or v <= 0 for v in (guild, actor, channel, source))
        or request.route != request.handler or event.get("dispatch") != "command"
    ):
        return "action_scope_mismatch", {}
    target = args.get("mention") or args.get("user_id")
    data = dict(actor=actor, channel=channel, target=target, reason=reason(args.get("reason")))
    display = ""
    if name == "warn":
        facts = event.get("member_target")
        if not isinstance(facts, dict) or facts.get("id") != target:
            return "unavailable", {}
        if not facts.get("present"):
            return "member_unavailable", {}
        if (
            actor == target or facts.get("target_owns_guild") or facts.get("target_is_client")
            or not (facts.get("actor_owns_guild") or facts.get("actor_outranks_target"))
        ):
            return "target_protected", {}
        if not data["reason"]:
            return "invalid_reason", {}
        display = str(facts.get("display_name") or target)[:80]
    else:
        code = str(args.get("warning_id") or "").lower()
        if not re.fullmatch(r"[a-f0-9]{8}", code):
            return "action_scope_mismatch", {}
        data["warning_id"] = code

    now = clock()
    record = dict(
        id=identifier(guild, source, name), guild=guild, created=now, request=data,
        status="prepared", steps={}, target_name=display, before_until=0,
    )
    await tag_operation(queue, record)
    if event.get("_expires_at_ms", now + 1) <= clock():
        return "unavailable", {}
    saved = await (store.warn(record) if name == "warn" else store.revoke(record))
    if saved["guild"] != guild or saved["request"] != data:
        return "action_scope_mismatch", {}
    if saved["status"] != "done":
        return "uncertain", {}
    result = saved["result"]
    return result.get("error") or result["status"], result.get("data", {})
