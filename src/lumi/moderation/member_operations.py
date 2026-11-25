import re

from redis.exceptions import RedisError

from lumi.contracts import DiscordActionResult, decode
from lumi.contracts.member_effects import (
    BAN_MEMBER,
    CLEAR_MEMBER_TIMEOUT,
    KICK_MEMBER,
    SET_MEMBER_TIMEOUT,
    UNBAN_MEMBER,
    MemberEffectSpec,
    member_effect_key,
    member_receipt_key,
)
from lumi.worker.actions import ActionUnavailable

from .operation_tags import tag_operation
from .rules import duration, identifier, reason
from .store import completed

OPERATIONS = {
    "ban": (BAN_MEMBER, {"banned"}),
    "kick": (KICK_MEMBER, {"kicked"}),
    "unban": (UNBAN_MEMBER, {"unbanned", "not_banned"}),
    "timeout": (SET_MEMBER_TIMEOUT, {"timed_out"}),
    "untimeout": (CLEAR_MEMBER_TIMEOUT, {"timeout_removed", "not_timed_out"}),
}
TARGET_FIELDS = ("mention", "user_id")
EFFECTS = {
    name: MemberEffectSpec(kind, TARGET_FIELDS, "reason") for name, (kind, _) in OPERATIONS.items()
}
REJECTED = frozenset({
    "action_scope_mismatch", "capability_denied", "discord_rejected", "expired_job",
    "guild_unavailable", "invalid_action", "member_unavailable", "permission_denied",
    "policy_denied", "privacy_pending", "stale_generation", "target_protected",
})


def outcome(stored, record):
    if stored["guild"] != record["guild"] or stored["request"] != record["request"]:
        return "action_scope_mismatch", {}
    if stored["status"] != "done":
        return "uncertain", {}
    result = stored["result"]
    return result.get("error") or result["status"], result.get("data", {})


async def settle(store, record, native, successes):
    if native.ok and native.status in successes:
        if "until" in record and native.data.get("until") != record["until"]:
            return "uncertain", {}
        record["before_until"] = native.data.get("before_until", 0)
        record["target_name"] = native.data.get("display_name", "")
        payload = {k: native.data[k] for k in ("display_name", "until") if k in native.data}
        completed(record, native.status, **payload)
    elif not native.ok and native.error_code in REJECTED:
        record.update(status="done", result=dict(status="failed", error=native.error_code, data={}))
    else:
        return "uncertain", {}
    await store.save(record)
    return outcome(record, record)


async def apply(store, queue, actions, request, job_id, clock, *, group="security"):
    name = request.handler.rpartition(".")[2]
    if store is None or name not in OPERATIONS:
        return "unavailable", {}
    kind, successes = OPERATIONS[name]
    event, args = request.event, request.arguments
    guild, actor, channel, source = (event.get(k) for k in (
        "guild_id", "actor_id", "channel_id", "message_id",
    ))
    targets = {args[field] for field in TARGET_FIELDS if args.get(field) is not None}
    identifiers = (guild, actor, channel, source, *targets)
    if (
        len(targets) != 1 or request.handler != request.route or event.get("dispatch") != "command"
        or any(type(v) is not int or not 0 < v < 2**64 for v in identifiers)
    ):
        return "action_scope_mismatch", {}
    target = targets.pop()
    data = dict(actor=actor, channel=channel, target=target, reason=reason(args.get("reason")))
    if kind == SET_MEMBER_TIMEOUT:
        data["duration"] = duration(str(args.get("duration") or ""))
    now = clock()
    deadline = event.get("_expires_at_ms", 0)
    if now >= deadline:
        return "unavailable", {}
    record = dict(
        id=identifier(guild, source, name), guild=guild, created=now, request=data,
        status="prepared", steps={}, target_name="", before_until=0,
    )
    if kind != BAN_MEMBER:

        await tag_operation(queue, record)
    stored, new = await store.begin(record)
    if not new:
        original_job = stored.get("native_job", "")
        if (
            stored["guild"] == guild and stored["request"] == data
            and stored["status"] == "sending" and re.fullmatch(r"[a-f0-9]{32}", original_job)
        ):
            try:
                raw = await queue.get(member_receipt_key(group, "moderation", original_job))
                if raw:
                    return await settle(store, stored, decode(raw, DiscordActionResult), successes)
            except (RedisError, ValueError):
                return "uncertain", {}
        return outcome(stored, record)
    if kind == SET_MEMBER_TIMEOUT:
        record["until"] = clock() + data["duration"] * 1000
    record.update(status="sending", native_job=job_id)
    await store.save(record)
    if clock() >= deadline:
        record.update(status="done", result=dict(status="failed", error="expired_job", data={}))
        await store.save(record)
        return "expired_job", {}
    try:
        native = await actions.request(
            job_id=job_id, kind=kind, guild_id=guild, channel_id=channel, message_id=source,
            member_id=target, reason=args.get("reason"), timeout_until_ms=record.get("until"),
            idempotency_key=member_effect_key(job_id),
        )
    except (ActionUnavailable, OSError, TimeoutError, RedisError):
        return "uncertain", {}
    return await settle(store, record, native, successes)
