from __future__ import annotations

from lumi.shared.private_feedback import (
    RESPONSE_LIFETIME_MS,
    PrivateFeedbackItem,
    PrivateFeedbackSnapshot,
    feedback_key,
    publish_feedback,
)

TEXT = {
    "claimed": "You’ve claimed it.",
    "taken": "Someone else claimed it.",
    "already_claimed": "You’ve already claimed a button.",
    "unavailable": "Not available to claim.",
    "timeout": "Couldn’t confirm the claim.",
}


async def project_feedback(redis, guild: int, state: dict, now_ms: int) -> None:
    for drop in (state.get("last_drop"), state.get("drop")):
        if not drop or not (saved := drop.get("feedback")):
            continue
        if saved["message"] != drop["message"] or saved["channel"] != drop["channel"]:
            continue
        ttl_ms = drop["close_at"] + RESPONSE_LIFETIME_MS - now_ms
        if ttl_ms <= 0 or not saved["verdicts"]:
            continue
        updates = {
            interaction: PrivateFeedbackItem(actor, f"lumi:halloween:claim:{drop['id']}~{slot}", TEXT[verdict])
            for interaction, (actor, slot, verdict) in saved["verdicts"].items()
        }
        await publish_feedback(
            redis,
            feedback_key("community", "halloween", guild, saved["channel"], saved["message"]),
            PrivateFeedbackSnapshot(saved["generation"], saved["revision"], saved["final"], updates),
            ttl_ms,
        )
