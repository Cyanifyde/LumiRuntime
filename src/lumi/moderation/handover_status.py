from dataclasses import dataclass

from lumi.contracts.handover_reference import PhaseReport

from .handover_store import GROUPS, HEARTBEAT_SKEW_MS, HEARTBEAT_TTL_MS

CHECK_PHASES = frozenset({"waiting", "quiesced"})
UNAVAILABLE = "unavailable"
BLOCKERS = {
    "giveaway_delivery": "A giveaway delivery needs to settle.",
    "event_drop": "Waiting for the event drop to settle.",
    "event_display": "An event display needs repair.",
    "reminder_delivery": "A reminder delivery needs to settle.",
    "feed_delivery": "A feed delivery needs to settle.",
    "image_incident": "A moderation incident needs to settle.",
    "moderation_action": "A moderation action needs review.",
    "modlog_delivery": "A Modlog delivery needs to settle.",
}


@dataclass(frozen=True, slots=True)
class Preflight:
    blocker: str | None


def preflight(acks, phase, now):

    if acks is None:
        return None
    reported = set()
    for ack in acks:
        at, idle = ack.get("at"), ack.get("idle")
        if (
            type(at) is not int or not now - HEARTBEAT_TTL_MS <= at <= now + HEARTBEAT_SKEW_MS
            or type(idle) is not bool or (phase == "quiesced" and not idle)
            or "blocker" not in ack
        ):
            return None
        blocker = ack["blocker"]
        if blocker is not None:
            if not isinstance(blocker, str) or blocker not in BLOCKERS:
                return None
            reported.add(blocker)
    return Preflight(next((text for key, text in BLOCKERS.items() if key in reported), None))


def check_phase(record, workers, expected, now):

    if workers != expected or set(workers) != GROUPS:
        return PhaseReport("pending")
    phase, epoch = record["phase"], record.get("epoch", 0)
    if phase == "building" and record.get("errors"):
        return PhaseReport("blocked", "A saved panel needs repair before resetting.")
    if phase not in (*CHECK_PHASES, "building", "publishing"):
        return PhaseReport("ready")
    reports = []
    for group in GROUPS:
        ack = record.get("acks", {}).get(group)
        if not isinstance(ack, dict) or (ack.get("phase"), ack.get("generation"), ack.get("epoch", 0)) != (
            phase, workers[group], epoch,
        ):
            return PhaseReport("pending")
        reports.append(ack)
    if phase in CHECK_PHASES:
        result = preflight(reports, phase, now)
        if result is None:
            return PhaseReport("pending")
        if result.blocker:
            return PhaseReport("blocked", result.blocker if phase == "waiting" else "A delivery is unresolved.")
    return PhaseReport("ready")
