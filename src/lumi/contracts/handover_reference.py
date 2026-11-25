import re
from typing import Literal

import msgspec

REQUESTS = "lumi:channel-reference:requests"
REPLIES = "lumi:channel-reference:reply:"

REQUEST_MS = 15_000
PARTICIPANTS = frozenset({"platform", "security", "community", "cards"})
PHASES = {
    "validate": {"building"}, "apply": {"applying"},
    "verify": {"publishing", "deleting"}, "rollback": {"rollback"},
    "preflight": {"waiting", "quiesced", "creating", "building", "applying", "publishing", "deleting"},
}


class ReferenceRequest(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    request: str
    operation: Literal["validate", "apply", "verify", "rollback", "preflight"]
    generation: int
    code: str
    phase: str
    epoch: int
    journal_revision: int
    revision: int
    lease: str
    deadline: int
    workers: dict[str, int] | None = None

    def valid(self, now):
        return (
            re.fullmatch(r"[a-f0-9]{32}", self.request)
            and re.fullmatch(r"[a-f0-9]{8}", self.code)
            and re.fullmatch(r"[a-f0-9]{32}", self.lease)
            and self.phase in PHASES.get(self.operation, ()) and self.generation > 0
            and self.revision > 0 and self.journal_revision > 0 and self.epoch >= 0
            and now < self.deadline <= now + REQUEST_MS
            and (
                self.workers is None if self.operation != "preflight" else (
                    self.workers is not None and self.workers.keys() == PARTICIPANTS
                    and all(0 < value < 2**64 for value in self.workers.values())
                    and self.workers["security"] == self.generation
                )
            )
        )


class PhaseReport(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    outcome: Literal["ready", "pending", "blocked"]
    blocker: str = ""

    def valid(self):
        return 0 < len(self.blocker) <= 200 if self.outcome == "blocked" else not self.blocker


class ReferenceReply(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    request: str
    status: Literal["ok", "conflict", "unavailable"]
    verified: bool = False
    message: str = ""
    phase_report: PhaseReport | None = None
