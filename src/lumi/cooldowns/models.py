from __future__ import annotations

from dataclasses import dataclass

from lumi.contracts.models import MAX_MENTION_RECIPIENTS

WINDOW_MS = 5_000
DELIVERY_MS = 300_000
MAX_RECIPIENTS = MAX_MENTION_RECIPIENTS
TERMINAL = frozenset({"sent", "skipped", "failed", "uncertain"})
DM_FAILURES = frozenset(
    {
        "dm_forbidden",
        "discord_rejected",
        "member_unavailable",
        "permission_denied",
        "channel_unavailable",
        "guild_unavailable",
    }
)
TRACKERS = {
    "sofi_tracker": ("sofi", {"drop": "drop_enabled", "series": "series_enabled"}),
    "karuta_tracker": ("karuta", {"drop": "drop_enabled", "pickup": "pickup_enabled"}),
    "glitch_tracker": ("glitch", {"drop": "drop_enabled", "grab": "grab_enabled"}),
}
CAPTIONS = {
    ("sofi_tracker", "drop"): "Sofi drop ready.",
    ("sofi_tracker", "series"): "Sofi series drop ready.",
    ("karuta_tracker", "drop"): "Karuta drop ready.",
    ("karuta_tracker", "pickup"): "Karuta pickup ready.",
    ("glitch_tracker", "drop"): "Glitch drop ready.",
    ("glitch_tracker", "grab"): "Glitch grab ready.",
}


@dataclass(frozen=True)
class Occurrence:
    tracker: str
    guild_id: int
    user_id: int
    kind: str
    revision: int
    due_at_ms: int
    channel_id: int
    mode: str

    @property
    def key(self):
        return self.tracker, self.guild_id, self.user_id, self.kind, self.revision

    @property
    def expires_at_ms(self):
        return self.due_at_ms + DELIVERY_MS


@dataclass(frozen=True)
class Batch:
    id: str
    tracker: str
    guild_id: int
    channel_id: int
    kind: str
    revision: int
    opened_at_ms: int
    flush_at_ms: int
    expires_at_ms: int
    state: str = "open"
    outcome: dict | None = None


@dataclass(frozen=True)
class Recipient:
    user_id: int
    expires_at_ms: int
    mode: str
    revision: int
