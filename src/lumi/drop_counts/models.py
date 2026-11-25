from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import msgspec

DAY_MS = 86_400_000
DISCORD_EPOCH_MS = 1_420_070_400_000
GAMES = ("glitch", "karuta", "sofi")
BOT_IDS = {
    "glitch": -1,
    "karuta": -2,
    "sofi": -3,
}
LABELS = {"glitch": "Glitch", "karuta": "Karuta", "sofi": "Sofi"}
COLOURS = {"glitch": "#65c9c4", "karuta": "#e8bc70", "sofi": "#b69ae7"}
RANGES = (7, 14, 28)


class DropRecord(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    guild_id: int
    channel_id: int
    message_id: int
    game: str

    @property
    def occurred_at_ms(self) -> int:

        return (self.message_id >> 22) + DISCORD_EPOCH_MS

    def valid(self) -> bool:
        return self.game in GAMES and all(
            type(value) is int and 10**16 <= value < 2**64
            for value in (self.guild_id, self.channel_id, self.message_id)
        )


@dataclass(frozen=True, slots=True)
class DailyCount:
    day_ms: int
    glitch: int = 0
    karuta: int = 0
    sofi: int = 0

    def count(self, game: str) -> int:
        if game not in GAMES:
            raise ValueError("Unknown game.")
        return getattr(self, game)


def day_start(now_ms: int) -> int:
    return now_ms // DAY_MS * DAY_MS


def date_label(epoch_ms: int) -> str:
    value = datetime.fromtimestamp(epoch_ms / 1000, UTC)
    return f"{value.day} {value:%b}"


def series_rows(start_ms: int, days: int, counts: dict[tuple[int, str], int]):
    if days not in RANGES or start_ms != day_start(start_ms):
        raise ValueError("Invalid chart range.")
    return tuple(
        DailyCount(
            start_ms + index * DAY_MS,
            *(int(counts.get((start_ms + index * DAY_MS, game), 0)) for game in GAMES),
        )
        for index in range(days)
    )
