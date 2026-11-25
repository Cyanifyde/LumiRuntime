from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from fractions import Fraction

import msgspec

from .models import DAY_MS, GAMES, DropRecord, day_start

METRICS = {
    "drops": "Most drops",
    "cards": "Most cards",
    "best": "Lowest number",
    "average": "Lowest average",
    "median": "Lowest median",
    "low_rate": "Below 100",
}
VIEWS = {"counts": "Counts", "stats": "Server", "user": "Player", "lb": "Leaderboard"}


def positive_number(value, maximum=2_147_483_647):
    return value if type(value) is int and 1 <= value <= maximum else None


class CardDetail(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    position: int
    number: int | None = None
    edition: int | None = None
    character: str | None = None
    series: str | None = None
    token: str = ""

    def valid(self):
        return (
            type(self.position) is int
            and 1 <= self.position <= 10
            and (
                self.number is None
                or (type(self.number) is int and 1 <= self.number <= 2_147_483_647)
            )
            and (self.edition is None or (type(self.edition) is int and 1 <= self.edition <= 100))
            and all(
                value is None or (isinstance(value, str) and 0 < len(value) <= 240)
                for value in (self.character, self.series)
            )
            and isinstance(self.token, str)
            and len(self.token) <= 100
        )


class DropDetail(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    source: DropRecord
    revision_ms: int
    order: int = 0
    dropper: int | None = None
    kind: str = "unknown"
    cards: tuple[CardDetail, ...] = ()
    slots: int | None = None
    image_ref: str = ""
    dropper_label: str = ""

    def valid(self):
        return (
            self.source.valid()
            and type(self.revision_ms) is int
            and self.source.occurred_at_ms <= self.revision_ms < 2**63
            and type(self.order) is int
            and 0 <= self.order < 2**63
            and (
                self.dropper is None
                or (type(self.dropper) is int and 10**16 <= self.dropper < 2**64)
            )
            and self.kind in {"user", "server", "unknown"}
            and (self.kind == "user") == (self.dropper is not None)
            and (self.slots is None or (type(self.slots) is int and 1 <= self.slots <= 10))
            and len(self.cards) <= 10
            and all(card.valid() for card in self.cards)
            and len({card.position for card in self.cards}) == len(self.cards)
            and (
                not self.image_ref
                or (
                    len(self.image_ref) == 64
                    and all(c in "0123456789abcdef" for c in self.image_ref)
                )
            )
            and (self.source.game == "karuta" or all(c.edition is None for c in self.cards))
            and isinstance(self.dropper_label, str)
            and len(self.dropper_label) <= 80
        )


def merge_detail(old: DropDetail | None, new: DropDetail) -> DropDetail:

    if old and (new.revision_ms, new.order) < (old.revision_ms, old.order):
        return old
    cards = {c.position: c for c in old.cards} if old else {}
    tokens = {c.token: c.position for c in cards.values() if c.token}
    for card in new.cards:
        position = tokens.get(card.token, card.position) if card.token else card.position
        previous = cards.get(position)
        if previous and previous.token and card.token and previous.token != card.token:
            continue
        values = {
            key: getattr(card, key)
            if getattr(card, key) is not None
            else getattr(previous, key)
            if previous
            else None
            for key in ("number", "edition", "character", "series")
        }
        cards[position] = CardDetail(
            position, **values, token=card.token or (previous.token if previous else "")
        )
    slots = new.slots if new.slots is not None else old.slots if old else None
    for position in range(1, (slots or 0) + 1):
        cards.setdefault(position, CardDetail(position))
    return msgspec.structs.replace(
        new,
        dropper=new.dropper if new.kind != "unknown" else old.dropper if old else None,
        kind=new.kind if new.kind != "unknown" else old.kind if old else "unknown",
        slots=slots,
        image_ref=new.image_ref or (old.image_ref if old else ""),
        dropper_label=""
        if new.kind == "server"
        else new.dropper_label or (old.dropper_label if old else ""),
        cards=tuple(cards[p] for p in sorted(cards)),
    )


@dataclass
class Summary:
    drops: int = 0
    cards: int = 0
    manual: int = 0
    automatic: int = 0
    unknown: int = 0
    numbers: Counter = field(default_factory=Counter)
    editions: Counter = field(default_factory=Counter)
    characters: Counter = field(default_factory=Counter)
    series: Counter = field(default_factory=Counter)
    users: set = field(default_factory=set)
    days: Counter = field(default_factory=Counter)
    hours: Counter = field(default_factory=Counter)

    def add(self, detail):
        self.drops += 1
        self.cards += len(detail.cards)
        self.manual += detail.kind == "user"
        self.automatic += detail.kind == "server"
        self.unknown += detail.kind == "unknown"
        if detail.dropper:
            self.users.add(detail.dropper)
        at = detail.source.occurred_at_ms
        self.days[day_start(at)] += 1
        self.hours[at // 3_600_000 % 24] += 1
        for card in detail.cards:
            if card.number is not None:
                self.numbers[card.number] += 1
            if card.edition is not None:
                self.editions[card.edition] += 1
            if card.character:
                self.characters[card.character, card.series or ""] += 1
            if card.series:
                self.series[card.series] += 1

    @property
    def samples(self):
        return sum(self.numbers.values())

    @property
    def average(self):
        return (
            Fraction(sum(n * count for n, count in self.numbers.items()), self.samples)
            if self.samples
            else None
        )

    @property
    def median(self):
        n = self.samples
        if not n:
            return None
        found, seen = [], 0
        for value, count in sorted(self.numbers.items()):
            for target in ((n - 1) // 2, n // 2):
                if seen <= target < seen + count:
                    found.append(value)
            seen += count
        return Fraction(sum(found), 2)

    @property
    def best(self):
        return min(self.numbers, default=None)

    @property
    def low_rate(self):
        return (
            Fraction(sum(count for n, count in self.numbers.items() if n < 100), self.samples)
            if self.samples
            else None
        )

    @property
    def average_edition(self):
        n = sum(self.editions.values())
        return Fraction(sum(e * count for e, count in self.editions.items()), n) if n else None

    def buckets(self):
        return tuple(
            sum(count for n, count in self.numbers.items() if low <= n <= high)
            for low, high in ((1, 9), (10, 99), (100, 999), (1000, 2_147_483_647))
        )


@dataclass
class Report:
    cutoff_ms: int
    start_ms: int
    end_ms: int
    games: dict[str, Summary] = field(default_factory=lambda: {g: Summary() for g in GAMES})
    players: dict[int, Summary] = field(default_factory=dict)
    labels: dict[int, str] = field(default_factory=dict)

    def add(self, detail, *, leaderboard=False):
        self.games[detail.source.game].add(detail)
        if leaderboard and detail.dropper:
            self.players.setdefault(detail.dropper, Summary()).add(detail)
        if detail.dropper and detail.dropper_label:
            self.labels[detail.dropper] = detail.dropper_label

    def leaderboard(self, metric):
        if metric not in METRICS:
            raise ValueError("Invalid metric.")
        if metric not in {"drops", "cards"} and sum(s.drops > 0 for s in self.games.values()) > 1:
            raise ValueError("Select one game for numerical rankings.")
        entries = [
            (user, getattr(summary, metric), summary) for user, summary in self.players.items()
        ]
        entries = [row for row in entries if row[1] is not None and row[2].drops]
        reverse = metric in {"drops", "cards", "low_rate"}
        entries.sort(key=lambda row: (-row[1] if reverse else row[1], row[0]))
        output, rank, previous = [], 0, None
        for index, (user, value, summary) in enumerate(entries):
            if index == 0 or value != previous:
                rank = index + 1
            output.append(
                (
                    rank,
                    user,
                    value,
                    summary.drops
                    if metric == "drops"
                    else summary.cards
                    if metric == "cards"
                    else summary.samples,
                )
            )
            previous = value
        return output

    @property
    def observed_days(self):
        return max(
            1, (self.end_ms - day_start(max(self.start_ms, self.cutoff_ms)) + DAY_MS - 1) // DAY_MS
        )
