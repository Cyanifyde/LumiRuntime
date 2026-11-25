from __future__ import annotations

import math
import random
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal

from lumi.members.search import edit_distance

MAX_WL = 2_147_483_647
EFFECTS = ("mosaic", "glass", "polar", "spectral", "ribbons", "adaptive")
TIMERS = (30, 60, 120)
ROUNDS = (1, 3, 5, 10, 20)
MAX_ROUNDS = 100
MIN_TIMER = 1
MAX_TIMER = 300
NUMBER = r"\d+(?:\.\d+)?[kK]?"
OP = r"(?:<=|>=|<|>|=)"


class GameError(ValueError):
    pass


@dataclass(frozen=True)
class Bounds:
    low: int = 0
    high: int = MAX_WL

    def contains(self, value: int | None) -> bool:
        return value is not None and self.low <= value <= self.high

    @property
    def label(self):
        if self.low == 0 and self.high == MAX_WL:
            return "All recorded WL"
        if self.low == self.high:
            return f"WL {self.low:,}"
        if self.high == MAX_WL:
            return f"WL ≥ {self.low:,}"
        return f"WL {self.low:,}\u2013{self.high:,}"


@dataclass(frozen=True)
class SetupOptions:
    query: str = ""
    rounds: int = 5
    timer: int = 60


def _number(value):
    value = value.lower()
    number = Decimal(value[:-1]) * 1000 if value.endswith("k") else Decimal(value)
    if number != int(number) or not 0 <= number <= MAX_WL:
        raise GameError("Use whole WL values from 0 to 2,147,483,647.")
    return int(number)


def parse_wishlist(raw: str) -> Bounds:
    if len(raw) > 128:
        raise GameError("WL filter is too long.")
    text = re.sub(r"\s+", "", raw).lower()
    if not text:
        return Bounds()
    predicates = []
    if match := re.fullmatch(rf"({NUMBER})\.\.({NUMBER})", text):
        predicates = [(">=", _number(match[1])), ("<=", _number(match[2]))]
    elif re.fullmatch(NUMBER, text):
        predicates = [("=", _number(text))]
    elif match := re.fullmatch(rf"(?:wl)?({OP})({NUMBER})", text):
        predicates = [(match[1], _number(match[2]))]
    elif match := re.fullmatch(rf"({NUMBER})({OP})(?:wl)?", text):
        reverse = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "="}
        predicates = [(reverse[match[2]], _number(match[1]))]
    elif match := re.fullmatch(rf"({NUMBER})({OP})(?:wl)?({OP})({NUMBER})", text):
        reverse = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "="}
        predicates = [(reverse[match[2]], _number(match[1])), (match[3], _number(match[4]))]
    else:
        raise GameError("Use a WL filter such as 5<<10 or 20>=>2.")
    low, high = 0, MAX_WL
    for op, number in predicates:
        if op == "=":
            low, high = max(low, number), min(high, number)
        elif op in {">", ">="}:
            low = max(low, number + (op == ">"))
        else:
            high = min(high, number - (op == "<"))
    if low > high:
        raise GameError("That WL range is empty.")
    return Bounds(low, high)


def validate_settings(rounds: int, timer: int) -> None:
    if type(rounds) is not int or not 1 <= rounds <= MAX_ROUNDS:
        raise GameError(f"Rounds must be 1\u2013{MAX_ROUNDS}.")
    if type(timer) is not int or not MIN_TIMER <= timer <= MAX_TIMER:
        raise GameError("Time must be 1s\u20135m.")


def parse_timer(raw: str) -> int:
    if not raw or len(raw) > 24:
        raise GameError("Use a time such as 90s or 3m.")
    if re.fullmatch(r"[0-9]{1,5}", raw):
        seconds = Decimal(raw)
    else:
        seconds = Decimal(0)
        position = 0
        for token in re.finditer(r"(?i)([0-9]{1,5}(?:\.[0-9]{1,3})?)([smh])", raw):
            if token.start() != position:
                raise GameError("Use a time such as 90s or 3m.")
            seconds += Decimal(token[1]) * {"s": 1, "m": 60, "h": 3600}[token[2].lower()]
            position = token.end()
        if position != len(raw):
            raise GameError("Use a time such as 90s or 3m.")
    if seconds != int(seconds) or not MIN_TIMER <= seconds <= MAX_TIMER:
        raise GameError("Time must be 1s\u20135m.")
    return int(seconds)


def parse_setup(raw: str) -> SetupOptions:
    if len(raw) > 256 or "\n" in raw or "\r" in raw:
        raise GameError("Invalid game options.")
    tokens, query, values = raw.split(), [], {}
    index = 0
    aliases = {"rounds": "rounds", "r": "rounds", "time": "timer", "timer": "timer", "t": "timer"}
    while index < len(tokens):
        token = tokens[index]
        index += 1
        if not token.startswith("-"):
            query.append(token)
            continue
        flag, separator, value = token.partition("=")
        name = flag.removeprefix("--").removeprefix("-").lower()
        if name not in aliases or flag.startswith("---"):
            raise GameError("Unknown game option.")
        key = aliases[name]
        if key in values:
            raise GameError("Use each option once.")
        if not separator:
            if index == len(tokens):
                raise GameError("Missing option value.")
            value = tokens[index]
            index += 1
        if key == "rounds":
            if not re.fullmatch(r"[0-9]{1,3}", value):
                raise GameError(f"Rounds must be 1\u2013{MAX_ROUNDS}.")
            values[key] = int(value)
        else:
            values[key] = parse_timer(value)
    result = SetupOptions(" ".join(query), **values)
    validate_settings(result.rounds, result.timer)
    parse_wishlist(result.query)
    return result


def next_effect(seed: int, queue: list[str] | tuple[str, ...], last: str, cycle: int):

    if (
        not isinstance(queue, (list, tuple))
        or any(effect not in EFFECTS for effect in queue)
        or len(set(queue)) != len(queue)
        or type(cycle) is not int
        or not 0 <= cycle <= MAX_ROUNDS
    ):
        raise GameError("Invalid filter order.")
    remaining = list(queue)
    if not remaining:
        rng = random.Random(f"kguess-filters-2:{seed}:{cycle}")
        remaining = list(EFFECTS)
        rng.shuffle(remaining)
        if remaining[0] == last:
            index = rng.randrange(1, len(remaining))
            remaining[0], remaining[index] = remaining[index], remaining[0]
        cycle += 1
    return remaining[0], remaining[1:], cycle


def normalize(value: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKC", value).casefold() if c.isalnum())


def matches(guess: str, answers: list[str] | tuple[str, ...]) -> bool:
    if not guess or len(guess) > 240:
        return False
    text = normalize(guess)
    if not text:
        return False
    for answer in answers:
        candidate = normalize(answer)
        if not candidate:
            continue
        if text == candidate:
            return True
        shortest = min(len(text), len(candidate))
        limit = 0 if shortest < 3 else 1 if len(candidate) <= 5 else 2
        if limit and edit_distance(text, candidate, limit) <= limit:
            return True
    return False


def _units(text):
    result = []
    for char in text:
        if result and (
            unicodedata.combining(char) or char in "\ufe0f\u200d" or result[-1].endswith("\u200d")
        ):
            result[-1] += char
        else:
            result.append(char)
    return result


def reveal_order(answer: str, seed: int) -> dict:
    chars = _units(answer)
    words, current = [], []
    for i, char in enumerate(chars):
        if any(c.isalnum() for c in char):
            current.append(i)
        elif current:
            words.append(current)
            current = []
    if current:
        words.append(current)
    visible = {i for word in words for i in (word if len(word) <= 3 else word[:1])}
    letters = [i for word in words for i in word]
    hidden = [i for i in letters if i not in visible]
    rng = random.Random(seed)
    if letters and not hidden:
        hidden = [rng.choice(letters)]
        visible.discard(hidden[0])
    rng.shuffle(hidden)
    return {"units": chars, "initial": sorted(visible), "order": hidden}


def mask_answer(reveal: dict, stage: int, *, complete=False) -> str:
    hidden = reveal["order"]
    shown = min(max(0, len(hidden) - 1), math.floor(len(hidden) * min(stage, 5) / 5))
    visible = set(reveal["initial"]) | set(hidden[:shown])
    return "".join(
        char if complete or index in visible or not any(c.isalnum() for c in char) else "_"
        for index, char in enumerate(reveal["units"])
    )


def points(elapsed_ms: int, duration_ms: int) -> int:
    return max(100, 1000 - (900 * max(0, elapsed_ms) // duration_ms))


def choose(pool: list[dict], mode: str, used: list[str], rng: random.Random) -> dict | None:
    eligible = [row for row in pool if answer_key(row, mode) not in used]
    return rng.choice(eligible) if eligible else None


def answer_key(row: dict, mode: str) -> str:


    return normalize(row["character"] if mode == "character" else row["series"])


def pool_size(pool: list[dict], mode: str) -> int:
    return len({answer_key(row, mode) for row in pool})
