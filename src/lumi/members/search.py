from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass

import msgspec

from lumi.contracts import MemberSearchCandidate, MemberSearchResult

MAX_QUERY = 100
MAX_CANDIDATES = 25
SESSION_MS = 900_000
SESSION_PREFIX = "lumi:member-search:session:"
IDENTIFIER = re.compile(r"^(?:<@!?(\d{17,20})>|(\d{17,20}))$")
TOKEN = re.compile(r"^[a-f0-9]{16}$")


class SearchSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    module: str
    owner: int
    guild: int
    channel: int
    query: str
    ids: tuple[int, ...]
    expires: int
    last_job: str = ""
    text: str | None = None


def normalize(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


def identifier(value: str) -> int | None:
    match = IDENTIFIER.fullmatch(value)
    return int(match[1] or match[2]) if match else None


@dataclass(frozen=True, slots=True)
class MemberIdentity:
    id: int
    username: str
    global_name: str | None = None
    nickname: str | None = None

    @property
    def display_name(self) -> str:
        return self.nickname or self.global_name or self.username

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                normalize(n) for n in (self.username, self.global_name, self.nickname) if n
            )
        )

    def candidate(self, match: str) -> MemberSearchCandidate:
        return MemberSearchCandidate(self.id, self.username, self.display_name, match)


def edit_distance(left: str, right: str, limit: int) -> int:

    if abs(len(left) - len(right)) > limit:
        return limit + 1
    previous = list(range(len(right) + 1))
    older = None
    for i, a in enumerate(left, 1):
        current = [limit + 1] * (len(right) + 1)
        current[0] = i
        for j in range(max(1, i - limit), min(len(right), i + limit) + 1):
            current[j] = min(
                previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (a != right[j - 1])
            )
            if older is not None and j > 1 and a == right[j - 2] and left[i - 2] == right[j - 1]:
                current[j] = min(current[j], older[j - 2] + 1)
        older, previous = previous, current
    return min(previous[-1], limit + 1)


def match_name(query: str, member: MemberIdentity, *, typos: bool = True) -> tuple[int, int] | None:
    query = normalize(query)
    names = member.names
    if query in names:
        return 0, 0
    if any(n.startswith(query) for n in names):
        return 1, 0
    if any(query in n for n in names):
        return 2, 0
    if not typos or len(query) < 3:
        return None
    limit = 1 if len(query) <= 5 else 2
    best = limit + 1
    variants = {
        word[:length]
        for name in names
        for word in {name, *re.findall(r"\w+", name)}
        for length in range(max(1, len(query) - limit), len(query) + limit + 1)
    }
    for value in variants:
        best = min(best, edit_distance(query, value, limit))
    return (3, best) if best <= limit else None


def search(query: str, members: Iterable[MemberIdentity]) -> MemberSearchResult:
    if not query or len(query) > MAX_QUERY or not normalize(query).strip():
        raise ValueError("invalid member query")
    members = tuple(members)
    best, matches = None, {}

    for typos in (False, True):
        for member in members:
            score = match_name(query, member, typos=typos)
            if score is None or (best is not None and score > best):
                continue
            if best is None or score < best:
                matches, best = {}, score
            matches[member.id] = member
        if matches:
            break
    if not matches:
        return MemberSearchResult("not_found")
    if len(matches) > MAX_CANDIDATES:
        return MemberSearchResult("too_many")
    kind = ("exact", "prefix", "substring", "typo")[best[0]]
    ordered = sorted(
        matches.values(), key=lambda m: (normalize(m.display_name), normalize(m.username), m.id)
    )
    return MemberSearchResult(
        "matched" if len(ordered) == 1 and kind != "typo" else "choose",
        tuple(m.candidate(kind) for m in ordered),
    )
