from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from .models import Card, Stamp

WINDOW_MS = 900_000


@dataclass(frozen=True, slots=True)
class CollectionPage:
    owner: int
    requester: int
    source: int
    guild: int
    channel: int
    created_ms: int
    stamp: Stamp
    start: int
    end: int
    visible: int
    total: int
    cards: tuple[Card, ...]
    query: dict

    def __post_init__(self):
        if min(self.owner, self.source, self.guild, self.channel, self.created_ms) <= 0:
            raise ValueError("incomplete collection provenance")
        if not 0 <= self.visible <= self.total <= 999999 or self.stamp.at_ms < self.created_ms:
            raise ValueError("invalid collection clock or count")
        if self.visible == 0:
            if self.start or self.end or self.cards:
                raise ValueError("invalid empty collection")
        elif not 1 <= self.start <= self.end <= self.visible or (
            len(self.cards) != self.end - self.start + 1 or not 1 <= len(self.cards) <= 10
        ):
            raise ValueError("incomplete collection rows")
        if len({c.code for c in self.cards}) != len(self.cards):
            raise ValueError("duplicate collection code")

    @property
    def epoch(self):

        return [self.created_ms, self.source]

    @property
    def revision(self):
        return [self.stamp.at_ms, self.stamp.order]

    @property
    def negative_allowed(self):
        query = self.query
        target = query.get("target")
        return bool(
            query.get("version") == 1
            and query.get("source_verified") is True
            and query.get("recognized") is True
            and query.get("absence_safe") is True
            and (target == self.owner if target is not None else self.requester == self.owner)
            and self.stamp.at_ms - self.created_ms <= WINDOW_MS
            and self.visible <= 1000
        )


def _matches(card: dict, filters: list[dict]) -> bool:
    for term in filters:
        field, operator, value = term.get("field"), term.get("op"), term.get("value")
        if field in {"print", "edition"}:
            actual = card.get("print_num" if field == "print" else "edition")
            if type(actual) is not int or type(value) is not int:
                return False
            if not {
                "=": actual == value,
                "!=": actual != value,
                "<": actual < value,
                "<=": actual <= value,
                ">": actual > value,
                ">=": actual >= value,
            }.get(operator, False):
                return False
        elif field == "code" and operator == "=":
            if card["code"] != value:
                return False
        else:
            return False
    return True


def _print_interval(page: CollectionPage, candidate: dict) -> bool:
    if page.query.get("order") != "print" or not page.cards:
        return False
    numbers = [c.print_num for c in page.cards]
    descending = page.query.get("direction") in {"desc", "reverse"}
    if len(set(numbers)) < 2 or numbers != sorted(numbers, reverse=descending):
        return False
    value = candidate.get("print_num")
    if type(value) is not int:
        return False
    lower, upper = min(numbers), max(numbers)
    genuine_last = page.end == page.visible == page.total

    covers_low = (page.start == 1 and not descending) or (genuine_last and descending)
    covers_high = (page.start == 1 and descending) or (genuine_last and not descending)
    return (covers_low or value > lower) and (covers_high or value < upper)


def _history(page, code, kind, previous=None):
    record = dict(
        code=code,
        kind=kind,
        owner=page.owner,
        previous_owner=previous,
        source=page.source,
        epoch=page.epoch,
        revision=page.revision,
    )
    record["id"] = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
    return record


def reconcile(page: CollectionPage, existing: dict, session: dict | None):

    fingerprint = hashlib.sha256(
        json.dumps(
            [
                page.start,
                page.end,
                page.visible,
                page.total,
                page.query,
                [asdict(c) for c in page.cards],
            ],
            sort_keys=True,
        ).encode()
    ).hexdigest()
    if session and (
        session["owner"] != page.owner
        or session["created_ms"] != page.created_ms
        or session["revision"] > page.revision
    ):
        return None
    if session and session["revision"] == page.revision:

        return ({}, [], session) if session.get("fingerprint") == fingerprint else None
    pages = dict(session.get("pages", {})) if session else {}
    key = str(page.start)
    this_page = dict(end=page.end, codes=[c.code for c in page.cards])
    signature = [page.owner, page.visible, page.total, page.query.get("key")]
    if (session and session["signature"] != signature) or (
        key in pages and pages[key] != this_page
    ):
        pages.clear()
    pages[key] = this_page
    if len(pages) > 100 or page.stamp.at_ms - page.created_ms > WINDOW_MS:
        pages = {key: this_page}
    session = dict(
        owner=page.owner,
        created_ms=page.created_ms,
        revision=page.revision,
        signature=signature,
        fingerprint=fingerprint,
        pages=pages,
    )
    changes, history = {}, []
    for card in page.cards:
        old = existing.get(card.code)
        if old and old["epoch"] > page.epoch:
            continue
        facts = dict(old["facts"]) if old else {}
        facts.update({name: value for name, value in asdict(card).items() if value is not None})
        record = dict(
            owner=page.owner, listed=True, epoch=page.epoch, seen_ms=page.stamp.at_ms, facts=facts
        )
        changes[card.code] = record
        if not old or not old["listed"] or old["owner"] != page.owner:
            kind = (
                "seen" if not old else "transferred" if old["owner"] != page.owner else "restored"
            )
            history.append(
                {**_history(page, card.code, kind, old["owner"] if old else None), "card": facts}
            )
    positions, seen, duplicate = {}, set(), False
    for start, entry in pages.items():
        for position, code in enumerate(entry["codes"], start=int(start)):
            if position in positions or code in seen:
                duplicate = True
            positions[position] = code
            seen.add(code)
    complete = bool(
        not duplicate
        and page.visible == page.total
        and len(positions) == page.visible
        and (not positions or (min(positions) == 1 and max(positions) == page.visible))
        and (page.visible <= 10 or page.query.get("stable_order") is True)
    )
    if page.negative_allowed:
        for code, old in existing.items():
            if (
                old["owner"] != page.owner
                or not old["listed"]
                or code in seen
                or old["epoch"] >= page.epoch
                or not _matches(old["facts"], page.query.get("filters", []))
            ):
                continue
            reason = (
                "complete_query"
                if complete
                else "print_interval"
                if (_print_interval(page, old["facts"]))
                else None
            )
            if reason:
                changes[code] = {
                    **old,
                    "listed": False,
                    "epoch": page.epoch,
                    "seen_ms": page.stamp.at_ms,
                }
                history.append(
                    {
                        **_history(page, code, "unlisted", page.owner),
                        "reason": reason,
                        "card": old["facts"],
                        "query": page.query,
                        "range": [page.start, page.end, page.visible, page.total],
                    }
                )
    return changes, history, session


def wishlist_changes(page: CollectionPage, records: dict, identities: dict[str, str]):
    values = {}
    for card in page.cards:
        if card.wishlist is not None:
            values.setdefault(identities[card.code], set()).add(card.wishlist)
    changes, history = {}, []
    stamp = [page.stamp.at_ms, page.source, page.stamp.order]
    for key, counts in values.items():
        old = records.get(key)
        if len(counts) != 1 or (old and old["stamp"] >= stamp):
            continue
        value = next(iter(counts))
        changes[key] = dict(value=value, stamp=stamp)
        if not old or old["value"] != value:
            event = dict(
                identity=key, value=value, previous=old["value"] if old else None, stamp=stamp
            )
            event["id"] = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
            history.append(event)
    return changes, history
