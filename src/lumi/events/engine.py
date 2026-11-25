from __future__ import annotations

import copy
import re
import secrets
from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Any

DAY = 86_400_000
HOUR = 3_600_000
DROP_GAP_MS = 600_000
MIN_REVEAL_MS = 2_000
SETTLEMENT_DELAY_MS = 5_000
PRIZE_CHANCE_PERCENT = 20


EXPECTED_PRIZE_WAIT = 10 * (DROP_GAP_MS + 6_000 + SETTLEMENT_DELAY_MS)
MAX_USERS = 1000
MAX_CODES = 5000
LEGACY_POINT_SCALING = "linear_1_20_v1"
POINT_SCALING = "linear_1_20_peak_5d_v2"
POINT_PEAK_LEAD_MS = 5 * DAY
LEGACY_SPIDER_SCORING = "half_treat_floor_v1"
THIRD_SPIDER_SCORING = "third_treat_floor_v2"
SPIDER_SCORING = "equal_treat_v3"
RELEASE_POLICY = "four_day_buffer_v1"
RNG = secrets.SystemRandom()


class EventError(ValueError):
    pass


def new_event() -> dict[str, Any]:
    return {
        "run": secrets.token_hex(8),
        "stage": "setup",
        "setup_step": "details",
        "paused": False,
        "organizer_id": 0,
        "config": {
            "points": 0,
            "hours": 0,
            "drop_ms": 0,
            "contribution_ms": DAY,
            "instructions": "",
            "channel": 0,
            "setup_channel": 0,
            "prize_pacing": True,
            "prize_release_policy": RELEASE_POLICY,
            "point_scaling": POINT_SCALING,
            "automatic_start": "after_contributions_24h_v1",
            "card_timetable": "ordered_v1",
        },
        "contribution_end": 0,
        "finish_at": 0,
        "drops_started_at": 0,
        "contributions": {},
        "ranking": [],
        "tallies": {},
        "scores": {},
        "starting": {},
        "pools": {},
        "handoffs": {},
        "drop": None,
        "last_settled": 0,
        "revision": 0,
        "processed": [],
        "outbox": [],
        "submissions_dm": None,
        "contribution_panels": [],
        "contribution_portal": None,
        "setup_messages": [],
        "standings_parts": [],
        "recent_claims": [],
        "prize_prompt": None,
        "prize_lookup": None,
        "main_prize": None,
    }


def drop_gap_ms(state: dict[str, Any]) -> int:

    gap = state["config"].get("drop_gap_ms", DROP_GAP_MS)
    return gap if type(gap) is int and DROP_GAP_MS <= gap <= DAY else DROP_GAP_MS


def codes(text: str) -> list[str]:
    parts = [part.strip().casefold() for part in re.split(r"[,\n\r]+", text) if part.strip()]
    if not parts or len(parts) > MAX_CODES:
        raise EventError("Enter card codes separated by commas.")
    if any(not re.fullmatch(r"[a-z0-9]{1,32}", part) for part in parts):
        raise EventError("Invalid card code list.")
    return list(dict.fromkeys(parts))


def frame_names(text: str) -> list[str]:

    parts = [" ".join(part.split()) for part in re.split(r"[,\n\r]+", text) if part.strip()]
    if not parts or len(parts) > MAX_CODES or any(
        not 1 <= len(part) <= 100 or any(ord(char) < 32 for char in part)
        for part in parts
    ):
        raise EventError("Enter frame names separated by commas, up to 100 characters each.")
    return parts


def duration(text: str, *, label: str = "Contribution") -> int:

    match = re.fullmatch(r"\s*(\d{1,6}(?:\.\d{1,3})?)\s*(m|h|d)\s*", text, re.IGNORECASE)
    if match is None:
        raise EventError("Use a time limit such as 30m, 2h or 30d.")
    try:
        value = Decimal(match[1]) * {"m": 60_000, "h": 3_600_000, "d": DAY}[match[2].lower()]
    except InvalidOperation as exc:
        raise EventError(f"Invalid {label.lower()} time limit.") from exc
    if value != int(value) or not 60_000 <= value <= 365 * DAY:
        raise EventError(f"{label} time limit must be between 1 minute and 365 days.")
    return int(value)


def duration_label(milliseconds: int) -> str:
    for unit, amount in (("d", DAY), ("h", 3_600_000), ("m", 60_000)):
        if milliseconds % amount == 0:
            return f"{milliseconds // amount}{unit}"
    return f"{Decimal(milliseconds) / 60_000}m"


def contribution_counts(state: dict[str, Any]) -> list[tuple[str, int]]:
    return sorted(
        ((user, len(entries)) for user, entries in state["contributions"].items() if entries),
        key=lambda item: (-item[1], int(item[0])),
    )


def event_channel(state: dict[str, Any]) -> int:

    return int(state["config"].get("setup_channel", state["config"].get("channel", 0)) or 0)


def users(text: str) -> list[int]:
    result = []
    for part in re.split(r"[,\n\r]+", text):
        if not part.strip():
            continue
        match = re.fullmatch(r"(?:<@!?)?(\d{17,20})>?", part.strip())
        if match is None:
            raise EventError("Use Discord user IDs or mentions.")
        result.append(int(match[1]))
    if not result or len(result) > MAX_USERS or len(result) != len(set(result)):
        raise EventError("Enter a unique, ordered list of people.")
    return result


def allocations(points: int, ranking: list[int]) -> dict[str, int]:

    if points < 0 or len(ranking) != len(set(ranking)) or len(ranking) > MAX_USERS:
        raise EventError("Invalid starting points.")
    remaining, awards = points, {}
    for user in ranking:
        award = remaining // 2
        awards[str(user)] = award
        remaining -= award
    return awards


def automatic(state: dict[str, Any]) -> bool:
    return state["config"].get("automatic_start") == "after_contributions_24h_v1"


def planned_start(state: dict[str, Any]) -> int:
    return state["contribution_end"] + DAY if automatic(state) and state["contribution_end"] else 0


def participants(state: dict[str, Any]) -> list[int]:
    return state.get("participants", state["ranking"])


def ordered_cards(state: dict[str, Any]) -> bool:
    return automatic(state) and state["config"].get("card_timetable") == "ordered_v1"


def tally_open(state: dict[str, Any]) -> bool:
    return state["stage"] == "tally" or (
        automatic(state) and state["stage"] in {"ready", "running"}
        and not state.get("starting_confirmed", False)
    )


def advance(state: dict[str, Any], now: int) -> None:
    if state["stage"] == "contributions" and now >= state["contribution_end"]:
        state["stage"] = "tally"
        if automatic(state):
            state["participants"] = [int(user) for user, entries in state["contributions"].items() if entries]
            for user in state["participants"]:
                state["scores"].setdefault(str(user), 0)
        else:
            state["paused"] = False
    due = planned_start(state)
    if (due and now >= due and state["stage"] in {"tally", "ready"}
            and not state["paused"] and state["config"].get("channel")):
        _start_drops(state, due, now)
    if state["stage"] == "running" and now >= state["finish_at"]:
        finish(state, now)


def _start_drops(state: dict[str, Any], start_at: int, now: int) -> None:
    state["stage"] = "running"
    state["finish_at"] = start_at + (
        state["config"].get("drop_ms") or state["config"].get("hours", 0) * HOUR
    )
    state["last_settled"] = now
    state["drops_started_at"] = start_at
    if paced(state):
        for pool in state["pools"].values():
            rebalance_pool(state, pool, start_at)


def finish(state: dict[str, Any], now: int) -> None:
    drop = state["drop"]
    if drop and drop["status"] in {"warning", "publishing"} and drop["open_at"]:
        drop["status"] = "open"
    if drop and drop["status"] in {"warning", "publishing"}:
        refund(state, drop)
        drop["status"] = "cancelled"
    elif drop and drop["status"] == "open":
        drop["close_at"] = min(drop["close_at"], now)
        drop["settle_at"] = drop["close_at"] + SETTLEMENT_DELAY_MS
    state["stage"], state["paused"] = "finished", False


def start(state: dict[str, Any], now: int) -> None:
    before = state["stage"]
    advance(state, now)
    if automatic(state) and before in {"tally", "ready"} and state["stage"] == "running":
        return
    if state["paused"]:
        state["paused"] = False
        state["last_settled"] = now
        advance(state, now)
        return
    if state["stage"] == "setup":
        if state["config"]["points"] <= 0 or (
            state["config"].get("drop_ms", 0) <= 0 and state["config"].get("hours", 0) <= 0
        ):
            raise EventError("Choose starting points and event duration.")
        state["stage"] = "contributions"
        state["contribution_end"] = now + state["config"].get("contribution_ms", DAY)
    elif state["stage"] == "ready":
        if not state["config"]["channel"]:
            raise EventError("Choose a channel.")
        if automatic(state):
            raise EventError("Drops start automatically one day after contributions close.")
        _start_drops(state, now, now)
    else:
        raise EventError("Finish the tally before starting drops.")


def contribute(state: dict[str, Any], actor: int, text: str, remove: bool, now: int) -> None:
    advance(state, now)
    if state["stage"] != "contributions" or state["paused"]:
        raise EventError("Contributions are closed.")
    entries = codes(text)
    if str(actor) not in state["contributions"] and len(state["contributions"]) >= MAX_USERS:
        raise EventError("Too many participants.")
    current = state["contributions"].setdefault(str(actor), [])
    if remove:
        state["contributions"][str(actor)] = [code for code in current if code not in entries]
    else:
        merged = list(dict.fromkeys([*current, *entries]))
        if len(merged) > MAX_CODES:
            raise EventError("Too many cards.")
        state["contributions"][str(actor)] = merged


def confirm_ranking(state: dict[str, Any]) -> None:
    if not tally_open(state) or not state["ranking"]:
        raise EventError("Enter the ranking after contributions close.")
    if automatic(state):
        if (set(state["ranking"]) != set(participants(state))
                or set(state.get("tallies", {})) != {str(user) for user in participants(state)}):
            raise EventError("Enter a tally for every contributor before confirming.")
        awards = allocations(state["config"]["points"], state["ranking"])
        for user, bonus in awards.items():
            state["scores"][user] = state["scores"].get(user, 0) + bonus
        state["starting"] = awards
        state["starting_confirmed"] = True
        if state["stage"] != "running":
            state["stage"] = "ready"
        return
    state["starting"] = allocations(state["config"]["points"], state["ranking"])
    state["scores"] = dict(state["starting"])
    state["stage"] = "ready"


def tally_person(state: dict[str, Any], user: int, worth: str) -> None:
    if not tally_open(state):
        raise EventError("Contributions must close first.")
    if automatic(state) and user not in participants(state):
        raise EventError("This person did not contribute.")
    if not re.fullmatch(r"\d{1,12}", worth.strip()):
        raise EventError("Use a whole total-worth value.")
    state.setdefault("tallies", {})[str(user)] = int(worth)
    state["ranking"] = sorted(
        (int(person) for person in state["tallies"]),
        key=lambda person: (-state["tallies"][str(person)], person),
    )


def pool_available(pool: dict[str, Any]) -> int:
    if pool["type"] == "cards":
        return len(pool["codes"])
    if pool["type"] == "frames" and "items" in pool:
        return len(pool["items"])
    return pool["available"]


def occupied_cards(state: dict[str, Any], *, excluding_pool: str | None = None) -> set[str]:
    occupied = {
        code
        for identifier, pool in state["pools"].items()
        if identifier != excluding_pool
        for code in pool["codes"]
    }
    occupied.update(item["code"] for item in state["handoffs"].values() if item.get("code"))
    drop = state["drop"]
    if drop and drop.get("prize") and drop["prize"].get("code") and not drop.get("refunded"):
        occupied.add(drop["prize"]["code"])
    return occupied


def paced(state: dict[str, Any]) -> bool:

    return state["config"].get("prize_pacing") is True


def release_deadline(state: dict[str, Any]) -> int:

    finish = state["finish_at"]
    if state["config"].get("prize_release_policy") != RELEASE_POLICY:
        return finish
    duration = max(0, finish - state["drops_started_at"])
    return finish - min(4 * DAY, duration // 2)


def first_pass_enabled(state: dict[str, Any]) -> bool:
    return paced(state) and state["config"].get("prize_release_policy") == RELEASE_POLICY


def sync_first_pass(state: dict[str, Any], pool: dict[str, Any], previous=None) -> None:

    if not first_pass_enabled(state):
        return
    codes, items = list(pool["codes"]), list(pool.get("items", []))
    held = (state.get("drop") or {}).get("prize") or {}
    if pool["reserved"] and held.get("code") and held["code"] not in codes:
        codes.insert(0, held["code"])
    if pool["reserved"] and held.get("item"):
        items.insert(0, held["item"])
    remaining = pool_available(pool) + pool["reserved"]
    if previous and previous.get("first_pass"):
        old = previous["first_pass"]

        def unseen(current, prior, pending):
            new = Counter(current) - Counter(prior)
            left = Counter(pending)
            result = []
            for name in current:
                if left[name]:
                    result.append(name)
                    left[name] -= 1
                elif new[name]:
                    result.append(name)
                    new[name] -= 1
            return result
        codes = unseen(codes, previous["codes"], old["codes"])
        items = unseen(items, previous.get("items", []), old["items"])
        remaining = min(remaining, max(0, old["remaining"] + pool_available(pool) - pool_available(previous)))
    if pool["type"] == "cards":
        remaining = len(codes)
    elif pool["type"] == "frames" and "items" in pool:
        remaining = len(items)
    pool["first_pass"] = {"remaining": remaining, "codes": codes, "items": items}


def first_pass_stock(pool: dict[str, Any]) -> int:
    return pool.get("first_pass", {}).get("remaining", 0)


def drop_quantity(pool: dict[str, Any], allowance: int | None = None) -> int:

    quantity = pool["cap"]
    if pool_available(pool) < quantity or allowance is not None and allowance < quantity:
        return 0
    return quantity


def record_first_release(pool: dict[str, Any], prize: dict[str, Any]) -> bool:

    if not prize.get("first_pass") or "first_pass" not in pool:
        return False
    pending = pool["first_pass"]

    quantity = prize.get("first_pass_quantity", prize["quantity"])
    if type(quantity) is not int or not 0 < quantity <= prize["quantity"] or pending["remaining"] < quantity:
        raise EventError("Invalid first-release stock.")
    pending["remaining"] -= quantity
    if prize.get("code"):
        pending["codes"].remove(prize["code"])
    if prize.get("item"):
        pending["items"].remove(prize["item"])
    return True


def take_named_stock(pool: dict[str, Any], first: bool, *, ordered=True, rng=RNG):

    code, item = None, None
    if pool["type"] == "cards":
        index = pool["codes"].index(pool["first_pass"]["codes"][0]) if first else (
            0 if ordered else rng.randrange(len(pool["codes"])))
        code = pool["codes"].pop(index)
    elif pool["type"] == "frames" and "items" in pool:
        index = pool["items"].index(pool["first_pass"]["items"][0]) if first else 0
        item = pool["items"].pop(index)
    return code, item


def pacing_deadline(state: dict[str, Any], now: int) -> int:
    target = release_deadline(state)

    return target if now < target else state["finish_at"]


def rebalance_pool(state: dict[str, Any], pool: dict[str, Any], now: int) -> None:

    if first_pass_enabled(state) and "first_pass" not in pool:
        sync_first_pass(state, pool)
    previous = pool.get("pacing", {})


    stock = pool_available(pool) + pool["reserved"]
    batches = stock // pool["cap"]
    deadline = pacing_deadline(state, now)
    remaining = max(0, deadline - now)
    origin = state.get("drops_started_at") or now


    due = now + max(0, remaining // max(1, batches) - EXPECTED_PRIZE_WAIT)


    claim_window = 0 if state["config"].get("prize_release_policy") == RELEASE_POLICY else min(
        DAY, max(HOUR, (state["finish_at"] - origin) // 30))
    due = origin + (due - origin) // HOUR * HOUR
    due = max(now, min(due, max(now, deadline - claim_window)))
    pool["pacing"] = {
        "anchor": now,
        "stock": stock,
        "next_at": due,
        "regular": previous.get("regular", False) or stock > 1,
        "rare": stock == 1 and not previous.get("regular", False),
        "hour": previous.get("hour", -1),
        "used": previous.get("used", 0),
        "limit": previous.get("limit", 0),
    }


def pool_pacing(state: dict[str, Any], pool: dict[str, Any], now: int) -> dict[str, int | bool]:

    plan = pool.get("pacing")
    if not plan:
        return {"ready": False, "next_at": 0, "allowance": 0, "weight": 0}
    deadline = pacing_deadline(state, now)
    remaining = max(0, deadline - now)
    stock = pool_available(pool)
    hour = (now - state["drops_started_at"]) // HOUR
    unseen = first_pass_stock(pool) if first_pass_enabled(state) else 0
    batches = (stock + pool["reserved"]) // pool["cap"]
    hours = max(1, (remaining + HOUR - 1) // HOUR)
    limit = ((batches + hours - 1) // hours) * pool["cap"]
    used = 0
    if plan["hour"] == hour:

        limit, used = plan["limit"], plan["used"]
    allowance = max(0, min(stock, limit - used))
    ready = bool(remaining and drop_quantity(pool, allowance) and (plan["rare"] or now >= plan["next_at"]))
    weight = max(1, (batches * 1_000_000 + hours - 1) // hours) if ready else 0
    if ready and not plan["rare"]:
        interval = max(HOUR, (deadline - plan["anchor"]) // max(1, batches))
        weight *= min(32, 1 + max(0, now - plan["next_at"]) // interval)
    return dict(
        ready=ready,
        next_at=max(plan["next_at"], state["drops_started_at"] + (hour + 1) * HOUR)
        if stock >= pool["cap"] and allowance < pool["cap"]
        else plan["next_at"],
        allowance=allowance,
        weight=weight,
        hour=hour,
        limit=limit,
        rare=plan["rare"],
        first_pass=bool(unseen),
    )


def put_pool(
    state: dict[str, Any], key: str, values: dict[str, str], *, now: int | None = None
) -> None:
    if state["stage"] == "finished":
        raise EventError("This event has finished.")
    if paced(state) and state["stage"] == "running" and now is None:
        raise EventError("A current timestamp is required.")
    kind = values.get("type", "")
    if kind not in {"tickets", "gems", "cards", "frames", "other"}:
        raise EventError("Choose a prize type.")
    try:
        available, cap = (int(values[field]) for field in ("stock", "cap"))
        weight = int(values.get("weight") or "1")
    except (ValueError, KeyError) as exc:
        raise EventError("Use whole numbers for stock, number per drop and weight.") from exc
    if not 0 <= available <= 1_000_000 or not 1 <= cap <= 1_000_000 or not 1 <= weight <= 1000:
        raise EventError("Invalid stock, number per drop or weight.")
    name = " ".join(values.get("name", "").split())
    if kind == "other" and (not name or len(name) > 100):
        raise EventError("Enter an item name, up to 100 characters.")
    previous = state["pools"].get(key)
    if previous and previous["type"] != kind:
        raise EventError("Create another pool to change its type.")
    if previous and previous["reserved"] and paced(state):
        raise EventError("This pool has a reserved prize. Use Add Stock.")
    card_codes = codes(name) if kind == "cards" and name else []
    items = None
    if kind == "frames":
        items = frame_names(values.get("name", "")) if name else []
        if len(items) == 1:
            if available > MAX_CODES:
                raise EventError("Too many frames.")
            items *= available
        elif len(items) != available:
            raise EventError("Frame stock must match the number of frame names.")
        if previous:

            requested = list(items)
            surviving = []
            for item in previous.get("items", [previous["name"]] * pool_available(previous)):
                if item in requested:
                    surviving.append(item)
                    requested.remove(item)
            items = surviving + requested
        else:
            RNG.shuffle(items)
    if kind == "cards":
        occupied = occupied_cards(state, excluding_pool=key)
        if state.get("main_prize"):
            occupied.add(state["main_prize"]["code"])
        if occupied.intersection(card_codes):
            raise EventError("A card is already pooled, reserved or awarded.")
        if ordered_cards(state):
            if previous:
                card_codes = [code for code in previous["codes"] if code in card_codes] + [
                    code for code in card_codes if code not in previous["codes"]]
            else:
                RNG.shuffle(card_codes)
    state["pools"][key] = {
        "type": kind,
        "name": {"tickets": "Tickets", "gems": "Gems", "cards": "Cards", "frames": "Frames"}.get(kind, name),
        "available": available,
        "codes": card_codes,
        "cap": 1 if kind in {"cards", "frames"} else cap,
        "weight": weight,
        "reserved": previous["reserved"] if previous else 0,
        "awarded": previous["awarded"] if previous else 0,
    }
    pool = state["pools"][key]
    if items is not None:
        pool["items"] = items
        pool["available"] = len(items)
    total = pool_available(pool) + pool["reserved"] + pool["awarded"]
    baseline = (pool_available(previous) + previous["reserved"] + previous["awarded"]) if previous else total
    pool["initial"] = previous.get("initial", baseline) if previous else total
    pool["added"] = (previous.get("added", 0) if previous else 0) + max(0, total - baseline)
    pool["removed"] = (previous.get("removed", 0) if previous else 0) + max(0, baseline - total)
    if previous and previous.get("pacing"):
        pool["pacing"] = previous["pacing"]
    sync_first_pass(state, pool, previous)
    if paced(state) and state["stage"] == "running":
        assert now is not None
        rebalance_pool(state, pool, now)


def add_stock(state: dict[str, Any], key: str, values: dict[str, str], now: int) -> None:
    pool = state["pools"].get(key)
    if pool is None or state["stage"] == "finished":
        raise EventError("Unavailable pool.")
    before = pool_available(pool)
    previous = copy.deepcopy(pool) if first_pass_enabled(state) else None
    pool.setdefault("initial", before + pool["reserved"] + pool["awarded"])
    if pool["type"] == "cards":
        if values.get("amount", "").strip():
            raise EventError("Enter card codes, not an amount.")
        entries = codes(values.get("codes", ""))
        occupied = occupied_cards(state, excluding_pool=key)
        if state.get("main_prize"):
            occupied.add(state["main_prize"]["code"])
        if occupied.intersection(entries):
            raise EventError("A card is already pooled, reserved or awarded.")
        merged = list(dict.fromkeys([*pool["codes"], *entries]))
        if len(merged) + pool["reserved"] > MAX_CODES:
            raise EventError("Too many cards.")
        if merged == pool["codes"]:
            return
        pool["codes"] = merged
    elif pool["type"] == "frames" and "items" in pool:
        if values.get("amount", "").strip():
            raise EventError("Enter frame names, not an amount.")
        entries = frame_names(values.get("codes", ""))
        if before + pool["reserved"] + len(entries) > MAX_CODES:
            raise EventError("Too many frames.")
        pool["items"].extend(entries)
        pool["available"] = len(pool["items"])
    else:
        amount = values.get("amount", "").strip()
        if values.get("codes", "").strip() or not re.fullmatch(r"[0-9]{1,7}", amount):
            raise EventError("Enter a whole extra amount.")
        extra = int(amount)
        if not 1 <= extra or pool["available"] + pool["reserved"] + extra > 1_000_000:
            raise EventError("Invalid stock amount.")
        pool["available"] += extra
    pool["added"] = pool.get("added", 0) + pool_available(pool) - before
    sync_first_pass(state, pool, previous)
    if paced(state) and state["stage"] == "running":
        rebalance_pool(state, pool, now)


def merge_frame_pools(state: dict[str, Any], key: str, now: int) -> None:

    sources = [(source, pool) for source, pool in state["pools"].items() if pool["type"] == "frames"]
    if state["stage"] == "finished" or not sources or any(pool["reserved"] for _, pool in sources):
        raise EventError("Frame pools cannot be merged now.")
    if key in state["pools"] and state["pools"][key]["type"] != "frames":
        raise EventError("Choose a frame pool.")
    if len(sources) == 1 and sources[0][0] == key and "items" in sources[0][1]:
        return
    if sum(pool_available(pool) for _, pool in sources) > MAX_CODES:
        raise EventError("Too many frames.")
    items = [item for _, pool in sources for item in pool.get("items", [pool["name"]] * pool_available(pool))]
    target = state["pools"].get(key)
    if target and "items" in target:
        incoming = [item for source, pool in sources if source != key
                    for item in pool.get("items", [pool["name"]] * pool_available(pool))]
        RNG.shuffle(incoming)
        items = list(target["items"]) + incoming
    else:
        RNG.shuffle(items)
    combined = {"type": "frames", "name": "Frames", "items": items, "codes": [],
                "available": len(items), "reserved": 0, "cap": 1,
                "weight": sum(pool["weight"] for _, pool in sources),
                "awarded": sum(pool["awarded"] for _, pool in sources),
                "initial": sum(pool.get("initial", pool_available(pool) + pool["awarded"]) for _, pool in sources),
                "added": sum(pool.get("added", 0) for _, pool in sources),
                "removed": sum(pool.get("removed", 0) for _, pool in sources)}
    combined["weight"] = min(1000, combined["weight"])
    if first_pass_enabled(state):
        pending_items = [item for _, pool in sources for item in (
            pool.get("first_pass", {}).get("items", pool["items"]) if "items" in pool else
            [pool["name"]] * pool.get("first_pass", {}).get("remaining", pool_available(pool)))]
        counts = Counter(pending_items)
        ordered_pending = []
        for item in items:
            if counts[item]:
                ordered_pending.append(item)
                counts[item] -= 1
        combined["first_pass"] = {"remaining": len(ordered_pending), "codes": [], "items": ordered_pending}
    if paced(state) and state["stage"] == "running":
        hour = (now - state["drops_started_at"]) // HOUR
        current = [pool["pacing"] for _, pool in sources if pool.get("pacing", {}).get("hour") == hour]
        combined["pacing"] = dict(hour=hour if current else -1,
                                 used=sum(plan["used"] for plan in current),
                                 limit=sum(plan["limit"] for plan in current),
                                 regular=any(pool.get("pacing", {}).get("regular") for _, pool in sources))
        rebalance_pool(state, combined, now)
    for source, _ in sources:
        del state["pools"][source]
    state["pools"][key] = combined


def reserve(
    state: dict[str, Any], rng: Any = RNG, *, now: int | None = None
) -> dict[str, Any] | None:
    pacing = paced(state)
    if pacing:
        if now is None:
            raise EventError("A current timestamp is required.")
        if (
            state["stage"] != "running"
            or state["paused"]
            or not state["drops_started_at"] <= now < state["finish_at"]
        ):
            return None
    stocked = []
    for key, pool in state["pools"].items():
        if not drop_quantity(pool):
            continue
        if pacing:
            assert now is not None
            if not pool.get("pacing"):
                rebalance_pool(state, pool, now)
            plan = pool_pacing(state, pool, now)
            if not plan["ready"]:
                continue
            stocked.append((key, pool, plan))
        else:
            stocked.append((key, pool, {"weight": pool["weight"]}))
    if not stocked or rng.randrange(100) >= PRIZE_CHANCE_PERCENT:
        return None


    if pacing:
        rare = [item for item in stocked if item[2]["rare"]]
        regular = [item for item in stocked if not item[2]["rare"]]
        rare_weight = sum(plan["weight"] for _, _, plan in rare)
        if rare and rng.randrange(600_000 + rare_weight) < rare_weight:
            stocked = rare
        else:
            stocked = regular
        if not stocked:
            return None
    choice = rng.randrange(sum(plan["weight"] for _, _, plan in stocked))
    for key, pool, plan in stocked:
        choice -= plan["weight"]
        if choice < 0:
            count = drop_quantity(pool, plan["allowance"] if pacing else None)
            unseen = bool(plan.get("first_pass"))
            first_quantity = min(count, first_pass_stock(pool)) if unseen else 0
            code, item = take_named_stock(pool, unseen, ordered=ordered_cards(state), rng=rng)
            if code is None:
                pool["available"] -= count
            pool["reserved"] += count
            prize = {"pool": key, "name": pool["name"], "quantity": count, "code": code}
            if item is not None:
                prize.update(item=item, name=item)
            if pacing:
                if unseen:
                    prize["first_pass"] = True
                    if first_quantity < count:
                        prize["first_pass_quantity"] = first_quantity
                pool["pacing"].update(
                    hour=plan["hour"],
                    used=(pool["pacing"]["used"] if pool["pacing"]["hour"] == plan["hour"] else 0)
                    + count,
                    limit=plan["limit"],
                )
                prize["paced_hour"] = plan["hour"]
            return prize
    return None


def point_scaling_values(state: dict[str, Any], now: int) -> tuple[int, int, int, int]:

    profile = state["config"].get("point_scaling")
    if profile not in (LEGACY_POINT_SCALING, POINT_SCALING) or state["stage"] != "running":
        return 1, 1, 1, 1
    start, finish = state.get("drops_started_at", 0), state.get("finish_at", 0)
    if any(type(value) is not int for value in (start, finish, now)):
        return 1, 1, 1, 1
    duration = finish - start
    if duration <= 0:
        return 1, 1, 1, 1
    ramp = duration
    if profile == POINT_SCALING:
        ramp -= min(POINT_PEAK_LEAD_MS, duration // 2)
    elapsed = min(ramp, max(0, now - start))
    usual = 1 + (38 * elapsed + ramp) // (2 * ramp)
    variation = 1 + (198 * elapsed + ramp) // (2 * ramp)
    low = max(1, (usual * (100 - variation) + 99) // 100)
    high = usual * (100 + variation) // 100
    return usual, variation, low, high


def point_bounds(state: dict[str, Any], now: int) -> tuple[int, int]:

    _usual, _variation, low, high = point_scaling_values(state, now)
    return low, high


def point_amount(drop: dict[str, Any]) -> int:

    amount = drop.get("point_amount", 1)
    if type(amount) is not int or not 1 <= amount <= 40:
        raise EventError("Invalid drop amount.")
    return amount


def spider_amount(drop: dict[str, Any]) -> int:

    amount = point_amount(drop)
    profile = drop.get("spider_scoring")
    if profile is None or profile == SPIDER_SCORING:
        return amount
    if profile == LEGACY_SPIDER_SCORING:
        return max(1, amount // 2)
    if profile == THIRD_SPIDER_SCORING:
        return max(1, amount // 3)
    raise EventError("Invalid spider scoring.")


def begin_drop(state: dict[str, Any], now: int, rng: Any = RNG) -> None:
    if state["stage"] != "running" or state["paused"] or state["drop"]:
        raise EventError("A drop cannot start now.")
    outcomes = ["treat", "treat", "spider", "prize"]
    rng.shuffle(outcomes)
    state["drop"] = {
        "id": secrets.token_hex(8),
        "status": "publishing",
        "message": 0,
        "channel": state["config"]["channel"],
        "outcomes": outcomes,
        "blank": "%" * rng.randint(4, 7),
        "prize": reserve(state, rng, now=now),
        "reveal_at": 0,
        "wait": rng.randint(MIN_REVEAL_MS // 1000, 10) * 1000,
        "open_at": 0,
        "close_at": 0,
        "settle_at": 0,
        "claims": {},
        "refunded": False,
        "results": [],
        "created": now,
    }
    low, high = point_bounds(state, now)

    state["drop"]["point_amount"] = low if low == high else rng.randint(low, high)
    state["drop"]["spider_scoring"] = SPIDER_SCORING


def refund(state: dict[str, Any], drop: dict[str, Any]) -> None:
    prize = drop["prize"]
    if not prize or drop["refunded"]:
        return
    pool = state["pools"][prize["pool"]]
    pool["reserved"] -= prize["quantity"]
    if prize["code"]:
        if ordered_cards(state) and not drop.get("prize_offered"):
            pool["codes"].insert(0, prize["code"])
        else:
            pool["codes"].append(prize["code"])
    else:
        pool["available"] += prize["quantity"]
        if prize.get("item") is not None:
            if drop.get("prize_offered"):
                pool["items"].append(prize["item"])
            else:
                pool["items"].insert(0, prize["item"])
    if (not drop.get("prize_offered") and "paced_hour" in prize
            and pool.get("pacing", {}).get("hour") == prize["paced_hour"]):
        pool["pacing"]["used"] = max(0, pool["pacing"]["used"] - prize["quantity"])
    drop["refunded"] = True


def evaluate_claims(
    state: dict[str, Any], receipts: list[dict[str, Any]], excluded: set[int]
) -> tuple[dict[str, int], dict[str, list[Any]]]:

    drop = state["drop"]
    claims: dict[str, int] = {}
    chosen: dict[int, str] = {}
    verdicts: dict[str, list[Any]] = {}
    for receipt in sorted(receipts, key=lambda item: int(item["interaction_id"]))[:5000]:
        identifier = str(receipt["interaction_id"])
        if identifier in verdicts:
            continue
        actor, slot, timestamp = (
            int(receipt["actor_id"]),
            str(receipt["slot"]),
            int(receipt["timestamp"]),
        )
        if (
            actor not in participants(state)
            or actor in excluded
            or slot not in {"0", "1", "2", "3"}
            or not drop["open_at"] <= timestamp < drop["close_at"]
        ):
            verdict = "unavailable"
        elif actor in chosen:
            verdict = "claimed" if chosen[actor] == slot else "already_claimed"
        elif slot in claims:
            verdict = "taken"
        else:
            claims[slot] = actor
            chosen[actor] = slot
            verdict = "claimed"
        verdicts[identifier] = [actor, slot, verdict]
    return claims, verdicts


def ordered_claims(
    state: dict[str, Any], receipts: list[dict[str, Any]], excluded: set[int]
) -> dict[str, int]:
    return evaluate_claims(state, receipts, excluded)[0]


def save_claim_feedback(state: dict[str, Any], verdicts: dict[str, list[Any]]) -> None:
    drop = state["drop"]


    drop["feedback"] = dict(
        schema=1,
        run=state["run"],
        drop=drop["id"],
        message=drop["message"],
        channel=drop["channel"],
        generation=state.get("generation", 0),
        revision=state["revision"] + 1,
        final=drop["status"] == "settled",
        verdicts=verdicts,
    )


def update_claims(
    state: dict[str, Any],
    receipts: list[dict[str, Any]],
    now: int,
    excluded: set[int] | None = None,
) -> None:
    drop = state["drop"]
    if not drop or drop["status"] != "open":
        return
    drop["claims"], verdicts = evaluate_claims(state, receipts, excluded or set())
    save_claim_feedback(state, verdicts)
    slots = [str(i) for i in range(4)]
    if all(slot in drop["claims"] for slot in slots):

        closing_timestamp = max(
            min(
                int(item["timestamp"])
                for item in receipts
                if str(item["slot"]) == slot and int(item["actor_id"]) == drop["claims"][slot]
                and drop["open_at"] <= int(item["timestamp"]) < drop["close_at"]
            )
            for slot in slots
        )
        drop["close_at"] = min(drop["close_at"], max(now, closing_timestamp + 1))
        drop["settle_at"] = drop["close_at"] + SETTLEMENT_DELAY_MS


def settle(
    state: dict[str, Any],
    receipts: list[dict[str, Any]],
    now: int,
    excluded: set[int] | None = None,
) -> None:
    drop = state["drop"]
    if not drop or drop["status"] != "open" or now < drop["settle_at"]:
        return
    amount = point_amount(drop)
    loss = spider_amount(drop)
    if drop["prize"] and drop["prize"].get("first_pass") and not drop.get("first_release_recorded"):
        drop["first_release_recorded"] = record_first_release(
            state["pools"][drop["prize"]["pool"]], drop["prize"])
    if drop["prize"] and first_pass_enabled(state):
        drop["prize_offered"] = True
    claims, verdicts = evaluate_claims(state, receipts, excluded or set())
    prize_claimed = False
    for slot, actor in claims.items():
        outcome = drop["outcomes"][int(slot)]
        key = str(actor)
        if outcome in {"spider", "treat"}:
            previous = state["scores"].get(key, 0)
            state["scores"][key] = max(0, previous + (amount if outcome == "treat" else -loss))
            actual = abs(state["scores"][key] - previous)
            drop["results"].append(
                {
                    "actor": actor,
                    "text": f"{'+' if outcome == 'treat' else '-'}{actual} "
                    + ("point" if actual == 1 else "points"),
                }
            )
        elif drop["prize"]:
            prize = drop["prize"]
            pool = state["pools"][prize["pool"]]
            pool["reserved"] -= prize["quantity"]
            pool["awarded"] += prize["quantity"]
            if paced(state):
                rebalance_pool(state, pool, now)
            state["handoffs"][drop["id"]] = {**prize, "actor": actor, "delivered": False}
            prize_claimed = True
            drop["results"].append({"actor": actor, "text": prize_label(prize)})
            state.setdefault("recent_claims", []).insert(
                0, {"actor": actor, "text": prize_label(prize), "at": now, "drop": drop["id"]}
            )
        else:
            drop["results"].append({"actor": actor, "text": "Blank"})
    if not prize_claimed:
        refund(state, drop)
        if drop.get("prize_offered"):
            rebalance_pool(state, state["pools"][drop["prize"]["pool"]], now)
    drop["claims"], drop["status"] = claims, "settled"
    save_claim_feedback(state, verdicts)
    state["last_settled"] = now
    state["recent_claims"] = state.get("recent_claims", [])[:5]


def prize_label(prize: dict[str, Any]) -> str:
    return f"Card {prize['code']}" if prize["code"] else f"{prize['quantity']} {prize['name']}"
