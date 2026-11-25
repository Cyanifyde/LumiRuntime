from __future__ import annotations

import copy
import json
import os
import re
import stat
from collections import Counter
from pathlib import Path

from lumi.events import engine

MAX_BYTES = 1024 * 1024
MAX_AGE_MS = 90_000
ID = re.compile(r"[1-9][0-9]{0,19}\Z")
CODE = re.compile(r"[a-z0-9]{1,32}\Z")
STAGES = {"setup", "contributions", "tally", "ready", "running", "finished"}


class ProjectionUnavailable(RuntimeError):
    pass


def snapshot(guild: int, state: dict, now: int) -> dict:
    pools = []
    for key, pool in state["pools"].items():
        remaining = engine.pool_available(pool) + pool["reserved"]
        codes = list(pool["codes"])
        items = list(pool.get("items", []))
        drop = state.get("drop") or {}
        held = drop.get("prize") or {}
        if (held.get("pool") == key and held.get("code") and pool["reserved"]
                and not drop.get("refunded")):
            codes.insert(0, held["code"])
        if (held.get("pool") == key and held.get("item") and pool["reserved"]
                and not drop.get("refunded")):
            items.insert(0, held["item"])
        plan = copy.deepcopy(pool.get("pacing"))
        if plan and held.get("pool") == key and pool["reserved"] and not drop.get("refunded"):

            if plan["hour"] == held.get("paced_hour"):
                plan["used"] = max(0, plan["used"] - held["quantity"])
        if plan and not plan["used"]:
            plan["hour"], plan["limit"] = -1, 0
        pools.append({
            "id": key, "type": pool["type"], "name": pool["name"],
            "initial": pool.get("initial", remaining + pool["awarded"]),
            "added": pool.get("added", 0), "removed": pool.get("removed", 0),
            "claimed": pool["awarded"], "remaining": remaining,
            "cap": pool["cap"], "weight": pool["weight"], "codes": codes if engine.ordered_cards(state) else sorted(codes),
            "pacing": plan,
            **({"items": items} if "items" in pool else {}),
            **({"first_pass": copy.deepcopy(pool["first_pass"])} if "first_pass" in pool else {}),
        })
    start = state.get("drops_started_at") or engine.planned_start(state)
    result = {
        "schema": 1, "guild_id": str(guild), "run": state["run"],
        "generation": state.get("generation", 0), "revision": state["revision"],
        "published_at": now, "stage": state["stage"], "paused": state["paused"],
        "start_at": start or None,
        "finish_at": (state.get("finish_at") or start + (
            state["config"].get("drop_ms") or state["config"].get("hours", 0) * engine.HOUR
        )) if start else None,
        "paced": engine.paced(state), "ordered_cards": engine.ordered_cards(state), "pools": pools,
        "release_policy": state["config"].get("prize_release_policy"),
        "drop_not_before": max(start, state.get("last_settled", 0)) + engine.drop_gap_ms(state) if start else None,
    }
    if "drop_gap_ms" in state["config"]:
        result["drop_gap_ms"] = engine.drop_gap_ms(state)
    validate(result)
    return result


def integer(value, low=0, high=10**15):
    if type(value) is not int or not low <= value <= high:
        raise ProjectionUnavailable("Unavailable.")


def validate(value: dict) -> None:
    fields = {"schema", "guild_id", "run", "generation", "revision", "published_at",
              "stage", "paused", "start_at", "finish_at", "paced", "ordered_cards", "pools"}
    if (not isinstance(value, dict) or not fields <= set(value)
            or not set(value) <= fields | {"drop_not_before", "release_policy", "drop_gap_ms"} or value["schema"] != 1):
        raise ProjectionUnavailable("Unavailable.")
    if (not isinstance(value["guild_id"], str) or not ID.fullmatch(value["guild_id"])
            or not isinstance(value["run"], str) or not re.fullmatch(r"[a-f0-9]{16}", value["run"])
            or value["stage"] not in STAGES or type(value["paused"]) is not bool
            or type(value["paced"]) is not bool or type(value["ordered_cards"]) is not bool):
        raise ProjectionUnavailable("Unavailable.")
    for name in ("generation", "revision", "published_at"):
        integer(value[name])
    if "drop_gap_ms" in value:
        integer(value["drop_gap_ms"], engine.DROP_GAP_MS, engine.DAY)
    if value.get("release_policy") not in (None, engine.RELEASE_POLICY):
        raise ProjectionUnavailable("Unavailable.")
    for name in ("start_at", "finish_at", "drop_not_before"):
        if value.get(name) is not None:
            integer(value[name], 1)
    if value["start_at"] and (not value["finish_at"] or not 0 < value["finish_at"] - value["start_at"] <= 365 * engine.DAY):
        raise ProjectionUnavailable("Unavailable.")
    if not isinstance(value["pools"], list) or len(value["pools"]) > 100:
        raise ProjectionUnavailable("Unavailable.")
    seen, all_codes = set(), set()
    for pool in value["pools"]:
        fields = {"id", "type", "name", "initial", "added", "removed", "claimed", "remaining", "cap", "weight", "codes", "pacing"}
        if (not isinstance(pool, dict) or not fields <= set(pool)
                or not set(pool) <= fields | {"items", "first_pass"}
                or not isinstance(pool["id"], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", pool["id"])
                or pool["id"] in seen or pool["type"] not in {"tickets", "gems", "cards", "frames", "other"}
                or not isinstance(pool["name"], str) or not 1 <= len(pool["name"]) <= 100
                or any(ord(char) < 32 for char in pool["name"])):
            raise ProjectionUnavailable("Unavailable.")
        seen.add(pool["id"])
        for name in ("initial", "added", "removed", "claimed", "remaining"):
            integer(pool[name], high=10**12)
        integer(pool["cap"], 1, 1_000_000)
        integer(pool["weight"], 1, 1000)
        if pool["initial"] + pool["added"] - pool["removed"] - pool["claimed"] != pool["remaining"]:
            raise ProjectionUnavailable("Unavailable.")
        if (not isinstance(pool["codes"], list) or len(pool["codes"]) > 5000
                or any(not isinstance(code, str) or not CODE.fullmatch(code) for code in pool["codes"])
                or len(set(pool["codes"])) != len(pool["codes"]) or all_codes.intersection(pool["codes"])
                or (pool["type"] == "cards" and len(pool["codes"]) != pool["remaining"])
                or (pool["type"] != "cards" and pool["codes"])):
            raise ProjectionUnavailable("Unavailable.")
        all_codes.update(pool["codes"])
        if "items" in pool and (
                pool["type"] != "frames" or pool["cap"] != 1
                or not isinstance(pool["items"], list) or len(pool["items"]) > engine.MAX_CODES
                or len(pool["items"]) != pool["remaining"]
                or any(not isinstance(item, str) or not 1 <= len(item) <= 100
                       or any(ord(char) < 32 for char in item) for item in pool["items"])):
            raise ProjectionUnavailable("Unavailable.")
        plan = pool["pacing"]
        if "first_pass" in pool:
            pending = pool["first_pass"]
            if not isinstance(pending, dict) or set(pending) != {"remaining", "codes", "items"}:
                raise ProjectionUnavailable("Unavailable.")
            integer(pending["remaining"], high=pool["remaining"])
            for key in ("codes", "items"):
                entries = pending[key]
                if (not isinstance(entries, list) or len(entries) > engine.MAX_CODES
                        or any(not isinstance(entry, str) for entry in entries)
                        or Counter(entries) - Counter(pool.get(key, []))):
                    raise ProjectionUnavailable("Unavailable.")
            if ((pool["type"] == "cards" and pending["remaining"] != len(pending["codes"]))
                    or (pool["type"] == "frames" and "items" in pool
                        and pending["remaining"] != len(pending["items"]))
                    or (pool["type"] != "cards" and pending["codes"])
                    or (pool["type"] != "frames" and pending["items"])):
                raise ProjectionUnavailable("Unavailable.")
        if plan is not None:
            if not isinstance(plan, dict) or set(plan) != {"anchor", "stock", "next_at", "regular", "rare", "hour", "used", "limit"}:
                raise ProjectionUnavailable("Unavailable.")
            for field in ("anchor", "stock", "next_at", "used", "limit"):
                integer(plan[field])
            integer(plan["hour"], -1)
            if type(plan["regular"]) is not bool or type(plan["rare"]) is not bool:
                raise ProjectionUnavailable("Unavailable.")


def timetable(value: dict, pool: dict, now: int, include_windows=True, *, limit=None) -> tuple[str, list[dict]]:

    start, finish = value["start_at"], value["finish_at"]
    if value["stage"] == "finished" or finish and now >= finish:
        return "finished", []
    if not pool["remaining"]:
        return "empty", []
    if pool["remaining"] < pool["cap"]:
        return "insufficient_stock", []
    if not start:
        return "not_scheduled", []
    if value["paused"]:
        return "paused", []
    if not value["paced"]:
        return "throughout", []
    state = {"config": {"prize_pacing": True, "prize_release_policy": value.get("release_policy"),
                        "drop_gap_ms": value.get("drop_gap_ms", engine.DROP_GAP_MS)}, "stage": "running", "paused": False,
             "drops_started_at": start, "finish_at": finish}
    simulated = {"type": pool["type"], "available": pool["remaining"], "reserved": 0,
                 "codes": list(pool["codes"]),
                 "cap": pool["cap"], "pacing": copy.deepcopy(pool["pacing"]) or {}}
    if "items" in pool:
        simulated["items"] = list(pool["items"])
    if "first_pass" in pool:
        simulated["first_pass"] = copy.deepcopy(pool["first_pass"])
    when = max(now, start, value.get("drop_not_before") or start)
    if not simulated["pacing"]:
        engine.rebalance_pool(state, simulated, start)
    if simulated["pacing"]["rare"]:
        return "throughout", []
    if not include_windows:
        return "projected", []
    rows = []
    for _ in range(engine.MAX_CODES + 24 * 365 + 1):
        if limit is not None and len(rows) >= limit:
            break
        plan = engine.pool_pacing(state, simulated, when)
        if not engine.drop_quantity(simulated) or when >= finish:
            break
        when = max(when, plan["next_at"])
        if when >= finish:
            break
        plan = engine.pool_pacing(state, simulated, when)
        if not plan["ready"]:
            when = start + (plan.get("hour", 0) + 1) * engine.HOUR
            continue
        amount = engine.drop_quantity(simulated, plan["allowance"])
        first = bool(plan.get("first_pass"))
        row = {"at": when, "quantity": amount}
        code, item = engine.take_named_stock(simulated, first)
        if code and value["ordered_cards"]:
            row["codes"] = [code]
        if item:
            row["items"] = [item]
        rows.append(row)
        simulated["available"] -= amount
        first_quantity = min(amount, engine.first_pass_stock(simulated)) if first else amount
        engine.record_first_release(simulated, {"first_pass": first, "quantity": amount,
                                               "first_pass_quantity": first_quantity,
                                               "code": code, "item": item})
        used = simulated["pacing"]["used"] if simulated["pacing"]["hour"] == plan["hour"] else 0
        simulated["pacing"].update(hour=plan["hour"], used=used + amount, limit=plan["limit"])
        if simulated["available"]:


            claimed_at = when + engine.MIN_REVEAL_MS + 1 + engine.SETTLEMENT_DELAY_MS
            engine.rebalance_pool(state, simulated, claimed_at)
            when = max(claimed_at + engine.drop_gap_ms(state), simulated["pacing"]["next_at"])
    return "projected", rows


def release_rate(value: dict, pool: dict, now: int) -> dict | None:
    if pool["type"] in {"cards", "frames"}:
        return None
    status, windows = timetable(value, pool, now, limit=25)
    if status != "projected" or len(windows) < 2:
        return None
    quantity = windows[0]["quantity"]
    intervals = [second["at"] - first["at"] for first, second in zip(windows, windows[1:])
                 if first["quantity"] == second["quantity"] == quantity]
    if not intervals:
        return None
    return {"quantity": quantity, "every_ms": min(intervals), "max_every_ms": max(intervals)}


class ProjectionReader:
    def __init__(self, root: Path):
        self.root = root

    def guild_ids(self) -> set[str]:
        try:
            return {p.stem for p in self.root.iterdir() if ID.fullmatch(p.stem) and p.suffix == ".json"}
        except OSError:
            raise ProjectionUnavailable("Unavailable.") from None

    def read(self, guild: str, now: int) -> dict:
        if not ID.fullmatch(guild):
            raise ProjectionUnavailable("Unavailable.")
        try:
            fd = os.open(self.root / (guild + ".json"), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(fd, "rb") as source:
                if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                    raise ProjectionUnavailable("Unavailable.")
                data = source.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise ProjectionUnavailable("Unavailable.")
            value = json.loads(data)
            validate(value)
            if value["guild_id"] != guild or not 0 <= now - value["published_at"] <= MAX_AGE_MS:
                raise ProjectionUnavailable("Unavailable.")
            return value
        except (OSError, ValueError, TypeError, KeyError):
            raise ProjectionUnavailable("Unavailable.") from None
