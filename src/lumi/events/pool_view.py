from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import contextlib
import json
import re
import secrets
from collections import Counter

from redis.exceptions import LockError

from lumi.contracts import DiscordIntent, PresentationControl, PresentationPage
from lumi.events.prizepool import snapshot, timetable

from . import engine

PREFIX = "lumi:halloween:pool:"
TTL_MS = 900_000


def literal(text: str) -> str:
    return re.sub(r"([\\`*_~|<>\[\]()])", r"\\\1", text).replace("@", "@\u200b")


def label(pool: dict, quantity: int, *, code=None, item=None) -> str:
    if code:
        return f"`{code}`"
    name = literal(item or pool["name"])
    if pool["type"] in {"gems", "tickets"}:
        name = pool["type"] if quantity != 1 else pool["type"][:-1]
    if pool["type"] not in {"cards", "frames"} or quantity != 1:
        return f"{quantity:,} {name}"
    return name


def entries(pool: dict, quantity: int | None = None):
    if pool["type"] == "cards":
        return [label(pool, 1, code=code) for code in pool["codes"]]
    if "items" in pool:
        return [label(pool, count, item=item) for item, count in Counter(pool["items"]).items()]
    return [label(pool, pool["remaining"] if quantity is None else quantity)]


def pages(lines: list[str], empty: str) -> tuple[str, ...]:
    result, current = [], []
    size = 0
    for line in lines:
        if current and (len(current) == 20 or size + len(line) + 1 > 3500):
            result.append("\n".join(current))
            current, size = [], 0
        current.append(line)
        size += len(line) + 1
    return tuple([*result, "\n".join(current)] if current else result) or (empty,)


def pool_pages(guild: int, state: dict, now: int) -> dict[str, tuple[str, ...]]:
    public = snapshot(guild, state, now)
    ready, scheduled = [], []
    if state["stage"] == "finished" or (public["finish_at"] and now >= public["finish_at"]):
        return {"ready": ("Event finished.",), "schedule": ("Event finished.",)}
    clock = {
        "config": state["config"],
        "drops_started_at": public["start_at"],
        "finish_at": public["finish_at"],
    }
    for pool in public["pools"]:
        if not pool["remaining"]:
            continue
        if (
            public["paused"]
            or state["stage"] != "running"
            or (public["start_at"] and now < public["start_at"])
            or pool["remaining"] < pool["cap"]
        ):
            when = (
                "Paused"
                if public["paused"]
                else "Needs stock"
                if pool["remaining"] < pool["cap"]
                else f"<t:{public['start_at'] // 1000}:f>"
                if public["start_at"]
                else "On start"
            )
            scheduled.extend((0, f"{when} · {text}") for text in entries(pool))
            continue
        simulated = {
            "type": pool["type"],
            "available": pool["remaining"],
            "reserved": 0,
            "codes": pool["codes"],
            "cap": pool["cap"],
            "pacing": pool["pacing"] or {},
            **({"items": pool["items"]} if "items" in pool else {}),
            **({"first_pass": pool["first_pass"]} if "first_pass" in pool else {}),
        }
        if public["paced"] and not simulated["pacing"]:


            engine.rebalance_pool(clock, simulated, now)
            pool["pacing"] = simulated["pacing"]
            if "first_pass" in simulated:
                pool["first_pass"] = simulated["first_pass"]
        status, windows = timetable(public, pool, now)
        if status == "throughout":
            if pool["type"] in {"cards", "frames"}:
                ready.extend(entries(pool))
            else:
                ready.append(label(pool, pool["cap"]))
                batches, remainder = divmod(pool["remaining"], pool["cap"])
                if batches > 1:
                    scheduled.append(
                        (0, f"No fixed times · {batches - 1:,} drops of {label(pool, pool['cap'])}")
                    )
                if remainder:
                    scheduled.append((0, f"Needs stock · {label(pool, remainder)}"))
            continue
        eligible = bool(windows and engine.pool_pacing(clock, simulated, now)["ready"])
        if pool["type"] == "cards" and not public["ordered_cards"]:
            pending = pool.get("first_pass", {}).get("codes", [])
            if pending and engine.first_pass_enabled(clock):
                if eligible:
                    ready.append(label(pool, 1, code=pending[0]))
                scheduled.extend(
                    (0, f"Unscheduled · {label(pool, 1, code=code)}")
                    for code in pool["codes"]
                    if not eligible or code != pending[0]
                )
            elif eligible:
                ready.extend(entries(pool))
            else:
                when = f"<t:{windows[0]['at'] // 1000}:f>" if windows else "Unscheduled"
                scheduled.extend(
                    (windows[0]["at"] if windows else 0, f"{when} · {text}")
                    for text in entries(pool)
                )
            continue
        used = Counter()
        for index, window in enumerate(windows):
            code = (window.get("codes") or [None])[0]
            item = (window.get("items") or [None])[0]
            text = label(pool, window["quantity"], code=code, item=item)
            used[code or item or ""] += window["quantity"]
            if eligible and index == 0:
                ready.append(text)
            else:
                scheduled.append((window["at"], f"<t:{window['at'] // 1000}:f> · {text}"))


        if pool["type"] == "cards" or "items" in pool:
            source = pool["codes"] if pool["type"] == "cards" else pool["items"]
            for name, count in (Counter(source) - used).items():
                text = label(
                    pool, count, **({"code": name} if pool["type"] == "cards" else {"item": name})
                )
                scheduled.append((0, f"Unscheduled · {text}"))
        elif remainder := pool["remaining"] - sum(used.values()):
            status = "Needs stock" if remainder < pool["cap"] else "Unscheduled stock"
            scheduled.append((0, f"{status} · {label(pool, remainder)}"))
    scheduled.sort(key=lambda row: (row[0] == 0, row[0]))
    return {
        "ready": pages(ready, "Nothing ready."),
        "schedule": pages([text for _, text in scheduled], "Nothing scheduled."),
    }


class EventPoolViewer:
    def __init__(self, *, generation, cache, store, clock):
        self.generation, self.cache, self.store, self.clock = generation, cache, store, clock
        self._work = asyncio.Semaphore(1)

    async def handle(self, request) -> DiscordIntent:
        event = request.event
        guild, actor, channel = (
            int(event.get(key) or 0) for key in ("guild_id", "actor_id", "channel_id")
        )
        if not guild or not actor or not channel:
            raise engine.EventError("Unavailable.")
        command = event.get("dispatch") == "command"
        if command and request.arguments.get("help"):
            return DiscordIntent(
                kind="send",
                presentation_mode="legacy",
                pages=(PresentationPage(description=f"{event.get('prefix', '.')}event pool"),),
            )
        token = secrets.token_hex(8) if command else str(request.arguments.get("session") or "")
        lock = self.cache.lock(PREFIX + "lock:" + token, timeout=120, blocking_timeout=2)
        if not await lock.acquire():
            raise engine.EventError("Busy. Try again.")
        try:
            now = self.clock()
            if command:
                session = dict(
                    owner=actor,
                    guild=guild,
                    channel=channel,
                    expires=now + TTL_MS,
                    generation=self.generation,
                    view="ready",
                    page=0,
                    revision=0,
                )
            else:
                raw = await self.cache.get(PREFIX + token)
                session = json.loads(raw) if raw else None
                bound = await self.cache.get(ui_key("message", token))
                if (
                    not session
                    or session["owner"] != actor
                    or session["guild"] != guild
                    or session["channel"] != channel
                    or session["expires"] <= now
                    or session["generation"] != self.generation
                    or bound is None
                    or int(bound) != event.get("message_id")
                ):
                    raise engine.EventError("This menu has expired.")
                if session["revision"] != request.arguments.get("revision"):
                    raise engine.EventError("Updated. Try again.")
                action = request.route.rpartition(".")[2]
                if action in {"pool_ready", "pool_schedule"}:
                    session["view"], session["page"] = action.removeprefix("pool_"), 0
                elif action in {"pool_previous", "pool_next"}:
                    session["page"] = max(
                        0, session["page"] + (-1 if action == "pool_previous" else 1)
                    )
                else:
                    raise engine.EventError("Unavailable.")
                session["revision"] += 1
            state = await self.store.read(guild)
            if not state or not state.get("organizer_id"):
                raise engine.EventError("No event.")
            if state.get("generation", 0) > self.generation or (
                not command and session.get("run") != state["run"]
            ):
                raise engine.EventError("This event has changed. Open a new menu.")
            session["run"] = state["run"]
            async with self._work:
                result = await asyncio.to_thread(pool_pages, guild, state, self.clock())
            if session["expires"] <= self.clock() or not await lock.owned():
                raise engine.EventError("This menu has expired.")
            selected = result[session["view"]]
            session["page"] = min(session["page"], len(selected) - 1)

            def button(action, text, disabled=False):
                return PresentationControl(
                    "button",
                    "halloween",
                    action,
                    f"{token}~{session['revision']}",
                    label=text,
                    disabled=disabled,
                )

            controls = [
                button("pool_ready", "Ready", session["view"] == "ready"),
                button("pool_schedule", "Schedule", session["view"] == "schedule"),
            ]
            if len(selected) > 1:
                controls += [
                    button("pool_previous", "Previous", session["page"] == 0),
                    button("pool_next", "Next", session["page"] == len(selected) - 1),
                ]
            footer = "Earliest releases; times may shift." if session["view"] == "schedule" else ""
            if len(selected) > 1:
                footer = f"{session['page'] + 1}/{len(selected)}" + (
                    f" · {footer}" if footer else ""
                )
            await self.cache.set(
                PREFIX + token, json.dumps(session), px=session["expires"] - self.clock()
            )
            return DiscordIntent(
                kind="send" if command else "edit",
                session_id=token,
                presentation_mode="legacy",
                controls=tuple(controls),
                pages=(
                    PresentationPage(
                        title="Ready prizes" if session["view"] == "ready" else "Prize schedule",
                        description=selected[session["page"]],
                        footer=footer or None,
                    ),
                ),
            )
        finally:
            with contextlib.suppress(LockError):
                await lock.release()
