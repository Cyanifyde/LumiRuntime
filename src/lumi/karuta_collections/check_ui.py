from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import weakref

from lumi.contracts import DiscordIntent, PresentationControl, PresentationPage

from .checks import ImportError, duplicates, matches, pages, parse_csv
from .csv_source import CSVSourceError, download_csv

MODULE = "karuta_collections"
PREFIX = "lumi:karuta-check:"
TTL = 900


class CheckUI:
    def __init__(self, module):
        self.module = module
        self.locks = weakref.WeakValueDictionary()

    def simple(self, text, *, kind="send", mode="v2"):
        return DiscordIntent(
            kind, pages=(PresentationPage(description=text),), presentation_mode=mode
        )

    async def handle(self, request, job):
        suffix = request.handler.rpartition(".")[2]
        if suffix == "kcimport":
            return await self.open_import(request)
        if suffix == "import_upload":
            return await self.upload(request)
        if suffix == "check_control":
            return await self.control(request)
        args, event = request.arguments, request.event
        prefix = event.get("prefix") or "."
        dupe = suffix == "kdupes"
        target = args.get("target") or args.get("target_id")
        if args.get("help") or (not dupe and not target):
            usage = (
                f"{prefix}kdupes [@user | ID]"
                if dupe
                else f"{prefix}wlcheck [kc | kwl] <@user | ID>"
            )
            return self.simple(usage, mode="legacy" if dupe else "v2")
        owner = int(event["actor_id"])
        target = int(target or owner)
        if not 10**16 <= target < 2**64:
            return self.simple("Invalid user.", mode="legacy" if dupe else "v2")
        state = dict(
            owner=owner,
            target=target,
            guild=int(event["guild_id"]),
            channel=int(event["channel_id"]),
            dupe=dupe,
            reverse=args.get("mode") == "kc",
            page=0,
            revision=1,
            expires=self.module.clock() + TTL * 1000,
        )
        state["token"] = secrets.token_hex(8)
        await self.load(state)
        await self.save(state)
        return self.render(state)

    async def load(self, state):
        inventory_owner = state["target"] if state["dupe"] or state["reverse"] else state["owner"]
        wishlist_owner = state["owner"] if state["reverse"] else state["target"]
        account = await self.module.inventory.accounts.account(inventory_owner)
        state.update(inventory_owner=inventory_owner, wishlist_owner=wishlist_owner)
        if not account["known"]:
            await self.set_pages(state, ["No saved inventory."])
            return
        if state["dupe"]:
            records = await asyncio.to_thread(duplicates, account["cards"])
        else:
            wishes = await self.module.inventory.accounts.wishlist(wishlist_owner)
            if wishes is None:
                await self.set_pages(state, ["No saved wishlist."])
                return
            records = await asyncio.to_thread(matches, account["cards"], wishes["entries"])
        descriptions = await asyncio.to_thread(pages, records, wishlist=not state["dupe"])
        await self.set_pages(
            state,
            descriptions or ["No saved duplicates." if state["dupe"] else "No saved matches."],
        )

    async def set_pages(self, state, descriptions):
        def fingerprint():
            return hashlib.sha256(json.dumps(descriptions, ensure_ascii=False).encode()).hexdigest()

        digest = await asyncio.to_thread(fingerprint)
        for offset in range(0, len(descriptions), 256):
            pipeline = self.module.cache.pipeline(transaction=True)
            for index, body in enumerate(descriptions[offset : offset + 256], offset):
                pipeline.setex(PREFIX + f"result:{digest}:{index}", TTL, body)
            await pipeline.execute()
        state.update(snapshot=digest, count=len(descriptions), body=descriptions[0])

    async def save(self, state):
        remaining = max(1, (state["expires"] - self.module.clock() + 999) // 1000)
        await self.module.cache.setex(PREFIX + state["token"], remaining, json.dumps(state))

    def render(self, state, *, kind="send"):
        index, count = state["page"], state["count"]
        controls = []
        prefix = "dupes" if state["dupe"] else "wishlist"
        for action, label, disabled in (
            ("previous", "Previous", index == 0),
            ("indicator", f"{index + 1}/{count}", True),
            ("next", "Next", index == count - 1),
        ):
            if count > 1:
                controls.append(
                    PresentationControl(
                        "button",
                        MODULE,
                        prefix + "_" + action,
                        f"{state['token']}~{state['revision']}",
                        label=label,
                        disabled=disabled,
                    )
                )
        if not state["dupe"]:
            controls.append(
                PresentationControl(
                    "button",
                    MODULE,
                    "wishlist_swap",
                    f"{state['token']}~{state['revision']}",
                    label="Swap",
                )
            )
        description = state["body"]
        if not state["dupe"]:
            description = (
                f"Cards: <@{state['inventory_owner']}> · Wishlist: <@{state['wishlist_owner']}>\n\n"
                + description
            )
        page = PresentationPage(
            title="Duplicate Characters" if state["dupe"] else "Wishlist matches",
            description=description,
            footer=None if state["dupe"] else "Saved data",
            controls=tuple(controls) if not state["dupe"] else (),
        )
        return DiscordIntent(
            kind,
            pages=(page,),
            session_id=state["token"],
            controls=tuple(controls) if state["dupe"] else (),
            presentation_mode="legacy" if state["dupe"] else "v2",
        )

    async def control(self, request):
        token = request.arguments["session"]
        lock = self.locks.setdefault(token, asyncio.Lock())
        async with lock:
            raw = await self.module.cache.get(PREFIX + token)
            if not raw:
                return None
            state = json.loads(raw)
            if (
                state.get("kind") == "import"
                or state["owner"] != request.event.get("actor_id")
                or state["guild"] != request.event.get("guild_id")
                or state["channel"] != request.event.get("channel_id")
                or state["expires"] <= self.module.clock()
                or state["revision"] != request.arguments.get("revision")
            ):
                return None
            action = request.route.rpartition(".")[2]
            expected = "dupes_" if state["dupe"] else "wishlist_"
            if not action.startswith(expected):
                return None
            action = action.removeprefix(expected)
            if action == "swap" and not state["dupe"]:
                state["reverse"] = not state["reverse"]
                state["page"] = 0
                await self.load(state)
            elif action in {"previous", "next"}:
                state["page"] = max(
                    0, min(state["count"] - 1, state["page"] + (1 if action == "next" else -1))
                )
            else:
                return None
            if action in {"previous", "next"}:
                raw = await self.module.cache.get(
                    PREFIX + f"result:{state['snapshot']}:{state['page']}"
                )
                if raw is None:
                    return None
                state["body"] = raw.decode() if isinstance(raw, bytes) else raw
            state["revision"] += 1
            await self.save(state)
            return self.render(state, kind="edit")

    def import_panel(self, state, *, kind="send", text=None):
        return DiscordIntent(
            kind,
            pages=(
                PresentationPage(
                    title=None if state.get("complete") else "Import collection",
                    description=text or "",
                    controls=()
                    if state.get("complete")
                    else (
                        PresentationControl(
                            "button",
                            MODULE,
                            "csv_upload",
                            f"{state['token']}~{state['revision']}",
                            label="Upload CSV",
                            style="primary",
                        ),
                    ),
                ),
            ),
            session_id=state["token"],
            presentation_mode="v2",
        )

    async def open_import(self, request):
        if request.arguments.get("help"):
            return self.simple(f"{request.event.get('prefix') or '.'}kcimport")
        state = dict(
            kind="import",
            owner=int(request.event["actor_id"]),
            guild=int(request.event["guild_id"]),
            channel=int(request.event["channel_id"]),
            revision=1,
            expires=self.module.clock() + TTL * 1000,
            token=secrets.token_hex(8),
        )
        await self.save(state)
        return self.import_panel(state)

    async def upload(self, request):
        token = request.arguments["session"]
        lock = self.locks.setdefault(token, asyncio.Lock())
        async with lock:
            raw = await self.module.cache.get(PREFIX + token)
            if raw is None:
                return None
            state, event = json.loads(raw), request.event
            if (
                state.get("kind") != "import"
                or state["owner"] != event.get("actor_id")
                or state["guild"] != event.get("guild_id")
                or state["channel"] != event.get("channel_id")
                or state["expires"] <= self.module.clock()
                or len(request.arguments.get("files", {}).get("csv", ())) != 1
            ):
                return None
            source = int(event["interaction_id"])
            if state.get("last_source") == source:
                return self.import_panel(state, kind="edit", text=state["last_result"])
            if state["revision"] != request.arguments["revision"] or state.get("complete"):
                return None
            text, ok = await self.import_csv(request)
            state.update(
                last_source=source,
                last_result=text,
                complete=ok,
                revision=state["revision"] + 1,
            )
            await self.save(state)
            return self.import_panel(state, kind="edit", text=text)

    async def import_csv(self, request, *, source=None, created=None, export_url=None):
        event = request.event
        source = int(source if source is not None else event["interaction_id"])
        created = int(created if created is not None else event["interaction_created_at_ms"])
        prior = await self.module.inventory.accounts.import_status(
            int(event["actor_id"]),
            source,
            created,
        )
        if prior is not None:
            return f"{prior:,} cards saved.", True
        deadline = event.get("_expires_at_ms", self.module.clock() + 60_000)
        if self.module.clock() >= deadline or min(source, created) <= 0:
            return "Expired.", False
        try:
            data = await download_csv(request, export_url)
            records = await asyncio.to_thread(parse_csv, data)
            if self.module.clock() >= deadline:
                return "Expired.", False
            count = await self.module.inventory.accounts.import_cards(
                int(event["actor_id"]),
                source,
                created,
                hashlib.sha256(data).hexdigest(),
                records,
                deadline=deadline,
                clock=self.module.clock,
            )
            return f"{count:,} cards saved.", True
        except (CSVSourceError, ImportError) as error:
            return str(error), False
