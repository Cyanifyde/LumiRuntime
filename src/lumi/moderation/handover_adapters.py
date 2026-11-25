from __future__ import annotations

import copy
import hashlib
import json

from lumi.panels.store import MySQLPanelStore
from lumi.settings.store import MySQLSettingsStore

from .handover_sql import giveaway_tables
from .handover_status import check_phase
from .handover_store import MySQLHandoverStore
from .purge import PurgeError

MISSING = {"missing_reference": True}
TABLE_KEYS = {
    "guild_channel_grants": ("guild_id", "scope", "module_key", "acl_key", "target_id"),
    "verification_panels": ("panel_id",),
    "reaction_role_panels": ("panel_id",),
    "button_role_panels": ("panel_id",),
    "pogdrop_feeds": ("provider_key", "guild_id"),
    "karuta_tracker_subscriptions": ("guild_id", "user_id"),
    "sofi_tracker_subscriptions": ("guild_id", "user_id"),
    "glitch_tracker_subscriptions": ("guild_id", "user_id"),
    "imageguard_config": ("guild_id",),
    "modlog_documents": ("guild_id", "kind", "id"),
    "halloween_events": ("guild_id",),
    "reminder_records": ("guild_id", "id"),
    "reminder_occurrences": ("id",),
    "giveaway_config": ("guild_id",),
    "giveaway_records": ("guild_id", "id"),
    "giveaway_expanded": ("guild_id", "id"),
}
CHILDREN = {
    "reaction_role_panel_messages": (
        "panel_id",
        "position",
        "message_id",
        "ownership",
        "applied_revision",
        "repair_state",
        "content_hash",
    ),
    "reaction_role_applied_routes": (
        "panel_id",
        "mapping_id",
        "message_id",
        "emoji_key",
        "role_id",
        "position",
        "applied_revision",
    ),
    "button_role_panel_messages": ("panel_id", "position", "message_id"),
}


def where(key):
    return " AND ".join(f"{column}=%s" for column in key)


async def rows(q, sql, parameters=()):
    await q.execute(sql, parameters)
    names = [entry[0] for entry in q.description]
    return [dict(zip(names, values, strict=True)) for values in await q.fetchall()]


def at(document, path):
    value = document
    for key in path:
        if (isinstance(value, dict) and key not in value) or (
            isinstance(value, list) and key >= len(value)
        ):
            return MISSING
        value = value[key]
    return copy.deepcopy(value)


def put(document, path, value):
    target = document
    for key in path[:-1]:
        target = target[key]
    key = path[-1]
    if value == MISSING:
        if isinstance(target, list):
            target.pop(key)
        else:
            target.pop(key, None)
    elif isinstance(target, list) and key == len(target):
        target.append(copy.deepcopy(value))
    else:
        target[key] = copy.deepcopy(value)


def protected_event(state):
    value = copy.deepcopy(state)
    for key in ("channel", "setup_channel"):
        value["config"].pop(key, None)
    for key in (
        "revision",
        "contribution_panels",
        "contribution_portal",
        "standings_parts",
        "setup_messages",
        "prize_prompt",
    ):
        value.pop(key, None)
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class ReferenceAdapters:
    def __init__(self, store):
        self.store = store

    async def execute(self, request, clock):
        if request.operation == "validate":
            await self.validate_plans(None, request=request, clock=clock)
        elif request.operation == "apply":
            await self.apply(request.code, clock(), request=request, clock=clock)
        elif request.operation == "verify":
            return await self.verify(request.code, request=request, clock=clock)
        elif request.operation == "rollback":
            await self.rollback(request.code, clock(), request=request, clock=clock)
        elif request.operation == "preflight":
            return await self.preflight(request, clock)
        else:
            raise ValueError("Unknown reference transaction")

    @staticmethod
    async def locked_record(q, request, clock, *, require_revision=True):


        await q.execute(
            "SELECT id FROM moderation_purge_jobs WHERE id=%s AND revision=%s AND lease_token=%s "
            "AND lease_until_ms>%s AND state IN ('queued','handover','cancelling','running') "
            "AND JSON_VALUE(document,'$.mode')='reset' FOR UPDATE",
            (request.code, request.revision, request.lease, clock()),
        )
        if not await q.fetchone():
            raise TimeoutError("Reference lease changed")
        await q.execute(
            "SELECT document,revision FROM moderation_channel_handovers WHERE id=%s FOR UPDATE",
            (request.code,),
        )
        raw = await q.fetchone()
        record = json.loads(raw[0]) if raw else {}
        if (
            not raw or (require_revision and int(raw[1]) != request.journal_revision)
            or (record.get("phase"), record.get("epoch", 0)) != (request.phase, request.epoch)
            or clock() >= request.deadline
        ):
            raise TimeoutError("Reference phase changed")
        return record

    async def preflight(self, request, clock):
        async with self.store.pool.acquire() as c, c.cursor() as q:
            try:


                record = await self.locked_record(q, request, clock, require_revision=False)
                timestamp = clock()
                workers = await self.store.read_workers(q, timestamp)
                if clock() >= request.deadline:
                    raise TimeoutError("Reference preflight expired")
                return check_phase(record, workers, request.workers, timestamp)
            finally:
                await c.rollback()

    async def validate_plans(self, record, *, request=None, clock=None):
        definitions = {
            "verification": ("verification_panels", "route_revision", "security", "verification"),
            "reaction": ("reaction_role_panels", "route_revision", "community", "reaction_roles"),
            "button": ("button_role_panels", "desired_revision", "community", "reaction_roles"),
        }
        seen, represented = set(), set()
        async with self.store.pool.acquire() as c, c.cursor() as q:
            try:
                if request is not None:
                    record = await self.locked_record(q, request, clock)
                await q.execute(
                    "SELECT 1 FROM button_role_panels WHERE guild_id=%s AND channel_id=%s "
                    "AND lifecycle <> 'deleted' AND pending_layout_create IS NOT NULL LIMIT 1",
                    (record["guild"], record["old"]),
                )
                if await q.fetchone():
                    raise PurgeError("A saved panel has an unconfirmed publication.")
                for group, plans in record.get("plans", {}).items():
                    for item in plans:
                        identity = (item["kind"], item["reference"], item["position"])
                        if identity in seen:
                            raise PurgeError("Duplicate replacement panel.")
                        seen.add(identity)
                        if item["kind"] == "giveaway":
                            if (
                                group != "cards"
                                or item["module"] != "giveaways"
                                or item["position"] != 0
                            ):
                                raise PurgeError("Invalid giveaway scope.")
                            value = {}
                            for table in await giveaway_tables(q):
                                await q.execute(
                                    f"SELECT document FROM {table} WHERE guild_id=%s "
                                    "AND id=%s AND channel_id=%s AND status!='migrated'",
                                    (record["guild"], item["reference"], record["old"]),
                                )
                                if raw := await q.fetchone():
                                    value = json.loads(raw[0])
                            if (
                                value.get("revision") != item["revision"]
                                or value.get("status") not in {"active", "ended"}
                                or item["sources"] != [value.get("message")]
                            ):
                                raise PurgeError("A giveaway changed.")
                            represented.add(("giveaway", item["reference"]))
                            continue
                        if item["kind"] in {"event", "event_setup"}:
                            if group != "community" or item["module"] != "halloween":
                                raise PurgeError("Invalid event panel scope.")
                            await q.execute(
                                "SELECT document FROM halloween_events WHERE guild_id=%s",
                                (record["guild"],),
                            )
                            raw = await q.fetchone()
                            event = json.loads(raw[0]) if raw else {}
                            if (
                                event.get("run") != item["reference"]
                                or event.get("revision") != item["revision"]
                            ):
                                raise PurgeError("The event changed. Try again.")
                            if item["kind"] == "event_setup":
                                if (
                                    event["stage"] != "setup"
                                    or event["organizer_id"] != item.get("owner")
                                    or item["sources"]
                                ):
                                    raise PurgeError("Invalid event setup panel.")
                            else:
                                collection = item["collection"]
                                if collection not in {
                                    "contribution_panels",
                                    "standings_parts",
                                    "prize_prompt",
                                    "contribution_portal",
                                }:
                                    raise PurgeError("Invalid event display.")
                                path = (
                                    (collection,)
                                    if collection in {"prize_prompt", "contribution_portal"}
                                    else (collection, item["index"])
                                )
                                part = at(event, path)
                                if (
                                    part == MISSING
                                    or part.get("channel") != record["old"]
                                    or part.get("message", 0) != item["old_message"]
                                    or set(item["sources"])
                                    != ({item["old_message"]} if item["old_message"] else set())
                                ):
                                    raise PurgeError("An event display changed.")
                            continue
                        if item["kind"] not in definitions:
                            raise PurgeError("Invalid saved panel.")
                        table, version, expected_group, module = definitions[item["kind"]]
                        if group != expected_group or item["module"] != module:
                            raise PurgeError("Invalid saved-panel scope.")
                        current = await rows(
                            q,
                            f"SELECT * FROM {table} WHERE panel_id=%s AND guild_id=%s AND "
                            f"channel_id=%s",
                            (item["reference"], record["guild"], record["old"]),
                        )
                        if (
                            len(current) != 1
                            or current[0][version] != item["revision"]
                            or current[0]["lifecycle"] not in {"active", "needs_repair"}
                        ):
                            raise PurgeError("A saved panel changed.")
                        parent = current[0]
                        represented.add((item["kind"], item["reference"]))
                        if item["kind"] == "verification":
                            sources = {parent["message_id"]} if parent["message_id"] else set()
                        else:
                            child = (
                                "reaction_role_panel_messages"
                                if item["kind"] == "reaction"
                                else "button_role_panel_messages"
                            )
                            await q.execute(
                                f"SELECT message_id FROM {child} WHERE panel_id=%s",
                                (item["reference"],),
                            )
                            sources = {int(r[0]) for r in await q.fetchall()}
                        if (
                            sources != set(item["sources"])
                            or parent.get("ownership", "lumi") != "lumi"
                        ):
                            raise PurgeError("A saved panel is not owned by Lumi.")
                for kind, (table, _, _, _) in definitions.items():
                    await q.execute(
                        f"SELECT panel_id FROM {table} WHERE guild_id=%s AND channel_id=%s AND "
                        f"lifecycle IN ('active','needs_repair')",
                        (record["guild"], record["old"]),
                    )
                    if any((kind, row[0]) not in represented for row in await q.fetchall()):
                        raise PurgeError("A saved panel is missing.")
                for table in await giveaway_tables(q):
                    await q.execute(
                        f"SELECT id FROM {table} WHERE guild_id=%s AND channel_id=%s "
                        "AND status IN ('active','ended')",
                        (record["guild"], record["old"]),
                    )
                    if any(("giveaway", row[0]) not in represented for row in await q.fetchall()):
                        raise PurgeError("A giveaway is missing.")
            finally:
                await c.rollback()

    async def scalar(self, q, journal, table, key, values):
        if table not in TABLE_KEYS or tuple(key) != TABLE_KEYS[table]:
            raise ValueError("Unknown channel adapter")
        columns = tuple(values)
        found = await rows(
            q,
            "SELECT "
            + ",".join(columns)
            + " FROM "
            + table
            + " WHERE "
            + where(key)
            + " FOR UPDATE",
            tuple(key.values()),
        )
        if len(found) != 1:
            raise PurgeError("A channel setting changed.")
        before = found[0]
        if before == values:
            return
        await q.execute(
            "UPDATE "
            + table
            + " SET "
            + ",".join(f"{column}=%s" for column in columns)
            + " WHERE "
            + where(key),
            (*values.values(), *key.values()),
        )
        journal.append(dict(kind="scalar", table=table, key=key, before=before, after=values))

    async def children(self, q, journal, table, panel, replacement):
        columns = CHILDREN[table]
        original = await rows(
            q,
            "SELECT "
            + ",".join(columns)
            + " FROM "
            + table
            + " WHERE panel_id=%s ORDER BY "
            + columns[1]
            + " FOR UPDATE",
            (panel,),
        )
        await q.execute("DELETE FROM " + table + " WHERE panel_id=%s", (panel,))
        for row in replacement:
            await q.execute(
                "INSERT INTO "
                + table
                + " ("
                + ",".join(columns)
                + ") VALUES ("
                + ",".join("%s" for _ in columns)
                + ")",
                tuple(row[column] for column in columns),
            )
        journal.append(
            dict(kind="children", table=table, panel=panel, before=original, after=replacement)
        )

    async def document(self, q, journal, table, key, changes, *, revision=True):
        if table not in TABLE_KEYS or tuple(key) != TABLE_KEYS[table]:
            raise ValueError("Unknown document adapter")
        await q.execute(
            "SELECT document FROM " + table + " WHERE " + where(key) + " FOR UPDATE",
            tuple(key.values()),
        )
        raw = await q.fetchone()
        if not raw:
            raise PurgeError("A saved document changed.")
        value = json.loads(raw[0])
        patches = []
        for path, replacement in changes:
            before = at(value, path)
            if before != replacement:
                put(value, path, replacement)
                patches.append(dict(path=list(path), before=before, after=replacement))
        if not patches:
            return value
        if revision:
            value["revision"] = value.get("revision", 0) + 1
        await self.write_document(q, table, key, value)
        journal.append(
            dict(kind="document", table=table, key=key, patches=patches, revision=revision)
        )
        return value

    @staticmethod
    async def write_document(q, table, key, value):
        fields, arguments = "document=%s", [json.dumps(value, separators=(",", ":"))]
        if table in {"halloween_events", "modlog_documents"}:
            fields += ",revision=%s"
            arguments.append(value["revision"])
        if table == "reminder_records":
            fields += ",channel_id=%s"
            arguments.append(value["channel_id"])
        if table == "reminder_occurrences":
            fields += ",state=%s,recover_ms=%s"
            arguments.extend((value["state"], value["recover_ms"]))
        if table in {"giveaway_records", "giveaway_expanded"}:
            fields += ",channel_id=%s,next_ms=%s"
            arguments.extend((value["channel"], value["next_ms"]))
        await q.execute(
            "UPDATE " + table + " SET " + fields + " WHERE " + where(key),
            (*arguments, *key.values()),
        )

    async def apply(self, code, now, *, request=None, clock=None):
        async with self.store.pool.acquire() as c, c.cursor() as q:
            try:
                if request is not None:
                    record = await self.locked_record(q, request, clock)
                else:
                    await q.execute(
                        "SELECT document FROM moderation_channel_handovers WHERE id=%s FOR UPDATE", (code,),
                    )
                    record = json.loads((await q.fetchone())[0])
                if record["phase"] != "applying":
                    return record
                guild, old, new = record["guild"], record["old"], record["new"]
                plans = [p for group in record.get("plans", {}).values() for p in group]
                if any(not p.get("message") for p in plans):
                    raise PurgeError("A replacement panel is unconfirmed.")
                journal = []

                await q.execute(
                    "SELECT revision FROM guild_settings WHERE guild_id=%s FOR UPDATE", (guild,)
                )
                settings_exist = await q.fetchone()
                await q.execute(
                    "SELECT guild_id FROM reminder_guild_locks WHERE guild_id=%s FOR UPDATE",
                    (guild,),
                )
                await q.fetchone()
                grants = await rows(
                    q,
                    "SELECT guild_id,scope,module_key,acl_key,target_id FROM guild_channel_grants"
                    " WHERE guild_id=%s AND target_id=%s AND target_type='channel' FOR UPDATE",
                    (guild, old),
                )
                for key in grants:
                    await self.scalar(q, journal, "guild_channel_grants", key, dict(target_id=new))
                for table, kind, version in (
                    ("verification_panels", "verification", "route_revision"),
                    ("reaction_role_panels", "reaction", "route_revision"),
                    ("button_role_panels", "button", "desired_revision"),
                ):
                    panels = await rows(
                        q,
                        f"SELECT * FROM {table} WHERE guild_id=%s AND channel_id=%s AND "
                        f"lifecycle<>'deleted' FOR UPDATE",
                        (guild, old),
                    )
                    for panel in panels:
                        if kind == "button" and panel.get("pending_layout_create") is not None:
                            raise PurgeError("A saved panel has an unconfirmed publication.")
                        selected = sorted(
                            (
                                p
                                for p in plans
                                if p["kind"] == kind and p["reference"] == panel["panel_id"]
                            ),
                            key=lambda p: p["position"],
                        )
                        if panel["lifecycle"] in {"active", "needs_repair"} and not selected:
                            raise PurgeError("A saved panel is missing.")
                        if any(p["revision"] != panel[version] for p in selected):
                            raise PurgeError("A saved panel changed.")
                        values = dict(channel_id=new, **{version: panel[version] + 1})
                        if selected:
                            values.update(lifecycle="active")
                            if kind != "button":
                                values["message_id"] = selected[0]["message"]
                            if kind != "verification":
                                values["applied_revision"] = panel[version] + 1
                        await self.scalar(
                            q, journal, table, dict(panel_id=panel["panel_id"]), values
                        )
                        if kind == "button" and selected:
                            await self.children(
                                q,
                                journal,
                                "button_role_panel_messages",
                                panel["panel_id"],
                                [
                                    dict(
                                        panel_id=panel["panel_id"],
                                        position=i,
                                        message_id=p["message"],
                                    )
                                    for i, p in enumerate(selected)
                                ],
                            )
                        if kind == "reaction" and selected:
                            actual = await rows(
                                q,
                                "SELECT mapping_id,emoji_key,role_id,position FROM "
                                "reaction_role_mappings WHERE panel_id=%s AND enabled=1 ORDER BY "
                                "position,mapping_id FOR UPDATE",
                                (panel["panel_id"],),
                            )
                            expected = [
                                dict(
                                    id=m["mapping_id"],
                                    emoji=m["emoji_key"],
                                    role=m["role_id"],
                                    position=m["position"],
                                )
                                for m in actual
                            ]
                            if expected != [m for p in selected for m in p["mappings"]]:
                                raise PurgeError("Reaction mappings changed.")
                            await self.children(
                                q,
                                journal,
                                "reaction_role_panel_messages",
                                panel["panel_id"],
                                [
                                    dict(
                                        panel_id=panel["panel_id"],
                                        position=i,
                                        message_id=p["message"],
                                        ownership="lumi",
                                        applied_revision=panel[version] + 1,
                                        repair_state="ready",
                                        content_hash=hashlib.sha256(
                                            json.dumps(p["intent"], sort_keys=True).encode()
                                        ).hexdigest(),
                                    )
                                    for i, p in enumerate(selected)
                                ],
                            )
                            routes = [
                                dict(
                                    panel_id=panel["panel_id"],
                                    mapping_id=m["id"],
                                    message_id=p["message"],
                                    emoji_key=m["emoji"],
                                    role_id=m["role"],
                                    position=m["position"],
                                    applied_revision=panel[version] + 1,
                                )
                                for p in selected
                                for m in p["mappings"]
                            ]
                            await self.children(
                                q,
                                journal,
                                "reaction_role_applied_routes",
                                panel["panel_id"],
                                routes,
                            )
                for table, pk in (
                    ("karuta_tracker_subscriptions", ("guild_id", "user_id")),
                    ("sofi_tracker_subscriptions", ("guild_id", "user_id")),
                    ("glitch_tracker_subscriptions", ("guild_id", "user_id")),
                    ("pogdrop_feeds", ("provider_key", "guild_id")),
                ):
                    keys = await rows(
                        q,
                        "SELECT "
                        + ",".join(pk)
                        + " FROM "
                        + table
                        + " WHERE guild_id=%s AND channel_id=%s FOR UPDATE",
                        (guild, old),
                    )
                    for key in keys:
                        await self.scalar(q, journal, table, key, dict(channel_id=new))
                await q.execute(
                    "SELECT document FROM imageguard_config WHERE guild_id=%s FOR UPDATE", (guild,)
                )
                raw = await q.fetchone()
                if raw and json.loads(raw[0]).get("log_channel") == old:
                    await self.document(
                        q,
                        journal,
                        "imageguard_config",
                        dict(guild_id=guild),
                        [(("log_channel",), new)],
                    )
                await self.modlog(q, journal, guild, old, new)
                await self.event(q, journal, record, plans)
                await self.giveaways(q, journal, record, plans, now)
                reminders = await rows(
                    q,
                    "SELECT id,guild_id FROM reminder_records WHERE guild_id=%s AND channel_id=%s"
                    " AND status IN ('active','paused') FOR UPDATE",
                    (guild, old),
                )
                for item in reminders:
                    await self.document(
                        q,
                        journal,
                        "reminder_records",
                        dict(guild_id=guild, id=item["id"]),
                        [(("channel_id",), new)],
                    )
                record.update(journal=journal, phase="publishing", applied=now)
                await self.republish(q, record, now, settings=bool(settings_exist))
                if request is not None and clock() >= request.deadline:
                    raise TimeoutError("Reference transaction expired before commit")
                record["revision"] = record.get("revision", 1) + 1
                await MySQLHandoverStore.save(q, record)
                await c.commit()
                return record
            finally:
                await c.rollback()

    async def modlog(self, q, journal, guild, old, new):

        await q.execute("SHOW TABLES LIKE 'modlog_documents'")
        if not await q.fetchone():
            return
        key = dict(guild_id=guild, kind="config", id="current")
        await q.execute(
            "SELECT document FROM modlog_documents "
            "WHERE guild_id=%s AND kind='config' AND id='current' FOR UPDATE",
            (guild,),
        )
        raw = await q.fetchone()
        if not raw:
            return
        cfg = json.loads(raw[0])
        changes = [
            (("destinations", category), new)
            for category, cid in cfg["destinations"].items()
            if cid == old
        ]
        if old in cfg["excluded"]:
            changes.append(
                (
                    ("excluded",),
                    list(dict.fromkeys(new if cid == old else cid for cid in cfg["excluded"])),
                )
            )
        await self.document(q, journal, "modlog_documents", key, changes)

    async def giveaways(self, q, journal, record, plans, now):
        await q.execute("SHOW TABLES LIKE 'giveaway_config'")
        if not await q.fetchone():
            return
        guild, old, new = record["guild"], record["old"], record["new"]
        await q.execute(
            "SELECT channel_id,revision FROM giveaway_config WHERE guild_id=%s FOR UPDATE", (guild,)
        )
        config = await q.fetchone()
        if config and config[0] == old:
            await self.scalar(
                q,
                journal,
                "giveaway_config",
                dict(guild_id=guild),
                dict(channel_id=new, revision=config[1] + 1),
            )
        for table in await giveaway_tables(q):
            await q.execute(
                f"SELECT id,document FROM {table} WHERE guild_id=%s AND channel_id=%s "
                "AND status IN ('active','ended') FOR UPDATE",
                (guild, old),
            )
            for identifier, raw in await q.fetchall():
                value = json.loads(raw)
                selected = [
                    p for p in plans if p["kind"] == "giveaway" and p["reference"] == identifier
                ]
                if (
                    len(selected) != 1
                    or selected[0]["revision"] != value["revision"]
                    or selected[0]["sources"] != [value["message"]]
                    or value.get("pending_edit")
                    or value.get("cancel_pending")
                ):
                    raise PurgeError("A giveaway changed.")
                await self.document(
                    q,
                    journal,
                    table,
                    dict(guild_id=guild, id=identifier),
                    [
                        (("channel",), new),
                        (("message",), selected[0]["message"]),
                        (("next_ms",), now),
                    ],
                )

    async def event(self, q, journal, record, plans):
        await q.execute(
            "SELECT document FROM halloween_events WHERE guild_id=%s FOR UPDATE", (record["guild"],)
        )
        raw = await q.fetchone()
        if not raw:
            return
        value = json.loads(raw[0])
        old, new = record["old"], record["new"]
        selected = [p for p in plans if p["kind"] in {"event", "event_setup"}]
        if any(
            p["reference"] != value["run"] or p["revision"] != value["revision"] for p in selected
        ):
            raise PurgeError("The event changed. Try again.")
        changes = []
        for key in ("channel", "setup_channel"):
            if value["config"].get(key) == old:
                changes.append((("config", key), new))
        if not changes:
            return
        if value.get("drop") or value.get("outbox"):
            raise PurgeError("An event drop is still active.")
        record["event_backup"] = copy.deepcopy(value)
        record["event_protected"] = protected_event(value)
        for p in selected:
            if p["kind"] == "event_setup":
                for index, part in enumerate(value.get("setup_messages", [])):
                    if part["channel"] == old and part["status"] == "pending":
                        changes.append((("setup_messages", index, "status"), "removed"))
                changes.append(
                    (
                        ("setup_messages", len(value["setup_messages"])),
                        dict(
                            channel=new,
                            message=p["message"],
                            status="pending",
                            retire_after="contributions",
                        ),
                    )
                )
                continue
            collection = p["collection"]
            if collection not in {
                "prize_prompt",
                "contribution_portal",
                "contribution_panels",
                "standings_parts",
            }:
                raise PurgeError("Invalid event display.")
            path = (
                (collection,)
                if collection in {"prize_prompt", "contribution_portal"}
                else (collection, p["index"])
            )
            part = at(value, path)
            if (
                part == MISSING
                or part.get("channel") != old
                or part.get("message", 0) != p["old_message"]
            ):
                raise PurgeError("An event display changed.")
            for key, replacement in dict(
                channel=new, message=p["message"], hash=p["digest"], status="applied"
            ).items():
                changes.append(((*path, key), replacement))
            if collection == "contribution_portal" and not any(
                part.get("message") == p["old_message"] and part.get("channel") == old
                for part in value.get("contribution_panels", [])
            ):
                changes.append(
                    (
                        ("contribution_panels", len(value["contribution_panels"])),
                        dict(channel=new, message=p["message"], hash=p["digest"], status="applied"),
                    )
                )
            portal = value.get("contribution_portal")
            if (
                portal
                and portal.get("channel") == old
                and portal.get("message") == p["old_message"]
            ):
                changes.extend(
                    [
                        (("contribution_portal", "channel"), new),
                        (("contribution_portal", "message"), p["message"]),
                    ]
                )
        await self.document(q, journal, "halloween_events", dict(guild_id=record["guild"]), changes)

    async def republish(self, q, record, now, *, settings=True):
        guild = record["guild"]
        if settings:
            await q.execute(
                "UPDATE guild_settings SET revision=revision+1 WHERE guild_id=%s", (guild,)
            )
            snapshot = await MySQLSettingsStore._snapshot(q, guild)
            await MySQLSettingsStore._insert_outbox(q, snapshot)
            record["policy_revision"] = snapshot.revision
        await MySQLPanelStore._bump_reaction_index(q)
        await q.execute("SELECT revision FROM panel_route_revisions WHERE topic='reaction_roles'")
        record["reaction_revision"] = int((await q.fetchone())[0])

        for prefix in ("karuta", "sofi", "glitch"):
            timers = await rows(
                q,
                f"SELECT t.user_id,t.timer_kind,t.revision,t.due_at,s.channel_id FROM "
                f"{prefix}_tracker_timers t JOIN {prefix}_tracker_subscriptions s "
                f"USING(guild_id,user_id) WHERE t.guild_id=%s AND t.active=1 FOR UPDATE",
                (guild,),
            )
            for timer in timers:
                await q.execute(
                    f"UPDATE {prefix}_tracker_timers SET revision=revision+1 WHERE guild_id=%s "
                    f"AND user_id=%s AND timer_kind=%s AND revision=%s",
                    (guild, timer["user_id"], timer["timer_kind"], timer["revision"]),
                )
                if timer["due_at"] is not None:
                    await q.execute(
                        f"INSERT INTO {prefix}_schedule_outbox "
                        f"(guild_id,user_id,timer_kind,revision,operation,due_at,channel_id) "
                        f"VALUES (%s,%s,%s,%s,'upsert',%s,%s)",
                        (
                            guild,
                            timer["user_id"],
                            timer["timer_kind"],
                            timer["revision"] + 1,
                            timer["due_at"],
                            timer["channel_id"],
                        ),
                    )
        records = await rows(
            q,
            "SELECT document FROM reminder_records WHERE guild_id=%s AND status='active' FOR "
            "UPDATE",
            (guild,),
        )
        for raw in records:
            reminder = json.loads(raw["document"])
            if not reminder.get("current"):
                continue
            await q.execute(
                "SELECT document FROM reminder_occurrences WHERE id=%s FOR UPDATE",
                (reminder["current"],),
            )
            raw_occurrence = await q.fetchone()
            if not raw_occurrence:
                continue
            occurrence = json.loads(raw_occurrence[0])
            if occurrence["state"] not in {"ready", "queued"}:
                if occurrence["state"] in {"checking", "sending", "ambiguous"}:
                    raise PurgeError("A reminder delivery is unresolved.")
                continue
            occurrence.update(
                channel_id=reminder["channel_id"],
                reminder_revision=reminder["revision"],
                dispatch_revision=occurrence["dispatch_revision"] + 1,
                state="queued",
                recover_ms=now + 300000,
            )
            await self.write_document(
                q, "reminder_occurrences", dict(id=occurrence["id"]), occurrence
            )
            entry = dict(
                id=f"{occurrence['id']}:{occurrence['dispatch_revision']}:upsert",
                guild_id=guild,
                created_ms=now,
                published=False,
                operation="upsert",
                occurrence_id=occurrence["id"],
                revision=occurrence["dispatch_revision"],
                user_id=occurrence["user_id"],
                channel_id=occurrence["channel_id"],
                due_ms=now,
                expires_ms=now + 300000,
            )
            await q.execute(
                "INSERT IGNORE INTO reminder_schedule_outbox "
                "(id,guild_id,created_ms,operation,published,document) VALUES "
                "(%s,%s,%s,'upsert',0,%s)",
                (entry["id"], guild, now, json.dumps(entry)),
            )

    async def verify(self, code, *, request=None, clock=None):
        record = None if request is not None else await self.store.get(code)
        async with self.store.pool.acquire() as c, c.cursor() as q:
            try:
                if request is not None:
                    record = await self.locked_record(q, request, clock)
                for change in record.get("journal", []):
                    if change["kind"] == "scalar":
                        columns, key = tuple(change["after"]), change["key"]
                        key = {k: change["after"].get(k, v) for k, v in key.items()}
                        found = await rows(
                            q,
                            "SELECT "
                            + ",".join(columns)
                            + " FROM "
                            + change["table"]
                            + " WHERE "
                            + where(key),
                            tuple(key.values()),
                        )
                        if found != [change["after"]]:
                            return False
                    elif change["kind"] == "document":
                        await q.execute(
                            "SELECT document FROM "
                            + change["table"]
                            + " WHERE "
                            + where(change["key"]),
                            tuple(change["key"].values()),
                        )
                        raw = await q.fetchone()
                        value = json.loads(raw[0]) if raw else {}
                        if any(
                            at(value, patch["path"]) != patch["after"]
                            for patch in change["patches"]
                        ):
                            return False
                        if (
                            change["table"] == "halloween_events"
                            and protected_event(value) != record["event_protected"]
                        ):
                            return False
                    else:
                        columns = CHILDREN[change["table"]]
                        found = await rows(
                            q,
                            "SELECT "
                            + ",".join(columns)
                            + " FROM "
                            + change["table"]
                            + " WHERE panel_id=%s ORDER BY "
                            + columns[1],
                            (change["panel"],),
                        )
                        if sorted(found, key=lambda r: str(r[columns[1]])) != sorted(
                            change["after"], key=lambda r: str(r[columns[1]])
                        ):
                            return False
                return True
            finally:
                await c.rollback()

    async def rollback(self, code, now, *, request=None, clock=None):
        async with self.store.pool.acquire() as c, c.cursor() as q:
            try:
                if request is not None:
                    record = await self.locked_record(q, request, clock)
                else:
                    await q.execute(
                        "SELECT document FROM moderation_channel_handovers WHERE id=%s FOR UPDATE", (code,),
                    )
                    record = json.loads((await q.fetchone())[0])
                if record.get("references_rolled_back"):
                    return record

                panel_revisions = {}
                for change in record.get("journal", []):
                    if change["kind"] != "scalar":
                        continue
                    version = {
                        "verification_panels": "route_revision",
                        "reaction_role_panels": "route_revision",
                        "button_role_panels": "desired_revision",
                    }.get(change["table"])
                    if version and version in change["after"]:
                        restored = dict(change["before"])
                        restored[version] = change["after"][version] + 1
                        if restored.get("applied_revision") == change["before"][version]:
                            restored["applied_revision"] = restored[version]
                        panel_revisions[(change["table"], change["key"]["panel_id"])] = restored
                for change in reversed(record.get("journal", [])):
                    if change["kind"] == "scalar":
                        columns, key, table = tuple(change["after"]), change["key"], change["table"]
                        current_key = {k: change["after"].get(k, v) for k, v in key.items()}
                        current = await rows(
                            q,
                            "SELECT "
                            + ",".join(columns)
                            + " FROM "
                            + table
                            + " WHERE "
                            + where(current_key)
                            + " FOR UPDATE",
                            tuple(current_key.values()),
                        )
                        if current != [change["after"]]:
                            raise PurgeError("A moved setting changed. Review the reset.")
                        restored = panel_revisions.get(
                            (table, key.get("panel_id")), change["before"]
                        )
                        await q.execute(
                            "UPDATE "
                            + table
                            + " SET "
                            + ",".join(f"{column}=%s" for column in columns)
                            + " WHERE "
                            + where(key),
                            (
                                *restored.values(),
                                *(
                                    change["after"][k] if k in change["after"] else v
                                    for k, v in key.items()
                                ),
                            ),
                        )
                    elif change["kind"] == "document":
                        key, table = change["key"], change["table"]
                        await q.execute(
                            "SELECT document FROM "
                            + table
                            + " WHERE "
                            + where(key)
                            + " FOR UPDATE",
                            tuple(key.values()),
                        )
                        value = json.loads((await q.fetchone())[0])
                        for patch in reversed(change["patches"]):
                            if at(value, patch["path"]) != patch["after"]:
                                raise PurgeError("A moved document changed. Review the reset.")
                            put(value, patch["path"], patch["before"])
                        if change["revision"]:
                            value["revision"] = value.get("revision", 0) + 1
                        await self.write_document(q, table, key, value)
                    else:
                        scratch = []
                        columns = CHILDREN[change["table"]]
                        current = await rows(
                            q,
                            "SELECT "
                            + ",".join(columns)
                            + " FROM "
                            + change["table"]
                            + " WHERE panel_id=%s ORDER BY "
                            + columns[1]
                            + " FOR UPDATE",
                            (change["panel"],),
                        )
                        if sorted(current, key=lambda r: str(r[columns[1]])) != sorted(
                            change["after"], key=lambda r: str(r[columns[1]])
                        ):
                            raise PurgeError("A rebuilt panel changed. Review the reset.")
                        restored = copy.deepcopy(change["before"])
                        parent = panel_revisions.get(("reaction_role_panels", change["panel"]))
                        if parent and "applied_revision" in columns:
                            for child in restored:
                                child["applied_revision"] = parent["applied_revision"]
                        await self.children(q, scratch, change["table"], change["panel"], restored)
                record.update(references_rolled_back=True, phase="rollback")
                await self.republish(q, record, now)
                if request is not None and clock() >= request.deadline:
                    raise TimeoutError("Reference transaction expired before commit")
                record["revision"] = record.get("revision", 1) + 1
                await MySQLHandoverStore.save(q, record)
                await c.commit()
                return record
            finally:
                await c.rollback()
