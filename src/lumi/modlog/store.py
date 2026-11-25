from __future__ import annotations

import asyncio
import copy
import json
import re
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass

from lumi.contracts.native_mutations import nickname_audit_reason
from lumi.moderation.store import MySQLStore as Connection
from lumi.storage.block_policy import relation as block_subjects

from .models import (
    ALL_EVENTS,
    CATEGORIES,
    MAX_FILE,
    MAX_STORAGE,
    TTL,
    Observation,
    category,
    config,
    identifier,
    role_changes,
)
from .mutations import matches as matches_suppression, matches_nickname
from .observations import interpret, millis, snapshot_key

SCHEMA = """CREATE TABLE IF NOT EXISTS modlog_documents (
 guild_id BIGINT UNSIGNED NOT NULL, kind VARCHAR(16) CHARACTER SET ascii NOT NULL,
 id VARCHAR(96) CHARACTER SET ascii NOT NULL, revision BIGINT NOT NULL DEFAULT 0,
 created BIGINT NOT NULL DEFAULT 0, expires BIGINT NOT NULL DEFAULT 0,
 subject BIGINT UNSIGNED NOT NULL DEFAULT 0, actor BIGINT UNSIGNED NOT NULL DEFAULT 0,
 channel BIGINT UNSIGNED NOT NULL DEFAULT 0, category VARCHAR(16) NOT NULL DEFAULT '',
 event VARCHAR(48) NOT NULL DEFAULT '', state VARCHAR(16) NOT NULL DEFAULT '',
 due BIGINT NOT NULL DEFAULT 0, document MEDIUMTEXT COLLATE utf8mb4_bin NOT NULL,
 PRIMARY KEY(guild_id,kind,id), INDEX modlog_history(guild_id,kind,created,id),
 INDEX modlog_delivery(kind,state,due), INDEX modlog_retention(expires),
 INDEX modlog_subject(guild_id,kind,subject,created),
 INDEX modlog_actor(guild_id,kind,actor,created)
) CHARACTER SET utf8mb4"""


@dataclass(frozen=True, slots=True)
class HistoryAccess:
    owner_id: int
    scoped_blocks: bool
    allow_nsfw: bool


class SQLTransaction:
    def __init__(self, q, guild):
        self.q, self.guild = q, guild
        self.changed_reply_scopes = set()

    async def get(self, kind, key):
        await self.q.execute(
            "SELECT document FROM modlog_documents WHERE guild_id=%s AND kind=%s AND id=%s",
            (self.guild, kind, str(key)),
        )
        row = await self.q.fetchone()
        return json.loads(row[0]) if row else None

    async def get_many(self, kind, keys):
        keys = tuple(keys)
        if not keys:
            return {}
        placeholders = ",".join("%s" for _ in keys)
        await self.q.execute(
            f"SELECT id,document FROM modlog_documents WHERE guild_id=%s AND kind=%s "
            f"AND id IN ({placeholders})",
            (self.guild, kind, *keys),
        )
        return {key: json.loads(raw) for key, raw in await self.q.fetchall()}

    async def put(self, kind, key, doc):
        if kind in {"config", "event"}:
            self.changed_reply_scopes.add((kind, str(key)))
        columns = (
            "revision",
            "created",
            "expires",
            "subject",
            "actor",
            "channel",
            "category",
            "event",
            "state",
            "due",
        )
        values = [
            doc.get(k, "" if k in {"category", "event", "state"} else 0)
            or ("" if k in {"category", "event", "state"} else 0)
            for k in columns
        ]
        values[7] = doc.get("kind", values[7]) if kind == "event" else values[7]
        await self.q.execute(
            "INSERT INTO modlog_documents (guild_id,kind,id,"
            + ",".join(columns)
            + ",document) VALUES ("
            + ",".join(["%s"] * 14)
            + ") ON DUPLICATE KEY UPDATE "
            + ",".join(k + "=VALUES(" + k + ")" for k in (*columns, "document")),
            (
                self.guild,
                kind,
                str(key),
                *values,
                json.dumps(doc, ensure_ascii=False, separators=(",", ":")),
            ),
        )

    async def remove(self, kind, key):
        if kind in {"config", "event"}:
            self.changed_reply_scopes.add((kind, str(key)))
        await self.q.execute(
            "DELETE FROM modlog_documents WHERE guild_id=%s AND kind=%s AND id=%s",
            (self.guild, kind, str(key)),
        )

    async def scan(
        self, kind, *, filters=None, limit=100, offset=0, all_guilds=False, oldest=False,
        history_access=None,
    ):
        where, args = ["kind=%s"], [kind]
        if not all_guilds:
            where.append("guild_id=%s")
            args.append(self.guild)
        for key, op, val in filters or ():
            assert key in {
                "created",
                "expires",
                "subject",
                "actor",
                "channel",
                "category",
                "event",
                "state",
                "due",
                "revision",
                "guild",
            } and op in {"=", ">", ">=", "<", "<=", "in", "not in"}
            key = "guild_id" if key == "guild" else key
            if op in {"in", "not in"}:
                where.append(
                    key
                    + (" IN (" if op == "in" else " NOT IN (")
                    + ",".join("%s" for _ in val)
                    + ")"
                )
                args.extend(val)
            else:
                where.append(key + op + "%s")
                args.append(val)
        if history_access is not None:


            subject_relation = block_subjects(scoped=history_access.scoped_blocks)
            for field in ("subject", "actor"):
                where.append(
                    f"NOT EXISTS (SELECT 1 FROM {subject_relation} b "
                    f"WHERE b.user_id=modlog_documents.{field} AND b.user_id<>%s)"
                )
                args.append(history_access.owner_id)
            if not history_access.allow_nsfw:
                where.append(
                    "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(document,'$.nsfw')), 'false') "
                    "NOT IN ('true','1')"
                )
        order = "ASC" if oldest else "DESC"
        await self.q.execute(
            "SELECT document FROM modlog_documents WHERE "
            + " AND ".join(where)
            + f" ORDER BY created {order},id {order} LIMIT %s OFFSET %s",
            (*args, min(limit, 1000), max(0, offset)),
        )
        return [json.loads(row[0]) for row in await self.q.fetchall()]

    async def total(self, kind, field=None, state=None):
        assert field in {None, "size"}

        aggregate = (
            "COUNT(*)"
            if field is None
            else "COALESCE(SUM(CAST(JSON_UNQUOTE(JSON_EXTRACT(document,'$.size')) AS UNSIGNED)),0)"
        )
        args = [self.guild, kind]
        where = "guild_id=%s AND kind=%s"
        if state is not None:
            where += " AND state=%s"
            args.append(state)
        await self.q.execute("SELECT " + aggregate + " FROM modlog_documents WHERE " + where, args)
        return int((await self.q.fetchone())[0])


class Store:
    revoke_reply_scopes = None

    async def revoke_stale_replies(self, tx):
        if self.revoke_reply_scopes is not None and tx.changed_reply_scopes:
            await self.revoke_reply_scopes(tx.guild, tx.changed_reply_scopes)

    async def identities(self, tx, row):
        for key, field in (("subject", "name"), ("actor", "actor_name")):
            member = await tx.get("snapshot", "member:" + str(row.get(key, 0)))
            if member and row.get(field) in {None, "Unknown", str(row.get(key, 0))}:
                row[field] = member.get("name") or row.get(field)
                if key == "subject" and not row.get("avatar"):
                    row["avatar"] = member.get("avatar")

    async def configuration(self, guild):
        async with self.transaction(guild) as tx:
            return await tx.get("config", "current") or config(guild)

    async def configurations(self):
        async with self.transaction(0) as tx:
            rows, offset = [], 0
            while page := await tx.scan("config", all_guilds=True, limit=1000, offset=offset):
                rows.extend(page)
                offset += len(page)
            return rows

    async def journal_done(self, guild, key, revision, now):
        async with self.transaction(guild) as tx:
            before = await tx.get("journal", key)
            if before is not None and before["revision"] >= revision:
                return
            await tx.put(
                "journal",
                key,
                dict(id=key, guild=guild, revision=revision, created=now, expires=now + TTL),
            )

    async def suppress(self, guild, channel, ids, batch, now, bot=0):
        async with self.transaction(guild) as tx:
            await tx.put(
                "suppress",
                "batch:" + batch,
                dict(
                    id="batch:" + batch,
                    guild=guild,
                    channel=channel,
                    actor=bot,
                    created=now,
                    expires=now + TTL,
                ),
            )
            for mid in ids:
                await tx.put(
                    "suppress",
                    str(mid),
                    dict(
                        id=str(mid),
                        guild=guild,
                        channel=channel,
                        batch=batch,
                        created=now,
                        expires=now + TTL,
                    ),
                )

    async def suppress_rename(self, guild, member, before, after, now):
        async with self.transaction(guild) as tx:
            await tx.put(
                "suppress",
                "nick:" + str(member),
                dict(
                    id="nick:" + str(member),
                    guild=guild,
                    before=before,
                    after=after,
                    created=now,
                    expires=now + 60000,
                ),
            )

    async def record_suppressions(self, obs, rows):
        async with self.transaction(obs.guild) as tx:
            if await tx.get("seen", obs.id):
                return
            previous = await tx.get_many("suppress", (row["id"] for row in rows))
            for row in rows:
                current = previous.get(row["id"])
                if current and (current["created"], current.get("capture_order", 0)) > (
                    row["created"], row["capture_order"],
                ):
                    continue
                await tx.put("suppress", row["id"], row)
            await tx.put("seen", obs.id, {
                "id": obs.id, "guild": obs.guild, "created": obs.occurred,
                "expires": obs.occurred + TTL,
            })

    async def configure(self, guild, revision, change, *, before_commit=None):
        async with self.transaction(guild) as tx:
            doc = await tx.get("config", "current") or config(guild)
            if doc["revision"] != revision:
                raise ValueError("Settings changed. Refresh.")
            change(doc)
            assert set(doc["events"]) <= ALL_EVENTS
            doc["revision"] += 1
            if before_commit is not None:
                await before_commit(tx)
            await tx.put("config", "current", doc)
            return doc

    async def ingest(self, obs: Observation, *, captured=False):
        async with self.transaction(obs.guild) as tx:
            if await tx.get("seen", obs.id):
                return
            cfg = await tx.get("config", "current") or config(obs.guild)
            key = ":".join(map(str, snapshot_key(obs)))
            before = await tx.get("snapshot", key)
            if captured and not obs.data.get("_baseline"):
                excluded = {
                    obs.channel, obs.data.get("parent_id"), obs.data.get("old_voice_parent_id"),
                    (obs.data.get("_voice_before") or {}).get("channel_id"),
                }.intersection(cfg["excluded"])
                if excluded:
                    return
                if obs.kind.startswith("MESSAGE_") and not cfg["bots"] and any(
                    obs.data.get(flag, (before or {}).get(flag)) for flag in ("bot", "webhook")
                ):
                    return
            if before and obs.order > 0 and before.get("_order", 0) >= obs.order:
                return
            snapshot, rows = interpret(obs, before)
            suppression = (
                await tx.get("suppress", "nick:" + str(obs.resource))
                if obs.kind == "GUILD_MEMBER_UPDATE"
                else None
            )
            if suppression:
                rows = [
                    r
                    for r in rows
                    if not (
                        r["kind"] == "nickname"
                        and matches_nickname(suppression, obs, r.get("before"), r.get("after"))
                    )
                ]
            if obs.kind == "MESSAGE_DELETE":
                owned = await tx.get("suppress", str(obs.resource))
                if matches_suppression(owned, obs) and owned["channel"] == obs.channel:
                    rows = []
            elif obs.kind == "MESSAGE_DELETE_BULK":
                remaining = []
                for mid in obs.data.get("ids", []):
                    prior = await tx.get("snapshot", f"message:{mid}")
                    if prior:
                        prior["_deleted"] = True
                        await tx.put("snapshot", f"message:{mid}", prior)
                    owned = await tx.get("suppress", str(mid))
                    if not (matches_suppression(owned, obs) and owned["channel"] == obs.channel):
                        remaining.append(mid)
                for row in rows:
                    row.update(ids=remaining, total=len(remaining))
                if not remaining:
                    rows = []
            snapshot.update(
                id=key, guild=obs.guild, expires=obs.occurred + TTL, created=obs.occurred
            )
            if obs.kind != "GUILD_AUDIT_LOG_ENTRY_CREATE":
                await tx.put("snapshot", key, snapshot)
            if obs.kind == "GUILD_AUDIT_LOG_ENTRY_CREATE":
                await self._audit(tx, obs, cfg)
            for row in rows:
                await self.identities(tx, row)
                if (
                    row["kind"] not in cfg["events"]
                    or (row["category"] == "messages" and row.get("bot") and not cfg["bots"])
                    or row["channel"] in cfg["excluded"]
                    or row.get("parent_id") in cfg["excluded"]
                ):
                    continue

                existing = await tx.get("event", row["id"])
                if existing:
                    if row.get("known_actor"):
                        row.update(
                            revision=existing["revision"] + 1,
                            format=existing["format"],
                            expires=existing["expires"],
                            created=existing["created"],
                        )
                        await tx.put("event", row["id"], row)
                        out = await tx.get("outbox", row["id"])
                        if out and out["state"] not in {"sending", "uncertain"}:
                            out.update(state="prepare", revision=row["revision"], due=obs.occurred)
                            await tx.put("outbox", row["id"], out)
                    continue
                if row["kind"] not in {
                    "message_delete",
                    "message_edit",
                    "attachments",
                    "bulk_delete",
                    "purge",
                    "rename",
                    "rename_undo",
                    "imageguard",
                }:
                    related = await tx.scan(
                        "event",
                        filters=[
                            ("subject", "=", row["subject"]),
                            ("created", ">=", obs.occurred - 15000),
                        ],
                        limit=10,
                    )
                    compatible = {row["kind"]}
                    if row["kind"] in {"kick", "ban"}:
                        compatible = {row["kind"], "depart"}
                    elif row["kind"] == "depart":
                        compatible = {"kick", "ban", "depart"}
                    matches = [
                        r
                        for r in related
                        if r["kind"] in compatible
                        and not (
                            r.get("journal_operation") and row.get("journal_operation")
                            and r["journal_operation"] != row["journal_operation"]
                        )
                        and (
                            r.get("known_actor")
                            or r.get("audit")
                            or row.get("known_actor")
                            or row["kind"] != r["kind"]
                        )
                    ]
                    if len(matches) == 1:
                        old = matches[0]
                        if not row.get("known_actor") and old.get("known_actor"):
                            continue
                        if old.get("audit") and not row.get("known_actor"):
                            row.update(
                                actor=old["actor"], reason=old.get("reason", ""), audit=old["audit"]
                            )
                        if row["kind"] == "depart" and old["kind"] in {"kick", "ban"}:
                            row.update(kind=old["kind"], category=old["category"])
                        row = {
                            **old,
                            **row,
                            "id": old["id"],
                            "revision": old["revision"] + 1,
                            "format": old["format"],
                        }
                        existing_out = await tx.get("outbox", old["id"])
                        await tx.put("event", row["id"], row)
                        if existing_out and existing_out["state"] not in {"sending", "uncertain"}:
                            existing_out.update(
                                revision=row["revision"], state="prepare", due=obs.occurred
                            )
                            await tx.put("outbox", row["id"], existing_out)
                        continue
                row["expires"] = obs.occurred + TTL
                row["config_revision"] = cfg["revision"]
                row["guild"] = obs.guild
                await tx.put("event", row["id"], row)
                await tx.put(
                    "outbox",
                    row["id"],
                    dict(
                        id=row["id"],
                        guild=obs.guild,
                        revision=1,
                        state="prepare",
                        due=obs.occurred,
                        created=obs.occurred,
                        expires=row["expires"],
                    ),
                )

            for f in snapshot.get("files", []):
                if not cfg["bots"] and (snapshot.get("bot") or snapshot.get("webhook")):
                    continue
                previous = await tx.get("file", f["id"])
                doc = {
                    **f,
                    "guild": obs.guild,
                    "channel": obs.channel,
                    "created": obs.occurred,
                    "expires": obs.occurred + TTL,
                }
                if previous:
                    doc = {**previous, "expires": max(previous["expires"], doc["expires"])}
                    if previous.get("sha"):
                        blob = await tx.get("blob", previous["sha"])
                        if blob:
                            blob["expires"] = max(blob["expires"], doc["expires"])
                            await tx.put("blob", previous["sha"], blob)
                await tx.put("file", f["id"], doc)
            await tx.put(
                "seen",
                obs.id,
                dict(id=obs.id, guild=obs.guild, created=obs.occurred, expires=obs.occurred + TTL),
            )

    async def _audit(self, tx, obs, cfg):
        d = obs.data
        action = int(d.get("action_type") or 0)
        if action == 31:
            d = {**d, "changes": [c for c in d.get("changes", []) if c["key"] != "position"]}
            if not d["changes"]:
                return
        target = int(d.get("target_id") or 0)
        actor = int(d.get("moderator") or d.get("user_id") or 0)
        lookup = {
            20: "kick",
            22: "ban",
            23: "unban",
            24: "timeout",
            25: "member_roles",
            72: "message_delete",
            73: "bulk_delete",
            74: "pin",
            75: "unpin",
            13: "permissions",
            14: "permissions",
            15: "permissions",
        }
        event = lookup.get(action)
        if action in {72, 73} and (
            match := re.match(r"Lumi purge ([a-f0-9]{8}) by \d+", str(d.get("reason") or ""))
        ):
            owned = await tx.get("suppress", "batch:" + match[1])
            channel = int((d.get("options") or {}).get("channel_id") or obs.channel or 0)
            if (
                matches_suppression(owned, obs) and owned.get("actor") == actor
                and (not channel or owned["channel"] == channel)
            ):
                return
        if action == 24:
            keys = {c["key"] for c in d.get("changes", [])}
            if "nick" in keys:
                event = "nickname"
                change = next(c for c in d["changes"] if c["key"] == "nick")
                owned = await tx.get("suppress", "nick:" + str(target))
                if (
                    matches_nickname(owned, obs, change.get("old_value"), change.get("new_value"))
                    and (
                        "operation" not in owned
                        or (
                            owned["actor"] == actor
                            and d.get("reason") in {
                                nickname_audit_reason(owned["operation"]),
                                "Lumi rename " + owned["operation"],
                            }
                        )
                    )
                ):
                    return
            elif "communication_disabled_until" not in keys:
                return
            else:
                change = next(c for c in d["changes"] if c["key"] == "communication_disabled_until")
                until = millis(change.get("new_value"))
                event = (
                    "untimeout"
                    if until <= obs.occurred
                    else "timeout_change"
                    if millis(change.get("old_value")) > obs.occurred
                    else "timeout"
                )
        if event is None:
            event = {
                1: "guild_update",
                10: "channel_create",
                11: "channel_update",
                12: "channel_delete",
                30: "role_create",
                31: "role_update",
                32: "role_delete",
                40: "invite",
                41: "invite",
                42: "invite",
                50: "webhook",
                51: "webhook",
                52: "webhook",
                60: "emoji",
                61: "emoji",
                62: "emoji",
                80: "integration",
                81: "integration",
                82: "integration",
                83: "integration",
                90: "sticker",
                91: "sticker",
                92: "sticker",
                100: "scheduled_event",
                101: "scheduled_event",
                102: "scheduled_event",
                110: "channel_create",
                111: "channel_update",
                112: "channel_delete",
            }.get(action)
        if event is None or event not in cfg["events"]:
            return
        options = d.get("options") or {}
        channel = int(options.get("channel_id") or obs.channel or 0)


        candidates = []
        if action not in {72, 73}:
            candidates = await tx.scan(
                "event",
                filters=[("subject", "=", target), ("created", ">=", obs.occurred - 15000)],
                limit=10,
            )
            candidates = [
                r
                for r in candidates
                if r["kind"] in {event, "depart" if event in {"kick", "ban"} else event}
                and not r.get("audit")
                and not (
                    r["kind"] == "role_update" and not role_changes(r["kind"], r.get("changes", {}))
                )
            ]
        if len(candidates) == 1:
            row = candidates[0]
            row.update(
                actor=row["actor"] if row.get("known_actor") else actor,
                audit=str(d["id"]),
                reason=row.get("reason", "")
                if row.get("known_actor")
                else str(d.get("reason") or "")[:512],
                revision=row["revision"] + 1,
                kind=event,
                category=category(event),
            )
            await self.identities(tx, row)
            await tx.put("event", row["id"], row)
            out = await tx.get("outbox", row["id"])
            if out and out["state"] not in {"sending", "uncertain"}:
                out.update(state="prepare", revision=row["revision"], due=obs.occurred)
                await tx.put("outbox", row["id"], out)
            return
        changes = {
            c["key"].replace("_", " ").title(): [c.get("old_value"), c.get("new_value")]
            for c in d.get("changes", [])
        }
        rid = identifier("audit", obs.guild, d["id"])
        if await tx.get("event", rid):
            return
        row = dict(
            id=rid,
            guild=obs.guild,
            kind=event,
            category=category(event),
            subject=target,
            name=str(target or "Server"),
            actor=actor,
            channel=channel,
            created=obs.occurred,
            expires=obs.occurred + TTL,
            revision=1,
            audit=str(d["id"]),
            format="v2",
            reason=str(d.get("reason") or "")[:512],
            changes=changes,
            files=[],
        )
        if event == "nickname":
            change = next(c for c in d["changes"] if c["key"] == "nick")
            row.update(before=change.get("old_value"), after=change.get("new_value"))
        if event in {"timeout", "timeout_change", "untimeout"}:
            change = next(c for c in d["changes"] if c["key"] == "communication_disabled_until")
            row.update(
                before=millis(change.get("old_value")),
                until=millis(change.get("new_value")),
                duration=max(0, millis(change.get("new_value")) - obs.occurred),
            )
        if event == "member_roles":
            row.update(
                added=[
                    x["name"]
                    for c in d.get("changes", [])
                    if c["key"] == "$add"
                    for x in c.get("new_value", [])
                ],
                removed=[
                    x["name"]
                    for c in d.get("changes", [])
                    if c["key"] == "$remove"
                    for x in c.get("new_value", [])
                ],
            )
        if action == 72:
            row.update(
                body=f"{int(options.get('count') or 1)} deletion(s) recorded in the audit log. "
                "Message identity unavailable."
            )
        if action == 73:
            row.update(total=int(options.get("count") or 0), audit_summary=True)
        await self.identities(tx, row)
        await tx.put("event", rid, row)
        await tx.put(
            "outbox",
            rid,
            dict(
                id=rid,
                guild=obs.guild,
                revision=1,
                state="prepare",
                due=obs.occurred,
                created=obs.occurred,
                expires=row["expires"],
            ),
        )

    async def pending(self, state, now, *, limit=20, guilds=None):
        if guilds is not None and not guilds:
            return []
        async with self.transaction(0) as tx:
            return await tx.scan(
                "outbox",
                filters=[("state", "=", state), ("due", "<=", now), ("expires", ">", now)]
                + ([("guild", "in", tuple(guilds))] if guilds is not None else []),
                all_guilds=True,
                limit=limit,
                oldest=True,
            )

    async def record(self, guild, rid):
        async with self.transaction(guild) as tx:
            return await tx.get("event", rid)

    async def prepare(self, guild, rid, revision, now):
        async with self.transaction(guild) as tx:
            row = await tx.get("event", rid)
            out = await tx.get("outbox", rid)
            if not row or not out or row["revision"] != revision or out["state"] != "prepare":
                return False
            for f in row.get("files", []):
                key = f"{f['id']}:{rid}"
                await tx.put(
                    "filelink",
                    key,
                    dict(
                        id=key,
                        guild=guild,
                        subject=f["id"],
                        record=rid,
                        created=row["created"],
                        expires=row["expires"],
                    ),
                )
            for f in row.get("files", []):
                saved = await tx.get("file", f["id"])
                if saved and saved["expires"] > now and saved.get("state", "") in {"", "reserved"}:
                    out["due"] = max(now + 1000, saved.get("due", 0))
                    await tx.put("outbox", rid, out)
                    return False
                if saved:
                    f.update(
                        {
                            k: v
                            for k, v in saved.items()
                            if k in {"sha", "status", "type", "name", "size"}
                        }
                    )
            await tx.put("event", rid, row)
            out.update(state="ready", due=now)
            await tx.put("outbox", rid, out)
            return True

    async def transition(
        self, guild, rid, expected, *, expected_revision=None, previous=None, **changes,
    ):
        async with self.transaction(guild) as tx:
            out = await tx.get("outbox", rid)
            if (
                not out
                or out["state"] not in expected
                or (expected_revision is not None and out["revision"] != expected_revision)
                or (previous is not None and out != previous)
            ):
                return None
            out.update(changes)
            await tx.put("outbox", rid, out)
            return out

    async def delivered(self, guild, rid, revision, message, now, *, attempt=None):
        async with self.transaction(guild) as tx:
            row = await tx.get("event", rid)
            out = await tx.get("outbox", rid)
            if (
                not row or not out or out["state"] not in {"sending", "uncertain"}
                or (attempt is not None and out.get("attempt") != attempt)
            ):
                return
            out.update(
                state="sent" if row["revision"] == revision else "prepare",
                revision=row["revision"],
                message=message,
                due=now,
            )
            await tx.put("outbox", rid, out)

    async def outbox(self, guild, rid):
        async with self.transaction(guild) as tx:
            return await tx.get("outbox", rid)

    async def publication_candidate(self, guild, rid, revision, now):
        async with self.transaction(guild) as tx:
            out = await tx.get("outbox", rid)
            row = await tx.get("event", rid)
            if (
                not out or not row or out["state"] != "ready" or out["due"] > now
                or out["revision"] != revision or row["revision"] != revision or row["expires"] <= now
            ):
                return None
            return row, out, await tx.get("config", "current") or config(guild)

    async def claim_publication(self, expected, plan, attempt, *, clock, issue):
        async with self.transaction(plan.guild) as tx:
            out = await tx.get("outbox", plan.record)
            row = await tx.get("event", plan.record)
            settings = await tx.get("config", "current") or config(plan.guild)
            if (
                not out or out != expected or out["state"] != "ready"
                or not row or row["revision"] != plan.revision
                or row["expires"] < plan.expires or plan.expires <= clock()
                or settings["revision"] != plan.config_revision
                or out.get("destination") not in {None, 0, plan.channel}
                or (out.get("message") or 0) != plan.message
            ):
                return None
            if not await issue():
                return None
            out.update(
                state="sending", destination=plan.channel, nonce=plan.nonce, due=plan.expires + 15000,
                attempt=attempt, attempt_job=plan.job, attempt_expires=plan.expires,
            )
            await tx.put("outbox", plan.record, out)
            return out

    async def recover_publication(self, expected, now, *, message=None, gone=False, retry_ms=0):
        guild, record = expected["guild"], expected["id"]
        async with self.transaction(guild) as tx:
            out = await tx.get("outbox", record)
            row = await tx.get("event", record)
            if (
                not row or row["expires"] <= now or not out or out != expected
                or out["state"] not in {"sending", "uncertain"} or out["due"] > now
            ):
                return False
            if gone:
                out.update(state="archived", due=0)
            elif out.get("message"):
                out.update(state="prepare", revision=row["revision"], due=now)
            elif message is not None:
                out.update(
                    state="sent" if row["revision"] == out["revision"] else "prepare",
                    message=message, revision=row["revision"], due=now,
                )
            else:
                if retry_ms <= 0:
                    raise ValueError("Missing recovery interval.")
                out.update(state="uncertain", due=now + retry_ms)
            await tx.put("outbox", record, out)
            return True

    async def history(self, guild, spec, allowed, now, *, access=None):
        async with self.transaction(guild) as tx:
            return await self._history(tx, spec, allowed, now, access=access)

    async def public_history(self, guild, spec, now, *, destination, revision, access, witness=None):
        if destination == 0:
            return [], False, False
        async with self.transaction(guild) as tx:
            settings = await tx.get("config", "current") or config(guild)
            if settings["revision"] != revision:
                raise ValueError("History destination changed.")
            destinations = settings["destinations"]
            allowed = tuple(
                cat for cat in CATEGORIES
                if (destinations.get(cat) or destinations.get("all")) == destination
            )
            rows, more = await self._history(tx, spec, allowed, now, access=access)
            if witness is not None:
                await witness(guild, revision, rows)
            return rows, more, bool(allowed)

    async def public_evidence(
        self, guild, record_id, now, *, destination, revision, allow_nsfw, posted_from=None,
        witness=None,
    ):
        if not destination:
            raise ValueError("Evidence destination unavailable.")
        async with self.transaction(guild) as tx:
            settings = await tx.get("config", "current") or config(guild)
            if settings["revision"] != revision:
                raise ValueError("Evidence destination changed.")
            row = await tx.get("event", record_id)
            if not row or row["expires"] <= now:
                raise ValueError("Record expired.")
            routes = settings["destinations"]
            if (routes.get(row["category"]) or routes.get("all")) != destination:
                raise ValueError("Evidence destination unavailable.")
            if row.get("nsfw") and not allow_nsfw:
                raise ValueError("Evidence destination unavailable.")
            if posted_from is not None:
                published = await tx.get("outbox", record_id)
                if not published or (published.get("message"), published.get("destination")) != posted_from:
                    raise ValueError("Evidence source unavailable.")
            snapshots = []
            if row["kind"] == "bulk_delete":
                for identifier in row.get("ids", ())[:100]:
                    snapshot = await tx.get("snapshot", "message:" + str(identifier))
                    if snapshot and (allow_nsfw or not snapshot.get("nsfw")):
                        snapshots.append(snapshot)
            if witness is not None:
                await witness(guild, revision, (row,))
            return row, snapshots

    @staticmethod
    async def _history(tx, spec, allowed, now, *, access):
        if not 0 <= spec.page <= 100000:
            raise ValueError("Invalid history page.")
        filters = [("created", ">=", now - spec.hours * 3600_000), ("expires", ">", now)]
        for key, val in (
            ("subject", spec.subject),
            ("actor", spec.actor),
            ("channel", spec.channel),
            ("event", spec.event),
            ("category", spec.category),
        ):
            if val:
                filters.append((key, "=", val))
        if not allowed:
            return [], False
        filters.append(("category", "in", tuple(allowed)))
        found = await tx.scan(
            "event", filters=filters, limit=6, offset=spec.page * 5, history_access=access,
        )
        return found[:5], len(found) > 5

    async def files_pending(self, now, *, guilds=None):
        if guilds is not None and not guilds:
            return []
        async with self.transaction(0) as tx:
            return await tx.scan(
                "file",
                filters=[
                    ("state", "in", ("", "reserved")),
                    ("due", "<=", now),
                    ("expires", ">", now),
                ]
                + ([("guild", "in", tuple(guilds))] if guilds is not None else []),
                all_guilds=True,
                limit=20,
                oldest=True,
            )

    async def download_slot(self, guild, now):
        async with self.transaction(0) as tx:
            slots = [await tx.get("download", str(i)) for i in range(2)]
            if any(s and s["due"] > now and s["subject"] == guild for s in slots):
                return None
            for i, slot in enumerate(slots):
                if not slot or slot["due"] <= now:
                    token = secrets.token_hex(16)
                    await tx.put(
                        "download",
                        str(i),
                        dict(
                            id=str(i),
                            guild=0,
                            subject=guild,
                            token=token,
                            due=now + 30000,
                            created=now,
                        ),
                    )
                    return str(i), token
            return None

    async def release_download(self, lease):
        async with self.transaction(0) as tx:
            slot = await tx.get("download", lease[0])
            if slot and slot["token"] == lease[1]:
                await tx.remove("download", lease[0])

    async def reserve(self, guild, file_id, now):
        async with self.transaction(guild) as tx:
            f = await tx.get("file", file_id)
            if (
                not f
                or f.get("state", "") not in {"", "reserved"}
                or f.get("due", 0) > now
                or f["expires"] <= now
            ):
                return None
            used = await tx.total("blob", "size") + await tx.total("file", "size", "reserved")
            if f.get("state") == "reserved":
                used -= f["size"]
            if used + f["size"] > MAX_STORAGE:
                f.update(state="failed", status="archive full", url="")
                await tx.put("file", file_id, f)
                return None
            f.update(state="reserved", due=now + 30000, attempts=f.get("attempts", 0) + 1)
            await tx.put("file", file_id, f)
            return f

    async def finish_file(
        self,
        guild,
        file_id,
        *,
        status,
        sha="",
        now=0,
        delay=0,
        save=None,
        size=None,
        media_type=None,
        name=None,
        attempt=None,
    ):
        async with self.transaction(guild) as tx:
            f = await tx.get("file", file_id)
            if (
                not f
                or f.get("state") in {"complete", "failed"}
                or f["expires"] <= now
                or (
                    attempt is not None
                    and (f.get("state") != "reserved" or f.get("attempts") != attempt)
                )
            ):
                return
            if status == "saved":
                actual = f["size"] if size is None else size
                blob = await tx.get("blob", sha)
                used = await tx.total("blob", "size") + await tx.total("file", "size", "reserved")
                if f.get("state") == "reserved":
                    used -= f["size"]
                if (
                    type(actual) is not int
                    or not 0 < actual <= MAX_FILE
                    or not re.fullmatch("[a-f0-9]{64}", sha)
                    or (blob and blob["size"] != actual)
                ):
                    status, sha = "invalid", ""
                elif used + (0 if blob else actual) > MAX_STORAGE:
                    status, sha = "archive full", ""
                else:


                    if save is not None:
                        await save()
                    f.setdefault("source_size", f["size"])
                    f.setdefault("source_type", f.get("type", ""))
                    f.setdefault("source_name", f["name"])
                    f["size"] = actual
                    if media_type is not None:
                        f["type"] = media_type
                    if name is not None:
                        f["name"] = name
            if status == "retry" and f.get("attempts", 0) < 4 and f["expires"] > now:
                f.update(
                    state="",
                    status="pending",
                    due=now + max(delay, min(300000, 5000 * 2 ** f.get("attempts", 0))),
                )
                await tx.put("file", file_id, f)
                return
            if status == "retry":
                status = "unavailable"
            f.update(
                state="complete" if status == "saved" else "failed", status=status, sha=sha, url=""
            )
            await tx.put("file", file_id, f)
            if sha:
                blob = await tx.get("blob", sha) or dict(
                    id=sha, guild=guild, size=f["size"], created=now, expires=f["expires"]
                )
                blob["expires"] = max(blob["expires"], f["expires"])
                await tx.put("blob", sha, blob)
            await self._refresh_files(tx, f, now)

    async def _refresh_files(self, tx, file, now):

        offset = 0
        while links := await tx.scan(
            "filelink",
            filters=[("subject", "=", file["id"]), ("expires", ">", now)],
            limit=1000,
            offset=offset,
        ):
            offset += len(links)
            for link in links:
                row = await tx.get("event", link["record"])
                out = await tx.get("outbox", link["record"])
                if not row or not out or row["expires"] <= now or out["state"] == "prepare":
                    continue
                changed = False
                for item in row.get("files", []):
                    if item["id"] != file["id"]:
                        continue
                    updates = {
                        k: v
                        for k, v in file.items()
                        if k in {"sha", "status", "type", "name", "size"}
                    }
                    if any(item.get(k) != v for k, v in updates.items()):
                        item.update(updates)
                        changed = True
                if not changed:
                    continue
                row["revision"] += 1
                await tx.put("event", row["id"], row)
                if out["state"] not in {"sending", "uncertain", "archived"}:
                    out.update(state="prepare", revision=row["revision"], due=now)
                    await tx.put("outbox", row["id"], out)

    async def file(self, guild, fid):
        async with self.transaction(guild) as tx:
            return await tx.get("file", fid)

    async def snapshot(self, guild, key):
        async with self.transaction(guild) as tx:
            return await tx.get("snapshot", key)

    async def stats(self, guild, now):
        async with self.transaction(guild) as tx:
            return dict(
                queued=sum(
                    [
                        await tx.total("outbox", state=s)
                        for s in ("prepare", "ready", "approved", "sending", "paused", "uncertain")
                    ]
                ),
                uncertain=await tx.total("outbox", state="uncertain"),
                archive_bytes=await tx.total("blob", "size"),
                skipped=await tx.total("file", state="failed"),
                gaps=await tx.scan("gap", limit=5),
            )

    async def gap(self, guild, now, reason):
        async with self.transaction(guild) as tx:
            previous = await tx.get("gap", "latest")
            if previous is not None and previous["created"] >= now:
                return
            await tx.put(
                "gap",
                "latest",
                dict(id="latest", guild=guild, created=now, expires=now + TTL, reason=reason),
            )

    async def cleanup(self, now):
        expired_blobs, candidates = [], []
        async with self.transaction(0) as tx:
            for kind in (
                "event",
                "snapshot",
                "outbox",
                "seen",
                "file",
                "filelink",
                "blob",
                "gap",
                "suppress",
                "journal",
            ):
                rows = await tx.scan(
                    kind,
                    filters=[("expires", ">", 0), ("expires", "<=", now)],
                    all_guilds=True,
                    limit=100,
                )
                candidates.extend((kind, row) for row in rows)
        for kind, row in candidates:
            async with self.transaction(row["guild"]) as tx:
                latest = await tx.get(kind, row["id"])
                if latest and 0 < latest["expires"] <= now:
                    if kind == "blob":
                        expired_blobs.append((row["guild"], row["id"]))
                        continue
                    await tx.remove(kind, row["id"])
        return expired_blobs


class MySQLStore(Store, Connection):
    async def migrate(self):
        async with self.pool.acquire() as c, c.cursor() as q:
            await q.execute(SCHEMA)
            await c.commit()

    @asynccontextmanager
    async def transaction(self, guild):
        async with self.pool.acquire() as c, c.cursor() as q:
            lock = f"modlog:{guild}"
            try:
                await q.execute("SELECT GET_LOCK(%s,3)", (lock,))
                if (await q.fetchone())[0] != 1:
                    raise TimeoutError("Modlog storage busy")
                await c.begin()
                tx = SQLTransaction(q, guild)
                yield tx
                await self.revoke_stale_replies(tx)
                await c.commit()
            finally:
                await c.rollback()
                await q.execute("SELECT RELEASE_LOCK(%s)", (lock,))
                await q.fetchone()


class MemoryTransaction:
    def __init__(self, rows, guild, blocked):
        self.rows, self.guild = rows, guild
        self.blocked = blocked
        self.changed_reply_scopes = set()

    async def get(self, kind, key):
        return copy.deepcopy(self.rows.get((self.guild, kind, str(key))))

    async def get_many(self, kind, keys):
        return {key: await self.get(kind, key) for key in keys}

    async def put(self, kind, key, doc):
        if kind in {"config", "event"}:
            self.changed_reply_scopes.add((kind, str(key)))
        self.rows[self.guild, kind, str(key)] = copy.deepcopy(doc)

    async def remove(self, kind, key):
        if kind in {"config", "event"}:
            self.changed_reply_scopes.add((kind, str(key)))
        self.rows.pop((self.guild, kind, str(key)), None)

    async def scan(
        self, kind, *, filters=None, limit=100, offset=0, all_guilds=False, oldest=False,
        history_access=None,
    ):
        def matches(row):
            if history_access is not None and (
                (row.get("nsfw") and not history_access.allow_nsfw)
                or any(
                    participant and participant != history_access.owner_id and self.blocked(participant)
                    for participant in (row.get("subject"), row.get("actor"))
                )
            ):
                return False
            for key, op, val in filters or ():
                got = row.get(
                    "kind" if key == "event" else key,
                    "" if key in {"state", "category", "event"} else 0,
                )
                import operator

                if not {
                    "=": operator.eq,
                    ">": operator.gt,
                    ">=": operator.ge,
                    "<": operator.lt,
                    "<=": operator.le,
                    "in": lambda a, b: a in b,
                    "not in": lambda a, b: a not in b,
                }[op](got, val):
                    return False
            return True

        values = [
            copy.deepcopy(v)
            for (g, k, _), v in self.rows.items()
            if k == kind and (all_guilds or g == self.guild) and matches(v)
        ]
        values.sort(key=lambda r: (r.get("created", 0), str(r.get("id", ""))), reverse=not oldest)
        return values[offset : offset + min(limit, 1000)]

    async def total(self, kind, field=None, state=None):
        rows = [
            v
            for (g, k, _), v in self.rows.items()
            if g == self.guild and k == kind and (state is None or v.get("state", "") == state)
        ]
        return len(rows) if field is None else sum(v.get(field, 0) for v in rows)


class MemoryStore(Store):
    def __init__(self, *, blocked=None):
        self.rows = {}
        self.lock = asyncio.Lock()
        self.blocked = blocked or (lambda _: False)

    @asynccontextmanager
    async def transaction(self, guild):
        async with self.lock:
            rows = copy.deepcopy(self.rows)
            tx = MemoryTransaction(rows, guild, self.blocked)
            yield tx
            await self.revoke_stale_replies(tx)
            self.rows = rows

    async def close(self):
        pass
