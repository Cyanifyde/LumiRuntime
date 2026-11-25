from __future__ import annotations

import asyncio
import json
import re
import time
from weakref import WeakValueDictionary

import discord
from msgspec.structs import replace

from lumi.contracts import TypedRequest

from lumi.shared.broker import QueueFull, QueueUnavailable
from .command_loading import NATIVE_COMMANDS
from .matcher import EdgeMatcher, MatchedRoute
from .members import fetch_member
from .owner_controls import owner_command_prefix

INDEX = "lumi:channel-watch:index"
PREFIX = "lumi:channel-watch:"
TOKEN = re.compile(r"^[a-f0-9]{32}$")
CURSOR = re.compile(r"^\d{1,20}-\d{1,20}$")
MAX_NATIVE_TEXT = 4000
APPEND = """
local raw = redis.call('GET', KEYS[3])
if not raw then return '' end
local binding = cjson.decode(raw)
if binding.closed or tonumber(ARGV[3]) > binding['end'] then return '' end
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then return '' end
if redis.call('XLEN', KEYS[1]) >= 10000 then return 'overflow' end
redis.call('HSET', KEYS[2], ARGV[1], 1)
local id = redis.call('XADD', KEYS[1], '*', 'entry', ARGV[2])
redis.call('EXPIRE', KEYS[1], 86400)
redis.call('EXPIRE', KEYS[2], 86400)
return id
"""
CLOSE_INDEX = """
local value = redis.call('HGET', KEYS[1], ARGV[1])
if value and cjson.decode(value).token == ARGV[2] then
  return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
"""


class ChannelWatches:
    def __init__(self, edge, clock=None):
        self.edge = edge
        self.redis = edge.queue_redis
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.channels = {}
        self.locks = {}
        self.faults = {}
        self.operations = WeakValueDictionary()
        self.matcher = EdgeMatcher()

    @staticmethod
    def field(guild, channel):
        return f"{guild}:{channel}"

    @staticmethod
    def binding(module, token):
        return PREFIX + f"binding:{module}:{token}"

    @staticmethod
    def journal(module, token):
        return PREFIX + f"journal:{module}:{token}"

    async def load(self):
        for field, raw in (await self.redis.hgetall(INDEX)).items():
            try:
                row = json.loads(raw)
                if row["end"] > self.clock() and not row.get("closed"):
                    row.pop("reconcile", None)
                    self.channels[self.field(row["guild"], row["channel"])] = row
                else:
                    await self.redis.hdel(INDEX, field)
            except (ValueError, KeyError, TypeError):
                await self.redis.hdel(INDEX, field)

    def route(self, module, name):
        return next((
            entry for entry in self.edge.registry.snapshot.passive.get(("message_create", None), ())
            if entry.module == module and entry.spec.route == name and entry.spec.channel_scoped
        ), None)

    async def _save(self, row):
        pipeline = self.redis.pipeline(transaction=True)
        pipeline.set(self.binding(row["module"], row["token"]), json.dumps(row), ex=86400)
        field = self.field(row["guild"], row["channel"])
        if row.get("closed"):
            pipeline.eval(CLOSE_INDEX, 1, INDEX, field, row["token"])
        else:
            pipeline.hset(INDEX, field, json.dumps(row))
        try:
            await pipeline.execute()
        except Exception:
            self.faults[row["token"]] = {**row, "closed": True, "error": "storage_unavailable"}
            raise
        if row.get("closed"):
            if self.channels.get(field, {}).get("token") == row["token"]:
                self.channels.pop(field, None)
            self.locks.pop(field, None)
        else:
            self.channels[field] = row

    def allowed(self, row, actor, roles=(), administrator=False, categories=(), *, route=None):
        if self.edge.blocklist.contains(actor):
            return False
        route = self.route(row["module"], row["route"]) if route is None else route
        if route is None:
            return False
        acl = route.spec.acl_key
        return self.edge.policy.allowed(
            guild_id=row["guild"],
            module=row["module"],
            acl_key=acl,
            public=True,
            role_ids=frozenset(roles),
            administrator=administrator,
            channel_ids=frozenset((row["channel"],)),
            category_ids=frozenset(categories),
        )

    async def execute(self, action, job, guild, send):
        if action.channel_watch is None:
            return self.edge._action_result(action, False, "failed", "invalid_channel_watch")
        key = (action.module, action.channel_watch.token)


        lock = self.operations.setdefault(key, asyncio.Lock())
        async with lock:
            return await self._execute(action, job, guild, send)

    async def _execute(self, action, job, guild, send):
        spec = action.channel_watch

        def fail(code):
            return self.edge._action_result(action, False, "failed", code)

        if (
            spec is None
            or not TOKEN.fullmatch(spec.token)
            or not CURSOR.fullmatch(spec.cursor)
            or action.channel_id != job.channel_id
            or action.guild_id != job.guild_id
            or self.route(action.module, spec.route) is None
        ):
            return fail("invalid_channel_watch")
        if spec.operation == "authorize_stop":


            try:
                member = await fetch_member(self.edge, guild, job.actor_id)
            except discord.NotFound:
                return fail("permission_denied")
            channel = self.edge.get_channel(job.channel_id)
            if member is None or channel is None or not channel.permissions_for(member).manage_messages:
                return fail("permission_denied")
            return self.edge._action_result(action, True, "authorized")
        raw = await self.redis.get(self.binding(action.module, spec.token))
        row = json.loads(raw) if raw else None
        if row and spec.token in self.faults:
            row = self.faults.pop(spec.token)
            await self._save(row)
        local = self.channels.get(self.field(job.guild_id, job.channel_id))
        if row and local and local.get("token") == spec.token and local.get("error"):
            row.update(closed=True, error=local["error"])
            await self._save(row)
        if row and (row["guild"], row["channel"], row["route"]) != (
            job.guild_id,
            job.channel_id,
            spec.route,
        ):
            return fail("channel_watch_scope_mismatch")
        if spec.operation == "publish":
            if row:
                return self.edge._action_result(action, bool(row.get("message")), "bound", data=row)
            field = self.field(job.guild_id, job.channel_id)
            existing = self.channels.get(field)
            if existing and existing["end"] > self.clock():
                return fail("channel_already_watched")
            if action.intent is None or action.intent.kind != "send":
                return fail("missing_presentation")
            row = dict(
                module=action.module,
                token=spec.token,
                route=spec.route,
                guild=job.guild_id,
                channel=job.channel_id,
                host=job.actor_id,
                start=0,
                end=self.clock() + spec.duration_ms + 30000,
                message=0,
                closed=False,
                pending=True,
            )
            await self._save(row)
            result = await send(replace(action, kind="send_message", channel_watch=None))
            if not result.ok:
                row.update(closed=True, pending=False, error=result.error_code or "ambiguous")
                await self._save(row)
                return result
            row.update(message=int(result.data["message_id"]), start=self.clock(), pending=False)
            row["end"] = row["start"] + spec.duration_ms
            await self._save(row)
            return self.edge._action_result(action, True, "published", data=row)
        if row is None:
            return fail("channel_watch_missing")
        row.pop("reconcile", None)
        if spec.operation == "edit":
            if (
                action.message_id != row.get("message")
                or action.intent is None
                or action.intent.kind != "edit"
            ):
                return fail("channel_watch_scope_mismatch")
            if row.get("deleted") or row.get("error"):
                return fail("channel_watch_unavailable")
            if (
                spec.revision < row.get("presentation_revision", -1)
                or (row.get("closed") and not spec.resolved)
                or (row.get("presentation_final") and not spec.resolved)
            ):
                return self.edge._action_result(action, True, "stale")

            row.update(presentation_revision=spec.revision, presentation_final=spec.resolved)
            await self._save(row)
            result = await send(replace(action, kind="edit_message", channel_watch=None))
            if result.data.get("deleted") is True:
                row.update(closed=True, deleted=True)
                await self._save(row)
            return result
        if spec.operation == "close":
            row["closed"] = True
            await self._save(row)
        if spec.operation == "forget":
            row["closed"] = True
            await self._save(row)
            await self.redis.delete(
                self.journal(action.module, spec.token),
                self.journal(action.module, spec.token) + ":seen",
            )
        if spec.operation in {"close", "forget"}:
            return self.edge._action_result(action, True, spec.operation, data=row)
        if spec.operation == "verify":
            if action.member_id is None or action.message_id is None:
                return fail("missing_candidate")
            found = await self.redis.xrange(
                self.journal(action.module, spec.token), spec.cursor, spec.cursor, count=1
            )
            if not found:
                return fail("candidate_missing")
            entry = json.loads(found[0][1].get(b"entry") or found[0][1].get("entry"))
            if entry["actor"] != action.member_id or entry["message"] != action.message_id:
                return fail("candidate_mismatch")
            try:
                member = await fetch_member(self.edge, guild, action.member_id)
            except discord.NotFound:
                return fail("candidate_ineligible")
            channel = self.edge.get_channel(row["channel"])
            if channel is None:
                return fail("channel_unavailable")
            permissions = channel.permissions_for(member)
            if not permissions.view_channel or not self.allowed(
                row,
                member.id,
                [r.id for r in member.roles],
                member.guild_permissions.administrator,
                entry.get("categories", ()),
            ):
                return fail("candidate_ineligible")
            return self.edge._action_result(action, True, "eligible")
        entries = []
        for identifier, fields in await self.redis.xrange(
            self.journal(action.module, spec.token), f"({spec.cursor}", "+", count=32
        ):
            entry = json.loads(fields.get(b"entry") or fields.get("entry"))
            eligible = self.allowed(
                row, entry["actor"], entry["roles"], entry["admin"], entry["categories"]
            )
            entries.append(
                {
                    **{k: entry[k] for k in ("actor", "message", "text", "at")},
                    "cursor": identifier.decode() if isinstance(identifier, bytes) else identifier,
                    "eligible": eligible,
                }
            )
        return self.edge._action_result(
            action, True, "read", data={**row, "entries": entries, "has_more": len(entries) == 32}
        )

    async def ingest(self, message):
        if message.guild is None or message.author.bot or message.webhook_id is not None:
            return
        field = self.field(message.guild.id, message.channel.id)
        row = self.channels.get(field)
        if row is None:
            return
        async with self.locks.setdefault(field, asyncio.Lock()):
            if row.get("closed") or self.clock() > row["end"]:
                return
            content = message.content or ""
            if not content.strip() or len(content) > MAX_NATIVE_TEXT:
                return
            route = self.route(row["module"], row["route"])
            if route is None:
                return
            prefix = self.edge.policy.prefix(message.guild.id)
            snapshot = self.edge._command_snapshot(message.guild.id)
            prefix = owner_command_prefix(snapshot, content, prefix) or prefix
            root = (
                content[len(prefix) :].split(maxsplit=1)[0].casefold()
                if content.startswith(prefix) and content[len(prefix) :].strip()
                else ""
            )
            if root in snapshot.commands or root in NATIVE_COMMANDS:
                return
            if not self.matcher.accepts_channel_watch(route, {
                "content": content,
                "author_id": message.author.id,
                "actor_id": message.author.id,
                "guild_id": message.guild.id,
                "channel_id": message.channel.id,
                "message_id": message.id,
                "embeds": bool(getattr(message, "embeds", ())),
                "attachments": bool(getattr(message, "attachments", ())),
                "prefix": prefix,
            }):
                return
            roles = [r.id for r in getattr(message.author, "roles", ())]
            admin = bool(
                getattr(getattr(message.author, "guild_permissions", None), "administrator", False)
            )
            category = getattr(message.channel, "category_id", None)
            categories = [category] if category else []
            if not self.allowed(row, message.author.id, roles, admin, categories, route=route):
                return
            entry = dict(
                actor=message.author.id,
                message=message.id,
                text=content,
                at=self.clock(),
                roles=roles,
                admin=admin,
                categories=categories,
            )
            journal = self.journal(row["module"], row["token"])
            added = await self.redis.eval(
                APPEND,
                3,
                journal,
                journal + ":seen",
                self.binding(row["module"], row["token"]),
                str(message.id),
                json.dumps(entry),
                str(entry["at"]),
            )
            if added in (b"overflow", "overflow"):
                row.update(closed=True, error="journal_full")
                await self._save(row)
            if added:
                await self.notify(row)

    async def notify(self, row):
        registered = self.route(row["module"], row["route"])
        if registered is None:
            return
        spec = registered.spec
        route = MatchedRoute(
            row["module"],
            registered.group,
            registered.generation,
            registered.stream,
            spec.deadline_ms,
            spec.queue_class,
            TypedRequest(
                spec.route,
                spec.handler,
                {"token": row["token"]},
                {
                    "dispatch": "channel",
                    "guild_id": row["guild"],
                    "channel_id": row["channel"],
                    "actor_id": row["host"],
                },
            ),
            presentation_mode=registered.presentation_mode,
            acl_key=spec.acl_key,
            public=True,
        )
        try:
            await self.edge.broker.submit(
                route, actor_id=row["host"], guild_id=row["guild"], channel_id=row["channel"]
            )
        except (QueueFull, QueueUnavailable):
            pass

    async def deleted(self, guild, channel, message):
        row = self.channels.get(self.field(guild, channel))
        if row and row["message"] == message:
            row.update(closed=True, deleted=True)
            await self._save(row)
            await self.notify(row)
