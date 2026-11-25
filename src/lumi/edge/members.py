from __future__ import annotations

import asyncio
import time
import weakref
from collections import OrderedDict
from contextlib import contextmanager
from types import SimpleNamespace

import discord

from lumi.shared.request_scope import memo_read

MEMBER_EVENTS = frozenset(
    {
        "MESSAGE_CREATE",
        "MESSAGE_REACTION_ADD",
        "INTERACTION_CREATE",
        "GUILD_MEMBERS_CHUNK",
        "GUILD_MEMBER_ADD",
        "GUILD_MEMBER_REMOVE",
        "GUILD_MEMBER_UPDATE",
        "GUILD_ROLE_CREATE",
        "GUILD_ROLE_UPDATE",
        "GUILD_ROLE_DELETE",
        "GUILD_CREATE",
        "GUILD_UPDATE",
        "GUILD_DELETE",
        "READY",
        "RESUMED",
        "USER_UPDATE",
    }
)
TTL = 5.0
LIMIT = 4096
_DEPARTED = object()


def _unknown_member():

    return discord.NotFound(
        SimpleNamespace(status=404, reason="Unknown Member"),
        {"code": 10007, "message": "Discord reported that this member left"},
    )


class MemberLookup:
    def __init__(self, edge, *, clock=time.monotonic, ttl=TTL, limit=LIMIT):
        self.edge, self.clock, self.ttl, self.limit = edge, clock, ttl, limit
        self.entries = OrderedDict()
        self.epoch = 0
        self.locks = weakref.WeakValueDictionary()
        self.probes = {}
        self.listings = {}
        self.observers = {}

    def prune(self):
        now = self.clock()
        for key, (expires, _) in tuple(self.entries.items()):
            if expires <= now:
                self.entries.pop(key, None)

    def invalidate(self, guild_id=None, user_id=None, *, listings=False, notify_observers=True):
        self.epoch += 1
        if notify_observers:
            for gid, members, changed in self.observers.values():
                if (guild_id is None or gid == guild_id) and (
                    user_id is None or user_id in members
                ):
                    changed[0] = True
        if listings or (guild_id is None and user_id is None):
            snapshots = getattr(self.edge, "_guild_member_snapshots", {})
            if guild_id is None:
                snapshots.clear()
            else:
                snapshots.pop(guild_id, None)
            for gid, probe in self.listings.items():
                if guild_id is None or gid == guild_id:
                    probe[0] = True
        for key, probe in self.probes.items():
            if (guild_id is None or key[0] == guild_id) and (user_id is None or key[1] == user_id):
                probe[0] = True
        if guild_id is None and user_id is None:
            self.entries.clear()
        elif guild_id is not None and user_id is not None:
            self.entries.pop((guild_id, user_id), None)
        else:
            for key in tuple(self.entries):
                if (guild_id is None or key[0] == guild_id) and (
                    user_id is None or key[1] == user_id
                ):
                    self.entries.pop(key, None)

    def remember(self, guild, member, *, epoch=None, gateway=False):
        uid = getattr(member, "id", None)
        if not isinstance(uid, int) or uid <= 0 or (epoch is not None and epoch != self.epoch):
            return
        blocklist = getattr(self.edge, "blocklist", None)
        if blocklist is not None and blocklist.contains(uid):
            self.invalidate(guild.id, uid, notify_observers=False)
            return
        if getattr(getattr(member, "guild", guild), "id", guild.id) != guild.id:
            return


        self._remember(guild.id, uid, member, gateway=gateway)

    def _remember(self, guild_id, uid, value, *, gateway):
        key = (guild_id, uid)
        self.entries[key] = (float("inf") if gateway else self.clock() + self.ttl, value)
        self.entries.move_to_end(key)
        while len(self.entries) > self.limit:
            self.entries.popitem(last=False)

    def _saved(self, guild, uid):
        blocklist = getattr(self.edge, "blocklist", None)
        if blocklist is not None and blocklist.contains(uid):
            self.invalidate(guild.id, uid, notify_observers=False)
            return None
        key = (guild.id, uid)
        row = self.entries.get(key)
        if row and row[0] > self.clock():
            self.entries.move_to_end(key)
            return row[1]
        self.entries.pop(key, None)
        return None

    def cached(self, guild, uid):
        member = self._saved(guild, uid)
        if member is _DEPARTED:
            return None
        blocklist = getattr(self.edge, "blocklist", None)
        if blocklist is not None and blocklist.contains(uid):
            return None
        if member is not None:
            return member
        getter = getattr(guild, "get_member", None)
        member = getter(uid) if getter else None
        if member is not None and getattr(member, "id", None) == uid:
            return member
        return None

    def departed(self, guild, uid):
        return self._saved(guild, uid) is _DEPARTED

    def _known(self, guild, uid, *, fresh):
        if not fresh:
            if self.departed(guild, uid):
                raise _unknown_member()
            return self.cached(guild, uid)
        member = self._saved(guild, uid)
        if member is _DEPARTED:
            raise _unknown_member()


        row = self.entries.get((guild.id, uid))
        return member if row is not None and row[0] == float("inf") else None

    async def fetch(self, guild, uid, *, fresh=False):
        if member := self._known(guild, uid, fresh=fresh):
            return member
        key = (guild.id, uid)
        lock = self.locks.setdefault(key, asyncio.Lock())
        async with lock:
            if member := self._known(guild, uid, fresh=fresh):
                return member
            if fresh:
                self.invalidate(guild.id, uid, notify_observers=False)
            probe = self.probes[key] = [False]
            try:
                member = await guild.fetch_member(uid)
            except discord.NotFound:
                if not probe[0]:
                    self.invalidate(guild.id, uid)
                elif self.departed(guild, uid):
                    raise _unknown_member() from None
                elif updated := self._saved(guild, uid):
                    return updated
                raise
            else:
                if not probe[0]:
                    self.remember(guild, member)
                elif self.departed(guild, uid):
                    raise _unknown_member()
                elif updated := self._saved(guild, uid):
                    return updated
            finally:
                self.probes.pop(key, None)
            return member

    async def query(self, guild, user_ids, *, fresh=False):
        members = {uid: None if fresh else self.cached(guild, uid) for uid in user_ids}
        missing = [uid for uid, member in members.items() if member is None and not self.departed(guild, uid)]
        if missing:
            epoch = self.epoch
            for member in await guild.query_members(user_ids=missing, cache=False):
                if member.id in members and not self.departed(guild, member.id):
                    members[member.id] = member
                    self.remember(guild, member, epoch=epoch)
        return [
            member for uid, member in members.items()
            if member is not None and not self.departed(guild, uid)
        ]

    @contextmanager
    def listing(self, guild_id):

        changed = self.listings[guild_id] = [False]
        try:
            yield changed
        finally:
            self.listings.pop(guild_id, None)

    @contextmanager
    def observe(self, guild_id, members):

        key, changed = object(), [False]
        self.observers[key] = guild_id, frozenset(members), changed
        try:
            yield changed
        finally:
            self.observers.pop(key, None)

    def gateway(self, event, data):


        if event in {"READY", "RESUMED"}:
            self.invalidate()
            return
        if event == "USER_UPDATE":
            self.invalidate(user_id=int(data["id"]), listings=True)
            return
        gid = int(
            data.get("guild_id") or (data.get("id") if event.startswith("GUILD_") else 0) or 0
        )
        if not gid:
            return
        if event in {"GUILD_CREATE", "GUILD_UPDATE", "GUILD_DELETE"} or event.startswith(
            "GUILD_ROLE_"
        ):
            self.invalidate(guild_id=gid, listings=True)
            return
        if event in {"GUILD_MEMBER_UPDATE", "GUILD_MEMBER_REMOVE", "GUILD_MEMBER_ADD"}:
            uid = int(data["user"]["id"])
            self.invalidate(gid, uid, listings=True)
            if event == "GUILD_MEMBER_REMOVE":
                if not getattr(self.edge, "blocklist", None) or not self.edge.blocklist.contains(uid):
                    self._remember(gid, uid, _DEPARTED, gateway=True)
                return
        guild = self.edge.get_guild(gid)
        if guild is None:
            return
        if event == "MESSAGE_CREATE":
            member = {**(data.get("member") or {}), "user": data.get("author") or {}}
            members = (member,)
        elif event in {"INTERACTION_CREATE", "MESSAGE_REACTION_ADD"}:
            members = (data.get("member") or {},)
        elif event == "GUILD_MEMBERS_CHUNK":
            members = data.get("members") or ()
        elif event in {"GUILD_MEMBER_ADD", "GUILD_MEMBER_UPDATE"}:
            members = (data,)
        else:
            return
        for raw in members:
            if "roles" not in raw or not (raw.get("user") or {}).get("id"):
                continue
            uid = int(raw["user"]["id"])
            blocklist = getattr(self.edge, "blocklist", None)
            if blocklist is not None and blocklist.contains(uid):
                self.invalidate(gid, uid)
                continue
            if event == "GUILD_MEMBERS_CHUNK" and self.departed(guild, uid):
                continue
            member = discord.Member(data=raw, guild=guild, state=self.edge._connection)
            self.invalidate(gid, member.id, notify_observers=False)
            self.remember(guild, member, gateway=True)


def lookup(edge):
    value = getattr(edge, "member_cache", None)
    if value is None:
        value = edge.member_cache = MemberLookup(edge)
    return value


async def fetch_member(edge, guild, uid, *, fresh=False):
    cache = lookup(edge)
    if fresh:
        return await cache.fetch(guild, uid, fresh=True)
    return await memo_read(
        "member", (id(edge), guild.id, uid, cache.epoch),
        lambda: cache.fetch(guild, uid),
    )


async def guild_roles(edge, guild):

    roles = getattr(guild, "roles", ())
    if any(role.id == guild.id for role in roles):
        return tuple(roles)
    return await memo_read(
        "guild-roles", (id(edge), guild.id, lookup(edge).epoch), guild.fetch_roles
    )


@contextmanager
def member_mutation(edge, guild, uid):
    lookup(edge).invalidate(guild.id, uid, listings=True)
    try:
        yield
    finally:
        lookup(edge).invalidate(guild.id, uid, listings=True)
