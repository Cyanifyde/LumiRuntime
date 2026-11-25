from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import time
import uuid

import discord
import msgspec

from lumi.contracts import decode
from lumi.contracts.reaction_subscriptions import ReactionObservation, ReactionSubscriptions
from lumi.shared.quiescence import tracked_read
from lumi.shared.broker import QueueFull, QueueUnavailable
from lumi.shared.emoji import canonical_emoji
from lumi.shared.reaction_subscriptions import index_keys
from lumi.transport import GROUPS, require_module_owner

from .admission import stamp
from .members import fetch_member
from .message_fetches import fetch_message
from .message_snapshots import capture_message

LOGGER = logging.getLogger(__name__)
_RELEASE = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] or (
    current and string.sub(current, 1, 1) == '{' and cjson.decode(current).activation == ARGV[1]
) then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class PersonalReactions:
    def __init__(self, edge):
        self.edge, self.redis = edge, edge.queue_redis
        self.users = {}
        self.cursors = None
        self.stopped = False

    def apply(self, group, raw):
        try:
            snapshot = decode(raw, ReactionSubscriptions)
            if snapshot.group != group or group not in GROUPS:
                return False
            require_module_owner(snapshot.module, group)
            bindings = {}
            for binding in snapshot.bindings:
                key = canonical_emoji(binding.emoji)
                if not key or not binding.route.startswith(snapshot.module + "."):
                    return False
                routes = bindings.setdefault(key, {})
                if binding.route in routes:
                    return False
                routes[binding.route] = binding.tag
        except (TypeError, ValueError):
            return False
        owner = (group, snapshot.module)
        scopes = self.users.setdefault(snapshot.user_id, {})
        previous = scopes.get(owner)
        if previous is not None and previous[0] >= snapshot.revision:
            return False
        scopes[owner] = snapshot.revision, bindings
        return True

    @tracked_read
    async def load(self):
        cursors = {}
        for group in GROUPS:
            latest, stream = index_keys(group)
            entries = await self.redis.xrevrange(stream, count=1)
            cursors[stream] = entries[0][0] if entries else "0-0"
            for raw in await self.redis.hvals(latest):
                self.apply(group, raw)
        self.cursors = cursors

    @tracked_read
    async def read(self, *, block=2000):
        for stream, entries in await self.redis.xread(self.cursors, count=64, block=block):
            stream = stream.decode() if isinstance(stream, bytes) else stream
            group = stream.rpartition(":")[2]
            for identifier, fields in entries:
                self.cursors[stream] = identifier
                raw = fields.get(b"snapshot") or fields.get("snapshot")
                if raw:
                    self.apply(group, raw)

    async def run(self):
        while not self.stopped:
            try:
                if self.cursors is None:
                    await self.load()
                await self.read()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.cursors = None
                LOGGER.warning("reaction_subscriptions_unavailable")
                await asyncio.sleep(1)

    def stop(self):
        self.stopped = True

    def forget(self, user):
        self.users.pop(user, None)

    def matches(self, user, emoji):
        return {
            (group, module, route): (revision, tag)
            for (group, module), (revision, bindings) in self.users.get(user, {}).items()
            for route, tag in bindings.get(emoji, {}).items()
        }

    def allowed_routes(self, guild, channel, member, matches, snapshot):
        edge = self.edge
        if member.bot:
            return ()
        roles, admin = edge._member_access(member)
        channels, categories = edge._channel_access(channel)
        return tuple(
            (key, matches[key])
            for route in snapshot.passive.get(("reaction_add", None), ())
            if route.spec.personal_reaction
            and (key := (route.group, route.module, route.spec.route)) in matches
            and edge.policy.allowed(
                guild_id=guild.id,
                module=route.module,
                acl_key=route.spec.acl_key,
                public=True,
                role_ids=roles,
                administrator=admin,
                channel_ids=channels,
                category_ids=categories,
            )
        )

    async def reaction(self, event_type, payload):
        received = int(time.time() * 1000)
        emoji = str(payload.emoji)
        canonical = canonical_emoji(emoji)
        matches = self.matches(payload.user_id, canonical)
        if not matches:
            return
        token = hashlib.sha256(canonical.encode()).hexdigest()[:24]
        key = (
            f"lumi:personal-reaction:present:{payload.user_id}:"
            f"{payload.guild_id}:{payload.message_id}:{token}"
        )
        if event_type == "reaction_remove":
            await self.redis.delete(key)
            return
        edge = self.edge
        guild = edge.get_guild(payload.guild_id)
        channel = edge.get_channel(payload.channel_id)
        if (
            guild is None
            or not isinstance(channel, (discord.TextChannel, discord.Thread))
            or channel.guild.id != guild.id
        ):
            return
        member = payload.member
        if member is None:
            with contextlib.suppress(discord.HTTPException):
                member = await fetch_member(edge, guild, payload.user_id)
        if member is None or guild.me is None:
            return
        snapshot = edge.registry.snapshot
        matches = self.allowed_routes(guild, channel, member, matches, snapshot)
        if not matches:
            return
        for person in (member, guild.me):
            permissions = channel.permissions_for(person)
            if not permissions.view_channel or not permissions.read_message_history:
                return
        activation = uuid.uuid4().hex
        if not await self.redis.set(key, activation, nx=True):
            return
        try:
            message = await fetch_message(edge, channel, payload.message_id, cached=True)
            value = capture_message(message)
            if value is None or (value["guild_id"], value["channel_id"], value["message_id"]) != (
                guild.id,
                channel.id,
                payload.message_id,
            ):
                await self.redis.eval(_RELEASE, 1, key, activation)
                return
            ingress = getattr(edge, "private_ingress", None)
            if ingress is not None and ingress.gate.blocks_identity(value["author_id"]):
                await self.redis.eval(_RELEASE, 1, key, activation)
                return


            await self.redis.eval(
                "if redis.call('GET',KEYS[1])==ARGV[1] then "
                "return redis.call('SET',KEYS[1],ARGV[2]) end return 0",
                1,
                key,
                activation,
                msgspec.json.encode(
                    {
                        "activation": activation,
                        "owner_id": payload.user_id,
                        "author_id": value["author_id"],
                        "source_message_id": value["message_id"],
                    }
                ),
            )
            for binding, (revision, tag) in matches:
                event = {
                    "kind": "reaction_add",
                    "actor_id": payload.user_id,
                    "author_id": payload.user_id,
                    "guild_id": guild.id,
                    "channel_id": channel.id,
                    "message_id": message.id,
                    "event_received_at_ms": received,
                    "_reaction_binding": binding,
                    "reaction_observation": msgspec.to_builtins(
                        ReactionObservation(activation, emoji, revision, received, tag, value)
                    ),
                }
                routes = edge.matcher.match_passive(
                    snapshot,
                    event_type="reaction_add",
                    event=event,
                )
                for route in routes:
                    stamp(
                        route, actor_id=payload.user_id, guild_id=guild.id, channel_id=channel.id,
                        snapshot=snapshot,
                    )
                    await edge.broker.submit(
                        route, actor_id=payload.user_id, guild_id=guild.id, channel_id=channel.id
                    )
        except (discord.HTTPException, QueueFull, QueueUnavailable, ValueError):
            await self.redis.eval(_RELEASE, 1, key, activation)
