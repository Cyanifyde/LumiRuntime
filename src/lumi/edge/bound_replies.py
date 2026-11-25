from __future__ import annotations

import asyncio
import time
from weakref import WeakValueDictionary

import discord
import msgspec
from redis.exceptions import RedisError

from lumi.shared.quiescence import tracked_read
from lumi.shared.subject_references import references

from .admission import admitted
from .message_fetches import UNKNOWN_MESSAGE, known_message

ACTION = "bind_message_reply"
PREFIX = "lumi:bound-reply:"
MAX_TTL_MS = 900_000


def route_for(edge, module, group, route):
    entry = edge.registry.snapshot.modules.get(module)
    if entry is not None and entry.manifest.group == group:
        return next(
            (
                spec
                for spec in entry.manifest.passive
                if spec.bound_human_reply and spec.route == route
            ),
            None,
        )


def now_ms():
    return int(time.time() * 1000)


class BoundReplies:
    def __init__(self, edge):
        self.edge = edge
        self.redis = edge.queue_redis
        self.bindings = {}
        self.locks = WeakValueDictionary()

    @tracked_read
    async def load(self):
        async for key in self.redis.scan_iter(match=f"{PREFIX}*", count=128):
            if raw := await self.redis.get(key):
                binding = msgspec.json.decode(raw)
                if not binding.get("closed") and binding["expires_at_ms"] > now_ms():
                    self.bindings[binding["prompt_id"]] = binding

    def forget(self, subject):
        self.bindings = {
            key: value for key, value in self.bindings.items() if not references(value, subject)
        }

    def prune(self):
        now = now_ms()
        for key, binding in tuple(self.bindings.items()):
            if binding["expires_at_ms"] <= now:
                self.bindings.pop(key, None)

    def known_output(self, guild_id, channel_id, identifier):
        binding = self.bindings.get(identifier)
        return (
            binding is not None
            and binding["expires_at_ms"] > now_ms()
            and (binding["guild_id"], binding["channel_id"]) == (guild_id, channel_id)
        )

    async def deleted(self, guild_id, channel_id, identifier):
        binding = self.bindings.get(identifier)
        if binding and (binding["guild_id"], binding["channel_id"]) == (guild_id, channel_id):
            async with self.locks.setdefault(identifier, asyncio.Lock()):
                self.bindings.pop(identifier, None)
                await self.redis.delete(f"{PREFIX}{identifier}")

    def matches(self, binding, guild, channel, actor):
        return (
            binding is not None
            and not binding.get("closed")
            and binding["expires_at_ms"] > now_ms()
            and (binding["guild_id"], binding["channel_id"], binding["owner_id"])
            == (guild, channel, actor)
        )

    def permitted(self, spec, module, member, channel):
        if member.bot:
            return False
        roles, admin = self.edge._member_access(member)
        channels, categories = self.edge._channel_access(channel)
        return self.edge.policy.allowed(
            guild_id=channel.guild.id,
            module=module,
            acl_key=spec.acl_key,
            public=True,
            role_ids=roles,
            administrator=admin,
            channel_ids=channels,
            category_ids=categories,
        )

    async def enrich(self, message, event):
        reference = getattr(message, "reference", None)
        guild = message.guild
        identifier = getattr(reference, "message_id", None)
        cached = self.bindings.get(identifier)
        if not guild or not cached:
            return False
        channel, actor = message.channel, message.author
        if (
            getattr(reference, "channel_id", channel.id) != channel.id
            or getattr(reference, "guild_id", None) not in (None, guild.id)
            or getattr(reference, "type", discord.MessageReferenceType.default)
            != discord.MessageReferenceType.default
            or not self.matches(cached, guild.id, channel.id, actor.id)
        ):
            return False
        try:
            raw = await self.redis.get(f"{PREFIX}{identifier}")
        except RedisError:
            return False
        current = msgspec.json.decode(raw) if raw else None
        if not self.matches(current, guild.id, channel.id, actor.id):
            self.bindings.pop(identifier, None)
            return False
        spec = route_for(self.edge, current["module"], current["group"], current["route"])
        if spec is None or not self.permitted(spec, current["module"], actor, channel):
            return False
        event["bound_reply"] = current
        if spec.bound_reply_activity_exempt:
            event["activity_exempt"] = True
        return True

    async def execute(self, action, job, guild):
        edge, spec = self.edge, action.bound_reply

        def fail(code):
            return edge._action_result(action, False, "failed", code)

        if (
            spec is None
            or action.channel_id != job.channel_id
            or not job.actor_id
            or not guild.me
            or not isinstance(action.content, str)
            or not action.content.strip()
            or len(action.content) > 2000
        ):
            return fail("invalid_action")
        channel = guild.get_channel_or_thread(job.channel_id or 0)
        if channel is None:
            return fail("channel_unavailable")

        if spec.operation == "open":
            registered = route_for(edge, job.module, job.group, spec.route)
            if (
                registered is None
                or not 0 < spec.ttl_ms <= MAX_TTL_MS
                or job.request.event.get("dispatch") not in {"command", "component", "passive"}
                or job.request.event.get("bound_reply")
                or action.message_id is not None
                or not admitted(job, acl=registered.acl_key, passive=True)
            ):
                return fail("action_scope_mismatch")
            field = (
                "send_messages_in_threads"
                if isinstance(channel, discord.Thread)
                else "send_messages"
            )
            if not getattr(channel.permissions_for(guild.me), field):
                return fail("channel_unavailable")
            mentions = discord.AllowedMentions(
                users=[discord.Object(job.actor_id)] if spec.ping_actor else False,
                roles=False,
                everyone=False,
                replied_user=False,
            )
            sent = await channel.send(action.content, allowed_mentions=mentions)
            binding = dict(
                token=spec.token,
                owner_id=job.actor_id,
                guild_id=guild.id,
                channel_id=channel.id,
                prompt_id=sent.id,
                source_message_id=job.request.event.get("message_id"),
                author_id=spec.source_author_id,
                expires_at_ms=now_ms() + spec.ttl_ms,
                module=job.module,
                group=job.group,
                route=spec.route,
            )
            await self.redis.set(f"{PREFIX}{sent.id}", msgspec.json.encode(binding), px=spec.ttl_ms)
            self.bindings[sent.id] = binding
            return edge._action_result(
                action,
                True,
                "sent",
                data={"message_id": sent.id, "expires_at_ms": binding["expires_at_ms"]},
            )

        binding = job.request.event.get("bound_reply") or {}
        registered = route_for(edge, job.module, job.group, job.request.route)
        if (
            spec.operation not in {"edit", "close"}
            or spec.route
            or spec.ttl_ms
            or spec.ping_actor
            or spec.source_author_id is not None
            or registered is None
            or registered.handler != job.request.handler
            or not admitted(job, acl=registered.acl_key, passive=True)
            or not self.matches(binding, guild.id, channel.id, job.actor_id)
            or (
                binding.get("module"),
                binding.get("group"),
                binding.get("route"),
                binding.get("token"),
            )
            != (job.module, job.group, job.request.route, spec.token)
            or action.message_id not in (None, binding["prompt_id"])
        ):
            return fail("action_scope_mismatch")
        identifier = binding["prompt_id"]
        async with self.locks.setdefault(identifier, asyncio.Lock()):
            if min(binding["expires_at_ms"], job.expires_at_ms, action.expires_at_ms) <= now_ms():
                return fail("expired_binding")
            return await self.update(action, channel, binding, identifier, fail)

    async def update(self, action, channel, binding, identifier, fail):
        edge = self.edge
        spec = action.bound_reply
        key = f"{PREFIX}{identifier}"
        raw = await self.redis.get(key)
        current = msgspec.json.decode(raw) if raw else None
        if current != binding:
            return fail("expired_binding")
        if spec.operation == "close":


            removed = await self.redis.eval(
                "if redis.call('GET',KEYS[1])==ARGV[1] then "
                "return redis.call('DEL',KEYS[1]) end return 0",
                1,
                key,
                raw,
            )
            self.bindings.pop(identifier, None)
            if not removed:
                return fail("expired_binding")
        target = known_message(edge, channel, identifier)
        try:
            await target.edit(
                content=action.content, allowed_mentions=discord.AllowedMentions.none()
            )
        except discord.NotFound as exc:
            if exc.code == UNKNOWN_MESSAGE:
                self.bindings.pop(identifier, None)
                await self.redis.delete(key)
            return fail("message_unavailable")
        return edge._action_result(action, True, "updated")
