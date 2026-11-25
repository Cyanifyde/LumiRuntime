from __future__ import annotations

import asyncio
import hashlib
import json
import time
import weakref

import discord
import msgspec

from lumi.contracts import JobEnvelope, PreparedReplyOffer, ResultEnvelope

from .matcher import MatchedReactionOffer
from .members import fetch_member
from .reaction_offers import offer_identity
from .registry import RegisteredReactionOffer


MARKER = "_prepared_reply"
_ADMIT = """
local old=redis.call('GET',KEYS[1])
local incoming=cjson.decode(ARGV[1])
local ttl=tonumber(ARGV[2])
if old then
 local v=cjson.decode(old)
 if v.author~=incoming.author or v.channel~=incoming.channel then return 0 end
 if v.phase=='claimed' or v.phase=='sending' then return 0 end
 if v.hash==incoming.hash and v.phase~='invalid' then return 0 end
 if v.revision>incoming.revision then return 0 end
 ttl=redis.call('PTTL',KEYS[1])
 if ttl<=0 then return 0 end
end
local cooldown=tonumber(ARGV[3])
if cooldown>0 and not redis.call('SET',KEYS[2],'1','NX','PX',cooldown) then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'PX',ttl)
local watched=redis.call('GET',KEYS[3])
local remaining=redis.call('PTTL',KEYS[3])
if not watched or (watched==incoming.author and remaining>=0 and remaining<ttl) then
 redis.call('SET',KEYS[3],incoming.author,'PX',ttl)
end
return 1
"""
_INVALIDATE = """
local raw=redis.call('GET',KEYS[1])
if not raw then return 0 end
local v=cjson.decode(raw)
if v.channel~=ARGV[1] or v.phase=='claimed' or v.phase=='sending' then return 0 end
if ARGV[2]~='' then
 if ARGV[3]~='' and v.revision>tonumber(ARGV[3]) then return 0 end
 if v.hash==ARGV[2] then return 0 end
 v.hash=ARGV[2]
 if ARGV[3]~='' then v.revision=tonumber(ARGV[3]) end
 v.phase='invalid'
else
 v.hash=''
 v.phase='sending'
end
redis.call('SET',KEYS[1],cjson.encode(v),'KEEPTTL')
return 1
"""
_PHASE = """
local raw=redis.call('GET',KEYS[1])
if not raw then return 0 end
local v=cjson.decode(raw)
if v.hash~=ARGV[1] or v.phase~=ARGV[2] or v.revision~=tonumber(ARGV[5])
   or v.channel~=ARGV[6] or v.author~=ARGV[7] then return 0 end
v.phase=ARGV[3]
if ARGV[4]~='' then v.actor=ARGV[4] end
redis.call('SET',KEYS[1],cjson.encode(v),'KEEPTTL')
return 1
"""


def source_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class PreparedReplies:
    def __init__(self, edge) -> None:
        self.edge = edge
        self.redis = edge.cache_redis
        self._locks = weakref.WeakValueDictionary()

    @staticmethod
    def key(route: RegisteredReactionOffer, guild_id: int, message_id: int) -> str:
        return f"lumi:prepared-reply:source:{route.group}:{route.module}:{route.spec.key}:{guild_id}:{message_id}"

    @staticmethod
    def cooldown_key(route: RegisteredReactionOffer, guild_id: int, author_id: int) -> str:
        return f"lumi:prepared-reply:rate:{route.group}:{route.module}:{route.spec.key}:{guild_id}:{author_id}"

    def lock(self, message_id: int) -> asyncio.Lock:
        return self._locks.setdefault(message_id, asyncio.Lock())

    async def state(self, route, guild_id: int, message_id: int) -> dict:
        raw = await self.redis.get(self.key(route, guild_id, message_id))
        return json.loads(raw) if raw else {}

    def source_route(self, work) -> RegisteredReactionOffer | None:
        marker = work.request.event.get(MARKER)
        if not isinstance(marker, str) or work.request.event.get("dispatch") != "passive":
            return None
        snapshot = self.edge.registry.snapshot
        route = snapshot.reaction_offer_keys.get((work.module, marker))
        module = snapshot.modules.get(work.module)
        if (
            route is None or module is None or not route.spec.prepared_only
            or (route.group, route.generation) != (work.group, work.generation)
        ):
            return None
        if not any(
            spec.prepare_reply == marker and spec.route == work.request.route
            and spec.handler == work.request.handler and spec.event == work.request.event.get("kind")
            for spec in module.manifest.passive
        ):
            return None
        return route

    @staticmethod
    def owns_callback(snapshot, job: JobEnvelope) -> bool:
        if MARKER in job.request.event:
            return True
        if job.request.event.get("dispatch") != "reaction_offer":
            return False
        prefix = f"{job.module}."
        if not job.request.route.startswith(prefix):
            return False
        route = snapshot.reaction_offer_keys.get((job.module, job.request.route[len(prefix):]))

        return bool(route and route.spec.prepared_only)

    def allowed(self, route, actor_id, channel, member=None, role_ids=()) -> bool:
        if not actor_id or self.edge.blocklist.contains(actor_id) or channel is None:
            return False
        guild = getattr(channel, "guild", None)
        if guild is None:
            return False
        member = member or guild.get_member(actor_id)
        if member is None and not role_ids:
            return False
        if member is not None and (bool(member.bot) or not hasattr(member, "roles")):
            return False
        roles = frozenset(r.id for r in member.roles) if member is not None else frozenset(role_ids)
        administrator = bool(member and member.guild_permissions.administrator)
        channels, categories = self.edge._channel_access(channel)
        return self.edge.policy.allowed(
            guild_id=guild.id,
            module=route.module,
            acl_key="",
            public=True,
            role_ids=roles,
            administrator=administrator,
            channel_ids=channels,
            category_ids=categories,
        )

    async def admit(self, work, event) -> bool:
        route = self.source_route(work)
        if route is None:
            return False
        guild_id, channel_id, message_id, actor = (
            int(event.get(k) or 0) for k in ("guild_id", "channel_id", "message_id", "author_id")
        )
        if min(guild_id, channel_id, message_id, actor) <= 0 or not isinstance(event.get("content"), str):
            return False
        if event.get("webhook_id") is not None or event.get("author_bot") is True:
            return False
        channel = self.edge.get_channel(channel_id)
        guild = getattr(channel, "guild", None)
        if guild is None or guild.id != guild_id:
            return False
        member = guild.get_member(actor)
        if event.get("author_bot") is not False and (member is None or member.bot):
            return False
        if not self.allowed(route, actor, channel, member, event.get("author_role_ids", ())):
            return False
        state = dict(
            hash=source_hash(event["content"]),
            revision=int(event.get("source_revision_ms") or 0),
            author=str(actor),
            channel=str(channel_id),
            phase="preparing",
        )
        async with self.lock(message_id):
            if not await self.redis.eval(
                _ADMIT, 3, self.key(route, guild_id, message_id), self.cooldown_key(route, guild_id, actor),
                f"lumi:passive-watch:{message_id}",
                json.dumps(state), route.spec.ttl_ms, route.spec.preparation_cooldown_ms,
            ):
                return False
            self.edge._remember_passive_watch(message_id, actor, route.spec.ttl_ms)
        return True

    async def changed(self, payload, *, revision_ms: int | None = None) -> None:
        if payload.guild_id and isinstance(payload.data, dict) and isinstance(payload.data.get("content"), str):
            await self._invalidate(
                payload.guild_id, payload.channel_id, payload.message_id,
                source_hash(payload.data["content"]), revision_ms,
            )

    async def deleted(self, guild_id: int, channel_id: int, message_id: int) -> None:
        if guild_id:
            await self._invalidate(guild_id, channel_id, message_id, "")

    async def _invalidate(
        self, guild_id: int, channel_id: int, message_id: int, fingerprint: str,
        revision_ms: int | None = None,
    ) -> None:
        routes = [
            route for route in self.edge.registry.snapshot.reaction_offer_keys.values()
            if route.spec.prepared_only
        ]
        if not routes:
            return
        async with self.lock(message_id):
            async with self.redis.pipeline(transaction=False) as pipe:
                for route in routes:
                    pipe.eval(
                        _INVALIDATE, 1, self.key(route, guild_id, message_id), str(channel_id),
                        fingerprint, str(revision_ms) if revision_ms is not None else "",
                    )
                invalidated = await pipe.execute()
            if fingerprint:
                for route, changed in zip(routes, invalidated, strict=True):
                    if changed:
                        await self.retire(route, guild_id, message_id)

    async def retire(self, route, guild_id: int, message_id: int) -> None:
        identity = offer_identity(route.module, route.spec.key, guild_id, message_id)
        removed, needs_remove = await self.edge.reaction_offers.retire(identity)
        if removed is not None and needs_remove:
            await self.edge._remove_offer_reaction(removed)

    async def register(self, job: JobEnvelope, result: ResultEnvelope) -> None:
        offer = result.prepared_reply_offer
        route = self.source_route(job)
        if (
            offer is None or route is None or result.intents or result.status != "ok"
            or (result.job_id, result.module, result.group, result.generation)
            != (job.job_id, job.module, job.group, job.generation)
            or job.expires_at_ms <= int(time.time() * 1_000)
        ):
            return
        raw = job.request.event.get("content")
        if (
            not isinstance(raw, str) or offer.source_hash != source_hash(raw)
            or offer.source_revision_ms != int(job.request.event.get("source_revision_ms") or 0)
            or offer.reply.kind != "send" or offer.reply.mention_actor or offer.reply.attachment_ref
            or offer.reply.attachments
        ):
            return
        guild_id, channel_id = job.guild_id, job.channel_id
        source_id = int(job.request.event.get("message_id") or 0)
        if not guild_id or not channel_id or source_id <= 0:
            return
        async with self.lock(source_id):
            channel = self.edge.get_channel(channel_id)
            if not isinstance(channel, (discord.TextChannel, discord.Thread)) or channel.guild.id != guild_id:
                return
            state = await self.state(route, guild_id, source_id)
            if (
                state.get("phase") != "preparing" or state.get("hash") != offer.source_hash
                or state.get("revision") != offer.source_revision_ms
                or state.get("author") != str(job.actor_id) or state.get("channel") != str(channel_id)
            ):
                return
            now_ms = int(time.time() * 1_000)
            remaining = await self.redis.pttl(self.key(route, guild_id, source_id))
            if remaining <= 0 or job.expires_at_ms <= now_ms:
                return
            matched = MatchedReactionOffer(
                route, msgspec.json.decode(msgspec.json.encode(offer)),
                dict(guild_id=guild_id, channel_id=channel_id, message_id=source_id, author_id=job.actor_id),
            )
            try:
                _, needs_reaction, _ = await self.edge.reaction_offers.register(
                    matched, matched.context, owner_id=0, expires_at_ms=now_ms + remaining,
                )
                if not await self.transition(route, guild_id, source_id, channel_id, offer, state, "preparing", "offered"):
                    await self.retire(route, guild_id, source_id)
                    return
                if needs_reaction:
                    await channel.get_partial_message(source_id).add_reaction(route.spec.emoji)
            except (discord.HTTPException, OSError, ValueError):
                await self.retire(route, guild_id, source_id)

    async def transition(self, route, guild_id, source_id, channel_id, offer, state, before, after, actor=""):
        return await self.redis.eval(
            _PHASE, 1, self.key(route, guild_id, source_id), offer.source_hash, before, after,
            str(actor), offer.source_revision_ms, str(channel_id), state.get("author", ""),
        )

    async def claim(self, route, offered, actor_id: int, *, member=None) -> bool:
        try:
            offer = msgspec.convert(offered.arguments, type=PreparedReplyOffer, strict=True)
        except ValueError:
            return False
        channel = self.edge.get_channel(offered.channel_id)
        if not isinstance(channel, (discord.TextChannel, discord.Thread)) or channel.guild.id != offered.guild_id:
            return False
        if member is None or member.id != actor_id:
            member = channel.guild.get_member(actor_id)
            if member is None:
                try:
                    member = await fetch_member(self.edge, channel.guild, actor_id)
                except (discord.HTTPException, AttributeError):
                    return False
        if not self.allowed(route, actor_id, channel, member):
            return False
        state = await self.state(route, offered.guild_id, offered.message_id)
        if not state or self.edge.blocklist.contains(int(state["author"])):
            return False
        return bool(await self.transition(
            route, offered.guild_id, offered.message_id, offered.channel_id, offer, state, "offered", "claimed", actor_id,
        ))

    async def final_allowed(self, job: JobEnvelope, result: ResultEnvelope) -> bool:
        key = job.request.event.get(MARKER)
        if not isinstance(key, str) or job.request.event.get("dispatch") != "reaction_offer":
            return False
        route = self.edge.registry.snapshot.reaction_offer_keys.get((job.module, key))
        if (
            route is None or not route.spec.prepared_only or (route.group, route.generation) != (job.group, job.generation)
            or job.request.route != f"{job.module}.{key}" or job.request.handler != route.spec.callback
            or job.expires_at_ms <= int(time.time() * 1_000)
        ):
            return False
        try:
            offer = msgspec.convert(job.request.arguments, type=PreparedReplyOffer, strict=True)
        except ValueError:
            return False
        if result.intents != (offer.reply,) or not job.guild_id or not job.channel_id:
            return False
        source_id = int(job.request.event.get("message_id") or 0)
        if job.request.event.get("offer_id") != offer_identity(job.module, key, job.guild_id, source_id):
            return False
        channel = self.edge.get_channel(job.channel_id)
        if not isinstance(channel, (discord.TextChannel, discord.Thread)) or channel.guild.id != job.guild_id:
            return False
        state = await self.state(route, job.guild_id, source_id)
        if (
            not state or state.get("actor") != str(job.actor_id)
            or self.edge.blocklist.contains(int(state["author"]))
        ):
            return False
        return bool(await self.transition(
            route, job.guild_id, source_id, job.channel_id, offer, state, "claimed", "sending", job.actor_id,
        ))
