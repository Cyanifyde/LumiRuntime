from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

import msgspec
from redis.asyncio import Redis

from lumi.shared.emoji import canonical_emoji


from .matcher import MatchedReactionOffer
from .registry import EdgeRouteRegistry

LOGGER = logging.getLogger(__name__)
MAX_ACTIVE_OFFERS = 5_000
MAX_RECORD_BYTES = 16 * 1024


def offer_identity(module: str, key: str, guild_id: int, message_id: int) -> str:
    identity = f"{module}:{key}:{guild_id}:{message_id}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


class ReactionOfferRecord(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    offer_id: str
    module: str
    key: str
    guild_id: int
    channel_id: int
    message_id: int
    emoji: str
    emoji_key: str
    owner_id: int
    claim_if_missing: bool
    remove_unauthorized: bool
    remove_after_dispatch: bool
    expires_at_ms: int
    arguments: dict[str, Any]
    context: dict[str, Any]
    require_owner: bool = False
    prepared_only: bool = False


class ReactionOfferStore:
    EXPIRY_KEY = "lumi:reaction-offers:expiry"
    RECORD_PREFIX = "lumi:reaction-offer:"

    def __init__(
        self,
        redis: Redis,
        registry: EdgeRouteRegistry,
        *,
        on_purge: Callable[[ReactionOfferRecord], Awaitable[None]] | None = None,
    ) -> None:
        self._redis = redis
        self._registry = registry
        self._on_purge = on_purge
        self._by_id: dict[str, ReactionOfferRecord] = {}
        self._by_pair: dict[tuple[int, str], set[str]] = defaultdict(set)
        self._locks: dict[str, asyncio.Lock] = {}
        self._stopped = False

    async def load(self) -> None:
        now_ms = int(time.time() * 1000)
        identifiers = await self._redis.zrangebyscore(
            self.EXPIRY_KEY,
            now_ms + 1,
            "+inf",
            start=0,
            num=MAX_ACTIVE_OFFERS,
        )
        if not identifiers:
            return
        offer_ids = [
            value.decode() if isinstance(value, bytes) else str(value) for value in identifiers
        ]
        payloads = await self._redis.mget(
            [f"{self.RECORD_PREFIX}{offer_id}" for offer_id in offer_ids]
        )
        missing: list[str] = []
        for offer_id, payload in zip(offer_ids, payloads, strict=True):
            if not payload:
                missing.append(offer_id)
                continue
            try:
                record = msgspec.json.decode(payload, type=ReactionOfferRecord, strict=True)
            except Exception:
                missing.append(offer_id)
                continue
            if record.schema != 1 or record.expires_at_ms <= now_ms:
                missing.append(offer_id)
                continue
            self._add_memory(record)
        if missing:
            await self._redis.zrem(self.EXPIRY_KEY, *missing)

    async def run(self) -> None:
        while not self._stopped:
            await asyncio.sleep(30)
            try:
                await self.prune()
            except Exception:
                LOGGER.exception("reaction_offer_prune_failed")

    def stop(self) -> None:
        self._stopped = True

    async def register(
        self,
        matched: MatchedReactionOffer,
        event: dict[str, Any],
        *,
        owner_id: int,
        expires_at_ms: int | None = None,
    ) -> tuple[ReactionOfferRecord, bool, bool]:
        route = matched.registered
        guild_id = int(event.get("guild_id") or 0)
        channel_id = int(event.get("channel_id") or 0)
        message_id = int(event.get("message_id") or 0)
        if min(guild_id, channel_id, message_id) <= 0:
            raise ValueError("reaction offer requires a guild message")
        offer_id = offer_identity(route.module, route.spec.key, guild_id, message_id)
        now_ms = int(time.time() * 1000)
        deadline = now_ms + route.spec.ttl_ms
        if expires_at_ms is not None:
            deadline = min(deadline, expires_at_ms)
        if deadline <= now_ms:
            raise ValueError("reaction offer already expired")
        previous = self._by_id.get(offer_id)
        effective_owner_id = max(0, int(owner_id))
        if route.spec.require_owner and not effective_owner_id:
            raise ValueError("reaction offer requires a resolved owner")
        if effective_owner_id == 0 and previous is not None:


            effective_owner_id = previous.owner_id
        record = ReactionOfferRecord(
            schema=1,
            offer_id=offer_id,
            module=route.module,
            key=route.spec.key,
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message_id,
            emoji=route.spec.emoji,
            emoji_key=canonical_emoji(route.spec.emoji),
            owner_id=effective_owner_id,
            claim_if_missing=route.spec.claim_if_missing,
            require_owner=route.spec.require_owner,
            prepared_only=route.spec.prepared_only,
            remove_unauthorized=route.spec.remove_unauthorized,
            remove_after_dispatch=route.spec.remove_after_dispatch,
            expires_at_ms=deadline,
            arguments=dict(matched.arguments),
            context=dict(matched.context),
        )
        payload = msgspec.json.encode(record)
        if len(payload) > MAX_RECORD_BYTES:
            raise ValueError("reaction offer context is too large")
        if offer_id not in self._by_id and len(self._by_id) >= MAX_ACTIVE_OFFERS:
            raise ValueError("reaction offer capacity is exhausted")
        ttl_ms = max(1, record.expires_at_ms - now_ms)
        pair = (message_id, record.emoji_key)
        needs_reaction = not self._by_pair.get(pair)
        pipeline = self._redis.pipeline(transaction=True)
        pipeline.set(f"{self.RECORD_PREFIX}{offer_id}", payload, px=ttl_ms)
        pipeline.zadd(self.EXPIRY_KEY, {offer_id: record.expires_at_ms})
        await pipeline.execute()
        if previous is not None:
            self._remove_memory(previous)
        self._add_memory(record)
        return record, needs_reaction, previous is None

    def records_for(
        self, message_id: int, emoji: str, event_type: str
    ) -> tuple[ReactionOfferRecord, ...]:
        now_ms = int(time.time() * 1000)
        pair = (message_id, canonical_emoji(emoji))
        records: list[ReactionOfferRecord] = []
        for offer_id in tuple(self._by_pair.get(pair, ())):
            record = self._by_id.get(offer_id)
            if record is None or record.expires_at_ms <= now_ms:
                continue
            route = self._registry.snapshot.reaction_offer_keys.get(
                (record.module, record.key)
            )
            if (
                route is None or route.spec.prepared_only != record.prepared_only
                or event_type not in route.spec.reaction_events
            ):
                continue
            records.append(record)
        return tuple(records)

    async def claim(
        self, offer_id: str, actor_id: int
    ) -> tuple[ReactionOfferRecord | None, bool]:
        lock = self._locks.setdefault(offer_id, asyncio.Lock())
        async with lock:
            record = self._by_id.get(offer_id)
            if record is None or record.expires_at_ms <= int(time.time() * 1000):
                return None, False
            if record.require_owner and not record.owner_id:
                return None, True
            if record.owner_id:
                return (record, False) if record.owner_id == actor_id else (None, True)
            if not record.claim_if_missing:
                return record, False
            updated = msgspec.structs.replace(record, owner_id=actor_id)
            payload = msgspec.json.encode(updated)
            ttl_ms = max(1, updated.expires_at_ms - int(time.time() * 1000))
            saved = await self._redis.set(
                f"{self.RECORD_PREFIX}{offer_id}", payload, px=ttl_ms, xx=True
            )
            if not saved:
                self._remove_memory(record)
                return None, False
            self._remove_memory(record)
            self._add_memory(updated)
            return updated, False

    async def retire(
        self, offer_id: str
    ) -> tuple[ReactionOfferRecord | None, bool]:
        record = self._by_id.get(offer_id)
        if record is None:
            return None, False
        self._remove_memory(record)
        pipeline = self._redis.pipeline(transaction=True)
        pipeline.delete(f"{self.RECORD_PREFIX}{offer_id}")
        pipeline.zrem(self.EXPIRY_KEY, offer_id)
        try:
            await pipeline.execute()
        except Exception:
            LOGGER.exception("reaction_offer_retire_failed", extra={"offer_id": offer_id})
        pair = (record.message_id, record.emoji_key)
        return record, not self._by_pair.get(pair)

    async def remove_message(self, message_id: int) -> None:
        records = [record for record in self._by_id.values() if record.message_id == message_id]
        if not records:
            return
        pipeline = self._redis.pipeline(transaction=True)
        for record in records:
            self._remove_memory(record)
            pipeline.delete(f"{self.RECORD_PREFIX}{record.offer_id}")
            pipeline.zrem(self.EXPIRY_KEY, record.offer_id)
        await pipeline.execute()

    async def prune(self) -> None:
        now_ms = int(time.time() * 1000)
        current = self._registry.snapshot.reaction_offer_keys
        expired_or_removed = [
            record
            for record in self._by_id.values()
            if (
                record.expires_at_ms <= now_ms or (record.module, record.key) not in current
                or current[(record.module, record.key)].spec.prepared_only != record.prepared_only
            )
        ]
        if not expired_or_removed:
            return
        pipeline = self._redis.pipeline(transaction=True)
        remove_reactions: list[ReactionOfferRecord] = []
        for record in expired_or_removed:
            self._remove_memory(record)
            if not self._by_pair.get((record.message_id, record.emoji_key)):
                remove_reactions.append(record)
            pipeline.delete(f"{self.RECORD_PREFIX}{record.offer_id}")
            pipeline.zrem(self.EXPIRY_KEY, record.offer_id)
        await pipeline.execute()
        if self._on_purge is not None:
            for record in remove_reactions:
                try:
                    await self._on_purge(record)
                except Exception:
                    LOGGER.exception(
                        "reaction_offer_cleanup_failed",
                        extra={"offer_id": record.offer_id},
                    )

    def _add_memory(self, record: ReactionOfferRecord) -> None:
        self._by_id[record.offer_id] = record
        self._by_pair[(record.message_id, record.emoji_key)].add(record.offer_id)

    def _remove_memory(self, record: ReactionOfferRecord) -> None:
        self._by_id.pop(record.offer_id, None)
        pair = (record.message_id, record.emoji_key)
        identifiers = self._by_pair.get(pair)
        if identifiers is None:
            return
        identifiers.discard(record.offer_id)
        if not identifiers:
            self._by_pair.pop(pair, None)
