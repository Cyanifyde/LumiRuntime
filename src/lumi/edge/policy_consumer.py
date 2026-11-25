from __future__ import annotations

import logging

from redis.asyncio import Redis

from lumi.contracts import GuildPolicySnapshot, decode
from lumi.shared.quiescence import tracked_read
from lumi.contracts.prefix import validate_prefix

from .policy import GuildPolicy, PolicyCache

LOGGER = logging.getLogger(__name__)


class PolicyConsumer:
    STREAM = "lumi:policy:updates"
    CURRENT = "lumi:policy:current"

    def __init__(self, redis: Redis, cache: PolicyCache) -> None:
        self.redis = redis
        self.cache = cache
        self._stopped = False
        self._primed = None

    @tracked_read
    async def reconstruct(self) -> str:
        latest = await self.redis.xrevrange(self.STREAM, count=1)
        cursor = latest[0][0] if latest else b"0-0"
        for payload in await self.redis.hvals(self.CURRENT):
            self._apply(payload)
        return cursor.decode() if isinstance(cursor, bytes) else str(cursor)

    async def prime(self) -> None:
        self._primed = await self.reconstruct()

    async def run(self) -> None:
        cursor, self._primed = self._primed, None
        if cursor is None:
            cursor = await self.reconstruct()
        while not self._stopped:
            cursor = await self._read_once(cursor)

    @tracked_read
    async def _read_once(self, cursor):
        messages = await self.redis.xread({self.STREAM: cursor}, count=64, block=5_000)
        for _, entries in messages:
            for identifier, fields in entries:
                cursor = identifier.decode() if isinstance(identifier, bytes) else str(identifier)
                payload = fields.get(b"policy") or fields.get("policy")
                if payload is not None:
                    self._apply(payload)
        return cursor

    def stop(self) -> None:
        self._stopped = True

    def _apply(self, payload: bytes) -> None:
        try:
            snapshot = decode(payload, GuildPolicySnapshot)
            if snapshot.schema != 2:
                raise ValueError("unsupported policy schema")
            validate_prefix(snapshot.prefix)
            policy = GuildPolicy(
                prefix=snapshot.prefix,
                enabled_modules=frozenset(snapshot.enabled_modules),
                enabled_commands=frozenset(snapshot.enabled_commands),
                module_roles={
                    grant.key: frozenset(grant.role_ids) for grant in snapshot.module_roles
                },
                command_roles={
                    grant.key: frozenset(grant.role_ids) for grant in snapshot.command_roles
                },
                module_channels={
                    grant.key: (
                        frozenset(grant.channel_ids),
                        frozenset(grant.category_ids),
                    )
                    for grant in snapshot.module_channels
                },
                command_channels={
                    grant.key: (
                        frozenset(grant.channel_ids),
                        frozenset(grant.category_ids),
                    )
                    for grant in snapshot.command_channels
                },
            )
            self.cache.upsert(snapshot.guild_id, snapshot.revision, policy)
        except Exception as exc:
            LOGGER.warning("policy_snapshot_rejected", extra={"error": type(exc).__name__})
