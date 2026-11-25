from lumi.contracts import encode
from lumi.shared.discord_effects import EFFECT_GRANT_SECONDS, payload_digest
from lumi.shared.reply_guards import ReplyGuard, guard_key, revoke_versions, snapshot_versions

from .models import config


class ReplySnapshot:


    def __init__(self, replies):
        self.replies, self.scope, self.versions = replies, None, ()

    async def capture(self, guild, revision, records):
        self.scope = guild, revision, frozenset(row["id"] for row in records)
        self.versions = await self.replies.versions(guild, records)


class Replies:
    def __init__(self, store, redis, clock, generation):
        self.store, self.redis, self.clock, self.generation = store, redis, clock, generation
        store.revoke_reply_scopes = self.revoke

    async def revoke(self, guild, scopes):
        await revoke_versions(self.redis, "security", "modlog", guild, scopes)

    async def versions(self, guild, records):
        return await snapshot_versions(
            self.redis, "security", "modlog", guild,
            (("config", "current"), *(("event", row["id"]) for row in records)),
            EFFECT_GRANT_SECONDS * 1000,
        )

    async def approve(
        self, request, job_id, intent, *, config_revision=None, private_target=0,
        participants=(), records=(), session_expires=None, require_admin=False, snapshot=None,
    ):
        now = self.clock()
        event = request.event
        actor, guild, channel = (event.get(key) for key in ("actor_id", "guild_id", "channel_id"))
        if any(
            type(identifier) is not int or not 0 < identifier < 2**64
            for identifier in (actor, guild, channel)
        ):
            raise ValueError("Missing reply scope.")
        ttl = EFFECT_GRANT_SECONDS * 1000
        deadline = min(now + ttl, event.get("_expires_at_ms", 0), *(r["expires"] for r in records))
        if session_expires is not None:
            deadline = min(deadline, session_expires)
        if deadline <= now:
            raise ValueError("Reply expired.")

        async def publish(versions=()):
            grant = ReplyGuard(
                1, "modlog", self.generation, actor, guild, channel, deadline,
                payload_digest(intent, ()),
                private_target=private_target,
                require_nsfw=any(row.get("nsfw") for row in records),
                require_admin=require_admin,
                capture_revision=(config_revision or 0) if require_admin else 0,
                participants=tuple(sorted(set(participants) | {
                    uid for row in records for uid in (row.get("subject"), row.get("actor")) if uid
                })),
                versions=versions,
            )
            remaining = deadline - self.clock()
            if remaining <= 0 or not await self.redis.set(
                guard_key("security", job_id), encode(grant), px=remaining, nx=True,
            ):
                raise ValueError("Reply unavailable.")

        if config_revision is None:
            if records or intent.attachments:
                raise ValueError("Missing evidence configuration.")
            await publish()
            return
        if snapshot is not None and snapshot.scope is not None:
            if snapshot.scope != (guild, config_revision, frozenset(row["id"] for row in records)):
                raise ValueError("Reply evidence binding changed.")
            await publish(snapshot.versions)
            return
        async with self.store.transaction(guild) as tx:
            settings = await tx.get("config", "current") or config(guild)
            if settings["revision"] != config_revision:
                raise ValueError("Reply configuration changed.")
            if records or intent.attachments:
                raise ValueError("Evidence has no transactional witness.")
            await publish(await self.versions(guild, ()))
