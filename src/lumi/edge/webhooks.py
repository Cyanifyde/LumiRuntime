from __future__ import annotations

import asyncio
import logging
import time
from urllib.parse import urlsplit

import discord
import msgspec
from redis.asyncio import Redis

from lumi.contracts import decode, encode
from lumi.contracts.observation_state import (
    WEBHOOK_TTL as BINDING_TTL,
    WEBHOOK_WIRE_MAX,
    WebhookBinding as WebhookBinding,
    webhook_key,
)

LOGGER = logging.getLogger(__name__)
IDLE_SECONDS = 24 * 60 * 60


class WebhookBindings:


    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def record(self, notice: WebhookBinding) -> None:
        if not notice.valid():
            raise ValueError("invalid webhook binding")
        await self._redis.set(
            webhook_key(notice.guild_id, notice.channel_id, notice.message_id),
            encode(notice),
            ex=BINDING_TTL,
            nx=True,
        )

    async def resolve(
        self,
        guild_id: int,
        channel_id: int,
        message_id: int,
        modules: frozenset[str],
        *,
        webhook_id: int | None = None,
        now_ms: int | None = None,
    ) -> WebhookBinding | None:
        if not modules:
            return None
        now = int(time.time() * 1000) if now_ms is None else now_ms
        raw = await self._redis.get(webhook_key(guild_id, channel_id, message_id))
        if not raw or len(raw) > WEBHOOK_WIRE_MAX:
            return None
        try:
            notice = decode(raw, WebhookBinding)
        except msgspec.DecodeError:
            return None
        if (
            notice.valid()
            and notice.module in modules
            and (notice.guild_id, notice.channel_id, notice.message_id)
            == (guild_id, channel_id, message_id)
            and webhook_id in (None, notice.webhook_id)
            and 0 <= now - notice.created_at_ms < BINDING_TTL * 1000
        ):
            return notice
        return None


def valid_discord_asset_url(value: str) -> bool:
    if len(value) > 2048:
        return False
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme == "https"
            and (parsed.hostname or "").casefold() in {"cdn.discordapp.com", "media.discordapp.net"}
            and parsed.port is None
            and parsed.username is None
            and parsed.password is None
        )
    except ValueError:
        return False


class WebhookDelivery:
    def __init__(self, edge, *, clock=None):
        self.edge = edge
        self.clock = clock or time.monotonic
        self.bindings = WebhookBindings(edge.queue_redis)
        self.hooks = {}
        self.locks = {}

    @staticmethod
    def key(channel, name):
        base = channel.parent if isinstance(channel, discord.Thread) else channel
        return base, (base.id, name) if base is not None else None

    async def hook(self, channel, name):
        base, key = self.key(channel, name)
        if not isinstance(base, discord.TextChannel):
            raise PermissionError("webhooks unavailable")
        async with self.locks.setdefault(key, asyncio.Lock()):
            me = base.guild.me
            permissions = base.permissions_for(me) if me else discord.Permissions.none()
            if not permissions.manage_webhooks or not permissions.send_messages:
                raise PermissionError("webhook permission unavailable")
            now = self.clock()
            cached = self.hooks.get(key)
            if cached and now - cached[1] < IDLE_SECONDS:
                self.hooks[key] = cached[0], now
                return cached[0]
            webhook = next(
                (
                    item
                    for item in await base.webhooks()
                    if item.name == name
                    and item.user is not None
                    and self.edge.user is not None
                    and item.user.id == self.edge.user.id
                ),
                None,
            )
            if webhook is None:
                webhook = await base.create_webhook(name=name, reason="Lumi managed webhook")
            self.hooks[key] = webhook, self.clock()
            return webhook

    async def prune(self):
        cutoff = self.clock() - IDLE_SECONDS
        for key in [key for key, (_, used) in self.hooks.items() if used <= cutoff]:
            async with self.locks.setdefault(key, asyncio.Lock()):
                entry = self.hooks.get(key)
                if entry is None or self.clock() - entry[1] < IDLE_SECONDS:
                    continue
                try:
                    await entry[0].delete(reason="Lumi managed webhook idle")
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    continue
                self.hooks.pop(key, None)

    async def execute(self, action, job, guild):
        reply = self.edge._action_result
        event = job.request.event
        registered = self.edge.registry.snapshot.modules.get(job.module)
        profile = (
            next(
                (
                    spec.recipient_webhook
                    for spec in registered.manifest.passive
                    if spec.route == job.request.route and spec.handler == job.request.handler
                ),
                None,
            )
            if registered
            else None
        )
        bound_reply = event.get("webhook_reply")
        recipient = next(
            (
                item
                for item in event.get("recipient_mentions", ())
                if isinstance(item, dict)
                and item.get("user_id") == action.member_id
                and item.get("kind") == "direct"
            ),
            None,
        )
        if (
            not profile
            or event.get("dispatch") != "passive"
            or (action.module, action.group, action.generation)
            != (job.module, job.group, job.generation)
            or not action.channel_id
            or action.channel_id != job.channel_id
            or action.guild_id != job.guild_id
            or not action.message_id
            or action.message_id != event.get("message_id")
            or not action.member_id
            or action.member_id not in event.get("mentioned_user_ids", ())
            or recipient is None
            or (
                isinstance(bound_reply, dict)
                and bound_reply.get("user_id") == action.member_id
                and bound_reply.get("module") == action.module
            )
        ):
            return reply(action, False, "failed", "action_scope_mismatch")
        if not action.content or len(action.content) > 2000:
            return reply(action, False, "failed", "invalid_notice_payload")
        username = str(recipient.get("display_name") or f"User {action.member_id}")[:80]
        avatar_url = recipient.get("avatar_url") or None
        if action.username not in (None, username) or action.avatar_url not in (None, avatar_url):
            return reply(action, False, "failed", "action_scope_mismatch")
        if avatar_url is not None and not valid_discord_asset_url(avatar_url):
            return reply(action, False, "failed", "invalid_notice_avatar")
        channel = await self.edge._action_channel(guild, action.channel_id)
        if channel is None:
            return reply(action, False, "failed", "channel_unavailable")
        _, key = self.key(channel, profile.name)
        webhook = None
        try:
            webhook = await self.hook(channel, profile.name)
            props = dict(
                content=action.content,
                username=username,
                avatar_url=avatar_url,
                allowed_mentions=discord.AllowedMentions.none(),
                wait=True,
            )
            if isinstance(channel, discord.Thread):
                props["thread"] = channel
            sent = await webhook.send(**props)
            cached = self.hooks.get(key)
            if cached and cached[0].id == webhook.id:
                self.hooks[key] = cached[0], self.clock()
            try:
                await self.bindings.record(
                    WebhookBinding(
                        guild.id,
                        channel.id,
                        sent.id,
                        webhook.id,
                        action.member_id,
                        action.message_id,
                        int(time.time() * 1000),
                        action.module,
                    )
                )
            except Exception:

                LOGGER.warning("webhook_binding_unavailable")
            return reply(
                action, True, "sent", data={"message_id": sent.id, "channel_id": sent.channel.id}
            )
        except discord.NotFound:
            cached = self.hooks.get(key)
            if cached and cached[0] is webhook:
                self.hooks.pop(key, None)
            return reply(action, False, "failed", "permission_denied")
        except (discord.Forbidden, PermissionError):
            return reply(action, False, "failed", "permission_denied")
        except discord.HTTPException as exc:
            return reply(
                action, False, "failed", "ambiguous" if exc.status >= 500 else "discord_rejected"
            )
