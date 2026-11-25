from __future__ import annotations

import asyncio
import hashlib
import re
import time
from datetime import UTC, datetime
from urllib.parse import unquote, urlsplit, urlunsplit

import discord
import msgspec

from lumi.contracts import encode
from lumi.contracts.models import EMBED_SOURCE_LIMIT, OWNED_FOOTER_WINDOW_MS, OWNED_HISTORY_PAGE
from lumi.contracts.wire import MAX_ENVELOPE_BYTES

from .admission import admitted, declared_scope
from .effect_grants import snowflake

READ = "read_owned_history"


class OwnedHistory:
    def __init__(self, edge, *, clock=None):
        self.edge = edge
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    async def execute(self, action, job, guild):
        result = self.edge._action_result
        if (
            action.kind != READ
            or (action.job_id, action.module, action.group, action.generation)
            != (job.job_id, job.module, job.group, job.generation)
            or action.guild_id != job.guild_id
            or action.guild_id != guild.id
            or not snowflake(action.channel_id)
        ):
            return result(action, False, "failed", "action_scope_mismatch")
        snapshot = self.edge.registry.snapshot
        dispatch = job.request.event.get("dispatch")
        from_admin = dispatch == "component"
        route = None
        if from_admin:
            allowed = declared_scope(snapshot, job, "own_history_footers") is True and admitted(job)
        elif dispatch == "scheduled":
            route = snapshot.scheduled.get((job.module, job.request.route))
            allowed = (
                route is not None
                and route.spec.own_history
                and route.spec.scope == "guild"
                and route.spec.handler == job.request.handler
                and job.actor_id == 0
                and (route.group, route.generation) == (job.group, job.generation)
                and action.channel_id == job.channel_id
            )
        else:
            allowed = False
        if not allowed:
            return result(action, False, "failed", "action_scope_mismatch")
        query = action.own_history
        now = self.clock()
        if (
            query is None
            or not 0 < query.after_ms <= now
            or not 1 <= query.limit <= OWNED_HISTORY_PAGE
            or (query.cursor is not None and not 0 < query.cursor < 2**64)
            or (query.reply_to is not None and not 0 < query.reply_to < 2**64)
            or sum(
                item is not None
                for item in (
                    query.reply_to, query.attachment_name, query.component_id,
                    query.nonce, query.footer_sha256,
                )
            )
            != 1
            or from_admin != (query.footer_sha256 is not None)
            or (
                query.footer_sha256 is not None
                and (
                    not re.fullmatch(r"[a-f0-9]{64}", query.footer_sha256)
                    or query.after_ms < job.created_at_ms - OWNED_FOOTER_WINDOW_MS
                )
            )
            or (query.attachment_name is not None and not 1 <= len(query.attachment_name) <= 256)
            or (query.component_id is not None and not 1 <= len(query.component_id) <= 100)
            or (query.nonce is not None and (
                query.cursor is not None or not re.fullmatch(r"[A-Za-z0-9:_-]{1,25}", query.nonce)
            ))
        ):
            return result(action, False, "failed", "invalid_history_query")
        after = datetime.fromtimestamp(query.after_ms / 1000, UTC)
        if query.cursor is not None and query.cursor <= discord.utils.time_snowflake(after):
            return result(action, False, "failed", "invalid_history_cursor")
        deadline = min(job.expires_at_ms, action.expires_at_ms)
        if deadline <= now:
            return result(action, False, "failed", "expired_job")
        channel = await self.edge._action_channel(guild, action.channel_id)
        if (
            channel is None or self.edge.user is None or channel.id != action.channel_id
            or (from_admin and getattr(getattr(channel, "guild", None), "id", None) != guild.id)
        ):
            return result(action, False, "failed", "channel_unavailable")
        permissions = channel.permissions_for(guild.me) if guild.me else discord.Permissions.none()
        if not permissions.view_channel or not permissions.read_message_history:
            return result(action, False, "failed", "permission_denied")
        if route and route.spec.own_history_private and (
            not isinstance(channel, discord.TextChannel)
            or channel.permissions_for(guild.default_role).view_channel
        ):
            return result(action, False, "failed", "permission_denied")
        if route and not all(
            getattr(permissions, flag) for flag in route.spec.own_history_bot_permissions
        ):
            return result(action, False, "failed", "permission_denied")

        page_bytes = MAX_ENVELOPE_BYTES - len(encode(result(
            action, True, "observed", data={
                "channel_id": channel.id, "cursor": 2**64 - 1, "scanned": query.limit,
                "complete": False, "messages": [],
            },
        )))
        matches, used, scanned = [], 0, 0
        cursor = query.cursor
        truncated = False
        try:
            async with asyncio.timeout(max(0, (deadline - self.clock()) / 1000)):
                async for message in channel.history(
                    after=discord.Object(id=query.cursor) if query.cursor else after,
                    limit=query.limit,
                    oldest_first=query.nonce is None,
                ):
                    if message.author.id == self.edge.user.id and message.webhook_id is None:
                        row = self.snapshot(action, message, query)
                        if row is not None:
                            size = len(msgspec.json.encode(row)) + 1
                            if used + size > page_bytes:
                                truncated = True
                                break
                            matches.append(row)
                            used += size
                    cursor = message.id
                    scanned += 1
        except discord.NotFound:
            return result(action, False, "failed", "channel_not_found")
        except (discord.HTTPException, TimeoutError, OSError):
            return result(action, False, "failed", "history_unavailable")
        if truncated and not scanned:
            return result(action, False, "failed", "history_page_too_large")
        return result(
            action,
            True,
            "observed",
            data={
                "channel_id": channel.id,
                "cursor": cursor,
                "scanned": scanned,
                "complete": not truncated and scanned < query.limit,
                "messages": matches,
            },
        )

    def snapshot(self, action, message, query):
        if query.footer_sha256 is not None:
            for embed in message.embeds[:EMBED_SOURCE_LIMIT]:
                footer = getattr(embed.footer, "text", None)
                digest = hashlib.sha256(footer.encode("utf-8")).hexdigest() if footer else None
                if digest == query.footer_sha256:
                    return {"id": message.id}
            return None
        if query.nonce is not None:
            nonce = getattr(message, "nonce", None)
            return {"id": message.id} if nonce is not None and str(nonce) == query.nonce else None
        if query.reply_to is not None:
            reference = getattr(message, "reference", None)
            if (
                getattr(reference, "message_id", None) != query.reply_to
                or getattr(reference, "channel_id", action.channel_id) != action.channel_id
                or getattr(reference, "guild_id", action.guild_id) not in (None, action.guild_id)
            ):
                return None
            return {
                "id": message.id,
                "content_sha256": hashlib.sha256(message.content.encode()).hexdigest(),
            }
        family = (
            "v2" if getattr(getattr(message, "flags", None), "components_v2", False) else "legacy"
        )
        row = {"id": message.id, "family": family}
        if query.component_id is not None:
            components = [
                component.to_dict() if hasattr(component, "to_dict") else component
                for component in getattr(message, "components", ())
            ]
            if query.component_id in self.edge._component_custom_ids(components):
                return {**row, "component_id": query.component_id}
            return None
        name = query.attachment_name
        attachments = [
            {"filename": name, "url": path, "size": item.size}
            for item in getattr(message, "attachments", ())[:10]
            if item.filename == name and (path := self.asset_path(item.url, name))
        ]
        embeds = [
            path
            for embed in getattr(message, "embeds", ())[:10]
            if (path := self.asset_path(embed.image.url, name))
        ]
        return (
            {**row, "attachments": attachments, "embed_images": embeds}
            if attachments or embeds
            else None
        )

    @staticmethod
    def asset_path(url, filename):
        if not url:
            return None
        try:
            parsed = urlsplit(url)
            if (
                parsed.scheme == "https"
                and parsed.hostname in {"cdn.discordapp.com", "media.discordapp.net"}
                and parsed.port in (None, 443)
                and parsed.username is None
                and parsed.password is None
                and unquote(parsed.path.rpartition("/")[2]) == filename
                and len(parsed.path) <= 4096
            ):
                return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        except ValueError:
            pass
        return None
