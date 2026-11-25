import asyncio
import time
from contextlib import suppress

import discord
from redis.exceptions import RedisError

from lumi.contracts.layout_message import ATTEMPT, MAX_LAYOUT_COMPONENTS, WRITE_LAYOUT_MESSAGE

from .admission import admitted, declared_scope
from .attachment_delivery import uploads
from .channel_publications import presentation
from .effect_grants import outcome, snowflake
from .intents import InvalidIntent
from .message_fetches import (
    UNKNOWN_MESSAGE,
    action_message_binding,
    cached_message,
    fetch_message,
    remember_owned_output,
)
from .panel_emojis import usable_panel_emoji


def layout_scope(snapshot, job, action):
    return (
        action.kind == WRITE_LAYOUT_MESSAGE
        and ATTEMPT.fullmatch(action.idempotency_key) is not None
        and job.guild_id == action.guild_id
        and job.request.event.get("dispatch") == "component"
        and declared_scope(snapshot, job, "layout_write") is True
        and admitted(job)
    )


async def execute(edge, action, job, guild):

    intent = action.intent

    def rejected(code):
        return outcome(edge, action, "rejected", error=code)

    target_id = action.message_id
    if (
        not layout_scope(edge.registry.snapshot, job, action)
        or not snowflake(action.channel_id)
        or not action.persistent
        or (target_id is not None and not snowflake(target_id))
        or any(value is not None for value in (action.page, action.content))
        or action.member_ids
        or len(action.role_ids) > MAX_LAYOUT_COMPONENTS
        or len(action.emoji_keys) > MAX_LAYOUT_COMPONENTS
        or (
            intent is None
            and (target_id is None or action.role_ids or action.emoji_keys)
        )
        or (
            intent is not None and (
                intent.kind != ("edit" if target_id else "send")
                or intent.presentation_mode != "v2"
                or intent.mention_actor or intent.session_id or intent.attachment_ref
                or intent.source_guild_ids or not intent.pages
                or any(not item.reference.startswith("tmp:") for item in intent.attachments)
            )
        )
    ):
        return rejected("action_scope_mismatch")

    if target_id is None:
        return await native_write(edge, action, job, guild)
    key = (guild.id, action.channel_id, target_id)
    async with edge.message_mutations.locks.setdefault(key, asyncio.Lock()):
        return await native_write(edge, action, job, guild)


async def native_write(edge, action, job, guild):
    intent, target_id = action.intent, action.message_id
    deadline = min(action.expires_at_ms, job.expires_at_ms)

    def rejected(code):
        return outcome(edge, action, "rejected", error=code)

    def missing():
        return outcome(edge, action, "missing", error="message_not_found")

    def live():
        return deadline > time.time_ns() // 1_000_000 and layout_scope(
            edge.registry.snapshot, job, action,
        )

    try:
        if not live():
            return rejected("expired_job")
        channel = await edge._action_channel(guild, action.channel_id)
        if channel is None or guild.me is None:
            return rejected("channel_unavailable")
        permissions = channel.permissions_for(guild.me)
        if not permissions.view_channel:
            return rejected("permission_denied")
        if intent is not None:
            can_send = (
                permissions.send_messages_in_threads if isinstance(channel, discord.Thread)
                else permissions.send_messages
            )
            if not can_send or (intent.attachments and not permissions.attach_files):
                return rejected("permission_denied")
            if action.role_ids and not guild.me.guild_permissions.manage_roles:
                return rejected("permission_denied")
            for role_id in set(action.role_ids):
                role = guild.get_role(role_id)
                if role is None or role.is_default() or role.managed or guild.me.top_role <= role:
                    return rejected("role_unavailable")
            for value in set(action.emoji_keys):
                emoji = discord.PartialEmoji.from_str(value)
                if emoji.id is not None and not usable_panel_emoji(
                    edge.get_emoji(emoji.id), channel,
                ):
                    return rejected("emoji_unavailable")
        target = None
        if target_id is not None:
            binding = await action_message_binding(edge.queue_redis, guild.id, target_id)
            if binding is not None:
                if (binding["module"], binding.get("output_channel")) != (
                    action.module, channel.id,
                ):
                    return rejected("message_not_owned")
            else:


                observed = cached_message(edge, channel, target_id)
                if observed is None:
                    if not permissions.read_message_history:
                        return rejected("permission_denied")
                    observed = await fetch_message(edge, channel, target_id, cached=True)
                if (
                    edge.user is None or observed.author.id != edge.user.id
                    or observed.webhook_id is not None
                ):
                    return rejected("message_not_owned")
            target = channel.get_partial_message(target_id)
        if intent is None:
            if not live():
                return rejected("expired_job")
            await target.delete()
            return outcome(edge, action, "deleted", message_id=target_id)
        kwargs = presentation(edge, action, intent, layout_message=True)
        async with uploads(edge, intent, job, action=action, deadline=deadline) as files:

            if not live():
                return rejected("expired_job")
            if target is None:
                nonce = action.idempotency_key.rsplit(":", 1)[1][:25]
                sent = await channel.send(**kwargs, files=list(files), nonce=nonce)
                message_id = sent.id
            else:
                sent = await target.edit(**kwargs, content=None, embeds=[], attachments=list(files))
                message_id = target_id
            with suppress(RedisError):
                await remember_owned_output(
                    edge.queue_redis, guild_id=guild.id, channel_id=channel.id,
                    message_id=message_id,
                    module=action.module, family="v2",
                )
            if sent is not None:
                edge._bind_view(kwargs, sent)
            disposition = "sent" if target is None else "edited"
            return outcome(edge, action, disposition, message_id=message_id)
    except discord.NotFound as error:
        if target_id is not None and error.code == UNKNOWN_MESSAGE:
            if intent is None:
                return outcome(edge, action, "deleted", message_id=target_id)
            return missing()
        return rejected("channel_unavailable")
    except discord.HTTPException as error:
        if 400 <= error.status < 500 and error.status != 408:
            return rejected("discord_rejected")
        return outcome(edge, action, "uncertain", error="ambiguous")
    except (InvalidIntent, ValueError):
        return rejected("invalid_action")
    except (TimeoutError, OSError):
        return outcome(edge, action, "uncertain", error="ambiguous")
