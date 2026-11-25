import time
from contextlib import suppress

import discord
from redis.exceptions import RedisError

from lumi.contracts.models import MAX_MENTION_RECIPIENTS
from lumi.shared.discord_effects import REPLY_TEXT

from .admission import declared_scope
from .effect_grants import EffectDenied, approve, outcome, snowflake
from .intents import IntentValidator, InvalidIntent
from .message_fetches import UNKNOWN_MESSAGE, action_message_binding, remember_owned_output


class TextReplies:
    def __init__(self, edge, *, clock=None):
        self.edge = edge
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    async def execute(self, action, job, guild):
        edge, intent, recipients = self.edge, action.intent, action.member_ids

        def receipt(disposition, *, error=None, message_id=0):
            return outcome(edge, action, disposition, error=error, message_id=message_id)

        spec = declared_scope(edge.registry.snapshot, job, "text_reply")
        if (
            action.kind != REPLY_TEXT
            or not snowflake(action.message_id)
            or intent is None
            or intent.kind != "send"
            or not intent.content
            or intent.pages
            or intent.attachments
            or intent.attachment_ref
            or intent.session_id
            or intent.mention_actor
            or intent.source_guild_ids
            or intent.presentation_mode != "legacy"
            or any(
                value is not None
                for value in (
                    action.content,
                    action.page,
                    action.embed_sources,
                    action.read_proof,
                )
            )
            or len(recipients) > MAX_MENTION_RECIPIENTS
            or len(set(recipients)) != len(recipients)
            or not all(snowflake(user) for user in recipients)
        ):
            return edge._action_result(action, False, "failed", "action_scope_mismatch")
        try:
            approved = await approve(edge, action, job, guild, spec, self.clock, history=True)
        except EffectDenied as denied:
            if denied.approved:
                return receipt("rejected", error=denied.code)
            return edge._action_result(action, False, "failed", denied.code)
        channel = approved.channel
        known = await action_message_binding(edge.queue_redis, guild.id, action.message_id)
        if known and (
            known["module"] != job.module or known.get("output_channel") not in (None, channel.id)
        ):
            return receipt("rejected", error="message_not_owned")
        try:
            IntentValidator().validate(intent, presentation_mode="legacy")
        except InvalidIntent:
            return receipt("rejected", error="invalid_action")
        view = edge.layouts.legacy_controls(
            intent.controls,
            module=action.module,
            registry=edge.registry.snapshot,
            owner_id=None,
            blocklist=edge.blocklist,
            timeout=None,
        )
        mentions = discord.AllowedMentions(
            everyone=False,
            roles=False,
            users=[discord.Object(id=user) for user in recipients],
            replied_user=False,
        )
        if approved.deadline <= self.clock():
            return receipt("rejected", error="expired_job")


        try:
            message = await channel.get_partial_message(action.message_id).reply(
                intent.content,
                mention_author=False,
                allowed_mentions=mentions,
                suppress_embeds=True,
                view=view,
            )
        except discord.NotFound as exc:
            if exc.code != UNKNOWN_MESSAGE:
                return receipt("rejected", error="discord_rejected")
            return receipt("missing", error="message_not_found")
        except discord.HTTPException as exc:
            if 400 <= exc.status < 500 and exc.status != 408:
                return receipt("rejected", error="discord_rejected")
            return receipt("uncertain", error="ambiguous")
        except (TimeoutError, OSError):
            return receipt("uncertain", error="ambiguous")
        with suppress(RedisError):
            await remember_owned_output(
                edge.queue_redis,
                guild_id=guild.id,
                channel_id=channel.id,
                message_id=message.id,
                module=action.module,
                family="legacy",
            )
        return receipt("sent", message_id=message.id)
