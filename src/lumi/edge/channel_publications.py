import time
from contextlib import suppress

import discord
from redis.exceptions import RedisError

from lumi.shared.discord_effects import PUBLISH_MESSAGE

from .admission import declared_scope
from .attachment_delivery import uploads
from .effect_grants import EffectDenied, approve, claim, delivery_allowed, outcome
from .intents import IntentValidator, InvalidIntent
from .message_fetches import remember_owned_output
from .registry import module_colour


class ChannelPublications:
    def __init__(self, edge, *, clock=None):
        self.edge = edge
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)

    async def execute(self, action, job, guild):
        edge, intent = self.edge, action.intent
        spec = declared_scope(edge.registry.snapshot, job, "channel_publication")
        if (
            spec is None
            or action.kind != PUBLISH_MESSAGE
            or action.message_id is not None
            or action.member_ids
            or intent is None
            or intent.kind != "send"
            or intent.attachment_ref
            or intent.session_id
            or intent.mention_actor
            or intent.source_guild_ids
            or intent.presentation_mode not in {"v2", "legacy"}
            or any(value is not None for value in (action.content, action.page))
            or spec.require_source_proof != (action.embed_sources is not None)
            or spec.require_source_proof != bool(action.read_proof)
            or any(
                not item.reference.startswith("job-file:" if spec.delivery is not None else "tmp:")
                for item in intent.attachments
            )
        ):
            return edge._action_result(action, False, "failed", "action_scope_mismatch")

        def rejected(code):
            return outcome(edge, action, "rejected", error=code)

        try:
            approved = await approve(
                edge,
                action,
                job,
                guild,
                spec,
                self.clock,
                history=spec.require_history,
                attachments=bool(intent.attachments),
            )
        except EffectDenied as denied:
            if denied.approved:
                return rejected(denied.code)
            return edge._action_result(action, False, "failed", denied.code)
        deadline = approved.deadline
        try:
            if spec.require_source_proof:
                verified = edge.message_sources.verified_until(action, action.embed_sources)
                if not verified:
                    return rejected("source_verification_expired")
                deadline = min(deadline, verified)
            kwargs = presentation(edge, action, intent, scheduled_files=spec.delivery is not None)
            async with uploads(edge, intent, job, action=action, deadline=deadline) as files:
                if len(files) == 1 and intent.presentation_mode == "legacy":
                    kwargs["file"] = files[0]
                elif files:
                    kwargs["files"] = list(files)
                if deadline <= self.clock():
                    return rejected("expired_job")
                if not await claim(edge, approved):
                    return rejected("effect_not_authorized")
                if deadline <= self.clock():
                    return rejected("expired_job")
                if not delivery_allowed(edge, guild, job, approved, spec.delivery):
                    return rejected("permission_denied")
                if approved.delivery is not None and approved.delivery.nonce is not None:
                    kwargs["nonce"] = approved.delivery.nonce
                try:
                    message = await approved.channel.send(**kwargs)
                except discord.HTTPException as exc:
                    if 400 <= exc.status < 500 and exc.status != 408:
                        return rejected("discord_rejected")
                    return outcome(edge, action, "uncertain", error="ambiguous")
                except (TimeoutError, OSError):
                    return outcome(edge, action, "uncertain", error="ambiguous")
            with suppress(RedisError):
                await remember_owned_output(
                    edge.queue_redis,
                    guild_id=guild.id,
                    channel_id=approved.channel.id,
                    message_id=message.id,
                    module=action.module,
                    family=intent.presentation_mode,
                )
            return outcome(edge, action, "sent", message_id=message.id)
        except (InvalidIntent, ValueError, OSError):
            return rejected("invalid_action")


def presentation(
    edge, action, intent, *, retained=frozenset(), scheduled_files=False, layout_message=False,
):
    registry = edge.registry.snapshot
    manifest = registry.modules[action.module].manifest
    allowed = manifest.action_presentation_modes or (manifest.presentation_mode,)
    IntentValidator().validate(
        intent,
        presentation_mode=manifest.presentation_mode,
        presentation_modes=allowed,
        retained_uploads=retained,
        scheduled_files=scheduled_files,
        layout_message=layout_message,
    )
    accent = module_colour(registry, action.module)
    binding = dict(
        module=action.module,
        registry=registry,
        owner_id=None,
        blocklist=edge.blocklist,
        timeout=None,
    )
    kwargs = {"allowed_mentions": discord.AllowedMentions.none()}
    if intent.presentation_mode == "v2":
        kwargs["view"] = edge.layouts.build_many(intent.pages, accent_color=accent, **binding)
    else:
        kwargs["embeds"] = edge.layouts.legacy_embeds(intent.pages, accent_color=accent)
        kwargs["view"] = edge.layouts.legacy_controls(intent.controls, **binding)
        if intent.content is not None:
            kwargs["content"] = intent.content
    return kwargs
