import asyncio
import time
from contextlib import AsyncExitStack, suppress
from weakref import WeakValueDictionary

import discord
from redis.exceptions import RedisError

from lumi.shared.discord_effects import MUTATE_MESSAGE, mutation_lane

from .admission import declared_scope
from .attachment_delivery import uploads
from .channel_publications import presentation
from .effect_grants import EffectDenied, approve, claim, delivery_allowed, outcome, snowflake
from .intents import IntentValidator, InvalidIntent
from .message_fetches import UNKNOWN_MESSAGE, action_message_binding, fetch_message, remember_owned_output
from .retained_uploads import resolve_gallery_uploads


class MessageMutations:
    def __init__(self, edge, *, clock=None):
        self.edge = edge
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.locks = WeakValueDictionary()

    async def execute(self, action, job, guild):
        edge, intent, mutation = self.edge, action.intent, action.message_mutation
        spec = declared_scope(edge.registry.snapshot, job, "message_mutation")
        replace = (
            mutation is not None
            and mutation.operation == "edit"
            and not mutation.preserve_attachments
        )
        if (
            spec is None
            or action.kind != MUTATE_MESSAGE
            or mutation is None
            or mutation.operation not in {"edit", "delete"}
            or not snowflake(action.message_id)
            or any(value is not None for value in (action.content, action.page))
            or action.member_ids
            or (
                mutation.operation == "delete"
                and (not spec.allow_delete or intent is not None or mutation.preserve_attachments)
            )
            or (
                mutation.operation == "edit"
                and (
                    intent is None
                    or intent.kind != "edit"
                    or intent.attachment_ref
                    or intent.session_id
                    or intent.mention_actor
                    or intent.source_guild_ids
                    or intent.presentation_mode not in {"legacy", "v2"}
                    or (mutation.preserve_attachments and intent.attachments)
                    or any(
                        not item.reference.startswith("job-file:" if spec.delivery is not None else "tmp:")
                        for item in intent.attachments
                    )
                )
            )
            or replace
            and not spec.allow_replacement
            or bool(replace and spec.require_replacement_proof)
            != (action.embed_sources is not None)
            or bool(replace and spec.require_replacement_proof) != bool(action.read_proof)
        ):
            return edge._action_result(action, False, "failed", "action_scope_mismatch")
        if mutation.owner_component is not None and not self.valid_marker(
            job, mutation.owner_component
        ):
            return edge._action_result(action, False, "failed", "action_scope_mismatch")
        key = (guild.id, action.channel_id, action.message_id)
        async with self.locks.setdefault(key, asyncio.Lock()):
            try:
                approved = await approve(
                    edge,
                    action,
                    job,
                    guild,
                    spec,
                    self.clock,
                    history=True,
                    attachments=bool(intent and intent.attachments),
                )
            except EffectDenied as denied:
                if denied.approved:
                    return outcome(edge, action, "rejected", error=denied.code)
                return edge._action_result(action, False, "failed", denied.code)
            return await self.mutate(action, job, guild, approved, spec)

    def valid_marker(self, job, marker):
        parts = marker.split(":", 3)
        if len(marker) > 100 or len(parts) != 4 or parts[0] != "lumi":
            return False
        route = self.edge.registry.snapshot.components.get((parts[1], parts[2]))
        return route is not None and route.module == job.module and route.group == job.group

    async def mutate(self, action, job, guild, approved, spec):
        edge, mutation, intent = self.edge, action.message_mutation, action.intent

        def rejected(code):
            return outcome(edge, action, "rejected", error=code)

        deadline = approved.deadline
        if action.embed_sources is not None:
            verified = edge.message_sources.verified_until(action, action.embed_sources)
            if not verified:
                return rejected("source_verification_expired")
            deadline = min(deadline, verified)
        lane = mutation_lane(job.group, job.module, guild.id, action.channel_id, action.message_id)
        if deadline <= self.clock():
            return rejected("expired_job")
        if spec.delivery is None and not await claim(edge, approved, lane=lane):
            return rejected("effect_not_authorized")
        channel = approved.channel
        try:
            if deadline <= self.clock():
                return rejected("expired_job")
            known = await action_message_binding(edge.queue_redis, guild.id, action.message_id)
            if known and (
                known["module"] != job.module
                or known.get("output_channel") not in (None, channel.id)
            ):
                return rejected("message_not_owned")
            indexed = bool(
                (known and known.get("output_channel") == channel.id)
                or (spec.delivery is not None and spec.delivery.recorded_target)
            )
            filenames = (
                frozenset(
                    name for page in intent.pages for name in IntentValidator._page_file_names(page)
                )
                if intent and mutation.preserve_attachments
                else frozenset()
            )
            media_read = bool(intent and intent.presentation_mode == "v2" and filenames)
            if media_read or not indexed:
                if not indexed and mutation.owner_component is None:
                    return rejected("message_not_owned")
                observed = await fetch_message(edge, channel, action.message_id)
                components = [
                    c.to_dict() if hasattr(c, "to_dict") else c
                    for c in getattr(observed, "components", ())
                ]
                if (
                    edge.user is None
                    or observed.author.id != edge.user.id
                    or observed.webhook_id is not None
                    or (
                        mutation.owner_component is not None
                        and mutation.owner_component not in edge._component_custom_ids(components)
                    )
                ):
                    return rejected("message_not_owned")
                if media_read:
                    IntentValidator().validate(
                        intent, presentation_mode="v2", retained_uploads=filenames
                    )
                    intent = resolve_gallery_uploads(intent, observed, channel.id, filenames)
                    filenames = frozenset()
            target = channel.get_partial_message(action.message_id)
            if mutation.operation == "delete":
                if deadline <= self.clock():
                    return rejected("expired_job")
                await target.delete()
                return outcome(edge, action, "deleted", message_id=action.message_id)
            async with AsyncExitStack() as stack:
                kwargs = presentation(
                    edge, action, intent, retained=filenames, scheduled_files=spec.delivery is not None,
                )
                kwargs.setdefault("content", None)
                kwargs.setdefault("embeds", [])
                if not mutation.preserve_attachments:
                    files = await stack.enter_async_context(
                        uploads(edge, intent, job, action=action, deadline=deadline),
                    )
                    kwargs["attachments"] = list(files)
                if deadline <= self.clock():
                    return rejected("expired_job")
                if spec.delivery is not None and not await claim(edge, approved, lane=lane):
                    return rejected("effect_not_authorized")
                if deadline <= self.clock():
                    return rejected("expired_job")
                if not delivery_allowed(edge, guild, job, approved, spec.delivery):
                    return rejected("permission_denied")
                await target.edit(**kwargs)
        except discord.NotFound as exc:
            if exc.code != UNKNOWN_MESSAGE:
                return rejected("discord_rejected")
            if mutation.operation == "delete":
                return outcome(edge, action, "deleted", message_id=action.message_id)
            return outcome(edge, action, "missing", error="message_not_found")
        except discord.HTTPException as exc:
            if 400 <= exc.status < 500 and exc.status != 408:
                return rejected("discord_rejected")
            return outcome(edge, action, "uncertain", error="ambiguous")
        except (InvalidIntent, ValueError):
            return rejected("invalid_action")
        except (TimeoutError, OSError):
            return outcome(edge, action, "uncertain", error="ambiguous")
        with suppress(RedisError):
            await remember_owned_output(
                edge.queue_redis,
                guild_id=guild.id,
                channel_id=channel.id,
                message_id=action.message_id,
                module=job.module,
                family=intent.presentation_mode,
            )
        return outcome(edge, action, "edited", message_id=action.message_id)
