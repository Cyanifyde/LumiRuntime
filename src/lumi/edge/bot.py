from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
import unicodedata
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import discord
import msgspec
from redis.asyncio import Redis
from redis.exceptions import RedisError

from lumi.contracts import (
    DiscordAction,
    DiscordActionResult,
    DiscordIntent,
    ImageSourceContext,
    JobEnvelope,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
)
from lumi.edge.operator_controls import RuntimeControlPlane
from lumi.contracts.member_profiles import MEMBER_PROFILE_ACTIONS
from lumi.contracts.layout_message import WRITE_LAYOUT_MESSAGE
from lumi.contracts.member_effects import MEMBER_EFFECT_PERMISSIONS
from lumi.contracts.role_access import ACTOR_ROLE_ACTIONS, ROLE_FACTS
from lumi.contracts.staff_recovery import RECOVERY_ACTIONS
from lumi.contracts.staff_moderation import STAFF_EFFECT
from lumi.contracts.channel_messages import CHANNEL_MESSAGE_ACTIONS
from lumi.contracts.channel_configuration import READ_CHANNEL_CONFIGURATION
from lumi.contracts.channel_resources import CHANNEL_RESOURCE_ACTIONS
from lumi.shared.discord_effects import UPDATE_BOUND_PANEL
from lumi.transport import response_state_key
from lumi.contracts.runtime_control import BLOCK_CONFIRMATION
from lumi.ui_keys import read_ui, remember_session, ui_key
from lumi.ui_keys import write_keys as ui_write_keys

from lumi.shared.attachment_payloads import (
    AttachmentPayloads, AttachmentUnavailable, action_scope, references,
)

from .attachment_delivery import kwargs_for

from .actions import EdgeActionConsumer
from lumi.contracts.activity_observations import ActivityObservation
from lumi.shared.emoji import canonical_emoji
from lumi.shared.links import source_urls
from .admission import access_epoch, admitted, bot_guild_allowed, private_reply_allowed, stamp
from .admission import project_bot_channel
from .activity_watermark import LastSpoke, last_activity, record_activity
from .activity_observations import ActivityRelay
from .actor_index import ActorRouteIndex
from .announcements import AnnouncementConsumer
from .blocking import GlobalBlocklist
from lumi.shared.broker import QueueFull, QueueUnavailable, RedisBroker
from .prepared_replies import MARKER as PREPARED_REPLY, PreparedReplies
from .catalog import AcceptedCatalogPublisher
from .channel_watches import ChannelWatches
from .channel_member_facts import ChannelMemberFacts
from .channel_publications import ChannelPublications
from .message_sources import MessageSources
from .message_mutations import MessageMutations
from .text_replies import TextReplies
from .command_loading import (
    NATIVE_COMMANDS,
    OWNER_NATIVE_COMMANDS,
    CommandLoading,
    response_ttl,
)
from .guild_catalogues import GuildCatalogueCache
from .gateway_capture import GatewayCaptures, GatewayConnectionState
from .guarded_replies import reply_allowed, requires_guard
from .help import HelpEntry, HelpView
from .intents import IntentValidator, ValidatedIntent
from .matcher import EdgeMatcher, EmojiLookupMiss, MatchedRoute, MatchError
from .member_search import execute as search_members
from .members import MemberLookup, fetch_member, lookup, member_mutation
from .message_fetches import (
    UNKNOWN_MESSAGE,
    action_message_binding, cached_message, content_digest, fetch_message, known_message, message_fetch_scope,
    owned_output_module,
    remember_message, remember_owned_output, unresolved_in_channel,
)
from .message_snapshots import (
    capture_message,
    capture_urls,
    embed_source,
    snapshot_after_raw_edit,
    snapshot_from_message,
)
from .metrics import RollingProcessingMetrics
from .owner_controls import owner_command_prefix, owner_job_spec
from .owned_history import OwnedHistory
from .personal_reactions import PersonalReactions
from .bound_replies import ACTION as BIND_MESSAGE_REPLY, BoundReplies
from .bound_panels import execute as update_bound_panel, remember_panel
from .policy import PolicyCache
from .policy_consumer import PolicyConsumer
from .previews import PreviewForms
from .reaction_index import ReactionRouteIndex
from .reaction_offers import ReactionOfferRecord, ReactionOfferStore
from .read_proofs import ReadProofs, message_author_scope
from .source_versions import SourceVersions
from lumi.shared.receipts import ComponentReceipts
from .registry import EdgeRouteRegistry, module_colour
from .response_revisions import revision_guard
from lumi.shared.request_scope import memo, memo_read, scoped
from .results import ResultConsumer
from .scheduler import ScheduledCallbackConsumer
from .webhooks import WebhookDelivery
from .views import (
    LUMI_COLOUR,
    BoundLayoutView,
    BoundLegacyView,
    ComponentsV2Builder,
    ComponentsV2Paginator,
    LegacyEmbedPaginator,
    lumi_page,
)

LOGGER = logging.getLogger(__name__)
SNOWFLAKE = re.compile(r"^(?:<@!?)?(\d{17,20})>?$")
NATIVE_ROOTS = frozenset(NATIVE_COMMANDS)


class LumiEdge(discord.Client):
    def __init__(
        self,
        *,
        owner_id: int,
        default_prefix: str,
        blocklist: GlobalBlocklist,
        registry: EdgeRouteRegistry,
        queue_redis: Redis,
        cache_redis: Redis,
        control: RuntimeControlPlane,
    ) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.dm_messages = True
        intents.message_content = True
        intents.members = True
        intents.guild_reactions = True
        intents.emojis_and_stickers = True
        intents.moderation = True
        intents.voice_states = True
        intents.invites = True
        intents.webhooks = True
        intents.integrations = True
        intents.guild_scheduled_events = True
        super().__init__(
            intents=intents,
            member_cache_flags=discord.MemberCacheFlags.none(),
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self.owner_id = owner_id
        self.default_prefix = default_prefix
        self.blocklist = blocklist
        self.registry = registry
        self.queue_redis = queue_redis
        self.cache_redis = cache_redis
        self.attachment_payloads = AttachmentPayloads(cache_redis)
        self.control = control
        self.policy = PolicyCache(default_prefix)
        self.gateway_captures = GatewayCaptures(self)
        self.gateway_relays = None
        self.policies = PolicyConsumer(queue_redis, self.policy)
        self.guild_catalogues = GuildCatalogueCache(queue_redis)
        self.personal_reactions = PersonalReactions(self)
        self.bound_replies = BoundReplies(self)
        self.matcher = EdgeMatcher()
        self.validator = IntentValidator()
        self.broker = RedisBroker(queue_redis)
        self.channel_watches = ChannelWatches(self)
        self.announcements = AnnouncementConsumer(queue_redis, registry)
        self.catalog = AcceptedCatalogPublisher(queue_redis, registry)
        self.results = ResultConsumer(queue_redis, self.broker, registry, self._complete_job)
        self.actions = EdgeActionConsumer(queue_redis, self.broker, registry, self._execute_action)
        self.scheduled = ScheduledCallbackConsumer(
            queue_redis, self.broker, registry,
            application_user_id=lambda: getattr(self.user, "id", None),
        )
        self.activity_relay = ActivityRelay(queue_redis)
        self.component_receipts = ComponentReceipts(queue_redis)
        self.reaction_index = ReactionRouteIndex(queue_redis)
        self.reaction_offers = ReactionOfferStore(
            cache_redis,
            registry,
            on_purge=self._remove_offer_reaction,
        )
        self.prepared_replies = PreparedReplies(self)
        self.member_cache = MemberLookup(self)
        self.actor_route_index = ActorRouteIndex(queue_redis)
        self.webhooks = WebhookDelivery(self)
        self.owned_history = OwnedHistory(self)
        self.channel_member_facts = ChannelMemberFacts(self)
        self.message_sources = MessageSources(self)
        self.message_author_proofs = ReadProofs(lambda: time.time_ns() // 1_000_000)
        self.source_versions = SourceVersions(self.message_author_proofs.clock)
        self.text_replies = TextReplies(self)
        self.channel_publications = ChannelPublications(self)
        self.message_mutations = MessageMutations(self)
        self.processing = RollingProcessingMetrics()
        self.queue_depths: dict[str, int] = {}
        self.layouts = ComponentsV2Builder(self._brand_icon_url)
        self.preview_forms = PreviewForms(cache_redis, self.layouts, owner_id=owner_id)
        self._background: set[asyncio.Task[Any]] = set()
        self._interactions: dict[str, discord.Interaction] = {}
        self._interaction_jobs: dict[str, JobEnvelope] = {}
        self._response_locks: dict[str, asyncio.Lock] = {}
        self._pending_messages: dict[str, discord.Message] = {}
        self._pending_jobs: dict[str, JobEnvelope] = {}
        self.command_loading = CommandLoading(self)
        self.private_loading = None
        self._guild_member_snapshots: dict[int, tuple[float, list[discord.Member]]] = {}
        self._guild_member_snapshot_locks: dict[int, asyncio.Lock] = {}
        self._offer_message_authors: OrderedDict[int, tuple[int, float]] = OrderedDict()
        self._passive_message_watches: OrderedDict[int, tuple[int, float]] = OrderedDict()


        self._last_event_order = time.time_ns() // 1_000
        self._closing = False
        self.handover = None

    def _get_state(self, **options):
        return GatewayConnectionState(
            dispatch=self.dispatch,
            handlers=self._handlers,
            hooks=self._hooks,
            http=self.http,
            **options,
        )

    async def setup_hook(self) -> None:
        if not self.blocklist.ready:
            raise RuntimeError("global blocklist must load before Gateway startup")
        if getattr(self, "private_ingress", None) is not None:
            await self.private_ingress.start()
            for consumer in (
                self.results,
                self.actions,
                self.scheduled,
                self.actor_route_index,
                self.policies,
                self.guild_catalogues,
                self.announcements,
                self.catalog,
                self.reaction_index,
                self.personal_reactions,
                self.bound_replies,
            ):
                consumer.ingress_gate = self.private_ingress.gate
        await self.broker.start()
        await self.channel_watches.load()
        await self.control.load()
        if projection := getattr(self, "control_projection", None):
            self._spawn(projection.run, "runtime-control-projection")
        await self.reaction_offers.load()
        await self.personal_reactions.load()
        await self.bound_replies.load()
        if self.private_loading is not None:
            await self.private_loading.recover()
            self._spawn(self.private_loading.run, "private-component-feedback")
        self.command_loading.arm_recovery()
        self._spawn_once(self._recover_command_loading, "command-loading-recovery")
        self._spawn(self._command_loading_loop, "command-loading-cleanup")
        ingress = getattr(self, "private_ingress", None)
        routing_gate = (
            contextlib.nullcontext(True) if ingress is None else ingress.gate.track(0, internal=True)
        )
        async with routing_gate as permitted:
            if permitted:


                for consumer in (self.announcements, self.policies):
                    with contextlib.suppress(RedisError):
                        await consumer.prime()
        if self.gateway_relays is not None:
            await self.gateway_relays.start()
        self._spawn(self.announcements.run, "manifest-consumer")
        self._spawn(self.catalog.run, "catalog-publisher")
        self._spawn(self.policies.run, "policy-consumer")
        self._spawn(self.guild_catalogues.run, "guild-catalogue-consumer")
        self._spawn(self.personal_reactions.run, "personal-reaction-consumer")
        self._spawn(self.results.run, "result-consumer")
        self._spawn(self.actions.run, "edge-action-consumer")
        self._spawn(self.scheduled.run, "scheduled-callback-consumer")
        self._spawn(self.reaction_index.run, "reaction-index-consumer")
        self._spawn(self.reaction_offers.run, "reaction-offer-cleaner")
        self._spawn(self.actor_route_index.run, "actor-route-index-consumer")
        self._spawn(self._maintenance_loop, "edge-maintenance")

    async def close(self) -> None:
        self._closing = True
        self.member_cache.invalidate()
        if getattr(self, "private_ingress", None) is not None:
            await self.private_ingress.close()
        self.announcements.stop()
        self.catalog.stop()
        self.policies.stop()
        self.guild_catalogues.stop()
        self.personal_reactions.stop()
        self.results.stop()
        self.actions.stop()
        self.scheduled.stop()
        self.reaction_index.stop()
        self.reaction_offers.stop()
        self.actor_route_index.stop()
        for task in self._background:
            task.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)
        if self.private_loading is not None:
            await self.private_loading.close()
        if self.gateway_relays is not None:
            await self.gateway_relays.close()

        for job in tuple(self._pending_jobs.values()):
            await self._finish_pending(job, restore=True)
            with contextlib.suppress(RedisError):
                await self.cache_redis.set(
                    response_state_key(job.job_id, job.group), "final", ex=response_ttl(job)
                )
        self._response_locks.clear()
        self._interactions.clear()
        self._interaction_jobs.clear()
        await super().close()

    async def on_disconnect(self) -> None:
        self.member_cache.invalidate()
        self.source_versions.clear()

    async def on_message(self, message: discord.Message) -> None:


        ingress = getattr(self, "private_ingress", None)
        if ingress is not None and await ingress.message(message):
            return
        if ingress is not None:
            async with ingress.gate.track(ingress.message_value(message)) as permitted:
                if permitted:
                    await LumiEdge._ordinary_message(self, message)
            return
        return await LumiEdge._ordinary_message(self, message)

    @scoped
    async def _ordinary_message(self, message: discord.Message) -> None:

        if self.blocklist.contains(message.author.id):
            return
        if self.user and message.author.id == self.user.id:
            return
        gate = getattr(self, "handover", None)
        if (
            gate
            and message.guild
            and await gate.ingress_blocked(message.guild.id, message.channel.id)
        ):
            prefix = self.policy.prefix(message.guild.id)
            root = (
                message.content[len(prefix) :].split(maxsplit=1)[0].casefold()
                if message.content.startswith(prefix) and message.content[len(prefix) :].strip()
                else ""
            )
            if not gate.control_command(root):
                return
        watches = getattr(self, "channel_watches", None)
        if watches is not None:
            try:
                await watches.ingest(message)
            except RedisError:
                watched = watches.channels.get(
                    watches.field(message.guild.id if message.guild else 0, message.channel.id)
                )
                if watched:
                    watched.update(closed=True, error="storage_unavailable")
                LOGGER.warning("channel_watch_ingress_unavailable")
        loading = getattr(self, "command_loading", None)
        if loading is None:

            return await LumiEdge._process_message(self, message)
        await loading.wait_recovered()
        route = None
        owns_loading = False
        try:
            route = self._early_command_route(message)
            if route is not None and not await loading.begin(route, message):
                return
            owns_loading = route is not None and route.loading
            record = loading.for_source(message.id)
            if record is None:
                return await self._process_message(message)
            remaining = (record.job.expires_at_ms - int(time.time() * 1000)) / 1000
            async with asyncio.timeout(max(0, remaining)):
                await self._process_message(message)
        except (RedisError, TimeoutError, QueueUnavailable):
            record = loading.for_source(message.id)
            if record is not None:
                async with self._response_locks.setdefault(record.job.job_id, asyncio.Lock()):
                    await loading.finish(record.job, text="Unavailable. Try again.")
            elif route is not None and (
                owns_loading or message.id not in loading.completed_sources
            ):
                with contextlib.suppress(discord.HTTPException):
                    await self._reply(message, "Unavailable. Try again.")
        except Exception:
            record = loading.for_source(message.id)
            if record is not None:
                async with self._response_locks.setdefault(record.job.job_id, asyncio.Lock()):
                    await loading.finish(record.job, text="Unavailable. Try again.")
            raise
        finally:
            if owns_loading:
                await loading.settle_unsubmitted(message.id)
                loading.completed_sources.pop(message.id, None)

    @message_fetch_scope
    async def _process_message(self, message: discord.Message) -> None:
        remember_message(self, message)
        event = await self._message_event(message)
        await self._handle_reaction_offer_message(message, event, "message_create")
        if message.author.bot:
            routes = await self._passive_routes("message_create", event)
            watch_ttl = max((route.watch_ttl_ms for route in routes), default=0)
            if watch_ttl:
                self._remember_passive_watch(message.id, message.author.id, watch_ttl)
                with contextlib.suppress(Exception):
                    await self.cache_redis.set(
                        f"lumi:passive-watch:{message.id}",
                        str(message.author.id),
                        px=watch_ttl,
                    )
            for route in routes:
                await self._submit(route, message)
            return

        if bound_replies := getattr(self, "bound_replies", None):
            await bound_replies.enrich(message, event)
        if not event.get("activity_exempt"):
            await self._record_activity_boundary(event)
        await self._dispatch_passive("message_create", event)
        if message.guild is not None and message.webhook_id is None:
            await self._dispatch_activity(message)

        if await self._mention_prefix_recovery(message):
            return

        prefix = self.policy.prefix(message.guild.id if message.guild else None)
        owner_prefix = owner_command_prefix(self.registry.snapshot, message.content, prefix)
        if owner_prefix is not None:
            if self.owner_id <= 0 or message.author.id != self.owner_id:
                return
            prefix = owner_prefix
        if not message.content.startswith(prefix):
            return
        body = message.content[len(prefix) :]
        root = body.partition(" ")[0].casefold()
        if self._command_help_requested(root, body[len(root) :], event.get("guild_id")):
            await self._help(message, root)
            return
        if root in NATIVE_ROOTS:
            await self._native(message, root, body[len(root) :].strip())
            return
        try:
            route = self.matcher.match_command(
                self._command_snapshot(event.get("guild_id")),
                content=message.content,
                prefix=prefix,
                event=event,
            )
        except MatchError:
            registered = self._command_snapshot(event.get("guild_id")).commands.get(root)
            if registered and registered.spec.help and not registered.spec.help.hidden:
                roles, administrator = self._member_access(message.author)
                channels, categories = self._channel_access(message.channel)
                if self._help_visible(
                    registered,
                    actor=message.author,
                    guild_id=message.guild.id if message.guild else None,
                    roles=roles,
                    administrator=administrator,
                    channels=channels,
                    categories=categories,
                    channel=message.channel,
                ):
                    await self._help(message, root)
            return
        if route is None:
            return
        if not self._command_allowed(route, message, root, owner_prefix is not None):
            return
        registered = self._command_snapshot(event.get("guild_id")).commands[root]
        if not bot_guild_allowed(message.guild, registered.spec.bot_guild_permissions):
            await self._reply(message, "Missing permission.")
            return
        if self.control.maintenance:
            await self._reply(message, "Maintenance in progress.")
            return
        if "reply_content" in registered.spec.context_fields:
            route = await self._hydrate_reply_content(route, message)
        if "image_source" in registered.spec.context_fields:
            route = await self._hydrate_image_source(route, message)
        if "reference_embed" in registered.spec.context_fields:
            route = self._project_reference_embed(route, message)
        if registered.spec.member_target:
            from .command_member_context import hydrate

            await hydrate(self, route, message, registered.spec.member_target)
        await self._submit(route, message)

    def _early_command_route(self, message: discord.Message) -> MatchedRoute | None:

        if message.author.bot or getattr(message, "webhook_id", None):
            return None
        prefix = self.policy.prefix(message.guild.id if message.guild else None)
        owner_prefix = owner_command_prefix(self.registry.snapshot, message.content, prefix)
        if owner_prefix is not None:
            if self.owner_id <= 0 or message.author.id != self.owner_id:
                return None
            prefix = owner_prefix
        if not message.content.startswith(prefix):
            return None
        body = message.content[len(prefix) :]
        root = body.partition(" ")[0].casefold()
        event = dict(
            actor_id=message.author.id,
            author_id=message.author.id,
            guild_id=message.guild.id if message.guild else None,
            channel_id=message.channel.id,
            message_id=message.id,
            prefix=prefix,
            dispatch="command",
        )
        if root in NATIVE_COMMANDS:
            if root in OWNER_NATIVE_COMMANDS and message.author.id != self.owner_id:
                return None
            spec = NATIVE_COMMANDS[root]
            return MatchedRoute(
                module="_edge",
                group="edge",
                generation=0,
                stream="lumi:jobs:edge",
                deadline_ms=spec.deadline_ms,
                queue_class="control",
                request=TypedRequest(spec.handler, spec.handler, {}, event),
                loading=spec.loading,
            )
        registered = self._command_snapshot(event.get("guild_id")).commands.get(root)
        if registered is None:
            return None
        try:
            route = self.matcher.match_command(
                self._command_snapshot(event.get("guild_id")),
                content=message.content,
                prefix=prefix,
                event=event,
            )
        except MatchError:

            if registered.spec.help is None or registered.spec.help.hidden:
                return None
            route = self.matcher._command_result(registered, {}, event)
        if route is None or not self._command_allowed(
            route, message, root, owner_prefix is not None
        ):
            return None
        if not bot_guild_allowed(message.guild, registered.spec.bot_guild_permissions):
            return None
        if self._command_help_requested(root, body[len(root) :], event.get("guild_id")):
            spec = NATIVE_COMMANDS["help"]
            return MatchedRoute(
                module="_edge",
                group="edge",
                generation=0,
                stream="lumi:jobs:edge",
                deadline_ms=spec.deadline_ms,
                queue_class="control",
                request=TypedRequest(spec.handler, spec.handler, {}, event),
                loading=spec.loading,
            )
        return route

    def _command_snapshot(self, guild_id: int | None):
        cache = getattr(self, "guild_catalogues", None)
        if cache is not None:
            return cache.effective(self.registry.snapshot, guild_id)
        return self.registry.snapshot

    def _command_help_requested(self, root: str, tail: str, guild_id: int | None = None) -> bool:
        if tail.strip().casefold() != "help":
            return False
        spec = NATIVE_COMMANDS.get(root)
        if spec is None:
            command = self._command_snapshot(guild_id).commands.get(root)
            spec = command.spec if command is not None else None
        return bool(spec and spec.help and not spec.help.hidden and root != "help")

    def _command_allowed(
        self, route: MatchedRoute, message: discord.Message, root: str, owner_prefix: bool
    ) -> bool:
        key = (
            self,
            route.module,
            route.generation,
            root,
            owner_prefix,
            message.author.id,
            message.channel.id,
            getattr(self.policy, "_epoch", 0),
            getattr(getattr(self, "member_cache", None), "epoch", 0),
        )
        allowed = memo(
            "command-access",
            key,
            lambda: self._command_allowed_uncached(route, message, root, owner_prefix),
        )
        if allowed:
            registered = self._command_snapshot(message.guild.id if message.guild else None).commands[root]
            project_bot_channel(route, message.channel, message.guild, registered.spec.bot_channel_facts)
            stamp(
                route,
                snapshot=self.registry.snapshot,
                actor_id=message.author.id,
                guild_id=message.guild.id if message.guild else None,
                channel_id=message.channel.id,
            )
            if registered.spec.actor_admission:
                route.request.event["_actor_admission"] = True
        return allowed

    def _command_allowed_uncached(
        self, route: MatchedRoute, message: discord.Message, root: str, owner_prefix: bool
    ) -> bool:
        registered = self._command_snapshot(message.guild.id if message.guild else None).commands[
            root
        ]
        scope = registered.spec.owner_scope
        if owner_prefix and scope != "global":
            return False
        if scope and (self.owner_id <= 0 or message.author.id != self.owner_id):
            return False
        if registered.spec.guild_only and message.guild is None:
            return False
        roles, administrator = self._member_access(message.author)
        if not self._has_permissions(
            message.author, registered.spec.required_permissions, message.channel
        ):
            return False
        if scope == "global":
            return True
        if registered.spec.guild_control:
            return message.guild is not None and administrator
        channels, categories = self._channel_access(message.channel)
        return self.policy.allowed(
            guild_id=message.guild.id if message.guild else None,
            module=route.module,
            acl_key=route.acl_key,
            public=registered.spec.public,
            role_ids=roles,
            administrator=administrator,
            channel_ids=channels,
            category_ids=categories,
        )

    async def _recover_command_loading(self) -> None:
        await self.wait_until_ready()
        while not self._closing:
            try:
                await self.command_loading.recover()
                return
            except (RedisError, TimeoutError):
                LOGGER.warning("command_loading_recovery_unavailable")
                await asyncio.sleep(1)

    async def _command_loading_loop(self) -> None:
        await self.command_loading.wait_recovered()
        while not self._closing:
            await asyncio.sleep(1)
            await self.command_loading.sweep()
            await self._sweep_interaction_responses()

    async def _sweep_interaction_responses(self) -> None:


        for job in tuple(self._interaction_jobs.values()):
            if self._pending_allowed(job):
                continue
            async with self._response_locks.setdefault(job.job_id, asyncio.Lock()):
                await self.cache_redis.set(
                    f"lumi:response-state:{job.job_id}", "final", ex=response_ttl(job)
                )
                self._interactions.pop(job.job_id, None)
                self._interaction_jobs.pop(job.job_id, None)
            self._response_locks.pop(job.job_id, None)

    async def _record_activity_boundary(self, event: dict[str, Any]) -> None:

        guild = int(event.get("guild_id") or 0)
        actor = int(event.get("actor_id") or 0)
        if (
            min(guild, actor) <= 0
            or event.get("author_bot")
            or event.get("webhook_id")
            or event.get("activity_exempt")
        ):
            return
        snapshot = self.registry.snapshot
        prefix = str(event.get("prefix") or ".")
        content = str(event.get("content") or "")
        root = (
            content[len(prefix) :].split(maxsplit=1)[0].casefold()
            if content.startswith(prefix) and content[len(prefix) :].strip()
            else ""
        )
        route = snapshot.commands.get(root)
        wants_boundary = route is not None and "activity_boundary" in route.spec.context_fields
        try:
            if wants_boundary and self.policy.module_enabled(guild, route.module):
                boundary = await last_activity(self.queue_redis, guild, actor)
                event["activity_boundary"] = dict(
                    at_ms=boundary.occurred_at_ms if boundary else 0,
                    message_id=boundary.message_id if boundary else 0,
                )
            elif not event.get("used_prefix") and any(
                self.policy.module_enabled(guild, module) for module in snapshot.activity_boundary_modules
            ):
                await record_activity(
                    self.queue_redis,
                    guild,
                    actor,
                    LastSpoke(int(event["created_at_ms"]), int(event["message_id"])),
                )
        except (RedisError, ValueError, UnicodeError):

            event["activity_boundary"] = None
            LOGGER.warning("activity_boundary_unavailable", extra={"guild_id": guild})

    @scoped
    @message_fetch_scope
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent) -> None:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(
                {
                    "raw": payload.data,
                    "author": getattr(
                        getattr(getattr(payload, "cached_message", None), "author", None), "id", 0
                    ),
                }
            ) as permitted:
                if permitted:
                    await LumiEdge._ordinary_raw_message_edit(self, payload)
            return
        return await LumiEdge._ordinary_raw_message_edit(self, payload)

    async def _ordinary_raw_message_edit(self, payload: discord.RawMessageUpdateEvent) -> None:
        data = payload.data if isinstance(payload.data, dict) else {}
        received_at_ms = int(time.time() * 1000)
        source_revision_ms = received_at_ms
        stamp = data.get("edited_timestamp")
        if isinstance(stamp, str):
            with contextlib.suppress(ValueError):
                changed_at = discord.utils.parse_time(stamp)
                if changed_at is not None:
                    source_revision_ms = int(changed_at.timestamp() * 1000)
        if prepared := getattr(self, "prepared_replies", None):
            with contextlib.suppress(RedisError):
                await prepared.changed(payload, revision_ms=source_revision_ms)
        author = data.get("author") if isinstance(data, dict) else None
        author_id = int(author.get("id", 0)) if isinstance(author, dict) else 0
        cached_message = payload.cached_message
        if not author_id and cached_message is not None:
            author_id = cached_message.author.id
        if not author_id:
            cached_author = self._offer_message_authors.get(payload.message_id)
            if cached_author and time.monotonic() - cached_author[1] <= 900:
                author_id = cached_author[0]
        watched_author = self._passive_watch_author(payload.message_id)
        if watched_author is None:
            with contextlib.suppress(Exception):
                watched_author = await self.cache_redis.get(
                    f"lumi:passive-watch:{payload.message_id}"
                )
        watched = watched_author is not None
        if not author_id and watched_author:
            author_id = int(watched_author)
        if author_id and self.blocklist.contains(author_id):
            return
        event = {
            "kind": "message_edit",
            "actor_id": author_id,
            "author_id": author_id,
            "message_id": payload.message_id,
            "guild_id": payload.guild_id,
            "channel_id": payload.channel_id,
            "content": data.get("content", "") if isinstance(data, dict) else "",
            "source_revision_ms": source_revision_ms,
            "_native_edit_fields": frozenset(data),
            "author_bot": author.get("bot", False) if isinstance(author, dict) else None,
            "webhook_id": data.get("webhook_id"),
            "embeds": data.get("embeds", ()) if isinstance(data, dict) else (),
            "attachments": data.get("attachments", ()) if isinstance(data, dict) else (),
            "components": data.get("components", ()) if isinstance(data, dict) else (),
            "edited_at_ms": received_at_ms,
            "event_received_at_ms": received_at_ms,
            "event_order": self._next_event_order(),
        }
        if (
            cached_message is not None
            and payload.guild_id
            and self.matcher.has_passive_candidates(
                self.registry.snapshot,
                event_type="message_edit",
                author_id=author_id or None,
                context_field="message_snapshot",
                enabled=lambda module: self.policy.module_enabled(payload.guild_id, module),
            )
        ):
            before = snapshot_from_message(cached_message)
            if before is not None:
                after = snapshot_after_raw_edit(before, data)
                if before.visible_signature != after.visible_signature:
                    event["message_snapshot"] = before.context()
        guild_id = payload.guild_id
        hydrated_message: discord.Message | None = None
        hydrated_context: dict[str, Any] | None = None
        hydration_attempted = False

        async def hydrate_message():
            nonlocal hydrated_message, hydration_attempted
            if not hydration_attempted:
                hydration_attempted = True
                channel = self.get_channel(payload.channel_id)
                with contextlib.suppress(discord.HTTPException):
                    if channel is None:
                        channel = await self.fetch_channel(payload.channel_id)
                    if isinstance(channel, (discord.TextChannel, discord.Thread)):
                        hydrated_message = await fetch_message(self, channel, payload.message_id)
            return hydrated_message

        async def project_hydrated(message: discord.Message) -> dict[str, Any]:
            nonlocal hydrated_context
            if hydrated_context is None:
                hydrated_context = await self._message_event(message, event_type="message_edit")
            return dict(hydrated_context)

        if (
            guild_id
            and self.matcher.needs_passive_hydration(
                self.registry.snapshot,
                event_type="message_edit",
                author_id=author_id or None,
                enabled=lambda module: self.policy.module_enabled(guild_id, module),
                watched=watched,
                partial_event=event,
            )
        ):
            if message := await hydrate_message():
                with contextlib.suppress(discord.HTTPException):
                    before = event.get("message_snapshot")
                    event_order = event["event_order"]
                    event = await project_hydrated(message)
                    event["kind"] = "message_edit"
                    event["edited_at_ms"] = received_at_ms
                    event["event_received_at_ms"] = received_at_ms
                    event["event_order"] = event_order
                    event["passive_hydrated"] = True
                    event["_native_edit_fields"] = frozenset(data)
                    if before is not None:
                        event["message_snapshot"] = before
                    if self.matcher.has_passive_candidates(
                        self.registry.snapshot, event_type="message_edit", author_id=author_id or None,
                        context_field="hydrated_message",
                        enabled=lambda module: self.policy.module_enabled(guild_id, module),
                    ):
                        event["hydrated_message"] = capture_message(message)
        await self._dispatch_passive("message_edit", event)
        if guild_id and self.matcher.has_reaction_offer_candidates(
            self.registry.snapshot,
            event_type="message_edit",
            author_id=author_id or None,
            enabled=lambda module: self.policy.module_enabled(guild_id, module),
        ):
            if message := await hydrate_message():
                with contextlib.suppress(discord.HTTPException):
                    full_event = await project_hydrated(message)
                    full_event["edited_at_ms"] = received_at_ms
                    await self._handle_reaction_offer_message(message, full_event, "message_edit")

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(
                getattr(getattr(getattr(payload, "cached_message", None), "author", None), "id", 0)
            ) as permitted:
                if permitted:
                    await LumiEdge._ordinary_raw_message_delete(self, payload)
            return
        return await LumiEdge._ordinary_raw_message_delete(self, payload)

    async def _ordinary_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        await LumiEdge._dispatch_message_delete(
            self, payload.guild_id, payload.channel_id, payload.message_id, payload.cached_message,
            int(time.time() * 1000),
        )

    async def _dispatch_message_delete(
        self, guild_id, channel_id: int, message_id: int, cached_message, received_at_ms: int,
    ) -> None:
        if prepared := getattr(self, "prepared_replies", None):
            with contextlib.suppress(RedisError):
                await prepared.deleted(guild_id, channel_id, message_id)
        if watches := getattr(self, "channel_watches", None):
            with contextlib.suppress(RedisError):
                await watches.deleted(guild_id, channel_id, message_id)
        if bound_replies := getattr(self, "bound_replies", None):
            with contextlib.suppress(RedisError):
                await bound_replies.deleted(guild_id, channel_id, message_id)
        self._offer_message_authors.pop(message_id, None)
        self._passive_message_watches.pop(message_id, None)
        with contextlib.suppress(Exception):
            await self.reaction_offers.remove_message(message_id)
        with contextlib.suppress(Exception):
            await self.cache_redis.delete(f"lumi:passive-watch:{message_id}")
        author_id = int(getattr(getattr(cached_message, "author", None), "id", 0) or 0)
        if author_id and self.blocklist.contains(author_id):
            return
        output_module = None
        if guild_id and cached_message is None and any(
            spec.own_output_deleted
            for registered in self.registry.snapshot.modules.values()
            for spec in registered.manifest.passive
        ):
            with contextlib.suppress(RedisError):
                output_module = await owned_output_module(self.queue_redis, guild_id, channel_id, message_id)
        event = {
            "kind": "message_delete",
            "actor_id": author_id,
            "author_id": author_id,
            "message_id": message_id,
            "guild_id": guild_id,
            "channel_id": channel_id,
            "event_received_at_ms": received_at_ms,
            "event_order": self._next_event_order(),
            "_own_output_deleted": bool(
                output_module or (
                    guild_id and self.user and author_id == self.user.id
                    and getattr(cached_message, "id", None) == message_id
                    and getattr(getattr(cached_message, "channel", None), "id", None) == channel_id
                    and getattr(getattr(cached_message, "guild", None), "id", None) == guild_id
                    and not getattr(cached_message, "webhook_id", None)
                )
            ),
            "_own_output_module": output_module,
        }
        if (
            cached_message is not None
            and guild_id
            and self.matcher.has_passive_candidates(
                self.registry.snapshot,
                event_type="message_delete",
                author_id=author_id or None,
                context_field="message_snapshot",
                enabled=lambda module: self.policy.module_enabled(guild_id, module),
            )
        ):
            snapshot = snapshot_from_message(cached_message)
            if snapshot is not None:
                event["message_snapshot"] = snapshot.context()
        await self._dispatch_passive("message_delete", event)

    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent) -> None:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(
                [message.author.id for message in getattr(payload, "cached_messages", ())]
            ) as permitted:
                if permitted:
                    await LumiEdge._ordinary_raw_bulk_message_delete(self, payload)
            return
        return await LumiEdge._ordinary_raw_bulk_message_delete(self, payload)

    async def _ordinary_raw_bulk_message_delete(
        self, payload: discord.RawBulkMessageDeleteEvent
    ) -> None:
        received_at_ms = int(time.time() * 1000)
        cached_messages = {
            message.id: message for message in getattr(payload, "cached_messages", ())
        }
        for message_id in payload.message_ids:
            await LumiEdge._dispatch_message_delete(
                self, payload.guild_id, payload.channel_id, message_id,
                cached_messages.get(message_id), received_at_ms,
            )

    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        await self._raw_reaction("reaction_add", payload)

    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        await self._raw_reaction("reaction_remove", payload)

    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel) -> None:
        await self._dispatch_resource_delete("channel_delete", channel.guild.id, channel.id)

    async def on_guild_role_delete(self, role: discord.Role) -> None:
        await self._dispatch_resource_delete("role_delete", role.guild.id, role.id)

    async def on_guild_emojis_update(
        self,
        guild: discord.Guild,
        before: tuple[discord.Emoji, ...],
        after: tuple[discord.Emoji, ...],
    ) -> None:
        remaining = {emoji.id for emoji in after}
        for emoji in before:
            if emoji.id not in remaining:
                await self._dispatch_resource_delete("emoji_delete", guild.id, emoji.id)

    async def _dispatch_resource_delete(
        self, event_type: str, guild_id: int, resource_id: int
    ) -> None:
        await self._dispatch_passive(
            event_type,
            {
                "kind": event_type,
                "actor_id": 0,
                "guild_id": guild_id,
                "resource_id": resource_id,
                "event_received_at_ms": int(time.time() * 1000),
            },
        )

    async def _raw_reaction(self, event_type: str, payload: discord.RawReactionActionEvent) -> None:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(payload.user_id) as permitted:
                if permitted:
                    await LumiEdge._ordinary_reaction(self, event_type, payload)
            return
        return await LumiEdge._ordinary_reaction(self, event_type, payload)

    @scoped
    @message_fetch_scope
    async def _ordinary_reaction(
        self, event_type: str, payload: discord.RawReactionActionEvent
    ) -> None:

        if self.blocklist.contains(payload.user_id):
            return
        if self.user and payload.user_id == self.user.id:
            return
        if payload.guild_id is None:
            return
        personal = getattr(self, "personal_reactions", None)
        if personal is not None:
            with contextlib.suppress(RedisError):
                await personal.reaction(event_type, payload)
        emoji = str(payload.emoji)
        offers = self.reaction_offers.records_for(payload.message_id, emoji, event_type)
        durable_reaction = self.reaction_index.contains(payload.message_id)
        if not offers and not durable_reaction:
            return
        fence_key = (
            f"lumi:reaction-fence:{payload.guild_id}:{payload.message_id}:{payload.user_id}:{emoji}"
        )
        try:
            revision = int(await self.cache_redis.incr(fence_key))
            await self.cache_redis.expire(fence_key, 3600)
        except Exception:
            if offers:
                return
            raise
        member_roles = [role.id for role in payload.member.roles] if payload.member else []
        event = {
            "kind": event_type,
            "actor_id": payload.user_id,
            "message_id": payload.message_id,
            "guild_id": payload.guild_id,
            "channel_id": payload.channel_id,
            "emoji": emoji,
            "reaction_revision": revision,
            "member_role_ids": member_roles,
            "event_received_at_ms": int(time.time() * 1000),
        }
        if offers:
            await self._dispatch_reaction_offers(offers, event, payload)
        if durable_reaction:
            await self._dispatch_passive(event_type, event)

    async def _dispatch_reaction_offers(
        self,
        offers: tuple[ReactionOfferRecord, ...],
        event: dict[str, Any],
        payload: discord.RawReactionActionEvent,
    ) -> None:
        remove_actor_reaction = False
        for offered in offers:
            if offered.guild_id != payload.guild_id or offered.channel_id != payload.channel_id:
                continue
            if not self.policy.module_enabled(offered.guild_id, offered.module):
                continue
            route = self.registry.snapshot.reaction_offer_keys.get((offered.module, offered.key))
            if route is None:
                continue
            try:
                if route.spec.prepared_only:
                    if not await self.prepared_replies.claim(
                        route, offered, payload.user_id, member=getattr(payload, "member", None),
                    ):
                        continue
                    claimed = msgspec.structs.replace(offered, owner_id=payload.user_id)
                    unauthorized = False
                else:
                    claimed, unauthorized = await self.reaction_offers.claim(
                        offered.offer_id, payload.user_id
                    )
            except Exception:
                LOGGER.exception(
                    "reaction_offer_claim_failed", extra={"offer_id": offered.offer_id}
                )
                continue
            if claimed is None:
                remove_actor_reaction = remove_actor_reaction or (
                    unauthorized and offered.remove_unauthorized
                )
                continue
            callback_event = dict(claimed.context)
            callback_event.update(event)
            callback_event["offer_id"] = claimed.offer_id
            callback_event["offer_owner_id"] = claimed.owner_id or payload.user_id
            matched = self.matcher.reaction_offer_callback(
                route,
                arguments=dict(claimed.arguments),
                event=callback_event,
            )
            try:
                await self.broker.submit(
                    matched,
                    actor_id=payload.user_id,
                    guild_id=claimed.guild_id,
                    channel_id=claimed.channel_id,
                )
                remove_actor_reaction = remove_actor_reaction or claimed.remove_after_dispatch
            except (QueueFull, QueueUnavailable):
                LOGGER.warning(
                    "reaction_offer_not_admitted",
                    extra={"module_name": claimed.module, "offer_id": claimed.offer_id},
                )
        if remove_actor_reaction and event.get("kind") == "reaction_add":
            await self._remove_actor_reaction(payload)

    async def _remove_actor_reaction(self, payload: discord.RawReactionActionEvent) -> None:
        guild = self.get_guild(payload.guild_id or 0)
        channel = self.get_channel(payload.channel_id)
        if channel is None:
            with contextlib.suppress(discord.HTTPException):
                channel = await self.fetch_channel(payload.channel_id)
        if guild is None or not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return
        me = guild.me
        if me is None or not channel.permissions_for(me).manage_messages:
            return
        with contextlib.suppress(discord.HTTPException):
            message = channel.get_partial_message(payload.message_id)
            await message.remove_reaction(payload.emoji, discord.Object(id=payload.user_id))

    async def _passive_routes(self, event_type: str, event: dict[str, Any]):
        guild_id = event.get("guild_id")
        routes = self.matcher.match_passive(
            self.registry.snapshot,
            event_type=event_type,
            event=event,
            enabled=lambda module: self.policy.module_enabled(guild_id, module),
        )
        admitted = []
        for route in routes:
            if PREPARED_REPLY in route.request.event:
                try:
                    if not await self.prepared_replies.admit(route, event):
                        continue
                except (RedisError, ValueError):
                    continue
            admitted.append(route)
        return tuple(admitted)

    async def _dispatch_passive(self, event_type: str, event: dict[str, Any]) -> None:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(event) as permitted:
                if permitted:
                    await LumiEdge._ordinary_dispatch_passive(self, event_type, event)
            return
        return await LumiEdge._ordinary_dispatch_passive(self, event_type, event)

    async def _ordinary_dispatch_passive(self, event_type: str, event: dict[str, Any]) -> None:
        guild_id = event.get("guild_id")
        for route in await self._passive_routes(event_type, event):
            try:
                actor_id = route.actor_id or int(event.get("actor_id") or 0)
                if actor_id and self.blocklist.contains(actor_id):
                    continue
                if route.request.event.get("bound_reply"):
                    stamp(
                        route, actor_id=actor_id, guild_id=guild_id, channel_id=event.get("channel_id"),
                        snapshot=self.registry.snapshot,
                    )
                await self.broker.submit(
                    route,
                    actor_id=actor_id,
                    guild_id=guild_id,
                    channel_id=event.get("channel_id"),
                )
            except (QueueFull, QueueUnavailable):
                LOGGER.warning(
                    "passive_event_not_admitted",
                    extra={"module_name": route.module, "event": event_type},
                )

    def _next_event_order(self) -> int:
        self._last_event_order = max(self._last_event_order + 1, time.time_ns() // 1_000)
        return self._last_event_order

    def _remember_passive_watch(self, message_id: int, author_id: int, ttl_ms: int) -> None:
        now = time.monotonic()
        deadline = now + ttl_ms / 1_000
        previous = self._passive_message_watches.get(message_id)
        if previous is not None and previous[0] == author_id:
            deadline = max(deadline, previous[1])
        self._passive_message_watches[message_id] = (author_id, deadline)
        self._passive_message_watches.move_to_end(message_id)
        while self._passive_message_watches:
            oldest_id, (_, expires_at) = next(iter(self._passive_message_watches.items()))
            if len(self._passive_message_watches) <= 4_096 and expires_at > now:
                break
            self._passive_message_watches.pop(oldest_id, None)

    def _passive_watch_author(self, message_id: int) -> int | None:
        watched = self._passive_message_watches.get(message_id)
        if watched is None:
            return None
        author_id, expires_at = watched
        if expires_at <= time.monotonic():
            self._passive_message_watches.pop(message_id, None)
            return None
        self._passive_message_watches.move_to_end(message_id)
        return author_id

    async def on_interaction(self, interaction: discord.Interaction) -> None:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None and await ingress.interaction(interaction):
            return
        if ingress is not None:
            async with ingress.gate.track(
                {"actor": interaction.user.id, "data": interaction.data}
            ) as permitted:
                if permitted:
                    await LumiEdge._ordinary_interaction(self, interaction)
            return
        return await LumiEdge._ordinary_interaction(self, interaction)

    @scoped
    @message_fetch_scope
    async def _ordinary_interaction(self, interaction: discord.Interaction) -> None:


        if self.blocklist.contains(interaction.user.id):
            return
        if getattr(interaction, "message", None) is not None:
            remember_message(self, interaction.message)
        data = interaction.data
        if not isinstance(data, dict):
            return
        custom_id = self._component_route_id(data.get("custom_id"))
        if custom_id is None:
            return
        claimed = await self.cache_redis.set(
            f"lumi:interaction-fence:{interaction.id}",
            "1",
            nx=True,
            ex=900,
        )
        if not claimed:
            return
        parts = custom_id.split(":", 3)
        if len(parts) != 4:
            return
        _, namespace, action, _ = parts
        if namespace == "preview":
            await self.preview_forms.handle(interaction, action, parts[3])
            return
        registry_snapshot = self.registry.snapshot
        registered = registry_snapshot.components.get((namespace, action))
        if registered is None:
            return
        scope = registered.spec.owner_scope
        if scope and (self.owner_id <= 0 or interaction.user.id != self.owner_id):
            await self._silence_interaction(interaction)
            return
        if registered.spec.human_guild_member and (
            interaction.guild_id is None
            or not isinstance(interaction.user, discord.Member)
            or interaction.user.guild.id != interaction.guild_id
            or interaction.user.bot
        ):
            await self._silence_interaction(interaction)
            return
        payload = parts[3] if len(parts) == 4 else ""
        session_id = payload.partition("~")[0]
        effective_guild_id = interaction.guild_id
        origin_channel_id = 0
        if effective_guild_id is None and session_id:
            origin_guild = await read_ui(
                self.cache_redis, "origin-guild", session_id, registered.group
            )
            origin_channel = await read_ui(
                self.cache_redis, "origin-channel", session_id, registered.group
            )
            if origin_guild:
                effective_guild_id = int(origin_guild)
            if origin_channel:
                origin_channel_id = int(origin_channel)
        message_id = interaction.message.id if interaction.message else None
        if message_id is None and session_id:
            stored_message = await read_ui(
                self.cache_redis, "message", session_id, registered.group
            )
            if stored_message:
                message_id = int(stored_message)
        component_values = data.get("values")
        if not isinstance(component_values, list):
            component_values = []
        channel_types: dict[str, int] = {}
        resolved_user_ids: set[str] = set()
        resolved_role_ids: set[str] = set()
        resolved = data.get("resolved")
        if isinstance(resolved, dict):
            if isinstance(resolved.get("channels"), dict):
                for identifier, value in resolved["channels"].items():
                    if isinstance(value, dict) and isinstance(value.get("type"), int):
                        channel_types[str(identifier)] = int(value["type"])
            for key in ("users", "members"):
                values = resolved.get(key)
                if isinstance(values, dict):
                    resolved_user_ids.update(str(identifier) for identifier in values)
            roles = resolved.get("roles")
            if isinstance(roles, dict):
                resolved_role_ids.update(str(identifier) for identifier in roles)
        interaction_guild = getattr(interaction, "guild", None)
        modal_submit = interaction.type is discord.InteractionType.modal_submit
        modal = registered.spec.modal
        modal_kinds = {field.kind for field in modal.fields} if modal_submit and modal else ()
        modal_values = self._modal_values(data) if modal_submit else {}
        single_value = next(iter(modal_values.values()), None) if len(modal_values) == 1 else None
        event = {
            "kind": "interaction",
            "actor_id": interaction.user.id,
            "guild_id": effective_guild_id,
            "channel_id": interaction.channel_id,
            "message_id": message_id,
            "origin_channel_id": origin_channel_id,
            "component_values": component_values,
            "component_channel_types": channel_types,
            "component_user_ids": sorted(resolved_user_ids),
            "component_role_ids": sorted(resolved_role_ids),
            "component_guild_role_ids": [
                role.id for role in getattr(interaction_guild, "roles", ())
            ] if "role_select" in modal_kinds else (),
            "component_guild_emojis": self._guild_emoji_catalog(
                getattr(interaction_guild, "emojis", ())
            ) if "emoji" in modal_kinds else (),
            "component_text": single_value if isinstance(single_value, str) else None,
            "component_modal_values": modal_values,
            "component_modal_files": self._modal_files(data) if modal_submit else {},
            "component_modal_submit": modal_submit,
            "member_role_ids": [role.id for role in getattr(interaction.user, "roles", ())],
            "interaction_id": str(interaction.id),
            "interaction_created_at_ms": int(interaction.created_at.timestamp() * 1000),
            "administrator": self._member_access(interaction.user)[1],
        }
        match_error: MatchError | None = None
        try:
            route = self.matcher.match_component(
                registry_snapshot,
                custom_id=custom_id,
                event=event,
            )
        except MatchError as exc:
            match_error = exc
            if (
                isinstance(exc, EmojiLookupMiss)
                and interaction_guild is not None
            ):
                try:
                    fetched_emojis = await interaction_guild.fetch_emojis()
                    event["component_guild_emojis"] = self._guild_emoji_catalog(fetched_emojis)
                    route = self.matcher.match_component(
                        registry_snapshot,
                        custom_id=custom_id,
                        event=event,
                    )
                    match_error = None
                except discord.HTTPException:
                    pass
                except MatchError as retry_exc:
                    match_error = retry_exc
        if match_error is not None:
            LOGGER.info(
                "component input rejected module=%s action=%s reason=%s",
                registered.module,
                action,
                match_error,
            )
            if interaction.type is discord.InteractionType.modal_submit:
                notice = (
                    "Emoji unavailable."
                    if "emoji" in str(match_error).casefold()
                    else "Invalid input."
                )
                with contextlib.suppress(discord.HTTPException):
                    await self._interaction_notice(interaction, notice)
            return
        if route is None:
            return
        if registered.spec.session_bound:
            if not re.fullmatch(r"[a-f0-9]{16,32}", session_id):
                return
            owner = await read_ui(self.cache_redis, "owner", session_id, registered.group)
            if owner is None:
                await self._interaction_notice(interaction, "This menu has expired.")
                return
            if int(owner) != interaction.user.id:
                await self._interaction_notice(interaction, "This menu is not yours.")
                return
            pipeline = self.cache_redis.pipeline(transaction=False)
            remember_session(pipeline, registered.group, session_id)
            pipeline.expire(ui_key("owner", session_id, registered.group), 900)
            pipeline.expire(ui_key("message", session_id, registered.group), 900)
            pipeline.expire(ui_key("origin-guild", session_id, registered.group), 900)
            pipeline.expire(ui_key("origin-channel", session_id, registered.group), 900)
            await pipeline.execute()
        policy_actor: discord.abc.User = interaction.user
        policy_channel = interaction.channel
        if interaction.guild_id is None and effective_guild_id is not None:
            origin_guild = self.get_guild(effective_guild_id)
            if origin_guild is not None:
                member = origin_guild.get_member(interaction.user.id)
                if member is None:
                    with contextlib.suppress(discord.HTTPException):
                        member = await fetch_member(self, origin_guild, interaction.user.id)
                if member is not None:
                    policy_actor = member
                if origin_channel_id:
                    policy_channel = origin_guild.get_channel_or_thread(origin_channel_id)
        roles, administrator = self._member_access(policy_actor)
        if not self._has_permissions(
            policy_actor, registered.spec.required_permissions, policy_channel
        ):
            await self._silence_interaction(interaction)
            return
        if not bot_guild_allowed(
            getattr(policy_channel, "guild", None) or getattr(interaction, "guild", None),
            registered.spec.bot_guild_permissions,
        ):
            if not interaction.response.is_done():
                await interaction.response.send_message("Missing permission.", ephemeral=True)
            return
        if scope == "global":
            pass
        elif registered.spec.guild_control:
            if interaction.guild_id is None or not administrator:
                await self._silence_interaction(interaction)
                return
        else:
            channels, categories = self._channel_access(policy_channel)
            if not self.policy.allowed(
                guild_id=effective_guild_id,
                module=route.module,
                acl_key=registered.spec.acl_key,
                public=registered.spec.public,
                role_ids=roles,
                administrator=administrator,
                channel_ids=channels,
                category_ids=categories,
            ):
                await self._silence_interaction(interaction)
                return
        if registered.spec.private_access_checkpoint:
            route.request.event["_access_checkpoint"] = access_epoch(self)
        if registered.spec.channel_selection is not None:
            from .interaction_context import selected_channels

            guild = self.get_guild(effective_guild_id) if effective_guild_id else None
            facts = selected_channels(
                guild, route.request.arguments.get("values"), registered.spec.channel_selection,
                registered.spec.channel_types,
            )
            if facts is None:
                await self._interaction_notice(interaction, "Unavailable. Try again.")
                return
            route.request.event["_selected_channels"] = facts
        if registered.spec.guild_resources is not None and (
            registered.spec.modal is None or interaction.type is discord.InteractionType.modal_submit
        ):
            from .guild_resources import project

            guild = self.get_guild(effective_guild_id) if effective_guild_id else None
            route.request.event["_guild_resources"] = project(
                guild, registered.spec.guild_resources, route.request.arguments.get("values", ()),
            )
        if (
            registered.spec.modal is not None
            and interaction.type is not discord.InteractionType.modal_submit
        ):
            if not interaction.response.is_done():
                specification = registered.spec.modal
                prefix_field = registered.spec.modal_prefix_field
                if prefix_field is not None:
                    value = self.policy.prefix(effective_guild_id)
                    specification = msgspec.structs.replace(
                        specification,
                        fields=tuple(
                            msgspec.structs.replace(field, value=value)
                            if field.key == prefix_field and field.minimum <= len(value) <= field.maximum
                            else field for field in specification.fields
                        ),
                    )
                if registered.spec.modal_defaults:
                    raw = await self.cache_redis.get(
                        f"lumi:modal-context:{route.module}:{payload}:{action}"
                    )
                    if raw is not None:
                        try:
                            defaults = json.loads(raw)
                            if (
                                defaults["owner"] != interaction.user.id
                                or defaults["guild"] != effective_guild_id
                            ):
                                return
                            if defaults["generation"] != registered.generation:
                                await self._interaction_notice(
                                    interaction, "This menu has expired."
                                )
                                return
                            fields = []
                            for field in specification.fields:
                                value = defaults["values"].get(field.key)
                                if value is not None:
                                    valid = isinstance(value, str) and (
                                        (field.kind == "text" and len(value) <= field.maximum)
                                        or (
                                            field.kind == "string_select"
                                            and value in {c.value for c in field.choices}
                                        )
                                    )
                                    if not valid:
                                        raise ValueError("invalid modal defaults")
                                    field = msgspec.structs.replace(field, value=value)
                                fields.append(field)
                            specification = msgspec.structs.replace(
                                specification, fields=tuple(fields)
                            )
                        except (KeyError, TypeError, ValueError):
                            await self._interaction_notice(interaction, "Unavailable.")
                            return
                await interaction.response.send_modal(
                    self.layouts.modal(
                        custom_id=custom_id,
                        specification=specification,
                    )
                )
            return
        if self.control.maintenance:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    view=self.layouts.text("Maintenance in progress."), ephemeral=True
                )
            return
        if registered.spec.native_selection:
            from .interaction_context import native_string_selection

            selected = native_string_selection(
                interaction.message, data.get("custom_id"), component_values,
                bot_id=getattr(self.user, "id", None),
            )
            if selected is None:
                await self._interaction_notice(interaction, "Invalid selection.")
                return
            route.request.event["_native_selection"] = selected
        if registered.spec.source_message_facts:
            from .interaction_context import component_source

            facts = component_source(
                interaction.message, guild_id=interaction.guild_id,
                channel_id=interaction.channel_id, bot_id=getattr(self.user, "id", None),
                component_ids=self._component_custom_ids,
            )
            if facts is not None:
                route.request.event["_component_message"] = facts
        if not interaction.response.is_done():
            if route.private_loading:
                await interaction.response.defer(ephemeral=True, thinking=True)
            else:
                await interaction.response.defer(ephemeral=registered.spec.ephemeral)
        feedback_spec = registered.spec.private_feedback
        if registered.spec.durable_receipts:
            try:
                recorded = await self.component_receipts.record(route.module, event, payload)
            except Exception:


                recorded = None if feedback_spec is not None else False
            if recorded is False:
                text = feedback_spec.rejected_text if feedback_spec else "Unavailable."
                if route.private_loading:
                    await interaction.edit_original_response(
                        content=text,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                else:
                    await interaction.followup.send(
                        text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
                    )
                return
        project_bot_channel(route, policy_channel, getattr(policy_channel, "guild", None), registered.spec.bot_channel_facts)
        stamp(
            route,
            snapshot=registry_snapshot,
            actor_id=interaction.user.id,
            guild_id=effective_guild_id,
            channel_id=getattr(policy_channel, "id", None),
        )
        job = RedisBroker.prepare(
            route,
            actor_id=interaction.user.id,
            guild_id=effective_guild_id,
            channel_id=interaction.channel_id,
        )


        self._interactions[job.job_id] = interaction
        self._interaction_jobs[job.job_id] = job
        private = getattr(self, "private_loading", None)
        if route.private_loading:
            if private is not None:
                await private.bind(job, interaction, feedback_spec)
                private.animate(job)
            else:

                with contextlib.suppress(discord.HTTPException):
                    await interaction.edit_original_response(
                        content=feedback_spec.timeout_text if feedback_spec else "Unavailable.",
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
        try:
            await self.broker.admit(job, stream=route.stream)
        except (QueueFull, QueueUnavailable):
            self._interactions.pop(job.job_id, None)
            self._interaction_jobs.pop(job.job_id, None)
            if route.private_loading:


                return
            with contextlib.suppress(discord.HTTPException):
                await interaction.followup.send(
                    view=self.layouts.text(
                        "Attempt recorded."
                        if registered.spec.durable_receipts
                        else "Unavailable. Try again."
                    ),
                    ephemeral=True,
                )

    async def _dispatch_activity(self, message: discord.Message) -> None:
        snapshot = self.registry.snapshot
        if self.control.maintenance or not snapshot.activity_observers:
            return
        guild = message.guild.id
        roles, administrator = self._member_access(message.author)
        channels, categories = self._channel_access(message.channel)
        sample = ActivityObservation(
            guild, message.channel.id, message.author.id, message.id,
            int(message.created_at.timestamp() * 1000),
        )
        for module in snapshot.activity_observers:
            registered = snapshot.modules[module]
            if registered.runtime_state != "online":
                continue
            if not self.policy.allowed(
                guild_id=guild,
                module=module,
                acl_key="",
                public=True,
                role_ids=roles,
                administrator=administrator,
                channel_ids=channels,
                category_ids=categories,
            ):
                continue
            with contextlib.suppress(RedisError, ValueError):
                await self.activity_relay.forward(registered.manifest, sample)

    async def _submit(self, route: MatchedRoute, message: discord.Message) -> None:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None and ingress.gate.rejects_submission(
            {"request": route.request, "message": ingress.message_value(message)}
        ):
            return
        actor_id = getattr(route, "actor_id", None) or message.author.id
        blocklist = getattr(self, "blocklist", None)
        if actor_id and blocklist is not None and blocklist.contains(actor_id):
            return
        try:
            loading = getattr(self, "command_loading", None)
            if (
                loading is not None
                and not message.author.bot
                and route.request.event.get("dispatch") == "command"
                and (getattr(route, "loading", False) or loading.for_source(message.id) is not None)
            ):
                await loading.submit(route, message)
            else:
                await self.broker.submit(
                    route,
                    actor_id=actor_id,
                    guild_id=message.guild.id if message.guild else None,
                    channel_id=message.channel.id,
                )
        except QueueFull:
            if not message.author.bot:
                await self._reply(message, "Busy. Try again.")
            return
        except (QueueUnavailable, RedisError):
            if not message.author.bot:
                await self._reply(message, "Unavailable. Try again.")
            return

    async def _hydrate_reply_content(
        self, route: MatchedRoute, message: discord.Message
    ) -> MatchedRoute:
        reply_content = ""
        reference = message.reference
        if reference is not None and reference.message_id:
            resolved = reference.resolved
            replied = resolved if isinstance(resolved, discord.Message) else None
            if unresolved_in_channel(reference, message.channel.id):
                with contextlib.suppress(discord.HTTPException):
                    replied = await fetch_message(
                        self,
                        message.channel,
                        reference.message_id,
                        cached=True,
                    )
            if replied is not None:
                reply_content = replied.content or ""
        event = dict(route.request.event)
        event["reply_content"] = reply_content
        request = route.request.__class__(
            route=route.request.route,
            handler=route.request.handler,
            arguments=route.request.arguments,
            event=event,
        )
        return replace(route, request=request)

    async def _hydrate_image_source(
        self, route: MatchedRoute, message: discord.Message
    ) -> MatchedRoute:


        source = self._image_source_from_message(message, origin="attachment")
        if source is None:
            reference = message.reference
            if reference is not None and reference.message_id:
                resolved = reference.resolved
                replied = resolved if isinstance(resolved, discord.Message) else None
                if unresolved_in_channel(reference, message.channel.id):
                    with contextlib.suppress(discord.HTTPException):
                        replied = await fetch_message(
                            self,
                            message.channel,
                            reference.message_id,
                            cached=True,
                        )
                if replied is not None:
                    source = self._image_source_from_message(replied, origin="reply")
        event = dict(route.request.event)
        event["image_source"] = source
        request = route.request.__class__(
            route=route.request.route,
            handler=route.request.handler,
            arguments=route.request.arguments,
            event=event,
        )
        return replace(route, request=request)

    def _project_reference_embed(self, route: MatchedRoute, message: discord.Message) -> MatchedRoute:

        reference, guild = message.reference, message.guild
        if not reference or not reference.message_id or guild is None:
            return route
        if (
            reference.channel_id != message.channel.id
            or reference.guild_id not in (None, guild.id)
            or reference.type != discord.MessageReferenceType.default
        ):
            observed = None
        elif isinstance(reference.resolved, discord.DeletedReferencedMessage):
            observed = None
        else:
            supplied = reference.resolved if isinstance(reference.resolved, discord.Message) else None
            source = cached_message(self, message.channel, reference.message_id, supplied=supplied)
            if source is None:
                return route
            try:
                observed = embed_source(
                    source, guild_id=guild.id, channel_id=message.channel.id,
                    message_id=reference.message_id,
                )
            except ValueError:
                observed = None
        return replace(route, request=msgspec.structs.replace(
            route.request, event={**route.request.event, "reference_embed": observed},
        ))

    @staticmethod
    def _image_source_from_message(
        message: discord.Message, *, origin: str
    ) -> dict[str, Any] | None:
        for attachment in message.attachments:
            content_type = str(attachment.content_type or "").casefold()
            suffix = Path(attachment.filename or "").suffix.casefold()
            if not content_type.startswith("image/") and suffix not in {
                ".png",
                ".jpg",
                ".jpeg",
                ".webp",
            }:
                continue
            return msgspec.to_builtins(
                ImageSourceContext(
                    url=str(attachment.url)[:4096],
                    content_type=content_type[:100],
                    file_name=str(attachment.filename or "")[:255],
                    size=max(0, int(attachment.size or 0)),
                    width=getattr(attachment, "width", None),
                    height=getattr(attachment, "height", None),
                    origin=origin,
                )
            )
        for embed in message.embeds:
            for media in (embed.image, embed.thumbnail):
                url = getattr(media, "proxy_url", None) or getattr(media, "url", None)
                if not url:
                    continue
                return msgspec.to_builtins(
                    ImageSourceContext(
                        url=str(url)[:4096],
                        width=getattr(media, "width", None),
                        height=getattr(media, "height", None),
                        origin=origin,
                    )
                )
        return None

    @scoped
    async def _complete_job(self, job: JobEnvelope, result: ResultEnvelope) -> None:
        try:
            ingress = getattr(self, "private_ingress", None)
            if ingress is not None:
                async with ingress.gate.track((job, result), internal=True) as permitted:
                    if permitted and not ingress.gate.fence.matches((job, result)):
                        await LumiEdge._complete_job_private(self, job, result)
                return
            return await LumiEdge._complete_job_private(self, job, result)
        finally:
            for intent in result.intents:
                if intent.attachments or intent.attachment_ref:
                    await self.attachment_payloads.discard(job.group, job.job_id, intent)

    async def _complete_job_private(self, job: JobEnvelope, result: ResultEnvelope) -> None:
        async with revision_guard(self, job, result) as current:
            if not current:
                self._interactions.pop(job.job_id, None)
                self._interaction_jobs.pop(job.job_id, None)
                return
            return await LumiEdge._complete_job_gated(self, job, result)

    async def _complete_job_gated(self, job: JobEnvelope, result: ResultEnvelope) -> None:
        gate = getattr(self, "handover", None)
        if gate:
            async with gate.job(job) as permitted:
                if not permitted:
                    self._interactions.pop(job.job_id, None)
                    getattr(self, "_interaction_jobs", {}).pop(job.job_id, None)
                    return
                return await gate.result(
                    job, result, lambda: LumiEdge._complete_job_unguarded(self, job, result)
                )
        return await LumiEdge._complete_job_unguarded(self, job, result)

    async def _complete_job_unguarded(self, job: JobEnvelope, result: ResultEnvelope) -> None:
        if job.request.event.get("_own_output_deleted") is True:
            return
        if any(intent.source_guild_ids for intent in result.intents):
            from .guild_sources import allowed_intent

            if not all([await allowed_intent(self, job, intent) for intent in result.intents]):
                result = msgspec.structs.replace(result, intents=())
        if job.request.event.get("_private_loading") is True:
            private = getattr(self, "private_loading", None)
            try:
                if private is not None and not requires_guard(self, job):
                    intent = None
                    if not private.uses_projection(job.job_id) and result.intents:
                        if len(result.intents) != 1 or result.intents[0].kind != "ephemeral":
                            raise ValueError("private loading requires one private result")
                        intent = self.validator.validate(
                            result.intents[0],
                            presentation_mode=self._job_presentation_mode(job),
                            presentation_modes=self._job_presentation_modes(job),
                        )
                    await private.result(job, intent)
            finally:
                self._interactions.pop(job.job_id, None)
                self._interaction_jobs.pop(job.job_id, None)
                await self.cache_redis.set(
                    f"lumi:response-state:{job.job_id}", "final", ex=response_ttl(job)
                )
            return
        loading = getattr(self, "command_loading", None)
        if loading is not None:
            await loading.wait_recovered()
        try:
            async with self._response_locks.setdefault(job.job_id, asyncio.Lock()):
                state = await self.cache_redis.get(f"lumi:response-state:{job.job_id}")
                if state == b"final" or state == "final":
                    return
                response_bound = job.job_id in self._pending_jobs or job.job_id in getattr(
                    self, "_interaction_jobs", {}
                )
                confirmation = result.error_code == BLOCK_CONFIRMATION
                confirmed = bool(confirmation and loading and await loading.block_notice_allowed(job))
                if confirmed and (projection := getattr(self, "control_projection", None)) is not None:


                    with contextlib.suppress(RedisError, TimeoutError):
                        async with asyncio.timeout(2):
                            await projection.refresh()
                if response_bound and not self._pending_allowed(job, allow_block=confirmed):
                    await self._finish_pending(job, restore=True)
                    await self.cache_redis.set(
                        f"lumi:response-state:{job.job_id}", "final", ex=response_ttl(job)
                    )
                    self._interactions.pop(job.job_id, None)
                    getattr(self, "_interaction_jobs", {}).pop(job.job_id, None)
                    return
                await self._complete_job_locked(job, result, confirmed=confirmed)
        finally:
            self._response_locks.pop(job.job_id, None)

    @message_fetch_scope
    async def _complete_job_locked(
        self, job: JobEnvelope, result: ResultEnvelope, *, confirmed=False,
    ) -> None:
        response_applied = False
        try:
            registered_module = self.registry.snapshot.modules.get(job.module)
            if registered_module is None:
                return
            loading = getattr(self, "command_loading", None)
            confirmation = result.error_code == BLOCK_CONFIRMATION
            if (confirmation and not confirmed) or (
                not confirmation and self.blocklist.contains(job.actor_id)
            ):
                await self._discard_job_response(job)
                return
            if job.request.event.get("dispatch") in {
                "passive",
                "reaction_offer",
                "scheduled",
            } and not self.policy.module_enabled(job.guild_id, job.module):
                return
            if result.prepared_reply_offer is not None:
                if not requires_guard(self, job, registered_module.manifest):
                    await self.prepared_replies.register(job, result)
                return
            if result.intents and PreparedReplies.owns_callback(self.registry.snapshot, job):
                if not await self.prepared_replies.final_allowed(job, result):
                    return
            if not result.intents:
                await self._discard_job_response(job)
                return
            if len(result.intents) != 1:
                raise ValueError("command results must contain exactly one final Discord intent")
            intent = self.validator.validate(
                result.intents[0],
                presentation_mode=self._job_presentation_mode(job),
                presentation_modes=self._job_presentation_modes(job),
            )
            if requires_guard(self, job, registered_module.manifest) and not await reply_allowed(
                self, job, result, claim=True,
            ):
                await self._discard_job_response(job)
                return
            if confirmation:
                if loading is not None:
                    response_applied = await loading.deliver_block_notice(job, intent)
                return
            checkpoint = job.request.event.get("_access_checkpoint")
            if checkpoint is not None and intent.kind not in {"ephemeral", "edit"}:
                return
            if loading is not None:
                await loading.delivering(job)
            if intent.kind == "send_parts":
                await self._send_continuation_parts(job, intent)
                response_applied = True
                if result.status == "ok":
                    await self._retire_completed_reaction_offer(job, None)
                return
            interaction = self._interactions.get(job.job_id)
            interaction_source = getattr(interaction, "message", None)
            private_edit = bool(
                intent.kind == "edit"
                and interaction_source is not None
                and getattr(interaction_source.flags, "ephemeral", False)
                and interaction_source.id == int(job.request.event.get("message_id") or 0)
            )
            if checkpoint is not None and (
                interaction is None or (intent.kind != "ephemeral" and not private_edit)
                or not await private_reply_allowed(self, job, interaction, checkpoint)
            ):
                return
            if intent.kind == "ephemeral" or private_edit:
                if interaction is None:
                    return
                await self._send_private_intent(interaction, job, intent, editing=private_edit)
                response_applied = True
                if result.status == "ok":
                    await self._retire_completed_reaction_offer(job, None)
                return
            channel = self.get_channel(job.channel_id or 0)
            if channel is None and job.channel_id:
                channel = await self.fetch_channel(job.channel_id)
            if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.DMChannel)):
                return
            source_id = int(job.request.event.get("message_id", 0))
            source: discord.Message | None = None
            if source_id:
                source = known_message(self, channel, source_id, supplied=interaction_source)
            if intent.kind == "edit" and source_id and source is None:
                return
            state_key = f"lumi:response-state:{job.job_id}"
            direct = await self.cache_redis.set(
                state_key, "final_direct", nx=True, ex=response_ttl(job)
            )
            if direct:
                sent = await self._send_intent(channel, source, job, intent)
            else:
                state = await self._wait_response_state(state_key)
                if state.startswith("queued:"):
                    queued_id = int(state.partition(":")[2])
                    queued = self._pending_messages.get(job.job_id)
                    if queued is None:
                        queued = known_message(self, channel, queued_id)
                    sent = await self._edit_or_send(queued, channel, source, job, intent)
                elif state in {"final", "final_direct"}:
                    return
                else:
                    sent = await self._send_intent(channel, source, job, intent)
            response_applied = True
            await self.cache_redis.set(state_key, "final", ex=response_ttl(job))
            if intent.session_id:
                await remember_panel(self, job, registered_module.manifest, intent, sent.id)
                await self._bind_ui_session(
                    session_id=intent.session_id,
                    group=job.group,
                    owner_id=job.actor_id,
                    message_id=sent.id,
                    guild_id=job.guild_id,
                    channel_id=job.channel_id,
                )
            if result.status == "ok":
                await self._retire_completed_reaction_offer(job, source)
        finally:
            await self._finish_pending(
                job,
                restore=not response_applied,
            )
            self._interactions.pop(job.job_id, None)
            getattr(self, "_interaction_jobs", {}).pop(job.job_id, None)
            await self.cache_redis.set(
                f"lumi:response-state:{job.job_id}", "final", ex=response_ttl(job)
            )
            if job.request.event.get("dispatch") in {"command", "component"}:
                elapsed_ms = int(time.time() * 1000) - job.admitted_at_ms
                self.processing.observe(float(max(0, elapsed_ms)))

    async def _send_private_intent(
        self,
        interaction: discord.Interaction,
        job: JobEnvelope,
        intent: ValidatedIntent,
        *,
        editing: bool,
    ) -> discord.Message:

        if editing:
            source = interaction.message
            raw = await self.cache_redis.get(f"lumi:ephemeral-family:{source.id}")
            metadata = json.loads(raw) if raw else None
            if (
                not metadata
                or metadata != {"module": job.module, "family": intent.presentation_mode}
                or source.flags.components_v2 != (intent.presentation_mode == "v2")
            ):
                raise ValueError("ephemeral presentation family changed or unbound")
        async with kwargs_for(self, intent, job, for_edit=editing) as kwargs:
            if editing:
                sent = await interaction.edit_original_response(**kwargs)
            else:
                sent = await interaction.followup.send(ephemeral=True, wait=True, **kwargs)
        self._bind_view(kwargs, sent)
        await self.cache_redis.setex(
            f"lumi:ephemeral-family:{sent.id}",
            900,
            json.dumps({"module": job.module, "family": intent.presentation_mode}),
        )
        if intent.session_id:
            await self._bind_ui_session(
                session_id=intent.session_id,
                group=job.group,
                owner_id=job.actor_id,
                message_id=sent.id,
                guild_id=job.guild_id,
                channel_id=job.channel_id,
            )
        return sent

    def _pending_allowed(self, job: JobEnvelope, *, allow_block=False) -> bool:
        if job.module == "_edge":
            root = job.request.handler.removeprefix("edge.")
            return bool(
                root in NATIVE_COMMANDS
                and job.expires_at_ms > int(time.time() * 1000)
                and not self.blocklist.contains(job.actor_id)
                and (root not in OWNER_NATIVE_COMMANDS or job.actor_id == self.owner_id)
            )
        registered = self.registry.snapshot.modules.get(job.module)
        return bool(
            registered is not None
            and registered.manifest.generation == job.generation
            and job.expires_at_ms > int(time.time() * 1000)
            and (allow_block or not self.blocklist.contains(job.actor_id))
            and (
                self._owner_control_job(job)
                or job.request.event.get("_guild_control") is True
                or self.policy.module_enabled(job.guild_id, job.module)
            )
        )

    def _owner_control_job(self, job: JobEnvelope) -> bool:
        spec = owner_job_spec(self.registry.snapshot, job)
        return bool(
            spec is not None and spec.owner_scope == "global"
            and self.owner_id > 0
            and job.actor_id == self.owner_id
            and job.request.event.get("actor_id") == self.owner_id
            and self._job_acl_key(job) == spec.acl_key
            and not self._job_public(job)
        )

    async def _finish_pending(self, job: JobEnvelope, *, restore: bool) -> None:
        loading = getattr(self, "command_loading", None)
        if loading is not None:
            await loading.finish(job, applied=not restore)

    async def _send_continuation_parts(self, job: JobEnvelope, intent: ValidatedIntent) -> None:
        channel = self.get_channel(job.channel_id or 0)
        if channel is None and job.channel_id:
            channel = await self.fetch_channel(job.channel_id)
        if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            return
        source_id = int(job.request.event.get("message_id", 0))
        source: discord.Message | None = None
        if source_id:
            source = known_message(self, channel, source_id)
        first = ValidatedIntent(
            kind="send",
            content=None,
            pages=(intent.pages[0],),
            attachment_ref=None,
            session_id=None,
            presentation_mode=intent.presentation_mode,
        )
        state_key = f"lumi:response-state:{job.job_id}"
        direct = await self.cache_redis.set(
            state_key, "final_direct", nx=True, ex=response_ttl(job)
        )
        if direct:
            await self._send_intent(channel, source, job, first)
        else:
            state = await self._wait_response_state(state_key)
            if state.startswith("queued:"):
                queued = self._pending_messages.get(job.job_id)
                if queued is None:
                    queued = known_message(self, channel, int(state.partition(":")[2]))
                await self._edit_or_send(queued, channel, source, job, first)
            elif state not in {"final", "final_direct"}:
                await self._send_intent(channel, source, job, first)
            else:
                return
        for page in intent.pages[1:]:
            part = ValidatedIntent(
                kind="send",
                content=None,
                pages=(page,),
                attachment_ref=None,
                session_id=None,
                presentation_mode=intent.presentation_mode,
            )
            await channel.send(**self._intent_kwargs(part, job))
        await self.cache_redis.set(state_key, "final", ex=response_ttl(job))

    async def _bind_ui_session(
        self,
        *,
        session_id: str,
        owner_id: int,
        message_id: int,
        guild_id: int | None,
        channel_id: int | None,
        group: str | None = None,
    ) -> None:

        pipeline = self.cache_redis.pipeline(transaction=True)
        bindings = {"owner": str(owner_id), "message": str(message_id)}
        if guild_id:
            bindings["origin-guild"] = str(guild_id)
        if channel_id:
            bindings["origin-channel"] = str(channel_id)
        for kind, value in bindings.items():
            for key in ui_write_keys(kind, session_id, group):
                pipeline.setex(key, 900, value)
        remember_session(pipeline, group, session_id)
        await pipeline.execute()

    async def _discard_job_response(self, job: JobEnvelope) -> None:
        loading = getattr(self, "command_loading", None)
        if loading is not None and await loading.finish(job, silent=True):
            return
        state_key = f"lumi:response-state:{job.job_id}"
        claimed = await self.cache_redis.set(state_key, "final", nx=True, ex=response_ttl(job))
        if claimed:
            return
        state = await self._wait_response_state(state_key)
        if not state.startswith("queued:"):
            await self.cache_redis.set(state_key, "final", ex=response_ttl(job))
            return
        channel = self.get_channel(job.channel_id or 0)
        if channel is None and job.channel_id:
            with contextlib.suppress(discord.HTTPException):
                channel = await self.fetch_channel(job.channel_id)
        if isinstance(channel, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            with contextlib.suppress(discord.HTTPException):
                queued = known_message(self, channel, int(state.partition(":")[2]))
                await queued.delete()
        await self.cache_redis.set(state_key, "final", ex=response_ttl(job))

    async def _retire_completed_reaction_offer(
        self, job: JobEnvelope, source: discord.Message | None
    ) -> None:
        if job.request.event.get("dispatch") != "reaction_offer":
            return
        offer_id = str(job.request.event.get("offer_id") or "")
        if not re.fullmatch(r"[a-f0-9]{32}", offer_id):
            return
        record, remove_discord_reaction = await self.reaction_offers.retire(offer_id)
        if record is None or not remove_discord_reaction or source is None or self.user is None:
            return
        with contextlib.suppress(discord.HTTPException):
            await source.remove_reaction(record.emoji, self.user)

    async def _remove_offer_reaction(self, record: ReactionOfferRecord) -> None:
        if self.user is None:
            return
        channel = self.get_channel(record.channel_id)
        if channel is None:
            with contextlib.suppress(discord.HTTPException):
                channel = await self.fetch_channel(record.channel_id)
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return
        if channel.guild.id != record.guild_id:
            return
        with contextlib.suppress(discord.HTTPException):
            message = channel.get_partial_message(record.message_id)
            await message.remove_reaction(record.emoji, self.user)

    async def _send_intent(
        self,
        channel: discord.abc.Messageable,
        source: discord.Message | None,
        job: JobEnvelope,
        intent: ValidatedIntent,
    ) -> discord.Message:
        if intent.kind == "edit" and source:
            async with kwargs_for(self, intent, job, for_edit=True) as kwargs:
                await source.edit(**kwargs)
            self._bind_view(kwargs, source)
            return source
        async with kwargs_for(self, intent, job) as kwargs:
            if source and (PREPARED_REPLY in job.request.event or isinstance(source, discord.PartialMessage)):


                reference = discord.MessageReference(
                    message_id=source.id,
                    channel_id=job.channel_id,
                    guild_id=job.guild_id,
                    fail_if_not_exists=False,
                )
                sent = await channel.send(**kwargs, reference=reference, mention_author=False)
            elif source:
                sent = await source.reply(**kwargs, mention_author=False)
            else:
                sent = await channel.send(**kwargs)
        self._bind_view(kwargs, sent)
        return sent

    async def _edit_or_send(
        self,
        queued: discord.Message | None,
        channel: discord.abc.Messageable,
        source: discord.Message | None,
        job: JobEnvelope,
        intent: ValidatedIntent,
    ) -> discord.Message:
        if queued:
            try:
                async with kwargs_for(self, intent, job, for_edit=True) as kwargs:
                    sent = await queued.edit(**kwargs)
            except discord.NotFound as exc:
                if exc.code != UNKNOWN_MESSAGE:
                    raise


                return await self._send_intent(channel, source, job, intent)
            self._bind_view(kwargs, sent)
            return sent
        return await self._send_intent(channel, source, job, intent)

    def _intent_kwargs(
        self,
        intent: ValidatedIntent,
        job: JobEnvelope,
        *,
        for_edit: bool = False,
        files: tuple[discord.File, ...] = (),
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"allowed_mentions": discord.AllowedMentions.none()}
        if intent.mention_actor:
            if job.request.event.get("dispatch") != "scheduled" or job.actor_id <= 0:
                raise ValueError("actor mentions are restricted to scheduled callbacks")
            kwargs["allowed_mentions"] = discord.AllowedMentions(
                everyone=False,
                roles=False,
                users=[discord.Object(id=job.actor_id)],
                replied_user=False,
            )
        registered = self.registry.snapshot.modules.get(job.module)
        if registered is None:
            raise ValueError("response module is no longer registered")
        interaction_guard = self._job_interaction_guard(job)
        if for_edit and intent.kind == "send" and intent.content is not None and not intent.pages:


            kwargs["suppress"] = False
        if intent.presentation_mode == "plain":
            kwargs["content"] = (
                f"<@{job.actor_id}> {intent.content}" if intent.mention_actor else intent.content
            )
            if for_edit:
                kwargs["embeds"] = []
            if intent.controls:
                kwargs["view"] = self.layouts.legacy_controls(
                    intent.controls,
                    module=job.module,
                    registry=self.registry.snapshot,
                    owner_id=job.actor_id,
                    blocklist=self.blocklist,
                    interaction_guard=interaction_guard,
                )
            elif for_edit:
                kwargs["view"] = None
        elif intent.presentation_mode == "legacy":
            accent = module_colour(self.registry.snapshot, job.module)
            kwargs["content"] = intent.content
            if intent.kind == "paginator" and len(intent.pages) > 1:
                view = LegacyEmbedPaginator(
                    owner_id=job.actor_id,
                    blocklist=self.blocklist,
                    builder=self.layouts,
                    pages=intent.pages,
                    accent_color=accent,
                    interaction_guard=interaction_guard,
                )
                kwargs["embeds"] = [view.embed]
                kwargs["view"] = view
            else:
                kwargs["embeds"] = self.layouts.legacy_embeds(intent.pages, accent_color=accent)
                if intent.controls:
                    kwargs["view"] = self.layouts.legacy_controls(
                        intent.controls,
                        module=job.module,
                        registry=self.registry.snapshot,
                        owner_id=None if intent.public_controls else job.actor_id,
                        blocklist=self.blocklist,
                        interaction_guard=interaction_guard,
                        timeout=None if intent.public_controls else 900,
                    )
                elif for_edit:
                    kwargs["view"] = None
        else:
            accent = module_colour(self.registry.snapshot, job.module)
            if for_edit:
                kwargs["content"] = None
                kwargs["embeds"] = []
            if intent.kind == "paginator":
                if not intent.pages:
                    raise ValueError("paginator has no pages")
                if len(intent.pages) == 1:
                    kwargs["view"] = self.layouts.build(
                        intent.pages[0],
                        accent_color=accent,
                        module=job.module,
                        registry=self.registry.snapshot,
                        owner_id=job.actor_id if intent.pages[0].controls else None,
                        blocklist=self.blocklist,
                        interaction_guard=interaction_guard,
                    )
                else:
                    view = ComponentsV2Paginator(
                        owner_id=job.actor_id,
                        blocklist=self.blocklist,
                        builder=self.layouts,
                        pages=intent.pages,
                        accent_color=accent,
                        module=job.module,
                        registry=self.registry.snapshot,
                        interaction_guard=interaction_guard,
                    )
                    kwargs["view"] = view
            elif len(intent.pages) == 1:
                kwargs["view"] = self.layouts.build(
                    intent.pages[0],
                    accent_color=accent,
                    module=job.module,
                    registry=self.registry.snapshot,
                    owner_id=job.actor_id if intent.pages[0].controls else None,
                    blocklist=self.blocklist,
                    interaction_guard=interaction_guard,
                )
            elif intent.pages:
                kwargs["view"] = self.layouts.build_many(
                    intent.pages,
                    accent_color=accent,
                    module=job.module,
                    registry=self.registry.snapshot,
                    owner_id=job.actor_id if any(page.controls for page in intent.pages) else None,
                    blocklist=self.blocklist,
                    interaction_guard=interaction_guard,
                )
            else:
                raise ValueError("Components V2 response is empty")
        if len(files) != len(references(intent)):
            raise ValueError("response uploads are not bound")
        if files:
            if for_edit:
                kwargs["attachments"] = list(files)
            elif intent.attachment_ref:
                kwargs["file"] = files[0]
            else:
                kwargs["files"] = list(files)
        elif for_edit:


            kwargs["attachments"] = []
        return kwargs

    @staticmethod
    def _bind_view(kwargs: dict[str, Any], message: discord.Message) -> None:
        view = kwargs.get("view")
        if isinstance(view, (BoundLayoutView, BoundLegacyView)):
            view.message = message

    @staticmethod
    def _job_presentation_modes(job: JobEnvelope) -> tuple[str, ...]:
        raw = job.request.event.get("_presentation_modes")
        if isinstance(raw, (list, tuple)) and raw and all(isinstance(value, str) for value in raw):
            return tuple(raw)
        return (LumiEdge._job_presentation_mode(job),)

    @staticmethod
    def _job_presentation_mode(job: JobEnvelope) -> str:
        raw = job.request.event.get("_presentation_mode")
        return raw if isinstance(raw, str) else "v2"

    @staticmethod
    def _job_acl_key(job: JobEnvelope) -> str:
        raw = job.request.event.get("_acl_key")
        return raw if isinstance(raw, str) else ""

    @staticmethod
    def _job_public(job: JobEnvelope) -> bool:
        raw = job.request.event.get("_public")
        return raw if isinstance(raw, bool) else False

    def _job_interaction_guard(self, job: JobEnvelope):
        async def allowed(interaction: discord.Interaction) -> bool:
            if self._owner_control_job(job):
                accepted = bool(
                    interaction.user.id == self.owner_id
                    and not self.blocklist.contains(interaction.user.id)
                    and interaction.guild_id == job.guild_id
                    and interaction.channel_id == job.channel_id
                )
                if not accepted:
                    await self._silence_interaction(interaction)
                return accepted
            effective_guild_id = interaction.guild_id
            policy_actor: discord.abc.User = interaction.user
            policy_channel = interaction.channel
            if interaction.guild_id is None and job.guild_id is not None:
                origin_guild = self.get_guild(job.guild_id)
                if origin_guild is not None:
                    member = origin_guild.get_member(interaction.user.id)
                    if member is None:
                        with contextlib.suppress(discord.HTTPException):
                            member = await fetch_member(self, origin_guild, interaction.user.id)
                    if member is not None:
                        policy_actor = member
                    origin_channel_id = int(job.request.event.get("origin_channel_id") or 0)
                    if not origin_channel_id and job.request.event.get("dispatch") == "command":
                        origin_channel_id = int(job.channel_id or 0)
                    if origin_channel_id:
                        policy_channel = origin_guild.get_channel_or_thread(origin_channel_id)
                effective_guild_id = job.guild_id
            if effective_guild_id != job.guild_id:
                await self._silence_interaction(interaction)
                return False
            roles, administrator = self._member_access(policy_actor)
            if job.request.event.get("_guild_control") is True:
                accepted = bool(interaction.guild_id is not None and administrator)
            else:
                channels, categories = self._channel_access(policy_channel)
                accepted = self.policy.allowed(
                    guild_id=effective_guild_id,
                    module=job.module,
                    acl_key=self._job_acl_key(job),
                    public=self._job_public(job),
                    role_ids=roles,
                    administrator=administrator,
                    channel_ids=channels,
                    category_ids=categories,
                )
            if not accepted:
                await self._silence_interaction(interaction)
            return accepted

        return allowed

    @staticmethod
    async def _silence_interaction(interaction: discord.Interaction) -> None:
        if interaction.response.is_done():
            return
        with contextlib.suppress(discord.HTTPException, discord.InteractionResponded):
            await interaction.response.defer()

    @scoped
    @message_fetch_scope
    async def _execute_action(self, action: DiscordAction) -> DiscordActionResult:
        try:
            return await LumiEdge._execute_action_scoped(self, action)
        finally:
            intent = action.intent
            transferred = action.channel_resource and action.channel_resource.operation == "publish_post"
            if intent and (intent.attachments or intent.attachment_ref) and not transferred:
                with contextlib.suppress(AttachmentUnavailable):
                    await self.attachment_payloads.discard(
                        action.group, action_scope(action.job_id, action.action_id), intent,
                    )

    async def _execute_action_scoped(self, action: DiscordAction) -> DiscordActionResult:
        ingress = getattr(self, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(action, internal=True) as permitted:
                if not permitted or ingress.gate.fence.matches(action):
                    return self._action_result(action, False, "failed", "admission_pending")
                if ingress.gate.subjects:
                    job = await self.broker.get_job(action.job_id)
                    if job is not None and ingress.gate.fence.matches(job):
                        return self._action_result(action, False, "failed", "admission_pending")
                return await LumiEdge._execute_action_private(self, action)
        return await LumiEdge._execute_action_private(self, action)

    async def _execute_action_private(self, action: DiscordAction) -> DiscordActionResult:
        async def request():
            try:
                return await LumiEdge._execute_action_unguarded(self, action)
            except AttachmentUnavailable:


                return self._action_result(action, False, "failed", "attachment_unavailable")

        gate = getattr(self, "handover", None)
        if gate:
            job = await self.broker.get_job(action.job_id)
            if job:
                transition = None
                if action.kind in CHANNEL_RESOURCE_ACTIONS:
                    from .channel_resources import resource_scope

                    if resource_scope(self.registry.snapshot, job, action) is None:
                        return self._action_result(action, False, "failed", "action_scope_mismatch")
                    transition = action.channel_resource.transition
                async with gate.job(job, action.channel_id, **({"transition": transition} if transition else {})) as permitted:
                    if not permitted:
                        return self._action_result(action, False, "failed", "action_scope_mismatch")
                    return await gate.execute(action, job, request)
        return await request()

    async def _execute_action_unguarded(self, action: DiscordAction) -> DiscordActionResult:
        scoped_delivery = action.intent is not None and bool(action.intent.source_guild_ids)
        if scoped_delivery and action.kind != "send_dm":
            return self._action_result(action, False, "failed", "action_scope_mismatch")
        if action.reaction_revision is not None:
            if not action.message_id or not action.member_id or not action.emoji:
                return self._action_result(action, False, "failed", "invalid_reaction_fence")
            key = (
                f"lumi:reaction-fence:{action.guild_id}:{action.message_id}:"
                f"{action.member_id}:{action.emoji}"
            )
            current = await self.cache_redis.get(key)
            if current is None or int(current) != action.reaction_revision:
                return self._action_result(action, True, "stale")

        job = await self.broker.get_job(action.job_id)
        if job is None:
            return self._action_result(action, False, "failed", "unknown_job")
        if self.blocklist.contains(job.actor_id):
            return self._action_result(action, False, "failed", "blocked")
        if scoped_delivery:
            from .guild_sources import allowed_intent

            if (
                action.member_id != job.actor_id or action.guild_id != job.guild_id
                or action.channel_id != job.channel_id
                or not await allowed_intent(self, job, action.intent)
            ):
                return self._action_result(action, False, "failed", "policy_denied")
        owner_spec = owner_job_spec(self.registry.snapshot, job)
        if owner_spec is not None and owner_spec.owner_scope == "global":
            if not self._owner_control_job(job):
                return self._action_result(action, False, "failed", "policy_denied")
            return await self._edit_owner_control(action, job)
        guild = self.get_guild(action.guild_id)
        if guild is None:
            return self._action_result(action, False, "failed", "guild_unavailable")
        if action.kind in RECOVERY_ACTIONS:


            from .staff_recovery import execute

            return await execute(self, action, job, guild)
        acl = self._job_acl_key(job)
        allowed = admitted(job) or (
            self.policy.command_enabled(action.guild_id, action.module, acl)
            if acl
            else self.policy.module_enabled(action.guild_id, action.module)
        )
        if not allowed:
            return self._action_result(action, False, "failed", "policy_denied")
        if action.kind == "channel_watch":
            return await self.channel_watches.execute(
                action,
                job,
                guild,
                lambda changed: LumiEdge._execute_action_unguarded(self, changed),
            )
        if action.kind == "read_interaction_context":
            from .interaction_context import execute

            return await execute(self, action, job, guild)
        if action.kind == "read_guild_sources":
            from .guild_sources import execute

            return await execute(self, action, job, guild)
        if action.kind == "read_actor_post_channels":
            from .actor_channels import execute

            return await execute(self, action, job, guild)
        if action.kind == "read_owned_history":
            return await self.owned_history.execute(action, job, guild)
        if action.kind == "read_channel_members":
            return await self.channel_member_facts.execute(action, job, guild)
        if action.kind == "read_embed_sources":
            return await self.message_sources.execute(action, job, guild)
        if action.kind == "reply_text":
            return await self.text_replies.execute(action, job, guild)
        if action.kind == "publish_channel_message":
            return await self.channel_publications.execute(action, job, guild)
        if action.kind == "mutate_owned_message":
            return await self.message_mutations.execute(action, job, guild)
        if action.kind == WRITE_LAYOUT_MESSAGE:
            from .layout_messages import execute

            return await execute(self, action, job, guild)
        if action.kind == UPDATE_BOUND_PANEL:
            return await update_bound_panel(self, action, job, guild)
        if action.kind == BIND_MESSAGE_REPLY:
            return await self.bound_replies.execute(action, job, guild)
        if action.kind in {"read_mention_batch_members", "send_mention_batch"}:
            from .mention_batches import execute

            return await execute(self, action, job, guild)
        if action.kind == "delete_source_messages":
            return await self._delete_source_messages(action, job, guild)
        if action.kind == STAFF_EFFECT:
            from .staff_moderation import execute

            return await execute(self, action, job, guild)
        if action.kind in MEMBER_EFFECT_PERMISSIONS:
            from .member_effects import execute

            return await execute(self, action, job, guild)
        if action.kind in MEMBER_PROFILE_ACTIONS:
            from .member_profiles import execute

            return await execute(self, action, job, guild)
        if action.kind in CHANNEL_MESSAGE_ACTIONS:
            from .channel_messages import execute

            return await execute(self, action, job, guild)
        if action.kind == READ_CHANNEL_CONFIGURATION:
            from .channel_configuration import execute

            return await execute(self, action, job, guild)
        if action.kind in CHANNEL_RESOURCE_ACTIONS:
            from .channel_resources import execute

            return await execute(self, action, job, guild)
        if action.kind in ROLE_FACTS:
            from .role_access import execute

            return await execute(self, action, job, guild)
        if action.kind in ACTOR_ROLE_ACTIONS:
            from .role_access import execute_actor_roles

            return await execute_actor_roles(self, action, job, guild)
        if action.kind == "search_members":
            return await search_members(self, action, job, guild)
        if action.kind == "resolve_members":
            if (
                len(action.member_ids) > 1000
                or any(value <= 0 for value in action.member_ids)
                or len(set(action.member_ids)) != len(action.member_ids)
            ):
                return self._action_result(action, False, "failed", "invalid_members")
            resolved_members = []
            for offset in range(0, len(action.member_ids), 100):
                identifiers = action.member_ids[offset : offset + 100]
                members = await lookup(self).query(guild, identifiers)
                for member in members:
                    context_channel = guild.get_channel_or_thread(
                        action.channel_id or job.channel_id or 0
                    )
                    channels, categories = self._channel_access(context_channel)
                    roles, administrator = self._member_access(member)
                    resolved_members.append(
                        {
                            "id": member.id,
                            "bot": member.bot,
                            "blocked": self.blocklist.contains(member.id),
                            "administrator": member.guild_permissions.administrator,
                            "allowed": self.policy.allowed(
                                guild_id=action.guild_id,
                                module=action.module,
                                acl_key=acl,
                                public=True,
                                role_ids=roles,
                                administrator=administrator,
                                channel_ids=channels,
                                category_ids=categories,
                            ),
                        }
                    )
            return self._action_result(action, True, "resolved", data={"members": resolved_members})
        if action.kind == "send_dm":
            if action.member_id is None or action.intent is None:
                return self._action_result(action, False, "failed", "missing_dm_payload")
            try:
                member = await fetch_member(self, guild, action.member_id)
            except (discord.NotFound, discord.Forbidden):
                return self._action_result(action, False, "failed", "member_unavailable")
            intent = self.validator.validate(
                action.intent,
                presentation_mode=(
                    self.registry.snapshot.modules[action.module].manifest.action_presentation_modes
                    or (self._job_presentation_mode(job),)
                )[0],
                presentation_modes=self.registry.snapshot.modules[
                    action.module
                ].manifest.action_presentation_modes
                or self._job_presentation_modes(job),
            )
            try:
                async with kwargs_for(self, intent, job, action=action) as kwargs:
                    sent = await member.send(**kwargs)
                await remember_owned_output(
                    self.queue_redis, guild_id=action.guild_id, channel_id=sent.channel.id,
                    message_id=sent.id, module=action.module, family=intent.presentation_mode,
                    ttl=366 * 86400
                    if any(
                        spec.reply_prompt
                        for spec in self.registry.snapshot.modules[action.module].manifest.passive
                    )
                    else 7 * 86400,
                )
                self._bind_view(kwargs, sent)
                if intent.session_id:
                    await self._bind_ui_session(
                        session_id=intent.session_id,
                        group=job.group,
                        owner_id=action.member_id,
                        message_id=sent.id,
                        guild_id=action.guild_id,
                        channel_id=action.channel_id or job.channel_id,
                    )
                return self._action_result(
                    action,
                    True,
                    "sent",
                    data={"message_id": sent.id, "channel_id": sent.channel.id},
                )
            except discord.Forbidden:
                return self._action_result(action, False, "failed", "dm_forbidden")
            except discord.HTTPException as exc:
                code = "ambiguous" if exc.status >= 500 else "discord_rejected"
                return self._action_result(action, False, "failed", code)

        if action.kind in {"send_message", "send_role_message"}:
            if action.channel_id is None or action.intent is None:
                return self._action_result(action, False, "failed", "missing_send_payload")
            role_message = action.kind == "send_role_message"
            if role_message and (
                type(action.role_id) is not int
                or action.role_id <= 0
                or action.role_id == guild.id
                or action.role_ids
                or action.intent.kind != "send"
                or action.intent.content is not None
                or action.intent.mention_actor
            ):
                return self._action_result(action, False, "failed", "invalid_role_message")
            channel = await self._action_channel(guild, action.channel_id)
            if channel is None:
                return self._action_result(action, False, "failed", "channel_unavailable")
            me = guild.me
            permissions = (
                channel.permissions_for(me) if me is not None else discord.Permissions.none()
            )
            can_send = permissions.send_messages or permissions.send_messages_in_threads
            if not permissions.view_channel or not can_send:
                return self._action_result(action, False, "failed", "permission_denied")
            source = None
            if action.forbid_source_channel or (action.respect_nsfw and not self._channel_is_nsfw(channel)):
                source = (
                    channel if channel.id == job.channel_id
                    else await self._action_channel(guild, job.channel_id)
                )
            if action.forbid_source_channel:
                forbidden = {int(job.channel_id or 0)}
                source_parent = getattr(source, "parent", None)
                if source_parent is not None:
                    forbidden.add(int(source_parent.id))
                if channel.id in forbidden:
                    return self._action_result(action, False, "failed", "source_destination_match")
            if (
                action.respect_nsfw
                and self._channel_is_nsfw(source)
                and not self._channel_is_nsfw(channel)
            ):
                return self._action_result(action, False, "failed", "nsfw_boundary")
            intent = self.validator.validate(
                action.intent,
                presentation_mode=(
                    self.registry.snapshot.modules[action.module].manifest.action_presentation_modes
                    or (self._job_presentation_mode(job),)
                )[0],
                presentation_modes=self.registry.snapshot.modules[
                    action.module
                ].manifest.action_presentation_modes
                or self._job_presentation_modes(job),
            )
            if role_message and intent.presentation_mode != "legacy":
                return self._action_result(action, False, "failed", "invalid_role_message")
            try:
                async with kwargs_for(self, intent, job, action=action) as kwargs:
                    notified = False
                    if role_message:
                        role = guild.get_role(action.role_id)
                        if (
                            role is not None
                            and role.guild.id == guild.id
                            and not role.is_default()
                            and not role.managed
                            and (role.mentionable or permissions.mention_everyone)
                        ):
                            kwargs["content"] = f"<@&{role.id}>"
                            kwargs["allowed_mentions"] = discord.AllowedMentions(
                                everyone=False,
                                users=False,
                                roles=[discord.Object(id=role.id)],
                                replied_user=False,
                            )
                            notified = True
                        else:
                            LOGGER.warning("role_message_ping_unavailable")
                    try:
                        sent = await channel.send(**kwargs)
                    except discord.Forbidden:
                        if not notified:
                            raise


                        kwargs["content"] = None
                        kwargs["allowed_mentions"] = discord.AllowedMentions.none()
                        notified = False
                        LOGGER.warning("role_message_ping_unavailable")
                        sent = await channel.send(**kwargs)
                await remember_owned_output(
                    self.queue_redis, guild_id=action.guild_id, channel_id=channel.id,
                    message_id=sent.id, module=action.module, family=intent.presentation_mode,
                    ttl=366 * 86400
                    if any(
                        spec.reply_prompt
                        for spec in self.registry.snapshot.modules[action.module].manifest.passive
                    )
                    else 7 * 86400,
                )
                self._bind_view(kwargs, sent)
                data = {"message_id": sent.id, "channel_id": sent.channel.id}
                if role_message:
                    data["role_notified"] = notified
                return self._action_result(
                    action,
                    True,
                    "sent",
                    data=data,
                )
            except discord.Forbidden:
                return self._action_result(action, False, "failed", "permission_denied")
            except discord.HTTPException as exc:
                code = "ambiguous" if exc.status >= 500 else "discord_rejected"
                return self._action_result(action, False, "failed", code)

        if action.kind == "send_recipient_webhook":
            return await self.webhooks.execute(action, job, guild)
        if action.kind in {
            "fetch_message",
            "create_message",
            "edit_message",
            "delete_message",
            "add_reaction",
            "remove_reaction",
        }:
            channel = await self._action_channel(guild, action.channel_id)
            if channel is None:
                return self._action_result(action, False, "failed", "channel_unavailable")
            me = guild.me
            permissions = (
                channel.permissions_for(me) if me is not None else discord.Permissions.none()
            )
            can_read = permissions.view_channel and permissions.read_message_history
            can_send = permissions.send_messages or permissions.send_messages_in_threads
            if action.kind == "create_message":
                if action.page is None:
                    return self._action_result(action, False, "failed", "missing_presentation")
                if not permissions.view_channel or not can_send:
                    return self._action_result(action, False, "failed", "permission_denied")
                view = await self._action_view(action)
                message = await channel.send(
                    view=view,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                await remember_owned_output(
                    self.queue_redis, guild_id=guild.id, channel_id=channel.id, message_id=message.id,
                    module=action.module, family="v2", ttl=7 * 86400,
                )
                self._bind_view({"view": view}, message)
                return self._action_result(
                    action,
                    True,
                    "created",
                    data={"message_id": message.id, "channel_id": message.channel.id},
                )
            if action.message_id is None:
                return self._action_result(action, False, "failed", "missing_message")
            if not can_read:
                return self._action_result(action, False, "failed", "permission_denied")
            binding, indexed, attached_source = None, False, None
            if action.kind in {"edit_message", "delete_message"}:
                from .interaction_context import admitted_component_source

                binding = await action_message_binding(self.queue_redis, guild.id, action.message_id)
                if binding and (
                    binding["module"] != action.module
                    or binding.get("output_channel", channel.id) != channel.id
                ):
                    return self._action_result(action, False, "failed", "message_not_owned")
                indexed = bool(binding and binding.get("output_channel") == channel.id)
                if not indexed:
                    attached_source = admitted_component_source(self, job, channel.id, action.message_id)
            proofs = getattr(self, "message_author_proofs", None)
            proven_author = bool(
                proofs is not None and action.read_proof and action.kind == "edit_message"
                and action.page is not None and action.intent is None
                and proofs.valid_until(action, message_author_scope(channel.id, action.message_id))
            )
            known = indexed or attached_source is not None or proven_author
            if known or action.kind in {"add_reaction", "remove_reaction"}:
                message = channel.get_partial_message(action.message_id)
            else:
                try:


                    message = await fetch_message(
                        self, channel, action.message_id,
                        cached=action.kind == "delete_message" or (
                            action.kind == "edit_message" and action.intent is None
                        ),
                    )
                except discord.NotFound as exc:
                    if action.kind == "delete_message" and exc.code == 10008:
                        return self._action_result(action, True, "deleted")
                    return self._action_result(
                        action, False, "failed", "message_unavailable",
                        data={"deleted": True} if exc.code == 10008 else None,
                    )
                except discord.Forbidden:
                    return self._action_result(action, False, "failed", "message_unavailable")
            if action.kind == "fetch_message":
                facts = self._message_snapshot(guild, channel, message, action.role_ids, action.emoji_keys)
                if action.prove_message_author and proofs is not None and facts["current_lumi"] and not facts["webhook"]:
                    facts["author_proof"] = proofs.issue(action, message_author_scope(channel.id, message.id))
                return self._action_result(
                    action,
                    True,
                    "found",
                    data=facts,
                )
            if action.kind in {"edit_message", "delete_message"}:
                if (
                    self.user is None
                    or (not known and (message.author.id != self.user.id or message.webhook_id is not None))
                ):
                    return self._action_result(action, False, "external", "message_not_editable")
                if action.kind == "delete_message":
                    try:
                        await message.delete()
                    except discord.NotFound as exc:
                        if exc.code != 10008:
                            return self._action_result(action, False, "failed", "message_unavailable")
                    return self._action_result(action, True, "deleted")
                if action.intent is not None:
                    intent = self.validator.validate(
                        action.intent,
                        presentation_mode=(
                            self.registry.snapshot.modules[
                                action.module
                            ].manifest.action_presentation_modes
                            or (self._job_presentation_mode(job),)
                        )[0],
                        presentation_modes=self.registry.snapshot.modules[
                            action.module
                        ].manifest.action_presentation_modes
                        or self._job_presentation_modes(job),
                    )
                    declared = self.registry.snapshot.modules[
                        action.module
                    ].manifest.action_presentation_modes
                    observed_v2 = None
                    if not indexed:
                        observed_v2 = (
                            attached_source["components_v2"] if attached_source is not None
                            else message.flags.components_v2
                        )
                    if binding:
                        existing_family = binding.get("family")
                    else:


                        existing_family = (
                            "v2"
                            if observed_v2
                            else declared[0]
                            if len(declared) == 1
                            else self._job_presentation_mode(job)
                            if not declared
                            else None
                        )
                    if existing_family != intent.presentation_mode or (
                        not indexed and observed_v2 != (existing_family == "v2")
                    ):
                        return self._action_result(
                            action, False, "failed", "presentation_family_changed"
                        )
                    try:
                        async with kwargs_for(self, intent, job, for_edit=True, action=action) as kwargs:
                            edited = await message.edit(**kwargs)
                    except discord.NotFound as exc:
                        return self._action_result(
                            action, False, "failed", "message_unavailable",
                            data={"deleted": True} if exc.code == 10008 else None,
                        )
                    self._bind_view(kwargs, edited or message)
                    return self._action_result(action, True, "edited")
                if action.page is None:
                    return self._action_result(action, False, "failed", "missing_presentation")
                view = await self._action_view(action)
                try:
                    edited = await message.edit(
                        content=None, embeds=[], view=view, allowed_mentions=discord.AllowedMentions.none(),
                    )
                except discord.NotFound as exc:
                    return self._action_result(
                        action, False, "failed", "message_unavailable",
                        data={"deleted": True} if exc.code == 10008 else None,
                    )
                self._bind_view({"view": view}, edited or message)
                return self._action_result(
                    action, True, "edited", data=self._native_edit_receipt(edited),
                )
            if not action.emoji:
                return self._action_result(action, False, "failed", "missing_emoji")
            emoji = discord.PartialEmoji.from_str(action.emoji)
            if action.kind == "add_reaction":
                if not permissions.add_reactions:
                    return self._action_result(action, False, "failed", "permission_denied")
                try:
                    await message.add_reaction(emoji)
                except discord.NotFound as exc:
                    return self._action_result(
                        action, False, "failed", "message_unavailable",
                        data={"deleted": True} if exc.code == 10008 else None,
                    )
                except discord.Forbidden:
                    return self._action_result(action, False, "failed", "message_unavailable")
                return self._action_result(action, True, "added")
            if self.user is None:
                return self._action_result(action, False, "failed", "bot_unavailable")
            try:
                await message.remove_reaction(emoji, self.user)
            except discord.NotFound as exc:
                return self._action_result(
                    action, False, "failed", "message_unavailable",
                    data={"deleted": True} if exc.code == 10008 else None,
                )
            except discord.Forbidden:
                return self._action_result(action, False, "failed", "message_unavailable")
            return self._action_result(action, True, "removed")

        if action.kind in {"add_member_role", "remove_member_role"}:
            return await self._execute_role_action(guild, action)
        return self._action_result(action, False, "failed", "unsupported_action")

    async def _edit_owner_control(
        self, action: DiscordAction, job: JobEnvelope
    ) -> DiscordActionResult:

        token = job.request.arguments.get("session")
        if (
            self.control.maintenance
            or job.request.event.get("dispatch") != "component"
            or action.kind != "edit_message"
            or action.intent is None
            or action.intent.kind != "edit"
            or action.intent.session_id != token
            or not isinstance(token, str)
            or re.fullmatch(r"[a-f0-9]{16}", token) is None
            or action.guild_id != job.guild_id
            or action.channel_id != job.channel_id
            or action.message_id != job.request.event.get("message_id")
        ):
            return self._action_result(action, False, "failed", "action_scope_mismatch")
        bound = await read_ui(self.cache_redis, "message", token, job.group)
        owner = await read_ui(self.cache_redis, "owner", token, job.group)
        if not bound or int(bound) != action.message_id or not owner or int(owner) != self.owner_id:
            return self._action_result(action, False, "failed", "session_unavailable")
        channel = self.get_channel(action.channel_id or 0)
        if channel is None and action.channel_id:
            channel = await self.fetch_channel(action.channel_id)
        if (
            not isinstance(channel, (discord.TextChannel, discord.Thread, discord.DMChannel))
            or getattr(getattr(channel, "guild", None), "id", None) != job.guild_id
            or (
                isinstance(channel, discord.DMChannel)
                and getattr(channel.recipient, "id", None) != self.owner_id
            )
        ):
            return self._action_result(action, False, "failed", "channel_unavailable")
        intent = self.validator.validate(action.intent, presentation_mode="v2")
        if self.user is None:
            return self._action_result(action, False, "failed", "message_not_editable")
        message = channel.get_partial_message(action.message_id)
        try:
            async with kwargs_for(self, intent, job, for_edit=True, action=action) as kwargs:
                edited = await message.edit(**kwargs)
        except (discord.NotFound, discord.Forbidden):
            return self._action_result(action, False, "failed", "message_unavailable")
        self._bind_view(kwargs, edited or message)
        return self._action_result(action, True, "edited")

    async def _action_view(self, action: DiscordAction) -> BoundLayoutView:
        registered = self.registry.snapshot.modules.get(action.module)
        if registered is None or action.page is None:
            raise ValueError("action module is unavailable")
        job = await self.broker.get_job(action.job_id)
        if job is None:
            raise ValueError("action job is unavailable")
        validated = self.validator.validate(
            DiscordIntent(kind="send", pages=(action.page,)),
            presentation_mode="v2",
        )
        return self.layouts.build(
            validated.pages[0],
            accent_color=module_colour(self.registry.snapshot, action.module),
            module=action.module,
            registry=self.registry.snapshot,
            owner_id=None if action.persistent else job.actor_id,
            blocklist=self.blocklist,
            timeout=None if action.persistent else 900,
        )

    async def _action_channel(
        self, guild: discord.Guild, channel_id: int | None
    ) -> discord.TextChannel | discord.Thread | None:
        return await memo_read(
            "action-channel",
            (id(self), guild.id, channel_id),
            lambda: LumiEdge._read_action_channel(self, guild, channel_id),
        )

    async def _read_action_channel(
        self, guild: discord.Guild, channel_id: int | None
    ) -> discord.TextChannel | discord.Thread | None:
        if channel_id is None:
            return None
        channel = guild.get_channel_or_thread(channel_id)
        if channel is None:
            with contextlib.suppress(discord.HTTPException):
                fetched = await self.fetch_channel(channel_id)
                if getattr(fetched, "guild", None) == guild:
                    channel = fetched
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            return channel
        return None

    @staticmethod
    def _channel_is_nsfw(channel: object) -> bool:
        target = channel
        parent = getattr(channel, "parent", None)
        if parent is not None and not callable(getattr(target, "is_nsfw", None)):
            target = parent
        check = getattr(target, "is_nsfw", None)
        return bool(check()) if callable(check) else False

    async def _execute_role_action(
        self, guild: discord.Guild, action: DiscordAction
    ) -> DiscordActionResult:
        if action.member_id is None:
            return self._action_result(action, False, "failed", "missing_role_target")
        me = guild.me
        if me is None or not me.guild_permissions.manage_roles:
            return self._action_result(action, False, "failed", "role_unavailable")
        reason = (action.reason or "Lumi role action")[:512]
        if action.role_id is None:
            return self._action_result(action, False, "failed", "missing_role_target")
        role = guild.get_role(action.role_id)
        if role is None or role.is_default() or role.managed or me.top_role <= role:
            return self._action_result(action, False, "failed", "role_unavailable")

        adding = action.kind == "add_member_role"
        request = self.http.add_role if adding else self.http.remove_role
        try:
            with member_mutation(self, guild, action.member_id):
                await request(guild.id, action.member_id, role.id, reason=reason)
        except (discord.NotFound, discord.Forbidden):
            return self._action_result(action, False, "failed", "member_unavailable")
        return self._action_result(action, True, "added" if adding else "removed")

    def _message_snapshot(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel | discord.Thread,
        message: discord.Message,
        role_ids: tuple[int, ...],
        emoji_keys: tuple[str, ...],
    ) -> dict[str, Any]:
        me = guild.me
        permissions = channel.permissions_for(me) if me else discord.Permissions.none()
        role_state = {
            str(identifier): {
                "exists": (role := guild.get_role(identifier)) is not None,
                "manageable": bool(
                    role
                    and me
                    and me.guild_permissions.manage_roles
                    and not role.managed
                    and not role.is_default()
                    and me.top_role > role
                ),
            }
            for identifier in role_ids[:20]
        }
        emoji_state: dict[str, bool] = {}
        for value in emoji_keys[:20]:
            parsed = discord.PartialEmoji.from_str(value)
            emoji = self.get_emoji(parsed.id) if parsed.id is not None else None
            emoji_state[value] = parsed.id is None or bool(
                emoji
                and emoji.guild_id == guild.id
                and emoji.available
                and (
                    not emoji.roles
                    or (me is not None and any(role in me.roles for role in emoji.roles))
                )
            )
        components = [component.to_dict() for component in message.components]
        return {
            "guild_id": guild.id,
            "channel_id": channel.id,
            "message_id": message.id,
            "author_id": message.author.id,
            "author_name": str(message.author),
            "author_avatar_url": str(message.author.display_avatar.url),
            "current_lumi": bool(self.user and message.author.id == self.user.id),
            "webhook": message.webhook_id is not None,
            "message_type": int(message.type.value),
            "content": message.content[:4000],
            "embeds": [embed.to_dict() for embed in message.embeds[:10]],
            "components": components,
            "component_custom_ids": self._component_custom_ids(components),
            "attachments": [
                {
                    "id": attachment.id,
                    "filename": attachment.filename,
                    "url": attachment.url,
                    "size": attachment.size,
                }
                for attachment in message.attachments[:10]
            ],
            "flags": message.flags.value,
            "components_v2": message.flags.components_v2,
            "reactions": [
                {"emoji": str(reaction.emoji), "count": reaction.count, "me": reaction.me}
                for reaction in message.reactions[:20]
            ],
            "created_at_ms": int(message.created_at.timestamp() * 1000),
            "permissions": {
                "view_channel": permissions.view_channel,
                "read_message_history": permissions.read_message_history,
                "send_messages": permissions.send_messages,
                "embed_links": permissions.embed_links,
                "add_reactions": permissions.add_reactions,
                "manage_messages": permissions.manage_messages,
            },
            "roles": role_state,
            "emojis": emoji_state,
        }

    @staticmethod
    def _native_edit_receipt(message) -> dict[str, Any]:

        if not isinstance(message, discord.Message):
            return {}
        return {
            "message_id": message.id,
            "channel_id": message.channel.id,
            "components_v2": message.flags.components_v2,
            "component_custom_ids": LumiEdge._component_custom_ids(
                [component.to_dict() for component in message.components],
            ),
        }

    @staticmethod
    def _component_custom_ids(components: list[dict[str, Any]]) -> tuple[str, ...]:
        identifiers: list[str] = []
        stack: list[Any] = list(components)
        while stack:
            item = stack.pop()
            if not isinstance(item, dict):
                continue
            custom_id = item.get("custom_id")
            if isinstance(custom_id, str):
                identifiers.append(custom_id)
            children = item.get("components")
            if isinstance(children, list):
                stack.extend(children)
            child = item.get("component")
            if isinstance(child, dict):
                stack.append(child)
        return tuple(identifiers)

    @staticmethod
    def _action_result(
        action: DiscordAction,
        ok: bool,
        status: str,
        error_code: str | None = None,
        *,
        data: dict[str, Any] | None = None,
    ) -> DiscordActionResult:
        return DiscordActionResult(
            schema=1,
            action_id=action.action_id,
            ok=ok,
            status=status,
            data=data or {},
            error_code=error_code,
            completed_at_ms=int(time.time() * 1000),
        )

    async def _wait_response_state(self, key: str) -> str:
        for _ in range(30):
            state = await self.cache_redis.get(key)
            if isinstance(state, bytes):
                state = state.decode()
            if state != "queueing":
                return str(state or "")
            await asyncio.sleep(0.05)
        return ""

    async def _native(self, message: discord.Message, root: str, arguments: str) -> None:
        if root in {"block", "unblock", "restart", "drain", "maintenance"}:
            if message.author.id != self.owner_id:
                return
        if root == "help":
            await self._help(message, arguments)
        elif root == "lumi":
            await self._reply(message, page=lumi_page(self._invite_url()))
        elif root == "ping":
            await self._ping(message)
        elif root in {"block", "unblock"}:
            await self._blocking(message, root, arguments)
        elif root == "restart":
            await self._restart(message, arguments)
        elif root == "drain":
            await self._drain(message, arguments)
        elif root == "maintenance":
            await self._maintenance(message, arguments)

    def _help_entries(self, actor, guild_id: int | None, channel) -> tuple[HelpEntry, ...]:
        snapshot = self._command_snapshot(guild_id)
        prefix = self.policy.prefix(guild_id)
        roles, administrator = self._member_access(actor)
        channels, categories = self._channel_access(channel)
        entries = [
            HelpEntry(
                module="_edge",
                module_name="Lumi",
                root=root,
                usage=self._format_help_usage(spec.help.usage, prefix),
                summary=spec.help.summary,
                colour=LUMI_COLOUR,
            )
            for root, spec in NATIVE_COMMANDS.items()
            if spec.help and not spec.help.hidden
        ]
        for root, command in sorted(snapshot.commands.items()):
            help_spec = command.spec.help
            if root != command.spec.root or help_spec is None or help_spec.hidden:
                continue
            if not self._help_visible(
                command,
                actor=actor,
                guild_id=guild_id,
                roles=roles,
                administrator=administrator,
                channels=channels,
                categories=categories,
                channel=channel,
            ):
                continue
            module = snapshot.modules[command.module]
            entries.append(
                HelpEntry(
                    module=command.module,
                    module_name=module.manifest.display_name,
                    root=root,
                    usage=self._format_help_usage(help_spec.usage, prefix),
                    summary=help_spec.summary,
                    colour=module_colour(snapshot, command.module),
                    aliases=command.spec.aliases,
                    unavailable=module.runtime_state != "online",
                )
            )
        for key, module in sorted(snapshot.modules.items()):
            help_spec = module.manifest.help
            if help_spec is None or help_spec.hidden:
                continue
            if not administrator and module.runtime_state != "online":
                continue
            if not self.policy.allowed(
                guild_id=guild_id,
                module=key,
                acl_key="",
                public=True,
                role_ids=roles,
                administrator=administrator,
                channel_ids=channels,
                category_ids=categories,
            ):
                continue
            entries.append(
                HelpEntry(
                    module=key,
                    module_name=module.manifest.display_name,
                    root=f"{key}:tool",
                    usage=help_spec.usage,
                    summary=help_spec.summary,
                    colour=module_colour(snapshot, key),
                    aliases=(key,),
                    unavailable=module.runtime_state != "online",
                    is_command=False,
                )
            )
        return tuple(
            sorted(
                entries,
                key=lambda row: (row.module != "_edge", row.module_name.casefold(), row.root),
            )
        )

    async def _help(self, message: discord.Message, query: str) -> None:
        guild_id = message.guild.id if message.guild else None
        entries = self._help_entries(message.author, guild_id, message.channel)

        def refresh(interaction: discord.Interaction) -> tuple[HelpEntry, ...]:
            return self._help_entries(
                interaction.user, guild_id, interaction.channel or message.channel
            )

        view = HelpView(
            owner_id=message.author.id,
            blocklist=self.blocklist,
            builder=self.layouts,
            entries=entries,
            refresh=refresh,
            prefix=self.policy.prefix(guild_id),
            query=query,
            guild_id=guild_id,
            channel_id=message.channel.id,
        )
        sent = await self._reply_with_kwargs(message, {"view": view})
        view.message = sent

    async def _ping(self, message: discord.Message) -> None:
        summary = self.processing.summary()
        modules = sorted(self.registry.snapshot.modules.items())
        chunks = [modules[index : index + 8] for index in range(0, len(modules), 8)] or [[]]
        pages: list[PresentationPage] = []
        for chunk in chunks:
            rows = []
            for name, module in chunk:
                group = module.manifest.group
                health = self.control.health[group]
                state = health.state if module.runtime_state == "online" else module.runtime_state
                latency = (
                    f"{health.round_trip_ms:.0f} ms"
                    if health.round_trip_ms is not None
                    else "unavailable"
                )
                queue_depth = self.queue_depths.get(group, 0)
                rows.append(f"{name} - {state}; worker {latency}; queue {queue_depth}")
            average = (
                "No samples"
                if summary.average_ms is None
                else f"{summary.average_ms:.0f} ms ({summary.samples})"
            )
            description = f"Gateway: {self.latency * 1000:.0f} ms\nProcessing: {average}\n\n" + (
                "\n".join(rows) or "No modules registered."
            )
            pages.append(PresentationPage(description=description))
        await self._reply_pages(message, tuple(pages))

    async def _blocking(self, message: discord.Message, operation: str, arguments: str) -> None:
        match = SNOWFLAKE.fullmatch(arguments)
        if not match:
            await self._reply(message, f"`.{operation} <id|mention>`")
            return
        target = int(match.group(1))
        if operation == "block" and target == self.owner_id:
            await self._reply(message, "Owner cannot be blocked.")
            return
        try:
            if operation == "block":
                changed = await self.blocklist.block(target, self.owner_id)
                text = "Blocked." if changed else "Already blocked."
            else:
                changed = await self.blocklist.unblock(target)
                text = "Unblocked." if changed else "Not blocked."
        except RuntimeError:
            text = "Block change unconfirmed."
        await self._reply(message, text)

    async def _restart(self, message: discord.Message, target: str) -> None:
        if not target:
            rows = [
                f"{name}: {module.manifest.group} ({module.runtime_state})"
                for name, module in sorted(self.registry.snapshot.modules.items())
            ]
            await self._reply(message, "\n".join(rows) or "No modules registered.")
            return
        group, modules = self.control.resolve(target)
        status = await self._reply(
            message,
            f"Restarting {group}: {', '.join(modules) or 'no registered modules'}.",
        )
        try:
            outcome = await self.control.restart(group)
            method = "hard restart" if outcome.hard_restart else outcome.deployment
            await status.edit(
                content=None,
                view=self.layouts.text(f"{outcome.group} restarted ({method})."),
            )
        except Exception:
            await status.edit(content=None, view=self.layouts.text("Restart failed."))

    async def _drain(self, message: discord.Message, arguments: str) -> None:
        parts = arguments.split()
        resume = len(parts) == 2 and parts[0].casefold() == "resume"
        target = parts[1] if resume else (parts[0] if len(parts) == 1 else "")
        if not target:
            await self._reply(
                message,
                "`.drain <module-or-worker>`\n`.drain resume <module-or-worker>`",
            )
            return
        try:
            group, modules = await self.control.set_drain(target, not resume)
        except Exception:
            await self._reply(message, "Drain change failed.")
            return
        state = "resumed" if resume else "draining"
        await self._reply(message, f"{group} {state}: {', '.join(modules)}.")

    async def _maintenance(self, message: discord.Message, argument: str) -> None:
        normalized = argument.casefold()
        if normalized not in {"on", "off"}:
            await self._reply(message, "`.maintenance on|off`")
            return
        try:
            await self.control.set_maintenance(normalized == "on")
        except Exception:
            await self._reply(message, "Maintenance change failed.")
            return
        await self._reply(message, f"Maintenance {normalized}.")

    async def _reply(
        self,
        message: discord.Message,
        description: str | None = None,
        *,
        page: PresentationPage | None = None,
        view: discord.ui.View | None = None,
    ) -> discord.Message:
        if page is None:
            page = PresentationPage(description=description or "")
        layout = view or self.layouts.build(page, accent_color=LUMI_COLOUR)
        sent = await self._reply_with_kwargs(message, {"view": layout})
        if isinstance(layout, BoundLayoutView):
            layout.message = sent
        return sent

    async def _reply_pages(
        self,
        message: discord.Message,
        pages: tuple[PresentationPage, ...],
    ) -> discord.Message:
        if not pages:
            raise ValueError("response has no pages")
        if len(pages) == 1:
            return await self._reply(message, page=pages[0])
        view = ComponentsV2Paginator(
            owner_id=message.author.id,
            blocklist=self.blocklist,
            builder=self.layouts,
            pages=pages,
            accent_color=LUMI_COLOUR,
        )
        sent = await self._reply_with_kwargs(message, {"view": view})
        view.message = sent
        return sent

    async def _reply_with_kwargs(
        self, message: discord.Message, kwargs: dict[str, Any]
    ) -> discord.Message:
        kwargs = {**kwargs, "allowed_mentions": discord.AllowedMentions.none()}
        loading = getattr(self, "command_loading", None)
        if loading is not None:
            sent = await loading.reply(message, kwargs)
            if sent is not None:
                return sent
        return await message.reply(**kwargs, mention_author=False)

    def _help_visible(
        self,
        command: Any,
        *,
        actor: discord.abc.User,
        guild_id: int | None,
        roles: frozenset[int],
        administrator: bool,
        channels: frozenset[int],
        categories: frozenset[int],
        channel=None,
    ) -> bool:
        module = self.registry.snapshot.modules.get(command.module)
        if module is None:
            return False
        if command.spec.guild_only and guild_id is None:
            return False
        if (
            command.spec.owner_scope
            and (self.owner_id <= 0 or actor.id != self.owner_id)
        ):
            return False
        if not self._has_permissions(actor, command.spec.required_permissions, channel):
            return False
        if command.spec.owner_scope == "global":
            return True
        if command.spec.guild_control:
            return bool(administrator and guild_id is not None)
        if not administrator and module.runtime_state != "online":
            return False
        return self.policy.allowed(
            guild_id=guild_id,
            module=command.module,
            acl_key=command.spec.acl_key,
            public=command.spec.public,
            role_ids=roles,
            administrator=administrator,
            channel_ids=channels,
            category_ids=categories,
        )

    def _brand_icon_url(self) -> str | None:
        if self.user is None:
            return None
        return str(self.user.display_avatar.url)

    def _usage_with_prefix(self, usage: str, prefix: str) -> str:


        if usage.startswith("."):
            return prefix + usage[1:]
        if usage.startswith(self.default_prefix):
            return prefix + usage[len(self.default_prefix) :]
        return prefix + usage

    def _format_help_usage(self, usage: str, prefix: str) -> str:
        lines = []
        for line in usage.splitlines():
            normalized = self._usage_with_prefix(line, prefix)
            command, separator, arguments = normalized.partition(" ")
            lines.append(f"`{command}`{separator}{arguments}")
        return "\n".join(lines)

    def _member_access(self, author: discord.abc.User) -> tuple[frozenset[int], bool]:
        if not isinstance(author, discord.Member):
            return frozenset(), False
        return memo(
            "member-access",
            (id(self), id(author), getattr(getattr(self, "member_cache", None), "epoch", 0)),
            lambda: (
                frozenset(role.id for role in author.roles),
                author.guild_permissions.administrator,
            ),
        )

    @staticmethod
    def _has_permissions(actor: discord.abc.User, required: tuple[str, ...], channel=None) -> bool:
        if not required:
            return True
        if not isinstance(actor, discord.Member):
            return False
        permissions = actor.guild_permissions
        if permissions.administrator:
            return True
        return all(
            bool(
                getattr(
                    channel.permissions_for(actor)
                    if name == "manage_messages" and channel is not None
                    else permissions,
                    name,
                    False,
                )
            )
            for name in required
        )

    def _channel_access(
        self, channel: discord.abc.GuildChannel | discord.Thread | None
    ) -> tuple[frozenset[int], frozenset[int]]:
        if channel is None or isinstance(channel, discord.DMChannel):
            return frozenset(), frozenset()
        channel_ids = {int(channel.id)}
        category_ids: set[int] = set()
        parent_id = getattr(channel, "parent_id", None)
        if parent_id:
            channel_ids.add(int(parent_id))
        category_id = getattr(channel, "category_id", None)
        if category_id:
            category_ids.add(int(category_id))
        parent = getattr(channel, "parent", None)
        parent_category = getattr(parent, "category_id", None)
        if parent_category:
            category_ids.add(int(parent_category))
        return frozenset(channel_ids), frozenset(category_ids)

    async def _mention_prefix_recovery(self, message: discord.Message) -> bool:
        if self.user is None or message.guild is None:
            return False
        if message.content not in {
            f"<@{self.user.id}> prefix",
            f"<@!{self.user.id}> prefix",
        }:
            return False
        _, administrator = self._member_access(message.author)
        if not administrator:
            return True
        await self._reply(message, f"Prefix: `{self.policy.prefix(message.guild.id)}`")
        return True

    def _component_route_id(self, value: object) -> str | None:
        if not isinstance(value, str) or len(value) > 100:
            return None
        if value.startswith("lumi:"):
            return value
        prefix, delimiter, payload = value.partition(":")
        if not delimiter or "~" in payload:
            return None
        aliases = getattr(self.registry.snapshot, "component_aliases", {})
        binding = aliases.get(prefix)
        if binding is None:
            return None
        route, alias = binding
        payload = payload.replace(alias.separator, "~")
        normalized = f"lumi:{route.spec.namespace}:{route.spec.action}:{payload}"
        if len(normalized) > 100 or route.pattern.fullmatch(payload) is None:
            return None
        return normalized

    def _modal_values(self, data: dict[str, Any]) -> dict[str, Any]:
        values: dict[str, Any] = {}
        stack = list(data.get("components", ()))
        while stack:
            item = stack.pop()
            if not isinstance(item, dict):
                continue
            children = item.get("components")
            if isinstance(children, list):
                stack.extend(children)
            child = item.get("component")
            if isinstance(child, dict):
                stack.append(child)
            key = item.get("custom_id")
            value = item.get("value")
            if isinstance(key, str) and isinstance(value, str):
                values[key] = value
                continue
            selections = item.get("values")
            if (
                isinstance(key, str)
                and isinstance(selections, list)
                and all(isinstance(selected, str) for selected in selections)
                and item.get("type") != 19
            ):
                values[key] = tuple(selections)
        return values

    @staticmethod
    def _guild_emoji_catalog(emojis: Any) -> list[dict[str, Any]]:
        return [
            {
                "id": emoji.id,
                "name": emoji.name,
                "animated": emoji.animated,
                "available": emoji.available,
            }
            for emoji in emojis
        ]

    @staticmethod
    def _modal_files(data: dict[str, Any]) -> dict[str, tuple[dict[str, Any], ...]]:
        resolved = data.get("resolved")
        attachments = resolved.get("attachments") if isinstance(resolved, dict) else None
        if not isinstance(attachments, dict):
            return {}
        values: dict[str, tuple[dict[str, Any], ...]] = {}
        stack = list(data.get("components", ()))
        while stack:
            item = stack.pop()
            if not isinstance(item, dict):
                continue
            children = item.get("components")
            if isinstance(children, list):
                stack.extend(children)
            child = item.get("component")
            if isinstance(child, dict):
                stack.append(child)
            key = item.get("custom_id")
            selected = item.get("values")
            if not (item.get("type") == 19 and isinstance(key, str) and isinstance(selected, list)):
                continue
            files: list[dict[str, Any]] = []
            for identifier in selected[:10]:
                metadata = attachments.get(str(identifier))
                if not isinstance(metadata, dict):
                    continue
                files.append(
                    {
                        field: metadata[field]
                        for field in (
                            "id",
                            "filename",
                            "url",
                            "proxy_url",
                            "size",
                            "content_type",
                            "width",
                            "height",
                        )
                        if field in metadata
                    }
                )
            values[key] = tuple(files)
        return values

    async def _interaction_notice(self, interaction: discord.Interaction, description: str) -> None:
        if not interaction.response.is_done():
            await interaction.response.send_message(
                view=self.layouts.text(description),
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )

    async def _handle_reaction_offer_message(
        self,
        message: discord.Message,
        event: dict[str, Any],
        event_type: str,
    ) -> None:
        guild_id = message.guild.id if message.guild else None
        author_id = message.author.id

        if getattr(message, "webhook_id", None) is not None:
            return

        def enabled(module: str) -> bool:
            return self.policy.module_enabled(guild_id, module)

        snapshot = self.registry.snapshot
        if not self.matcher.has_reaction_offer_candidates(
            snapshot,
            event_type=event_type,
            author_id=author_id,
            enabled=enabled,
        ):
            return
        self._offer_message_authors[message.id] = (author_id, time.monotonic())
        self._offer_message_authors.move_to_end(message.id)
        while len(self._offer_message_authors) > 1_024:
            self._offer_message_authors.popitem(last=False)
        matched_offers = self.matcher.match_reaction_offers(
            snapshot,
            event_type=event_type,
            event=event,
            enabled=enabled,
        )
        for matched in matched_offers:
            owner_id = 0
            for field in matched.registered.spec.audience_fields:
                try:
                    candidate = int(event.get(field) or 0)
                except (TypeError, ValueError):
                    candidate = 0
                if candidate > 0:
                    owner_id = candidate
                    break
            if owner_id and self.blocklist.contains(owner_id):
                continue
            if matched.registered.spec.require_owner and not owner_id:
                continue
            record: ReactionOfferRecord | None = None
            new_offer = False
            try:
                record, needs_reaction, new_offer = await self.reaction_offers.register(
                    matched,
                    event,
                    owner_id=owner_id,
                )
                has_reaction = any(
                    reaction.me and canonical_emoji(str(reaction.emoji)) == record.emoji_key
                    for reaction in message.reactions
                )
                if needs_reaction or not has_reaction:
                    await message.add_reaction(matched.registered.spec.emoji)
            except (discord.Forbidden, discord.HTTPException, ValueError):
                if record is not None and new_offer:
                    with contextlib.suppress(Exception):
                        await self.reaction_offers.retire(record.offer_id)
                LOGGER.warning(
                    "reaction_offer_registration_failed",
                    extra={
                        "module_name": matched.registered.module,
                        "message_id": message.id,
                    },
                )
            except Exception:
                if record is not None and new_offer:
                    with contextlib.suppress(Exception):
                        await self.reaction_offers.retire(record.offer_id)
                LOGGER.exception(
                    "reaction_offer_cache_unavailable",
                    extra={
                        "module_name": matched.registered.module,
                        "message_id": message.id,
                    },
                )

    async def _delete_source_messages(self, action, job, guild) -> DiscordActionResult:

        proof = job.request.event.get("_source_cleanup")

        def denied(code="invalid_source_cleanup"):
            return self._action_result(action, False, "failed", code)

        if (
            job.request.event.get("dispatch") != "passive"
            or not isinstance(proof, dict) or proof.get("schema") != 2
            or action.channel_id != job.channel_id or guild.id != job.guild_id
            or any(value is not None for value in (action.message_id, action.member_id))
            or not re.fullmatch(r"[a-f0-9]{64}", str(proof.get("content_digest", "")))
        ):
            return denied()
        identifiers = tuple(proof.get(key) for key in ("source", "command", "prompt"))
        if any(type(value) is not int or value <= 0 for value in identifiers) or len(set(identifiers)) != 3:
            return denied()
        source, command, _ = identifiers
        registered = self.registry.snapshot.modules.get(action.module)
        if (
            registered is None
            or (registered.manifest.group, registered.manifest.generation) != (job.group, job.generation)
            or not any(
                route.source_cleanup and route.route == job.request.route
                and route.handler == job.request.handler and proof.get("source_author") in route.bot_ids
                for route in registered.manifest.passive
            )
        ):
            return denied()
        channel = await self._action_channel(guild, job.channel_id)
        if channel is None or channel.id != job.channel_id or getattr(channel, "guild", guild).id != guild.id:
            return denied("permission_denied")
        try:
            member = await fetch_member(self, guild, job.actor_id)
        except discord.HTTPException:
            return denied("permission_denied")
        if member is None or member.bot:
            return denied("permission_denied")
        roles, administrator = self._member_access(member)
        channels, categories = self._channel_access(channel)
        if not self.policy.allowed(
            guild_id=guild.id, module=job.module, acl_key=self._job_acl_key(job),
            public=True, role_ids=roles, administrator=administrator,
            channel_ids=channels, category_ids=categories,
        ):
            return denied("policy_denied")
        permissions = channel.permissions_for(guild.me) if guild.me else discord.Permissions.none()
        if not (permissions.view_channel and permissions.read_message_history and permissions.manage_messages):
            return denied("permission_denied")


        current = cached_message(self, channel, command)
        if current is not None and content_digest(current.content or "") != proof["content_digest"]:
            return denied()
        for message_id in (source, command):
            try:
                await channel.get_partial_message(message_id).delete()
            except discord.NotFound as exc:
                if exc.code != 10008:
                    return denied("channel_unavailable")
        return self._action_result(action, True, "deleted")

    @message_fetch_scope
    async def _message_event(
        self, message: discord.Message, *, event_type: str = "message_create",
    ) -> dict[str, Any]:
        event = await memo_read(
            "message-context",
            (id(self), id(message), event_type),
            lambda: LumiEdge._build_message_event(self, message, event_type=event_type),
        )

        return dict(event)

    async def _build_message_event(
        self, message: discord.Message, *, event_type: str,
    ) -> dict[str, Any]:
        remember_message(self, message)
        guild = message.guild
        prefix = self.policy.prefix(guild.id if guild else None)
        bot_mention_prefix = bool(
            self.user and re.match(rf"^<@!?{self.user.id}>(?:\s|$)", message.content or "")
        )
        recipients: dict[int, tuple[str, discord.abc.User]] = {}
        webhook_reply = None
        reference = message.reference
        resolved_reference = reference.resolved if reference is not None else None
        reply_message = (
            resolved_reference if isinstance(resolved_reference, discord.Message) else None
        )
        if reply_message is not None:
            remember_message(self, reply_message)
        snapshot = self.registry.snapshot
        projected_access: dict[str, bool] = {}

        def projection_enabled(module):
            if module not in projected_access:
                projected_access[module] = bool(guild and self.policy.module_enabled(guild.id, module))
            return projected_access[module]

        def wants_message_field(field):
            return bool(guild and self.matcher.has_passive_candidates(
                snapshot, event_type=event_type, author_id=message.author.id, context_field=field,
                enabled=projection_enabled,
            ))

        if guild and message.author.bot and unresolved_in_channel(reference, message.channel.id) and (
            self.matcher.has_reaction_offer_candidates(
                snapshot, event_type=event_type, author_id=message.author.id, enabled=projection_enabled,
            )
            or wants_message_field("reply_author_id")
            or wants_message_field("reply_reference_message_id")
        ):
            with contextlib.suppress(discord.HTTPException):
                reply_message = await fetch_message(
                    self,
                    message.channel,
                    reference.message_id,
                    cached=True,
                )

        def add_recipient(subject: discord.abc.User | None, kind: str) -> None:
            if subject is None:
                return
            user_id = int(getattr(subject, "id", 0) or 0)
            if user_id <= 0 or user_id == message.author.id or bool(subject.bot):
                return
            priority = {"direct": 0, "reply": 1, "role": 2, "everyone": 3, "here": 4}
            previous = recipients.get(user_id)
            if previous is None or priority[kind] < priority[previous[0]]:
                recipients[user_id] = (kind, subject)

        human_guild_message = guild is not None and not message.author.bot and not message.webhook_id
        if human_guild_message and wants_message_field("recipient_mentions"):
            for subject in message.mentions:
                add_recipient(subject, "direct")
            webhook_binding = None
            if reference is not None:
                same_reference_channel = (
                    bool(reference.message_id)
                    and getattr(reference, "channel_id", message.channel.id) == message.channel.id
                    and getattr(reference, "guild_id", guild.id) in (None, guild.id)
                    and getattr(reference, "type", discord.MessageReferenceType.default)
                        == discord.MessageReferenceType.default
                )
                def same_source(target):
                    return (
                        target.guild is not None and target.id == reference.message_id
                        and target.guild.id == guild.id and target.channel.id == message.channel.id
                    )

                bound = getattr(self, "bound_replies", None)
                known_output = same_reference_channel and bound and bound.known_output(
                    guild.id, message.channel.id, reference.message_id,
                )
                if same_reference_channel and not known_output and (
                    resolved_reference is None
                    or (reply_message is not None and same_source(reply_message) and reply_message.webhook_id)
                ):
                    subscribed = tuple(
                        route for route in snapshot.passive.get((event_type, None), ())
                        if route.spec.recipient_webhook and projection_enabled(route.module)
                    )
                    try:
                        webhook_binding = await self.webhooks.bindings.resolve(
                            guild.id, message.channel.id, reference.message_id,
                            frozenset(route.module for route in subscribed),
                            webhook_id=reply_message.webhook_id if reply_message else None,
                        )
                    except Exception:
                        LOGGER.warning("webhook_binding_lookup_unavailable")
                if (
                    same_reference_channel and not known_output
                    and reply_message is None and webhook_binding is None
                    and resolved_reference is None
                ):
                    with contextlib.suppress(discord.HTTPException):
                        reply_message = await fetch_message(
                            self,
                            message.channel,
                            reference.message_id,
                            cached=True,
                        )
                if reply_message is not None and same_source(reply_message):
                    add_recipient(reply_message.author, "reply")

            role_ids = {role.id for role in message.role_mentions}
            if role_ids or message.mention_everyone:
                try:
                    members = await self._guild_members(guild)
                except (discord.HTTPException, TimeoutError):
                    LOGGER.warning(
                        "broadcast_mention_members_unavailable",
                        extra={"guild_id": guild.id, "message_id": message.id},
                    )
                    members = []
                broadcast_kind = "everyone" if "@everyone" in message.content else "here"
                for member in members:
                    member_roles = {role.id for role in member.roles}
                    if role_ids & member_roles:
                        add_recipient(member, "role")
                    if message.mention_everyone:
                        add_recipient(member, broadcast_kind)

            verified: dict[int, discord.Member | None] = {}

            async def verify_recipient(user_id, subject=None):
                if user_id in verified:
                    return verified[user_id]
                member = (
                    subject if isinstance(subject, discord.Member) else guild.get_member(user_id)
                )
                if member is None:
                    with contextlib.suppress(discord.HTTPException):
                        member = await fetch_member(self, guild, user_id)
                if member is None or member.bot or not self._member_can_read(message.channel, member):
                    member = None
                verified[user_id] = member
                return member

            for user_id, (_kind, subject) in tuple(recipients.items()):
                if await verify_recipient(user_id, subject) is None:
                    recipients.pop(user_id, None)
            if webhook_binding is not None and webhook_binding.user_id != message.author.id:
                member = await verify_recipient(webhook_binding.user_id)
                if member is not None:
                    webhook_reply = {
                        "module": webhook_binding.module,
                        "user_id": webhook_binding.user_id,
                        "original_message_id": webhook_binding.original_message_id,
                        "display_name": getattr(member, "display_name", None) or member.name,
                        "avatar_url": str(member.display_avatar.url),
                    }

        labels: dict[str, str] = {}
        for subject in message.mentions:
            shown = getattr(subject, "display_name", None) or subject.name
            labels[f"<@{subject.id}>"] = f"@{shown}"
            labels[f"<@!{subject.id}>"] = f"@{shown}"
        for role in message.role_mentions:
            labels[f"<@&{role.id}>"] = f"@{role.name}"
        for channel in message.channel_mentions:
            labels[f"<#{channel.id}>"] = f"#{channel.name}"

        interaction = getattr(message, "interaction_metadata", None)
        interaction_user = getattr(interaction, "user", None)
        interaction_user_id = int(getattr(interaction_user, "id", 0) or 0)
        reply_author_id = reply_message.author.id if reply_message else None
        reply_prompt_module = None
        reply_reference_message_id = (
            reply_message.reference.message_id
            if reply_message and reply_message.reference
            else None
        )
        command_reference = getattr(reply_message, "reference", None)

        def native_reply(ref):
            return (
                ref and getattr(ref, "type", discord.MessageReferenceType.default)
                == discord.MessageReferenceType.default
                and getattr(ref, "channel_id", message.channel.id) == message.channel.id
                and getattr(ref, "guild_id", None) in (None, guild.id)
            )

        if (
            guild
            and reply_message
            and native_reply(reference)
            and native_reply(command_reference)
            and reply_message.id == reference.message_id
            and not reply_message.author.bot
            and reply_message.webhook_id is None
            and reply_reference_message_id
            and reply_message.channel.id == message.channel.id
            and reply_message.guild
            and reply_message.guild.id == guild.id
            and wants_message_field("reply_reference_message_id")
        ):


            with contextlib.suppress(Exception):
                reply_prompt_module = await owned_output_module(
                    self.queue_redis, guild.id, message.channel.id, reply_reference_message_id,
                )
        embed_payloads = [embed.to_dict() for embed in message.embeds]
        embed_author_name = ""
        embed_author_icon_url = ""
        if message.embeds:
            embed_author_name = str(getattr(message.embeds[0].author, "name", "") or "")
            embed_author_icon_url = str(getattr(message.embeds[0].author, "icon_url", "") or "")
        embed_titles = [str(embed.title or "") for embed in message.embeds if embed.title]
        embed_descriptions = [
            str(embed.description or "") for embed in message.embeds if embed.description
        ]
        document_parts = [message.content or ""]
        source_parts = [message.content or ""]
        primary_image_url = ""
        primary_embed_image_url = ""
        for embed in message.embeds:
            if not primary_embed_image_url:
                primary_embed_image_url = str(getattr(embed.image, "url", None) or "")
            document_parts.extend(
                (
                    str(embed.title or ""),
                    str(getattr(embed.author, "name", "") or ""),
                    str(embed.description or ""),
                    str(getattr(embed.footer, "text", "") or ""),
                )
            )
            for field in embed.fields:
                document_parts.extend((str(field.name or ""), str(field.value or "")))
            if embed.description:
                source_parts.append(str(embed.description))
            source_parts.extend(str(field.value or "") for field in embed.fields if field.value)
            if not primary_image_url:
                for candidate in (
                    getattr(embed.image, "url", None),
                    getattr(embed.image, "proxy_url", None),
                    getattr(embed.thumbnail, "url", None),
                    getattr(embed.thumbnail, "proxy_url", None),
                ):
                    if candidate:
                        primary_image_url = str(candidate)
                        break
        if not primary_image_url:
            for attachment in message.attachments:
                if str(attachment.content_type or "").startswith("image/"):
                    primary_image_url = attachment.url
                    break
        message_document = unicodedata.normalize(
            "NFKC", "\n".join(document_parts).replace("\r\n", "\n").replace("\r", "\n")
        )[:32_000]
        source_text = unicodedata.normalize(
            "NFKC", "\n".join(source_parts).replace("\r\n", "\n").replace("\r", "\n")
        ).strip()[:4_096]
        image_source = self._image_source_from_message(message, origin="attachment")
        event = {
            "kind": "message",
            "actor_id": message.author.id,
            "author_id": message.author.id,
            "author_bot": bool(message.author.bot),
            "author_name": getattr(message.author, "display_name", None) or message.author.name,
            "author_avatar_url": str(message.author.display_avatar.url),
            "author_role_ids": [role.id for role in getattr(message.author, "roles", ())],
            "message_id": message.id,
            "guild_id": guild.id if guild else None,
            "guild_name": guild.name if guild else "",
            "channel_id": message.channel.id,
            "channel_name": getattr(message.channel, "name", "Direct Message"),
            "content": message.content,
            "created_at_ms": int(message.created_at.timestamp() * 1000),
            "source_revision_ms": int(
                (getattr(message, "edited_at", None) or message.created_at).timestamp() * 1000
            ),
            "webhook_id": getattr(message, "webhook_id", None),
            "embeds": embed_payloads,
            "embed_title": embed_titles[0] if embed_titles else "",
            "embed_author_name": embed_author_name,
            "embed_author_icon_url": embed_author_icon_url,
            "embed_titles": embed_titles,
            "embed_descriptions": embed_descriptions,
            "message_document": message_document,
            "source_text": source_text,
            "primary_image_url": primary_image_url,
            "primary_embed_image_url": primary_embed_image_url,
            "image_source": image_source,
            "channel_parent_id": getattr(getattr(message.channel, "parent", None), "id", None),
            "channel_nsfw": bool(
                message.channel.is_nsfw()
                if callable(getattr(message.channel, "is_nsfw", None))
                else False
            ),
            "interaction_user_id": interaction_user_id,
            "reference_message_id": reference.message_id if reference else None,
            "reply_author_id": reply_author_id,
            "reply_content": (reply_message.content or "")[:4000] if reply_message else "",
            "reply_reference_message_id": reply_reference_message_id,
            "reply_prompt_module": reply_prompt_module,
            "_trusted_reply_chain": reply_prompt_module is not None,
            "reply_author_bot": bool(reply_message.author.bot) if reply_message else None,
            "reply_webhook_id": reply_message.webhook_id if reply_message else None,
            "reply_created_at_ms": (
                int(reply_message.created_at.timestamp() * 1000) if reply_message else None
            ),
            "reply_edited_at_ms": (
                int(reply_message.edited_at.timestamp() * 1000)
                if reply_message and reply_message.edited_at
                else 0
                if reply_message
                else None
            ),
            "reply_channel_id": reply_message.channel.id if reply_message else None,
            "reply_guild_id": (
                reply_message.guild.id if reply_message and reply_message.guild else None
            ),
            "attachments": [
                {"id": attachment.id, "filename": attachment.filename, "url": attachment.url}
                for attachment in message.attachments
            ],
            "components": [component.to_dict() for component in message.components],
            "reply": {
                "message_id": message.reference.message_id if message.reference else None,
                "author_id": reply_author_id,
            },
            "recipient_mentions": [
                {
                    "user_id": user_id,
                    "kind": kind,
                    "display_name": getattr(subject, "display_name", None) or subject.name,
                    "avatar_url": str(subject.display_avatar.url),
                }
                for user_id, (kind, subject) in recipients.items()
            ],
            "webhook_reply": webhook_reply,
            "mentioned_user_ids": [subject.id for subject in message.mentions],
            "mentioned_role_ids": [role.id for role in message.role_mentions],
            "mentions_everyone": "@everyone" in message.content,
            "mentions_here": "@here" in message.content,
            "entity_labels": sorted(labels.items()),
            "used_prefix": message.content.startswith(prefix) or bot_mention_prefix,
            "prefix": prefix,
            "administrator": self._member_access(message.author)[1],
        }
        index = getattr(self, "actor_route_index", None)
        active_modules = []
        if index is not None and human_guild_message:
            for group, module in index.matches(guild.id, message.author.id):
                registered = snapshot.modules.get(module)
                if registered and registered.manifest.group == group and projection_enabled(module):
                    active_modules.append(module)
            active_modules.sort()
        event["active_actor_modules"] = active_modules
        relevant = bool(recipients or webhook_reply or active_modules)
        if relevant and wants_message_field("captured_message"):
            captured = capture_message(message)
            event["captured_message"] = captured
            watch_ttl = self.matcher.passive_watch_ttl(
                snapshot, event_type="message_edit", author_id=message.author.id,
                context_field="hydrated_message", enabled=projection_enabled,
            ) if recipients or webhook_reply else 0
            if (
                watch_ttl
                and captured
                and source_urls(message.content).difference(capture_urls(captured))
            ):


                if self._passive_watch_author(message.id) is None:
                    self._remember_passive_watch(message.id, message.author.id, watch_ttl)
                with contextlib.suppress(Exception):
                    await self.cache_redis.set(
                        f"lumi:passive-watch:{message.id}",
                        str(message.author.id),
                        px=watch_ttl,
                        nx=True,
                    )
        requester_id = None
        if interaction_user_id and not getattr(interaction_user, "bot", True):
            requester_id = interaction_user_id
        elif (
            reply_message is not None
            and not reply_message.author.bot
            and reply_message.webhook_id is None
            and reply_message.guild == guild
            and reply_message.channel.id == message.channel.id
        ):
            requester_id = reply_author_id
        event["requester_id"] = requester_id
        event["interaction_name"] = str(getattr(interaction, "name", "") or "")[:100]
        if guild and not message.author.bot:
            command = None
            if event_type == "message_create" and message.content.startswith(prefix):
                root = message.content[len(prefix) :].partition(" ")[0].casefold()
                command = self._command_snapshot(guild.id).commands.get(root)
            if (
                (command and "visible_channel_ids" in command.spec.context_fields)
                or (relevant and wants_message_field("visible_channel_ids"))
            ):
                event["visible_channel_ids"] = self._visible_channel_ids(guild, message.author)
        return event

    async def _guild_members(self, guild: discord.Guild) -> list[discord.Member]:
        now = time.monotonic()
        cached = self._guild_member_snapshots.get(guild.id)
        if cached is not None and now - cached[0] < 60:
            return cached[1]
        lock = self._guild_member_snapshot_locks.setdefault(guild.id, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            cached = self._guild_member_snapshots.get(guild.id)
            if cached is not None and now - cached[0] < 60:
                return cached[1]


            for _ in range(2):
                with lookup(self).listing(guild.id) as changed:
                    members = [member async for member in guild.fetch_members(limit=None)]
                    if not changed[0]:
                        self._guild_member_snapshots[guild.id] = (now, members)
                        return members
            raise TimeoutError("Guild roster changed during the read")

    @staticmethod
    def _member_can_read(
        channel: discord.abc.GuildChannel | discord.Thread | discord.abc.Messageable,
        member: discord.Member,
    ) -> bool:
        if not hasattr(channel, "permissions_for"):
            return True
        permissions = channel.permissions_for(member)
        return bool(permissions.view_channel and permissions.read_message_history)

    @classmethod
    def _visible_channel_ids(
        cls,
        guild: discord.Guild,
        subject: discord.Member | discord.User,
    ) -> list[int]:
        member = subject if isinstance(subject, discord.Member) else guild.get_member(subject.id)
        if member is None:
            return []
        channels = (*guild.channels, *guild.threads)
        return [channel.id for channel in channels if cls._member_can_read(channel, member)]

    def _invite_url(self) -> str:
        if not self.user:
            return "https://discord.com/oauth2/authorize"
        return discord.utils.oauth_url(self.user.id, permissions=discord.Permissions.none())

    async def _maintenance_loop(self) -> None:
        next_health = 0.0
        next_webhook_prune = 0.0
        while True:
            await asyncio.sleep(1)
            self.member_cache.prune()
            await self.broker.probe()
            await self.broker.sweep_expired()
            await self.registry.mark_stale(stale_after_ms=35_000)
            now = time.monotonic()
            if now >= next_health:
                await self.control.refresh_health()
                self.queue_depths = await self.broker.pending_by_group()
                next_health = now + 5
            if now >= next_webhook_prune:
                await self.webhooks.prune()
                self.bound_replies.prune()
                next_webhook_prune = now + 60

    def _spawn(self, factory: Callable[[], Awaitable[Any]], name: str) -> None:
        task = asyncio.create_task(self._run_background(factory, name), name=name)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def _spawn_once(self, factory: Callable[[], Awaitable[Any]], name: str) -> None:
        task = asyncio.create_task(self._run_once(factory, name), name=name)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    @staticmethod
    async def _run_once(
        factory: Callable[[], Awaitable[Any]],
        name: str,
    ) -> None:
        try:
            await factory()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("edge_background_task_failed", extra={"task": name})

    async def _run_background(
        self,
        factory: Callable[[], Awaitable[Any]],
        name: str,
    ) -> None:
        while not self._closing:
            try:
                await factory()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("edge_background_task_failed", extra={"task": name})
            if not self._closing:
                await asyncio.sleep(1)
