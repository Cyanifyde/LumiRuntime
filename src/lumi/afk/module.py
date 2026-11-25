from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import contextlib
import hashlib
import logging
import secrets
import time
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo, available_timezones

import msgspec
from redis.asyncio import Redis

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    GuildSourceSpec,
    HelpSpec,
    Manifest,
    MatcherExpr,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    RecipientWebhookSpec,
    ResultEnvelope,
    TemporaryAttachment,
    TypedRequest,
    decode,
    encode,
)
from lumi.worker.actions import EdgeActionClient

from .context import combine_last_spoke
from .conversations import conversations
from .publisher import AFKActivePublisher
from .renderer import (
    AFKRenderReply,
    AFKRenderRequest,
    close_afk_message_renderer,
    get_afk_message_renderer,
)
from .store import AFKInteraction, AFKState, MySQLAFKStore

LOGGER = logging.getLogger(__name__)
REGION_HELP_URL = "https://whatismytimezone.net/"
REGION_HINT = "its often continent/capital ie Europe/London"
PAGE_SIZE = 5
SESSION_TTL = 900
_REGIONS = {value.casefold(): value for value in available_timezones()}
_REGIONS.setdefault("utc", "UTC")
_DISCORD_AVATAR_HOSTS = frozenset({"cdn.discordapp.com", "media.discordapp.net"})
_AVATAR_SUFFIXES = frozenset({"gif", "jpeg", "jpg", "png", "webp"})


def _render_avatar_url(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").lower()
        if parsed.scheme != "https" or parsed.port is not None or host not in _DISCORD_AVATAR_HOSTS:
            return value
    except ValueError:
        return value
    path = parsed.path
    stem, separator, suffix = path.rpartition(".")
    if (
        not separator
        or suffix.casefold() not in _AVATAR_SUFFIXES
        or ("/avatars/" not in path and not path.startswith("/embed/avatars/"))
    ):
        return value
    return urlunsplit(("https", host, f"{stem}.webp", "size=128", ""))


class AFKRecapSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    timezone_name: str

    visible_channel_ids: tuple[int, ...]
    page: int = 0
    selected_author_id: int | None = None
    search_query: str | None = None
    since_ms: int | None = None
    since_message_id: int = 0
    namespace: Literal["afk", "afks", "afk_back"] = "afk"
    command_prefix: str = "."
    unread_before: datetime | None = None
    global_scope: bool = False
    guild_ids: tuple[int, ...] = ()
    guild_names: tuple[tuple[int, str], ...] = ()
    frozen_selection: bool = False
    strict_since: bool = False
    clear_before: datetime | None = None


class AFKSettingsSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int


class AFKModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        store: MySQLAFKStore,
        temp_root: Path,
        media_store=None,
    ) -> None:
        self._generation = generation
        self._queue = queue_redis
        self._cache = cache_redis
        self._store = store
        self._temp_root = temp_root
        self._media_store = media_store
        self._actions = EdgeActionClient(
            queue_redis,
            module="afk",
            group="community",
            generation=generation,
        )
        self._publisher = AFKActivePublisher(store, queue_redis)
        self._publisher_task: asyncio.Task[None] | None = None
        self._retention_task: asyncio.Task[None] | None = None
        self._draining = False
        self._retention_busy = False

    @property
    def manifest(self) -> Manifest:
        token = r"(?P<session>[a-f0-9]{16})"
        page = r"(?P<page>[0-9]{1,10})"

        def component(
            action: str,
            *,
            input_kind: str = "none",
            modal: ModalSpec | None = None,
        ) -> ComponentSpec:
            return ComponentSpec(
                namespace="afk",
                action=action,
                handler="afk.page",
                grammar=rf"^{token}~{page}$",
                captures=(
                    CaptureSpec("session"),
                    CaptureSpec("page", "integer", minimum=0, maximum=2_147_483_647),
                ),
                deadline_ms=120_000,
                acl_key="afkn",
                public=True,
                input_kind=input_kind,
                modal=modal,
                session_bound=True,
                context_fields=("origin_channel_id",),
                presentation_mode="legacy",
                guild_sources=GuildSourceSpec("afkn"),
            )

        def settings_component(action: str, *, toggle: bool = False) -> ComponentSpec:
            return ComponentSpec(
                namespace="afk",
                action=action,
                handler="afk.settings",
                grammar=rf"^{token}~(?P<revision>[0-9]{{1,19}})$" if toggle else rf"^{token}$",
                captures=(
                    CaptureSpec("session"),
                    CaptureSpec(
                        "revision", "integer", minimum=0, maximum=9_223_372_036_854_775_807
                    ),
                )
                if toggle
                else (CaptureSpec("session"),),
                deadline_ms=120_000,
                acl_key="afk",
                public=True,
                input_kind="none" if toggle else "string_values",
                session_bound=True,
                context_fields=("origin_channel_id",),
                presentation_mode="v2",
            )

        recap_components = (
            component("previous"),
            component("next"),
            component(
                "jump",
                input_kind="modal",
                modal=ModalSpec(
                    "Page",
                    (
                        ModalFieldSpec(
                            "page", "Page", minimum=1, maximum=10, placeholder="Page number"
                        ),
                    ),
                ),
            ),
            component(
                "search",
                input_kind="modal",
                modal=ModalSpec(
                    "Search",
                    (
                        ModalFieldSpec(
                            "query",
                            "Word or phrase",
                            minimum=1,
                            maximum=200,
                            placeholder="Word or phrase",
                        ),
                    ),
                ),
            ),
            component("filter_user", input_kind="user_ids"),
            component("clear"),
        )
        return Manifest(
            schema=2,
            module="afk",
            group="community",
            generation=self._generation,
            job_stream="lumi:jobs:community",
            display_name="AFK",
            summary="AFK notices and recaps.",
            presentation_mode="v2",
            commands=(
                CommandSpec(
                    root="afk",
                    handler="afk.command",
                    grammar=r"^(?:\s+(?P<first>\S+)(?:\s+(?P<rest>[\s\S]+))?)?$",
                    captures=(
                        CaptureSpec("first", required=False, maximum=1900),
                        CaptureSpec("rest", required=False, maximum=1870),
                    ),
                    help=HelpSpec(
                        ".afk [reason]\n.afk off\n.afk settings",
                        "Set AFK, global scope and recap delivery.",
                    ),
                    acl_key="afk",
                    public=True,
                    presentation_modes=("v2", "legacy"),
                    guild_sources=GuildSourceSpec("afk"),
                    context_fields=(
                        "mentioned_user_ids",
                        "prefix",
                        "visible_channel_ids",
                    ),
                ),
                CommandSpec(
                    root="afkn",
                    handler="afk.recap",
                    grammar=r"^(?:\s+(?P<help>help))?$",
                    captures=(CaptureSpec("help", required=False),),
                    help=HelpSpec(".afkn", "Replies and pings from the last 48 hours."),
                    acl_key="afkn",
                    public=True,
                    presentation_mode="legacy",
                    context_fields=("prefix", "visible_channel_ids"),
                    guild_sources=GuildSourceSpec("afkn"),
                ),
                CommandSpec(
                    root="afks",
                    handler="afk.since",
                    grammar=r"^(?:\s+(?P<help>help))?$",
                    captures=(CaptureSpec("help", required=False),),
                    help=HelpSpec(".afks", "Replies and pings since your last message."),
                    acl_key="afks",
                    public=True,
                    presentation_mode="legacy",
                    guild_sources=GuildSourceSpec("afks", include_activity=True),
                    context_fields=(
                        "prefix",
                        "visible_channel_ids",
                        "activity_boundary",
                    ),
                ),
                CommandSpec(
                    root="region",
                    handler="afk.region",
                    grammar=(
                        r"^(?:\s+(?:(?P<operation>off|help)|"
                        r"(?P<timezone>[A-Za-z0-9_+\-/]{1,100})))?$"
                    ),
                    captures=(
                        CaptureSpec("operation", required=False),
                        CaptureSpec("timezone", required=False, maximum=100),
                    ),
                    help=HelpSpec(
                        ".region <timezone>\n.region off\n.region Europe/London",
                        "Set your timezone for recaps, local time and reminders.",
                    ),
                    acl_key="region",
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
            passive=(
                PassiveMatcherSpec(
                    route="afk.message",
                    handler="afk.message",
                    event="message_create",
                    expression=MatcherExpr("any", children=(
                        MatcherExpr("exists", "recipient_mentions"),
                        MatcherExpr("exact", "active_actor_modules", "afk"),
                        MatcherExpr("exact", "webhook_reply.module", "afk"),
                    )),
                    deadline_ms=120_000,
                    presentation_modes=("v2", "legacy"),
                    guild_sources=GuildSourceSpec("afk"),
                    recipient_webhook=RecipientWebhookSpec("Lumi AFK"),
                    context_fields=(
                        "author_name",
                        "author_avatar_url",
                        "channel_name",
                        "content",
                        "recipient_mentions",
                        "webhook_reply",
                        "mentioned_user_ids",
                        "entity_labels",
                        "captured_message",
                        "activity_exempt",
                        "used_prefix",
                        "prefix",
                        "visible_channel_ids",
                    ),
                ),
                PassiveMatcherSpec(
                    route="afk.media",
                    handler="afk.media",
                    event="message_edit",
                    expression=MatcherExpr("exists", field="hydrated_message"),
                    hydrate=True,
                    hydrate_on=("embeds",),
                    watch_ttl_ms=120_000,
                    deadline_ms=120_000,
                    context_fields=("hydrated_message",),
                ),
            ),
            components=(
                *recap_components,
                component("clear_history"),
                component("confirm_clear_history"),
                component("cancel_clear_history"),
                *(
                    msgspec.structs.replace(
                        spec, namespace="afks", acl_key="afks", guild_sources=GuildSourceSpec("afks"),
                    )
                    for spec in recap_components
                ),
                *(
                    msgspec.structs.replace(
                        spec, namespace="afk_back", acl_key="afk", guild_sources=GuildSourceSpec("afk"),
                    )
                    for spec in recap_components
                ),
                settings_component("settings_delivery"),
                settings_component("settings_global_status", toggle=True),
                settings_component("settings_global_recaps", toggle=True),
            ),
            capabilities=("send_dm", "send_recipient_webhook", "read_guild_sources"),
            version="1.10.2",
        )

    async def start(self) -> None:
        self._temp_root.mkdir(parents=True, exist_ok=True)
        self._publisher_task = asyncio.create_task(
            self._publisher.run(), name="afk-active-publisher"
        )
        self._retention_task = asyncio.create_task(self._retention_loop(), name="afk-retention")

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        try:
            if request.handler == "afk.command":
                intent = await self._command(request, job_id)
            elif request.handler in {"afk.recap", "afk.since"}:
                intent = await self._recap(request, job_id)
            elif request.handler == "afk.region":
                intent = await self._region(request)
            elif request.handler == "afk.message":
                intent = await self._message(request, job_id)
            elif request.handler == "afk.media":
                await self._media_update(request)
                intent = None
            elif request.handler == "afk.page":
                intent = await self._page(request, job_id)
            elif request.handler == "afk.settings":
                intent = await self._settings_action(request)
            else:
                intent = self._simple("Unavailable.")
            return self._result(job_id, intent)
        except Exception:
            LOGGER.exception(
                "afk_request_failed",
                extra={"handler": request.handler, "guild_id": request.event.get("guild_id")},
            )
            if request.event.get("dispatch") == "passive":
                return self._result(job_id, None)
            if request.event.get("dispatch") == "component":
                return self._result(
                    job_id,
                    DiscordIntent(
                        kind="ephemeral",
                        pages=(PresentationPage(description="Unavailable."),),
                    ),
                )
            return self._result(job_id, self._simple("Unavailable."))

    async def _command(self, request: TypedRequest, job_id: str) -> DiscordIntent | None:
        guild_id, actor_id = self._scope(request)
        first = str(request.arguments.get("first") or "")
        rest = str(request.arguments.get("rest") or "")
        lowered = first.casefold()
        prefix = str(request.event.get("prefix") or ".")
        if lowered == "help" and not rest:
            return self._simple(f"{prefix}afk [reason]\n{prefix}afk off\n{prefix}afk settings")
        if lowered == "off" and not rest:
            active = await self._store.get_status(guild_id, actor_id)
            context = (
                await self._global_context(request, job_id)
                if active and active.global_scope
                else None
            )
            state = (
                await self._store.clear_afk(
                    guild_id, actor_id, returned_at=self._event_time(request)
                )
                if active
                else None
            )
            await self._publisher.flush_safely()
            if state is None:
                return self._simple("You weren't AFK.")
            return await self._return_recap(
                request,
                job_id,
                state,
                manual=True,
                scope_context=context,
            )
        if lowered == "settings" and not rest:
            session = AFKSettingsSession(
                token=secrets.token_hex(8),
                owner_id=actor_id,
                guild_id=guild_id,
            )
            await self._save_settings_session(session)
            return await self._settings_page(session, kind="send")

        reason = " ".join(value for value in (first, rest) if value).strip() or "AFK"

        if any(
            f"<@{user_id}>" in reason or f"<@!{user_id}>" in reason
            for user_id in request.event.get("mentioned_user_ids", ())
        ):
            return self._simple("AFK message cannot include user mentions.")
        started_at = datetime.fromtimestamp(
            int(request.event.get("created_at_ms") or int(time.time() * 1000)) / 1000,
            tz=UTC,
        )
        await self._store.set_afk(
            guild_id,
            actor_id,
            reason,
            int(request.event.get("channel_id") or 0),
            started_at,
        )
        await self._publisher.flush_safely()
        return self._simple(f"AFK set{f': {reason}' if reason != 'AFK' else '.'}")

    async def _settings_action(self, request: TypedRequest) -> DiscordIntent:
        token = str(request.arguments.get("session") or "")
        session = await self._load_settings_session(token)
        if session is None or session.owner_id != int(request.event.get("actor_id") or 0):
            return self._ephemeral("Unavailable.", presentation_mode="v2")
        action = request.route.removeprefix("afk.")
        if action in {"settings_global_status", "settings_global_recaps"}:
            name = action.removeprefix("settings_")
            scopes = await self._store.get_scopes(session.owner_id)
            revision = request.arguments.get("revision")
            if type(revision) is not int:
                return self._ephemeral("Unavailable.", presentation_mode="v2")
            await self._store.set_scope(
                session.guild_id,
                session.owner_id,
                name,
                not getattr(scopes, name),
                expected_revision=revision,
            )
            await self._publisher.flush_safely()
            return await self._settings_page(session, kind="edit")
        values = request.arguments.get("values")
        if not isinstance(values, (list, tuple)) or len(values) != 1:
            return self._ephemeral("Unavailable.", presentation_mode="v2")
        value = str(values[0]).casefold()
        action = request.route.removeprefix("afk.")
        if action == "settings_delivery":
            if value not in {"dm", "channel", "none"}:
                return self._ephemeral("Unavailable.", presentation_mode="v2")
            await self._store.set_mode(session.guild_id, session.owner_id, value)
        else:
            return self._ephemeral("Unavailable.", presentation_mode="v2")
        return await self._settings_page(session, kind="edit")

    async def _settings_page(self, session: AFKSettingsSession, *, kind: str) -> DiscordIntent:
        delivery = await self._store.get_mode(session.guild_id, session.owner_id)
        scopes = await self._store.get_scopes(session.owner_id)
        delivery_labels = {"dm": "DM", "channel": "Channel", "none": "None"}
        controls = (
            *(
                PresentationControl(
                    kind="button",
                    namespace="afk",
                    action="settings_" + name,
                    payload=f"{session.token}~{scopes.revision}",
                    label=f"{label}: {'On' if getattr(scopes, name) else 'Off'}",
                )
                for name, label in (
                    ("global_status", "Global AFK"),
                    ("global_recaps", "Global AFKN/AFKS"),
                )
            ),
            PresentationControl(
                kind="string_select",
                namespace="afk",
                action="settings_delivery",
                payload=session.token,
                placeholder="Delivery",
                options=tuple(
                    PresentationOption(label, value, default=value == delivery)
                    for value, label in delivery_labels.items()
                ),
            ),
        )
        return DiscordIntent(
            kind=kind,
            pages=(
                PresentationPage(
                    description=(f"## AFK Settings\nDelivery: {delivery_labels[delivery]}"),
                    controls=controls,
                ),
            ),
            session_id=session.token,
            presentation_mode="v2",
        )

    async def _region(self, request: TypedRequest) -> DiscordIntent:
        _guild_id, actor_id = self._scope(request)
        operation = str(request.arguments.get("operation") or "")
        prefix = str(request.event.get("prefix") or ".")
        if operation == "off":
            await self._store.clear_region(actor_id)
            return self._simple("Region cleared.")
        supplied = str(request.arguments.get("timezone") or "")
        if operation == "help":
            return self._simple(
                f"{prefix}region <timezone>\n{prefix}region off\n"
                f"{REGION_HINT}\n"
                f"[Find your timezone]({REGION_HELP_URL})"
            )
        if not supplied:
            current = await self._store.get_region(actor_id)
            text = (f"Region: `{current}`\n" if current else "") + (
                f"{prefix}region <timezone>\n{REGION_HINT}\n[Find your timezone]({REGION_HELP_URL})"
            )
            return self._simple(text)
        canonical = _REGIONS.get(supplied.casefold())
        if canonical is None:
            return self._simple(
                f"Invalid timezone.\n{REGION_HINT}\n[Find your timezone]({REGION_HELP_URL})"
            )
        await self._store.set_region(actor_id, canonical)
        local_time = datetime.now(ZoneInfo(canonical)).strftime("%d/%m/%Y %I:%M %p")
        return self._simple(f"Region: `{canonical}`\nLocal time: `{local_time}`")

    async def _recap(self, request: TypedRequest, job_id: str = "") -> DiscordIntent:
        guild_id, actor_id = self._scope(request)
        since_only = request.handler == "afk.since"
        if request.arguments.get("help"):
            prefix = str(request.event.get("prefix") or ".")
            return self._simple(
                f"{prefix}{'afks' if since_only else 'afkn'}", presentation_mode="legacy"
            )
        prefix = str(request.event.get("prefix") or ".")
        region = await self._saved_region(actor_id)
        if region is None:
            return self._simple(self._region_prompt(prefix), presentation_mode="legacy")
        scopes = await self._store.get_scopes(actor_id)
        context = (
            await self._global_context(request, job_id, include_activity=since_only)
            if scopes.global_recaps
            else None
        )
        guilds = tuple(int(g["id"]) for g in context["guilds"]) if context else (guild_id,)
        since_ms, since_message_id = None, 0
        if since_only:
            boundary = request.event.get("activity_boundary") or {}
            since_ms = (
                context["last_spoke_ms"] if context
                else boundary.get("at_ms", request.event.get("afk_since_ms"))
            )
            since_message_id = (
                context["last_spoke_message_id"]
                if context
                else boundary.get("message_id", request.event.get("afk_since_message_id"))
            )
            if type(since_ms) is not int or type(since_message_id) is not int:
                return self._simple("Activity unavailable. Try again.", presentation_mode="legacy")
            if since_ms == since_message_id == 0:
                return self._simple("No earlier activity recorded yet.", presentation_mode="legacy")
            command_time = int(request.event.get("created_at_ms") or 0)
            command_id = int(request.event.get("message_id") or 0)
            if min(since_ms, since_message_id) <= 0 or (since_ms, since_message_id) >= (
                command_time,
                command_id,
            ):
                return self._simple("Activity changed. Try again.", presentation_mode="legacy")
        interactions = await self._recap_interactions(
            guild_id,
            actor_id,
            since_ms,
            since_message_id,
            guild_ids=guilds,
            strict_since=scopes.global_recaps and since_only,
        )
        if not interactions and since_only:
            return self._simple(
                (
                    "No replies or pings since you last spoke."
                    if since_only
                    else "No replies or pings in the past 48 hours."
                ),
                presentation_mode="legacy",
            )
        if since_ms is not None:
            oldest = min(int(item.occurred_at.timestamp() * 1000) for item in interactions)
            if oldest < since_ms:
                since_ms, since_message_id = oldest, 0
        session = AFKRecapSession(
            secrets.token_hex(8),
            actor_id,
            guild_id,
            region,
            (),
            since_ms=since_ms,
            since_message_id=since_message_id,
            namespace="afks" if since_only else "afk",
            command_prefix=prefix,
            global_scope=scopes.global_recaps,
            guild_ids=guilds,
            guild_names=tuple((int(g["id"]), str(g["name"])) for g in context["guilds"])
            if context
            else (),
            strict_since=scopes.global_recaps and since_only,
        )
        session = await self._freeze_session(session, interactions)
        return await self._rendered_recap(interactions, session, kind="send")

    async def _page(self, request: TypedRequest, job_id: str = "") -> DiscordIntent:
        token = str(request.arguments.get("session") or "")
        session = await self._load_session(token)
        if (
            session is None
            or session.owner_id != int(request.event.get("actor_id") or 0)
            or request.route.partition(".")[0] != session.namespace
        ):
            return self._ephemeral("Unavailable.")

        if session.global_scope:
            context = await self._global_context(request, job_id)
            if not set(session.guild_ids) <= {int(g["id"]) for g in context["guilds"]}:
                return self._ephemeral("Access changed. Reopen recap.")
        action = request.route.partition(".")[2]
        if action in {"clear_history", "confirm_clear_history", "cancel_clear_history"}:
            if session.namespace != "afk":
                return self._ephemeral("Unavailable.")
            if action == "clear_history":
                if session.clear_before is None:
                    session = msgspec.structs.replace(
                        session, clear_before=await self._store.read_cutoff()
                    )
                await self._save_session(session)
                return DiscordIntent(
                    kind="edit",
                    pages=(
                        PresentationPage(
                            description="clear all mentions?"
                        ),
                    ),
                    session_id=session.token,
                    controls=(
                        PresentationControl(
                            kind="button",
                            namespace="afk",
                            action="confirm_clear_history",
                            payload=f"{session.token}~0",
                            label="Confirm",
                            style="danger",
                        ),
                        PresentationControl(
                            kind="button",
                            namespace="afk",
                            action="cancel_clear_history",
                            payload=f"{session.token}~0",
                            label="Cancel",
                        ),
                    ),
                    presentation_mode="legacy",
                )
            if session.clear_before is None:
                return self._ephemeral("Unavailable.")
            if action == "confirm_clear_history":
                await self._store.clear_history(
                    session.owner_id,
                    session.guild_ids if session.global_scope else (session.guild_id,),
                    session.clear_before,
                )
                if self._media_store is not None:
                    await self._media_store.prune_afk()
                await self._cache.delete(f"lumi:afk:session:{session.token}")
                session = msgspec.structs.replace(
                    session,
                    token=secrets.token_hex(8),
                    page=0,
                    selected_author_id=None,
                    search_query=None,
                    clear_before=None,
                    frozen_selection=False,
                )
                interactions = await self._recap_interactions(
                    session.guild_id,
                    session.owner_id,
                    guild_ids=session.guild_ids or (session.guild_id,),
                )
                session = await self._freeze_session(session, interactions)
                return await self._rendered_recap(interactions, session, kind="edit")
            session = msgspec.structs.replace(session, clear_before=None)
        elif session.clear_before is not None:
            return self._ephemeral("Confirm or cancel first.")

        region = await self._saved_region(session.owner_id)
        if region is None:
            return self._ephemeral(self._region_prompt(session.command_prefix))
        session = msgspec.structs.replace(session, timezone_name=region)
        if action == "filter_user":
            values = request.arguments.get("values")
            if not isinstance(values, (list, tuple)) or len(values) != 1:
                return self._ephemeral("Unavailable.")
            try:
                selected_author_id = int(values[0])
            except (TypeError, ValueError):
                return self._ephemeral("Unavailable.")
            if selected_author_id <= 0:
                return self._ephemeral("Unavailable.")
            session = msgspec.structs.replace(
                session,
                page=0,
                selected_author_id=selected_author_id,
            )
        elif action == "search":
            values = request.arguments.get("values")
            raw_query = values.get("query") if isinstance(values, dict) else None
            query = self._normalize_search(str(raw_query or ""))
            if not query or len(query) > 200:
                return self._ephemeral("Enter a word or phrase.")
            session = msgspec.structs.replace(session, page=0, search_query=query)
        elif action == "clear":
            session = msgspec.structs.replace(
                session,
                page=0,
                selected_author_id=None,
                search_query=None,
            )
        elif action not in {"previous", "next", "jump", "cancel_clear_history"}:
            return self._ephemeral("Unavailable.")

        interactions = await self._recap_interactions(
            session.guild_id,
            session.owner_id,
            session.since_ms,
            session.since_message_id,
            unread_before=session.unread_before,
            guild_ids=session.guild_ids or (session.guild_id,),
            strict_since=session.strict_since,
        )
        if session.frozen_selection:
            sources = await self._store.recap_selection(session.token, session.owner_id)
            interactions = tuple(i for i in interactions if (i.guild_id, i.message_id) in sources)
        interactions = self._session_interactions(interactions, session)
        entries = await self._media_groups(interactions)
        page_count = (len(entries) + PAGE_SIZE - 1) // PAGE_SIZE

        if action == "jump":
            values = request.arguments.get("values")
            raw_page = values.get("page") if isinstance(values, dict) else None
            supplied = str(raw_page or "").strip()
            requested = int(supplied) - 1 if supplied.isdecimal() else -1
            if requested < 0 or requested >= page_count:
                return self._ephemeral(
                    f"Page must be between 1 and {page_count}."
                    if page_count
                    else "No pages available."
                )
        elif action in {"previous", "next"}:
            requested = int(request.arguments.get("page") or 0)
        else:
            requested = 0

        last_page = max(0, page_count - 1)
        session = msgspec.structs.replace(session, page=min(max(0, requested), last_page))
        await self._save_session(session)
        return await self._rendered_recap(interactions, session, kind="edit")

    async def _recap_interactions(
        self,
        guild_id,
        actor_id,
        since_ms=None,
        since_message_id=0,
        *,
        unread_before=None,
        guild_ids=None,
        since_at=None,
        strict_since=False,
    ):
        since = (
            datetime.fromtimestamp(since_ms // 1000, UTC).replace(
                microsecond=(since_ms % 1000) * 1000
            )
            if since_ms is not None
            else None
        )
        if since_at is not None:
            since = since_at
        interactions = []
        for source in guild_ids if guild_ids is not None else (guild_id,):
            interactions.extend(
                await self._store.interactions(
                    source,
                    actor_id,
                    since=since,
                    retained=since is not None,
                    unread_before=unread_before,
                )
            )
        if guild_ids is not None and len(guild_ids) > 1:
            interactions.sort(key=lambda i: (i.occurred_at, i.message_id), reverse=True)

        return tuple(
            item
            for item in interactions
            if (not strict_since or since is None or item.occurred_at >= since)
            and (since is None or item.occurred_at != since or item.message_id > since_message_id)
        )

    async def _global_context(self, request, job_id, *, include_activity=False):
        guild, actor = self._scope(request)
        result = await self._actions.request(
            job_id=job_id,
            kind="read_guild_sources",
            guild_id=guild,
            channel_id=int(
                request.event.get("origin_channel_id") or request.event.get("channel_id") or 0
            ),
            member_id=actor,
            idempotency_key=f"afk-context:{job_id}:{secrets.token_hex(4)}",
        )
        if not result.ok or not isinstance(result.data, dict) or not result.data.get("guilds"):
            raise ValueError("AFK scope unavailable")
        if include_activity:
            occurred, message = combine_last_spoke(result.data)
            return {**result.data, "last_spoke_ms": occurred, "last_spoke_message_id": message}
        return result.data

    async def _freeze_session(self, session, interactions):
        await self._store.freeze_recap(session.token, session.owner_id, interactions)
        session = msgspec.structs.replace(session, frozen_selection=True)
        await self._save_session(session)
        return session

    async def _message(self, request: TypedRequest, job_id: str) -> DiscordIntent | None:
        guild_id, actor_id = self._scope(request)
        notice_recipients = self._notice_recipients(request)
        recipients = tuple(
            (int(item.get("user_id") or 0), str(item.get("kind") or "direct"))
            for item in notice_recipients.values()
        )
        occurred_at = datetime.fromtimestamp(
            int(request.event.get("created_at_ms") or int(time.time() * 1000)) / 1000,
            tz=UTC,
        )
        saved = None
        if self._media_store is not None and request.event.get("captured_message"):
            from lumi.notes.capture import saved_message

            try:
                source = saved_message(request.event["captured_message"])
            except (TypeError, ValueError, msgspec.ValidationError):
                source = None
            if source and (source.guild_id, source.channel_id, source.message_id, source.author_id) == (
                guild_id, request.event.get("channel_id"), request.event.get("message_id"), actor_id,
            ):
                saved = source
        if recipients:
            await self._store.record_interactions(
                guild_id=guild_id,
                recipients=recipients,
                author_id=actor_id,
                author_name=str(request.event.get("author_name") or "Someone"),
                author_avatar_url=str(request.event.get("author_avatar_url") or "") or None,
                channel_id=int(request.event.get("channel_id") or 0),
                channel_name=str(request.event.get("channel_name") or "channel"),
                message_id=int(request.event.get("message_id") or 0),
                content=saved.content if saved else str(request.event.get("content") or ""),
                entity_labels=tuple(
                    (str(item[0]), str(item[1]))
                    for item in request.event.get("entity_labels", ())
                    if isinstance(item, (list, tuple)) and len(item) == 2
                ),
                occurred_at=occurred_at,
                reply_roots=tuple(
                    (int(item.get("user_id") or 0), int(item["reply_to_message_id"]))
                    for item in notice_recipients.values()
                    if item.get("reply_to_message_id")
                ),
            )
            if saved and saved.media:
                for recipient, _ in recipients:
                    await self._media_store.save(
                        recipient, saved, int(time.time() * 1000), kind="afk"
                    )

        fallback = await self._send_afk_notices(request, job_id, notice_recipients)
        recap: DiscordIntent | None = None
        if not bool(request.event.get("used_prefix")) and not request.event.get(
            "activity_exempt"
        ):
            active = await self._store.get_status(guild_id, actor_id)
            context = (
                await self._global_context(request, job_id)
                if active and active.global_scope
                else None
            )
            eligible = context is None or guild_id in {int(g["id"]) for g in context["guilds"]}
            state = (
                await self._store.clear_afk(guild_id, actor_id, returned_at=occurred_at)
                if active and eligible
                else None
            )
            if state is not None:
                await self._publisher.flush_safely()
                recap = await self._return_recap(
                    request, job_id, state, manual=False, scope_context=context
                )
        if recap is not None and fallback:
            return self._merge_notice(recap, fallback)
        if recap is not None:
            return recap
        return self._simple(fallback) if fallback else None

    async def _media_update(self, request):
        if self._media_store is None:
            return
        from lumi.shared.links import source_urls
        from lumi.notes.capture import saved_message
        from lumi.notes.media import safe_media_url

        try:
            saved = saved_message(request.event.get("hydrated_message"))
        except (TypeError, ValueError, msgspec.ValidationError):
            return
        if (saved.guild_id, saved.channel_id, saved.message_id, saved.author_id) != (
            request.event.get("guild_id"),
            request.event.get("channel_id"),
            request.event.get("message_id"),
            request.event.get("actor_id"),
        ) or any(item.preview for item in saved.media):
            return
        for user, content in await self._store.media_sources(
            saved.guild_id, saved.channel_id, saved.message_id, saved.author_id
        ):
            original = source_urls(content)
            media = tuple(
                item
                for item in saved.media
                if item.original_url in original and safe_media_url(item.url) and not item.spoiler
            )
            if media:
                await self._media_store.enrich_afk_media(
                    user, msgspec.structs.replace(saved, media=media), int(time.time() * 1000)
                )

    @staticmethod
    def _notice_recipients(request: TypedRequest) -> dict[int, dict]:
        recipients = {
            item["user_id"]: item
            for item in request.event.get("recipient_mentions", ())
            if isinstance(item, dict) and type(item.get("user_id")) is int and item["user_id"] > 0
        }
        reply = request.event.get("webhook_reply")
        if isinstance(reply, dict) and reply.get("module") == "afk":
            user, original = reply.get("user_id"), reply.get("original_message_id")
            if type(user) is int and user > 0 and type(original) is int and original > 0:
                recipient = recipients.get(user)
                if not recipient or recipient.get("kind") not in {"direct", "reply"}:
                    recipient = {**reply, "kind": "reply"}
                recipients[user] = {**recipient, "reply_to_message_id": original}
        return recipients

    async def _send_afk_notices(self, request: TypedRequest, job_id: str, recipients: dict) -> str:
        direct = {
            int(value) for value in request.event.get("mentioned_user_ids", ()) if int(value) > 0
        }


        direct.difference_update(
            user for user, item in recipients.items() if item.get("reply_to_message_id")
        )
        direct.intersection_update(
            user for user, item in recipients.items() if item.get("kind") == "direct"
        )
        if not direct:
            return ""
        statuses = await self._store.statuses(
            int(request.event.get("guild_id") or 0), tuple(direct)
        )
        async def deliver(state: AFKState) -> str | None:
            subject = recipients.get(state.user_id, {})
            name = str(subject.get("display_name") or f"User {state.user_id}")[:80]
            content = state.reason or "AFK"
            try:
                result = await self._actions.request(
                    job_id=job_id,
                    kind="send_recipient_webhook",
                    guild_id=state.guild_id,
                    channel_id=int(request.event.get("channel_id") or 0),
                    message_id=int(request.event.get("message_id") or 0),
                    member_id=state.user_id,
                    content=content,
                    idempotency_key=(
                        f"afk-notice:{request.event.get('message_id')}:{state.user_id}"
                    ),
                )
            except Exception:
                LOGGER.exception(
                    "afk_notice_action_failed",
                    extra={"guild_id": state.guild_id, "user_id": state.user_id},
                )
                result = None
            if result is not None and (result.ok or result.error_code == "ambiguous"):
                return None
            return f"{name} is AFK{f': {state.reason}' if state.reason else '.'}"

        delivered = await asyncio.gather(*(deliver(state) for state in statuses))
        fallback: list[str] = []
        for line in delivered:
            if line:
                fallback.append(line)
        return "\n".join(fallback)

    async def _return_recap(
        self,
        request: TypedRequest,
        job_id: str,
        state: AFKState,
        *,
        manual: bool,
        scope_context=None,
    ) -> DiscordIntent | None:
        return_guild = int(request.event.get("guild_id") or state.guild_id)
        mode = await self._store.get_mode(return_guild, state.user_id)


        await self._confirm_deliveries()
        if mode == "none":
            return None
        unread_before = await self._store.read_cutoff()
        context = (
            (scope_context or await self._global_context(request, job_id))
            if state.global_scope
            else None
        )
        guilds = tuple(int(g["id"]) for g in context["guilds"]) if context else (state.guild_id,)
        since = state.history_since or state.started_at
        interactions = await self._recap_interactions(
            return_guild,
            state.user_id,
            int(since.timestamp() * 1000),
            unread_before=unread_before,
            guild_ids=guilds,
            since_at=since,
        )
        if mode == "none" or not interactions:
            return None
        prefix = str(request.event.get("prefix") or ".")
        region = await self._saved_region(state.user_id)
        session = None
        if region is None:
            recap = self._simple(self._region_prompt(prefix), presentation_mode="legacy")
        else:
            since_ms = min(
                int(state.started_at.timestamp() * 1000),
                min(int(item.occurred_at.timestamp() * 1000) for item in interactions),
            )
            session = AFKRecapSession(
                secrets.token_hex(8),
                state.user_id,
                return_guild,
                region,
                (),
                command_prefix=prefix,
                since_ms=since_ms,
                namespace="afk_back",
                unread_before=unread_before,
                global_scope=state.global_scope,
                guild_ids=guilds,
                guild_names=tuple((int(g["id"]), str(g["name"])) for g in context["guilds"])
                if context
                else (),
            )
            session = await self._freeze_session(session, interactions)
            recap = await self._rendered_recap(interactions, session, kind="send")
            if recap.attachments:
                await self._store.stage_recap(
                    session.token, state.guild_id, state.user_id, interactions
                )
                await self._save_session(session)
        if mode == "channel" or (manual and not recap.attachments):
            return recap
        try:
            result = await self._actions.request(
                job_id=job_id,
                kind="send_dm",
                guild_id=return_guild,
                channel_id=int(request.event.get("channel_id") or 0),
                member_id=state.user_id,
                intent=recap,
                idempotency_key=f"afk-recap:{request.event.get('message_id')}:{state.user_id}",
                wait_seconds=30,
            )
            if result.ok and session is not None and recap.attachments:
                await self._store.confirm_recap(session.token)
        except Exception:
            LOGGER.exception(
                "afk_recap_dm_action_failed",
                extra={"guild_id": state.guild_id, "user_id": state.user_id},
            )
        return None

    async def _rendered_recap(
        self,
        interactions: tuple[AFKInteraction, ...],
        session: AFKRecapSession,
        *,
        kind: str,
    ) -> DiscordIntent:
        region = await self._saved_region(session.owner_id)
        if region is None:
            message = self._region_prompt(session.command_prefix)
            return (
                self._ephemeral(message)
                if kind == "edit"
                else self._simple(message, presentation_mode="legacy")
            )
        expanded = await self._media_groups(interactions)
        groups = tuple(group for group, _ in expanded)
        page_count = (len(groups) + PAGE_SIZE - 1) // PAGE_SIZE
        page = min(session.page, max(0, page_count - 1))
        controls = self._page_controls(session, page, page_count)
        if not interactions:
            empty_text = (
                "No replies or pings in the past 48 hours."
                if session.namespace == "afk"
                and session.selected_author_id is None
                and session.search_query is None
                else "No matches."
            )
            return DiscordIntent(
                kind=kind,
                pages=(PresentationPage(description=empty_text),),
                session_id=session.token,
                controls=controls,
                presentation_mode="legacy",
            )
        visible = groups[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
        offsets = [start for _, start in expanded[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]]
        media = {}
        if self._media_store is not None and visible:
            for source in {item.guild_id for group in visible for item in group}:
                media.update(
                    await self._media_store.afk_messages(
                        session.owner_id,
                        source,
                        [
                            item.message_id
                            for group in visible
                            for item in group
                            if item.guild_id == source
                        ],
                    )
                )
        from lumi.notes.module import render_media

        requests = [
            AFKRenderRequest(
                message_id=item.message_id,
                channel_name=(
                    f"{dict(session.guild_names).get(item.guild_id, str(item.guild_id))}"
                    f" · {item.channel_name}"
                    if session.global_scope
                    else item.channel_name
                ),
                author_name=item.author_name,
                avatar_url=_render_avatar_url(item.author_avatar_url),
                content=item.content,
                occurred_at=item.occurred_at,
                timezone_name=region,
                entity_labels=item.entity_labels,
                media=render_media(media[item.message_id], offset)
                if item.message_id in media
                else (),
                replies=tuple(
                    AFKRenderReply(
                        message_id=reply.message_id,
                        author_name=reply.author_name,
                        avatar_url=_render_avatar_url(reply.author_avatar_url),
                        content=reply.content,
                        occurred_at=reply.occurred_at,
                        entity_labels=reply.entity_labels,
                        media=render_media(media[reply.message_id], offset)
                        if reply.message_id in media
                        else (),
                    )
                    for reply in group[1:]
                ),
            )
            for group, offset in zip(visible, offsets, strict=True)
            for item in group[:1]
        ]
        rendered = await get_afk_message_renderer().render_many(requests)
        if await self._saved_region(session.owner_id) != region:
            message = "Region changed. Try again."
            return (
                self._ephemeral(message)
                if kind == "edit"
                else self._simple(message, presentation_mode="legacy")
            )
        attachments = tuple(
            await asyncio.gather(
                *(
                    self._temporary_attachment(data, group[-1].message_id)
                    for group, data in zip(visible, rendered, strict=True)
                )
            )
        )
        pages = tuple(
            PresentationPage(
                description=(
                    f"[Open message](https://discord.com/channels/"
                    f"{item.guild_id}/{item.channel_id}/{item.message_id})"
                    + "".join(
                        f" · [Reply {index}](https://discord.com/channels/"
                        f"{reply.guild_id}/{reply.channel_id}/{reply.message_id})"
                        for index, reply in enumerate(group[1:], 1)
                    )
                ),
                file_name=attachment.file_name,
            )
            for group, attachment in zip(visible, attachments, strict=True)
            for item in group[:1]
        )
        return DiscordIntent(
            kind=kind,
            pages=pages,
            attachments=attachments,
            session_id=session.token,
            controls=controls,
            presentation_mode="legacy",
            source_guild_ids=tuple(sorted({i.guild_id for i in interactions}))
            if session.global_scope
            else (),
        )

    async def _media_groups(self, interactions):
        groups = conversations(interactions)
        counts = {}
        if self._media_store is not None and interactions:
            for source in {item.guild_id for item in interactions}:
                counts.update(
                    await self._media_store.afk_counts(
                        interactions[0].user_id,
                        source,
                        [item.message_id for item in interactions if item.guild_id == source],
                    )
                )
        return tuple(
            (group, index * 10)
            for group in groups
            for index in range(max(counts.get(item.message_id, 1) for item in group))
        )

    @staticmethod
    def _page_controls(
        session: AFKRecapSession, page: int, page_count: int
    ) -> tuple[PresentationControl, ...]:
        token = session.token
        buttons = [
            PresentationControl(
                kind="button",
                namespace=session.namespace,
                action="previous",
                payload=f"{token}~{max(0, page - 1)}",
                label="Previous",
                disabled=page == 0,
            ),
            PresentationControl(
                kind="button",
                namespace=session.namespace,
                action="jump",
                payload=f"{token}~{page}",
                label=f"{page + 1}/{page_count}" if page_count else "0/0",
                disabled=page_count <= 1,
            ),
            PresentationControl(
                kind="button",
                namespace=session.namespace,
                action="next",
                payload=f"{token}~{min(max(0, page_count - 1), page + 1)}",
                label="Next",
                disabled=page_count <= 1 or page == page_count - 1,
            ),
            PresentationControl(
                kind="button",
                namespace=session.namespace,
                action="search",
                payload=f"{token}~{page}",
                label="Search",
            ),
        ]
        if session.selected_author_id is not None or session.search_query is not None:
            buttons.append(
                PresentationControl(
                    kind="button",
                    namespace=session.namespace,
                    action="clear",
                    payload=f"{token}~{page}",
                    label="Reset",
                )
            )
        history_controls = (
            (
                PresentationControl(
                    kind="button",
                    namespace="afk",
                    action="clear_history",
                    payload=f"{token}~{page}",
                    label="Clear",
                    style="danger",
                ),
            )
            if session.namespace == "afk"
            else ()
        )
        return (
            *buttons,
            PresentationControl(
                kind="user_select",
                namespace=session.namespace,
                action="filter_user",
                payload=f"{token}~{page}",
                placeholder="User",
                min_values=1,
                max_values=1,
                default_ids=(session.selected_author_id,)
                if session.selected_author_id is not None
                else (),
            ),
            *history_controls,
        )

    async def _temporary_attachment(self, data: bytes, message_id: int) -> TemporaryAttachment:
        identifier = secrets.token_hex(24)
        path = self._temp_root / identifier
        await asyncio.to_thread(path.write_bytes, data)
        return TemporaryAttachment(
            reference=f"tmp:{identifier}",
            file_name=f"afk-{message_id}-{identifier[:8]}.webp",
            sha256=hashlib.sha256(data).hexdigest(),
            size=len(data),
            expires_at_ms=int(time.time() * 1000) + 120_000,
        )

    async def _save_session(self, session: AFKRecapSession) -> None:
        await self._store.recap_lease(session.guild_id, session.owner_id)
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.setex(f"lumi:afk:session:{session.token}", SESSION_TTL, encode(session))
        pipeline.setex(ui_key("owner", session.token), SESSION_TTL, str(session.owner_id))
        await pipeline.execute()

    async def _save_settings_session(self, session: AFKSettingsSession) -> None:
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.setex(f"lumi:afk:settings:{session.token}", SESSION_TTL, encode(session))
        pipeline.setex(ui_key("owner", session.token), SESSION_TTL, str(session.owner_id))
        await pipeline.execute()

    async def _load_session(self, token: str) -> AFKRecapSession | None:
        payload = await self._cache.get(f"lumi:afk:session:{token}")
        if not payload:
            return None
        session = self._decode_recap_session(payload)
        await self._cache.expire(f"lumi:afk:session:{token}", SESSION_TTL)
        await self._cache.expire(ui_key("owner", token), SESSION_TTL)
        return session

    @staticmethod
    def _decode_recap_session(payload: bytes) -> AFKRecapSession:
        try:
            return decode(payload, AFKRecapSession)
        except msgspec.ValidationError:

            legacy = msgspec.json.decode(payload)
            retired = {"channel_query", "recap_format", "show_region_hint"}
            if not isinstance(legacy, dict) or not retired.intersection(legacy):
                raise
            for field in retired:
                legacy.pop(field, None)
            return msgspec.convert(legacy, type=AFKRecapSession)

    async def _load_settings_session(self, token: str) -> AFKSettingsSession | None:
        payload = await self._cache.get(f"lumi:afk:settings:{token}")
        if not payload:
            return None
        session = decode(payload, AFKSettingsSession)
        await self._cache.expire(f"lumi:afk:settings:{token}", SESSION_TTL)
        await self._cache.expire(ui_key("owner", token), SESSION_TTL)
        return session

    async def _saved_region(self, user_id: int) -> str | None:
        region = await self._store.get_region(user_id)
        return _REGIONS.get(region.casefold()) if isinstance(region, str) else None

    @staticmethod
    def _merge_notice(intent: DiscordIntent, notice: str) -> DiscordIntent:
        if not intent.pages:
            return intent
        first = intent.pages[0]
        updated = msgspec.structs.replace(
            first,
            description=(f"{notice}\n{first.description}" if first.description else notice),
        )
        return msgspec.structs.replace(intent, pages=(updated, *intent.pages[1:]))

    @staticmethod
    def _region_prompt(prefix: str) -> str:
        return f"Set `{prefix}region <timezone>` first.\n{REGION_HINT}"

    @classmethod
    def _session_interactions(
        cls,
        interactions: tuple[AFKInteraction, ...],
        session: AFKRecapSession,
    ) -> tuple[AFKInteraction, ...]:
        return tuple(
            item
            for item in interactions
            if (session.selected_author_id is None or item.author_id == session.selected_author_id)
            and (
                session.search_query is None
                or session.search_query in cls._normalize_search(item.content)
            )
        )

    @staticmethod
    def _normalize_search(value: str) -> str:
        return " ".join(unicodedata.normalize("NFKC", value).casefold().split())

    @staticmethod
    def _ephemeral(text: str, *, presentation_mode: str | None = None) -> DiscordIntent:
        return DiscordIntent(
            kind="ephemeral",
            pages=(PresentationPage(description=text),),
            presentation_mode=presentation_mode,
        )

    @staticmethod
    def _simple(text: str, *, presentation_mode: str | None = None) -> DiscordIntent:
        return DiscordIntent(
            kind="send",
            pages=(PresentationPage(description=text),),
            presentation_mode=presentation_mode,
        )

    def _result(self, job_id: str, intent: DiscordIntent | None) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="afk",
            group="community",
            generation=self._generation,
            status="ok",
            intents=(intent,) if intent is not None else (),
            completed_at_ms=int(time.time() * 1000),
        )

    @staticmethod
    def _event_time(request):
        return datetime.fromtimestamp(
            int(request.event.get("created_at_ms") or int(time.time() * 1000)) / 1000, tz=UTC
        )

    @staticmethod
    def _scope(request: TypedRequest) -> tuple[int, int]:
        guild_id = int(request.event.get("guild_id") or 0)
        actor_id = int(request.event.get("actor_id") or 0)
        if guild_id <= 0 or actor_id <= 0:
            raise ValueError("AFK requires a guild actor")
        return guild_id, actor_id

    async def _confirm_deliveries(self) -> None:
        tokens = await self._store.pending_recaps()
        if not tokens:
            return


        values = await self._cache.mget(
            [
                key
                for token in tokens
                for key in (ui_key("message", token), f"lumi:afk:session:{token}")
            ]
        )
        for index, token in enumerate(tokens):
            bound, payload = values[index * 2 : index * 2 + 2]
            if not payload:
                continue
            try:
                session = self._decode_recap_session(payload)
            except msgspec.DecodeError:
                continue

            if session.namespace != "afk_back" or session.token != token:
                continue
            value = bound.decode() if isinstance(bound, bytes) else str(bound or "")
            if value.isdigit() and int(value) > 0:
                await self._store.confirm_recap(token)

    async def _retention_loop(self) -> None:
        tick = 0
        while True:
            if self._draining:
                await asyncio.sleep(0.05)
                continue
            self._retention_busy = True
            try:
                await self._confirm_deliveries()
                moved = await self._store.prune()
                if tick % 12 == 0:
                    await self._store.archive.cleanup()
                tick += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("afk_retention_failed")
                moved = False
            finally:
                self._retention_busy = False
            await asyncio.sleep(1 if moved else 5)

    def begin_drain(self):
        self._draining = True
        self._publisher.draining = True

    def resume(self):
        self._draining = False
        self._publisher.draining = False

    def drain_ready(self):
        return not self._retention_busy and not self._publisher.busy

    async def close(self) -> None:
        self._publisher.stop()
        for task in (self._publisher_task, self._retention_task):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._publisher_task, self._retention_task) if task),
            return_exceptions=True,
        )
        await close_afk_message_renderer()
        with contextlib.suppress(OSError):
            for path in self._temp_root.glob("*"):
                if path.is_file() and time.time() - path.stat().st_mtime > 120:
                    path.unlink()
