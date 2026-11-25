from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import secrets
import time
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import msgspec

from lumi.afk.renderer import AFKRenderMedia, AFKRenderRequest, get_afk_message_renderer
from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    MatcherExpr,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResponseRevision,
    ResultEnvelope,
    TemporaryAttachment,
)
from lumi.contracts.reaction_subscriptions import ReactionObservation
from lumi.reminders.commands import duration, pretty_duration
from lumi.shared.response_revisions import publish_revision
from lumi.ui_keys import ui_key

from .capture import saved_message
from .media import MediaService
from .models import ReactionCapture, ReminderMapping, SavedMessage, emoji_key, valid_emoji
from .publisher import ShortcutPublisher, shortcut_tag

LOGGER = logging.getLogger(__name__)
TTL = 900_000


def components(module="notes"):
    namespace, acl = ("notes", "notes") if module == "notes" else ("quickreminders", "remind")
    modals = {
        "search": ModalSpec(
            "Search notes", (ModalFieldSpec("query", "Text", required=False, maximum=200),)
        ),
        "author_search": ModalSpec(
            "Find authors", (ModalFieldSpec("query", "Name or ID", required=False, maximum=100),)
        ),
        "emoji": ModalSpec(
            "Notes emoji", (ModalFieldSpec("emoji", "Emoji", kind="emoji", maximum=100),)
        ),
        "mapping": ModalSpec(
            "Reminder emoji",
            (
                ModalFieldSpec("emoji", "Emoji", kind="emoji", maximum=100),
                ModalFieldSpec("duration", "When", maximum=100, placeholder="1h 30m or ask"),
            ),
        ),
        "before": ModalSpec(
            "Delete before",
            (ModalFieldSpec("value", "Date", maximum=10, placeholder="YYYY-MM-DD"),),
        ),
        "older": ModalSpec(
            "Delete older than", (ModalFieldSpec("value", "Age", maximum=100, placeholder="30d"),)
        ),
        "server": ModalSpec(
            "Delete from server",
            (
                ModalFieldSpec(
                    "value",
                    "Server ID",
                    maximum=20,
                    required=False,
                    placeholder="Blank = this server",
                ),
            ),
        ),
        "delete_search": ModalSpec(
            "Find notes", (ModalFieldSpec("query", "Text", required=False, maximum=200),)
        ),
    }
    actions = (
        "previous",
        "next",
        "search",
        "users",
        "author_search",
        "author_previous",
        "author_next",
        "authors",
        "home",
        "settings",
        "emoji",
        "clear_emoji",
        "delete",
        "select",
        "pick",
        "delete_search",
        "delete_previous",
        "delete_next",
        "selected",
        "before",
        "older",
        "server",
        "all",
        "confirm",
        "cancel",
    )
    if module == "reminders":
        actions = ("settings", "mapping", "remove_mapping", "delivery")
    selects = {"authors", "pick", "remove_mapping", "delivery"}
    return tuple(
        ComponentSpec(
            namespace,
            action,
            f"{module}.ui",
            r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,8})$",
            captures=(
                CaptureSpec("session"),
                CaptureSpec("revision", "integer", minimum=0, maximum=99_999_999),
            ),
            acl_key=acl,
            public=True,
            session_bound=True,
            deadline_ms=120_000,
            input_kind="modal"
            if action in modals
            else "string_values"
            if action in selects
            else "none",
            modal=modals.get(action),
            presentation_mode="v2",
            context_fields=("prefix",),
        )
        for action in actions
    )


def capture_from_request(request):
    event = request.event
    observation = msgspec.convert(event.get("reaction_observation"), type=ReactionObservation)
    message = saved_message(observation.message)
    capture = ReactionCapture(
        observation.activation, observation.emoji, observation.revision, observation.received_at_ms,
        message,
    )
    if (
        event.get("dispatch") != "passive"
        or capture.message.guild_id != event.get("guild_id")
        or capture.message.channel_id != event.get("channel_id")
        or capture.message.message_id != event.get("message_id")
        or any(media.preview for media in capture.message.media)
    ):
        raise ValueError("Invalid reaction capture.")
    return capture, observation.tag


def render_media(message, start=0):
    values = []
    for media in message.media[start : start + 10]:
        preview = None
        if media.preview:
            try:
                preview = base64.b64decode(media.preview, validate=True)
            except ValueError:
                pass
        values.append(AFKRenderMedia(media.url, preview, media.original_url))
    return tuple(values)


class NotesModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        cache_redis,
        store,
        region_provider,
        temp_root: Path,
        reminder_store=None,
        renderer=None,
        media_service=None,
        clock=None,
    ):
        self.generation, self.queue, self.cache, self.store = (
            generation,
            queue_redis,
            cache_redis,
            store,
        )
        self.region_provider, self.temp_root = region_provider, temp_root
        self.reminder_store = reminder_store
        self.renderer = renderer or get_afk_message_renderer()
        self.media = media_service or MediaService(store)
        self.publisher = ShortcutPublisher(store, queue_redis)
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.task = None
        self.stopped = self.draining = self.busy = False
        self.wake = asyncio.Event()
        self.last_archive = 0

    @property
    def manifest(self):
        return Manifest(
            2,
            "notes",
            "community",
            self.generation,
            "lumi:jobs:community",
            display_name="Notes",
            summary="Save messages with a reaction.",
            version="1.0.2",
            commands=(
                CommandSpec(
                    "notes",
                    "notes.ui",
                    r"^\s*(?P<section>settings)?\s*$",
                    captures=(CaptureSpec("section", required=False),),
                    acl_key="notes",
                    public=True,
                    context_fields=("prefix",),
                    deadline_ms=120_000,
                    help=HelpSpec(
                        ".notes\n.notes settings", "View, search and manage your saved messages."
                    ),
                ),
            ),
            passive=(
                PassiveMatcherSpec(
                    "notes.reaction",
                    "notes.reaction",
                    "reaction_add",
                    MatcherExpr("exists", field="reaction_observation"),
                    subject_field="actor_id",
                    subject_required=True,
                    personal_reaction=True,
                    acl_key="notes",
                    context_fields=("reaction_observation",),
                ),
            ),
            components=components(),
        )

    async def start(self):
        self.temp_root.mkdir(parents=True, exist_ok=True)
        if self.temp_root.is_symlink():
            raise ValueError("Unsafe temporary storage.")
        await self.publisher.flush(restore=True)
        archive = getattr(self.store, "archive", None)
        if archive:
            await archive.collect_orphans()
        self.task = asyncio.create_task(self.run(), name="notes-maintenance")

    async def run(self):
        while not self.stopped:
            self.wake.clear()
            if not self.draining:
                self.busy = True
                try:
                    await self.publisher.flush()
                    await self.media.tick()
                    archive = getattr(self.store, "archive", None)
                    if not self.draining and archive and self.clock() - self.last_archive >= 60_000:
                        await archive.archive_old(self.clock())
                        await archive.collect_orphans()
                        if hasattr(self.store, "prune_afk"):
                            await self.store.prune_afk()
                        await self.store.cleanup_receipts(self.clock())
                        self.last_archive = self.clock()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.warning("notes_maintenance_unavailable")
                finally:
                    self.busy = False
            try:
                await asyncio.wait_for(self.wake.wait(), 2)
            except TimeoutError:
                pass

    def begin_drain(self):
        self.draining = True
        self.media.draining = True

    def resume(self):
        self.draining = False
        self.media.draining = False
        self.wake.set()

    def drain_ready(self):
        return not self.busy

    async def close(self):
        self.begin_drain()
        self.stopped = True
        self.wake.set()
        if self.task:
            await self.task
        await self.media.close()

    def result(self, job, module="notes", intent=None, session=None):
        return ResultEnvelope(
            1,
            job,
            module,
            "community",
            self.generation,
            "ok",
            intents=(intent,) if intent else (),
            response_revision=ResponseRevision(session["token"], session["revision"])
            if session
            else None,
        )

    async def handle(self, request, job_id, generation):
        return await self.handle_as(request, job_id, generation, "notes")

    async def handle_as(self, request, job, generation, module):
        if (
            generation != self.generation
            or request.event.get("_expires_at_ms", self.clock() + 1) <= self.clock()
        ):
            return self.result(job, module)
        if request.handler == "notes.reaction":
            try:
                capture, tag = capture_from_request(request)
            except (TypeError, ValueError, msgspec.ValidationError):
                return self.result(job)
            user = int(request.event["actor_id"])
            prefs = await self.store.shortcuts(user)
            if (
                prefs.notes_emoji and emoji_key(prefs.notes_emoji) == emoji_key(capture.emoji)
                and tag == shortcut_tag(prefs.notes_emoji)
            ):
                await self.store.save(
                    user, capture.message, capture.received_at_ms, activation=capture.activation
                )
                self.wake.set()
            return self.result(job)
        if request.event.get("dispatch") == "command":
            session = dict(
                token=secrets.token_hex(8),
                owner_id=int(request.event["actor_id"]),
                guild_id=int(request.event["guild_id"]),
                channel_id=int(request.event["channel_id"]),
                expires_at_ms=self.clock() + TTL,
                revision=0,
                module=module,
                page=0,
                query="",
                authors=[],
                author_query="",
                author_page=0,
                view="settings"
                if module == "reminders" or request.arguments.get("section") == "settings"
                else "notes",
                prefix=request.event.get("prefix") or ".",
                selected=[],
                delete_query="",
                delete_page=0,
                frozen=[],
            )
            intent = await self.render(session, "send")
            await self.save_session(session)
            return self.result(job, module, intent, session)
        token = str(request.arguments.get("session") or "")
        async with self.cache.lock(
            f"lumi:personal-ui-lock:{module}:{token}", timeout=120, blocking_timeout=3
        ):
            raw = await self.cache.get(f"lumi:personal-ui:{module}:{token}")
            session = msgspec.json.decode(raw) if raw else None
            if (
                not session
                or session["owner_id"] != request.event.get("actor_id")
                or session["guild_id"] != request.event.get("guild_id")
                or session["channel_id"] != request.event.get("channel_id")
                or session["expires_at_ms"] <= self.clock()
                or session["revision"] != request.arguments.get("revision")
            ):
                return self.result(job, module)
            try:
                await self.mutate(session, request)
                session["notice"] = session.get("notice", "")
            except (ValueError, ZoneInfoNotFoundError) as error:
                session["notice"] = str(error)[:100]
            session["revision"] += 1
            intent = await self.render(session, "edit")
            await self.save_session(session)
            return self.result(job, module, intent, session)

    async def save_session(self, session):
        ttl = max(1, (session["expires_at_ms"] - self.clock() + 999) // 1000)
        p = self.cache.pipeline(transaction=True)
        p.set(
            f"lumi:personal-ui:{session['module']}:{session['token']}",
            msgspec.json.encode(session),
            ex=ttl,
        )
        p.set(ui_key("owner", session['token']), str(session["owner_id"]), ex=ttl)
        publish_revision(
            p, "community", session["token"], module=session["module"], actor_id=session["owner_id"],
            guild_id=session["guild_id"], channel_id=session["channel_id"], revision=session["revision"],
            expires_at_ms=session["expires_at_ms"], ttl=ttl,
        )
        await p.execute()

    def control(
        self, session, action, label=None, *, options=(), selected=(), maximum=1, disabled=False
    ):
        return PresentationControl(
            "string_select" if options else "button",
            "notes" if session["module"] == "notes" else "quickreminders",
            action,
            f"{session['token']}~{session['revision']}",
            label=None if options else label,
            placeholder=label if options else None,
            options=tuple(options),
            min_values=0 if selected else 1,
            max_values=maximum,
            disabled=disabled,
        )

    async def mutate(self, s, request):
        action = request.route.rsplit(".", 1)[-1]
        values = request.arguments.get("values", {})
        s["notice"] = ""
        user = s["owner_id"]
        if action in {"previous", "next"}:
            s["page"] = max(0, s["page"] + (1 if action == "next" else -1))
        elif action == "search":
            s.update(query=str(values.get("query", ""))[:200], page=0)
        elif action == "users":
            s.update(view="users", author_page=0)
        elif action == "author_search":
            s.update(author_query=str(values.get("query", ""))[:100], author_page=0)
        elif action in {"author_previous", "author_next"}:
            s["author_page"] = max(0, s["author_page"] + (1 if action == "author_next" else -1))
        elif action == "authors":
            page, _ = await self.store.author_page(
                user, query=s["author_query"], page=s["author_page"]
            )
            visible = {str(identifier) for identifier, _ in page}
            supplied = {str(value) for value in values}
            if not supplied <= visible:
                raise ValueError("User unavailable.")
            selected = sorted(
                ({int(v) for v in s["authors"]} - {int(v) for v in visible})
                | {int(v) for v in supplied}
            )
            if len(selected) > 25:
                raise ValueError("Choose up to 25 users.")
            s["authors"] = selected
            s["page"] = 0
        elif action in {"home", "settings", "delete", "select", "cancel"}:
            s["view"] = {
                "home": "notes",
                "settings": "settings",
                "delete": "delete",
                "select": "select",
                "cancel": "delete",
            }[action]
            if action in {"delete", "cancel"}:
                s.update(frozen=[], selected=[])
        elif action in {"emoji", "clear_emoji"}:
            value = valid_emoji(str(values.get("emoji", ""))) if action == "emoji" else ""
            await self.store.configure(
                user, lambda prefs: msgspec.structs.replace(prefs, notes_emoji=value)
            )
            await self.publisher.flush()
        elif action == "mapping":
            value = valid_emoji(str(values.get("emoji", "")))
            timing = str(values.get("duration", "")).strip()
            wait = 0 if timing.casefold() == "ask" else duration(timing, False)

            def change(prefs):
                entries = [m for m in prefs.reminders if emoji_key(m.emoji) != emoji_key(value)]
                if len(entries) >= 25:
                    raise ValueError("Maximum 25 emojis.")
                return msgspec.structs.replace(
                    prefs, reminders=(*entries, ReminderMapping(value, wait))
                )

            await self.store.configure(user, change)
            await self.publisher.flush()
        elif action == "remove_mapping":
            supplied = set(values)
            await self.store.configure(
                user,
                lambda prefs: msgspec.structs.replace(
                    prefs,
                    reminders=tuple(
                        m for m in prefs.reminders if emoji_key(m.emoji) not in supplied
                    ),
                ),
            )
            await self.publisher.flush()
        elif action == "delivery":
            if len(values) != 1 or values[0] not in {"dm", "channel"}:
                raise ValueError("Choose DM or Channel.")
            await self.reminder_store.set_reaction_delivery(s["guild_id"], user, values[0])
        elif action == "delete_search":
            s.update(delete_query=str(values.get("query", ""))[:200], delete_page=0)
        elif action in {"delete_previous", "delete_next"}:
            s["delete_page"] = max(0, s["delete_page"] + (1 if action == "delete_next" else -1))
        elif action == "pick":
            ids = await self.store.matching_ids(user, query=s["delete_query"])
            visible = set(ids[s["delete_page"] * 25 : s["delete_page"] * 25 + 25])
            supplied = set(values)
            if not supplied <= visible:
                raise ValueError("Note unavailable.")
            s["selected"] = sorted((set(s["selected"]) - visible) | supplied)
        elif action in {"selected", "before", "older", "server", "all"}:
            before, guild = None, 0
            if action == "selected":
                ids = s["selected"]
            else:
                raw = str(values.get("value", "")) if isinstance(values, dict) else ""
                if action == "before":
                    zone = await self.zone(user)
                    if not zone:
                        raise ValueError("Set your region first.")
                    try:
                        before = int(
                            datetime.strptime(raw, "%Y-%m-%d")
                            .replace(tzinfo=ZoneInfo(zone))
                            .timestamp()
                            * 1000
                        )
                    except ValueError as error:
                        raise ValueError("Use YYYY-MM-DD.") from error
                elif action == "older":
                    before = self.clock() - duration(raw, False)
                elif action == "server":
                    if raw and (not raw.isascii() or not raw.isdecimal() or len(raw) > 20):
                        raise ValueError("Use a server ID.")
                    guild = int(raw) if raw else s["guild_id"]
                ids = await self.store.matching_ids(user, before=before, guild=guild)
            s.update(frozen=list(ids), view="confirm")
        elif action == "confirm":
            count = await self.store.delete(user, s["frozen"])
            s.update(view="delete", frozen=[], selected=[], notice=f"Deleted {count}.")

    async def zone(self, user):
        value = await self.region_provider(user)
        try:
            return ZoneInfo(value).key if value else None
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            return None

    async def render(self, s, kind):
        def c(action, label=None, **kw):
            return self.control(s, action, label, **kw)

        pages, attachments = [], []
        user, view = s["owner_id"], s["view"]
        notice = s.get("notice", "")
        if view == "notes":
            zone = await self.zone(user)
            controls = (c("search", "Search"), c("users", "Users"), c("settings", "Settings"))
            if not zone:
                pages = [
                    PresentationPage(
                        description=f"{s['prefix']}region <timezone>\n"
                        "its often continent/capital ie Europe/London",
                        controls=controls,
                    )
                ]
            else:
                cards, total = await self.store.card_page(
                    user, query=s["query"], authors=s["authors"], page=s["page"]
                )
                pages = [
                    PresentationPage(
                        title="Notes",
                        description=notice or ("No notes." if not total else ""),
                        footer=f"{s['page'] + 1}/{max(1, (total + 4) // 5)}",
                        controls=(
                            *controls,
                            c("previous", "Previous", disabled=s["page"] <= 0),
                            c("next", "Next", disabled=(s["page"] + 1) * 5 >= total),
                        ),
                    )
                ]
                for row, start in cards:
                    message = msgspec.convert(row["document"], type=SavedMessage)
                    data = await self.renderer.render(
                        AFKRenderRequest(
                            message.message_id,
                            f"{message.guild_name} · {message.channel_name}",
                            message.author_name,
                            message.avatar_url or None,
                            message.content if start == 0 else "",
                            datetime.fromtimestamp(message.occurred_at_ms / 1000, UTC),
                            zone,
                            message.entity_labels,
                            media=render_media(message, start),
                        )
                    )
                    identifier = secrets.token_hex(24)
                    self.temp_root.mkdir(parents=True, exist_ok=True)
                    if self.temp_root.is_symlink():
                        raise ValueError("Unsafe temporary storage.")
                    from .archive import file_io

                    await file_io((self.temp_root / identifier).write_bytes, data)
                    filename = f"note-{message.message_id}-{start}.webp"
                    attachments.append(
                        TemporaryAttachment(
                            f"tmp:{identifier}",
                            filename,
                            hashlib.sha256(data).hexdigest(),
                            len(data),
                            self.clock() + 120_000,
                        )
                    )
                    pages.append(
                        PresentationPage(
                            file_name=filename, description=f"[Open message]({message.jump_url})"
                        )
                    )
                if await self.zone(user) != zone:
                    for attachment in attachments:
                        (self.temp_root / attachment.reference.removeprefix("tmp:")).unlink(
                            missing_ok=True
                        )
                    attachments = []
                    pages = [
                        PresentationPage(
                            description="Region changed. Try again.", controls=controls
                        )
                    ]
        elif view == "users":
            people, total = await self.store.author_page(
                user, query=s["author_query"], page=s["author_page"]
            )
            controls = [c("author_search", "Search"), c("home", "Back")]
            if people:
                controls.append(
                    c(
                        "authors",
                        "Users",
                        options=tuple(
                            PresentationOption(
                                name[:80] or str(identifier),
                                str(identifier),
                                default=identifier in s["authors"],
                            )
                            for identifier, name in people
                        ),
                        selected=(True,),
                        maximum=min(25, len(people)),
                    )
                )
            controls.extend(
                (
                    c("author_previous", "Previous", disabled=s["author_page"] <= 0),
                    c("author_next", "Next", disabled=(s["author_page"] + 1) * 25 >= total),
                )
            )
            pages = [
                PresentationPage(
                    title="Users",
                    description="" if people else "No users.",
                    controls=tuple(controls),
                )
            ]
        elif view == "settings":
            prefs = await self.store.shortcuts(user)
            if s["module"] == "notes":
                pages = [
                    PresentationPage(
                        title="Notes",
                        description=notice or prefs.notes_emoji or "Not set.",
                        controls=(
                            c("emoji", "Set emoji"),
                            c("clear_emoji", "Clear"),
                            c("delete", "Delete notes"),
                            c("home", "Back"),
                        ),
                    )
                ]
            else:
                mode = await self.reminder_store.reaction_delivery(s["guild_id"], user)
                controls = [
                    c("mapping", "Set emoji"),
                    c(
                        "delivery",
                        "Delivery",
                        options=(
                            PresentationOption("DM", "dm", default=mode == "dm"),
                            PresentationOption("Channel", "channel", default=mode == "channel"),
                        ),
                    ),
                ]
                if prefs.reminders:
                    controls.append(
                        c(
                            "remove_mapping",
                            "Remove emoji",
                            options=tuple(
                                PresentationOption(
                                    (
                                        f"{m.emoji} · "
                                        + (
                                            pretty_duration(m.duration_ms)
                                            if m.duration_ms
                                            else "Ask when"
                                        )
                                    )[:100],
                                    emoji_key(m.emoji),
                                )
                                for m in prefs.reminders
                            ),
                        )
                    )
                pages = [
                    PresentationPage(
                        title="Quick reminders", description=notice, controls=tuple(controls)
                    )
                ]
        elif view == "delete":
            pages = [
                PresentationPage(
                    title="Delete notes",
                    description=notice,
                    controls=tuple(
                        c(action, label)
                        for action, label in (
                            ("select", "Select notes"),
                            ("before", "Before date"),
                            ("older", "Older than"),
                            ("server", "Server"),
                            ("all", "All notes"),
                            ("settings", "Back"),
                        )
                    ),
                )
            ]
        elif view == "select":
            ids = await self.store.matching_ids(user, query=s["delete_query"])
            selected = ids[s["delete_page"] * 25 : s["delete_page"] * 25 + 25]
            options = []
            for identifier in selected:
                row = await self.store.snapshot(identifier)
                if row:
                    text = (
                        " ".join(row["document"]["content"].split())
                        or row["document"]["author_name"]
                    )
                    options.append(
                        PresentationOption(
                            text[:80], identifier, default=identifier in s["selected"]
                        )
                    )
            controls = [c("delete_search", "Search"), c("cancel", "Back")]
            if options:
                controls.append(
                    c("pick", "Notes", options=options, selected=(True,), maximum=len(options))
                )
            controls.extend(
                (
                    c("delete_previous", "Previous", disabled=s["delete_page"] <= 0),
                    c("delete_next", "Next", disabled=(s["delete_page"] + 1) * 25 >= len(ids)),
                    c("selected", "Delete selected", disabled=not s["selected"]),
                )
            )
            pages = [
                PresentationPage(
                    title="Delete notes",
                    description="" if options else "No notes.",
                    controls=tuple(controls),
                )
            ]
        elif view == "confirm":
            pages = [
                PresentationPage(
                    description=f"Delete {len(s['frozen'])} notes?",
                    controls=(
                        c("confirm", "Delete", disabled=not s["frozen"]),
                        c("cancel", "Cancel"),
                    ),
                )
            ]
        if notice and pages and view not in {"delete", "notes"}:
            pages[0] = msgspec.structs.replace(pages[0], description=notice)
        return DiscordIntent(
            kind,
            pages=tuple(pages),
            attachments=tuple(attachments),
            session_id=s["token"],
            presentation_mode="v2",
        )
