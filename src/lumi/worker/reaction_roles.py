from __future__ import annotations

from lumi.ui_keys import ui_key

import logging
import re
import secrets
import time
from hashlib import sha256
from pathlib import Path

import emoji as emoji_lib
import msgspec
from redis.asyncio import Redis

from lumi.contracts import (
    ChannelTransitionSpec,
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    MatcherExpr,
    ModalChoice,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    PresentationBlock,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
    decode,
    encode,
)
from lumi.panels import (
    ButtonRoleBlock,
    ButtonRoleContainer,
    ButtonRolePanel,
    MySQLPanelStore,
    ReactionMapping,
    ReactionPanel,
)

from lumi.contracts.layout_message import WRITE_LAYOUT_MESSAGE
from lumi.contracts.role_access import READ_ACTOR_ROLES, WRITE_ACTOR_ROLES

from .actions import EdgeActionClient
from .button_roles import ButtonPanelError, ButtonRoleService, first_file, one_selected
from .reaction_publisher import ReactionIndexPublisher

LOGGER = logging.getLogger(__name__)
TEXT_CHANNEL_TYPES = (0, 5, 10, 11, 12)

CUSTOM_EMOJI = re.compile(r"^<a?:[A-Za-z0-9_]{2,32}:\d{17,20}>$")
MESSAGE_LINK = re.compile(
    r"^(?:https://(?:canary\.|ptb\.)?discord\.com/channels/)?"
    r"(?P<guild>\d{17,20})[ /](?P<channel>\d{17,20})[ /](?P<message>\d{17,20})/?$"
)


class ReactionRoleSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    screen: str = "home"
    panel_id: str = ""
    channel_id: int = 0
    message_id: int = 0
    ownership: str = "lumi"
    mapping_id: str = ""
    emoji: str = ""
    label: str = ""
    role_id: int = 0
    preview_channel_id: int = 0
    panel_kind: str = "reaction"
    container_id: str = ""
    block_id: str = ""
    container_page: int = 0
    block_page: int = 0
    mapping_page: int = 0
    preview_page: int = 0


class ReactionRolesModule:
    SESSION_TTL = 900

    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        store: MySQLPanelStore,
        temp_root: Path = Path("/REDACTED/tmp"),
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._store = store
        self._actions = EdgeActionClient(
            queue_redis,
            module="reaction_roles",
            group="community",
            generation=generation,
        )
        self._button_roles = ButtonRoleService(
            store=store,
            actions=self._actions,
            temp_root=temp_root,
        )
        self._publisher = ReactionIndexPublisher(store, queue_redis)
        self._publisher_task = None
        self._last_panels: dict[int, tuple[ReactionPanel, ...]] = {}
        self._last_mappings: dict[str, tuple[ReactionMapping, ...]] = {}
        self._last_button_panels: dict[int, tuple[ButtonRolePanel, ...]] = {}
        self._last_button_containers: dict[
            str, tuple[ButtonRoleContainer, ...]
        ] = {}
        self._last_button_blocks: dict[str, tuple[ButtonRoleBlock, ...]] = {}

    @property
    def manifest(self) -> Manifest:
        session = r"(?P<session>[a-f0-9]{16})"
        manager = ("manage_roles",)
        dashboard_deadline = 120_000

        def button(action: str) -> ComponentSpec:
            return ComponentSpec(
                "reactionrole",
                action,
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            )

        components = (
            ComponentSpec(
                "reactionrole",
                "pick_panel",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="string_values",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "pick_mapping",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="string_values",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "channel",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="channel_ids",
                deadline_ms=dashboard_deadline,
                channel_types=TEXT_CHANNEL_TYPES,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "role",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="role_ids",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "attach",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="modal",
                deadline_ms=dashboard_deadline,
                modal=ModalSpec(
                    "Attach message",
                    (
                        ModalFieldSpec(
                            "message",
                            "Message link or IDs",
                            minimum=1,
                            maximum=200,
                        ),
                    ),
                ),
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "details",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="modal",
                deadline_ms=dashboard_deadline,
                modal=ModalSpec(
                    "Mapping",
                    (
                        ModalFieldSpec(
                            "emoji",
                            "Emoji",
                            kind="emoji",
                            minimum=1,
                            maximum=160,
                            placeholder="Emoji or :name:",
                        ),
                        ModalFieldSpec("label", "Label", required=False, maximum=200),
                    ),
                ),
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            *(
                button(action)
                for action in (
                    "create",
                    "save_panel",
                    "add",
                    "save_mapping",
                    "remove",
                    "up",
                    "down",
                    "mappings_prev",
                    "mappings_next",
                    "repair",
                    "detach",
                    "back",
                    "panel_back",
                    "inspect",
                    "preview",
                    "convert",
                )
            ),
            ComponentSpec(
                "reactionrole",
                "preview_channel",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="channel_ids",
                deadline_ms=dashboard_deadline,
                channel_types=TEXT_CHANNEL_TYPES,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "button_pick_container",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="string_values",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "button_pick_block",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="string_values",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            ComponentSpec(
                "reactionrole",
                "button_channel",
                "reaction_roles.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="channel_ids",
                deadline_ms=dashboard_deadline,
                channel_types=TEXT_CHANNEL_TYPES,
                session_bound=True,
                acl_key="reactionrole",
                required_permissions=manager,
            ),
            *(
                ComponentSpec(
                    "reactionrole",
                    action,
                    "reaction_roles.dashboard",
                    rf"^{session}$",
                    captures=(CaptureSpec("session"),),
                    input_kind="modal",
                    deadline_ms=dashboard_deadline,
                    modal=ModalSpec(
                        "Heading",
                        (
                            ModalFieldSpec("text", "Heading", minimum=1, maximum=256),
                            ModalFieldSpec(
                                "level",
                                "Size",
                                kind="string_select",
                                choices=(
                                    ModalChoice("Large", "1"),
                                    ModalChoice("Medium", "2"),
                                    ModalChoice("Small", "3"),
                                ),
                            ),
                        ),
                    ),
                    session_bound=True,
                    acl_key="reactionrole",
                    required_permissions=manager,
                )
                for action in ("button_add_heading", "button_edit_heading")
            ),
            *(
                ComponentSpec(
                    "reactionrole",
                    action,
                    "reaction_roles.dashboard",
                    rf"^{session}$",
                    captures=(CaptureSpec("session"),),
                    input_kind="modal",
                    deadline_ms=dashboard_deadline,
                    modal=ModalSpec(
                        "Text",
                        (
                            ModalFieldSpec(
                                "text",
                                "Text",
                                style="paragraph",
                                minimum=1,
                                maximum=4000,
                            ),
                        ),
                    ),
                    session_bound=True,
                    acl_key="reactionrole",
                    required_permissions=manager,
                )
                for action in ("button_add_text", "button_edit_text")
            ),
            *(
                ComponentSpec(
                    "reactionrole",
                    action,
                    "reaction_roles.dashboard",
                    rf"^{session}$",
                    captures=(CaptureSpec("session"),),
                    input_kind="modal",
                    deadline_ms=dashboard_deadline,
                    modal=ModalSpec(
                        "Image URL",
                        (ModalFieldSpec("url", "HTTPS image URL", minimum=1, maximum=2048),),
                    ),
                    session_bound=True,
                    acl_key="reactionrole",
                    required_permissions=manager,
                )
                for action in ("button_add_image_url", "button_edit_image_url")
            ),
            *(
                ComponentSpec(
                    "reactionrole",
                    action,
                    "reaction_roles.dashboard",
                    rf"^{session}$",
                    captures=(CaptureSpec("session"),),
                    input_kind="modal",
                    deadline_ms=dashboard_deadline,
                    modal=ModalSpec(
                        "Image",
                        (
                            ModalFieldSpec(
                                "image",
                                "Image",
                                kind="file_upload",
                                file_types=("image",),
                            ),
                        ),
                    ),
                    session_bound=True,
                    acl_key="reactionrole",
                    required_permissions=manager,
                )
                for action in ("button_upload_image", "button_edit_upload")
            ),
            *(
                ComponentSpec(
                    "reactionrole",
                    action,
                    "reaction_roles.dashboard",
                    rf"^{session}$",
                    captures=(CaptureSpec("session"),),
                    input_kind="modal",
                    deadline_ms=dashboard_deadline,
                    modal=ModalSpec(
                        "Role",
                        (
                            ModalFieldSpec("role", "Role", kind="role_select"),
                            ModalFieldSpec(
                                "description",
                                "Description",
                                required=False,
                                maximum=1000,
                            ),
                            ModalFieldSpec(
                                "emoji",
                                "Button emoji",
                                kind="emoji",
                                minimum=1,
                                maximum=160,
                                placeholder="Emoji or :name:",
                            ),
                        ),
                    ),
                    session_bound=True,
                    acl_key="reactionrole",
                    required_permissions=manager,
                )
                for action in ("button_add_role", "button_edit_role")
            ),
            *(
                button(action)
                for action in (
                    "create_button",
                    "button_add_container",
                    "button_containers_prev",
                    "button_containers_next",
                    "button_add_separator",
                    "button_toggle_clear",
                    "button_container_up",
                    "button_container_down",
                    "button_delete_container",
                    "button_block_up",
                    "button_block_down",
                    "button_delete_block",
                    "button_items_prev",
                    "button_items_next",
                    "button_create",
                    "button_repair",
                    "button_delete_panel",
                    "button_preview",
                    "button_preview_prev",
                    "button_preview_next",
                    "button_panel_back",
                    "button_container_back",
                )
            ),
            ComponentSpec(
                "reactionrole",
                "toggle_role",
                "reaction_roles.button_public",
                r"^(?P<block>[a-f0-9]{32})$",
                captures=(CaptureSpec("block"),),
                public=True,
                ephemeral=True,
                deadline_ms=30_000,
                actor_role_access=True,
                human_guild_member=True,
                source_message_facts=True,
            ),
            ComponentSpec(
                "reactionrole",
                "clear_roles",
                "reaction_roles.button_public",
                r"^(?P<container>[a-f0-9]{32})$",
                captures=(CaptureSpec("container"),),
                public=True,
                ephemeral=True,
                deadline_ms=30_000,
                actor_role_access=True,
                human_guild_member=True,
                source_message_facts=True,
            ),
        )
        passive = (
            PassiveMatcherSpec(
                route="reaction_roles.add",
                handler="reaction_roles.event",
                event="reaction_add",
                expression=MatcherExpr("exists", field="message_id"),
                queue_class="ordinary",
            ),
            PassiveMatcherSpec(
                route="reaction_roles.remove",
                handler="reaction_roles.event",
                event="reaction_remove",
                expression=MatcherExpr("exists", field="message_id"),
                queue_class="ordinary",
            ),
            PassiveMatcherSpec(
                route="reaction_roles.message_deleted",
                handler="reaction_roles.deleted",
                event="message_delete",
                expression=MatcherExpr("exists", field="message_id"),
                queue_class="ordinary",
            ),
            *(
                PassiveMatcherSpec(
                    route=f"reaction_roles.{kind}_deleted",
                    handler="reaction_roles.resource_deleted",
                    event=f"{kind}_delete",
                    expression=MatcherExpr("exists", field="resource_id"),
                    queue_class="ordinary",
                )
                for kind in ("channel", "role", "emoji")
            ),
        )
        return Manifest(
            schema=2,
            module="reaction_roles",
            channel_transition=ChannelTransitionSpec(quiesce_guild=True, rebuild_posts=True),
            group="community",
            generation=self._generation,
            job_stream="lumi:jobs:community",
            display_name="Reaction Roles",
            summary="Reaction role panels.",
            presentation_mode="v2",
            commands=(
                CommandSpec(
                    root="reactionrole",
                    handler="reaction_roles.open",
                    grammar=r"^$",
                    help=HelpSpec(".reactionrole", "Create and manage reaction-role panels."),
                    acl_key="reactionrole",
                    required_permissions=manager,
                ),
            ),
            passive=passive,
            components=tuple(
                msgspec.structs.replace(spec, layout_write=True)
                if spec.session_bound and spec.required_permissions == manager
                else spec
                for spec in components
            ),
            capabilities=(
                "fetch_message",
                "create_message",
                "edit_message",
                "add_reaction",
                "remove_reaction",
                "add_member_role",
                "remove_member_role",
                READ_ACTOR_ROLES,
                WRITE_ACTOR_ROLES,
                WRITE_LAYOUT_MESSAGE,
                "delete_message",
            ),
            version="1.3.0",
        )

    async def start(self) -> None:
        import asyncio

        await self._button_roles.start()
        self._publisher_task = asyncio.create_task(
            self._publisher.run(), name="reaction-index-publisher"
        )

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        actor_id = int(request.event.get("actor_id") or 0)
        guild_id = int(request.event.get("guild_id") or 0)
        try:
            if request.handler == "reaction_roles.button_public":
                message_id = int(request.event.get("message_id") or 0)
                if request.route.endswith("toggle_role"):
                    notice = await self._button_roles.toggle_role(
                        guild_id=guild_id,
                        message_id=message_id,
                        actor_id=actor_id,
                        block_id=str(request.arguments.get("block") or ""),
                        job_id=job_id,
                    )
                else:
                    notice = await self._button_roles.clear_roles(
                        guild_id=guild_id,
                        message_id=message_id,
                        actor_id=actor_id,
                        container_id=str(request.arguments.get("container") or ""),
                        job_id=job_id,
                    )
                return self._result(job_id, self._page("ephemeral", notice))
            if request.handler == "reaction_roles.event":
                return await self._reaction_event(request, job_id, guild_id, actor_id)
            if request.handler == "reaction_roles.deleted":
                mark_missing = getattr(
                    self._store, "mark_reaction_message_missing", None
                )
                panel = await (
                    mark_missing(
                        guild_id, int(request.event.get("message_id") or 0)
                    )
                    if mark_missing is not None
                    else self._store.get_reaction_panel_by_message(
                        guild_id, int(request.event.get("message_id") or 0)
                    )
                )
                if panel and mark_missing is None:
                    await self._store.set_reaction_lifecycle(
                        guild_id, panel.panel_id, "needs_repair", 0
                    )
                    await self._publisher.flush_safely()
                elif panel:
                    await self._publisher.flush_safely()
                get_button_panel = getattr(self._store, "get_button_panel_by_message", None)
                button_panel = (
                    await get_button_panel(
                        guild_id, int(request.event.get("message_id") or 0)
                    )
                    if get_button_panel is not None
                    else None
                )
                if button_panel:
                    await self._store.set_button_panel_lifecycle(
                        guild_id, button_panel.panel_id, "needs_repair", 0
                    )
                return self._empty(job_id)
            if request.handler == "reaction_roles.resource_deleted":
                if guild_id:
                    kind = request.route.removeprefix("reaction_roles.").removesuffix("_deleted")
                    await self._store.mark_reaction_resource_missing(
                        guild_id, kind, int(request.event.get("resource_id") or 0)
                    )
                    mark_button = getattr(self._store, "mark_button_resource_missing", None)
                    if mark_button is not None:
                        await mark_button(
                            guild_id, kind, int(request.event.get("resource_id") or 0)
                        )
                    await self._publisher.flush_safely()
                return self._empty(job_id)
            if not actor_id or not guild_id:
                return self._result(job_id, self._page("send", "Unavailable."))
            if request.handler == "reaction_roles.open":
                session = ReactionRoleSession(secrets.token_hex(8), actor_id, guild_id)
                await self._save(session)
                return self._result(job_id, self._dashboard("send", session))
            return await self._dashboard_action(request, job_id, guild_id, actor_id)
        except Exception:
            LOGGER.exception(
                "reaction_role_request_failed",
                extra={"handler": request.handler, "guild_id": guild_id},
            )
            return self._result(job_id, self._page("ephemeral", "Unavailable."))

    async def _reaction_event(
        self, request: TypedRequest, job_id: str, guild_id: int, actor_id: int
    ) -> ResultEnvelope:
        message_id = int(request.event.get("message_id") or 0)
        emoji = str(request.event.get("emoji") or "")
        mapping = await self._store.mapping_for_emoji(guild_id, message_id, emoji)
        if mapping is None:
            return self._empty(job_id)
        kind = "add_member_role" if request.route.endswith(".add") else "remove_member_role"
        emoji_digest = sha256(emoji.encode("utf-8")).hexdigest()[:16]
        await self._actions.request(
            job_id=job_id,
            kind=kind,
            guild_id=guild_id,
            message_id=message_id,
            member_id=actor_id,
            role_id=mapping.role_id,
            emoji=emoji,
            reaction_revision=int(request.event.get("reaction_revision") or 0),
            reason="Reaction role",
            idempotency_key=(
                f"reaction:{message_id}:{actor_id}:{emoji_digest}:"
                f"{request.event.get('reaction_revision', 0)}"
            ),
        )
        return self._empty(job_id)

    async def _dashboard_action(
        self, request: TypedRequest, job_id: str, guild_id: int, actor_id: int
    ) -> ResultEnvelope:
        token = str(request.arguments.get("session") or "")
        session = await self._load(token)
        if session is None or session.owner_id != actor_id or session.guild_id != guild_id:
            return self._result(job_id, self._page("ephemeral", "Unavailable."))
        action = request.route.partition(".")[2]
        values = tuple(request.arguments.get("values", ()))
        notice: str | None = None
        if action.startswith("button_") or action == "create_button":
            return await self._button_dashboard_action(
                request, job_id, session, action, guild_id, actor_id
            )
        if action == "create":
            session = ReactionRoleSession(token, actor_id, guild_id, screen="new_panel")
        elif action == "channel":
            session = msgspec.structs.replace(session, channel_id=int(values[0]) if values else 0)
        elif action == "preview_channel":
            session = msgspec.structs.replace(
                session, preview_channel_id=int(values[0]) if values else 0
            )
        elif action == "save_panel":
            notice, session = await self._create_panel(session, job_id, actor_id)
        elif action == "attach":
            reference = str(dict(request.arguments.get("values", {})).get("message") or "")
            notice, session = await self._attach(session, job_id, actor_id, reference)
        elif action == "pick_panel":
            panel_id = str(values[0]) if values else ""
            if panel_id.startswith("b:"):
                button_panel = await self._store.get_button_panel(
                    guild_id, panel_id.removeprefix("b:")
                )
                if button_panel:
                    session = msgspec.structs.replace(
                        session,
                        screen="button_panel",
                        panel_kind="button",
                        panel_id=button_panel.panel_id,
                        channel_id=button_panel.channel_id,
                        message_id=0,
                        container_id="",
                        block_id="",
                        container_page=0,
                        block_page=0,
                    )
                else:
                    notice = "Unavailable."
            elif panel := await self._store.get_reaction_panel(guild_id, panel_id):
                session = msgspec.structs.replace(
                    session,
                    screen="panel",
                    panel_id=panel.panel_id,
                    channel_id=panel.channel_id,
                    message_id=panel.message_id or 0,
                    ownership=panel.ownership,
                    mapping_id="",
                    panel_kind="reaction",
                )
            else:
                notice = "Unavailable."
        elif action == "add":
            if len(await self._store.list_mappings(session.panel_id)) >= 100:
                notice = "Panel full."
            else:
                session = msgspec.structs.replace(
                    session,
                    screen="mapping",
                    mapping_id="",
                    emoji="",
                    label="",
                    role_id=0,
                )
        elif action in {"mappings_prev", "mappings_next"}:
            mappings = await self._store.list_mappings(session.panel_id)
            maximum = max(0, (len(mappings) - 1) // 20)
            offset = -1 if action.endswith("_prev") else 1
            session = msgspec.structs.replace(
                session,
                mapping_page=max(0, min(maximum, session.mapping_page + offset)),
            )
        elif action == "pick_mapping":
            mapping_id = str(values[0]) if values else ""
            mapping = next(
                (
                    item
                    for item in await self._store.list_mappings(session.panel_id)
                    if item.mapping_id == mapping_id
                ),
                None,
            )
            if mapping:
                session = msgspec.structs.replace(
                    session,
                    screen="mapping",
                    mapping_id=mapping.mapping_id,
                    emoji=mapping.emoji_key,
                    label=mapping.label or "",
                    role_id=mapping.role_id,
                )
        elif action == "role":
            session = msgspec.structs.replace(session, role_id=int(values[0]) if values else 0)
        elif action == "details":
            modal = dict(request.arguments.get("values", {}))
            session = msgspec.structs.replace(
                session,
                emoji=str(modal.get("emoji") or "").strip(),
                label=str(modal.get("label") or "").strip()[:200],
            )
        elif action == "save_mapping":
            notice, session = await self._save_mapping(session, job_id, actor_id)
        elif action == "remove":
            notice, session = await self._remove_mapping(session, job_id, actor_id)
        elif action in {"up", "down"}:
            await self._store.move_mapping(
                session.panel_id,
                session.mapping_id,
                -1 if action == "up" else 1,
                actor_id,
            )
            await self._refresh_panel(session, job_id)
            session = msgspec.structs.replace(session, screen="panel", mapping_id="")
        elif action == "detach":
            await self._store.set_reaction_lifecycle(
                guild_id, session.panel_id, "detached", actor_id
            )
            await self._publisher.flush_safely()
            session = msgspec.structs.replace(session, screen="home", panel_id="")
            notice = "Detached."
        elif action == "repair":
            panel = await self._store.get_reaction_panel(guild_id, session.panel_id)
            if panel is None or panel.message_id is None:
                notice = "Use Attach to reconnect."
            else:
                notice = (
                    "Repaired."
                    if await self._reconcile_reaction_panel(
                        panel, job_id, actor_id, force=True
                    )
                    else "Needs repair."
                )
            session = msgspec.structs.replace(session, screen="panel")
        elif action == "inspect":
            notice = await self._inspect_panel(session, job_id, actor_id)
        elif action == "preview":
            notice = await self._preview_panel(session, job_id)
        elif action == "convert":


            notice = "Awaiting approval."
        elif action == "panel_back":
            session = msgspec.structs.replace(session, screen="panel", mapping_id="")
        elif action == "back":
            session = msgspec.structs.replace(session, screen="home", panel_id="")
        await self._save(session)
        return self._result(job_id, self._dashboard("edit", session, notice))

    async def _button_dashboard_action(
        self,
        request: TypedRequest,
        job_id: str,
        session: ReactionRoleSession,
        action: str,
        guild_id: int,
        actor_id: int,
    ) -> ResultEnvelope:
        values = tuple(request.arguments.get("values", ()))
        notice: str | None = None
        changed = False
        if action == "create_button":
            panel_id = secrets.token_hex(16)
            await self._store.create_button_panel(panel_id, guild_id, actor_id)
            container = await self._store.add_button_container(
                panel_id, secrets.token_hex(16), actor_id
            )
            session = msgspec.structs.replace(
                session,
                screen="button_container",
                panel_kind="button",
                panel_id=panel_id,
                channel_id=0,
                container_id=container.container_id,
                block_id="",
                container_page=0,
                block_page=0,
            )
        elif action == "button_channel":
            panel = await self._store.get_button_panel(guild_id, session.panel_id)
            if panel is None or panel.lifecycle == "deleted":
                notice = "Unavailable."
            elif not values:
                notice = "Select a channel."
            else:
                channel_id = int(values[0])
                moved = await self._button_roles.move_channel(
                    panel,
                    channel_id,
                    job_id=job_id,
                    actor_id=actor_id,
                )
                if not moved:
                    notice = "Needs repair."
                elif panel.channel_id != channel_id:
                    session = msgspec.structs.replace(
                        session, channel_id=channel_id
                    )
                    changed = panel.lifecycle != "draft"
        elif action == "button_add_container":
            container = await self._store.add_button_container(
                session.panel_id, secrets.token_hex(16), actor_id
            )
            containers = await self._store.list_button_containers(session.panel_id)
            session = msgspec.structs.replace(
                session,
                screen="button_container",
                container_id=container.container_id,
                block_id="",
                container_page=max(0, (len(containers) - 1) // 25),
                block_page=0,
            )
            changed = True
        elif action in {"button_containers_prev", "button_containers_next"}:
            containers = await self._store.list_button_containers(session.panel_id)
            maximum = max(0, (len(containers) - 1) // 25)
            offset = -1 if action.endswith("_prev") else 1
            session = msgspec.structs.replace(
                session,
                container_page=max(0, min(maximum, session.container_page + offset)),
            )
        elif action == "button_pick_container":
            container_id = str(values[0]) if values else ""
            container = await self._store.get_button_container(
                session.panel_id, container_id
            )
            if container is None:
                notice = "Unavailable."
            else:
                session = msgspec.structs.replace(
                    session,
                    screen="button_container",
                    container_id=container.container_id,
                    block_id="",
                    block_page=0,
                )
        elif action == "button_pick_block":
            block_id = str(values[0]) if values else ""
            block = await self._store.get_button_block(block_id)
            if block is None or block.container_id != session.container_id:
                notice = "Unavailable."
            else:
                session = msgspec.structs.replace(
                    session, screen="button_block", block_id=block_id
                )
        elif action in {
            "button_add_heading",
            "button_edit_heading",
            "button_add_text",
            "button_edit_text",
            "button_add_image_url",
            "button_edit_image_url",
            "button_upload_image",
            "button_edit_upload",
            "button_add_role",
            "button_edit_role",
        }:
            notice, changed = await self._save_button_block_from_modal(
                request, session, action, actor_id
            )
            if changed and (
                action.startswith("button_add_") or action == "button_upload_image"
            ):
                blocks = await self._store.list_button_blocks(session.container_id)
                session = msgspec.structs.replace(
                    session,
                    screen="button_container",
                    block_page=max(0, (len(blocks) - 1) // 25),
                )
        elif action == "button_add_separator":
            block = ButtonRoleBlock(
                secrets.token_hex(16), session.container_id, "separator", 0
            )
            try:
                await self._store.save_button_block(session.panel_id, block, actor_id)
                notice, changed = "Saved.", True
                blocks = await self._store.list_button_blocks(session.container_id)
                session = msgspec.structs.replace(
                    session, block_page=max(0, (len(blocks) - 1) // 25)
                )
            except ValueError as exc:
                notice = str(exc)
        elif action == "button_toggle_clear":
            container = await self._store.get_button_container(
                session.panel_id, session.container_id
            )
            blocks = await self._store.list_button_blocks(session.container_id)
            if container is None:
                notice = "Unavailable."
            elif not container.clear_enabled and not any(
                item.kind == "role" for item in blocks
            ):
                notice = "Add a role first."
            else:
                changed = await self._store.toggle_button_container_clear(
                    session.panel_id, session.container_id, actor_id
                )
                notice = "Saved." if changed else "Unavailable."
        elif action in {"button_container_up", "button_container_down"}:
            changed = await self._store.move_button_container(
                session.panel_id,
                session.container_id,
                -1 if action.endswith("_up") else 1,
                actor_id,
            )
        elif action == "button_delete_container":
            containers = await self._store.list_button_containers(session.panel_id)
            if len(containers) <= 1:
                notice = "Keep one container."
            else:
                changed = await self._store.delete_button_container(
                    session.panel_id, session.container_id, actor_id
                )
                session = msgspec.structs.replace(
                    session, screen="button_panel", container_id="", block_id=""
                )
        elif action in {"button_block_up", "button_block_down"}:
            changed = await self._store.move_button_block(
                session.panel_id,
                session.container_id,
                session.block_id,
                -1 if action.endswith("_up") else 1,
                actor_id,
            )
        elif action in {"button_items_prev", "button_items_next"}:
            blocks = await self._store.list_button_blocks(session.container_id)
            maximum = max(0, (len(blocks) - 1) // 25)
            offset = -1 if action.endswith("_prev") else 1
            session = msgspec.structs.replace(
                session,
                block_page=max(0, min(maximum, session.block_page + offset)),
            )
        elif action == "button_delete_block":
            blocks = await self._store.list_button_blocks(session.container_id)
            panel = await self._store.get_button_panel(guild_id, session.panel_id)
            if len(blocks) <= 1 and panel is not None and panel.lifecycle != "draft":
                notice = "Keep one item."
            else:
                selected = next(
                    (item for item in blocks if item.block_id == session.block_id),
                    None,
                )
                container = await self._store.get_button_container(
                    session.panel_id, session.container_id
                )
                if (
                    selected is not None
                    and selected.kind == "role"
                    and container is not None
                    and container.clear_enabled
                    and not any(
                        item.kind == "role" and item.block_id != selected.block_id
                        for item in blocks
                    )
                ):
                    await self._store.toggle_button_container_clear(
                        session.panel_id, session.container_id, actor_id
                    )
                changed = await self._store.delete_button_block(
                    session.panel_id,
                    session.container_id,
                    session.block_id,
                    actor_id,
                )
                session = msgspec.structs.replace(
                    session,
                    screen="button_container",
                    block_id="",
                    block_page=max(0, (len(blocks) - 2) // 25),
                )
        elif action in {"button_create", "button_repair"}:
            panel = await self._store.get_button_panel(guild_id, session.panel_id)
            if panel is None:
                notice = "Unavailable."
            else:
                try:
                    created = panel.lifecycle == "draft"
                    synced = await self._button_roles.reconcile(
                        panel, job_id=job_id, actor_id=actor_id
                    )
                    notice = (
                        "Created." if synced and created else "Saved."
                    ) if synced else "Needs repair."
                except ButtonPanelError as exc:
                    if panel.lifecycle != "draft":
                        await self._store.set_button_panel_lifecycle(
                            panel.guild_id,
                            panel.panel_id,
                            "needs_repair",
                            actor_id,
                        )
                    notice = str(exc)
        elif action in {
            "button_preview",
            "button_preview_prev",
            "button_preview_next",
        }:
            panel = await self._store.get_button_panel(guild_id, session.panel_id)
            if panel is None:
                intent = self._page("ephemeral", "Unavailable.")
            else:
                try:
                    if action == "button_preview":
                        page_number = 0
                    else:
                        page_number = session.preview_page + (
                            -1 if action.endswith("_prev") else 1
                        )
                    page, attachments, total = await self._button_roles.preview(
                        panel, session.container_id, page_number
                    )
                    page_number = max(0, min(page_number, total - 1))
                    session = msgspec.structs.replace(
                        session, preview_page=page_number
                    )
                    controls = list(page.blocks)
                    if total > 1:
                        controls.append(
                            PresentationBlock(
                                kind="action_row",
                                controls=(
                                    self._button(
                                        session,
                                        "button_preview_prev",
                                        "Previous",
                                        disabled=page_number == 0,
                                    ),
                                    self._button(
                                        session,
                                        "button_preview_next",
                                        "Next",
                                        disabled=page_number == total - 1,
                                    ),
                                ),
                            )
                        )
                    intent = DiscordIntent(
                        kind="ephemeral" if action == "button_preview" else "edit",
                        session_id=session.token,
                        pages=(PresentationPage(blocks=tuple(controls)),),
                        attachments=attachments,
                    )
                except ButtonPanelError as exc:
                    intent = self._page("ephemeral", str(exc))
            await self._save(session)
            return self._result(job_id, intent)
        elif action == "button_delete_panel":
            notice = await self._delete_button_panel(session, job_id, actor_id)
            session = msgspec.structs.replace(
                session,
                screen="home",
                panel_kind="reaction",
                panel_id="",
                container_id="",
                block_id="",
                container_page=0,
                block_page=0,
            )
        elif action == "button_panel_back":
            session = msgspec.structs.replace(
                session, screen="button_panel", container_id="", block_id=""
            )
        elif action == "button_container_back":
            session = msgspec.structs.replace(
                session, screen="button_container", block_id=""
            )
        else:
            notice = "Unavailable."
        if changed:
            synced = await self._sync_button_panel(session, job_id, actor_id)
            if synced is not None:
                notice = synced
        await self._save(session)
        return self._result(job_id, self._dashboard("edit", session, notice))

    async def _save_button_block_from_modal(
        self,
        request: TypedRequest,
        session: ReactionRoleSession,
        action: str,
        actor_id: int,
    ) -> tuple[str, bool]:
        modal = dict(request.arguments.get("values", {}))
        edit = "_edit_" in action
        block_id = session.block_id if edit else secrets.token_hex(16)
        existing = await self._store.get_button_block(block_id) if edit else None
        if edit and (existing is None or existing.container_id != session.container_id):
            return "Unavailable.", False
        if "heading" in action:
            text = str(modal.get("text") or "").strip()
            level = int(one_selected(modal, "level") or 0)
            if not text or level not in {1, 2, 3}:
                return "Incomplete.", False
            block = ButtonRoleBlock(
                block_id,
                session.container_id,
                "heading",
                existing.position if existing else 0,
                text=text[:256],
                heading_level=level,
            )
        elif action.endswith("_text"):
            text = str(modal.get("text") or "").strip()
            if not text:
                return "Incomplete.", False
            block = ButtonRoleBlock(
                block_id,
                session.container_id,
                "text",
                existing.position if existing else 0,
                text=text[:4000],
            )
        elif "image_url" in action or "upload" in action:
            try:
                if "image_url" in action:
                    asset = await self._button_roles.ingest_url(
                        session.guild_id, str(modal.get("url") or "").strip()
                    )
                else:
                    metadata = first_file(request.arguments.get("files"), "image")
                    if metadata is None:
                        return "Select an image.", False
                    asset = await self._button_roles.ingest_upload(
                        session.guild_id, metadata
                    )
            except ButtonPanelError as exc:
                return str(exc), False
            block = ButtonRoleBlock(
                block_id,
                session.container_id,
                "image",
                existing.position if existing else 0,
                asset_id=asset.asset_id,
            )
        else:
            role_id = int(one_selected(modal, "role") or 0)
            emoji = self._normalize_button_emoji(str(modal.get("emoji") or ""))
            description = str(modal.get("description") or "").strip()[:1000]
            if not role_id or not emoji:
                return "Incomplete.", False
            block = ButtonRoleBlock(
                block_id,
                session.container_id,
                "role",
                existing.position if existing else 0,
                role_id=role_id,
                description=description or None,
                emoji_key=emoji,
            )
        try:
            await self._store.save_button_block(session.panel_id, block, actor_id)
        except ValueError as exc:
            return str(exc), False
        return "Saved.", True

    async def _sync_button_panel(
        self, session: ReactionRoleSession, job_id: str, actor_id: int
    ) -> str | None:
        panel = await self._store.get_button_panel(session.guild_id, session.panel_id)
        if panel is None or panel.lifecycle == "draft":
            return None
        try:
            return (
                "Saved."
                if await self._button_roles.reconcile(
                    panel, job_id=job_id, actor_id=actor_id
                )
                else "Needs repair."
            )
        except ButtonPanelError as exc:
            if panel.lifecycle != "draft":
                await self._store.set_button_panel_lifecycle(
                    panel.guild_id,
                    panel.panel_id,
                    "needs_repair",
                    actor_id,
                )
            return str(exc)

    async def _delete_button_panel(
        self, session: ReactionRoleSession, job_id: str, actor_id: int
    ) -> str:
        panel = await self._store.get_button_panel(session.guild_id, session.panel_id)
        if panel is None:
            return "Unavailable."
        deleted = await self._button_roles.delete_panel(panel, job_id=job_id, actor_id=actor_id)
        return "Deleted." if deleted else "Needs repair."

    @staticmethod
    def _normalize_button_emoji(value: str) -> str:
        value = value.strip()
        if CUSTOM_EMOJI.fullmatch(value) or emoji_lib.is_emoji(value):
            return value
        return ""

    async def _create_panel(
        self, session: ReactionRoleSession, job_id: str, actor_id: int
    ) -> tuple[str, ReactionRoleSession]:
        if not session.channel_id:
            return "Incomplete.", session
        panel_id = session.panel_id or secrets.token_hex(16)
        panel = ReactionPanel(
            panel_id,
            session.guild_id,
            session.channel_id,
            None,
            "pending",
            "lumi",
            "new",
            1,
        )
        await self._store.stage_reaction_panel(panel, actor_id)
        result = await self._actions.request(
            job_id=job_id,
            kind="create_message",
            guild_id=session.guild_id,
            channel_id=session.channel_id,
            page=self._public_panel(()),
            persistent=True,
            idempotency_key=f"reaction-panel-create:{panel_id}:1",
        )
        if not result.ok:
            await self._store.set_reaction_lifecycle(
                session.guild_id, panel_id, "needs_repair", actor_id
            )
            return "Needs repair.", msgspec.structs.replace(session, panel_id=panel_id)
        await self._store.activate_reaction_panel(
            session.guild_id,
            panel_id,
            int(result.data["channel_id"]),
            int(result.data["message_id"]),
            "lumi",
            actor_id,
        )
        active = await self._store.get_reaction_panel(session.guild_id, panel_id)
        if active is None or not await self._reconcile_reaction_panel(
            active, job_id, actor_id
        ):
            return "Needs repair.", msgspec.structs.replace(
                session, panel_id=panel_id, message_id=int(result.data["message_id"])
            )
        return "Created.", msgspec.structs.replace(
            session,
            screen="panel",
            panel_id=panel_id,
            message_id=int(result.data["message_id"]),
            ownership="lumi",
        )

    async def _attach(
        self,
        session: ReactionRoleSession,
        job_id: str,
        actor_id: int,
        reference: str,
    ) -> tuple[str, ReactionRoleSession]:
        match = MESSAGE_LINK.fullmatch(reference.strip().replace(",", " "))
        if match is None or int(match.group("guild")) != session.guild_id:
            return "Invalid message.", session
        channel_id = int(match.group("channel"))
        message_id = int(match.group("message"))
        result = await self._actions.request(
            job_id=job_id,
            kind="fetch_message",
            guild_id=session.guild_id,
            channel_id=channel_id,
            message_id=message_id,
            idempotency_key=f"reaction-panel-fetch:{message_id}:{job_id}",
            prove_message_author=True,
        )
        if not result.ok:
            return "Unavailable.", session
        ownership = (
            "lumi"
            if result.data.get("current_lumi") and not result.data.get("webhook")
            else "external"
        )
        panel_id = session.panel_id or secrets.token_hex(16)
        panel = ReactionPanel(
            panel_id,
            session.guild_id,
            channel_id,
            message_id,
            "active",
            ownership,
            "new",
            1,
        )
        await self._store.stage_reaction_panel(panel, actor_id)
        await self._store.activate_reaction_panel(
            session.guild_id,
            panel_id,
            channel_id,
            message_id,
            ownership,
            actor_id,
        )
        active = await self._store.get_reaction_panel(session.guild_id, panel_id)
        if active is None or not await self._reconcile_reaction_panel(
            active, job_id, actor_id, observed_source=(channel_id, message_id, result.data.get("author_proof")),
        ):
            return "Needs repair.", msgspec.structs.replace(
                session,
                panel_id=panel_id,
                channel_id=channel_id,
                message_id=message_id,
                ownership=ownership,
            )
        return "Attached.", msgspec.structs.replace(
            session,
            screen="panel",
            panel_id=panel_id,
            channel_id=channel_id,
            message_id=message_id,
            ownership=ownership,
        )

    async def _inspect_panel(self, session: ReactionRoleSession, job_id: str, actor_id: int) -> str:
        panel = await self._store.get_reaction_panel(session.guild_id, session.panel_id)
        if panel is None or panel.message_id is None:
            return "Unavailable."
        mappings = await self._store.list_mappings(panel.panel_id)
        result = await self._actions.request(
            job_id=job_id,
            kind="fetch_message",
            guild_id=session.guild_id,
            channel_id=panel.channel_id,
            message_id=panel.message_id,
            role_ids=tuple(mapping.role_id for mapping in mappings),
            emoji_keys=tuple(mapping.emoji_key for mapping in mappings),
            idempotency_key=f"reaction-inspect:{panel.panel_id}:{job_id}",
        )
        classification = "needs_repair"
        if result.ok and mappings:
            classification = self._classify_preflight(result.data, mappings)
        await self._store.record_preflight(
            "reaction_roles", panel.panel_id, classification, result.data
        )
        if classification == "needs_repair":
            await self._store.set_reaction_lifecycle(
                session.guild_id, panel.panel_id, "needs_repair", actor_id
            )
            await self._publisher.flush_safely()
            return "Needs repair."
        await self._store.activate_reaction_panel(
            session.guild_id,
            panel.panel_id,
            panel.channel_id,
            panel.message_id,
            "lumi" if classification == "editable" else "external",
            actor_id,
        )
        await self._publisher.flush_safely()
        return "Editable." if classification == "editable" else "External."

    @staticmethod
    def _classify_preflight(
        snapshot: dict[str, object], mappings: tuple[ReactionMapping, ...]
    ) -> str:
        roles = dict(snapshot.get("roles", {}))
        emojis = dict(snapshot.get("emojis", {}))
        permissions = dict(snapshot.get("permissions", {}))
        resources = all(
            bool(dict(roles.get(str(mapping.role_id), {})).get("manageable"))
            and bool(emojis.get(mapping.emoji_key))
            for mapping in mappings
        )
        if (
            not resources
            or int(snapshot.get("message_type", -1)) not in {0, 19}
            or not permissions.get("add_reactions")
        ):
            return "needs_repair"
        if (
            snapshot.get("current_lumi")
            and not snapshot.get("webhook")
            and not snapshot.get("attachments")
        ):
            return "editable"
        if snapshot.get("current_lumi") and not snapshot.get("webhook"):
            return "needs_repair"
        return "external"

    async def _preview_panel(self, session: ReactionRoleSession, job_id: str) -> str:
        panel = await self._store.get_reaction_panel(session.guild_id, session.panel_id)
        classification = await self._store.migration_classification(
            "reaction_roles", session.panel_id
        )
        if (
            panel is None
            or classification not in {"editable", "external"}
            or not session.preview_channel_id
        ):
            return "Inspect and select a preview channel."
        mappings = await self._store.list_mappings(panel.panel_id)
        preview_message = 0
        preview_channel = session.preview_channel_id
        for position, page in enumerate(self._reaction_public_pages(mappings)):
            result = await self._actions.request(
                job_id=job_id,
                kind="create_message",
                guild_id=session.guild_id,
                channel_id=session.preview_channel_id,
                page=page,
                persistent=True,
                idempotency_key=(
                    f"reaction-preview:{panel.panel_id}:{position}:{job_id}"
                ),
            )
            if not result.ok:
                return "Unavailable."
            part_message = int(result.data["message_id"])
            preview_message = preview_message or part_message
            preview_channel = int(result.data["channel_id"])
            for mapping in mappings[position * 20 : (position + 1) * 20]:
                reaction = await self._actions.request(
                    job_id=job_id,
                    kind="add_reaction",
                    guild_id=session.guild_id,
                    channel_id=preview_channel,
                    message_id=part_message,
                    emoji=mapping.emoji_key,
                    idempotency_key=(
                        f"reaction-preview-emoji:{panel.panel_id}:"
                        f"{mapping.mapping_id}:{job_id}"
                    ),
                )
                if not reaction.ok:
                    return "Preview needs repair."
        await self._store.record_preview(
            "reaction_roles", panel.panel_id, preview_channel, preview_message
        )
        return "Preview sent."

    async def _convert_panel(self, session: ReactionRoleSession, job_id: str, actor_id: int) -> str:
        panel = await self._store.get_reaction_panel(session.guild_id, session.panel_id)
        classification = await self._store.migration_classification(
            "reaction_roles", session.panel_id
        )
        if (
            panel is None
            or panel.message_id is None
            or classification not in {"editable", "external"}
        ):
            return "Unavailable."
        mappings = await self._store.list_mappings(panel.panel_id)
        observed = {"channel_id": panel.channel_id, "message_id": panel.message_id}
        if classification == "editable":
            edited = await self._actions.request(
                job_id=job_id,
                kind="edit_message",
                guild_id=session.guild_id,
                channel_id=panel.channel_id,
                message_id=panel.message_id,
                page=self._public_panel(mappings),
                persistent=True,
                idempotency_key=f"reaction-convert:{panel.panel_id}:{panel.route_revision}",
            )
            if (
                not edited.ok
                or (edited.data.get("channel_id"), edited.data.get("message_id"))
                != (panel.channel_id, panel.message_id)
                or not edited.data.get("components_v2")
            ):
                return "Needs repair."
            observed.update(edited.data)
        acknowledgements = []
        for mapping in mappings:
            added = await self._actions.request(
                job_id=job_id,
                kind="add_reaction",
                guild_id=session.guild_id,
                channel_id=panel.channel_id,
                message_id=panel.message_id,
                emoji=mapping.emoji_key,
                idempotency_key=(
                    f"reaction-convert-emoji:{panel.panel_id}:"
                    f"{mapping.mapping_id}:{panel.route_revision}"
                ),
            )
            if not added.ok:
                return "Needs repair."
            acknowledgements.append({"emoji": mapping.emoji_key, "me": True})
        await self._store.activate_reaction_panel(
            session.guild_id,
            panel.panel_id,
            panel.channel_id,
            panel.message_id,
            "lumi" if classification == "editable" else "external",
            actor_id,
        )
        await self._store.mark_converted(
            "reaction_roles", panel.panel_id, {**observed, "reactions": acknowledgements},
        )
        await self._publisher.flush_safely()
        return "Converted."

    async def _reconcile_reaction_panel(
        self,
        panel: ReactionPanel,
        job_id: str,
        actor_id: int,
        *,
        force: bool = False,
        observed_source: tuple[int, int, str | None] | None = None,
    ) -> bool:

        for _attempt in range(3):
            current = await self._store.get_reaction_panel(
                panel.guild_id, panel.panel_id
            )
            if current is None or current.lifecycle in {"deleted", "detached"}:
                return False
            mappings = await self._store.list_mappings(current.panel_id)
            pages = self._reaction_public_pages(mappings)
            parts = {
                part.position: part
                for part in await self._store.list_reaction_parts(current.panel_id)
            }
            old_routes = await self._store.list_applied_reaction_routes(
                current.panel_id
            )
            applied_pairs = {
                (route.message_id, self._emoji_identity(route.emoji_key))
                for route in old_routes
            }
            desired_parts: list[tuple[int, str, str]] = []
            failed = False
            for position, page in enumerate(pages):
                page_hash = sha256(page.description.encode("utf-8")).hexdigest()
                part = parts.get(position)
                if (
                    part is None
                    and position == 0
                    and current.message_id is not None
                ):
                    message_id = current.message_id
                    ownership = current.ownership
                    repair_state = "ready"
                    content_hash = ""
                elif part is not None:
                    message_id = part.message_id
                    ownership = part.ownership
                    repair_state = part.repair_state
                    content_hash = part.content_hash
                else:
                    message_id = 0
                    ownership = "lumi"
                    repair_state = "missing"
                    content_hash = ""
                if not message_id or repair_state == "missing":
                    if position == 0 and ownership != "lumi":
                        failed = True
                        break
                    created = await self._actions.request(
                        job_id=job_id,
                        kind="create_message",
                        guild_id=current.guild_id,
                        channel_id=current.channel_id,
                        page=page,
                        persistent=True,
                        idempotency_key=(
                            f"reaction-part-create:{current.panel_id}:"
                            f"{current.route_revision}:{position}:{job_id}"
                        ),
                    )
                    if not created.ok:
                        failed = True
                        break
                    message_id = int(created.data["message_id"])
                    ownership = "lumi"
                    await self._store.save_reaction_part(
                        current.panel_id,
                        position,
                        message_id,
                        ownership,
                        current.route_revision,
                        page_hash,
                    )
                elif ownership == "lumi" and (force or content_hash != page_hash):
                    edited = await self._actions.request(
                        job_id=job_id,
                        kind="edit_message",
                        guild_id=current.guild_id,
                        channel_id=current.channel_id,
                        message_id=message_id,
                        page=page,
                        persistent=True,
                        read_proof=(
                            observed_source[2]
                            if observed_source and observed_source[:2] == (current.channel_id, message_id)
                            else None
                        ),
                        idempotency_key=(
                            f"reaction-part-edit:{current.panel_id}:"
                            f"{position}:{page_hash}"
                        ),
                    )
                    if not edited.ok:
                        failed = True
                        break
                desired_parts.append((message_id, ownership, page_hash))
                chunk = mappings[position * 20 : (position + 1) * 20]
                for mapping in chunk:
                    pair = (message_id, self._emoji_identity(mapping.emoji_key))
                    if not force and pair in applied_pairs:
                        continue
                    added = await self._actions.request(
                        job_id=job_id,
                        kind="add_reaction",
                        guild_id=current.guild_id,
                        channel_id=current.channel_id,
                        message_id=message_id,
                        emoji=mapping.emoji_key,
                        idempotency_key=(
                            f"reaction-part-add:{current.panel_id}:"
                            f"{current.route_revision}:{mapping.mapping_id}:{message_id}"
                        ),
                    )
                    if not added.ok:
                        failed = True
                        break
                if failed:
                    break
            if failed:
                await self._store.set_reaction_lifecycle(
                    current.guild_id, current.panel_id, "needs_repair", actor_id
                )
                return False
            applied = await self._store.apply_reaction_document(
                current.guild_id,
                current.panel_id,
                tuple(desired_parts),
                current.route_revision,
                actor_id,
            )
            if not applied:
                continue
            desired_pairs = {
                (
                    desired_parts[index // 20][0],
                    self._emoji_identity(mapping.emoji_key),
                )
                for index, mapping in enumerate(mappings)
            }
            for route in old_routes:
                pair = (route.message_id, self._emoji_identity(route.emoji_key))
                if pair in desired_pairs:
                    continue
                await self._actions.request(
                    job_id=job_id,
                    kind="remove_reaction",
                    guild_id=current.guild_id,
                    channel_id=current.channel_id,
                    message_id=route.message_id,
                    emoji=route.emoji_key,
                    idempotency_key=(
                        f"reaction-part-remove:{current.panel_id}:"
                        f"{current.route_revision}:{route.mapping_id}:{route.message_id}"
                    ),
                )
            for channel_id, message_id in await self._store.pending_reaction_cleanup(
                current.panel_id
            ):
                deleted = await self._actions.request(
                    job_id=job_id,
                    kind="delete_message",
                    guild_id=current.guild_id,
                    channel_id=channel_id,
                    message_id=message_id,
                    idempotency_key=(
                        f"reaction-part-cleanup:{current.panel_id}:{message_id}"
                    ),
                )
                await self._store.finish_reaction_cleanup(
                    current.panel_id,
                    message_id,
                    None if deleted.ok or deleted.error_code == "channel_unavailable" else (
                        deleted.error_code or "delete_failed"
                    ),
                )
            await self._publisher.flush_safely()
            return True
        await self._store.set_reaction_lifecycle(
            panel.guild_id, panel.panel_id, "needs_repair", actor_id
        )
        return False

    async def _save_mapping(
        self, session: ReactionRoleSession, job_id: str, actor_id: int
    ) -> tuple[str, ReactionRoleSession]:
        emoji = self._normalize_emoji(session.emoji)
        if not emoji or not session.role_id or not session.panel_id:
            return "Incomplete.", session
        existing = await self._store.list_mappings(session.panel_id)
        panel = await self._store.get_reaction_panel(session.guild_id, session.panel_id)
        if panel is None:
            return "Unavailable.", session
        selected = next((item for item in existing if item.mapping_id == session.mapping_id), None)
        duplicate = next(
            (
                item
                for item in existing
                if self._emoji_identity(item.emoji_key) == self._emoji_identity(emoji)
            ),
            None,
        )
        if selected and self._emoji_identity(selected.emoji_key) != self._emoji_identity(emoji):
            await self._store.disable_mapping(session.panel_id, selected.mapping_id, actor_id)
        target = duplicate or selected
        mapping_id = target.mapping_id if target else secrets.token_hex(16)
        position = target.position if target else len(existing)
        try:
            await self._store.upsert_mapping(
                ReactionMapping(
                    mapping_id,
                    session.panel_id,
                    emoji,
                    session.role_id,
                    session.label or None,
                    position,
                    True,
                ),
                actor_id,
            )
        except ValueError as exc:
            return str(exc), session
        panel = await self._store.get_reaction_panel(
            session.guild_id, session.panel_id
        )
        if panel is not None and panel.source == "legacy" and panel.ownership == "unknown":
            return "Saved.", msgspec.structs.replace(
                session, screen="panel", mapping_id=""
            )
        synced = bool(
            panel
            and await self._reconcile_reaction_panel(panel, job_id, actor_id)
        )
        count = len(await self._store.list_mappings(session.panel_id))
        return (
            "Saved." if synced else "Needs repair.",
            msgspec.structs.replace(
                session,
                screen="panel",
                mapping_id="",
                mapping_page=max(0, (count - 1) // 20),
            ),
        )

    async def _remove_mapping(
        self, session: ReactionRoleSession, job_id: str, actor_id: int
    ) -> tuple[str, ReactionRoleSession]:
        mapping = await self._store.disable_mapping(session.panel_id, session.mapping_id, actor_id)
        if mapping is None:
            return "Unavailable.", session
        panel = await self._store.get_reaction_panel(
            session.guild_id, session.panel_id
        )
        if panel is not None and panel.source == "legacy" and panel.ownership == "unknown":
            return "Removed.", msgspec.structs.replace(
                session, screen="panel", mapping_id=""
            )
        synced = bool(
            panel
            and await self._reconcile_reaction_panel(panel, job_id, actor_id)
        )
        count = len(await self._store.list_mappings(session.panel_id))
        return (
            "Removed." if synced else "Needs repair.",
            msgspec.structs.replace(
                session,
                screen="panel",
                mapping_id="",
                mapping_page=min(session.mapping_page, max(0, (count - 1) // 20)),
            ),
        )

    async def _refresh_panel(self, session: ReactionRoleSession, job_id: str) -> bool:
        panel = await self._store.get_reaction_panel(session.guild_id, session.panel_id)
        if panel is None or not panel.message_id:
            return False
        return await self._reconcile_reaction_panel(
            panel, job_id, session.owner_id
        )

    def _dashboard(
        self, kind: str, session: ReactionRoleSession, notice: str | None = None
    ) -> DiscordIntent:
        if session.screen == "home":
            page = self._home(session, notice)
        elif session.screen == "new_panel":
            page = self._new_panel(session, notice)
        elif session.screen == "mapping":
            page = self._mapping_page(session, notice)
        elif session.screen == "button_panel":
            page = self._button_panel_page(session, notice)
        elif session.screen == "button_container":
            page = self._button_container_page(session, notice)
        elif session.screen == "button_block":
            page = self._button_block_page(session, notice)
        else:
            page = self._panel_page(session, notice)
        return DiscordIntent(kind=kind, session_id=session.token, pages=(page,))

    def _home(self, session: ReactionRoleSession, notice: str | None) -> PresentationPage:
        text = "## Reaction Roles" + (f"\n{notice}" if notice else "")
        panels = self._last_panels.get(session.guild_id, ())
        button_panels = self._last_button_panels.get(session.guild_id, ())
        controls: list[PresentationControl] = []
        options = tuple(
            [
                PresentationOption(
                    f"Reaction - #{panel.channel_id}",
                    panel.panel_id,
                    "Needs repair" if panel.lifecycle == "needs_repair" else None,
                )
                for panel in panels
            ]
            + [
                PresentationOption(
                    (
                        f"Button - #{panel.channel_id}"
                        if panel.channel_id
                        else "Button - Draft"
                    ),
                    f"b:{panel.panel_id}",
                    "Needs repair" if panel.lifecycle == "needs_repair" else None,
                )
                for panel in button_panels
            ]
        )[:25]
        if options:
            controls.append(
                PresentationControl(
                    "string_select",
                    "reactionrole",
                    "pick_panel",
                    session.token,
                    placeholder="Panel",
                    options=options,
                )
            )
        controls.extend(
            (
                self._button(session, "create", "Reaction panel", "success"),
                self._button(session, "create_button", "Button panel", "success"),
                self._button(session, "attach", "Attach"),
            )
        )
        return PresentationPage(description=text, controls=tuple(controls))

    def _button_panel_page(
        self, session: ReactionRoleSession, notice: str | None
    ) -> PresentationPage:
        panel = next(
            (
                item
                for item in self._last_button_panels.get(session.guild_id, ())
                if item.panel_id == session.panel_id
            ),
            None,
        )
        containers = self._last_button_containers.get(session.panel_id, ())
        if panel is None:
            return PresentationPage(description="Unavailable.")
        channel = f"<#{panel.channel_id}>" if panel.channel_id else "No channel"
        text = f"## Button Panel\n{channel} - {len(containers)} containers"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = [
            PresentationControl(
                "channel_select",
                "reactionrole",
                "button_channel",
                session.token,
                placeholder="Channel",
                default_ids=(panel.channel_id,) if panel.channel_id else (),
                channel_types=TEXT_CHANNEL_TYPES,
            )
        ]
        if containers:
            maximum_page = max(0, (len(containers) - 1) // 25)
            page_number = min(session.container_page, maximum_page)
            start = page_number * 25
            controls.append(
                PresentationControl(
                    "string_select",
                    "reactionrole",
                    "button_pick_container",
                    session.token,
                    placeholder="Container",
                    options=tuple(
                        PresentationOption(
                            f"Container {index}", container.container_id
                        )
                        for index, container in enumerate(
                            containers[start : start + 25], start + 1
                        )
                    ),
                )
            )
            if maximum_page:
                controls.extend(
                    (
                        self._button(
                            session,
                            "button_containers_prev",
                            "Previous",
                            disabled=page_number == 0,
                        ),
                        self._button(
                            session,
                            "button_containers_next",
                            "Next",
                            disabled=page_number == maximum_page,
                        ),
                    )
                )
        controls.extend(
            (
                self._button(session, "button_add_container", "Add container"),
                self._button(
                    session,
                    "button_create" if panel.lifecycle == "draft" else "button_repair",
                    "Create" if panel.lifecycle == "draft" else "Repair",
                    "success",
                    disabled=not panel.channel_id,
                ),
                self._button(session, "button_delete_panel", "Delete", "danger"),
                self._button(session, "back", "Back"),
            )
        )
        return PresentationPage(description=text, controls=tuple(controls))

    def _button_container_page(
        self, session: ReactionRoleSession, notice: str | None
    ) -> PresentationPage:
        containers = self._last_button_containers.get(session.panel_id, ())
        container = next(
            (item for item in containers if item.container_id == session.container_id),
            None,
        )
        blocks = self._last_button_blocks.get(session.container_id, ())
        if container is None:
            return PresentationPage(description="Unavailable.")
        number = next(
            (
                index
                for index, item in enumerate(containers, 1)
                if item.container_id == container.container_id
            ),
            1,
        )
        total_blocks = sum(
            len(self._last_button_blocks.get(item.container_id, ()))
            for item in containers
        )
        panel_full = total_blocks >= 100
        text = f"## Container {number}\n{len(blocks)} items"
        if container.clear_enabled:
            text += " - Clear enabled"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = []
        if blocks:
            maximum_page = max(0, (len(blocks) - 1) // 25)
            page_number = min(session.block_page, maximum_page)
            start = page_number * 25
            controls.append(
                PresentationControl(
                    "string_select",
                    "reactionrole",
                    "button_pick_block",
                    session.token,
                    placeholder="Item",
                    options=tuple(
                        PresentationOption(
                            self._button_block_label(block), block.block_id
                        )
                        for block in blocks[start : start + 25]
                    ),
                )
            )
            if maximum_page:
                controls.extend(
                    (
                        self._button(
                            session,
                            "button_items_prev",
                            "Previous",
                            disabled=page_number == 0,
                        ),
                        self._button(
                            session,
                            "button_items_next",
                            "Next",
                            disabled=page_number == maximum_page,
                        ),
                    )
                )
        controls.extend(
            (
                self._button(
                    session, "button_add_heading", "Heading", disabled=panel_full
                ),
                self._button(session, "button_add_text", "Text", disabled=panel_full),
                self._button(
                    session, "button_add_image_url", "Image URL", disabled=panel_full
                ),
                self._button(
                    session, "button_upload_image", "Upload image", disabled=panel_full
                ),
                self._button(
                    session, "button_add_separator", "Separator", disabled=panel_full
                ),
                self._button(session, "button_add_role", "Role", disabled=panel_full),
                self._button(
                    session,
                    "button_toggle_clear",
                    "Remove Clear" if container.clear_enabled else "Add Clear",
                ),
                self._button(session, "button_container_up", "Up"),
                self._button(session, "button_container_down", "Down"),
                self._button(
                    session,
                    "button_preview",
                    "Preview",
                    disabled=not blocks,
                ),
                self._button(
                    session,
                    "button_delete_container",
                    "Delete",
                    "danger",
                    disabled=len(containers) <= 1,
                ),
                self._button(session, "button_panel_back", "Back"),
            )
        )
        return PresentationPage(description=text, controls=tuple(controls))

    def _button_block_page(
        self, session: ReactionRoleSession, notice: str | None
    ) -> PresentationPage:
        blocks = self._last_button_blocks.get(session.container_id, ())
        block = next(
            (item for item in blocks if item.block_id == session.block_id),
            None,
        )
        if block is None:
            return PresentationPage(description="Unavailable.")
        text = f"## {self._button_block_label(block)}"
        if block.kind == "role" and block.role_id:
            text += f"\n<@&{block.role_id}>"
            if block.description:
                text += f" - {block.description}"
            text += f"\n{block.emoji_key}"
        elif block.text:
            text += f"\n{block.text}"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = []
        if block.kind == "heading":
            controls.append(self._button(session, "button_edit_heading", "Edit"))
        elif block.kind == "text":
            controls.append(self._button(session, "button_edit_text", "Edit"))
        elif block.kind == "image":
            controls.extend(
                (
                    self._button(session, "button_edit_image_url", "Image URL"),
                    self._button(session, "button_edit_upload", "Upload image"),
                )
            )
        elif block.kind == "role":
            controls.append(self._button(session, "button_edit_role", "Edit"))
        controls.extend(
            (
                self._button(session, "button_block_up", "Up"),
                self._button(session, "button_block_down", "Down"),
                self._button(session, "button_delete_block", "Delete", "danger"),
                self._button(session, "button_container_back", "Back"),
            )
        )
        return PresentationPage(description=text, controls=tuple(controls))

    @staticmethod
    def _button_block_label(block: ButtonRoleBlock) -> str:
        if block.kind == "heading":
            return f"Heading - {(block.text or '')[:80]}"
        if block.kind == "text":
            return f"Text - {(block.text or '')[:84]}"
        if block.kind == "role":
            return f"Role - {block.role_id}"
        return {"image": "Image", "separator": "Separator"}.get(
            block.kind, "Item"
        )

    def _new_panel(self, session: ReactionRoleSession, notice: str | None) -> PresentationPage:
        text = "## Reaction Roles\n"
        text += f"Channel: <#{session.channel_id}>" if session.channel_id else "Channel: Not set"
        if notice:
            text += f"\n{notice}"
        return PresentationPage(
            description=text,
            controls=(
                PresentationControl(
                    "channel_select",
                    "reactionrole",
                    "channel",
                    session.token,
                    placeholder="Channel",
                    default_ids=(session.channel_id,) if session.channel_id else (),
                    channel_types=TEXT_CHANNEL_TYPES,
                ),
                self._button(session, "save_panel", "Create", "success"),
                self._button(session, "back", "Back"),
            ),
        )

    def _panel_page(self, session: ReactionRoleSession, notice: str | None) -> PresentationPage:
        mappings = self._last_mappings.get(session.panel_id, ())
        text = f"## Reaction Roles\n<#{session.channel_id}>"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = []
        if mappings:
            maximum_page = max(0, (len(mappings) - 1) // 20)
            page_number = min(session.mapping_page, maximum_page)
            start = page_number * 20
            controls.append(
                PresentationControl(
                    "string_select",
                    "reactionrole",
                    "pick_mapping",
                    session.token,
                    placeholder="Mapping",
                    options=tuple(
                        PresentationOption(
                            (mapping.label or mapping.emoji_key)[:100],
                            mapping.mapping_id,
                            mapping.emoji_key[:100],
                        )
                        for mapping in mappings[start : start + 20]
                    ),
                )
            )
            if maximum_page:
                controls.extend(
                    (
                        self._button(
                            session,
                            "mappings_prev",
                            "Previous",
                            disabled=page_number == 0,
                        ),
                        self._button(
                            session,
                            "mappings_next",
                            "Next",
                            disabled=page_number == maximum_page,
                        ),
                    )
                )
        controls.extend(
            (
                self._button(
                    session,
                    "add",
                    "Add",
                    "success",
                    disabled=len(mappings) >= 100,
                ),
                self._button(session, "attach", "Attach"),
                self._button(session, "repair", "Repair"),
                PresentationControl(
                    "channel_select",
                    "reactionrole",
                    "preview_channel",
                    session.token,
                    placeholder="Preview channel",
                    default_ids=(session.preview_channel_id,) if session.preview_channel_id else (),
                    channel_types=TEXT_CHANNEL_TYPES,
                ),
                self._button(session, "inspect", "Inspect"),
                self._button(session, "preview", "Preview"),
                self._button(session, "convert", "Convert", "danger", disabled=True),
                self._button(session, "detach", "Detach", "danger"),
                self._button(session, "back", "Back"),
            )
        )
        return PresentationPage(description=text, controls=tuple(controls))

    def _mapping_page(self, session: ReactionRoleSession, notice: str | None) -> PresentationPage:
        text = "## Mapping\n"
        text += f"Emoji: {session.emoji or 'Not set'}\n"
        text += f"Role: <@&{session.role_id}>\n" if session.role_id else "Role: Not set\n"
        text += f"Label: {session.label or 'None'}"
        if notice:
            text += f"\n{notice}"
        return PresentationPage(
            description=text,
            controls=(
                PresentationControl(
                    "role_select",
                    "reactionrole",
                    "role",
                    session.token,
                    placeholder="Role",
                    default_ids=(session.role_id,) if session.role_id else (),
                ),
                self._button(session, "details", "Emoji and label"),
                self._button(session, "save_mapping", "Save", "success"),
                self._button(session, "up", "Up", disabled=not session.mapping_id),
                self._button(session, "down", "Down", disabled=not session.mapping_id),
                self._button(
                    session,
                    "remove",
                    "Remove",
                    "danger",
                    disabled=not session.mapping_id,
                ),
                self._button(session, "panel_back", "Back"),
            ),
        )

    @staticmethod
    def _public_panel(
        mappings: tuple[ReactionMapping, ...], *, continuation: bool = False
    ) -> PresentationPage:
        header = "## Reaction Roles"
        footer = "React to add or remove roles."
        values = tuple(mapping.label or f"<@&{mapping.role_id}>" for mapping in mappings)
        fixed = len(mappings)
        if not continuation:
            fixed += len(header) + len(footer) + 1
        fixed += sum(len(mapping.emoji_key) + 1 for mapping in mappings)
        available = max(0, 4000 - fixed)

        def rendered_length(cap: int) -> int:
            return sum(min(len(value), cap) for value in values)

        low, high = 0, max((len(value) for value in values), default=0)
        while low < high:
            middle = (low + high + 1) // 2
            if rendered_length(middle) <= available:
                low = middle
            else:
                high = middle - 1

        def clipped(value: str) -> str:
            if len(value) <= low:
                return value
            if low <= 1:
                return "…"[:low]
            return value[: low - 1] + "…"

        lines = [] if continuation else [header]
        lines.extend(
            f"{mapping.emoji_key} {clipped(value)}"
            for mapping, value in zip(mappings, values, strict=True)
        )
        if not continuation:
            lines.append(footer)
        return PresentationPage(description="\n".join(lines))

    @classmethod
    def _reaction_public_pages(
        cls, mappings: tuple[ReactionMapping, ...]
    ) -> tuple[PresentationPage, ...]:
        chunks = tuple(
            mappings[offset : offset + 20] for offset in range(0, len(mappings), 20)
        ) or ((),)
        return tuple(
            cls._public_panel(chunk, continuation=index > 0)
            for index, chunk in enumerate(chunks)
        )

    @staticmethod
    def _normalize_emoji(value: str) -> str:
        value = value.strip()
        if CUSTOM_EMOJI.fullmatch(value):
            return value
        if not value or len(value) > 32 or any(character.isspace() for character in value):
            return ""
        return value

    @staticmethod
    def _emoji_identity(value: str) -> str:
        normalized = value.strip()
        if CUSTOM_EMOJI.fullmatch(normalized):
            return f"custom:{normalized.rpartition(':')[2].removesuffix('>')}"
        return normalized

    @staticmethod
    def _button(
        session: ReactionRoleSession,
        action: str,
        label: str,
        style: str = "secondary",
        *,
        disabled: bool = False,
    ) -> PresentationControl:
        return PresentationControl(
            "button",
            "reactionrole",
            action,
            session.token,
            label=label,
            style=style,
            disabled=disabled,
        )

    async def _save(self, session: ReactionRoleSession) -> None:
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.setex(
            f"lumi:reaction_roles:session:{session.token}",
            self.SESSION_TTL,
            encode(session),
        )
        pipeline.setex(
            ui_key("owner", session.token),
            self.SESSION_TTL,
            str(session.owner_id),
        )
        await pipeline.execute()
        self._last_panels[session.guild_id] = await self._store.list_reaction_panels(
            session.guild_id
        )
        self._last_button_panels[
            session.guild_id
        ] = await self._store.list_button_panels(session.guild_id)
        if session.panel_id and session.panel_kind == "reaction":
            self._last_mappings[session.panel_id] = await self._store.list_mappings(
                session.panel_id
            )
        if session.panel_id and session.panel_kind == "button":
            containers = await self._store.list_button_containers(session.panel_id)
            self._last_button_containers[session.panel_id] = containers
            for container in containers:
                self._last_button_blocks[
                    container.container_id
                ] = await self._store.list_button_blocks(container.container_id)

    async def _load(self, token: str) -> ReactionRoleSession | None:
        payload = await self._cache.get(f"lumi:reaction_roles:session:{token}")
        return decode(payload, ReactionRoleSession) if payload else None

    @staticmethod
    def _page(kind: str, text: str) -> DiscordIntent:
        return DiscordIntent(kind=kind, pages=(PresentationPage(description=text),))

    def _result(self, job_id: str, intent: DiscordIntent) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="reaction_roles",
            group="community",
            generation=self._generation,
            status="ok",
            intents=(intent,),
            completed_at_ms=int(time.time() * 1000),
        )

    def _empty(self, job_id: str) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="reaction_roles",
            group="community",
            generation=self._generation,
            status="ok",
            completed_at_ms=int(time.time() * 1000),
        )

    async def close(self) -> None:
        if self._publisher_task:
            self._publisher.stop()
            self._publisher_task.cancel()
            import asyncio

            await asyncio.gather(self._publisher_task, return_exceptions=True)
