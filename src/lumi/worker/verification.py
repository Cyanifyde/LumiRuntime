from __future__ import annotations

import asyncio
import logging
import secrets
import time
from weakref import WeakValueDictionary

import msgspec
from redis.asyncio import Redis

from lumi.contracts import (
    ChannelTransitionSpec,
    ComponentAliasSpec,
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
    ResultEnvelope,
    TypedRequest,
    decode,
    encode,
)
from lumi.contracts.role_access import READ_ACTOR_ROLES, WRITE_ACTOR_ROLES, RoleChange
from lumi.panels import MySQLPanelStore, VerificationPanel
from lumi.ui_keys import ui_key

from .actions import EdgeActionClient
from .actor_roles import ActorRoles

LOGGER = logging.getLogger(__name__)
TEXT_CHANNEL_TYPES = (0, 5, 10, 11, 12)


class VerificationSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    panel_id: str = ""
    channel_id: int = 0
    unverified_role_id: int = 0
    verified_role_id: int = 0
    body: str = "Press Verify to continue."
    screen: str = "home"
    preview_channel_id: int = 0


class VerificationModule:
    SESSION_TTL = 900

    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        store: MySQLPanelStore,
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._store = store
        self._actor_locks = WeakValueDictionary()
        self._actions = EdgeActionClient(
            queue_redis,
            module="verification",
            group="security",
            generation=generation,
        )

    @property
    def manifest(self) -> Manifest:
        session = r"(?P<session>[a-f0-9]{16})"
        panel = r"(?P<panel>[a-f0-9]{32})"
        admin = ("administrator",)
        dashboard_deadline = 120_000
        components = (
            ComponentSpec(
                "verification",
                "pick",
                "verification.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="string_values",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="verification",
                required_permissions=admin,
            ),
            *(
                ComponentSpec(
                    "verification",
                    action,
                    "verification.dashboard",
                    rf"^{session}$",
                    captures=(CaptureSpec("session"),),
                    deadline_ms=dashboard_deadline,
                    session_bound=True,
                    acl_key="verification",
                    required_permissions=admin,
                )
                for action in (
                    "create",
                    "back",
                    "save",
                    "repair",
                    "delete",
                    "inspect",
                    "preview",
                    "convert",
                )
            ),
            ComponentSpec(
                "verification",
                "channel",
                "verification.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="channel_ids",
                deadline_ms=dashboard_deadline,
                channel_types=TEXT_CHANNEL_TYPES,
                session_bound=True,
                acl_key="verification",
                required_permissions=admin,
            ),
            ComponentSpec(
                "verification",
                "preview_channel",
                "verification.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="channel_ids",
                deadline_ms=dashboard_deadline,
                channel_types=TEXT_CHANNEL_TYPES,
                session_bound=True,
                acl_key="verification",
                required_permissions=admin,
            ),
            ComponentSpec(
                "verification",
                "unverified",
                "verification.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="role_ids",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="verification",
                required_permissions=admin,
            ),
            ComponentSpec(
                "verification",
                "verified",
                "verification.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="role_ids",
                deadline_ms=dashboard_deadline,
                session_bound=True,
                acl_key="verification",
                required_permissions=admin,
            ),
            ComponentSpec(
                "verification",
                "text",
                "verification.dashboard",
                rf"^{session}$",
                captures=(CaptureSpec("session"),),
                input_kind="modal",
                deadline_ms=dashboard_deadline,
                modal=ModalSpec(
                    "Verification text",
                    (
                        ModalFieldSpec(
                            "body",
                            "Text",
                            style="paragraph",
                            minimum=1,
                            maximum=2000,
                        ),
                    ),
                ),
                session_bound=True,
                acl_key="verification",
                required_permissions=admin,
            ),
            ComponentSpec(
                "verification",
                "verify",
                "verification.verify",
                rf"^{panel}$",
                captures=(CaptureSpec("panel"),),
                acl_key="verification",
                public=True,
                ephemeral=True,
                actor_role_access=True,
                human_guild_member=True,
                source_message_facts=True,
            ),
            ComponentSpec(
                "verification",
                "legacy",
                "verification.verify",
                r"^(?P<unverified>[0-9]{17,20})~(?P<verified>[0-9]{17,20})$",
                legacy_aliases=(ComponentAliasSpec("verify_btn"),),
                captures=(
                    CaptureSpec("unverified", "snowflake"),
                    CaptureSpec("verified", "snowflake"),
                ),
                acl_key="verification",
                public=True,
                ephemeral=True,
                actor_role_access=True,
                human_guild_member=True,
                source_message_facts=True,
            ),
        )
        passive = tuple(
            PassiveMatcherSpec(
                route=f"verification.{kind}_deleted",
                handler="verification.resource_deleted",
                event=f"{kind}_delete",
                expression=MatcherExpr(
                    "exists", field="message_id" if kind == "message" else "resource_id"
                ),
                queue_class="security",
            )
            for kind in ("message", "channel", "role")
        )
        return Manifest(
            schema=2,
            module="verification",
            channel_transition=ChannelTransitionSpec(quiesce_guild=True, rebuild_posts=True),
            group="security",
            generation=self._generation,
            job_stream="lumi:jobs:security",
            display_name="Verification",
            summary="Role verification.",
            presentation_mode="v2",
            commands=(
                CommandSpec(
                    root="verification",
                    handler="verification.open",
                    grammar=r"^$",
                    help=HelpSpec(
                        ".verification", "Configure the verification gate and member roles."
                    ),
                    acl_key="verification",
                    queue_class="security",
                    required_permissions=admin,
                ),
            ),
            passive=passive,
            components=components,
            capabilities=(
                "fetch_message",
                "create_message",
                "edit_message",
                "delete_message",
                READ_ACTOR_ROLES,
                WRITE_ACTOR_ROLES,
            ),
            version="1.0.2",
        )

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        actor_id = int(request.event.get("actor_id") or 0)
        guild_id = int(request.event.get("guild_id") or 0)
        try:
            if request.handler == "verification.resource_deleted":
                if guild_id:
                    kind = request.route.removeprefix("verification.").removesuffix("_deleted")
                    resource_id = request.event.get(
                        "message_id" if kind == "message" else "resource_id"
                    )
                    await self._store.mark_verification_resource_missing(
                        guild_id, kind, int(resource_id or 0)
                    )
                return self._empty(job_id)
            if not actor_id or not guild_id:
                return self._result(job_id, self._page("send", "Unavailable."))
            if request.handler == "verification.open":
                session = VerificationSession(secrets.token_hex(8), actor_id, guild_id)
                await self._save(session)
                return self._result(job_id, self._dashboard("send", session))
            if request.handler == "verification.verify":
                return await self._verify(request, job_id, guild_id, actor_id)
            return await self._dashboard_action(request, job_id, guild_id, actor_id)
        except Exception:
            LOGGER.exception(
                "verification_request_failed",
                extra={"handler": request.handler, "guild_id": guild_id},
            )
            return self._result(job_id, self._page("ephemeral", "Unavailable."))

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
        if action == "create":
            session = VerificationSession(token, actor_id, guild_id, screen="edit")
        elif action == "back":
            session = msgspec.structs.replace(session, screen="home", panel_id="")
        elif action == "pick":
            panel_id = str(values[0]) if values else ""
            panel = await self._store.get_verification(guild_id, panel_id)
            if panel is None:
                notice = "Unavailable."
            else:
                session = VerificationSession(
                    token,
                    actor_id,
                    guild_id,
                    panel.panel_id,
                    panel.channel_id,
                    panel.unverified_role_id,
                    panel.verified_role_id,
                    panel.body,
                    "edit",
                    0,
                )
        elif action == "channel":
            session = msgspec.structs.replace(session, channel_id=int(values[0]) if values else 0)
        elif action == "preview_channel":
            session = msgspec.structs.replace(
                session, preview_channel_id=int(values[0]) if values else 0
            )
        elif action == "unverified":
            session = msgspec.structs.replace(
                session, unverified_role_id=int(values[0]) if values else 0
            )
        elif action == "verified":
            session = msgspec.structs.replace(
                session, verified_role_id=int(values[0]) if values else 0
            )
        elif action == "text":
            body = str(dict(request.arguments.get("values", {})).get("body") or "").strip()
            if body:
                session = msgspec.structs.replace(session, body=body)
        elif action in {"save", "repair"}:
            notice, session = await self._save_panel(
                session, job_id, actor_id, force_repair=action == "repair"
            )
        elif action == "inspect":
            notice = await self._inspect_panel(session, job_id, actor_id)
        elif action == "preview":
            notice = await self._preview_panel(session, job_id)
        elif action == "convert":


            notice = "Awaiting approval."
        elif action == "delete":
            panel = await self._store.get_verification(guild_id, session.panel_id)
            if panel is None:
                notice = "Unavailable."
            elif panel.source == "legacy" and panel.ownership == "unknown":
                notice = "Awaiting approval."
            else:
                if panel.message_id:
                    deleted = await self._actions.request(
                        job_id=job_id,
                        kind="delete_message",
                        guild_id=guild_id,
                        channel_id=panel.channel_id,
                        message_id=panel.message_id,
                        idempotency_key=f"verification-delete:{panel.panel_id}:{panel.route_revision}",
                    )
                    if not deleted.ok:
                        await self._store.mark_verification(
                            guild_id, panel.panel_id, "needs_repair", actor_id
                        )
                        notice = "Needs repair."
                        await self._save(session)
                        return self._result(job_id, self._dashboard("edit", session, notice))
                await self._store.mark_verification(guild_id, panel.panel_id, "deleted", actor_id)
                session = msgspec.structs.replace(session, screen="home", panel_id="")
                notice = "Deleted."
        await self._save(session)
        return self._result(job_id, self._dashboard("edit", session, notice))

    async def _save_panel(
        self,
        session: VerificationSession,
        job_id: str,
        actor_id: int,
        *,
        force_repair: bool,
    ) -> tuple[str, VerificationSession]:
        if (
            not session.channel_id
            or not session.unverified_role_id
            or not session.verified_role_id
            or session.unverified_role_id == session.verified_role_id
            or not session.body.strip()
        ):
            return "Incomplete.", session
        existing = (
            await self._store.get_verification(session.guild_id, session.panel_id)
            if session.panel_id
            else None
        )
        panel_id = session.panel_id or secrets.token_hex(16)
        panel = VerificationPanel(
            panel_id,
            session.guild_id,
            session.channel_id,
            existing.message_id if existing else None,
            session.body.strip(),
            session.unverified_role_id,
            session.verified_role_id,
            "pending" if existing is None or force_repair else existing.lifecycle,
            existing.ownership if existing else "lumi",
            existing.source if existing else "new",
            (existing.route_revision + 1) if existing else 1,
        )
        await self._store.stage_verification(panel, actor_id)
        public_page = self._public_panel(panel)
        if (
            existing
            and existing.message_id
            and existing.source == "legacy"
            and existing.ownership == "unknown"
            and not force_repair
        ):
            return "Saved.", msgspec.structs.replace(session, panel_id=panel_id)
        if existing and existing.message_id and not force_repair:
            result = await self._actions.request(
                job_id=job_id,
                kind="edit_message",
                guild_id=session.guild_id,
                channel_id=session.channel_id,
                message_id=existing.message_id,
                page=public_page,
                persistent=True,
                idempotency_key=f"verification-edit:{panel_id}:{panel.route_revision}",
            )
            if not result.ok:
                await self._store.mark_verification(
                    session.guild_id, panel_id, "needs_repair", actor_id
                )
                return "Needs repair.", msgspec.structs.replace(session, panel_id=panel_id)
            await self._store.activate_verification(
                session.guild_id,
                panel_id,
                session.channel_id,
                existing.message_id,
                actor_id,
            )
        else:
            result = await self._actions.request(
                job_id=job_id,
                kind="create_message",
                guild_id=session.guild_id,
                channel_id=session.channel_id,
                page=public_page,
                persistent=True,
                idempotency_key=f"verification-create:{panel_id}:{panel.route_revision}",
            )
            if not result.ok:
                await self._store.mark_verification(
                    session.guild_id, panel_id, "needs_repair", actor_id
                )
                return "Needs repair.", msgspec.structs.replace(session, panel_id=panel_id)
            await self._store.activate_verification(
                session.guild_id,
                panel_id,
                int(result.data["channel_id"]),
                int(result.data["message_id"]),
                actor_id,
            )
        return "Saved.", msgspec.structs.replace(session, panel_id=panel_id)

    async def _inspect_panel(self, session: VerificationSession, job_id: str, actor_id: int) -> str:
        panel = await self._store.get_verification(session.guild_id, session.panel_id)
        if panel is None or panel.message_id is None:
            return "Unavailable."
        result = await self._actions.request(
            job_id=job_id,
            kind="fetch_message",
            guild_id=session.guild_id,
            channel_id=panel.channel_id,
            message_id=panel.message_id,
            role_ids=(panel.unverified_role_id, panel.verified_role_id),
            idempotency_key=f"verification-inspect:{panel.panel_id}:{job_id}",
        )
        classification = "needs_repair"
        if result.ok:
            classification = self._classify_preflight(
                result.data,
                (panel.unverified_role_id, panel.verified_role_id),
            )
        await self._store.record_preflight(
            "verification", panel.panel_id, classification, result.data
        )
        if classification == "needs_repair":
            await self._store.mark_verification(
                session.guild_id, panel.panel_id, "needs_repair", actor_id
            )
            return "Needs repair."
        await self._store.activate_verification(
            session.guild_id,
            panel.panel_id,
            panel.channel_id,
            panel.message_id,
            actor_id,
        )
        return "Editable."

    @staticmethod
    def _classify_preflight(snapshot: dict[str, object], role_ids: tuple[int, int]) -> str:
        roles = dict(snapshot.get("roles", {}))
        role_ready = all(
            bool(dict(roles.get(str(role_id), {})).get("manageable")) for role_id in role_ids
        )
        permissions = dict(snapshot.get("permissions", {}))
        component_ids = tuple(str(value) for value in snapshot.get("component_custom_ids", ()))
        expected_legacy = f"verify_btn:{role_ids[0]}:{role_ids[1]}"
        route_ready = expected_legacy in component_ids or any(
            value.startswith("lumi:verification:verify:") for value in component_ids
        )
        editable = bool(
            snapshot.get("current_lumi")
            and not snapshot.get("webhook")
            and int(snapshot.get("message_type", -1)) in {0, 19}
            and not snapshot.get("attachments")
        )
        if editable and role_ready and route_ready and permissions.get("send_messages"):
            return "editable"
        return "needs_repair"

    async def _preview_panel(self, session: VerificationSession, job_id: str) -> str:
        panel = await self._store.get_verification(session.guild_id, session.panel_id)
        classification = await self._store.migration_classification(
            "verification", session.panel_id
        )
        if panel is None or classification != "editable" or not session.preview_channel_id:
            return "Inspect and select a preview channel."
        result = await self._actions.request(
            job_id=job_id,
            kind="create_message",
            guild_id=session.guild_id,
            channel_id=session.preview_channel_id,
            page=self._public_panel(panel),
            persistent=True,
            idempotency_key=f"verification-preview:{panel.panel_id}:{job_id}",
        )
        if not result.ok:
            return "Unavailable."
        await self._store.record_preview(
            "verification",
            panel.panel_id,
            int(result.data["channel_id"]),
            int(result.data["message_id"]),
        )
        return "Preview sent."

    async def _convert_panel(self, session: VerificationSession, job_id: str, actor_id: int) -> str:
        panel = await self._store.get_verification(session.guild_id, session.panel_id)
        classification = await self._store.migration_classification(
            "verification", session.panel_id
        )
        if panel is None or panel.message_id is None or classification != "editable":
            return "Unavailable."
        result = await self._actions.request(
            job_id=job_id,
            kind="edit_message",
            guild_id=session.guild_id,
            channel_id=panel.channel_id,
            message_id=panel.message_id,
            page=self._public_panel(panel),
            persistent=True,
            idempotency_key=f"verification-convert:{panel.panel_id}:{panel.route_revision}",
        )
        if (
            not result.ok
            or (result.data.get("channel_id"), result.data.get("message_id"))
            != (panel.channel_id, panel.message_id)
            or not result.data.get("components_v2")
            or f"lumi:verification:verify:{panel.panel_id}"
            not in result.data.get("component_custom_ids", ())
        ):
            await self._store.mark_verification(
                session.guild_id, panel.panel_id, "needs_repair", actor_id
            )
            return "Needs repair."
        await self._store.activate_verification(
            session.guild_id,
            panel.panel_id,
            panel.channel_id,
            panel.message_id,
            actor_id,
        )
        await self._store.mark_converted("verification", panel.panel_id, result.data)
        return "Converted."

    async def _verify(
        self, request: TypedRequest, job_id: str, guild_id: int, actor_id: int
    ) -> ResultEnvelope:
        panel_id = str(request.arguments.get("panel") or "")
        message_id = int(request.event.get("message_id") or 0)
        if panel_id:
            panel = await self._store.get_verification(guild_id, panel_id)
        else:
            panel = await self._store.get_verification_by_message(guild_id, message_id)
            if panel is not None and (
                int(request.arguments.get("unverified") or 0) != panel.unverified_role_id
                or int(request.arguments.get("verified") or 0) != panel.verified_role_id
            ):
                panel = None
        if (
            panel is None
            or panel.lifecycle != "active"
            or panel.message_id != message_id
        ):
            return self._result(job_id, self._page("ephemeral", "Unavailable."))
        roles = ActorRoles(
            self._actions, job_id=job_id, guild_id=guild_id, channel_id=panel.channel_id,
            message_id=message_id, actor_id=actor_id,
        )
        grant, remove = panel.verified_role_id, panel.unverified_role_id
        text = "Unavailable."
        async with self._actor_locks.setdefault((guild_id, actor_id), asyncio.Lock()):
            result = await roles.settled()
            if result is None:
                facts = await roles.observe()
                if facts is None:
                    return self._result(job_id, self._page("ephemeral", text))
                joined, assigned = facts
                changes = []
                if grant not in assigned:
                    changes.append(RoleChange(grant, True))
                if remove in assigned:
                    changes.append(RoleChange(remove, False))
                result = await roles.apply(joined, (grant, remove), changes)
            outcome = result.data or {}
            granted = {"role": grant, "present": True} in outcome.get("applied", ())
            if result.ok:
                text = "Verified." if granted else "Already verified."
            elif result.status == "rejected" and granted:


                if await roles.settled(1) is None:
                    await roles.apply(outcome["joined"], (grant,), (RoleChange(grant, False),), step=1)
        return self._result(job_id, self._page("ephemeral", text))

    def _dashboard(
        self, kind: str, session: VerificationSession, notice: str | None = None
    ) -> DiscordIntent:
        if session.screen == "home":
            return DiscordIntent(
                kind=kind,
                session_id=session.token,
                pages=(self._home_page(session, notice),),
            )
        return DiscordIntent(
            kind=kind,
            session_id=session.token,
            pages=(self._edit_page(session, notice),),
        )

    def _home_page(self, session: VerificationSession, notice: str | None) -> PresentationPage:

        cached = getattr(self, "_last_panels", {}).get(session.guild_id, ())
        text = "## Verification"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = []
        if cached:
            controls.append(
                PresentationControl(
                    "string_select",
                    "verification",
                    "pick",
                    session.token,
                    placeholder="Panel",
                    options=tuple(
                        PresentationOption(
                            f"#{panel.channel_id}",
                            panel.panel_id,
                            "Needs repair" if panel.lifecycle == "needs_repair" else None,
                        )
                        for panel in cached[:25]
                    ),
                )
            )
        controls.append(self._button(session, "create", "Create", "success"))
        return PresentationPage(description=text, controls=tuple(controls))

    def _edit_page(self, session: VerificationSession, notice: str | None) -> PresentationPage:
        text = "## Verification\n"
        text += (
            f"Channel: <#{session.channel_id}>\n" if session.channel_id else "Channel: Not set\n"
        )
        text += (
            f"Unverified: <@&{session.unverified_role_id}>\n"
            if session.unverified_role_id
            else "Unverified: Not set\n"
        )
        text += (
            f"Verified: <@&{session.verified_role_id}>\n"
            if session.verified_role_id
            else "Verified: Not set\n"
        )
        text += session.body
        if notice:
            text += f"\n{notice}"
        controls = (
            PresentationControl(
                "channel_select",
                "verification",
                "channel",
                session.token,
                placeholder="Channel",
                default_ids=(session.channel_id,) if session.channel_id else (),
                channel_types=TEXT_CHANNEL_TYPES,
            ),
            PresentationControl(
                "role_select",
                "verification",
                "unverified",
                session.token,
                placeholder="Unverified role",
                default_ids=(session.unverified_role_id,) if session.unverified_role_id else (),
            ),
            PresentationControl(
                "role_select",
                "verification",
                "verified",
                session.token,
                placeholder="Verified role",
                default_ids=(session.verified_role_id,) if session.verified_role_id else (),
            ),
            PresentationControl(
                "channel_select",
                "verification",
                "preview_channel",
                session.token,
                placeholder="Preview channel",
                default_ids=(session.preview_channel_id,) if session.preview_channel_id else (),
                channel_types=TEXT_CHANNEL_TYPES,
            ),
            self._button(session, "text", "Text"),
            self._button(session, "save", "Save", "success"),
            self._button(session, "repair", "Repair"),
            self._button(session, "inspect", "Inspect", disabled=not session.panel_id),
            self._button(session, "preview", "Preview", disabled=not session.panel_id),
            self._button(session, "convert", "Convert", "danger", disabled=True),
            self._button(session, "delete", "Delete", "danger", disabled=not session.panel_id),
            self._button(session, "back", "Back"),
        )
        return PresentationPage(description=text, controls=controls)

    def _public_panel(self, panel: VerificationPanel) -> PresentationPage:
        return PresentationPage(
            description=panel.body,
            controls=(
                PresentationControl(
                    "button",
                    "verification",
                    "verify",
                    panel.panel_id,
                    label="Verify",
                    style="success",
                ),
            ),
        )

    @staticmethod
    def _button(
        session: VerificationSession,
        action: str,
        label: str,
        style: str = "secondary",
        *,
        disabled: bool = False,
    ) -> PresentationControl:
        return PresentationControl(
            "button",
            "verification",
            action,
            session.token,
            label=label,
            style=style,
            disabled=disabled,
        )

    async def _save(self, session: VerificationSession) -> None:
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.setex(
            f"lumi:verification:session:{session.token}",
            self.SESSION_TTL,
            encode(session),
        )
        pipeline.setex(
            ui_key("owner", session.token),
            self.SESSION_TTL,
            str(session.owner_id),
        )
        await pipeline.execute()
        panels = await self._store.list_verification(session.guild_id)
        if not hasattr(self, "_last_panels"):
            self._last_panels: dict[int, tuple[VerificationPanel, ...]] = {}
        self._last_panels[session.guild_id] = panels

    async def _load(self, token: str) -> VerificationSession | None:
        payload = await self._cache.get(f"lumi:verification:session:{token}")
        return decode(payload, VerificationSession) if payload else None

    @staticmethod
    def _page(kind: str, text: str) -> DiscordIntent:
        return DiscordIntent(kind=kind, pages=(PresentationPage(description=text),))

    def _result(self, job_id: str, intent: DiscordIntent) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="verification",
            group="security",
            generation=self._generation,
            status="ok",
            intents=(intent,),
            completed_at_ms=int(time.time() * 1000),
        )

    def _empty(self, job_id: str) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="verification",
            group="security",
            generation=self._generation,
            status="ok",
            completed_at_ms=int(time.time() * 1000),
        )

    async def close(self) -> None:
        return None
