from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import secrets
import time
import weakref

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
    ModalFieldSpec,
    ModalSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
    decode,
    encode,
)
from lumi.settings.catalog import SettingsCatalog
from lumi.settings.prefix import validate_prefix
from lumi.settings.publisher import PolicyOutboxPublisher
from lumi.settings.store import GuildSettingsState, MySQLSettingsStore


class SettingsSession(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    screen: str = "home"
    group: str = ""
    module: str = ""
    command: str = ""
    page: int = 0


class SettingsModule:
    SESSION_TTL = 900

    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        cache_redis: Redis,
        store: MySQLSettingsStore,
        default_prefix: str = ".",
    ) -> None:
        self._generation = generation
        self._queue = queue_redis
        self._cache = cache_redis
        self._store = store
        self._default_prefix = default_prefix
        self.catalog = SettingsCatalog(queue_redis, store)
        self.publisher = PolicyOutboxPublisher(store, queue_redis)
        self._session_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )

    @property
    def manifest(self) -> Manifest:
        session = r"(?P<session>[a-f0-9]{16})"
        page = rf"{session}~(?P<page>[0-9]{{1,4}})"
        enabled = rf"{session}~(?P<enabled>[01])"
        components = tuple(
            msgspec.structs.replace(
                component,
                session_bound=True,
                guild_control=True,
                captures=(CaptureSpec("session"),)
                + (
                    (CaptureSpec("page", "integer", minimum=0, maximum=9999),)
                    if component.action in {"group_page", "commands_page"}
                    else (
                        (CaptureSpec("enabled", "boolean"),)
                        if component.action in {"set_module", "set_command"}
                        else ()
                    )
                ),
            )
            for component in (
                ComponentSpec("settings", "home", "settings.action", rf"^{session}$"),
                ComponentSpec(
                    "settings",
                    "pick_group",
                    "settings.action",
                    rf"^{session}$",
                    input_kind="string_values",
                ),
                ComponentSpec("settings", "group_page", "settings.action", rf"^{page}$"),
                ComponentSpec(
                    "settings",
                    "pick_module",
                    "settings.action",
                    rf"^{session}$",
                    input_kind="string_values",
                ),
                ComponentSpec("settings", "module", "settings.action", rf"^{session}$"),
                ComponentSpec("settings", "set_module", "settings.action", rf"^{enabled}$"),
                ComponentSpec("settings", "module_access", "settings.action", rf"^{session}$"),
                ComponentSpec("settings", "commands", "settings.action", rf"^{session}$"),
                ComponentSpec("settings", "commands_page", "settings.action", rf"^{page}$"),
                ComponentSpec("settings", "noop", "settings.action", rf"^{session}$"),
                ComponentSpec(
                    "settings",
                    "pick_command",
                    "settings.action",
                    rf"^{session}$",
                    input_kind="string_values",
                ),
                ComponentSpec("settings", "command", "settings.action", rf"^{session}$"),
                ComponentSpec("settings", "set_command", "settings.action", rf"^{enabled}$"),
                ComponentSpec("settings", "command_access", "settings.action", rf"^{session}$"),
                ComponentSpec(
                    "settings",
                    "set_roles",
                    "settings.action",
                    rf"^{session}$",
                    input_kind="role_ids",
                ),
                ComponentSpec(
                    "settings",
                    "set_channels",
                    "settings.action",
                    rf"^{session}$",
                    input_kind="channel_ids",
                ),
                ComponentSpec(
                    "settings", "prefix", "settings.action", rf"^{session}$",
                    input_kind="modal",
                    modal=ModalSpec("Prefix", (ModalFieldSpec("prefix", "Prefix", minimum=1, maximum=5),)),
                    modal_prefix_field="prefix",
                ),

                ComponentSpec(
                    "settings",
                    "set_prefix",
                    "settings.action",
                    rf"^{session}$",
                    input_kind="text",
                ),
            )
        )
        return Manifest(
            schema=2,
            module="settings",
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group="platform",
            generation=self._generation,
            job_stream="lumi:jobs:platform",
            display_name="Settings",
            summary="Server settings.",
            presentation_mode="v2",
            accent_style="brand",
            version="1.1.0",
            commands=(
                CommandSpec(
                    root="settings",
                    handler="settings.open",
                    grammar=r"^(?:\s+(?P<help>help))?$",
                    captures=(CaptureSpec("help", required=False, maximum=4),),
                    help=HelpSpec(
                        ".settings", "Configure commands, access, channels and the server prefix."
                    ),
                    acl_key="settings",
                    queue_class="control",
                    public=False,
                    configure_in_guild=False,
                    guild_control=True,
                ),
            ),
            components=components,
        )

    async def start(self) -> None:
        self._catalog_task = asyncio.create_task(self.catalog.run(), name="settings-catalog")
        self._publisher_task = asyncio.create_task(
            self.publisher.run(), name="settings-policy-outbox"
        )

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        actor_id = int(request.event.get("actor_id", 0))
        guild_id = int(request.event.get("guild_id", 0))
        if not actor_id or not guild_id:
            return self._result(job_id, DiscordIntent(kind="send", pages=()))
        if request.handler == "settings.open":
            if request.arguments.get("help"):
                return self._result(job_id, self._simple(".settings"))
            token = secrets.token_hex(8)
            session = SettingsSession(token, actor_id, guild_id)
            await self._save_session(session)
            state = await self._state(session)
            return self._result(job_id, self._screen(session, self._home(session, state), "send"))

        token = str(request.arguments.get("session") or "")
        lock = self._session_locks.setdefault(token, asyncio.Lock())
        async with lock:
            return await self._handle_session(request, job_id, actor_id, guild_id, token)

    async def _handle_session(
        self,
        request: TypedRequest,
        job_id: str,
        actor_id: int,
        guild_id: int,
        token: str,
    ) -> ResultEnvelope:
        session = await self._load_session(token)
        if session is None or session.owner_id != actor_id or session.guild_id != guild_id:
            return self._result(job_id, DiscordIntent(kind="edit", pages=()))
        action = request.route.partition(".")[2]
        notice: str | None = None
        try:
            session = await self._apply_action(session, action, request.arguments, actor_id)
        except (LookupError, ValueError):
            notice = "Unavailable."
        session = self._normalize_session(session)
        await self._save_session(session)
        state = await self._state(session)
        page = self._render(session, state, notice)
        return self._result(job_id, self._screen(session, page, "edit"))

    def _normalize_session(self, session: SettingsSession) -> SettingsSession:
        online = {module.key: module for module in self.catalog.online_modules}
        if session.module and session.module not in online:
            return msgspec.structs.replace(
                session, screen="home", group="", module="", command="", page=0
            )
        if session.module and session.command:
            commands = {command.acl_key for command in online[session.module].commands}
            if session.command not in commands:
                return msgspec.structs.replace(session, screen="module", command="", page=0)
        return session

    async def _apply_action(
        self,
        session: SettingsSession,
        action: str,
        arguments: dict[str, object],
        actor_id: int,
    ) -> SettingsSession:
        online = {module.key: module for module in self.catalog.online_modules}
        values = tuple(str(value) for value in arguments.get("values", ()))
        if action == "home":
            return msgspec.structs.replace(
                session, screen="home", group="", module="", command="", page=0
            )
        if action == "pick_group":
            group = values[0] if values else ""
            if group not in {module.group for module in online.values()}:
                raise LookupError
            return msgspec.structs.replace(
                session, screen="group", group=group, module="", command="", page=0
            )
        if action == "group_page":
            return msgspec.structs.replace(
                session, screen="group", page=int(arguments.get("page", 0))
            )
        if action == "pick_module":
            module = values[0] if values else ""
            if module not in online:
                raise LookupError
            return msgspec.structs.replace(
                session, screen="module", module=module, command="", page=0
            )
        if action == "module":
            return msgspec.structs.replace(session, screen="module", command="", page=0)
        if action in {"prefix", "set_prefix"}:
            value = arguments.get("values", {}).get("prefix") if action == "prefix" else arguments.get("value")
            prefix = validate_prefix(str(value or ""))
            await self._store.set_prefix(session.guild_id, prefix, actor_id)
            await self.publisher.flush_safely()
            return msgspec.structs.replace(
                session, screen="home", group="", module="", command="", page=0
            )
        if session.module not in online:
            raise LookupError
        module = online[session.module]
        if action == "set_module":
            await self._store.set_module_enabled(
                session.guild_id,
                session.module,
                tuple(command.acl_key for command in module.commands),
                bool(arguments.get("enabled")),
                actor_id,
            )
            await self.publisher.flush_safely()
            return msgspec.structs.replace(session, screen="module")
        if action == "module_access":
            return msgspec.structs.replace(session, screen="module_access")
        if action == "commands":
            return msgspec.structs.replace(session, screen="commands", command="", page=0)
        if action == "commands_page":
            return msgspec.structs.replace(
                session, screen="commands", page=int(arguments.get("page", 0))
            )
        if action == "pick_command":
            command = values[0] if values else ""
            if command not in {item.acl_key for item in module.commands}:
                raise LookupError
            return msgspec.structs.replace(session, screen="command", command=command, page=0)
        command_keys = {command.acl_key for command in module.commands}
        if action in {"command", "set_command", "command_access"} and (
            not session.command or session.command not in command_keys
        ):
            raise LookupError
        if action == "command":
            return msgspec.structs.replace(session, screen="command")
        if session.command and session.command not in command_keys:
            raise LookupError
        if action == "set_command":
            await self._store.set_command_enabled(
                session.guild_id,
                session.module,
                session.command,
                bool(arguments.get("enabled")),
                actor_id,
            )
            await self.publisher.flush_safely()
            return msgspec.structs.replace(session, screen="command")
        if action == "command_access":
            return msgspec.structs.replace(session, screen="command_access")
        if action == "set_roles":
            if session.screen not in {"module_access", "command_access"}:
                raise LookupError
            scope = "command" if session.screen == "command_access" else "module"
            await self._store.set_roles(
                session.guild_id,
                scope,
                session.module,
                session.command if scope == "command" else "",
                tuple(int(value) for value in arguments.get("values", ())),
                actor_id,
            )
            await self.publisher.flush_safely()
            return session
        if action == "set_channels":
            if session.screen not in {"module_access", "command_access"}:
                raise LookupError
            scope = "command" if session.screen == "command_access" else "module"
            channel_types = {
                int(key): int(value)
                for key, value in dict(arguments.get("channel_types", {})).items()
            }
            identifiers = tuple(int(value) for value in arguments.get("values", ()))
            categories = tuple(value for value in identifiers if channel_types.get(value) == 4)
            channels = tuple(value for value in identifiers if channel_types.get(value) != 4)
            await self._store.set_channels(
                session.guild_id,
                scope,
                session.module,
                session.command if scope == "command" else "",
                channels,
                categories,
                actor_id,
            )
            await self.publisher.flush_safely()
            return session
        return session

    async def _state(self, session: SettingsSession) -> GuildSettingsState:
        command_map = {
            module.key: tuple(command.acl_key for command in module.commands)
            for module in self.catalog.current.modules
            if module.key != "settings"
        }
        await self._store.ensure_guild(session.guild_id, self._default_prefix, session.owner_id)
        await self._store.sync_catalog(session.guild_id, command_map)
        await self.publisher.flush_safely()
        return await self._store.read_state(
            session.guild_id, self._default_prefix, session.owner_id
        )

    def _render(
        self, session: SettingsSession, state: GuildSettingsState, notice: str | None
    ) -> PresentationPage:
        if session.screen == "home":
            return self._home(session, state, notice)
        if session.screen == "group":
            return self._group(session, state, notice)
        if session.screen == "module":
            return self._module(session, state, notice)
        if session.screen == "module_access":
            return self._access(session, state, False, notice)
        if session.screen == "commands":
            return self._commands(session, state, notice)
        if session.screen == "command":
            return self._command(session, state, notice)
        if session.screen == "command_access":
            return self._access(session, state, True, notice)
        return self._home(session, state, notice)

    def _home(
        self,
        session: SettingsSession,
        state: GuildSettingsState,
        notice: str | None = None,
    ) -> PresentationPage:
        modules = self.catalog.online_modules
        enabled = sum(
            bool(state.modules.get(module.key) and state.modules[module.key].enabled)
            for module in modules
        )
        groups = sorted({module.group for module in modules})
        text = f"## Settings\nPrefix: `{state.prefix}`\nModules: {enabled}/{len(modules)}"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = []
        if groups:
            controls.append(
                self._select(
                    session,
                    "pick_group",
                    "Group",
                    tuple(PresentationOption(self._label(group), group) for group in groups),
                )
            )
        controls.append(self._button(session, "prefix", "Prefix", "primary"))
        return PresentationPage(description=text, controls=tuple(controls))

    def _group(
        self, session: SettingsSession, state: GuildSettingsState, notice: str | None = None
    ) -> PresentationPage:
        modules = sorted(
            (module for module in self.catalog.online_modules if module.group == session.group),
            key=lambda module: module.display_name.casefold(),
        )
        pages = max(1, (len(modules) + 24) // 25)
        page = min(session.page, pages - 1)
        visible = modules[page * 25 : (page + 1) * 25]
        text = f"## {self._label(session.group)}"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = []
        if visible:
            controls.append(
                self._select(
                    session,
                    "pick_module",
                    "Module",
                    tuple(
                        PresentationOption(
                            module.display_name,
                            module.key,
                            "On"
                            if state.modules.get(module.key) and state.modules[module.key].enabled
                            else "Off",
                        )
                        for module in visible
                    ),
                )
            )
        if pages > 1:
            controls.extend(self._page_buttons(session, "group_page", page, pages))
        controls.append(self._button(session, "home", "Back"))
        return PresentationPage(description=text, controls=tuple(controls))

    def _module(
        self, session: SettingsSession, state: GuildSettingsState, notice: str | None = None
    ) -> PresentationPage:
        module = self._catalog_module(session.module)
        configured = state.modules.get(session.module)
        enabled = bool(configured and configured.enabled)
        text = f"## {module.display_name}\n{'On' if enabled else 'Off'}"
        if notice:
            text += f"\n{notice}"
        controls = [
            self._button(
                session,
                "set_module",
                "Disable" if enabled else "Enable",
                "danger" if enabled else "success",
                payload=f"{session.token}~{int(not enabled)}",
            ),
            self._button(session, "module_access", "Access"),
        ]
        if module.commands:
            controls.append(self._button(session, "commands", "Commands"))
        controls.append(
            self._button(session, "group_page", "Back", payload=f"{session.token}~0")
        )
        return PresentationPage(description=text, controls=tuple(controls))

    def _commands(
        self, session: SettingsSession, state: GuildSettingsState, notice: str | None = None
    ) -> PresentationPage:
        module = self._catalog_module(session.module)
        commands = module.commands
        pages = max(1, (len(commands) + 24) // 25)
        page = min(session.page, pages - 1)
        visible = commands[page * 25 : (page + 1) * 25]
        text = f"## {module.display_name}"
        if notice:
            text += f"\n{notice}"
        controls: list[PresentationControl] = []
        if visible:
            controls.append(
                self._select(
                    session,
                    "pick_command",
                    "Command",
                    tuple(
                        PresentationOption(
                            f"{command.root}",
                            command.acl_key,
                            {
                                "enabled": "On",
                                "disabled": "Off",
                                "pending": "Pending",
                            }.get(
                                state.commands.get((module.key, command.acl_key)).state,
                                "Pending",
                            )
                            if state.commands.get((module.key, command.acl_key))
                            else "Pending",
                        )
                        for command in visible
                    ),
                )
            )
        if pages > 1:
            controls.extend(self._page_buttons(session, "commands_page", page, pages))
        controls.append(self._button(session, "module", "Back"))
        return PresentationPage(description=text, controls=tuple(controls))

    def _command(
        self, session: SettingsSession, state: GuildSettingsState, notice: str | None = None
    ) -> PresentationPage:
        module = self._catalog_module(session.module)
        command = next(item for item in module.commands if item.acl_key == session.command)
        configured = state.commands.get((session.module, session.command))
        status = configured.state if configured else "pending"
        shown_status = {"enabled": "On", "disabled": "Off", "pending": "Pending"}.get(
            status, "Pending"
        )
        text = f"## .{command.root}\n{shown_status}"
        if notice:
            text += f"\n{notice}"
        label = "Disable" if status == "enabled" else "Approve" if status == "pending" else "Enable"
        controls = (
            self._button(
                session,
                "set_command",
                label,
                "danger" if status == "enabled" else "success",
                payload=f"{session.token}~{int(status != 'enabled')}",
            ),
            self._button(session, "command_access", "Access"),
            self._button(session, "commands", "Back"),
        )
        return PresentationPage(description=text, controls=controls)

    def _access(
        self,
        session: SettingsSession,
        state: GuildSettingsState,
        command: bool,
        notice: str | None = None,
    ) -> PresentationPage:
        module = self._catalog_module(session.module)
        if command:
            configured = state.commands.get((session.module, session.command))
            command_root = next(
                item.root for item in module.commands if item.acl_key == session.command
            )
            title = f".{command_root}"
            back = "command"
        else:
            configured = state.modules.get(session.module)
            title = module.display_name
            back = "module"
        roles = configured.roles if configured else ()
        channels = configured.channels if configured else ()
        categories = configured.categories if configured else ()
        channel_count = len(channels) + len(categories)
        text = (
            f"## {title}\n"
            f"Roles: {len(roles) if roles else 'Default'}\n"
            f"Channels: {channel_count if channel_count else 'All'}"
        )
        if notice:
            text += f"\n{notice}"
        controls = (
            PresentationControl(
                kind="role_select",
                namespace="settings",
                action="set_roles",
                payload=session.token,
                placeholder="Roles",
                min_values=0,
                max_values=25,
                default_ids=roles[:25],
            ),
            PresentationControl(
                kind="channel_select",
                namespace="settings",
                action="set_channels",
                payload=session.token,
                placeholder="Channels",
                min_values=0,
                max_values=25,
                default_ids=(*channels, *categories)[:25],
            ),
            self._button(session, back, "Back"),
        )
        return PresentationPage(description=text, controls=controls)

    def _catalog_module(self, key: str):
        for module in self.catalog.online_modules:
            if module.key == key:
                return module
        raise LookupError(key)

    def _screen(self, session: SettingsSession, page: PresentationPage, kind: str) -> DiscordIntent:
        return DiscordIntent(kind=kind, pages=(page,), session_id=session.token)

    def _simple(self, text: str) -> DiscordIntent:
        return DiscordIntent(kind="send", pages=(PresentationPage(description=text),))

    def _button(
        self,
        session: SettingsSession,
        action: str,
        label: str,
        style: str = "secondary",
        *,
        payload: str | None = None,
        disabled: bool = False,
    ) -> PresentationControl:
        return PresentationControl(
            kind="button",
            namespace="settings",
            action=action,
            payload=payload or session.token,
            label=label,
            style=style,
            disabled=disabled,
        )

    def _select(
        self,
        session: SettingsSession,
        action: str,
        placeholder: str,
        options: tuple[PresentationOption, ...],
    ) -> PresentationControl:
        return PresentationControl(
            kind="string_select",
            namespace="settings",
            action=action,
            payload=session.token,
            placeholder=placeholder,
            options=options,
        )

    def _page_buttons(
        self, session: SettingsSession, action: str, page: int, pages: int
    ) -> tuple[PresentationControl, ...]:
        return (
            self._button(
                session,
                action,
                "Previous",
                payload=f"{session.token}~{max(0, page - 1)}",
                disabled=page == 0,
            ),
            PresentationControl(
                kind="button",
                namespace="settings",
                action="noop",
                payload=session.token,
                label=f"{page + 1}/{pages}",
                disabled=True,
            ),
            self._button(
                session,
                action,
                "Next",
                payload=f"{session.token}~{min(pages - 1, page + 1)}",
                disabled=page == pages - 1,
            ),
        )

    async def _save_session(self, session: SettingsSession) -> None:
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.setex(f"lumi:settings:session:{session.token}", self.SESSION_TTL, encode(session))
        pipeline.setex(ui_key("owner", session.token), self.SESSION_TTL, str(session.owner_id))
        await pipeline.execute()

    async def _load_session(self, token: str) -> SettingsSession | None:
        payload = await self._cache.get(f"lumi:settings:session:{token}")
        if not payload:
            return None
        session = decode(payload, SettingsSession)
        await self._cache.expire(f"lumi:settings:session:{token}", self.SESSION_TTL)
        await self._cache.expire(ui_key("owner", token), self.SESSION_TTL)
        return session

    def _result(self, job_id: str, intent: DiscordIntent) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="settings",
            group="platform",
            generation=self._generation,
            status="ok",
            intents=(intent,) if intent.pages else (),
            completed_at_ms=int(time.time() * 1000),
        )

    @staticmethod
    def _label(value: str) -> str:
        return value.replace("_", " ").replace("-", " ").title()

    async def close(self) -> None:
        self.catalog.stop()
        self.publisher.stop()
        tasks = [
            task
            for task in (
                getattr(self, "_catalog_task", None),
                getattr(self, "_publisher_task", None),
            )
            if task is not None
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
