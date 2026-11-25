from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import discord

from lumi.edge.colours import LUMI_COLOUR
from lumi.contracts import ModalSpec, PresentationBlock, PresentationControl, PresentationPage

from .blocking import GlobalBlocklist
from .registry import RegistrySnapshot

MAX_V2_COMPONENTS = 40
INLINE_MEDIA_SUFFIXES = frozenset({"gif", "jpeg", "jpg", "png", "webp"})
InteractionGuard = Callable[[discord.Interaction], Awaitable[bool]]


class InvalidPresentation(ValueError):
    pass


class _ModalStringSelect(discord.ui.Select):
    def to_component_dict(self) -> dict[str, Any]:
        payload = super().to_component_dict()
        payload.pop("disabled", None)
        return payload


class _ModalRoleSelect(discord.ui.RoleSelect):
    def to_component_dict(self) -> dict[str, Any]:
        payload = super().to_component_dict()
        payload.pop("disabled", None)
        return payload


class _ModalUserSelect(discord.ui.UserSelect):
    def to_component_dict(self) -> dict[str, Any]:
        payload = super().to_component_dict()
        payload.pop("disabled", None)
        return payload


class _ModalChannelSelect(discord.ui.ChannelSelect):
    def to_component_dict(self) -> dict[str, Any]:
        payload = super().to_component_dict()
        payload.pop("disabled", None)
        return payload


def _discord_channel_types(values: tuple[int, ...]) -> list[discord.ChannelType]:
    selected = values or (0, 2, 4, 5, 10, 11, 12, 13, 15, 16)
    return [discord.ChannelType(value) for value in selected]


class ComponentsV2Builder:


    def __init__(self, icon_url: Callable[[], str | None]) -> None:
        self._icon_url = icon_url

    def build(
        self,
        page: PresentationPage,
        *,
        accent_color: int = LUMI_COLOUR,
        module: str | None = None,
        registry: RegistrySnapshot | None = None,
        owner_id: int | None = None,
        blocklist: GlobalBlocklist | None = None,
        interaction_guard: InteractionGuard | None = None,
        timeout: float | None = 900,
    ) -> BoundLayoutView:
        view = BoundLayoutView(
            owner_id=owner_id,
            blocklist=blocklist,
            interaction_guard=interaction_guard,
            timeout=timeout,
        )
        view.add_item(
            self.container(
                page,
                accent_color=accent_color,
                module=module,
                registry=registry,
            )
        )
        if view.total_children_count > MAX_V2_COMPONENTS:
            raise InvalidPresentation("Components V2 payload exceeds 40 components")
        self._delegate_worker_controls(view)
        return view

    def build_many(
        self,
        pages: tuple[PresentationPage, ...],
        *,
        accent_color: int = LUMI_COLOUR,
        module: str | None = None,
        registry: RegistrySnapshot | None = None,
        owner_id: int | None = None,
        blocklist: GlobalBlocklist | None = None,
        interaction_guard: InteractionGuard | None = None,
        timeout: float | None = 900,
    ) -> BoundLayoutView:
        if not pages:
            raise InvalidPresentation("Components V2 collection is empty")
        self._validate_collection_controls(
            pages,
            module=module,
            registry=registry,
        )
        view = BoundLayoutView(
            owner_id=owner_id,
            blocklist=blocklist,
            interaction_guard=interaction_guard,
            timeout=timeout,
        )
        for page in pages:
            view.add_item(
                self.container(
                    page,
                    accent_color=accent_color,
                    module=module,
                    registry=registry,
                )
            )
        if view.total_children_count > MAX_V2_COMPONENTS:
            raise InvalidPresentation("Components V2 payload exceeds 40 components")
        self._delegate_worker_controls(view)
        return view

    @staticmethod
    def _delegate_worker_controls(view) -> None:


        view.routed_custom_ids = frozenset(
            item.custom_id for item in view.walk_children()
            if getattr(item, "custom_id", None)
        )

    def _validate_collection_controls(
        self,
        pages: tuple[PresentationPage, ...],
        *,
        module: str | None,
        registry: RegistrySnapshot | None,
    ) -> None:
        identifiers: set[str] = set()
        for page in pages:
            controls = list(page.controls)
            controls.extend(control for block in page.blocks for control in block.controls)
            for control in controls:
                self._validate_control(control, module=module, registry=registry)
                custom_id = f"lumi:{control.namespace}:{control.action}:{control.payload}"
                if custom_id in identifiers:
                    raise InvalidPresentation("control identifier is duplicated")
                identifiers.add(custom_id)

    def text(
        self,
        description: str,
        *,
        accent_color: int = LUMI_COLOUR,
        owner_id: int | None = None,
        blocklist: GlobalBlocklist | None = None,
    ) -> BoundLayoutView:
        return self.build(
            PresentationPage(description=description),
            accent_color=accent_color,
            owner_id=owner_id,
            blocklist=blocklist,
        )

    @staticmethod
    def legacy_embed(
        page: PresentationPage,
        *,
        accent_color: int = LUMI_COLOUR,
    ) -> discord.Embed:

        if not 0 < accent_color <= 0xFFFFFF:
            raise InvalidPresentation("invalid accent colour")
        timestamp = (
            datetime.fromtimestamp(page.timestamp_ms / 1000, tz=UTC)
            if page.timestamp_ms is not None
            else None
        )
        embed = discord.Embed(
            title=page.title,
            url=page.url if page.title else None,
            description=page.description or None,
            colour=accent_color,
            timestamp=timestamp,
        )
        if page.author_name:
            embed.set_author(name=page.author_name, icon_url=page.author_icon_url)
        for field in page.fields:
            embed.add_field(name=field.name, value=field.value, inline=field.inline)
        if page.footer:
            embed.set_footer(text=page.footer)
        image_url = f"attachment://{page.file_name}" if page.file_name else page.image_url
        if image_url:
            embed.set_image(url=image_url)
        if page.thumbnail_url:
            embed.set_thumbnail(url=page.thumbnail_url)
        return embed

    def legacy_embeds(
        self,
        pages: tuple[PresentationPage, ...],
        *,
        accent_color: int = LUMI_COLOUR,
    ) -> list[discord.Embed]:
        return [self.legacy_embed(page, accent_color=accent_color) for page in pages]

    def legacy_controls(
        self,
        controls: tuple[PresentationControl, ...],
        *,
        module: str,
        registry: RegistrySnapshot,
        owner_id: int | None,
        blocklist: GlobalBlocklist,
        interaction_guard: InteractionGuard | None = None,
        timeout: float | None = 900,
    ) -> BoundLegacyView:
        view = BoundLegacyView(
            owner_id=owner_id,
            blocklist=blocklist,
            interaction_guard=interaction_guard,
            timeout=timeout,
        )
        custom_ids: set[str] = set()
        for control in controls:
            self._validate_control(control, module=module, registry=registry)
            if owner_id is None:
                registered = registry.components[(control.namespace, control.action)]
                if not registered.spec.public or registered.spec.session_bound:
                    raise InvalidPresentation("public controls must use declared public routes")
            custom_id = f"lumi:{control.namespace}:{control.action}:{control.payload}"
            if len(custom_id) > 100:
                raise InvalidPresentation("control identifier exceeds Discord's limit")
            if custom_id in custom_ids:
                raise InvalidPresentation("control identifier is duplicated")
            custom_ids.add(custom_id)
            if control.kind == "button":
                item: discord.ui.Item[Any] = discord.ui.Button(
                    label=control.label,
                    emoji=control.emoji,
                    style=self._button_style(control.style),
                    custom_id=custom_id,
                    disabled=control.disabled,
                )
            elif control.kind == "string_select":
                item = discord.ui.Select(
                    custom_id=custom_id,
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    options=[
                        discord.SelectOption(
                            label=option.label,
                            value=option.value,
                            description=option.description,
                            emoji=option.emoji,
                            default=option.default,
                        )
                        for option in control.options
                    ],
                    disabled=control.disabled,
                )
            elif control.kind == "user_select":
                item = discord.ui.UserSelect(
                    custom_id=custom_id,
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier, type=discord.SelectDefaultValueType.user
                        )
                        for identifier in control.default_ids
                    ],
                    disabled=control.disabled,
                )
            elif control.kind == "role_select":
                item = discord.ui.RoleSelect(
                    custom_id=custom_id,
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier, type=discord.SelectDefaultValueType.role
                        )
                        for identifier in control.default_ids
                    ],
                    disabled=control.disabled,
                )
            elif control.kind == "channel_select":
                item = discord.ui.ChannelSelect(
                    custom_id=custom_id,
                    channel_types=self._channel_types(control.channel_types),
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier, type=discord.SelectDefaultValueType.channel
                        )
                        for identifier in control.default_ids
                    ],
                    disabled=control.disabled,
                )
            else:
                raise InvalidPresentation("unsupported control kind")
            try:
                view.add_item(item)
            except ValueError as exc:
                raise InvalidPresentation("legacy controls exceed Discord's layout limits") from exc
        self._delegate_worker_controls(view)
        return view

    def modal(self, *, custom_id: str, specification: ModalSpec) -> EdgeModal:
        if len(custom_id) > 100:
            raise InvalidPresentation("modal identifier exceeds Discord's limit")
        return EdgeModal(custom_id=custom_id, specification=specification)

    def container(
        self,
        page: PresentationPage,
        *,
        accent_color: int,
        module: str | None = None,
        registry: RegistrySnapshot | None = None,
    ) -> discord.ui.Container:
        if not 0 < accent_color <= 0xFFFFFF:
            raise InvalidPresentation("invalid accent colour")
        container = discord.ui.Container(accent_color=accent_color)
        if page.blocks:
            self._add_blocks(
                container,
                page.blocks,
                module=module,
                registry=registry,
            )
            return container
        body = self._body(page)
        thumbnail_url = page.thumbnail_url or page.author_icon_url
        if thumbnail_url:
            container.add_item(
                discord.ui.Section(
                    discord.ui.TextDisplay(body or "\u200b"),
                    accessory=discord.ui.Thumbnail(thumbnail_url),
                )
            )
        elif body:
            container.add_item(discord.ui.TextDisplay(body))
        for field in page.fields:
            container.add_item(discord.ui.Separator())
            container.add_item(discord.ui.TextDisplay(f"**{field.name}**\n{field.value}"))
        if page.image_url:
            container.add_item(discord.ui.MediaGallery(discord.MediaGalleryItem(page.image_url)))
        if page.file_name:
            attachment_url = f"attachment://{page.file_name}"
            suffix = page.file_name.rpartition(".")[2].lower()
            if suffix in INLINE_MEDIA_SUFFIXES:
                container.add_item(
                    discord.ui.MediaGallery(discord.MediaGalleryItem(attachment_url))
                )
            else:
                container.add_item(discord.ui.File(attachment_url))
        if page.footer:
            container.add_item(discord.ui.Separator())
            container.add_item(discord.ui.TextDisplay(f"-# {page.footer}"))
        if page.timestamp_ms is not None:
            stamp = int(datetime.fromtimestamp(page.timestamp_ms / 1000, tz=UTC).timestamp())
            container.add_item(discord.ui.TextDisplay(f"-# <t:{stamp}:f>"))
        self._add_controls(container, page.controls, module=module, registry=registry)
        return container

    def _body(self, page: PresentationPage) -> str:
        parts: list[str] = []
        if page.author_name:
            parts.append(f"**{page.author_name}**")
        if page.title:
            title = f"[{page.title}]({page.url})" if page.url else page.title
            parts.append(f"## {title}")
        if page.description:
            parts.append(page.description)
        return "\n".join(parts)

    def _add_controls(
        self,
        container: discord.ui.Container,
        controls: tuple[PresentationControl, ...],
        *,
        module: str | None,
        registry: RegistrySnapshot | None,
    ) -> None:
        buttons: list[discord.ui.Button[Any]] = []
        custom_ids: set[str] = set()

        def flush_buttons() -> None:
            nonlocal buttons
            if buttons:
                container.add_item(discord.ui.ActionRow(*buttons))
                buttons = []

        for control in controls:
            self._validate_control(control, module=module, registry=registry)
            custom_id = f"lumi:{control.namespace}:{control.action}:{control.payload}"
            if len(custom_id) > 100:
                raise InvalidPresentation("control identifier exceeds Discord's limit")
            if custom_id in custom_ids:
                raise InvalidPresentation("control identifier is duplicated")
            custom_ids.add(custom_id)
            if control.kind == "button":
                buttons.append(
                    discord.ui.Button(
                        label=control.label,
                        emoji=control.emoji,
                        style=self._button_style(control.style),
                        custom_id=custom_id,
                        disabled=control.disabled,
                    )
                )
                if len(buttons) == 5:
                    flush_buttons()
                continue
            flush_buttons()
            item: discord.ui.Item[Any]
            if control.kind == "string_select":
                item = discord.ui.Select(
                    custom_id=custom_id,
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    options=[
                        discord.SelectOption(
                            label=option.label,
                            value=option.value,
                            description=option.description,
                            emoji=option.emoji,
                            default=option.default,
                        )
                        for option in control.options
                    ],
                    disabled=control.disabled,
                )
            elif control.kind == "user_select":
                item = discord.ui.UserSelect(
                    custom_id=custom_id,
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier, type=discord.SelectDefaultValueType.user
                        )
                        for identifier in control.default_ids
                    ],
                    disabled=control.disabled,
                )
            elif control.kind == "role_select":
                item = discord.ui.RoleSelect(
                    custom_id=custom_id,
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier, type=discord.SelectDefaultValueType.role
                        )
                        for identifier in control.default_ids
                    ],
                    disabled=control.disabled,
                )
            elif control.kind == "channel_select":
                item = discord.ui.ChannelSelect(
                    custom_id=custom_id,
                    channel_types=self._channel_types(control.channel_types),
                    placeholder=control.placeholder,
                    min_values=control.min_values,
                    max_values=control.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier, type=discord.SelectDefaultValueType.channel
                        )
                        for identifier in control.default_ids
                    ],
                    disabled=control.disabled,
                )
            else:
                raise InvalidPresentation("unsupported control kind")
            container.add_item(discord.ui.ActionRow(item))
        flush_buttons()

    def _add_blocks(
        self,
        container: discord.ui.Container,
        blocks: tuple[PresentationBlock, ...],
        *,
        module: str | None,
        registry: RegistrySnapshot | None,
    ) -> None:
        custom_ids: set[str] = set()
        for block in blocks:
            if block.kind == "gallery":
                container.add_item(
                    discord.ui.MediaGallery(
                        *(
                            discord.MediaGalleryItem(
                                item.media_url or f"attachment://{item.file_name}",
                                description=item.description,
                                spoiler=item.spoiler,
                            )
                            for item in block.items
                        )
                    )
                )
                continue
            if block.kind == "heading":
                container.add_item(discord.ui.TextDisplay(f"{'#' * block.level} {block.text}"))
                continue
            if block.kind == "text":
                container.add_item(discord.ui.TextDisplay(block.text))
                continue
            if block.kind == "media":
                source = block.media_url or f"attachment://{block.file_name}"
                container.add_item(
                    discord.ui.MediaGallery(
                        discord.MediaGalleryItem(
                            source,
                            description=block.description,
                            spoiler=block.spoiler,
                        )
                    )
                )
                continue
            if block.kind == "file":
                container.add_item(
                    discord.ui.File(f"attachment://{block.file_name}", spoiler=block.spoiler)
                )
                continue
            if block.kind == "separator":
                spacing = (
                    discord.SeparatorSpacing.large
                    if block.spacing == "large"
                    else discord.SeparatorSpacing.small
                )
                container.add_item(discord.ui.Separator(visible=block.divider, spacing=spacing))
                continue
            if block.kind == "section":
                control = block.controls[0]
                custom_id = self._ordered_control_id(
                    control,
                    module=module,
                    registry=registry,
                    custom_ids=custom_ids,
                )
                container.add_item(
                    discord.ui.Section(
                        discord.ui.TextDisplay(block.text),
                        accessory=discord.ui.Button(
                            label=control.label,
                            emoji=control.emoji,
                            style=self._button_style(control.style),
                            custom_id=custom_id,
                            disabled=control.disabled,
                        ),
                    )
                )
                continue
            items: list[discord.ui.Item[Any]] = []
            for control in block.controls:
                custom_id = self._ordered_control_id(
                    control,
                    module=module,
                    registry=registry,
                    custom_ids=custom_ids,
                )
                if control.kind == "button":
                    item: discord.ui.Item[Any] = discord.ui.Button(
                        label=control.label,
                        emoji=control.emoji,
                        style=self._button_style(control.style),
                        custom_id=custom_id,
                        disabled=control.disabled,
                    )
                elif control.kind == "string_select":
                    item = discord.ui.Select(
                        custom_id=custom_id,
                        placeholder=control.placeholder,
                        min_values=control.min_values,
                        max_values=control.max_values,
                        options=[
                            discord.SelectOption(
                                label=option.label,
                                value=option.value,
                                description=option.description,
                                emoji=option.emoji,
                                default=option.default,
                            )
                            for option in control.options
                        ],
                        disabled=control.disabled,
                    )
                elif control.kind == "user_select":
                    item = discord.ui.UserSelect(
                        custom_id=custom_id,
                        placeholder=control.placeholder,
                        min_values=control.min_values,
                        max_values=control.max_values,
                        default_values=[
                            discord.SelectDefaultValue(
                                id=identifier,
                                type=discord.SelectDefaultValueType.user,
                            )
                            for identifier in control.default_ids
                        ],
                        disabled=control.disabled,
                    )
                elif control.kind == "role_select":
                    item = discord.ui.RoleSelect(
                        custom_id=custom_id,
                        placeholder=control.placeholder,
                        min_values=control.min_values,
                        max_values=control.max_values,
                        default_values=[
                            discord.SelectDefaultValue(
                                id=identifier,
                                type=discord.SelectDefaultValueType.role,
                            )
                            for identifier in control.default_ids
                        ],
                        disabled=control.disabled,
                    )
                elif control.kind == "channel_select":
                    item = discord.ui.ChannelSelect(
                        custom_id=custom_id,
                        channel_types=self._channel_types(control.channel_types),
                        placeholder=control.placeholder,
                        min_values=control.min_values,
                        max_values=control.max_values,
                        default_values=[
                            discord.SelectDefaultValue(
                                id=identifier,
                                type=discord.SelectDefaultValueType.channel,
                            )
                            for identifier in control.default_ids
                        ],
                        disabled=control.disabled,
                    )
                else:
                    raise InvalidPresentation("unsupported ordered control kind")
                items.append(item)
            container.add_item(discord.ui.ActionRow(*items))

    def _ordered_control_id(
        self,
        control: PresentationControl,
        *,
        module: str | None,
        registry: RegistrySnapshot | None,
        custom_ids: set[str],
    ) -> str:
        self._validate_control(control, module=module, registry=registry)
        custom_id = f"lumi:{control.namespace}:{control.action}:{control.payload}"
        if len(custom_id) > 100:
            raise InvalidPresentation("control identifier exceeds Discord's limit")
        if custom_id in custom_ids:
            raise InvalidPresentation("control identifier is duplicated")
        custom_ids.add(custom_id)
        return custom_id

    def _validate_control(
        self,
        control: PresentationControl,
        *,
        module: str | None,
        registry: RegistrySnapshot | None,
    ) -> None:
        if module is None or registry is None:
            raise InvalidPresentation("worker control lacks registry context")
        registered = registry.components.get((control.namespace, control.action))
        if registered is None or registered.module != module:
            raise InvalidPresentation("worker supplied an undeclared control")
        if registered.pattern.fullmatch(control.payload) is None:
            raise InvalidPresentation("control payload violates its declared grammar")
        expected = {
            "button": "none",
            "string_select": "string_values",
            "user_select": "user_ids",
            "role_select": "role_ids",
            "channel_select": "channel_ids",
        }.get(control.kind)
        if control.kind == "button" and registered.spec.modal is not None:
            expected = "modal"
        if expected is None or registered.spec.input_kind != expected:
            raise InvalidPresentation("control type does not match its declaration")
        if control.kind == "string_select" and not 1 <= len(control.options) <= 25:
            raise InvalidPresentation("string select requires 1 to 25 options")
        if not 0 <= control.min_values <= control.max_values <= 25:
            raise InvalidPresentation("invalid selection limits")
        if len(control.default_ids) > 25:
            raise InvalidPresentation("too many default selections")
        if tuple(control.channel_types) != tuple(registered.spec.channel_types):
            raise InvalidPresentation("channel types do not match their declaration")

    @staticmethod
    def _channel_types(values: tuple[int, ...]) -> list[discord.ChannelType]:
        return _discord_channel_types(values)

    @staticmethod
    def _button_style(value: str) -> discord.ButtonStyle:
        styles = {
            "primary": discord.ButtonStyle.primary,
            "secondary": discord.ButtonStyle.secondary,
            "success": discord.ButtonStyle.success,
            "danger": discord.ButtonStyle.danger,
        }
        try:
            return styles[value]
        except KeyError as exc:
            raise InvalidPresentation("invalid button style") from exc


class EdgeModal(discord.ui.Modal):
    def __init__(self, *, custom_id: str, specification: ModalSpec) -> None:
        super().__init__(title=specification.title, custom_id=custom_id, timeout=900)
        for field in specification.fields:
            if field.kind in {"text", "emoji"}:
                component: discord.ui.Item[Any] = discord.ui.TextInput(
                    custom_id=field.key,
                    style=(
                        discord.TextStyle.paragraph
                        if field.style == "paragraph"
                        else discord.TextStyle.short
                    ),
                    required=field.required,
                    min_length=field.minimum or None,
                    max_length=field.maximum,
                    placeholder=field.placeholder,
                    default=field.value,
                )
            elif field.kind == "role_select":
                component = _ModalRoleSelect(
                    custom_id=field.key,
                    min_values=field.min_values,
                    max_values=field.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier,
                            type=discord.SelectDefaultValueType.role,
                        )
                        for identifier in field.default_ids
                    ],
                    required=field.required,
                )
            elif field.kind == "user_select":
                component = _ModalUserSelect(
                    custom_id=field.key,
                    min_values=field.min_values,
                    max_values=field.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier,
                            type=discord.SelectDefaultValueType.user,
                        )
                        for identifier in field.default_ids
                    ],
                    required=field.required,
                )
            elif field.kind == "channel_select":
                component = _ModalChannelSelect(
                    custom_id=field.key,
                    channel_types=_discord_channel_types(field.channel_types),
                    min_values=field.min_values,
                    max_values=field.max_values,
                    default_values=[
                        discord.SelectDefaultValue(
                            id=identifier,
                            type=discord.SelectDefaultValueType.channel,
                        )
                        for identifier in field.default_ids
                    ],
                    required=field.required,
                )
            elif field.kind == "file_upload":
                component = discord.ui.FileUpload(
                    custom_id=field.key,
                    required=field.required,
                    min_values=field.min_values,
                    max_values=field.max_values,
                )
            elif field.kind == "string_select":
                component = _ModalStringSelect(
                    custom_id=field.key,
                    min_values=field.min_values,
                    max_values=field.max_values,
                    required=field.required,
                    options=[
                        discord.SelectOption(
                            label=choice.label,
                            value=choice.value,
                            description=choice.description,
                            default=choice.value == field.value,
                        )
                        for choice in field.choices
                    ],
                )
            else:
                raise InvalidPresentation("unsupported modal field kind")
            label_kwargs: dict[str, Any] = {"text": field.label, "component": component}
            self.add_item(discord.ui.Label(**label_kwargs))


class BoundLayoutView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        owner_id: int | None,
        blocklist: GlobalBlocklist | None,
        interaction_guard: InteractionGuard | None = None,
        timeout: float | None = 900,
    ) -> None:
        super().__init__(timeout=timeout)
        self.owner_id = owner_id
        self.blocklist = blocklist
        self.interaction_guard = interaction_guard
        self.message: discord.Message | None = None
        self.routed_custom_ids: frozenset[str] = frozenset()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if (getattr(interaction, "data", None) or {}).get("custom_id") in self.routed_custom_ids:
            return False
        if self.blocklist and self.blocklist.contains(interaction.user.id):
            return False
        if self.owner_id is not None and interaction.user.id != self.owner_id:
            return False
        if self.interaction_guard is not None:
            return await self.interaction_guard(interaction)
        return True

    async def on_timeout(self) -> None:
        for child in self.walk_children():
            if hasattr(child, "disabled"):
                child.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class BoundLegacyView(discord.ui.View):
    def __init__(
        self,
        *,
        owner_id: int | None,
        blocklist: GlobalBlocklist | None,
        interaction_guard: InteractionGuard | None = None,
        timeout: float | None = 900,
    ) -> None:
        super().__init__(timeout=timeout)
        self.owner_id = owner_id
        self.blocklist = blocklist
        self.interaction_guard = interaction_guard
        self.message: discord.Message | None = None
        self.routed_custom_ids: frozenset[str] = frozenset()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if (getattr(interaction, "data", None) or {}).get("custom_id") in self.routed_custom_ids:
            return False
        if self.blocklist and self.blocklist.contains(interaction.user.id):
            return False
        if self.owner_id is not None and interaction.user.id != self.owner_id:
            return False
        if self.interaction_guard is not None:
            return await self.interaction_guard(interaction)
        return True

    async def on_timeout(self) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class ComponentsV2Paginator(BoundLayoutView):
    def __init__(
        self,
        *,
        owner_id: int,
        blocklist: GlobalBlocklist,
        builder: ComponentsV2Builder,
        pages: tuple[PresentationPage, ...],
        accent_color: int,
        accent_colors: tuple[int, ...] | None = None,
        module: str | None = None,
        registry: RegistrySnapshot | None = None,
        interaction_guard: InteractionGuard | None = None,
        timeout: float | None = 900,
    ) -> None:
        super().__init__(
            owner_id=owner_id,
            blocklist=blocklist,
            interaction_guard=interaction_guard,
            timeout=timeout,
        )
        self.builder = builder
        self.pages = pages
        self.accent_color = accent_color
        self.accent_colors = accent_colors
        self.module = module
        self.registry = registry
        self.index = 0
        self._render()

    def _render(self) -> None:
        self.clear_items()
        container = self.builder.container(
            self.pages[self.index],
            accent_color=(
                self.accent_colors[self.index]
                if self.accent_colors is not None
                else self.accent_color
            ),
            module=self.module,
            registry=self.registry,
        )
        previous = discord.ui.Button(
            label="Previous", style=discord.ButtonStyle.secondary, disabled=self.index == 0
        )
        indicator = discord.ui.Button(
            label=f"{self.index + 1}/{len(self.pages)}",
            style=discord.ButtonStyle.secondary,
            disabled=True,
        )
        next_button = discord.ui.Button(
            label="Next",
            style=discord.ButtonStyle.secondary,
            disabled=self.index == len(self.pages) - 1,
        )

        async def go_previous(interaction: discord.Interaction) -> None:
            self.index -= 1
            self._render()
            await interaction.response.edit_message(view=self)

        async def go_next(interaction: discord.Interaction) -> None:
            self.index += 1
            self._render()
            await interaction.response.edit_message(view=self)

        previous.callback = go_previous
        next_button.callback = go_next
        container.add_item(discord.ui.ActionRow(previous, indicator, next_button))
        self.add_item(container)
        if self.total_children_count > MAX_V2_COMPONENTS:
            raise InvalidPresentation("Components V2 payload exceeds 40 components")


class LegacyEmbedPaginator(BoundLegacyView):
    def __init__(
        self,
        *,
        owner_id: int,
        blocklist: GlobalBlocklist,
        builder: ComponentsV2Builder,
        pages: tuple[PresentationPage, ...],
        accent_color: int,
        interaction_guard: InteractionGuard | None = None,
        timeout: float | None = 900,
    ) -> None:
        super().__init__(
            owner_id=owner_id,
            blocklist=blocklist,
            interaction_guard=interaction_guard,
            timeout=timeout,
        )
        self.builder = builder
        self.pages = pages
        self.accent_color = accent_color
        self.index = 0
        self._render()

    @property
    def embed(self) -> discord.Embed:
        return self.builder.legacy_embed(self.pages[self.index], accent_color=self.accent_color)

    def _render(self) -> None:
        self.clear_items()
        previous = discord.ui.Button(
            label="Previous",
            style=discord.ButtonStyle.secondary,
            disabled=self.index == 0,
        )
        indicator = discord.ui.Button(
            label=f"{self.index + 1}/{len(self.pages)}",
            style=discord.ButtonStyle.secondary,
            disabled=True,
        )
        next_button = discord.ui.Button(
            label="Next",
            style=discord.ButtonStyle.secondary,
            disabled=self.index == len(self.pages) - 1,
        )

        async def go_previous(interaction: discord.Interaction) -> None:
            self.index -= 1
            self._render()
            await interaction.response.edit_message(embeds=[self.embed], view=self)

        async def go_next(interaction: discord.Interaction) -> None:
            self.index += 1
            self._render()
            await interaction.response.edit_message(embeds=[self.embed], view=self)

        previous.callback = go_previous
        next_button.callback = go_next
        self.add_item(previous)
        self.add_item(indicator)
        self.add_item(next_button)


class ColouredContainersView(BoundLayoutView):


    def __init__(
        self,
        *,
        owner_id: int,
        blocklist: GlobalBlocklist,
        builder: ComponentsV2Builder,
        pages: tuple[tuple[PresentationPage, int], ...],
        timeout: float | None = 900,
    ) -> None:
        super().__init__(owner_id=owner_id, blocklist=blocklist, timeout=timeout)
        self.builder = builder
        self.batches = self._pack(pages)
        self.index = 0
        self._render()

    def _pack(
        self, pages: tuple[tuple[PresentationPage, int], ...]
    ) -> tuple[tuple[tuple[PresentationPage, int], ...], ...]:
        batches: list[list[tuple[PresentationPage, int]]] = []
        current: list[tuple[PresentationPage, int]] = []
        count = 0
        for page, colour in pages:
            container = self.builder.container(page, accent_color=colour)
            item_count = 1 + sum(1 for _ in container.walk_children())
            if current and (len(current) >= 8 or count + item_count > 35):
                batches.append(current)
                current = []
                count = 0
            current.append((page, colour))
            count += item_count
        if current:
            batches.append(current)
        return tuple(tuple(batch) for batch in batches)

    def _render(self) -> None:
        self.clear_items()
        for page, colour in self.batches[self.index]:
            self.add_item(self.builder.container(page, accent_color=colour))
        if len(self.batches) > 1:
            previous = discord.ui.Button(
                label="Previous",
                style=discord.ButtonStyle.secondary,
                disabled=self.index == 0,
            )
            indicator = discord.ui.Button(
                label=f"{self.index + 1}/{len(self.batches)}",
                style=discord.ButtonStyle.secondary,
                disabled=True,
            )
            next_button = discord.ui.Button(
                label="Next",
                style=discord.ButtonStyle.secondary,
                disabled=self.index == len(self.batches) - 1,
            )

            async def go_previous(interaction: discord.Interaction) -> None:
                self.index -= 1
                self._render()
                await interaction.response.edit_message(view=self)

            async def go_next(interaction: discord.Interaction) -> None:
                self.index += 1
                self._render()
                await interaction.response.edit_message(view=self)

            previous.callback = go_previous
            next_button.callback = go_next
            self.add_item(discord.ui.ActionRow(previous, indicator, next_button))
        if self.total_children_count > MAX_V2_COMPONENTS:
            raise InvalidPresentation("Components V2 payload exceeds 40 components")


def lumi_page(invite_url: str) -> PresentationPage:
    return PresentationPage(description=f"Lumi our overlord\n\n[Invite Lumi]({invite_url})")
