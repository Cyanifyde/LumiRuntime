from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import re
import secrets
import socket
import time
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from weakref import WeakValueDictionary

import aiohttp
from PIL import Image, UnidentifiedImageError
from redis.exceptions import RedisError

from lumi.contracts import (
    PresentationBlock,
    PresentationControl,
    PresentationPage,
    TemporaryAttachment,
)
from lumi.contracts.role_access import RoleChange
from lumi.panels import (
    ButtonRoleAsset,
    ButtonRoleBlock,
    ButtonRoleContainer,
    ButtonRolePanel,
    MySQLPanelStore,
)

from .actions import ActionUnavailable, EdgeActionClient
from .actor_roles import ActorRoles
from .button_layout import ButtonLayout, ButtonLayoutError, ButtonLayoutPublisher

MAX_ASSET_BYTES = 8 * 1024 * 1024
MAX_IMAGE_PIXELS = 20_000_000
MAX_IMAGE_SIDE = 8192
CUSTOM_EMOJI = re.compile(r"^<a?:[A-Za-z0-9_]{2,32}:(\d{17,20})>$")
FORMAT_DETAILS = {
    "PNG": ("image/png", ".png"),
    "JPEG": ("image/jpeg", ".jpg"),
    "WEBP": ("image/webp", ".webp"),
    "GIF": ("image/gif", ".gif"),
}


class ButtonPanelError(ValueError):
    pass


class _PublicResolver(aiohttp.abc.AbstractResolver):
    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        records = await loop.getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
            family=family,
        )
        resolved: list[dict[str, Any]] = []
        for record_family, _, proto, _, address in records:
            ip = ipaddress.ip_address(address[0])
            if not ip.is_global:
                raise OSError("image host resolves to a non-public address")
            resolved.append(
                {
                    "hostname": host,
                    "host": str(ip),
                    "port": port,
                    "family": record_family,
                    "proto": proto,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        if not resolved:
            raise OSError("image host has no public address")
        return resolved

    async def close(self) -> None:
        return None


class ButtonRoleService:


    def __init__(
        self,
        *,
        store: MySQLPanelStore,
        actions: EdgeActionClient,
        temp_root: Path,
    ) -> None:
        self._store = store
        self._actions = actions
        self._publisher = ButtonLayoutPublisher(store, actions)
        self._temp_root = temp_root
        self._panel_locks: dict[str, asyncio.Lock] = {}
        self._actor_locks = WeakValueDictionary()

    async def start(self) -> None:
        self._temp_root.mkdir(parents=True, exist_ok=True)

    async def ingest_url(self, guild_id: int, url: str) -> ButtonRoleAsset:
        data, suggested = await self._download(url, None)
        return await self._store_asset(guild_id, data, suggested)

    async def ingest_upload(
        self, guild_id: int, metadata: dict[str, Any]
    ) -> ButtonRoleAsset:
        url = str(metadata.get("url") or "")
        expected_size = int(metadata.get("size") or 0)
        if expected_size <= 0 or expected_size > MAX_ASSET_BYTES:
            raise ButtonPanelError("Image is too large.")
        data, suggested = await self._download(url, expected_size)
        return await self._store_asset(
            guild_id,
            data,
            str(metadata.get("filename") or suggested),
        )

    async def reconcile(
        self,
        panel: ButtonRolePanel,
        *,
        job_id: str,
        actor_id: int,
    ) -> bool:
        lock = self._panel_locks.setdefault(panel.panel_id, asyncio.Lock())
        async with lock:
            for _ in range(3):
                current = await self._store.get_button_panel(
                    panel.guild_id, panel.panel_id
                )
                if current is None or current.lifecycle == "deleted":
                    raise ButtonPanelError("Unavailable.")
                if not current.channel_id:
                    raise ButtonPanelError("Channel required.")
                document = None
                try:
                    if await self._publisher.recover(current):
                        document, roles = await self._document(current)
                        message_ids = await self._publisher.publish(current, document, roles, job_id)
                    else:
                        message_ids = None
                except (ActionUnavailable, RedisError, TimeoutError, ButtonLayoutError):
                    message_ids = None
                finally:
                    if document is not None:
                        await self._discard_uploads(document.attachments)
                if not message_ids:
                    await self._store.set_button_panel_lifecycle(
                        current.guild_id,
                        current.panel_id,
                        "needs_repair",
                        actor_id,
                    )
                    return False
                if await self._store.apply_button_document(
                    current.guild_id,
                    current.panel_id,
                    message_ids,
                    current.desired_revision,
                    actor_id,
                    expected_channel=current.channel_id,
                ):
                    return True
            await self._store.set_button_panel_lifecycle(
                panel.guild_id, panel.panel_id, "needs_repair", actor_id
            )
            return False

    async def move_channel(
        self,
        panel: ButtonRolePanel,
        channel_id: int,
        *,
        job_id: str,
        actor_id: int,
    ) -> bool:
        lock = self._panel_locks.setdefault(panel.panel_id, asyncio.Lock())
        async with lock:
            current = await self._store.get_button_panel(
                panel.guild_id, panel.panel_id
            )
            if current is None or current.lifecycle == "deleted":
                return False
            if current.channel_id == channel_id:
                return True
            if not await self._remove_posts(current, job_id, actor_id):
                return False
            return await self._store.set_button_panel_channel(
                current.guild_id,
                current.panel_id,
                channel_id,
                actor_id,
            )

    async def delete_panel(self, panel, *, job_id, actor_id):
        lock = self._panel_locks.setdefault(panel.panel_id, asyncio.Lock())
        async with lock:
            current = await self._store.get_button_panel(panel.guild_id, panel.panel_id)
            if current is None or current.lifecycle == "deleted":
                return False
            if not await self._remove_posts(current, job_id, actor_id):
                return False
            await self._store.set_button_panel_lifecycle(
                current.guild_id, current.panel_id, "deleted", actor_id,
            )
            return True

    async def _remove_posts(self, panel, job_id, actor_id):
        try:
            if await self._publisher.recover(panel):
                for message_id in await self._store.list_button_messages(panel.panel_id):
                    if not await self._publisher.delete(panel, job_id, message_id):
                        break
                else:
                    return True
        except (ActionUnavailable, RedisError, TimeoutError):
            pass
        await self._store.set_button_panel_lifecycle(
            panel.guild_id, panel.panel_id, "needs_repair", actor_id,
        )
        return False

    async def _discard_uploads(self, attachments):
        for item in attachments:
            try:
                await asyncio.to_thread(
                    (self._temp_root / item.reference.removeprefix("tmp:")).unlink, missing_ok=True,
                )
            except OSError:

                pass

    async def document(self, panel: ButtonRolePanel) -> ButtonLayout:
        document, _ = await self._document(panel)
        return document

    async def _document(self, panel: ButtonRolePanel):
        containers = await self._store.list_button_containers(panel.panel_id)
        if not containers:
            raise ButtonPanelError("Add a container.")
        pages: list[PresentationPage] = []
        attachments: list[TemporaryAttachment] = []
        roles = {}
        for container in containers:
            source = await self._store.list_button_blocks(container.container_id)
            if not source and not container.clear_enabled:
                continue
            container_pages, files = await self._render_container(
                panel, container, max_components=40, source=source,
            )
            pages.extend(container_pages)
            attachments.extend(files)
            roles.update((block.block_id, block) for block in source if block.kind == "role")
        if not pages:
            raise ButtonPanelError("Add an item.")
        return ButtonLayout(
            pages=tuple(pages),
            message_ids=await self._store.list_button_messages(panel.panel_id),
            attachments=tuple(attachments),
        ), roles

    async def preview(
        self, panel: ButtonRolePanel, container_id: str, page_index: int
    ) -> tuple[PresentationPage, tuple[TemporaryAttachment, ...], int]:
        container = await self._store.get_button_container(panel.panel_id, container_id)
        if container is None:
            raise ButtonPanelError("Select a container.")
        pages, attachments = await self._render_container(
            panel, container, max_components=37
        )
        selected = max(0, min(page_index, len(pages) - 1))
        names = {
            block.file_name
            for block in pages[selected].blocks
            if block.kind == "media" and block.file_name
        }
        selected_attachments = tuple(
            attachment for attachment in attachments if attachment.file_name in names
        )
        for attachment in attachments:
            if attachment not in selected_attachments:
                identifier = attachment.reference.removeprefix("tmp:")
                await asyncio.to_thread(
                    (self._temp_root / identifier).unlink, missing_ok=True
                )
        return pages[selected], selected_attachments, len(pages)

    async def _render_container(
        self,
        panel: ButtonRolePanel,
        container: ButtonRoleContainer,
        *,
        max_components: int,
        source=None,
    ) -> tuple[tuple[PresentationPage, ...], tuple[TemporaryAttachment, ...]]:
        if source is None:
            source = await self._store.list_button_blocks(container.container_id)
        if not source:
            raise ButtonPanelError("Empty container.")
        pages: list[PresentationPage] = []
        blocks: list[PresentationBlock] = []
        attachments: list[TemporaryAttachment] = []
        component_count = 1
        text_count = 0
        attachment_count = 0
        attachment_bytes = 0

        def flush() -> None:
            nonlocal blocks, component_count, text_count
            nonlocal attachment_count, attachment_bytes
            if blocks:
                pages.append(PresentationPage(blocks=tuple(blocks)))
            blocks = []
            component_count = 1
            text_count = 0
            attachment_count = 0
            attachment_bytes = 0

        for item in source:
            block: PresentationBlock
            attachment: TemporaryAttachment | None = None
            block_components = 1
            block_text = 0
            block_bytes = 0
            if item.kind == "heading":
                block = PresentationBlock(
                    kind="heading",
                    text=item.text or "",
                    level=item.heading_level or 2,
                )
                block_text = len(item.text or "") + (item.heading_level or 2) + 1
            elif item.kind == "text":
                block = PresentationBlock(kind="text", text=item.text or "")
                block_text = len(item.text or "")
            elif item.kind == "separator":
                block = PresentationBlock(kind="separator")
            elif item.kind == "image":
                if not item.asset_id:
                    raise ButtonPanelError("Image needs repair.")
                asset = await self._store.get_button_asset(item.asset_id)
                if asset is None or asset.guild_id != panel.guild_id:
                    raise ButtonPanelError("Image needs repair.")
                suffix = Path(asset.file_name).suffix.casefold() or ".png"
                file_name = f"rr-{item.block_id}{suffix}"
                attachment = await self._temporary_attachment(asset.payload, file_name)
                block = PresentationBlock(kind="media", file_name=attachment.file_name)
                block_bytes = attachment.size
            elif item.kind == "role":
                if not item.role_id or not item.emoji_key:
                    raise ButtonPanelError("Role row needs repair.")
                text = f"<@&{item.role_id}>"
                if item.description:
                    text += f" - {item.description}"
                block = PresentationBlock(
                    kind="section",
                    text=text,
                    controls=(
                        PresentationControl(
                            kind="button",
                            namespace="reactionrole",
                            action="toggle_role",
                            payload=item.block_id,
                            emoji=item.emoji_key,
                        ),
                    ),
                )
                block_components = 3
                block_text = len(text)
            else:
                raise ButtonPanelError("Unsupported block.")
            if (
                component_count + block_components > max_components
                or text_count + block_text > 4000
                or attachment_count + int(attachment is not None) > 10
                or attachment_bytes + block_bytes > 25 * 1024 * 1024
            ):
                flush()
            if (
                1 + block_components > max_components
                or block_text > 4000
                or block_bytes > 25 * 1024 * 1024
            ):
                raise ButtonPanelError("Item is too large.")
            blocks.append(block)
            component_count += block_components
            text_count += block_text
            if attachment is not None:
                attachments.append(attachment)
                attachment_count += 1
                attachment_bytes += block_bytes
        if container.clear_enabled:
            clear = PresentationBlock(
                kind="action_row",
                controls=(
                    PresentationControl(
                        kind="button",
                        namespace="reactionrole",
                        action="clear_roles",
                        payload=container.container_id,
                        label="Clear",
                    ),
                ),
            )
            if component_count + 2 > max_components:
                flush()
            blocks.append(clear)
            component_count += 2
        flush()
        return tuple(pages), tuple(attachments)

    async def toggle_role(
        self,
        *,
        guild_id: int,
        message_id: int,
        actor_id: int,
        block_id: str,
        job_id: str,
    ) -> str:
        block = await self._store.get_button_block(block_id)
        if block is None or block.kind != "role" or not block.role_id:
            return "Unavailable."
        container = await self._store.get_button_container_by_id(block.container_id)
        if container is None:
            return "Unavailable."
        panel = await self._store.get_button_panel(guild_id, container.panel_id)
        if panel is None or panel.lifecycle not in {"active", "needs_repair"}:
            return "Unavailable."
        if message_id not in await self._store.list_button_messages(panel.panel_id):
            return "Unavailable."
        roles = ActorRoles(
            self._actions, job_id=job_id, guild_id=guild_id, channel_id=panel.channel_id,
            message_id=message_id, actor_id=actor_id,
        )
        async with self._actor_locks.setdefault((guild_id, actor_id), asyncio.Lock()):
            result = await roles.settled()
            if result is None:
                facts = await roles.observe()
                if facts is None:
                    return "Unavailable."
                joined, assigned = facts
                result = await roles.apply(joined, (block.role_id,), (
                    RoleChange(block.role_id, block.role_id not in assigned),
                ))
        applied = (result.data or {}).get("applied", ())
        if not result.ok or len(applied) != 1 or applied[0]["role"] != block.role_id:
            return "Unavailable."
        return "Added." if applied[0]["present"] else "Removed."

    async def clear_roles(
        self,
        *,
        guild_id: int,
        message_id: int,
        actor_id: int,
        container_id: str,
        job_id: str,
    ) -> str:
        container = await self._store.get_button_container_by_id(container_id)
        if container is None or not container.clear_enabled:
            return "Unavailable."
        panel = await self._store.get_button_panel(guild_id, container.panel_id)
        if panel is None or panel.lifecycle not in {"active", "needs_repair"}:
            return "Unavailable."
        if message_id not in await self._store.list_button_messages(panel.panel_id):
            return "Unavailable."
        role_ids = await self._store.button_roles_for_container(container_id)
        if not role_ids:
            return "Nothing to clear."
        roles = ActorRoles(
            self._actions, job_id=job_id, guild_id=guild_id, channel_id=panel.channel_id,
            message_id=message_id, actor_id=actor_id,
        )
        async with self._actor_locks.setdefault((guild_id, actor_id), asyncio.Lock()):
            result = await roles.settled()
            if result is None:
                facts = await roles.observe()
                if facts is None:
                    return "Unavailable."
                joined, assigned = facts
                result = await roles.apply(joined, role_ids, (
                    RoleChange(role, False) for role in dict.fromkeys(role_ids) if role in assigned
                ))
        if result.ok:
            return "Cleared." if result.status == "applied" else "Nothing to clear."
        return "Unavailable."

    async def _download(
        self, url: str, expected_size: int | None
    ) -> tuple[bytes, str]:
        if len(url) > 2048:
            raise ButtonPanelError("Invalid image URL.")
        try:
            parsed = urlsplit(url)
            port = parsed.port
        except ValueError as exc:
            raise ButtonPanelError("Invalid image URL.") from exc
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or port not in {None, 443}
        ):
            raise ButtonPanelError("Invalid image URL.")
        connector = aiohttp.TCPConnector(resolver=_PublicResolver(), ttl_dns_cache=0)
        timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_read=10)
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(
                    url,
                    allow_redirects=False,
                    headers={"User-Agent": "Lumi/1.0"},
                ) as response:
                    if response.status != 200:
                        raise ButtonPanelError("Image unavailable.")
                    length = int(response.headers.get("Content-Length") or 0)
                    if length > MAX_ASSET_BYTES:
                        raise ButtonPanelError("Image is too large.")
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        size += len(chunk)
                        if size > MAX_ASSET_BYTES:
                            raise ButtonPanelError("Image is too large.")
                        chunks.append(chunk)
        except ButtonPanelError:
            raise
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            raise ButtonPanelError("Image unavailable.") from exc
        data = b"".join(chunks)
        if expected_size is not None and len(data) != expected_size:
            raise ButtonPanelError("Image changed while downloading.")
        name = Path(parsed.path).name or "image"
        return data, name[:80]

    async def _store_asset(
        self, guild_id: int, data: bytes, suggested_name: str
    ) -> ButtonRoleAsset:
        if not data or len(data) > MAX_ASSET_BYTES:
            raise ButtonPanelError("Image is too large.")
        try:
            with Image.open(BytesIO(data)) as image:
                image_format = str(image.format or "").upper()
                width, height = image.size
                if (
                    width <= 0
                    or height <= 0
                    or width > MAX_IMAGE_SIDE
                    or height > MAX_IMAGE_SIDE
                    or width * height > MAX_IMAGE_PIXELS
                ):
                    raise ButtonPanelError("Invalid image.")
                image.verify()
        except ButtonPanelError:
            raise
        except (Image.DecompressionBombError, UnidentifiedImageError, OSError, ValueError) as exc:
            raise ButtonPanelError("Invalid image.") from exc
        if (
            image_format not in FORMAT_DETAILS
        ):
            raise ButtonPanelError("Invalid image.")
        media_type, suffix = FORMAT_DETAILS[image_format]
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(suggested_name).stem).strip("-.")
        file_name = f"{(stem or 'image')[:80]}{suffix}"
        digest = hashlib.sha256(data).hexdigest()
        return await self._store.store_button_asset(
            ButtonRoleAsset(
                asset_id=secrets.token_hex(16),
                guild_id=guild_id,
                sha256=digest,
                file_name=file_name,
                media_type=media_type,
                width=width,
                height=height,
                payload=data,
            )
        )

    async def _temporary_attachment(
        self, data: bytes, file_name: str
    ) -> TemporaryAttachment:
        identifier = secrets.token_hex(16)
        await asyncio.to_thread((self._temp_root / identifier).write_bytes, data)
        return TemporaryAttachment(
            reference=f"tmp:{identifier}",
            file_name=file_name,
            sha256=hashlib.sha256(data).hexdigest(),
            size=len(data),
            expires_at_ms=int(time.time() * 1000) + 120_000,
        )


def custom_emoji_id(value: str) -> int | None:
    match = CUSTOM_EMOJI.fullmatch(value)
    return int(match.group(1)) if match else None


def first_file(files: object, key: str) -> dict[str, Any] | None:
    if not isinstance(files, dict):
        return None
    values = files.get(key)
    if not isinstance(values, (list, tuple)) or len(values) != 1:
        return None
    value = values[0]
    return dict(value) if isinstance(value, dict) else None


def one_selected(values: dict[str, Any], key: str) -> str:
    selected = values.get(key, ())
    if isinstance(selected, (list, tuple)) and len(selected) == 1:
        return str(selected[0])
    return ""
