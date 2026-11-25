from __future__ import annotations

import secrets
from dataclasses import dataclass

from lumi.contracts import DiscordIntent, PresentationPage, TemporaryAttachment
from lumi.contracts.layout_message import (
    MAX_LAYOUT_COMPONENTS,
    WRITE_LAYOUT_MESSAGE,
    ordered_component_count,
)
from lumi.panels.store import ButtonPanelCreation


@dataclass(frozen=True, slots=True)
class ButtonLayout:
    pages: tuple[PresentationPage, ...]
    message_ids: tuple[int, ...]
    attachments: tuple[TemporaryAttachment, ...]


@dataclass(slots=True)
class LayoutBatch:
    pages: tuple[PresentationPage, ...]
    files: tuple[TemporaryAttachment, ...]
    roles: tuple[int, ...]
    emojis: tuple[str, ...]


class ButtonLayoutError(ValueError):
    pass


def pack_document(document, role_blocks):

    if (
        not 1 <= len(document.pages) <= 100 or len(document.attachments) > 100
        or len(set(document.message_ids)) != len(document.message_ids)
    ):
        raise ButtonLayoutError("Invalid panel layout.")
    uploads = {item.file_name: item for item in document.attachments}
    if len(uploads) != len(document.attachments):
        raise ButtonLayoutError("Duplicate panel attachment.")
    batches = []
    pages, files, roles, emojis = [], [], [], []
    components = size = 0
    for page in document.pages:
        page_files = []
        page_components = ordered_component_count(page.blocks)
        page_roles, page_emojis = [], []
        for block in page.blocks:
            if block.file_name:
                item = uploads.pop(block.file_name, None)
                if item is None:
                    raise ButtonLayoutError("Missing panel attachment.")
                page_files.append(item)
            if block.kind == "section":
                role = role_blocks.get(block.controls[0].payload)
                if role is None or not role.role_id or not role.emoji_key:
                    raise ButtonLayoutError("Role row needs repair.")
                page_roles.append(role.role_id)
                page_emojis.append(role.emoji_key)
        page_size = sum(item.size for item in page_files)
        if (
            page_components > MAX_LAYOUT_COMPONENTS or len(page_files) > 10
            or page_size > 25 * 1024 * 1024
        ):
            raise ButtonLayoutError("Panel layout is too large.")
        if pages and (
            components + page_components > MAX_LAYOUT_COMPONENTS
            or len(files) + len(page_files) > 10
            or size + page_size > 25 * 1024 * 1024
        ):
            batches.append(LayoutBatch(tuple(pages), tuple(files), tuple(roles), tuple(emojis)))
            pages, files, roles, emojis = [], [], [], []
            components = size = 0
        pages.append(page)
        files.extend(page_files)
        roles.extend(page_roles)
        emojis.extend(page_emojis)
        components += page_components
        size += page_size
    if uploads:
        raise ButtonLayoutError("Unused panel attachment.")
    batches.append(LayoutBatch(tuple(pages), tuple(files), tuple(roles), tuple(emojis)))
    return tuple(batches)


def new_attempt():
    return "layout-write:" + secrets.token_hex(16)


class ButtonLayoutPublisher:
    def __init__(self, store, actions):
        self.store, self.actions = store, actions

    async def recover(self, panel):
        entry = await self.store.pending_button_creation(panel)
        if entry is None:
            return True
        result = await self.actions.read_layout_receipt(entry.attempt)


        settled, _ = await self._record_creation(panel, entry, result)
        return settled

    @staticmethod
    def _matches(result, panel, attempt, parent=None):
        if result is None:
            return None
        reply = result.data.get("delivery")
        if not isinstance(reply, dict) or (
            reply.get("guild_id"), reply.get("channel_id"),
            reply.get("parent_id"), reply.get("attempt"),
        ) != (panel.guild_id, panel.channel_id, parent, attempt):
            return None
        return reply

    async def _record_creation(self, panel, entry, result, *, live=False):
        if entry.channel != panel.channel_id:
            return False, None
        receipt = self._matches(result, panel, entry.attempt)
        if receipt is None:


            if (
                live and result is not None and not result.ok and not result.data
                and result.error_code in {
                    "invalid_action", "unknown_job", "expired_job", "action_scope_mismatch",
                    "stale_generation", "capability_denied", "policy_denied", "blocked",
                    "guild_unavailable", "write_journal_unavailable",
                }
            ):
                return await self.store.settle_button_creation(panel, entry, None), None
            return False, None
        message_id = receipt.get("message_id")
        if (
            result.ok and receipt.get("outcome") == "sent"
            and type(message_id) is int and 0 < message_id < 2**64
        ):
            return await self.store.settle_button_creation(panel, entry, message_id), message_id
        if not result.ok and receipt.get("outcome") == "rejected":
            return await self.store.settle_button_creation(panel, entry, None), None
        return False, None

    async def publish(self, panel, document, role_blocks, job_id):
        batches = pack_document(document, role_blocks)
        retained = []
        for position, batch in enumerate(batches):
            previous = document.message_ids[position] if position < len(document.message_ids) else 0
            if previous:
                attempt = new_attempt()
                result = await self.write(panel, job_id, attempt, previous, batch=batch)
                reply = self._matches(result, panel, attempt, previous)
                if (
                    result.ok and reply and reply.get("outcome") == "edited"
                    and reply.get("message_id") == previous
                ):
                    retained.append(previous)
                    continue
                if (
                    not reply or reply.get("outcome") != "missing"
                    or result.error_code != "message_not_found"
                ):
                    return None
            entry = ButtonPanelCreation(
                panel.channel_id, panel.desired_revision, position, previous, new_attempt(),
            )
            if not await self.store.reserve_button_creation(panel, entry):
                return None
            result = await self.write(panel, job_id, entry.attempt, batch=batch)
            settled, message_id = await self._record_creation(panel, entry, result, live=True)
            if not settled or message_id is None:
                return None
            retained.append(message_id)
        for obsolete in document.message_ids[len(batches):]:
            if not await self.delete(panel, job_id, obsolete):
                return None
        return tuple(retained)

    async def delete(self, panel, job_id, message_id):
        attempt = new_attempt()
        result = await self.write(panel, job_id, attempt, message_id)
        reply = self._matches(result, panel, attempt, message_id)
        return bool(result.ok and reply and reply.get("outcome") == "deleted"
                    and reply.get("message_id") == message_id)

    async def write(self, panel, job_id, attempt, message_id=None, *, batch=None):
        intent = DiscordIntent(
            "edit" if message_id else "send", pages=batch.pages, attachments=batch.files,
            presentation_mode="v2",
        ) if batch else None
        return await self.actions.request(
            job_id=job_id, kind=WRITE_LAYOUT_MESSAGE,
            guild_id=panel.guild_id, channel_id=panel.channel_id, message_id=message_id,
            idempotency_key=attempt, intent=intent, persistent=True,
            role_ids=batch.roles if batch else (), emoji_keys=batch.emojis if batch else (),
            wait_seconds=30,
        )
