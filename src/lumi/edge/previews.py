from __future__ import annotations

import re
import time

import discord
import msgspec

from lumi.contracts import ComponentSpec, Manifest, ModalSpec

from .validation import validate_manifest


class PreviewForm(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    owner_id: int
    guild_id: int
    channel_id: int
    message_id: int
    expires_at_ms: int
    modal: ModalSpec


class PreviewForms:
    def __init__(self, cache, layouts, *, owner_id: int, clock=None) -> None:
        self.cache, self.layouts, self.owner_id = cache, layouts, owner_id
        self.clock = clock or (lambda: int(time.time() * 1000))

    async def handle(self, interaction, action: str, payload: str) -> None:
        async def notice(text):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    view=self.layouts.text(text),
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

        match = re.fullmatch(r"([a-f0-9]{32})(?:~([0-9]{17,20}))?", payload)
        if match is None or action not in {"open", "submit"}:
            return
        nonce, opening = match.groups()
        if interaction.user.id != self.owner_id:
            await notice("This preview is not yours.")
            return
        key = f"lumi:preview-form:{nonce}"
        try:
            raw = await self.cache.get(key)
            if not raw or len(raw) > 32_768:
                await notice("This preview has expired.")
                return
            form = msgspec.json.decode(raw, type=PreviewForm)
            now = self.clock()
            if (
                form.owner_id != self.owner_id
                or form.guild_id != interaction.guild_id
                or form.channel_id != interaction.channel_id
                or not now < form.expires_at_ms <= now + 86_400_000
                or (interaction.message is not None and interaction.message.id != form.message_id)
            ):
                await notice("This preview is unavailable.")
                return
            if action == "submit":
                if not opening or not await self.cache.get(f"{key}:opened:{opening}"):
                    await notice("This preview has expired.")
                    return

                await notice("Preview only—nothing saved.")
                return
            if opening or interaction.message is None:
                return
            validate_manifest(
                Manifest(
                    2,
                    "ui_preview",
                    "platform",
                    1,
                    "lumi:jobs:platform",
                    display_name="UI Preview",
                    summary="Read-only owner preview; not a public module.",
                    components=(
                        ComponentSpec(
                            "preview",
                            "open",
                            "preview.open",
                            input_kind="modal",
                            modal=form.modal,
                        ),
                    ),
                )
            )
            await self.cache.set(
                f"{key}:opened:{interaction.id}",
                "1",
                px=min(900_000, form.expires_at_ms - now),
            )
            await interaction.response.send_modal(
                self.layouts.modal(
                    custom_id=f"lumi:preview:submit:{nonce}~{interaction.id}",
                    specification=form.modal,
                )
            )
        except Exception:

            await notice("Preview unavailable.")
