from __future__ import annotations

import asyncio
import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass

import discord

from lumi.contracts import PresentationPage

from .views import LUMI_COLOUR, BoundLayoutView, ColouredContainersView, ComponentsV2Builder


def search_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def short_summary(value: str) -> str:
    first = value.strip().splitlines()[0] if value.strip() else ""
    first = re.split(r"(?<=[.!?])\s", first, maxsplit=1)[0]
    return first if len(first) <= 100 else first[:97].rsplit(" ", 1)[0] + "…"


@dataclass(frozen=True, slots=True)
class HelpEntry:
    module: str
    module_name: str
    root: str
    usage: str
    summary: str
    colour: int
    aliases: tuple[str, ...] = ()
    unavailable: bool = False
    is_command: bool = True

    def matches(self, query: str) -> bool:
        haystack = search_text(
            " ".join(
                (self.module, self.module_name, self.root, *self.aliases, self.summary, self.usage)
            )
        )
        return all(word in haystack for word in search_text(query).split())


class HelpSearchModal(discord.ui.Modal):
    def __init__(self, view: HelpView) -> None:
        super().__init__(title="Search help", timeout=900)
        self.help_view = view
        self.query = discord.ui.TextInput(max_length=80, required=False, default=view.query)
        self.add_item(discord.ui.Label(text="Command or keyword", component=self.query))

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.help_view.update(interaction, search=str(self.query))


class HelpView(BoundLayoutView):


    def __init__(
        self,
        *,
        owner_id: int,
        blocklist,
        builder: ComponentsV2Builder,
        entries: tuple[HelpEntry, ...],
        refresh: Callable[[discord.Interaction], tuple[HelpEntry, ...]],
        prefix: str,
        query: str = "",
        guild_id: int | None = None,
        channel_id: int,
    ) -> None:
        super().__init__(owner_id=owner_id, blocklist=blocklist, timeout=900)
        self.builder, self.entries, self.refresh = builder, entries, refresh
        self.prefix, self.guild_id, self.channel_id = prefix, guild_id, channel_id
        self.query, self.module, self.command, self.index = "", "", "", 0
        self._return_state = ("", "", 0)
        self._lock = asyncio.Lock()
        self._expired = False
        self._search(query)
        self._render()

    def _search(self, query: str) -> None:
        query = query.strip()[:80]
        if query.startswith(self.prefix):
            query = query[len(self.prefix) :]
        key = search_text(query)
        self.command = next((r.root for r in self.entries if key in (r.root, *r.aliases)), "")
        entry = next((r for r in self.entries if r.root == self.command), None)
        self._return_state = ("", entry.module if entry else "", 0)
        self.module = ""
        self.query = "" if self.command else query
        self.index = 0

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self._expired or not await super().interaction_check(interaction):
            return False
        if interaction.guild_id != self.guild_id or interaction.channel_id != self.channel_id:
            return False
        source = interaction.message
        return not (self.message and source and self.message.id != source.id)

    async def on_timeout(self) -> None:
        async with self._lock:
            self._expired = True
            await super().on_timeout()

    async def update(self, interaction: discord.Interaction, **change) -> None:
        async with self._lock:
            if not await self.interaction_check(interaction):
                return


            self.entries = self.refresh(interaction)
            if "search" in change:
                self._search(change["search"])
            elif "module" in change:
                self.module, self.command, self.query = change["module"], "", ""
                self.index = 0
            elif "command" in change:
                self._return_state = (self.query, self.module, self.index)
                self.command, self.query = change["command"], ""
                self.index = 0
            elif "back" in change:
                self.query, self.module, self.index = self._return_state
                self.command = ""
            else:
                self.index += change.get("page", 0)
            self._render()
            await interaction.response.edit_message(
                view=self, allowed_mentions=discord.AllowedMentions.none()
            )

    def _render(self) -> None:
        self.clear_items()
        detail = next((r for r in self.entries if r.root == self.command), None)
        if self.command:
            visible = (detail,) if detail else ()
        else:
            visible = tuple(
                r
                for r in self.entries
                if (not self.module or r.module == self.module) and r.matches(self.query)
            )
        pages: list[tuple[PresentationPage, int]] = []
        groups: dict[str, list[HelpEntry]] = {}
        for entry in visible:
            groups.setdefault(entry.module, []).append(entry)
        section_entries: dict[int, tuple[HelpEntry, ...]] = {}
        for records in groups.values():
            for start in range(0, len(records), 20):
                chunk = tuple(records[start : start + 20])
                first = chunk[0]
                description = (
                    " · ".join(f"`{self.prefix}{r.root}`" for r in chunk)
                    if all(r.is_command for r in chunk)
                    else "\n".join(
                        f"`{self.prefix}{r.root}`" if r.is_command else r.usage for r in chunk
                    )
                )
                if detail:
                    description = "\n".join(dict.fromkeys(
                        (detail.usage, short_summary(detail.summary))
                    ))
                    if detail.unavailable:
                        description += "\nUnavailable."
                page = PresentationPage(title=first.module_name, description=description)
                pages.append((page, first.colour))
                section_entries[id(page)] = chunk
        if not pages:
            pages.append((PresentationPage(title="Help", description="No results."), LUMI_COLOUR))

        packed = ColouredContainersView._pack(self, tuple(pages))
        batches = []
        for batch in packed:
            current, count = [], 0
            for page, colour in batch:
                size = len(section_entries.get(id(page), ()))
                if current and count + size > 25:
                    batches.append(tuple(current))
                    current, count = [], 0
                current.append((page, colour))
                count += size
            if current:
                batches.append(tuple(current))
        self.batches = tuple(batches)
        self.index = max(0, min(self.index, len(self.batches) - 1))
        shown = []
        for page, colour in self.batches[self.index]:
            self.add_item(self.builder.container(page, accent_color=colour))
            shown.extend(section_entries.get(id(page), ()))
        modules = dict((r.module, r.module_name) for r in self.entries)
        if len(modules) > 24:
            modules = {r.module: r.module_name for r in shown}
        selector = discord.ui.Select(
            placeholder="Module",
            options=[discord.SelectOption(label="All modules", value="__all__")]
            + [
                discord.SelectOption(label=name[:100], value=key, default=key == self.module)
                for key, name in modules.items()
            ],
        )

        async def choose_module(interaction):
            value = selector.values[0]
            await self.update(interaction, module="" if value == "__all__" else value)

        selector.callback = choose_module
        self.add_item(discord.ui.ActionRow(selector))
        if shown and not detail:
            commands = discord.ui.Select(
                placeholder="Command" if all(r.is_command for r in shown) else "Command or tool",
                options=[
                    discord.SelectOption(
                        label=(f"{self.prefix}{r.root}" if r.is_command else r.module_name)[:100],
                        value=r.root,
                        description=short_summary(r.summary) or None,
                    )
                    for r in shown
                ],
            )

            async def choose_command(interaction):
                await self.update(interaction, command=commands.values[0])

            commands.callback = choose_command
            self.add_item(discord.ui.ActionRow(commands))
        search = discord.ui.Button(label="Search", style=discord.ButtonStyle.primary)

        async def open_search(interaction):
            if await self.interaction_check(interaction):
                await interaction.response.send_modal(HelpSearchModal(self))

        search.callback = open_search
        controls = [search]
        if self.command:
            back = discord.ui.Button(label="Back")

            async def go_back(interaction):
                await self.update(interaction, back=True)

            back.callback = go_back
            controls.append(back)
        if len(self.batches) > 1:
            previous = discord.ui.Button(label="Previous", disabled=self.index == 0)
            next_button = discord.ui.Button(
                label="Next", disabled=self.index == len(self.batches) - 1
            )

            async def go_previous(interaction):
                await self.update(interaction, page=-1)

            async def go_next(interaction):
                await self.update(interaction, page=1)

            previous.callback, next_button.callback = go_previous, go_next
            controls.extend(
                (
                    previous,
                    discord.ui.Button(label=f"{self.index + 1}/{len(self.batches)}", disabled=True),
                    next_button,
                )
            )
        self.add_item(discord.ui.ActionRow(*controls))
