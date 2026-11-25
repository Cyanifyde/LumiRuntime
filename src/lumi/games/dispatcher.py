from __future__ import annotations

import re

from lumi.contracts import DiscordIntent, PresentationPage

from .kguess import GameError


class GamesDispatcher:
    def __init__(self):
        self.games = {}

    def register(self, name, summary, command):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", name) or name in self.games:
            raise ValueError("invalid or duplicate game")
        self.games[name] = (summary, command)

    async def dispatch(self, request, job):
        raw = str(request.arguments.get("raw") or "").strip()
        prefix = str(request.event.get("prefix") or ".")
        if not raw or raw.lower() == "help":
            return DiscordIntent(
                "send",
                pages=(
                    PresentationPage(
                        description="\n".join(
                            f"`{prefix}games {name}` · {summary}"
                            for name, (summary, _) in self.games.items()
                        )
                    ),
                ),
            )
        name, _, tail = raw.partition(" ")
        game = self.games.get(name.lower())
        if game is None:
            raise GameError("Unknown game.")
        return await game[1](tail.strip(), request, job)
