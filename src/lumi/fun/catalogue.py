from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files

import msgspec

from lumi.contracts import FunCatalogueSnapshot, FunCategoryDefinition

from .rules import category_name, gif_url, stable_id, url_batch


class Gif(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: str
    url: str
    canonical: str
    contributor: int
    starter: bool = False
    hidden: bool = False


class Category(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: str
    name: str
    mode: str
    creator: int
    gifs: tuple[Gif, ...] = ()
    starter: bool = False
    hidden: bool = False
    blocked: bool = False


class Catalogue(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int = 1
    revision: int = 0
    categories: tuple[Category, ...] = ()
    blocked_urls: tuple[str, ...] = ()

    def snapshot(self, guild_id: int) -> FunCatalogueSnapshot:
        return FunCatalogueSnapshot(
            1,
            guild_id,
            self.revision,
            tuple(
                FunCategoryDefinition(c.id, c.name, c.mode) for c in self.categories if not c.hidden
            ),
        )


@lru_cache(maxsize=1)
def starter() -> Catalogue:
    data = json.loads(files(__package__).joinpath("starter.json").read_text(encoding="utf-8"))
    return Catalogue(
        categories=tuple(
            Category(
                stable_id("starter:" + item["name"]),
                item["name"],
                item["mode"],
                0,
                tuple(
                    Gif(
                        stable_id(item["name"] + ":" + gif_url(url)[1]),
                        url,
                        gif_url(url)[1],
                        0,
                        starter=True,
                    )
                    for url in item["urls"]
                ),
                starter=True,
            )
            for item in data
        )
    )


@dataclass(frozen=True)
class Mutation:
    kind: str
    actor: int
    revision: int
    category: str = ""
    gif: str = ""
    name: str = ""
    mode: str = ""
    urls: str = ""


def mutate(
    state: Catalogue, change: Mutation, *, moderator: bool, conflicts: frozenset[str]
) -> tuple[Catalogue, str]:
    if change.revision != state.revision:
        return state, "Changed. Try again."
    categories = list(state.categories)
    blocked = set(state.blocked_urls)
    if change.kind == "create":
        name = category_name(change.name, conflicts)
        if change.mode not in {"solo", "targeted"}:
            raise ValueError("Choose Solo or Targeted.")
        existing = next((c for c in categories if c.name == name), None)
        if existing:
            raise ValueError("Removed by a moderator." if existing.blocked else "Category exists.")
        if sum(not c.hidden for c in categories) >= 100 or len(categories) >= 1000:
            raise ValueError("Category limit reached.")
        categories.append(
            Category(stable_id(f"local:{name}:{state.revision}"), name, change.mode, change.actor)
        )
        notice = "Added."
    else:
        index = next((i for i, c in enumerate(categories) if c.id == change.category), None)
        if index is None:
            raise ValueError("Category unavailable.")
        category = categories[index]
        gifs = list(category.gifs)
        if category.hidden and change.kind != "restore_category":
            raise ValueError("Category removed.")
        if change.kind == "add":
            values = url_batch(change.urls)
            added = duplicates = 0
            errors = []
            for position, value in enumerate(values, 1):
                try:
                    original, canonical = gif_url(value)
                    if canonical in blocked and not moderator:
                        raise ValueError("Removed by a moderator")
                    previous = next((g for g in gifs if g.canonical == canonical), None)
                    if previous and not previous.hidden:
                        duplicates += 1
                        continue
                    if sum(not g.hidden for g in gifs) >= 250 or len(gifs) >= 1000:
                        raise ValueError("GIF limit reached")
                    if previous:
                        if not moderator:
                            raise ValueError("Removed by a moderator")
                        gifs[gifs.index(previous)] = msgspec.structs.replace(previous, hidden=False)
                    else:
                        gifs.append(
                            Gif(
                                stable_id(category.id + ":" + canonical),
                                original,
                                canonical,
                                change.actor,
                            )
                        )
                    if moderator:
                        blocked.discard(canonical)
                    added += 1
                except ValueError as exc:
                    errors.append(f"{position}: {exc}")
            notice = f"Added {added} · Duplicate {duplicates}"
            if errors:
                notice += "\n" + "\n".join(errors)
            category = msgspec.structs.replace(category, gifs=tuple(gifs))
        elif change.kind in {"remove_gif", "restore_gif"}:
            gi = next((i for i, g in enumerate(gifs) if g.id == change.gif), None)
            if gi is None:
                raise ValueError("GIF unavailable.")
            gif = gifs[gi]
            if change.kind == "restore_gif":
                if not moderator or (not gif.hidden and gif.canonical not in blocked):
                    raise ValueError("Moderator only.")
                if gif.hidden and sum(not g.hidden for g in gifs) >= 250:
                    raise ValueError("GIF limit reached.")
                gifs[gi] = msgspec.structs.replace(gif, hidden=False)
                blocked.discard(gif.canonical)
                notice = "Restored."
            else:
                if not moderator and (gif.starter or gif.contributor != change.actor):
                    raise ValueError("Only your GIFs.")
                if moderator:
                    gifs[gi] = msgspec.structs.replace(gif, hidden=True)
                    blocked.add(gif.canonical)
                else:
                    gifs.pop(gi)
                notice = "Removed."
            category = msgspec.structs.replace(category, gifs=tuple(gifs))
        elif change.kind == "remove_category":
            if not moderator and (
                category.starter
                or category.creator != change.actor
                or any(not g.hidden for g in gifs)
            ):
                raise ValueError("Only your empty categories.")
            if moderator:
                category = msgspec.structs.replace(category, hidden=True, blocked=True)
            else:
                categories.pop(index)
                index = None
            notice = "Removed."
        elif change.kind == "restore_category":
            if not moderator or not category.hidden:
                raise ValueError("Moderator only.")
            category_name(category.name, conflicts)
            if sum(not c.hidden for c in categories) >= 100:
                raise ValueError("Category limit reached.")
            category = msgspec.structs.replace(category, hidden=False, blocked=False)
            notice = "Restored."
        else:
            raise ValueError("Unavailable.")
        if index is not None:
            categories[index] = category
    next_state = msgspec.structs.replace(
        state, categories=tuple(categories), blocked_urls=tuple(sorted(blocked))
    )
    if next_state != state:
        next_state = msgspec.structs.replace(next_state, revision=state.revision + 1)
    return next_state, notice
