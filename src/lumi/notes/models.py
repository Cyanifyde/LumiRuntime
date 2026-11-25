from __future__ import annotations

import hashlib
import unicodedata
from typing import Annotated, Literal

import msgspec


class MediaSource(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    url: Annotated[str, msgspec.Meta(max_length=2048)]
    original_url: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    filename: Annotated[str, msgspec.Meta(max_length=256)] = ""
    spoiler: bool = False
    preview: str = ""


class SavedMessage(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    guild_id: Annotated[int, msgspec.Meta(gt=0)]
    channel_id: Annotated[int, msgspec.Meta(gt=0)]
    message_id: Annotated[int, msgspec.Meta(gt=0)]
    author_id: Annotated[int, msgspec.Meta(gt=0)]
    author_name: Annotated[str, msgspec.Meta(max_length=256)]
    content: Annotated[str, msgspec.Meta(max_length=12000)]
    occurred_at_ms: Annotated[int, msgspec.Meta(gt=0)]
    guild_name: Annotated[str, msgspec.Meta(max_length=256)] = ""
    channel_name: Annotated[str, msgspec.Meta(max_length=256)] = ""
    avatar_url: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    entity_labels: tuple[tuple[str, str], ...] = ()
    media: Annotated[tuple[MediaSource, ...], msgspec.Meta(max_length=40)] = ()

    @property
    def jump_url(self) -> str:
        return f"https://discord.com/channels/{self.guild_id}/{self.channel_id}/{self.message_id}"


class ReminderMapping(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    emoji: Annotated[str, msgspec.Meta(max_length=100)]
    duration_ms: Annotated[int, msgspec.Meta(ge=0, le=365 * 86400000)] = 0


class ShortcutSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    user_id: Annotated[int, msgspec.Meta(gt=0)]
    revision: Annotated[int, msgspec.Meta(ge=0)] = 0
    notes_emoji: Annotated[str, msgspec.Meta(max_length=100)] = ""
    reminders: Annotated[tuple[ReminderMapping, ...], msgspec.Meta(max_length=25)] = ()
    schema: Literal[1] = 1


class ReactionCapture(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    activation: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")]
    emoji: Annotated[str, msgspec.Meta(max_length=100)]
    revision: Annotated[int, msgspec.Meta(ge=0)]
    received_at_ms: Annotated[int, msgspec.Meta(gt=0)]
    message: SavedMessage
    duration_ms: Annotated[int, msgspec.Meta(ge=0, le=365 * 86400000)] = 0


def emoji_key(value: str) -> str:

    from lumi.shared.emoji import canonical_emoji as canonical

    return canonical(value)


def valid_emoji(value: str) -> str:
    import re

    import emoji

    value = value.strip()
    if re.fullmatch(r"<a?:[A-Za-z0-9_]{1,32}:[0-9]{17,20}>", value):
        return value
    if len(value) <= 100 and emoji.is_emoji(value):
        return unicodedata.normalize("NFC", value)
    raise ValueError("Choose an emoji.")


def source_key(kind: str, user: int, guild: int, message: int) -> str:
    return hashlib.sha256(f"{kind}:{user}:{guild}:{message}".encode()).hexdigest()[:32]


def search_text(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()
