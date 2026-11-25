from __future__ import annotations

import re
from typing import Annotated, Literal

import msgspec

from lumi.shared.discord_assets import attachment_url as attachment_url
from lumi.shared.discord_assets import profile_image as profile_image
from lumi.shared.discord_assets import safe_image as safe_image
from lumi.shared.discord_observations import Observation as Observation
from lumi.shared.discord_observations import identifier as identifier

TTL = 7 * 86400_000
SESSION_TTL = 900_000
MAX_FILE = 10 * 1024 * 1024
MAX_STORAGE = 1024**3
CATEGORIES = ("messages", "members", "moderation", "voice", "server")
EVENTS = {
    "messages": ("message_edit", "message_delete", "bulk_delete", "attachments", "pin", "unpin"),
    "members": ("join", "depart", "nickname", "username", "display_name", "member_roles", "avatar"),
    "moderation": (
        "warn",
        "unwarn",
        "kick",
        "ban",
        "unban",
        "timeout",
        "timeout_change",
        "untimeout",
        "purge",
        "rename",
        "rename_undo",
        "imageguard",
    ),
    "voice": ("voice_join", "voice_leave", "voice_move", "voice_state", "camera", "stream"),
    "server": (
        "channel_create",
        "channel_delete",
        "channel_update",
        "role_create",
        "role_delete",
        "role_update",
        "permissions",
        "guild_update",
        "emoji",
        "sticker",
        "invite",
        "webhook",
        "integration",
        "scheduled_event",
    ),
}
ALL_EVENTS = frozenset(x for values in EVENTS.values() for x in values)
TITLES = {
    "message_edit": "Message edited",
    "message_delete": "Message deleted",
    "bulk_delete": "Messages deleted",
    "attachments": "Attachments changed",
    "pin": "Message pinned",
    "unpin": "Message unpinned",
    "join": "Member joined",
    "depart": "Member departed",
    "nickname": "Nickname changed",
    "username": "Username changed",
    "display_name": "Display name changed",
    "member_roles": "Member roles changed",
    "avatar": "Profile image changed",
    "warn": "Warning issued",
    "unwarn": "Warning revoked",
    "kick": "Member kicked",
    "ban": "Member banned",
    "unban": "Member unbanned",
    "timeout": "Member timed out",
    "timeout_change": "Timeout changed",
    "untimeout": "Timeout removed",
    "purge": "Purge finished",
    "rename": "Rename finished",
    "rename_undo": "Undo finished",
    "imageguard": "ImageGuard action",
    "voice_join": "Joined voice",
    "voice_leave": "Left voice",
    "voice_move": "Moved voice channels",
    "voice_state": "Voice state changed",
    "camera": "Camera changed",
    "stream": "Stream changed",
    "channel_create": "Channel created",
    "channel_delete": "Channel deleted",
    "channel_update": "Channel changed",
    "role_create": "Role created",
    "role_delete": "Role deleted",
    "role_update": "Role changed",
    "permissions": "Channel permissions changed",
    "guild_update": "Server changed",
    "emoji": "Emoji changed",
    "sticker": "Sticker changed",
    "invite": "Invite changed",
    "webhook": "Webhook changed",
    "integration": "Integration changed",
    "scheduled_event": "Scheduled event changed",
}


class ModlogSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    operation: Literal["context", "configure", "query", "detail", "files", "publish"]
    record: Annotated[str, msgspec.Meta(max_length=32)] = ""
    revision: int = 0
    session: Annotated[str, msgspec.Meta(max_length=16)] = ""
    category: Annotated[str, msgspec.Meta(max_length=16)] = ""
    event: Annotated[str, msgspec.Meta(max_length=48)] = ""
    subject: int = 0
    actor: int = 0
    channel: int = 0
    hours: Literal[1, 24, 168] = 168
    page: int = 0
    setting: Literal["", "destination", "events", "excluded", "bots"] = ""
    ids: Annotated[tuple[int, ...], msgspec.Meta(max_length=25)] = ()
    events: Annotated[tuple[str, ...], msgspec.Meta(max_length=25)] = ()
    enabled: bool = False


def config(guild: int) -> dict:
    return dict(
        guild=guild,
        revision=0,
        destinations={"all": 0},
        events=sorted(ALL_EVENTS),
        excluded=[],
        bots=False,
    )


def category(kind: str) -> str:
    return next((k for k, values in EVENTS.items() if kind in values), "server")


def units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def role_changes(kind: str, changes: dict) -> dict:
    return {
        key: value
        for key, value in changes.items()
        if not (kind.startswith("role_") and key.casefold() == "position")
    }


def clip(text: object, limit: int = 800) -> str:
    value = str(text)
    if units(value) <= limit:
        return value
    excerpt = value.encode("utf-16-le")[: max(0, limit - 1) * 2].decode("utf-16-le", "ignore")

    for token in DISCORD_TOKEN.finditer(value):
        if token.start() < len(excerpt) < token.end():
            excerpt = excerpt[: token.start()]
            break
    return excerpt + "…"


DISCORD_TOKEN = re.compile(
    r"<(?:a?:[A-Za-z0-9_]{1,64}:[0-9]{1,20}|@(?:!|&|\$)?[0-9]{1,20}|"
    r"#[0-9]{1,20}|t:-?[0-9]{1,20}(?::[A-Za-z])?|/[^<>\n]{1,100}:[0-9]{1,20})>"
)


def message_text(value: object, limit: int = 800) -> str:

    text = re.sub(r"@(everyone|here)\b", "@\u200b\\1", str(value), flags=re.IGNORECASE)
    return clip(text, limit)


def text_chunks(text: str, limit: int = 700) -> list[str]:

    pieces, current, size = [], [], 0
    for part in re.split("(" + DISCORD_TOKEN.pattern + ")", text):
        atoms = (part,) if DISCORD_TOKEN.fullmatch(part) else part
        for atom in atoms:
            width = units(atom)
            if current and size + width > limit:
                pieces.append("".join(current))
                current, size = [], 0
            current.append(atom)
            size += width
    if current or not pieces:
        pieces.append("".join(current) or "[Empty]")
    return pieces


def escape(value: object, limit: int = 800) -> str:
    text = str(value).replace("@", "@\u200b")
    text = re.sub(r"([\\`*_~|>#\[\]()])", r"\\\1", text)
    return clip(text, limit)


def public_record(record: dict) -> dict:

    permitted = {
        "id",
        "guild",
        "kind",
        "category",
        "subject",
        "actor",
        "channel",
        "created",
        "revision",
        "name",
        "avatar",
        "channel_name",
        "body",
        "before",
        "after",
        "reason",
        "warning",
        "duration",
        "until",
        "changes",
        "added",
        "removed",
        "counts",
        "from",
        "to",
        "age",
        "stay",
        "resource_type",
        "parent",
        "group",
        "incident_url",
        "total",
        "operation",
        "nsfw",
        "format",
        "image_before",
        "image_after",
        "audit",
        "actor_name",
        "message",
        "delivery",
        "previous_unknown",
        "current_roles",
        "known_actor",
        "webhook",
        "page",
        "pages",
    }
    result = {k: v for k, v in record.items() if k in permitted}
    result["files"] = [
        {
            k: v
            for k, v in f.items()
            if k in {"id", "name", "size", "type", "status", "sha", "label"}
        }
        for f in record.get("files", [])
    ]
    return result
