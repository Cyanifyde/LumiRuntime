from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

import msgspec

from lumi.contracts import encode
from lumi.contracts.message_capture import (
    CapturedAttachment, CapturedEmbed, CapturedImage, CapturedMessage, CapturedSticker,
)


def embed_source(message, *, guild_id, channel_id, message_id):

    if (
        message.id != message_id or message.channel.id != channel_id
        or message.guild is None or message.guild.id != guild_id
    ):
        raise ValueError("source identity mismatch")
    embeds = []

    for item in message.embeds[:2]:
        raw = item.to_dict()
        title, description = raw.get("title"), raw.get("description")
        url = (raw.get("image") or {}).get("url")
        for value, ceiling in ((title, 256), (description, 8000), (url, 2048)):
            if value is not None and (not isinstance(value, str) or len(value) > ceiling):
                raise ValueError("source exceeds projection limits")
        embeds.append(dict(title=title, description=description, image=dict(url=url)))
    return dict(
        guild=guild_id, channel=channel_id, author=message.author.id, webhook=message.webhook_id,
        embeds=embeds, message=message.id, created=int(message.created_at.timestamp() * 1000),
        edited=int(message.edited_at.timestamp() * 1000) if message.edited_at else 0,
    )


@dataclass(frozen=True, slots=True)
class MessageSnapshot:
    channel_id: int
    author_id: int
    message_content: str
    embed_content: str
    attachment_content: str
    embed_image_url: str | None
    attachment_image_url: str | None
    author_name: str
    author_avatar_url: str | None

    @property
    def visible_signature(self) -> tuple[str, str, str, str | None, str | None]:
        return (
            self.message_content,
            self.embed_content,
            self.attachment_content,
            self.embed_image_url,
            self.attachment_image_url,
        )

    def context(self) -> dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "author_id": self.author_id,
            "message_content": self.message_content,
            "embed_content": self.embed_content,
            "attachment_content": self.attachment_content,
            "image_url": self.embed_image_url or self.attachment_image_url,
            "author_name": self.author_name,
            "author_avatar_url": self.author_avatar_url,
        }


def _safe_url(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or len(text) > 2_048:
        return None
    try:
        parsed = urlsplit(text)
    except ValueError:
        return None
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        return None
    return text


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    converter = getattr(value, "to_dict", None)
    if callable(converter):
        converted = converter()
        if isinstance(converted, dict):
            return converted
    return {}


def _embed_parts(embeds: object) -> tuple[str, str | None]:
    parts: list[str] = []
    image_url: str | None = None
    for raw_embed in embeds or ():
        embed = _mapping(raw_embed)
        title = str(embed.get("title") or "").strip()
        description = str(embed.get("description") or "").strip()
        if title:
            parts.append(title)
        if description:
            parts.append(description)
        fields = embed.get("fields")
        if isinstance(fields, list):
            for raw_field in fields:
                if not isinstance(raw_field, dict):
                    continue
                name = str(raw_field.get("name") or "").strip()
                value = str(raw_field.get("value") or "").strip()
                if name and value:
                    parts.append(f"{name}\n{value}")
                elif name or value:
                    parts.append(name or value)
        if image_url is None:
            for key in ("image", "thumbnail"):
                candidate = embed.get(key)
                if not isinstance(candidate, dict):
                    continue
                for url_key in ("url", "proxy_url"):
                    if safe := _safe_url(candidate.get(url_key)):
                        image_url = safe
                        break
                if image_url:
                    break
    return "\n".join(parts).strip()[:8_192], image_url


def _attachment_parts(attachments: object) -> tuple[str, str | None]:
    names: list[str] = []
    image_url: str | None = None
    for raw_attachment in attachments or ():
        if isinstance(raw_attachment, dict):
            filename = raw_attachment.get("filename")
            url = raw_attachment.get("url")
            content_type = raw_attachment.get("content_type")
        else:
            filename = getattr(raw_attachment, "filename", None)
            url = getattr(raw_attachment, "url", None)
            content_type = getattr(raw_attachment, "content_type", None)
        names.append(str(filename or "attachment").strip()[:128] or "attachment")
        if image_url is None and str(content_type or "").startswith("image/"):
            image_url = _safe_url(url)
    content = f"[Attachments: {', '.join(names)}]" if names else ""
    return content, image_url


def snapshot_from_message(message: object) -> MessageSnapshot | None:
    guild = getattr(message, "guild", None)
    channel = getattr(message, "channel", None)
    author = getattr(message, "author", None)
    if guild is None or channel is None or author is None:
        return None
    channel_id = int(getattr(channel, "id", 0) or 0)
    author_id = int(getattr(author, "id", 0) or 0)
    if channel_id <= 0 or author_id <= 0:
        return None
    embed_content, embed_image_url = _embed_parts(getattr(message, "embeds", ()))
    attachment_content, attachment_image_url = _attachment_parts(
        getattr(message, "attachments", ())
    )
    avatar = getattr(author, "display_avatar", None)
    return MessageSnapshot(
        channel_id=channel_id,
        author_id=author_id,
        message_content=str(getattr(message, "content", "") or "").strip(),
        embed_content=embed_content,
        attachment_content=attachment_content,
        embed_image_url=embed_image_url,
        attachment_image_url=attachment_image_url,
        author_name=str(
            getattr(author, "display_name", None) or getattr(author, "name", None) or "Unknown user"
        )[:256],
        author_avatar_url=_safe_url(getattr(avatar, "url", None)),
    )


def snapshot_after_raw_edit(
    before: MessageSnapshot,
    data: dict[str, Any],
) -> MessageSnapshot:
    changes: dict[str, Any] = {}
    if "content" in data:
        changes["message_content"] = str(data.get("content") or "").strip()
    if "embeds" in data:
        embed_content, embed_image_url = _embed_parts(data.get("embeds"))
        changes["embed_content"] = embed_content
        changes["embed_image_url"] = embed_image_url
    if "attachments" in data:
        attachment_content, attachment_image_url = _attachment_parts(data.get("attachments"))
        changes["attachment_content"] = attachment_content
        changes["attachment_image_url"] = attachment_image_url
    return replace(before, **changes)


def capture_message(message: object) -> dict[str, Any] | None:

    guild = getattr(message, "guild", None)
    channel = getattr(message, "channel", None)
    author = getattr(message, "author", None)
    created = getattr(message, "created_at", None)
    if not all((guild, channel, author)) or not isinstance(created, datetime):
        return None
    if not all(type(value) is int and value > 0 for value in (
        guild.id, channel.id, author.id, message.id,
    )):
        return None

    def image(value):
        if not isinstance(value, dict):
            return None
        link, proxy = (_safe_url(value.get(key)) or "" for key in ("url", "proxy_url"))
        return CapturedImage(link, proxy if proxy != link else "") if link or proxy else None

    def embed(item):
        raw = _mapping(item)
        return CapturedEmbed(
            kind=str(raw.get("type") or "")[:32],
            title=str(raw.get("title") or "")[:256],
            description=str(raw.get("description") or "")[:4096],
            url=_safe_url(raw.get("url")) or "",
            author=str(_mapping(raw.get("author")).get("name") or "")[:256],
            footer=str(_mapping(raw.get("footer")).get("text") or "")[:2048],
            fields=tuple(
                (str(field.get("name") or "")[:256], str(field.get("value") or "")[:1024])
                for field in (raw.get("fields") or ())[:25] if isinstance(field, dict)
            ),
            image=image(raw.get("image")),
            thumbnail=image(raw.get("thumbnail")),
        )

    def attachment(item):
        key_fields = ("url", "proxy_url", "filename", "content_type", "width")
        raw = item if isinstance(item, dict) else {key: getattr(item, key, None) for key in key_fields}
        link, proxy = (_safe_url(raw.get(key)) or "" for key in ("url", "proxy_url"))
        width = raw.get("width")
        return CapturedAttachment(
            str(raw.get("filename") or "attachment")[:256], link, proxy if proxy != link else "",
            str(raw.get("content_type") or "")[:256], width if type(width) is int else None,
        )

    def sticker(item):
        if isinstance(item, dict):
            identifier, name, kind = item.get("id"), item.get("name"), item.get("format_type")
        else:
            identifier, name = getattr(item, "id", None), getattr(item, "name", None)
            kind = getattr(getattr(item, "format", None), "value", None)
        identifier = str(identifier)
        if type(kind) is int and 17 <= len(identifier) <= 20 and identifier.isascii() and identifier.isdigit():
            return CapturedSticker(identifier, str(name or "")[:240], kind)

    labels = []
    for person in getattr(message, "mentions", ()):
        name = str(getattr(person, "display_name", "Someone"))[:256]
        labels.extend(((f"<@{person.id}>", "@" + name), (f"<@!{person.id}>", "@" + name)))
    for role in getattr(message, "role_mentions", ()):
        labels.append((f"<@&{role.id}>", "@" + str(role.name)[:256]))
    for target in getattr(message, "channel_mentions", ()):
        labels.append((f"<#{target.id}>", "#" + str(target.name)[:256]))
    attachments = getattr(message, "attachments", ())
    embeds = getattr(message, "embeds", ())
    stickers = getattr(message, "stickers", ())
    if len(attachments) > 10 or len(embeds) > 10 or len(stickers) > 3:
        return None
    projection = CapturedMessage(
        schema=1, guild_id=guild.id, channel_id=channel.id, message_id=message.id,
        author_id=author.id,
        author_name=str(getattr(author, "display_name", None) or getattr(author, "name", None) or "Unknown user")[:256],
        content=str(getattr(message, "content", "") or "")[:4000],
        occurred_at_ms=int(created.timestamp() * 1000),
        guild_name=str(getattr(guild, "name", ""))[:256],
        channel_name=str(getattr(channel, "name", "channel"))[:256],
        avatar_url=_safe_url(getattr(getattr(author, "display_avatar", None), "url", None)) or "",
        entity_labels=tuple(labels),
        attachments=tuple(map(attachment, attachments)),
        embeds=tuple(map(embed, embeds)),
        stickers=tuple(observed for item in stickers if (observed := sticker(item))),
    )
    try:
        encode(projection)
    except ValueError:
        return None
    return msgspec.to_builtins(projection)


def capture_urls(projection):

    urls = set()
    for embed in projection.get("embeds", ()):
        for key in ("image", "thumbnail"):
            if item := embed.get(key):
                urls.add(embed.get("url", ""))
                urls.update((item.get("url", ""), item.get("proxy_url", "")))
    for item in projection.get("attachments", ()):
        urls.update((item.get("url", ""), item.get("proxy_url", "")))
    return urls
