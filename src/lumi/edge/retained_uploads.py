from __future__ import annotations

import re
from urllib.parse import urlsplit

from msgspec.structs import replace

from .intents import InvalidIntent


def _upload_name(url, channel_id):
    if not isinstance(url, str) or len(url) > 2048:
        return None
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in {"cdn.discordapp.com", "media.discordapp.net"}
            or parsed.username or parsed.password or parsed.port not in {None, 443}
        ):
            return None
    except ValueError:
        return None
    match = re.fullmatch(r"/attachments/([0-9]{17,20})/[0-9]{17,20}/([^/]{1,200})", parsed.path)
    return match[2] if match and match[1] == str(channel_id) else None


def resolve_gallery_uploads(intent, message, channel_id, expected):
    referenced = {
        item.file_name
        for page in intent.pages
        for block in page.blocks if block.kind == "gallery"
        for item in block.items if item.file_name
    }
    if referenced != expected:
        raise InvalidIntent("gallery does not match the retained uploads")
    if not expected:
        return intent
    urls = {}

    def bind(url):
        name = _upload_name(url, channel_id)
        if name in expected:
            urls[name] = url

    def walk(components):
        for component in components:
            for item in component.get("items", ()):
                bind(item.get("media", {}).get("url"))
            walk(component.get("components", ()))

    walk([c.to_dict() if hasattr(c, "to_dict") else c for c in message.components])
    for attachment in message.attachments:
        if attachment.filename in expected and _upload_name(attachment.url, channel_id) == attachment.filename:
            bind(attachment.url)
    if set(urls) != expected:
        raise InvalidIntent("existing upload not found")
    return replace(intent, pages=tuple(
        replace(page, blocks=tuple(
            replace(block, items=tuple(
                replace(item, media_url=urls[item.file_name], file_name=None)
                if item.file_name else item
                for item in block.items
            )) if block.kind == "gallery" else block
            for block in page.blocks
        )) for page in intent.pages
    ))
