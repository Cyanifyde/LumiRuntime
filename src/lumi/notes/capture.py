import msgspec

from lumi.contracts.message_capture import CapturedMessage
from lumi.shared.links import source_urls

from .models import MediaSource, SavedMessage


def saved_message(value):
    captured = msgspec.convert(value, type=CapturedMessage)
    text = captured.content.strip()
    linked = source_urls(text)
    media, seen, embeds, details, files = [], set(), [], [], []

    def add(url, original="", filename="", spoiler=False):
        if not url or url in seen:
            return
        seen.add(url)
        media.append(MediaSource(url, original, filename, spoiler))

    for attached in captured.attachments:
        name = attached.filename
        if (
            attached.content_type.startswith("image/")
            or attached.width
            or name.casefold().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))
        ):
            add(attached.proxy_url or attached.url, attached.url, name, name.startswith("SPOILER_"))
        else:
            files.append(f"{name}: {attached.url}")

    for embed in captured.embeds:
        if not (embed.kind == "gifv" and embed.url in linked):
            embeds.extend(part.strip() for part in (embed.title, embed.description) if part.strip())
            for name, value in embed.fields:
                parts = (part.strip() for part in (name, value) if part.strip())
                if field := "\n".join(parts):
                    embeds.append(field)
            details.extend(part for part in (embed.author, embed.footer) if part)
        for image in (embed.image, embed.thumbnail):
            if image is not None:
                add(image.proxy_url or image.url, embed.url or image.url)
                if embed.kind == "gifv":
                    break
    for sticker in captured.stickers:
        extension = {1: "png", 2: "png", 3: "json", 4: "gif"}.get(sticker.kind)
        if not extension:
            continue
        host = "media.discordapp.net" if sticker.kind == 4 else "cdn.discordapp.com"
        url = f"https://{host}/stickers/{sticker.id}.{extension}"
        add(url, url, f"{sticker.name or 'Sticker'}.{extension}")
    if len(media) > 40:
        raise ValueError("Unsupported message media shape.")
    content = "\n".join(
        part for part in (text, "\n".join(embeds).strip()[:8192], *details, *files) if part
    )
    return SavedMessage(
        guild_id=captured.guild_id,
        channel_id=captured.channel_id,
        message_id=captured.message_id,
        author_id=captured.author_id,
        author_name=captured.author_name,
        content=content[:12000],
        occurred_at_ms=captured.occurred_at_ms,
        guild_name=captured.guild_name,
        channel_name=captured.channel_name,
        avatar_url=captured.avatar_url,
        entity_labels=captured.entity_labels,
        media=tuple(media),
    )
