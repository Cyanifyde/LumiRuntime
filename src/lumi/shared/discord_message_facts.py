def body_facts(message):

    return dict(
        author=message.author.id, bot=bool(getattr(message.author, "bot", False)),
        webhook=bool(getattr(message, "webhook_id", None)), pinned=message.pinned,
        deletable=message.type.is_deletable(), content=message.content,
        attachment_types=[str(getattr(item, "content_type", "") or "").casefold()[:100] for item in message.attachments],
        embeds=bool(message.embeds), image_embed=any(getattr(e, "image", None) for e in message.embeds),
        video_embed=any(getattr(e, "video", None) for e in message.embeds), stickers=bool(message.stickers),
        components=bool(getattr(message, "components", ())), poll=bool(getattr(message, "poll", None)),
        mentions=bool(message.mentions or message.role_mentions or message.mention_everyone),
        reference=bool(message.reference),
    )
