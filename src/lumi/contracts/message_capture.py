from typing import Annotated, Literal

import msgspec


class CapturedImage(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    url: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    proxy_url: Annotated[str, msgspec.Meta(max_length=2048)] = ""


class CapturedEmbed(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    kind: Annotated[str, msgspec.Meta(max_length=32)] = ""
    title: Annotated[str, msgspec.Meta(max_length=256)] = ""
    description: Annotated[str, msgspec.Meta(max_length=4096)] = ""
    url: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    author: Annotated[str, msgspec.Meta(max_length=256)] = ""
    footer: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    fields: Annotated[tuple[tuple[str, str], ...], msgspec.Meta(max_length=25)] = ()
    image: CapturedImage | None = None
    thumbnail: CapturedImage | None = None


class CapturedAttachment(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True
):
    filename: Annotated[str, msgspec.Meta(max_length=256)]
    url: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    proxy_url: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    content_type: Annotated[str, msgspec.Meta(max_length=256)] = ""
    width: int | None = None


class CapturedSticker(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: Annotated[str, msgspec.Meta(pattern=r"^[0-9]{17,20}$")]
    name: Annotated[str, msgspec.Meta(max_length=240)]
    kind: int


class CapturedMessage(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    schema: Literal[1]
    guild_id: Annotated[int, msgspec.Meta(gt=0)]
    channel_id: Annotated[int, msgspec.Meta(gt=0)]
    message_id: Annotated[int, msgspec.Meta(gt=0)]
    author_id: Annotated[int, msgspec.Meta(gt=0)]
    author_name: Annotated[str, msgspec.Meta(max_length=256)]
    content: Annotated[str, msgspec.Meta(max_length=4000)]
    occurred_at_ms: Annotated[int, msgspec.Meta(gt=0)]
    guild_name: Annotated[str, msgspec.Meta(max_length=256)] = ""
    channel_name: Annotated[str, msgspec.Meta(max_length=256)] = ""
    avatar_url: Annotated[str, msgspec.Meta(max_length=2048)] = ""
    entity_labels: tuple[tuple[str, str], ...] = ()
    attachments: Annotated[tuple[CapturedAttachment, ...], msgspec.Meta(max_length=10)] = ()
    embeds: Annotated[tuple[CapturedEmbed, ...], msgspec.Meta(max_length=10)] = ()
    stickers: Annotated[tuple[CapturedSticker, ...], msgspec.Meta(max_length=3)] = ()
