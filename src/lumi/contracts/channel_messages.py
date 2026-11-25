from typing import Annotated

import msgspec

READ_CHANNEL_MESSAGES = "read_channel_messages"
DELETE_CHANNEL_MESSAGES = "delete_channel_messages"
CHANNEL_MESSAGE_ACTIONS = frozenset({READ_CHANNEL_MESSAGES, DELETE_CHANNEL_MESSAGES})

MESSAGE_PAGE = 100


Snowflake = Annotated[int, msgspec.Meta(ge=1)]


class ChannelMessageAccess(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    include_content: bool = False
    allow_delete: bool = False


class ChannelMessageQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    before: Snowflake | None = None
    message: Snowflake | None = None
    limit: Annotated[int, msgspec.Meta(ge=1, le=MESSAGE_PAGE)] = MESSAGE_PAGE
    content: bool = False
    author_roles: bool = False


class ChannelMessageDeletion(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    operation: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{8}$")]
    messages: Annotated[tuple[Snowflake, ...], msgspec.Meta(min_length=1, max_length=MESSAGE_PAGE)]
    audit_reason: Annotated[str, msgspec.Meta(max_length=512)] | None = None
