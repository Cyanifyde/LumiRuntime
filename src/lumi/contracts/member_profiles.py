from typing import Annotated

import msgspec

from .native_mutations import OperationID

LIST_GUILD_MEMBERS = "list_guild_members"
READ_MEMBER_PROFILE = "read_member_profile"
SET_MEMBER_NICKNAME = "set_member_nickname"
MEMBER_PROFILE_READS = frozenset({LIST_GUILD_MEMBERS, READ_MEMBER_PROFILE})
MEMBER_PROFILE_ACTIONS = MEMBER_PROFILE_READS | {SET_MEMBER_NICKNAME}

MEMBER_PAGE_SIZE = 1_000

NICKNAME_WIRE_LIMIT = 1_800
Nickname = Annotated[str, msgspec.Meta(max_length=NICKNAME_WIRE_LIMIT)] | None


class MemberProfileAccess(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True
):
    actor_permissions: tuple[str, ...]
    bot_permissions: tuple[str, ...]
    owner_only: bool = False
    allow_directory: bool = False
    allow_nickname_write: bool = False


class NicknameMutation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    operation_id: OperationID

    joined_at: Annotated[str, msgspec.Meta(min_length=1, max_length=40)]
    before: Nickname
    nickname: Nickname
