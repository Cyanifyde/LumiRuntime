import hashlib
from typing import Annotated

import msgspec

MUTATION_ATTEMPT = "DISCORD_MUTATION_ATTEMPT"
DELETE_MESSAGES = "delete_messages"
EDIT_NICKNAME = "edit_nickname"
NATIVE_MUTATION_KINDS = frozenset({DELETE_MESSAGES, EDIT_NICKNAME})
OperationID = Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{8}$")]
NicknameDigest = Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{64}$")]


class DeleteMessages(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag=DELETE_MESSAGES, tag_field="kind",
):
    guild: int
    actor: int
    operation: OperationID
    channel: int
    messages: Annotated[tuple[int, ...], msgspec.Meta(min_length=1, max_length=100)]


class EditNickname(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag=EDIT_NICKNAME, tag_field="kind",
):
    guild: int
    actor: int
    operation: OperationID
    member: int
    change: NicknameDigest


type NativeMutation = DeleteMessages | EditNickname


class RecordedMutation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    attempt: NativeMutation
    application_id: int


def mutation_kind(attempt):
    return type(attempt).__struct_config__.tag


def nickname_change(before, after):
    return hashlib.sha256(msgspec.json.encode((before, after))).hexdigest()


def nickname_audit_reason(operation):
    return f"Lumi nickname {operation}"


def decode_attempt(value):
    record = msgspec.convert(value, type=RecordedMutation, strict=True)
    attempt = record.attempt
    targets = (
        (attempt.channel, *attempt.messages) if isinstance(attempt, DeleteMessages)
        else (attempt.member,)
    )
    if (
        any(
            type(uid) is not int or not 0 < uid < 2**64
            for uid in (record.application_id, attempt.guild, attempt.actor, *targets)
        )
        or (
            isinstance(attempt, DeleteMessages)
            and len(set(attempt.messages)) != len(attempt.messages)
        )
    ):
        raise ValueError("Invalid Discord mutation scope.")
    return record
