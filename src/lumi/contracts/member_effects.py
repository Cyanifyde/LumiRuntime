from typing import Literal

import msgspec

BAN_MEMBER = "ban_member"
KICK_MEMBER = "kick_member"
UNBAN_MEMBER = "unban_member"
SET_MEMBER_TIMEOUT = "set_member_timeout"
CLEAR_MEMBER_TIMEOUT = "clear_member_timeout"
MEMBER_EFFECT_PERMISSIONS = {
    BAN_MEMBER: "ban_members",
    KICK_MEMBER: "kick_members",
    UNBAN_MEMBER: "ban_members",
    SET_MEMBER_TIMEOUT: "moderate_members",
    CLEAR_MEMBER_TIMEOUT: "moderate_members",
}
TIMEOUT_MAX_MS = 28 * 86400_000


class MemberEffectSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    kind: Literal[
        "ban_member", "kick_member", "unban_member", "set_member_timeout", "clear_member_timeout",
    ]
    target_fields: tuple[str, ...]
    reason_field: str | None = None


def member_effect_key(job_id):
    return f"member-effect:{job_id}"


def member_receipt_key(group, module, job_id):
    return f"lumi:edge-action-receipt:{group}:{module}:{member_effect_key(job_id)}"
