import unicodedata
from typing import Literal

import msgspec

OBSERVE_RECOVERY = "observe_staff_recovery"
EDIT_RECOVERY = "edit_staff_recovery"
RECOVERY_ACTIONS = frozenset({OBSERVE_RECOVERY, EDIT_RECOVERY})


def snowflake(value):
    return type(value) is int and 0 < value < 2**64


class StaffRecoveryAccess(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    viewer_permissions: tuple[str, ...]
    bot_permissions: tuple[str, ...]


class StaffRecoveryTarget(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    log: int
    source: int
    member: int
    marker: str
    evidence: Literal["none", "confirmed", "legacy", "uncertain"] = "none"
    evidence_id: int = 0
    check_roles: bool = False
    check_ban: bool = False

    def valid(self, source_channel):
        return (
            all(snowflake(value) for value in (source_channel, self.log, self.source, self.member))
            and 1 <= len(self.marker) <= 128
            and all(not unicodedata.category(char).startswith("C") for char in self.marker)
            and (snowflake(self.evidence_id) if self.evidence in {"confirmed", "legacy"} else self.evidence_id == 0)
        )
