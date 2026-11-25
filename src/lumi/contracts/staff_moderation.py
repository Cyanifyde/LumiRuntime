from typing import Literal

import msgspec

STAFF_EFFECT = "apply_staff_effect"
STAFF_READS = frozenset({"inspect", "observe_source"})


class StaffModerationAccess(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    viewer_permissions: tuple[str, ...]
    log_permissions: tuple[str, ...]
    protected_role_permissions: tuple[str, ...]
    operations: tuple[str, ...]


class StaffEffect(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    operation: Literal["inspect", "observe_source", "send", "edit", "delete", "add_role", "remove_role", "kick", "ban"]
    log: int
    revision: int
    reason: str = ""
    proof: str | None = None
    evidence: int | None = None
    role: int | None = None
    inspect_roles: tuple[int, ...] = ()


def policy_lane(group, module, guild):
    return f"lumi:discord-mutation-current:{group}:{module}:{guild}:staff"


def policy_token(generation, revision):
    return f"{generation}:{revision}"
