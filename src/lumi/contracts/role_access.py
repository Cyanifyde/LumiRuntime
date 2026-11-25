from typing import Annotated

import msgspec

READ_GUILD_ROLES = "read_guild_roles"
SET_ROLE_MEMBERSHIP = "set_role_membership"
ROLE_FACTS = frozenset({READ_GUILD_ROLES, SET_ROLE_MEMBERSHIP})
READ_ACTOR_ROLES = "read_actor_roles"
WRITE_ACTOR_ROLES = "write_actor_roles"
ACTOR_ROLE_ACTIONS = frozenset({READ_ACTOR_ROLES, WRITE_ACTOR_ROLES})
MAX_ACTOR_ROLES = 250


ROLE_FACT_PAGE = 64


class RoleAccessSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    target_fields: tuple[str, ...] = ()
    allow_write: bool = False


class RoleFactsQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    page: Annotated[int, msgspec.Meta(ge=0)] = 0
    refresh_target: bool = False


class RoleMembership(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    joined: str | None
    before: bool
    present: bool


class RoleChange(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    role: Annotated[int, msgspec.Meta(ge=1)]
    present: bool


class ActorRoleChanges(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    joined: Annotated[str, msgspec.Meta(max_length=40)] | None

    required: Annotated[tuple[int, ...], msgspec.Meta(max_length=MAX_ACTOR_ROLES)]
    changes: Annotated[tuple[RoleChange, ...], msgspec.Meta(max_length=MAX_ACTOR_ROLES)]


class RoleTargetBinding(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    module: str
    guild: int
    channel: int
    actor: int
    target: int
    expires: int


class RoleFact(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: int
    name: Annotated[str, msgspec.Meta(max_length=100)]
    assignable: bool


def role_target_key(group, token):
    return f"lumi:ui-role-target:{group}:{token}"


def role_write_key(job_id, role, attempt):
    return f"role-membership:{job_id}:{role}:{attempt}"


def role_write_receipt(group, module, key):
    return f"lumi:edge-action-receipt:{group}:{module}:{key}"


def actor_role_attempt(job_id, step=0):
    return f"role-membership:actor:{job_id}:{step}"
