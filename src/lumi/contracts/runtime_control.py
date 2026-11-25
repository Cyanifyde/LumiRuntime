import re
from typing import Literal

import msgspec

from lumi.transport import GROUPS

GENERATION = "lumi:generation:platform"
STATE = "lumi:runtime-control:v1:state"
VERSION = "lumi:runtime-control:v1:version"
REQUESTS = "lumi:runtime-control:v1:requests"
REPLIES = "lumi:runtime-control:v1:reply:"
BLOCK_REQUESTS = "lumi:runtime-control:v2:actor-requests:tools"
BLOCK_REPLIES = "lumi:runtime-control:v2:actor-replies:tools:"
BLOCK_RECEIPTS = "lumi:runtime-control:v2:actor-receipt:"
ACTOR_ADMISSIONS = "lumi:pending:actor-admission:"
BLOCK_CONFIRMATION = "temporary_block_applied"
REQUEST_MS = 15_000
MAX_PAYLOAD = 4 * 1024 * 1024


def identifier(value):
    return type(value) is int and 0 < value < 2**64


def job_identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{32}", value) is not None


class ActorAdmission(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    job: str
    actor: int
    group: str
    module: str
    route: str
    generation: int
    deadline: int


class BlockRequest(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    job: str
    reply: str
    deadline: int

    def valid(self, now):
        return (
            job_identifier(self.job)
            and job_identifier(self.reply)
            and now < self.deadline <= now + REQUEST_MS
        )


class BlockReceipt(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    job: str
    deadline: int
    expires: int

    def matches(self, job, now):
        return self.job == job and now < self.deadline < self.expires


class ControlProjection(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    generation: int
    serial: int
    permanent: frozenset[int]
    temporary: dict[int, int]
    maintenance: bool
    drains: frozenset[str]

    def version(self):
        return f"{self.generation}:{self.serial}".encode("ascii")

    def valid(self):
        return (
            identifier(self.generation)
            and identifier(self.serial)
            and all(identifier(user) for user in self.permanent)
            and all(
                identifier(user) and identifier(expiry) for user, expiry in self.temporary.items()
            )
            and self.drains <= set(GROUPS)
        )


class ControlRequest(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    identifier: str
    generation: int
    operation: Literal["block", "unblock", "maintenance", "drain"]
    deadline: int
    subject: int | None = None
    owner: int | None = None
    enabled: bool | None = None
    group: str | None = None

    def valid(self, now, owner):
        if (
            not re.fullmatch(r"[a-f0-9]{32}", self.identifier)
            or not identifier(self.generation)
            or not now < self.deadline <= now + REQUEST_MS
        ):
            return False
        values = set(msgspec.to_builtins(self)) - {
            "identifier",
            "generation",
            "operation",
            "deadline",
        }
        if self.owner != owner or not identifier(owner):
            return False
        if self.operation in {"block", "unblock"}:
            return (
                values == {"subject", "owner"}
                and identifier(self.subject)
                and self.subject != owner
            )
        if self.operation == "maintenance":
            return values == {"owner", "enabled"} and type(self.enabled) is bool
        return (
            self.operation == "drain"
            and values == {"owner", "enabled", "group"}
            and (self.group in GROUPS and type(self.enabled) is bool)
        )


class ControlReply(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    identifier: str
    generation: int
    serial: int
    changed: bool
