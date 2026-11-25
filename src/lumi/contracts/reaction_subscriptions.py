from typing import Annotated, Literal

import msgspec


class ReactionSubscription(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    emoji: Annotated[str, msgspec.Meta(min_length=1, max_length=100)]
    route: Annotated[
        str, msgspec.Meta(pattern=r"^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)+$", max_length=128)
    ]
    tag: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")]


class ReactionSubscriptions(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    group: str
    module: Annotated[str, msgspec.Meta(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
    user_id: Annotated[int, msgspec.Meta(gt=0)]
    revision: Annotated[int, msgspec.Meta(ge=0)]
    bindings: Annotated[tuple[ReactionSubscription, ...], msgspec.Meta(max_length=64)]


class ReactionObservation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    activation: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")]
    emoji: Annotated[str, msgspec.Meta(min_length=1, max_length=100)]
    revision: Annotated[int, msgspec.Meta(ge=0)]
    received_at_ms: Annotated[int, msgspec.Meta(gt=0)]
    tag: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")]
    message: dict
