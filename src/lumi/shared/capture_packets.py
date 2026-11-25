from typing import Annotated, Literal

import msgspec

from .capture_relay import Transfer
from .discord_observations import Observation

CAPTURE_PAGE = 100
GapKind = Literal[
    "gateway_started", "session_replaced", "spool_full", "capture_error", "baseline_error",
]


class CaptureBatch(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    client_id: int
    records: Annotated[tuple[Observation, ...], msgspec.Meta(min_length=1, max_length=CAPTURE_PAGE)]


class CaptureSignal(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    guild: Annotated[int, msgspec.Meta(gt=0)]
    kind: GapKind
    occurred: Annotated[int, msgspec.Meta(gt=0)]
    token: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")] = "0" * 32
    count: Annotated[int, msgspec.Meta(ge=0)] = 0


class CoverageBatch(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    signals: Annotated[
        tuple[CaptureSignal, ...], msgspec.Meta(min_length=1, max_length=CAPTURE_PAGE),
    ]


def encode_batch(records, client_id):
    return Transfer(msgspec.json.encode(CaptureBatch(1, client_id, tuple(records))))


def decode_batch(frame):
    return msgspec.json.decode(frame.body, type=CaptureBatch)


def encode_coverage(signals):
    return Transfer(msgspec.json.encode(CoverageBatch(1, tuple(signals))))


def decode_coverage(frame):
    return msgspec.json.decode(frame.body, type=CoverageBatch)
