from __future__ import annotations

import msgspec

MAX_ENVELOPE_BYTES = 64 * 1024

ROUTING_REVISION = 2


def encode(value: object) -> bytes:
    payload = msgspec.json.encode(value)
    if len(payload) > MAX_ENVELOPE_BYTES:
        raise ValueError("envelope exceeds 64 KiB")
    return payload


def decode[T](payload: bytes, target: type[T]) -> T:
    if len(payload) > MAX_ENVELOPE_BYTES:
        raise ValueError("envelope exceeds 64 KiB")
    return msgspec.json.decode(payload, type=target, strict=True)
