from __future__ import annotations

import re
from dataclasses import dataclass

import msgspec

ACTIVITY_PREFIX = "lumi:activity-watermark:v1:"
WEBHOOK_PREFIX = "lumi:webhook-origin:"
CUTOVER = "lumi:transport-cutover:observations:v1"
CUTOVER_VERSION = b"1"
WEBHOOK_TTL = 48 * 60 * 60
WEBHOOK_WIRE_MAX = 2048
_VALUE = re.compile(r"([0-9]{16}):([0-9]{20})")


def snowflake(value):
    return type(value) is int and 0 < value < 2**64


@dataclass(frozen=True, slots=True)
class LastSpoke:
    occurred_at_ms: int
    message_id: int


def activity_key(guild_id: int, user_id: int) -> str:
    if not (snowflake(guild_id) and snowflake(user_id)):
        raise ValueError("invalid activity scope")
    return f"{ACTIVITY_PREFIX}{guild_id}:{user_id}"


def encode_watermark(value: LastSpoke) -> str:
    if not 0 < value.occurred_at_ms < 10**16 or not snowflake(value.message_id):
        raise ValueError("invalid activity boundary")

    return f"{value.occurred_at_ms:016d}:{value.message_id:020d}"


def parse_watermark(raw) -> LastSpoke | None:
    if raw is None:
        return None
    value = raw.decode("ascii") if isinstance(raw, bytes) else raw
    match = _VALUE.fullmatch(value)
    if not match:
        raise ValueError("invalid stored activity boundary")
    boundary = LastSpoke(int(match[1]), int(match[2]))
    encode_watermark(boundary)
    return boundary


def webhook_key(guild_id: int, channel_id: int, message_id: int) -> str:
    return f"{WEBHOOK_PREFIX}{guild_id}:{channel_id}:{message_id}"


class WebhookBinding(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    guild_id: int
    channel_id: int
    message_id: int
    webhook_id: int
    user_id: int
    original_message_id: int
    created_at_ms: int
    module: str

    def valid(self):
        return (
            0 < self.created_at_ms < 2**53 - WEBHOOK_TTL * 1000
            and self.original_message_id < self.message_id
            and re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", self.module) is not None
            and all(
                snowflake(i)
                for i in (
                    self.guild_id,
                    self.channel_id,
                    self.message_id,
                    self.webhook_id,
                    self.user_id,
                    self.original_message_id,
                )
            )
        )
