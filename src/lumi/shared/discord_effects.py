import hashlib
import re

from lumi.contracts import encode
from lumi.transport import GROUPS


REPLY_TEXT = "reply_text"
PUBLISH_MESSAGE = "publish_channel_message"
MUTATE_MESSAGE = "mutate_owned_message"
UPDATE_BOUND_PANEL = "update_bound_panel"

EFFECT_GRANT_SECONDS = 30


def effect_key(group, job, attempt):
    if (
        group not in GROUPS
        or not re.fullmatch(r"[a-f0-9]{32}", job)
        or not re.fullmatch(
            r"[A-Za-z0-9:_-]{1,160}",
            attempt,
        )
    ):
        raise ValueError("invalid Discord effect key")
    return f"lumi:discord-effect-grant:{group}:{job}:{attempt}"


def payload_digest(intent, recipients):
    return hashlib.sha256(encode((intent, tuple(recipients)))).hexdigest()


def effect_receipt_key(group, module, attempt):
    if (
        group not in GROUPS or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", module)
        or not re.fullmatch(r"effect:[a-f0-9]{32}", attempt)
    ):
        raise ValueError("invalid scheduled effect attempt")
    return f"lumi:edge-action-receipt:{group}:{module}:{attempt}"


def source_digest(query):
    return None if query is None else hashlib.sha256(encode(query)).hexdigest()


def panel_binding(actor, acl, session):
    return source_digest(("actor_panel", actor, acl, session))


def panel_payload(session, page):
    return source_digest((session, page))


def mutation_lane(group, module, guild, channel, message):
    if (
        group not in GROUPS
        or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", module)
        or any(
            type(identifier) is not int or not 0 < identifier < 2**64
            for identifier in (guild, channel, message)
        )
    ):
        raise ValueError("invalid Discord message lane")
    return f"lumi:discord-mutation-current:{group}:{module}:{guild}:{channel}:{message}"


def member_operation_lane(group, module, guild, operation):
    if (
        group not in GROUPS or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", module)
        or type(guild) is not int or not 0 < guild < 2**64
        or not re.fullmatch(r"[a-f0-9]{8}", operation)
    ):
        raise ValueError("invalid Discord member operation lane")
    return f"lumi:discord-mutation-current:{group}:{module}:{guild}:members:{operation}"


def channel_operation_lane(group, module, guild, channel, operation):
    if not re.fullmatch(r"[a-f0-9]{8}", operation):
        raise ValueError("invalid Discord channel operation lane")
    namespace, _ = mutation_lane(group, module, guild, channel, channel).rsplit(":", 1)
    return f"{namespace}:operation:{operation}"


_STAGE_MUTATION = (
    "if not redis.call('SET',KEYS[1],ARGV[1],'NX','EX',ARGV[2]) then return 0 end "
    "redis.call('SET',KEYS[2],KEYS[1],'EX',ARGV[2]); return 1"
)


async def stage_mutation(redis, key, lane, encoded_grant):
    return bool(
        await redis.eval(_STAGE_MUTATION, 2, key, lane, encoded_grant, EFFECT_GRANT_SECONDS)
    )
