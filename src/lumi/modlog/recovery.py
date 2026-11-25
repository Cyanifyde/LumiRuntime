import hashlib
import re

from redis.exceptions import RedisError

from lumi.contracts import OwnedHistoryQuery, encode
from lumi.contracts.models import OWNED_HISTORY_PAGE
from lumi.worker.actions import ActionUnavailable
from .publication import receipt, settle

RECOVER = "modlog.recover"
STATES = ("sending", "uncertain")
RETRY_MS = 60_000


def identity(out):
    return hashlib.sha256(encode(tuple(
        out.get(key) for key in ("guild", "id", "state", "revision", "destination", "message", "nonce", "due")
    ))).hexdigest()


async def recover(store, queue, actions, request, job, *, generation, clock):
    event, args = request.event, request.arguments
    guild, channel = event.get("guild_id"), event.get("channel_id")
    record, expected = args.get("record"), args.get("attempt")
    now = clock()
    if (
        request.route != RECOVER or request.handler != RECOVER
        or event.get("dispatch") != "scheduled" or event.get("actor_id") != 0
        or event.get("_job_generation") != generation
        or type(event.get("_expires_at_ms")) is not int or event["_expires_at_ms"] <= now
        or type(guild) is not int or guild <= 0 or type(channel) is not int or channel <= 0
        or not isinstance(record, str) or not re.fullmatch(r"[a-f0-9]{32}", record)
        or not isinstance(expected, str)
    ):
        return
    out = await store.outbox(guild, record)
    if (
        not out or out["state"] not in STATES or out.get("destination") != channel
        or out["due"] > now or out["expires"] <= now or identity(out) != expected
    ):
        return
    response = await receipt(queue, out)
    if response is not None and await settle(store, out, response, clock()):
        return
    if out.get("message"):


        await store.recover_publication(out, clock())
        return
    nonce = out.get("nonce")
    message = None
    gone = False
    if isinstance(nonce, str) and re.fullmatch(r"[A-Za-z0-9:_-]{1,25}", nonce):
        try:
            response = await actions.request(
                kind="read_owned_history", job_id=job, guild_id=guild, channel_id=channel,
                own_history=OwnedHistoryQuery(max(1, out["created"] - 1), OWNED_HISTORY_PAGE, nonce=nonce),
                idempotency_key=f"modlog-recover:{job}", wait_seconds=12,
            )
        except (ActionUnavailable, RedisError, TimeoutError, OSError):
            response = None
        if response is not None:
            gone = response.error_code == "channel_not_found"
            if response.ok and response.data.get("channel_id") == channel:
                matches = response.data.get("messages")
                if isinstance(matches, list) and len(matches) == 1 and isinstance(matches[0], dict):
                    found = matches[0].get("id")
                    if type(found) is int and 0 < found < 2**64:
                        message = found


    await store.recover_publication(out, clock(), message=message, gone=gone, retry_ms=RETRY_MS)
