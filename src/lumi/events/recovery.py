from __future__ import annotations

import hashlib

from lumi.contracts import OwnedHistoryQuery
from lumi.contracts.models import OWNED_FOOTER_WINDOW_MS, OWNED_HISTORY_PAGE
from lumi.worker.actions import ActionUnavailable


LEGACY_SEARCH_LIMIT = 2000


def snowflake(value):
    return type(value) is int and 0 < value < 2**64


async def observe_publication(
    actions, *, job: str, guild: int, channel: int, marker: str,
    attempt: str, now_ms: int, window_ms: int = OWNED_FOOTER_WINDOW_MS,
) -> int | None:

    if not snowflake(channel) or not 0 < window_ms <= OWNED_FOOTER_WINDOW_MS:
        return None
    after = max(1, now_ms - window_ms)
    digest = hashlib.sha256(marker.encode("utf-8")).hexdigest()
    found = 0
    cursor = None
    for page in range(LEGACY_SEARCH_LIMIT // OWNED_HISTORY_PAGE):
        try:
            result = await actions.request(
                job_id=job,
                kind="read_owned_history",
                guild_id=guild,
                channel_id=channel,
                idempotency_key=f"{attempt}:{page}",
                own_history=OwnedHistoryQuery(
                    after, OWNED_HISTORY_PAGE, cursor, footer_sha256=digest,
                ),
            )
        except (ActionUnavailable, TimeoutError):
            return None
        if not result.ok or result.data.get("channel_id") != channel:
            return None
        rows = result.data.get("messages")
        scanned, complete = result.data.get("scanned"), result.data.get("complete")
        next_cursor = result.data.get("cursor")
        if (
            type(scanned) is not int or not 0 <= scanned <= OWNED_HISTORY_PAGE
            or not isinstance(rows, (list, tuple)) or len(rows) > scanned
            or type(complete) is not bool or (complete and scanned == OWNED_HISTORY_PAGE)
            or (scanned and (not snowflake(next_cursor) or next_cursor <= (cursor or 0)))
            or (not scanned and (not complete or next_cursor != cursor))
        ):
            return None
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"id"}:
                return None
            message = row["id"]
            if not snowflake(message) or not (cursor or 0) < message <= next_cursor or found:
                return None
            found = message
        if complete:
            return found
        cursor = next_cursor
    return None
