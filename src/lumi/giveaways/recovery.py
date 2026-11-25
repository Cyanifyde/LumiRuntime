from __future__ import annotations

import hashlib
from urllib.parse import urlsplit

import msgspec

from lumi.contracts import OwnedHistoryQuery
from lumi.contracts.models import OWNED_HISTORY_PAGE

from . import expanded as v2
from . import models as engine
from .protocol import post_location, publication_binding

READ_HISTORY = "read_owned_history"

SEARCH_LIMIT = 1000


def publication_marker(record, observed):
    if v2.is_v2(record) and record.get("ui_version") == 2:
        return observed.get("family") == "v2" and observed.get("component_id") == join_id(record)
    filename = engine.filename(record)

    def verified(url):
        try:
            value = engine.image_url(str(url or ""))
        except ValueError:
            return False
        parts = urlsplit(value).path.split("/")
        return parts[2] == str(record["channel"]) and parts[-1] == filename

    return observed.get("family") == "legacy" and (
        any(verified(url) for url in observed.get("embed_images", ()))
        or any(
            item["filename"] == filename
            and item["size"] == record["image"]["size"]
            and verified(item["url"])
            for item in observed.get("attachments", ())
        )
    )


def join_id(record):
    return f"lumi:giveaway:join:{record['id']}"


async def recover_history(request, record, job, *, reply=None):

    if reply is None:
        group = "publication"
        binding = publication_binding(record)
        since = record["created"]
        digest = None
        selector = (
            {"component_id": join_id(record)}
            if v2.is_v2(record) and record.get("ui_version") == 2
            else {"attachment_name": engine.filename(record)}
        )
    else:
        group = "completion"
        delivery = record["deliveries"][f"result:{record['round']}"]
        binding = {
            **post_location(record),
            "round": record["round"],
            "attempt": delivery.get("attempt"),
        }
        since = delivery.get("at") or record["created"]
        digest = hashlib.sha256(reply.encode()).hexdigest()
        selector = {"reply_to": record["message"]}
    found, cursor, inspected = 0, None, 0
    while inspected < SEARCH_LIMIT:
        limit = min(OWNED_HISTORY_PAGE, SEARCH_LIMIT - inspected)
        result = await request(
            job_id=job,
            kind=READ_HISTORY,
            guild_id=record["guild"],
            channel_id=record["channel"],
            own_history=OwnedHistoryQuery(since, limit, cursor, **selector),
            idempotency_key=f"gw-history:{record['id']}:{group}:{job}:{cursor or 0}",
            wait_seconds=30,
        )
        if not result.ok:
            return None
        page = result.data
        rows, count, following = page.get("messages"), page.get("scanned"), page.get("cursor")
        if (
            page.get("channel_id") != record["channel"]
            or type(page.get("complete")) is not bool
            or type(count) is not int
            or not 0 <= count <= limit
            or not isinstance(rows, list)
            or len(rows) > count
            or (count and (type(following) is not int or following <= (cursor or 0)))
        ):
            return None
        for row in rows:
            if (
                not isinstance(row, dict)
                or type(row.get("id")) is not int
                or not (cursor or 0) < row["id"] <= following
            ):
                return None
            matches = (
                publication_marker(record, row)
                if digest is None
                else row.get("content_sha256") == digest
            )
            if matches:
                if found and row["id"] != found:
                    return workflow_receipt(result, group, binding, 0)
                found = row["id"]
        if page["complete"]:
            return workflow_receipt(result, group, binding, found)
        if count == 0:
            return None
        inspected += count
        cursor = following

    return workflow_receipt(result, group, binding, 0)


def workflow_receipt(result, group, binding, message_id):
    return msgspec.structs.replace(
        result,
        ok=bool(message_id),
        error_code=None if message_id else "ambiguous",
        data={
            group: {
                **binding,
                "disposition": "sent" if message_id else "uncertain",
                "message_id": message_id,
            }
        },
    )
