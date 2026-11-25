from __future__ import annotations

import hashlib
import json
import re
import secrets
from urllib.parse import urlsplit

from lumi.contracts import DiscordIntent, PresentationControl, PresentationPage

MODULE = "giveaways"
BOTS = {-2: "karuta", -3: "sofi", -1: "glitch"}
LABELS = {"karuta": "Karuta", "sofi": "Sofi", "glitch": "Glitch"}
ID = re.compile(r"^[a-f0-9]{16}$")
FRESH_MS = 300_000
WINDOW = 300_000
DISCORD_EPOCH = 1_420_070_400_000
ACTIVE = frozenset({"publishing", "active", "drawing"})


class GiveawayError(ValueError):
    pass


def parse_duration(raw: str) -> int:
    from .durations import duration

    return duration(raw)


def image_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"cdn.discordapp.com", "media.discordapp.net"}
        or parsed.username
        or parsed.password
        or parsed.port not in {None, 443}
        or not re.fullmatch(r"/attachments/\d{17,20}/\d{17,20}/[^/]{1,200}", parsed.path)
    ):
        raise GiveawayError("Unsupported card image.")
    return value


def source_fingerprint(card: dict) -> str:
    value = dict(card)
    value["image_url"] = image_url(value["image_url"]).partition("?")[0]
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def new_record(identifier, guild, host, channel, command, source_channel, source, wait, image, now):
    card = dict(source)
    card.pop("image_url")
    return dict(
        id=identifier,
        guild=guild,
        host=host,
        channel=channel,
        command=command,
        source_channel=source_channel,
        card=card,
        source_hash=source_fingerprint(source),
        duration=wait,
        image=image,
        created=now,
        status="publishing",
        revision=1,
        message=0,
        ends=0,
        entries={},
        winner=None,
        winners=[],
        round=0,
        draw=None,
        deliveries={"publish": dict(state="pending", message=0, at=now)},
        rendered="",
        error="",
        next_ms=now,
    )


def count(record):
    return sum(bool(value) for value in record["entries"].values())


def toggle_entry(record, actor, key, now):
    if record.get("version") == 2:
        from .expanded import set_entry

        return set_entry(record, actor, key, now)
    receipts = record.setdefault("entry_receipts", {})
    if key in receipts:
        saved = receipts[key]
        if saved[0] != actor:
            raise GiveawayError("Unavailable.")
        return saved[1]
    if record["status"] != "active" or now >= record["ends"]:
        raise GiveawayError("Entries closed.")
    if actor == record["host"]:
        raise GiveawayError("You cannot enter your own giveaway.")
    entered = not record["entries"].get(str(actor), False)
    record["entries"][str(actor)] = entered
    receipts[key] = [actor, entered]
    record["next_ms"] = min(record["next_ms"], record["ends"])
    return entered


def ready_to_reroll(record):
    return (
        record["status"] == "ended"
        and record["winner"] is not None
        and record["deliveries"].get(f"result:{record['round']}", {}).get("state") == "sent"
    )


def begin_draw(record, now, *, reroll=False, expected=None, selected=None):
    if record.get("version") == 2:
        from .expanded import begin_draw as expanded_draw

        return expanded_draw(record, now, reroll=reroll, expected=expected, selected=selected)
    if expected is not None and record["revision"] != expected:
        raise GiveawayError("This giveaway changed.")
    if reroll:
        if not ready_to_reroll(record):
            raise GiveawayError("Reroll unavailable.")
    elif record["status"] != "active" or now < record["ends"]:
        return False
    excluded = set(record["winners"]) | {record["host"]}
    order = [
        int(user)
        for user, entered in record["entries"].items()
        if entered and int(user) not in excluded
    ]
    secrets.SystemRandom().shuffle(order)
    record["revision"] += 1
    record["status"] = "drawing"
    record["error"] = ""
    record["draw"] = dict(order=order, cursor=0, previous=record["winner"], reroll=reroll)
    record["next_ms"] = now
    return True


def accept_candidates(record, users, eligible, now):
    if record.get("version") == 2:
        from .expanded import accept_candidates as expanded_accept

        return expanded_accept(record, users, eligible, now)
    if record["status"] != "drawing":
        raise GiveawayError("This giveaway changed.")
    draw = record["draw"]
    expected = draw["order"][draw["cursor"] : draw["cursor"] + len(users)]
    if not users or users != expected or not set(eligible) <= set(users):
        raise GiveawayError("Invalid draw result.")
    winner = next((user for user in users if user in eligible), None)
    draw["cursor"] += len(users)
    if winner is not None:
        record["winner"] = winner
        record["winners"].append(winner)
        record["round"] += 1
        record.setdefault("draw_history", []).append(
            dict(round=record["round"], winner=winner, at=now)
        )
        record["deliveries"][f"result:{record['round']}"] = dict(state="pending", message=0, at=now)
        record["status"], record["draw"] = "ended", None
    elif draw["cursor"] == len(draw["order"]):
        finish_empty(record, now)
    record["next_ms"] = now


def finish_empty(record, now):
    if record.get("version") == 2:
        from .expanded import finish_draw

        return finish_draw(record, now)
    draw = record["draw"]
    if draw["reroll"]:
        record["winner"] = draw["previous"]
        record["error"] = "No other eligible entrants."
    else:
        record["winner"] = None
        record["round"] = 1
        record["deliveries"]["result:1"] = dict(state="pending", message=0, at=now)
    record["status"], record["draw"] = "ended", None
    record["next_ms"] = now


def stop_deleted(record, now):
    if record["status"] in {"cancelled", "failed"}:
        return
    if record.get("version") == 2:
        record["pending_edit"] = None
        record.pop("edit_in_flight", None)
    record.update(
        status="cancelled",
        revision=record["revision"] + 1,
        draw=None,
        next_ms=9_000_000_000_000_000,
        deleted_at=now,
    )
    for delivery in record["deliveries"].values():
        if delivery["state"] == "pending":
            delivery["state"] = "cancelled"


def signature(record):
    if record.get("version") == 2:
        from .expanded import signature as expanded_signature

        return expanded_signature(record)
    return ":".join(
        str(value)
        for value in (
            record["revision"],
            record["status"],
            record["round"],
            ready_to_reroll(record),
            record.get("error", ""),
        )
    )


def filename(record):
    return f"giveaway-{record['id']}{record['image']['suffix']}"


def presentation(record):
    if record.get("version") == 2 and record.get("ui_version") == 2:
        from .expanded import presentation as expanded_presentation

        return expanded_presentation(record)
    lines = [f"`{record['card']['code']}`", f"<@{record['host']}>"]
    status = record["status"]
    if record["ends"]:
        lines.append(f"<t:{record['ends'] // 1000}:F>")
    controls = (
        PresentationControl(
            "button",
            "giveaway",
            "join",
            record["id"],
            label="Join",
            emoji="🎉",
            style="success",
            disabled=status != "active",
        ),
    )
    return DiscordIntent(
        "send",
        pages=(
            PresentationPage(
                description="\n".join(lines),
                file_name=filename(record),
            ),
        ),
        controls=controls,
        presentation_mode="legacy",
        public_controls=True,
    )


def completion(record):
    if record.get("version") == 2:
        from .expanded import completion as expanded_completion

        return expanded_completion(record)
    result = (
        f"<@{record['winner']}> won `{record['card']['code']}`."
        if record["winner"]
        else f"No eligible entrants for `{record['card']['code']}`."
    )
    return f"<@{record['host']}> · {result}"
