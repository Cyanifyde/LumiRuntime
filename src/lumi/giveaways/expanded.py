from __future__ import annotations

import copy
import hashlib
import json
import re
import secrets

from lumi.contracts import (
    DiscordIntent,
    PresentationMedia,
    PresentationPage,
)
from lumi.contracts import (
    PresentationBlock as Block,
)
from lumi.contracts import (
    PresentationControl as Control,
)

from .models import ID, LABELS, GiveawayError

NEVER = 9_000_000_000_000_000
DAY = 86_400_000
MAX_PRIZES, MAX_WINNERS, MAX_IMAGES = 10, 25, 10


def is_v2(record):
    return record.get("version") == 2


def escaped(text):
    return re.sub(r"([\\`*_~|>\[\]#])", r"\\\1", str(text)).replace("\n", " ")


def integer(value, minimum, maximum, label):
    if type(value) is not int or not minimum <= value <= maximum:
        raise GiveawayError(f"Invalid {label}.")
    return value


def rules(value=None):
    value = value or {}
    if not isinstance(value, dict) or set(value) - {
        "roles",
        "mode",
        "excluded",
        "account_ms",
        "member_ms",
    }:
        raise GiveawayError("Invalid entry rules.")
    result = dict(roles=[], mode=value.get("mode", "all"), excluded=[], account_ms=0, member_ms=0)
    if result["mode"] not in {"all", "any"}:
        raise GiveawayError("Choose all or any roles.")
    for key in ("roles", "excluded"):
        values = value.get(key, [])
        if not isinstance(values, (list, tuple)) or len(values) > 10:
            raise GiveawayError("Choose up to ten roles.")
        result[key] = sorted({integer(v, 1, 2**64 - 1, "role") for v in values})
    if set(result["roles"]) & set(result["excluded"]):
        raise GiveawayError("A role cannot be required and excluded.")
    for key in ("account_ms", "member_ms"):
        result[key] = integer(value.get(key, 0), 0, 100 * 366 * DAY, "minimum age")
    return result


def config_defaults(config):
    settings = config.get("settings") or {}
    baseline = rules(settings.get("rules"))
    if config.get("required_role"):

        legacy = rules(dict(roles=[config["required_role"]]))
    else:
        legacy = rules()
    return dict(
        title="",
        description="",
        layout=settings.get("layout", "image-first"),
        duration=settings.get("duration", DAY),
        mode="bundle",
        prizes=[],
        baseline=[baseline, legacy],
        rules=rules(),
        banner=None,
    )


def validate_spec(value, *, complete=False):
    if not isinstance(value, dict) or set(value) - {
        "title",
        "description",
        "layout",
        "duration",
        "mode",
        "rules",
        "baseline",
        "prizes",
        "banner",
    }:
        raise GiveawayError("Invalid giveaway.")
    result = copy.deepcopy(value)
    for key, limit in (("title", 180), ("description", 800)):
        if not isinstance(result.get(key, ""), str) or len(result.get(key, "")) > limit:
            raise GiveawayError(f"{key.title()} is too long.")
        result.setdefault(key, "")
    if result.get("layout") not in {"image-first", "text-first"}:
        raise GiveawayError("Choose an image-first or text-first layout.")
    integer(result.get("duration"), 60_000, 30 * DAY, "duration")
    if result.get("mode") not in {"bundle", "separate"}:
        raise GiveawayError("Choose bundle or separate winners.")
    result["rules"] = rules(result.get("rules"))
    baseline = result.get("baseline", [])
    if not isinstance(baseline, list) or len(baseline) > 2:
        raise GiveawayError("Invalid server requirements.")
    result["baseline"] = [rules(v) for v in baseline]
    prizes = result.get("prizes")
    if not isinstance(prizes, list) or not (1 if complete else 0) <= len(prizes) <= MAX_PRIZES:
        raise GiveawayError("Add one to ten prizes.")
    identifiers, cards = set(), set()
    for prize in prizes:
        if not isinstance(prize, dict) or not ID.fullmatch(str(prize.get("id", ""))):
            raise GiveawayError("Invalid prize.")
        if set(prize) - {
            "id",
            "kind",
            "card",
            "image",
            "proof",
            "source_channel",
            "name",
            "copies",
        }:
            raise GiveawayError("Invalid prize.")
        if prize["id"] in identifiers:
            raise GiveawayError("Duplicate prize.")
        identifiers.add(prize["id"])
        if prize.get("kind") == "card":
            card = prize.get("card", {})
            if (
                not isinstance(card, dict)
                or set(card)
                - {
                    "game",
                    "owner",
                    "message",
                    "created",
                    "edited",
                    "code",
                    "print",
                    "edition",
                    "character",
                    "series",
                    "version",
                    "generation",
                }
                or card.get("game") not in LABELS
                or not re.fullmatch(r"[A-Za-z0-9]{1,32}", str(card.get("code", "")))
            ):
                raise GiveawayError("Invalid verified card.")
            key = card["game"], card["code"].casefold()
            if key in cards or prize.get("copies", 1) != 1:
                raise GiveawayError("Each card can be awarded only once.")
            cards.add(key)
            if not prize.get("image") or not re.fullmatch(
                "[a-f0-9]{64}", str(prize.get("proof", ""))
            ):
                raise GiveawayError("Card proof unavailable.")
            integer(prize.get("source_channel"), 1, 2**64 - 1, "source channel")
            integer(card.get("message"), 1, 2**64 - 1, "source message")
            for key in ("character", "series"):
                if not isinstance(card.get(key), str) or not 1 <= len(card[key]) <= 250:
                    raise GiveawayError("Invalid card name.")
        elif prize.get("kind") == "custom":
            if not isinstance(prize.get("name"), str) or not 1 <= len(prize["name"].strip()) <= 180:
                raise GiveawayError("Enter a prize, including the amount per winner.")
            integer(prize.get("copies", 1), 1, MAX_WINNERS, "winner count")
        else:
            raise GiveawayError("Invalid prize type.")
    if sum(len(prize_name(p)) for p in prizes) > 1000:
        raise GiveawayError("Shorten the prize names so the complete list fits.")
    if len(slots(result)) > MAX_WINNERS:
        raise GiveawayError("Choose at most 25 winner slots.")
    images = image_items(result)
    if len(images) > MAX_IMAGES:
        raise GiveawayError("Use up to ten images, totalling at most 25 MiB.")
    raw_images = [p.get("image") for p in prizes] + [result.get("banner")]
    for image in (i for i in raw_images if i is not None):
        if not isinstance(image, dict):
            raise GiveawayError("Invalid prize image.")
        if set(image) - {"sha", "size", "suffix", "mime", "width", "height"}:
            raise GiveawayError("Invalid prize image.")
        if not isinstance(image, dict) or not re.fullmatch(
            "[a-f0-9]{64}", str(image.get("sha", ""))
        ):
            raise GiveawayError("Invalid prize image.")
        integer(image.get("size"), 1, 10 * 1024 * 1024, "image size")
        if image.get("suffix") not in {".png", ".jpg", ".webp", ".gif"}:
            raise GiveawayError("Invalid prize image format.")
    if sum(i["size"] for _, i, _ in images) > 25 * 1024 * 1024:
        raise GiveawayError("Use up to ten images, totalling at most 25 MiB.")


    shown = dict(
        spec=result,
        id="0" * 16,
        host=2**64 - 1,
        entries={},
        ends=9_000_000_000_000,
        status="active",
        round=0,
        deliveries={},
        awards=[],
    )
    blocks = presentation(shown).pages[0].blocks
    text_units = sum(
        len(b.text.encode("utf-16-le")) // 2 + (b.level + 1 if b.kind == "heading" else 0)
        for b in blocks
    )
    shown["awards"] = [
        dict(slot=s["id"], prizes=s["prizes"], user=2**64 - 2 - i)
        for i, s in enumerate(slots(result))
    ]
    if text_units > 3900 or len(completion(shown).encode("utf-16-le")) // 2 > 1900:
        raise GiveawayError("Shorten the title, description or prize names so everything fits.")
    return result


def prize_name(prize):
    if prize["kind"] == "card":
        card = prize["card"]
        return f"{LABELS[card['game']]} · {escaped(card.get('character', ''))} · `{card['code']}`"
    return f"Custom · {escaped(prize['name'])}"


def bundle_prize(prize):
    name = prize_name(prize)
    if prize.get("copies", 1) > 1:
        name += f" × {prize['copies']}"
    return name


def slots(spec):
    if spec["mode"] == "bundle":
        return [dict(id="bundle", prizes=[p["id"] for p in spec["prizes"]])]
    return [
        dict(id=f"{p['id']}:{i}", prizes=[p["id"]])
        for p in spec["prizes"]
        for i in range(p.get("copies", 1))
    ]


def image_items(spec):
    result, seen = [], set()
    entries = [(p["id"], p.get("image"), prize_name(p)) for p in spec["prizes"]]
    entries += [("banner", spec.get("banner"), "Giveaway banner")]
    for key, image, alt in entries:
        if image is not None and (
            not isinstance(image, dict)
            or not re.fullmatch("[a-f0-9]{64}", str(image.get("sha", "")))
        ):
            raise GiveawayError("Invalid prize image.")
        if image and image.get("sha") not in seen:
            seen.add(image.get("sha"))
            result.append((key, image, alt[:1024]))
    return result


def file_name(record, image):
    return f"giveaway-{record['id']}-{image['sha'][:16]}{image['suffix']}"


def card_keys(spec):
    return {
        (p["card"]["game"], p["card"]["code"].casefold())
        for p in spec["prizes"]
        if p["kind"] == "card"
    }


def requirements(spec):
    lines = []
    for rule in [*spec.get("baseline", []), spec["rules"]]:
        if rule["roles"]:
            lines.append(
                ("All: " if rule["mode"] == "all" else "Any: ")
                + " · ".join(f"<@&{r}>" for r in rule["roles"])
            )
        if rule["excluded"]:
            lines.append("Excluded: " + " · ".join(f"<@&{r}>" for r in rule["excluded"]))
        for key, label in (("account_ms", "Account"), ("member_ms", "In server")):
            if rule[key]:
                lines.append(f"{label} ≥ {age_label(rule[key])}")
    return "\n".join(dict.fromkeys(lines))


def age_label(value):
    for amount, unit in ((DAY, "d"), (3_600_000, "h"), (60_000, "m")):
        if value % amount == 0:
            return f"{value // amount}{unit}"
    return f"{value // 1000}s"


def eligible_rules(spec, role_ids, created_ms, joined_ms, now):
    roleset = set(role_ids)
    for rule in [*spec.get("baseline", []), spec["rules"]]:
        required = set(rule["roles"])
        if roleset & set(rule["excluded"]):
            return False
        if required and not (required <= roleset if rule["mode"] == "all" else required & roleset):
            return False
        for limit, observed in ((rule["account_ms"], created_ms), (rule["member_ms"], joined_ms)):
            if limit and (observed is None or now - observed < limit):
                return False
    return True


def new_record(identifier, guild, host, channel, command, source_channel, draft_id, spec, now):
    return dict(
        version=2,
        id=identifier,
        guild=guild,
        host=host,
        channel=channel,
        command=command,
        source_channel=source_channel,
        draft_id=draft_id,
        spec=validate_spec(spec, complete=True),
        duration=spec["duration"],
        created=now,
        status="publishing",
        revision=1,
        message=0,
        ends=0,
        entries={},
        winner=None,
        winners=[],
        awards=[],
        round=0,
        draw=None,
        pending_edit=None,
        deliveries={"publish": dict(state="pending", message=0, at=now)},
        rendered="",
        error="",
        next_ms=now,
        ui_version=2,
    )


def upgrade(record):
    value = copy.deepcopy(record)
    card = dict(value["card"])
    spec = config_defaults(dict(required_role=value.get("required_role", 0)))
    spec.update(
        duration=value["duration"],
        prizes=[
            dict(
                id=value["id"],
                kind="card",
                card=card,
                copies=1,
                image=value["image"],
                proof=value["source_hash"],
                source_channel=value["source_channel"],
            )
        ],
    )
    value.update(
        version=2,
        spec=spec,
        awards=[],
        pending_edit=None,
        ui_version=1,
        draft_id="legacy:" + value["id"],
    )
    return value


def set_entry(record, actor, key, now, *, entered=True):
    receipts = record.setdefault("entry_receipts", {})
    if key in receipts:
        if receipts[key][0] != actor:
            raise GiveawayError("Unavailable.")
        return receipts[key][1]
    if record.get("pending_edit"):
        raise GiveawayError("Updating. Try again.")
    if record["status"] != "active" or now >= record["ends"]:
        raise GiveawayError("Entries closed.")
    if actor == record["host"]:
        raise GiveawayError("You cannot enter your own giveaway.")
    record["entries"][str(actor)] = entered
    receipts[key] = [actor, entered]
    record["next_ms"] = min(record["next_ms"], now + 5_000)
    return entered


def begin_draw(record, now, *, reroll=False, expected=None, selected=None):
    if record.get("pending_edit"):
        return False
    if expected is not None and record["revision"] != expected:
        raise GiveawayError("This giveaway changed.")
    if reroll:
        from .models import ready_to_reroll

        if not ready_to_reroll(record):
            raise GiveawayError("Reroll unavailable.")
        selected = selected or [a["slot"] for a in record["awards"] if a["user"]]
        if (
            not selected
            or len(set(selected)) != len(selected)
            or not set(selected) <= {a["slot"] for a in record["awards"] if a["user"]}
        ):
            raise GiveawayError("Select awarded prize slots.")
        selected = [a["slot"] for a in record["awards"] if a["slot"] in selected]
    elif record["status"] != "active" or now < record["ends"]:
        return False
    else:
        selected = [slot["id"] for slot in slots(record["spec"])]
    excluded = set(record["winners"]) | {record["host"]}
    order = [
        int(u) for u, entered in record["entries"].items() if entered and int(u) not in excluded
    ]
    secrets.SystemRandom().shuffle(order)
    record["revision"] += 1
    record.update(
        status="drawing",
        error="",
        next_ms=now,
        draw=dict(order=order, cursor=0, reroll=reroll, selected=selected, found=[]),
    )
    return True


def accept_candidates(record, users, eligible, now):
    if record["status"] != "drawing":
        raise GiveawayError("This giveaway changed.")
    draw = record["draw"]
    if (
        not users
        or users != draw["order"][draw["cursor"] : draw["cursor"] + len(users)]
        or not set(eligible) <= set(users)
    ):
        raise GiveawayError("Invalid draw result.")
    draw["found"].extend(u for u in users if u in eligible)
    draw["cursor"] += len(users)
    if len(draw["found"]) >= len(draw["selected"]) or draw["cursor"] == len(draw["order"]):
        finish_draw(record, now)
    record["next_ms"] = now


def finish_draw(record, now):
    draw = record["draw"]
    found = draw["found"][: len(draw["selected"])]
    if draw["reroll"] and len(found) < len(draw["selected"]):
        record.update(status="ended", draw=None, error="No other eligible entrants.", next_ms=now)
        return
    assignments = dict(zip(draw["selected"], found, strict=False))
    if draw["reroll"]:
        for award in record["awards"]:
            if award["slot"] in assignments:
                award["user"] = assignments[award["slot"]]
    else:
        record["awards"] = [
            dict(slot=s["id"], user=assignments.get(s["id"]), prizes=s["prizes"])
            for s in slots(record["spec"])
        ]
    record["winners"].extend(found)
    record["winner"] = next((a["user"] for a in record["awards"] if a["user"]), None)
    record["round"] += 1
    record.setdefault("draw_history", []).append(
        dict(round=record["round"], awards=copy.deepcopy(record["awards"]), at=now)
    )
    announced = [a for a in record["awards"] if not draw["reroll"] or a["slot"] in draw["selected"]]
    record["deliveries"][f"result:{record['round']}"] = dict(
        state="pending", message=0, at=now, awards=copy.deepcopy(announced)
    )
    record.update(status="ended", draw=None, next_ms=now)


def signature(record):
    from .models import count

    return hashlib.sha256(
        json.dumps(
            [
                record["spec"],
                record["revision"],
                record["status"],
                record["round"],
                record["ends"],
                count(record),
                record["awards"],
                record.get("error"),
            ],
            sort_keys=True,
            ensure_ascii=False,
        ).encode()
    ).hexdigest()


def presentation(record, *, urls=None, preview=False):
    from .models import count

    spec = record["spec"]
    if len(spec["prizes"]) == 1:
        first = spec["prizes"][0]
        default_title = first["name"] if first["kind"] == "custom" else first["card"]["character"]
    else:
        default_title = "Prize bundle"
    title = spec["title"] or default_title
    heading = escaped(title)
    blocks = [
        Block("heading", text=heading, level=2)
        if len(heading) <= 250
        else Block("text", text="## " + heading)
    ]
    if spec["description"]:
        blocks.append(Block("text", text=spec["description"]))
    media = []
    for _, image, alt in image_items(spec):
        filename = file_name(record, image)
        source = (urls or {}).get(filename)
        media.append(
            PresentationMedia(
                media_url=source, file_name=None if source else filename, description=alt
            )
        )
    if media and spec["layout"] == "image-first":
        blocks.append(Block("gallery", items=tuple(media)))
    prizes = []
    for p in spec["prizes"]:
        name = prize_name(p)
        if spec["mode"] == "separate" and p.get("copies", 1) > 1:
            name += f" each · {p['copies']} winners"
        elif spec["mode"] == "bundle" and p.get("copies", 1) > 1:
            name += f" × {p['copies']}"
        prizes.append(name)
    blocks.append(Block("text", text="\n".join(prizes)))
    winners = len(slots(spec))
    blocks.append(
        Block(
            "text",
            text=(
                f"<@{record['host']}> · {winners} "
                f"{'winner' if winners == 1 else 'winners'} · {count(record)} entries"
            ),
        )
    )
    if record.get("ends"):
        stamp = record["ends"] // 1000
        blocks.append(Block("text", text=f"Ends <t:{stamp}:F> · <t:{stamp}:R>"))
    if require := requirements(spec):
        blocks.extend((Block("separator"), Block("text", text=require)))
    if media and spec["layout"] == "text-first":
        blocks.append(Block("gallery", items=tuple(media)))
    blocks.append(
        Block(
            "action_row",
            controls=(
                Control(
                    "button",
                    "giveaway",
                    "join",
                    record["id"],
                    label="Join",
                    emoji="🎉",
                    style="primary",
                    disabled=preview or record["status"] != "active",
                ),
            ),
        )
    )
    return DiscordIntent(
        "send", pages=(PresentationPage(blocks=tuple(blocks)),), presentation_mode="v2"
    )


def completion(record):
    prizes = {p["id"]: p for p in record["spec"]["prizes"]}
    lines = [f"<@{record['host']}>"]
    grouped = {}
    for award in (
        record["deliveries"].get(f"result:{record['round']}", {}).get("awards", record["awards"])
    ):
        grouped.setdefault(tuple(award["prizes"]), []).append(award["user"])
    for keys, users in grouped.items():
        label = ", ".join(
            (bundle_prize if record["spec"]["mode"] == "bundle" else prize_name)(prizes[p])
            for p in keys
        )
        winners = [f"<@{u}>" for u in users if u]
        if len(winners) > 1:
            label += " each"
        lines.append(((" · ".join(winners) + " won ") if winners else "Unawarded · ") + label)
        missing = users.count(None)
        if missing and winners:
            lines.append(f"{missing} unawarded.")
    return "\n".join(lines)


def preset(spec):
    result = copy.deepcopy(spec)
    result["baseline"] = []

    result["prizes"] = [p for p in result["prizes"] if p["kind"] == "custom"]
    return result
