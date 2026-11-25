from __future__ import annotations

import hashlib
import re
import secrets
import shlex
from datetime import datetime

import emoji
import msgspec

from lumi.contracts import PurgeBoundary, PurgeFlag, PurgeSpec, PurgeText
from lumi.shared.discord_message_facts import body_facts
from .rules import identifier

FLAGS = frozenset(
    {
        "bots",
        "webhooks",
        "files",
        "images",
        "videos",
        "audio",
        "links",
        "invites",
        "embeds",
        "mentions",
        "replies",
        "stickers",
        "emoji",
        "duplicates",
    }
)
TEXT = frozenset({"contains", "not", "exact", "starts", "ends"})
AUTHORS = {
    "user": "users",
    "role": "roles",
    "exclude_user": "exclude_users",
    "exclude_role": "exclude_roles",
}
ALIASES = {
    "users": "user",
    "roles": "role",
    "exclude_users": "exclude_user",
    "exclude_roles": "exclude_role",
    "attachments": "files",
    "to": "after",
}
LINK = re.compile(r"https?://[^\s<>]+|(?:www\.)?discord(?:\.gg/|(?:app)?\.com/invite/)\S+", re.I)
INVITE = re.compile(
    r"(?:https?://)?(?:www\.)?discord(?:\.gg/|(?:app)?\.com/invite/)[A-Za-z0-9-]+", re.I
)
CUSTOM_EMOJI = re.compile(r"<a?:[A-Za-z0-9_]{1,32}:[0-9]{17,20}>")
UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
MAX_QUERY = 2000
DISCORD_EPOCH_MS = 1_420_070_400_000
CONFIRM_MS = 900_000
HELP = (
    "{p}purge <1-100000|channel> [filters]\n"
    "{p}purge to:<message> · time:2hs · after:<message>\n"
    "user:@user role:@role bots:on files:off links:on\n"
    'contains:"text" not:"text" exact:"text" starts:"text" ends:"text"\n'
    "images videos audio invites embeds mentions replies stickers emoji duplicates: on/off\n"
    "before:<message> after:<message> to:<message> time:2hs older:7d newer:1h pins:on/off\n"
    "exclude_user:<IDs> exclude_role:<IDs> · Lists: commas · Filters: AND\n"
    "{p}purge status [id] · cancel [id] · resume <id>\n"
    "Count limits messages inspected. Pins stay by default."
)


class PurgeError(ValueError):
    pass


def age(value):
    pattern = (
        r"([0-9]{1,9})(seconds?|secs?|s|minutes?|mins?|ms?|hours?|hrs?|hs?|days?|ds?|weeks?|ws?)"
    )
    parts = list(re.finditer(pattern, value, re.I))
    if not parts or "".join(m[0] for m in parts).casefold() != value.casefold():
        raise PurgeError("Use a duration such as 1h30m.")
    units = [m[2][0].casefold() for m in parts]
    result = sum(int(m[1]) * UNITS[unit] for m, unit in zip(parts, units, strict=True))
    if len(set(units)) != len(units) or not 1 <= result <= 36500 * 86400:
        raise PurgeError("Invalid duration.")
    return result


def timestamp(value):
    discord_time = re.fullmatch(r"<t:([0-9]{9,11})(?::[tTdDfFR])?>", value)
    if discord_time:
        return int(discord_time[1]) * 1000
    if re.fullmatch(r"[0-9]{10}|[0-9]{13}", value):
        return int(value) * (1000 if len(value) == 10 else 1)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        result = int(parsed.timestamp() * 1000)
        if result <= 0:
            raise ValueError
        return result
    except (ValueError, OverflowError, OSError) as exc:
        raise PurgeError(
            "Use a duration, Discord timestamp, Unix timestamp or date-time with timezone."
        ) from exc


def boundary(value):
    if re.fullmatch(r"[0-9]{17,20}", value):
        return PurgeBoundary(int(value))
    match = re.fullmatch(
        r"https://(?:(?:canary|ptb)\.)?discord(?:app)?\.com/channels/"
        r"([0-9]{17,20})/([0-9]{17,20})/([0-9]{17,20})",
        value,
    )
    if not match:
        raise PurgeError("Use a message ID or Discord message link.")
    return PurgeBoundary(int(match[3]), int(match[1]), int(match[2]))


def parse(raw):

    if not isinstance(raw, str) or len(raw) > MAX_QUERY:
        raise PurgeError("Query is too long.")
    try:
        tokens = shlex.split(raw, posix=True)
    except ValueError as exc:
        raise PurgeError("Check quotation marks.") from exc
    if not tokens or tokens == ["help"]:
        return "help", None, None
    first_token = tokens.pop(0)
    first = first_token.casefold()
    if first in {"status", "cancel", "resume"}:
        if len(tokens) > 1 or (tokens and not re.fullmatch(r"[a-f0-9]{8}", tokens[0])):
            raise PurgeError("Use an eight-character purge ID.")
        if first == "resume" and not tokens:
            raise PurgeError("Include the purge ID.")
        return first, tokens[0] if tokens else None, None
    implicit_scope = first != "channel" and not re.fullmatch(r"[0-9]{1,6}", first)
    if implicit_scope:
        if not re.match(r"[a-zA-Z_]+[:=]", first_token):
            raise PurgeError("Use a count, channel, or message/time boundary.")
        tokens.insert(0, first_token)
    limit = None if first == "channel" or implicit_scope else int(first)
    if limit is not None and not 1 <= limit <= 100000:
        raise PurgeError("Count: 1-100000.")
    data, flags, texts, seen = {"limit": limit}, [], [], set()
    for token in tokens:
        match = re.fullmatch(r"([a-zA-Z_]+)[:=]([\s\S]+)", token)
        if not match:
            raise PurgeError("Use filter:value.")
        key, value = ALIASES.get(match[1].lower(), match[1].lower()), match[2]
        if key in seen:
            raise PurgeError("Use each filter once.")
        seen.add(key)
        if key in AUTHORS:
            identifiers = []
            for item in value.split(","):
                pattern = (
                    r"(?:<@&([0-9]{17,20})>|([0-9]{17,20}))"
                    if "role" in key
                    else r"(?:<@!?([0-9]{17,20})>|([0-9]{17,20}))"
                )
                m = re.fullmatch(pattern, item)
                if not m:
                    raise PurgeError("Use mentions or IDs, separated by commas.")
                identifiers.append(int(m[1] or m[2]))
            if len(identifiers) > 25:
                raise PurgeError("Use at most 25 users or roles per filter.")
            data[AUTHORS[key]] = tuple(sorted(set(identifiers)))
        elif key in FLAGS or key == "pins":
            if value.casefold() not in {"on", "off", "true", "false"}:
                raise PurgeError("Use on or off.")
            enabled = value.casefold() in {"on", "true"}
            if key == "pins":
                data["keep_pins"] = enabled
            else:
                flags.append(PurgeFlag(key, enabled))
        elif key in TEXT:
            if not value.strip() or len(value) > 256:
                raise PurgeError("Text: 1-256 characters.")
            texts.append(PurgeText(key, value))
        elif key in {"before", "after"}:
            data[key] = boundary(value)
        elif key in {"older", "newer"}:
            data[key + "_seconds"] = age(value)
        elif key == "time":
            try:
                data["newer_seconds"] = age(value)
            except PurgeError:
                data["since_ms"] = timestamp(value)
        else:
            raise PurgeError("Unknown filter: " + key + ".")
    spec = PurgeSpec(
        **data,
        flags=tuple(sorted(flags, key=lambda f: f.name)),
        text=tuple(sorted(texts, key=lambda t: t.name)),
    )
    if implicit_scope and not (
        spec.before
        or spec.after
        or spec.older_seconds
        or spec.newer_seconds
        or spec.since_ms is not None
    ):
        raise PurgeError("Include a count, channel, or message/time boundary.")
    if set(spec.users) & set(spec.exclude_users) or set(spec.roles) & set(spec.exclude_roles):
        raise PurgeError("Conflicting author filters.")
    if spec.older_seconds and spec.newer_seconds and spec.older_seconds >= spec.newer_seconds:
        raise PurgeError("The age window is empty.")
    if spec.before and spec.after and spec.after.message_id >= spec.before.message_id:
        raise PurgeError("The message window is empty.")
    if "time" in seen and "newer" in seen:
        raise PurgeError("Use time or newer, not both.")
    return "start", None, spec


def reset(spec):
    return (
        spec.limit is None
        and not spec.keep_pins
        and not (
            spec.users
            or spec.roles
            or spec.exclude_users
            or spec.exclude_roles
            or spec.flags
            or spec.text
            or spec.before
            or spec.after
            or spec.older_seconds
            or spec.newer_seconds
            or spec.since_ms is not None
        )
    )


def build_plan(spec, *, guild, channel, actor, source, created, admitted, now):

    before, after = source, 0
    for key in ("before", "after"):
        reference = getattr(spec, key)
        if reference is not None:
            if reference.guild_id not in (None, guild) or reference.channel_id not in (None, channel):
                raise PurgeError("Use a message from this channel.")
            if key == "before":
                before = min(before, reference.message_id)
            else:
                after = reference.message_id
    frozen = created if type(created) is int and 0 < created <= admitted else admitted
    if spec.older_seconds:
        before = min(before, (frozen - spec.older_seconds * 1000 - DISCORD_EPOCH_MS) << 22)
    lower = spec.since_ms or (frozen - spec.newer_seconds * 1000 if spec.newer_seconds else 0)
    if lower:
        if lower >= frozen:
            raise PurgeError("Time must be before this command.")
        after = max(after, ((lower - DISCORD_EPOCH_MS) << 22) | ((1 << 22) - 1))
    if after >= before:
        raise PurgeError("The message window is empty.")
    return dict(
        key=identifier(guild, source, "purge"), guild=guild, channel=channel, actor=actor,
        source=source, created=now, cutoff=frozen, spec=msgspec.to_builtins(spec),
        before=before, after=after, cursor=before, inspected=0, deleted=0, skipped=0,
        unconfirmed=0, pending=[], individual=[], sending=None, exhausted=False,
        state="confirm" if spec.limit is None else "queued", mode="reset" if reset(spec) else "purge",
        token=secrets.token_hex(8), expires=now + CONFIRM_MS, message=0, rendered="", stage="prepare",
    )


def duplicate_key(message):
    return duplicate_observation(body_facts(message))


def duplicate_observation(facts):
    if facts.get("restricted"):
        return None
    content = facts["content"]
    if not content.strip() or any(facts[k] for k in ("attachment_types", "embeds", "stickers", "components", "poll")):
        return None
    return hashlib.sha256(f"{facts['author']}\0{content}".encode()).hexdigest()


def matches(message, spec, roles=None):

    return matches_observation(dict(body_facts(message), roles=roles), spec)


def matches_observation(facts, spec):

    if facts.get("restricted") or (facts["pinned"] and spec.keep_pins) or not facts["deletable"]:
        return False
    author = facts["author"]
    if (spec.users and author not in spec.users) or author in spec.exclude_users:
        return False
    webhook = facts["webhook"]
    if spec.roles or spec.exclude_roles:
        roles = facts.get("roles")
        if webhook or roles is None:
            return False
        if (spec.roles and not set(spec.roles).intersection(roles)) or set(
            spec.exclude_roles
        ).intersection(roles):
            return False
    content = facts["content"]

    def media(kind):
        return facts.get(f"{kind}_embed", False) or any(t.startswith(kind + "/") for t in facts["attachment_types"])

    for flag in spec.flags:
        match flag.name:
            case "duplicates":
                continue
            case "bots":
                value = facts["bot"] or webhook
            case "webhooks":
                value = webhook
            case "files":
                value = bool(facts["attachment_types"])
            case "images" | "videos":
                value = media(flag.name[:-1])
            case "audio":
                value = media("audio")
            case "links":
                value = bool(LINK.search(content))
            case "invites":
                value = bool(INVITE.search(content))
            case "replies":
                value = facts["reference"]
            case "emoji":
                value = bool(CUSTOM_EMOJI.search(content) or emoji.emoji_list(content))
            case _:
                value = facts[flag.name]
        if value != flag.enabled:
            return False
    folded = content.casefold()
    for text in spec.text:
        needle = text.value.casefold()
        tests = {
            "contains": needle in folded,
            "not": needle not in folded,
            "exact": needle == folded,
            "starts": folded.startswith(needle),
            "ends": folded.endswith(needle),
        }
        if not tests[text.name]:
            return False
    return True
