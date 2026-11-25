import hashlib
from datetime import datetime
from typing import Annotated, Any

import msgspec

from .discord_assets import attachment_url, profile_image, safe_image


class Observation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")]
    guild: int
    kind: Annotated[str, msgspec.Meta(max_length=48)]
    resource: int
    occurred: int
    order: int
    channel: int = 0
    actor: int = 0
    data: dict[str, Any] = msgspec.field(default_factory=dict)


def identifier(*parts: object) -> str:
    return hashlib.sha256(":".join(str(p) for p in parts).encode()).hexdigest()[:32]


def millis(value) -> int:
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return 0


def project(kind: str, data: dict, *, role_name=None) -> dict:
    def label(role_id):
        return (role_name(int(role_id)) if role_name is not None else None) or str(role_id)

    if kind.startswith("MESSAGE_"):
        result = {k: data[k] for k in ("content", "pinned") if k in data}
        author = data.get("author") or {}
        if author:
            result.update(
                subject=int(author.get("id") or 0),
                name=str(author.get("global_name") or author.get("username") or "Unknown"),
                bot=bool(author.get("bot")),
                webhook=bool(data.get("webhook_id")),
            )
            if author.get("avatar"):
                result["avatar"] = safe_image(
                    f"https://cdn.discordapp.com/avatars/{result['subject']}/{author['avatar']}.png"
                )
        if "attachments" in data:
            files = []
            for raw in data["attachments"][:10]:
                channel, attachment = int(data["channel_id"]), int(raw["id"])
                url = str(raw.get("url") or "")
                files.append(
                    dict(
                        id=attachment,
                        name=str(raw.get("filename") or "attachment")[:256],
                        size=max(0, int(raw.get("size") or 0)),
                        type=str(raw.get("content_type") or "")[:128],
                        url=url if attachment_url(url, channel, attachment) else "",
                        status="pending",
                    )
                )
            result["files"] = files
        result["message"] = int(data.get("id") or 0)
        result["edited"] = millis(data.get("edited_timestamp"))
        if kind == "MESSAGE_DELETE_BULK":
            result["ids"] = [int(x) for x in data.get("ids", [])[:100]]
        return result
    if kind.startswith("GUILD_MEMBER_"):
        user = data.get("user") or {}
        result = {k: data[k] for k in ("nick", "joined_at", "pending") if k in data}
        result.update(
            subject=int(user.get("id") or 0),
            name=str(user.get("global_name") or user.get("username") or "Unknown"),
            bot=bool(user.get("bot")),
        )
        if "username" in user:
            result["username"] = user["username"]
        if "global_name" in user:
            result["global_name"] = user["global_name"]
        if "roles" in data:
            result["role_ids"] = [int(x) for x in data["roles"][:250]]
            result["roles"] = {str(x): label(x) for x in result["role_ids"]}
        if "communication_disabled_until" in data:
            result["until"] = millis(data["communication_disabled_until"])
        if "avatar" in user:
            result["user_avatar"] = (
                profile_image(
                    f"https://cdn.discordapp.com/avatars/{result['subject']}/{user['avatar']}.png"
                )
                if user["avatar"]
                else None
            )
        if "avatar" in data:
            result["guild_avatar"] = (
                profile_image(
                    f"https://cdn.discordapp.com/guilds/{data.get('guild_id')}/users/{result['subject']}/avatars/{data['avatar']}.png"
                )
                if data["avatar"]
                else None
            )
        return result
    if kind == "VOICE_STATE_UPDATE":
        result = {
            key: data[key]
            for key in (
                "channel_id",
                "self_mute",
                "self_deaf",
                "mute",
                "deaf",
                "self_video",
                "self_stream",
                "suppress",
            )
            if key in data
        }
        result["subject"] = int(data["user_id"])
        member = data.get("member") or {}
        user = member.get("user") or {}
        result["name"] = str(
            member.get("nick")
            or user.get("global_name")
            or user.get("username")
            or result["subject"]
        )
        return result
    if kind == "GUILD_AUDIT_LOG_ENTRY_CREATE":
        result = {k: data.get(k) for k in ("id", "action_type", "target_id", "user_id", "reason")}

        allowed = {
            "name",
            "nick",
            "topic",
            "rate_limit_per_user",
            "nsfw",
            "permissions",
            "allow",
            "deny",
            "communication_disabled_until",
            "$add",
            "$remove",
            "color",
            "hoist",
            "mentionable",
            "archived",
            "locked",
            "verification_level",
            "explicit_content_filter",
            "default_message_notifications",
            "mfa_level",
            "afk_timeout",
        }
        changes = []
        for c in data.get("changes", [])[:25]:
            if c.get("key") not in allowed:
                continue
            item = {k: c[k] for k in ("key", "old_value", "new_value") if k in c}
            if c["key"] in {"$add", "$remove"}:
                item["new_value"] = [
                    {"id": int(r["id"]), "name": str(r.get("name") or r["id"])[:256]}
                    for r in c.get("new_value", [])[:25]
                ]
            changes.append(item)
        result["changes"] = changes
        result["options"] = {
            k: v
            for k, v in (data.get("options") or {}).items()
            if k in {"channel_id", "count", "message_id", "id", "type", "role_name"}
        }
        return result
    if kind in {"GUILD_BAN_ADD", "GUILD_BAN_REMOVE"}:
        u = data.get("user") or {}
        return dict(
            subject=int(u.get("id") or 0),
            name=str(u.get("global_name") or u.get("username") or "Unknown"),
        )
    if kind in {"GUILD_EMOJIS_UPDATE", "GUILD_STICKERS_UPDATE"}:
        key = "emojis" if kind == "GUILD_EMOJIS_UPDATE" else "stickers"
        return {
            "items": [
                {k: item[k] for k in ("id", "name", "animated") if k in item}
                for item in data.get(key, [])[:500]
            ]
        }
    raw = data.get("role") or data
    allowed = (
        "name",
        "type",
        "topic",
        "parent_id",
        "position",
        "nsfw",
        "rate_limit_per_user",
        "archived",
        "locked",
        "auto_archive_duration",
        "default_auto_archive_duration",
        "default_thread_rate_limit_per_user",
        "permissions",
        "color",
        "hoist",
        "mentionable",
        "verification_level",
        "explicit_content_filter",
        "default_message_notifications",
        "mfa_level",
        "afk_timeout",
        "system_channel_id",
        "afk_channel_id",
        "rules_channel_id",
        "public_updates_channel_id",
        "description",
        "scheduled_start_time",
        "scheduled_end_time",
        "status",
        "icon",
        "banner",
    )
    result = {k: raw[k] for k in allowed if k in raw}
    if kind.startswith("GUILD_ROLE_"):
        result.pop("position", None)
    metadata = raw.get("thread_metadata") or {}
    result.update(
        {k: metadata[k] for k in ("archived", "locked", "auto_archive_duration") if k in metadata}
    )
    if "permission_overwrites" in raw:
        result["overwrites"] = {
            str(x["id"]): {
                **{k: x[k] for k in ("type", "allow", "deny")},
                "label": label(x["id"]),
            }
            for x in raw["permission_overwrites"][:100]
        }

    return result
