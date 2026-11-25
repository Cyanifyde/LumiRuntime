from __future__ import annotations

import copy

from lumi.shared.discord_observations import millis as millis

from .models import Observation, category, identifier, profile_image, role_changes
from .permissions import FLAGS
from .permissions import labels as permission_labels


def permission_changes(old, new):
    result = {}
    for target in old.keys() | new.keys():
        a, b = old.get(target, {}), new.get(target, {})
        for name, bit in FLAGS.items():
            if name in {
                "read_messages",
                "external_emojis",
                "external_stickers",
                "manage_permissions",
                "use_slash_commands",
            }:
                continue

            def state(item, bit=bit):
                return (
                    "Allow"
                    if int(item.get("allow", 0)) & bit
                    else "Deny"
                    if int(item.get("deny", 0)) & bit
                    else "Inherit"
                )

            if state(a) != state(b):
                label = b.get("label") or a.get("label") or target
                result[f"{label} · {name.replace('_', ' ').title()}"] = [state(a), state(b)]
    return result


def snapshot_key(obs: Observation) -> tuple[str, int | str]:
    k = obs.kind
    if k == "LUMI_OPERATION" and obs.data.get("id"):

        return k, obs.data["id"]
    if k.startswith("MESSAGE_"):
        return "message", obs.resource
    if k.startswith("GUILD_MEMBER_"):
        return "member", obs.resource
    if k == "VOICE_STATE_UPDATE":
        return "voice", obs.resource
    if k.startswith("GUILD_ROLE_"):
        return "role", obs.resource
    if k.startswith(("CHANNEL_", "THREAD_")):
        return "channel", obs.resource
    if k.startswith("GUILD_SCHEDULED_EVENT_"):
        return "scheduled_event", obs.resource
    return k.removesuffix("_UPDATE"), obs.resource


def interpret(obs: Observation, before: dict | None) -> tuple[dict, list[dict]]:
    d = copy.deepcopy(obs.data)
    baseline = d.pop("_baseline", False)
    baseline_order = d.pop("_baseline_order", 0)
    if baseline and before:

        if before.get("_order", 0) >= baseline_order:
            return before, []
    current = {**(before or {}), **d, "_order": baseline_order if baseline else obs.order}
    if "user_avatar" in d or "guild_avatar" in d:
        for key in ("user_avatar", "guild_avatar"):
            if key in current:
                current[key] = profile_image(current[key])
        current["avatar"] = current.get("guild_avatar") or current.get("user_avatar")
        d["avatar"] = current["avatar"]
    if baseline:
        return current, []
    if before and before.get("_order", 0) >= obs.order:
        return before, []
    if obs.kind == "WEBHOOKS_UPDATE":


        return current, []
    result = []

    def emit(kind, **extra):
        row = dict(
            id=identifier(obs.id, kind, extra.get("subject", obs.resource)),
            guild=obs.guild,
            kind=kind,
            category=category(kind),
            subject=int(
                current.get("subject") or (0 if obs.kind.startswith("MESSAGE_") else obs.resource)
            ),
            actor=obs.actor,
            channel=obs.channel,
            message=int(current.get("message") or 0),
            created=obs.occurred,
            revision=1,
            name=current.get("name") or str(obs.resource),
            avatar=current.get("avatar"),
            channel_name=current.get("channel_name", ""),
            nsfw=bool(current.get("nsfw")),
            parent_id=int(current.get("parent_id") or 0),
            bot=bool(current.get("bot") or current.get("webhook")),
            webhook=bool(current.get("webhook")),
            files=[],
        )
        row.update(extra)
        row["format"] = "v2"
        result.append(row)

    k = obs.kind
    if k == "MESSAGE_CREATE":
        return current, []
    if k == "MESSAGE_UPDATE":
        if "content" in d and (before is None or before.get("content") != d["content"]):
            emit(
                "message_edit",
                before=(before or {}).get("content", "Earlier text unavailable."),
                after=d["content"],
                previous_unknown="content" not in (before or {}),
            )
        old_files = {f["id"]: f for f in (before or {}).get("files", [])}
        new_files = {f["id"]: f for f in current.get("files", [])}
        if "files" in d and old_files.keys() != new_files.keys():
            added = [new_files[x] for x in new_files.keys() - old_files.keys()]
            removed = [old_files[x] for x in old_files.keys() - new_files.keys()]
            emit("attachments", added=added, removed=removed, files=added + removed)
        if "pinned" in d and before is not None and bool(before.get("pinned")) != bool(d["pinned"]):
            emit("pin" if d["pinned"] else "unpin", body=current.get("content", ""))
    elif k == "MESSAGE_DELETE":
        if not (before or {}).get("_deleted"):
            emit(
                "message_delete",
                id=identifier("deleted", obs.guild, obs.resource),
                body=current.get("content", "Earlier text unavailable."),
                files=current.get("files", []),
            )
        current["_deleted"] = True
    elif k == "MESSAGE_DELETE_BULK":
        emit("bulk_delete", ids=d.get("ids", []), total=len(d.get("ids", [])))
    elif k == "GUILD_MEMBER_ADD":
        if before and not before.get("_deleted") and before.get("joined_at") == d.get("joined_at"):
            return current, []
        current.pop("_deleted", None)
        current["_joined"] = obs.occurred
        emit("join", age=max(0, obs.occurred - ((obs.resource >> 22) + 1420070400000)))
    elif k == "GUILD_MEMBER_REMOVE":
        emit(
            "depart",
            stay=max(0, obs.occurred - millis(current.get("joined_at")))
            if current.get("joined_at")
            else None,
            removed=list(current.get("roles", {}).values()),
        )
        current["_deleted"] = True
    elif k == "GUILD_MEMBER_UPDATE":
        for key, event in (
            ("nick", "nickname"),
            ("username", "username"),
            ("global_name", "display_name"),
            ("avatar", "avatar"),
        ):
            if key in d and (before is None or before.get(key) != d[key]):
                if event == "avatar":


                    previous = profile_image((before or {}).get(key))
                    latest = profile_image(d[key])
                    if before is not None and key in before and previous != latest:
                        emit(event, image_before=previous, image_after=latest)
                else:
                    emit(
                        event,
                        before=(before or {}).get(key),
                        after=d[key],
                        previous_unknown=before is None,
                    )
        if "role_ids" in d and (
            before is None or set(before.get("role_ids", [])) != set(d["role_ids"])
        ):
            old = (before or {}).get("roles", {})
            new = current.get("roles", {})
            emit(
                "member_roles",
                added=[new[x] for x in new.keys() - old.keys()] if before is not None else [],
                removed=[old[x] for x in old.keys() - new.keys()],
                previous_unknown=before is None,
                current_roles=list(new.values()),
            )
        if "until" in d and (before or {}).get("until", 0) != d["until"]:
            active = bool(d["until"] and d["until"] > obs.occurred)
            event = (
                "timeout_change"
                if active and (before or {}).get("until", 0) > obs.occurred
                else "timeout"
                if active
                else "untimeout"
            )
            emit(
                event,
                until=d["until"],
                before=(before or {}).get("until"),
                duration=max(0, d["until"] - obs.occurred),
            )
    elif k == "VOICE_STATE_UPDATE":
        if before is None and "_voice_before" in d:
            before = d["_voice_before"]
            current.pop("_voice_before", None)
        old_channel = (before or {}).get("channel_id")
        new_channel = current.get("channel_id")
        if old_channel != new_channel:
            if old_channel and new_channel:
                emit(
                    "voice_move",
                    **{
                        "from": (before or {}).get("voice_name")
                        or d.get("old_voice_name")
                        or str(old_channel),
                        "to": current.get("voice_name") or str(new_channel),
                    },
                )
            elif new_channel:
                current["_connected"] = obs.occurred
                emit("voice_join")
            elif old_channel:
                emit(
                    "voice_leave",
                    stay=obs.occurred - before["_connected"] if before.get("_connected") else None,
                    channel=int(old_channel),
                )
        labels = {
            "self_mute": "Microphone muted",
            "self_deaf": "Self deafened",
            "mute": "Server muted",
            "deaf": "Server deafened",
            "suppress": "Stage suppressed",
        }
        changes = {
            label: [before.get(key), d[key]]
            for key, label in labels.items()
            if before is not None and key in d and before.get(key) != d[key]
        }
        if changes:
            emit("voice_state", changes=changes)
        for key, kind in (("self_video", "camera"), ("self_stream", "stream")):
            if before is not None and key in d and before.get(key) != d[key]:
                emit(kind, changes={key: [before.get(key), d[key]]})
    elif k in {"GUILD_BAN_ADD", "GUILD_BAN_REMOVE"}:
        emit("ban" if k.endswith("ADD") else "unban")
    elif k in {"GUILD_EMOJIS_UPDATE", "GUILD_STICKERS_UPDATE"}:
        previous = {int(x["id"]): x for x in (before or {}).get("items", [])}
        latest = {int(x["id"]): x for x in d.get("items", [])}
        if before is not None:
            for rid in previous.keys() | latest.keys():
                if previous.get(rid) != latest.get(rid):
                    item = latest.get(rid) or previous[rid]
                    emit(
                        "emoji" if "EMOJIS" in k else "sticker",
                        subject=rid,
                        name=item.get("name", str(rid)),
                        operation="created"
                        if rid not in previous
                        else "deleted"
                        if rid not in latest
                        else "updated",
                    )
    elif k == "GUILD_AUDIT_LOG_ENTRY_CREATE":
        return current, []
    elif k == "LUMI_OPERATION":
        emit(d["event"], **{key: v for key, v in d.items() if key not in {"event", "_order"}})
    elif k.endswith(("CREATE", "DELETE", "UPDATE")):
        group = (
            "role"
            if "ROLE" in k
            else "channel"
            if k.startswith(("CHANNEL", "THREAD"))
            else "scheduled_event"
            if "SCHEDULED_EVENT" in k
            else "guild"
            if k == "GUILD_UPDATE"
            else "invite"
            if "INVITE" in k
            else "webhook"
            if "WEBHOOK" in k
            else "integration"
        )
        operation = (
            "created" if k.endswith("CREATE") else "deleted" if k.endswith("DELETE") else "updated"
        )
        event = (
            group
            + (
                "_create"
                if operation == "created"
                else "_delete"
                if operation == "deleted"
                else "_update"
            )
            if group in {"channel", "role", "guild"}
            else group
        )
        changes = {
            key.replace("_", " ").title(): [(before or {}).get(key), v]
            for key, v in d.items()
            if key not in {"channel_name", "nsfw", "name", "resource_type", "parent", "bot"}
            and (before or {}).get(key) != v
        }
        if "name" in d and before and before.get("name") != d["name"]:
            changes["Name"] = [before.get("name"), d["name"]]
        changes.pop("Overwrites", None)
        changes = role_changes(event, changes)
        if (
            before is not None
            and "overwrites" in before
            and "overwrites" in d
            and before.get("overwrites") != d["overwrites"]
        ):
            emit(
                "permissions",
                changes=permission_changes(before.get("overwrites", {}), d["overwrites"]),
            )
        if "permissions" in d and before and before.get("permissions") != d["permissions"]:
            changes["Permissions"] = [
                permission_labels(x) for x in (before.get("permissions"), d["permissions"])
            ]
        if "nsfw" in d and before and before.get("nsfw") != d["nsfw"]:
            changes["Age restricted"] = [before.get("nsfw"), d["nsfw"]]
        if before is None and operation == "updated" and group in {"channel", "role", "guild"}:
            return current, []
        if operation != "updated" or changes or group in {"webhook", "integration", "invite"}:
            emit(
                event,
                operation=operation,
                changes=changes,
                resource_type=current.get("resource_type"),
                parent=current.get("parent"),
            )
    return current, result
