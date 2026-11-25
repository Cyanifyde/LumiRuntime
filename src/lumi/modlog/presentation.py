from __future__ import annotations

from lumi.contracts import PresentationBlock as Block
from lumi.contracts import (
    PresentationField as Field,
)
from lumi.contracts import (
    PresentationMedia,
    PresentationPage,
)

from .message_diff import edit_text
from .models import (
    TITLES,
    clip,
    escape,
    message_text,
    profile_image,
    role_changes,
    safe_image,
    text_chunks,
    units,
)


def detail_fields(record):
    edited = record.get("kind") == "message_edit"
    result = (
        [Field(label, text) for label, text in edit_text(record, limit=700, full=True)]
        if edited
        else []
    )
    sources = [
        (key.title(), record[key], key != "reason" and record.get("category") == "messages")
        for key in ("before", "after", "body", "reason")
        if isinstance(record.get(key), str)
        and record[key].strip()
        and not (edited and key in {"before", "after"})
    ]
    sources.extend(
        (str(key), f"{pair[0]} → {pair[1]}", False)
        for key, pair in role_changes(record["kind"], record.get("changes", {})).items()
    )
    for key in ("added", "removed"):
        if record.get(key):
            sources.append(
                (
                    key.title(),
                    "\n".join(
                        str(x.get("name", "attachment")) if isinstance(x, dict) else str(x)
                        for x in record[key]
                    ),
                    False,
                )
            )
    for label, source, native in sources:
        text = (message_text if native else escape)(source, 1000000)
        pieces = text_chunks(text)
        result.extend(
            Field(escape(label, 160) + (f" · {i + 1}" if len(pieces) > 1 else ""), chunk)
            for i, chunk in enumerate(pieces)
        )
    return result


def duration(ms: int) -> str:
    seconds = max(0, ms // 1000)
    for amount, label in ((86400, "d"), (3600, "h"), (60, "m")):
        if seconds >= amount:
            return f"{seconds // amount}{label}"
    return f"{seconds}s"


def value(v) -> str:
    if v is None:
        return "None"
    if isinstance(v, bool):
        return "On" if v else "Off"
    return escape(v)


def context_line(record: dict) -> str:

    kind = record["kind"]
    name = escape(record.get("name") or "Unknown", 200)
    subject = int(record.get("subject") or 0)
    channel = int(record.get("channel") or 0)
    parts = []
    if record.get("category") == "messages":
        if kind != "bulk_delete":
            author = name if record.get("webhook") else f"<@{subject}>" if subject else "Unknown"
            parts.append("From: " + author)
    elif record.get("category") in {"members", "moderation", "voice"}:
        if subject:
            parts.append(f"Member: <@{subject}>")
    elif kind.startswith("role_"):
        role = f"<@&{subject}>" if subject and kind != "role_delete" else name
        parts.append("Role: " + role)
    elif kind.startswith("channel_") or kind == "permissions":
        target = f"<#{subject}>" if subject and kind != "channel_delete" else name
        parts.append("Channel: " + target)
    else:
        parts.append(name)
    if channel and record.get("category") != "server":
        parts.append(f"In: <#{channel}>")
    if record.get("actor"):
        parts.append(f"By: <@{int(record['actor'])}>")
    return "\u2003".join(parts)


def render(record: dict, *, files: dict | None = None, private=False, page=0) -> PresentationPage:
    kind = record["kind"]
    if kind == "avatar":
        record = {
            **record,
            "image_before": profile_image(record.get("image_before")),
            "image_after": profile_image(record.get("image_after")),
        }
    title = TITLES.get(kind, "Server changed")
    fields, sections = [], []
    body = ""
    if kind == "message_edit":
        fields.extend(Field(label, text) for label, text in edit_text(record))
    elif kind in {"message_delete", "pin", "unpin"}:
        body = message_text(record.get("body", "Earlier text unavailable.") or "")
        if not body.strip():
            body = ""
    elif kind == "bulk_delete":
        body = f"**{record.get('total', 0)} messages deleted**"
    elif kind == "attachments":
        for label in ("added", "removed"):
            if record.get(label):
                fields.append(
                    Field(
                        label.title(), "\n".join(escape(f["name"], 150) for f in record[label][:5])
                    )
                )
    elif kind == "join":
        if record.get("age") is not None:
            body = f"Account age · {duration(record['age'])}"
    elif kind == "depart":
        if record.get("stay") is not None:
            body = f"Member for {duration(record['stay'])}"
        if record.get("removed"):
            fields.append(
                Field("Roles", clip(" · ".join(escape(x) for x in record["removed"][:10]), 800))
            )
    elif kind in {"nickname", "username", "display_name"}:
        previous = (
            "Earlier value unavailable"
            if record.get("previous_unknown")
            else value(record.get("before"))
        )
        body = clip(
            f"{previous} → {value(record.get('after'))}",
            800,
        )
    elif kind == "member_roles":
        if record.get("previous_unknown"):
            fields.append(
                Field(
                    "Current roles",
                    clip(" · ".join(escape(x) for x in record.get("current_roles", [])[:10]), 800)
                    or "None",
                )
            )
            body = "Earlier roles unavailable."
        for key in ("added", "removed"):
            if record.get(key):
                fields.append(
                    Field(key.title(), clip(" · ".join(escape(x) for x in record[key][:10]), 800))
                )
    elif kind in {
        "warn",
        "unwarn",
        "kick",
        "ban",
        "unban",
        "timeout",
        "timeout_change",
        "untimeout",
    }:
        if kind in {"timeout", "timeout_change"} and record.get("until"):
            body = f"Until <t:{record['until'] // 1000}:f>"
            if record.get("duration"):
                body = f"**{duration(record['duration'])}** · " + body
            if kind == "timeout_change" and record.get("before"):
                fields.append(Field("Previous expiry", f"<t:{int(record['before']) // 1000}:f>"))
        if record.get("reason"):
            fields.append(Field("Reason", escape(record["reason"])))
        if record.get("warning"):
            fields.append(Field("Warning", escape(record["warning"], 80)))
    elif kind in {"purge", "rename", "rename_undo"}:
        counts = record.get("counts", {})
        body = " · ".join(
            f"**{int(number)}** {escape(key, 32)}"
            for key, number in counts.items()
            if number or key in {"deleted", "changed", "restored"}
        )
    elif kind == "imageguard":
        body = escape(record.get("group") or "ImageGuard")
        fields.extend(Field(str(k).title(), escape(v)) for k, v in record.get("counts", {}).items())
        if record.get("incident_url"):
            body += f"\n[Incident]({record['incident_url']})"
    elif kind in {"voice_join", "voice_leave", "voice_move"}:
        if kind == "voice_move":
            body = (
                f"{escape(record.get('from', 'Unknown'))} → {escape(record.get('to', 'Unknown'))}"
            )
        if kind == "voice_leave" and record.get("stay") is not None:
            body = f"Connected for {duration(record['stay'])}"
    elif kind in {"voice_state", "camera", "stream"}:
        for key, pair in record.get("changes", {}).items():
            if kind in {"camera", "stream"}:
                title = ("Camera " if kind == "camera" else "Stream ") + (
                    "started" if pair[1] else "stopped"
                )
            else:
                fields.append(Field(key, f"{value(pair[0])} → {value(pair[1])}"))
    elif kind == "avatar":
        pass
    else:
        if record.get("operation"):
            title = title.removesuffix(" changed") + " " + record["operation"]
        if record.get("resource_type"):
            body = escape(record["resource_type"])
        if record.get("parent"):
            body += (" · " if body else "") + escape(record["parent"])
        for key, pair in list(role_changes(kind, record.get("changes", {})).items())[:5]:
            fields.append(
                Field(escape(key, 160), clip(f"{value(pair[0])} → {value(pair[1])}", 800))
            )
        for key in ("added", "removed"):
            if record.get(key):
                fields.append(
                    Field(key.title(), clip("\n".join(escape(x) for x in record[key][:10]), 800))
                )
    if not record.get("actor") and kind in {
        "kick",
        "ban",
        "unban",
        "warn",
        "unwarn",
        "timeout",
        "timeout_change",
        "untimeout",
    }:
        sections.append("Actor unknown")
    if body and not private:
        sections.insert(0, body)
    if private:
        sections.append("Member ID · " + str(record.get("subject") or record.get("message") or "—"))
        sections.append("Record · " + record["id"])
        full = detail_fields(record)
        if "detail_fields" in record:
            fields = [Field(x["name"], x["value"]) for x in record["detail_fields"]]
            full = []
        if full:
            fields = full[max(0, page) * 4 : max(0, page) * 4 + 4]
    link = None
    if kind in {"message_edit", "attachments", "pin", "unpin"} and record.get("message"):
        link = f"https://discord.com/channels/{record['guild']}/{record['channel']}/{record['message']}"
    media = record.get("format") == "v2"
    title = " ".join(word[:1].upper() + word[1:] for word in title.split(" "))
    context = context_line(record)
    if media:
        blocks = [Block("text", text=f"**{escape(title, 256)}**")]
        if context:
            blocks.append(Block("text", text=context))
        blocks.append(Block("separator", spacing="small"))
        content = []
        if sections:
            text = "\n".join(sections)
            content.append((len(blocks), "", text))
            blocks.append(Block("text", text=text))
        for field in fields:
            prefix = f"**{field.name}**\n"
            content.append((len(blocks), prefix, field.value))
            blocks.append(Block("text", text=prefix + field.value))
        for key, label in (("image_before", "Previous"), ("image_after", "Current")):
            if safe_image(record.get(key)):
                blocks.extend(
                    (
                        Block("text", text=f"**{label}**"),
                        Block(
                            "gallery",
                            items=(
                                PresentationMedia(
                                    media_url=safe_image(record[key]),
                                    description=label,
                                    spoiler=True,
                                ),
                            ),
                        ),
                    )
                )
            elif kind == "avatar":
                blocks.append(Block("text", text=f"**{label}**\nDefault avatar"))
        entries = record.get("files", [])[:3]
        for item in entries:
            file_name = (files or {}).get(str(item["id"]))
            if file_name:
                blocks.append(
                    Block(
                        "gallery",
                        items=(
                            PresentationMedia(
                                file_name=file_name, description=item["name"], spoiler=True
                            ),
                        ),
                    )
                    if item.get("type", "").startswith("image/")
                    else Block("file", file_name=file_name, spoiler=True)
                )
            else:
                blocks.append(
                    Block(
                        "text",
                        text=escape(item["name"], 150)
                        + " · "
                        + escape(item.get("status", "unavailable"), 60),
                    )
                )
        if link:
            blocks.append(Block("text", text=f"[Message]({link})"))
        blocks.append(Block("text", text=f"-# <t:{record['created'] // 1000}:f>"))
        if not private:


            indexes = {index for index, _, _ in content}
            budget = 3900 - sum(units(b.text) for i, b in enumerate(blocks) if i not in indexes)
            budget -= sum(units(prefix) for _, prefix, _ in content)
            if sum(units(text) for _, _, text in content) > budget:
                for left, (index, prefix, text) in zip(
                    range(len(content), 0, -1), content, strict=True
                ):
                    excerpt = clip(text, max(1, budget // left))
                    budget -= units(excerpt)
                    blocks[index] = Block("text", text=prefix + excerpt)
        return PresentationPage(blocks=tuple(blocks))
    description = context
    if sections:
        description += ("\n\n" if description else "") + "\n".join(sections)
    if link:
        description += ("\n\n" if description else "") + f"[Message]({link})"
    return PresentationPage(
        author_name=title,
        description=description,
        fields=tuple(fields[:8]),
        timestamp_ms=record["created"],
    )


def summary(record: dict) -> str:
    name = escape(record.get("name") or str(record.get("subject") or "Server"), 150)
    title = escape(TITLES.get(record["kind"], "Server changed"), 100)
    return f"**{title}** · {name}\n<t:{record['created'] // 1000}:f>"
