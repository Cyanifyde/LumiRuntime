from __future__ import annotations

from lumi.contracts import PresentationControl, PresentationPage

TTL = 900_000
TERMINAL = ("done", "expired", "failed")
RUNNABLE = ("listing", "queued", "running", "undoing", "confirm", "undo_confirm")
HELP = "{p}rename <name> · set <literal name> · undo · status · cancel · resume"


def declined(row):
    if row["state"] != "undo_confirm":
        return "expired"
    if row["previous"] in {"listing", "confirm"} and not row["approved"]:
        return "cancelled"
    return row["previous"]


def parse(raw):
    if raw is None or raw == "":
        return "status", None
    if not isinstance(raw, str) or len(raw) > 4000:
        raise ValueError("Invalid name.")

    if raw.startswith("set "):
        name = raw[4:]
        if not name:
            raise ValueError("Include a name.")
        return "start", name
    if raw.casefold() in {"undo", "status", "cancel", "resume", "help"}:
        return raw.casefold(), None
    return "start", raw


def escaped(name):

    for char in ("\\", "`", "*", "_", "~", "|", ">", "#", "[", "]", "(", ")"):
        name = name.replace(char, "\\" + char)
    return "\u2068" + name + "\u2069"


def page(row, counts):
    controls = ()
    if row["state"] == "listing":
        return PresentationPage(description="Preparing rename…")
    if row["state"] in {"confirm", "undo_confirm"}:
        name = escaped(row["name"])
        if len(name) > 1800:
            name = "Name supplied."
        text = (
            f"Rename up to {counts['total']} members?\n{name}"
            if row["state"] == "confirm"
            else "Restore saved nicknames?\nChanged names stay."
        )
        controls = tuple(
            PresentationControl(
                "button",
                "moderation",
                action,
                f"{row['token']}~{row['id']}",
                label=label,
                style=style,
            )
            for action, label, style in (
                ("rename_confirm", "Confirm", "danger"),
                ("rename_decline", "Cancel", "secondary"),
            )
        )
    else:
        verb = "Restored" if row["mode"] == "undo" else "Renamed"
        text = f"{verb} {counts['restored'] if row['mode'] == 'undo' else counts['applied']}"
        text += f" · Skipped {counts['skipped']} · Failed {counts['failed']}"
        text = f"Rename {row['id']} · {row['state'].capitalize()}\n" + text
        if row.get("error"):
            text += "\n" + row["error"]
    return PresentationPage(description=text, controls=controls)


def summarize(statuses):
    return dict(
        total=sum(statuses.values()),
        applied=sum(
            statuses.get(k, 0) for k in ("applied", "undo_sending", "restore_failed", "restored")
        ),
        restored=statuses.get("restored", 0),
        skipped=sum(statuses.get(k, 0) for k in ("skipped", "unchanged", "changed", "left")),
        failed=sum(
            statuses.get(k, 0)
            for k in ("failed", "apply_sending", "undo_sending", "restore_failed")
        ),
    )
