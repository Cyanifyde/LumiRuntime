from lumi.contracts import PresentationPage
from lumi.shared.display_text import escape_embed_text


def evidence_page(row, group, summary):
    name = escape_embed_text(row.get("group_name", group["name"]))
    return PresentationPage(
        title="ImageGuard",
        footer="ImageGuard " + row["id"],
        description=(
            f"<@{row['user']}> · <#{row['channel']}>\n"
            f"{name} · {row['match']} · distance {row['distance']}\n{summary}"
        ),
    )


def summary(row, group):
    steps = row.get("steps", {})
    lines = []
    if "delete" in steps:
        state = steps["delete"]["state"]
        lines.append("Message · " + ("deleted" if state == "done" else state))
    added = sum(n.startswith("add_") and s["state"] == "done" for n, s in steps.items())
    removed = sum(n.startswith("remove_") and s["state"] == "done" for n, s in steps.items())
    skipped = sum(n.startswith(("add_", "remove_")) and s["state"] != "done" for n, s in steps.items())
    if added or removed or skipped:
        lines.append(f"Roles · +{added} / -{removed}" + (f" · {skipped} skipped" if skipped else ""))
    if "terminal" in steps:
        lines.append(row.get("action", group.get("action", "Action")).title() + " · " + steps["terminal"]["state"])
    return "\n".join(lines)
