from __future__ import annotations

import re

from lumi.contracts import PresentationControl as Control
from lumi.contracts import PresentationOption as Option
from lumi.contracts import PresentationPage as Page
from lumi.members.search import edit_distance, normalize

PREFIX = "lumi:moderation:role:"
TTL = 900_000
PAGE_SIZE = 20
HELP = "`{p}role <@member | member ID> [role name | @role | role ID]`"
ROLE_ID = re.compile(r"^(?:<@&(\d{17,20})>|(\d{17,20}))$")


def parse(raw):
    text = str(raw or "").strip()
    found = re.fullmatch(r"(?:<@!?(\d{17,20})>|(\d{17,20}))(?:\s+([\s\S]{1,100}))?", text)
    if not found:
        raise ValueError("invalid role command")
    return int(found[1] or found[2]), (found[3] or "").strip()


def words(text):
    return re.findall(r"[^\W_]+", normalize(text))


def score(query, name):
    query, name = normalize(query).strip(), normalize(name).strip()
    if not query:
        return None
    if query == name:
        return 0, 0
    parts, terms = words(name), words(query)
    phrases = [" ".join(parts[i : i + len(terms)]) for i in range(len(parts))] if terms else []
    if terms and " ".join(terms) in phrases:
        return 1, 0
    candidates = {name, *parts, *phrases}
    prefix = [len(c) - len(query) for c in candidates if c.startswith(query)]
    if prefix:
        return 2, min(prefix)
    substring = [len(c) - len(query) for c in candidates if query in c]
    if substring:
        return 3, min(substring)
    if len(query) < 3:
        return None
    limit = 1 if len(query) <= 5 else 2
    closest = min((edit_distance(query, c, limit) for c in candidates), default=limit + 1)
    return (4, closest) if closest <= limit else None


def match_roles(query, roles):

    direct = ROLE_ID.fullmatch(query.strip())
    if direct:
        selected = int(direct[1] or direct[2])
        return [r for r in roles if r.id == selected]
    ranked = [(score(query, r.name), r) for r in roles]
    best = min((s for s, _ in ranked if s is not None), default=None)
    return sorted(
        (r for s, r in ranked if s is not None and s == best),
        key=lambda r: (normalize(r.name), r.id),
    )


def mention(role):
    return f"<@&{role}>"


def page(data):
    token, revision = data["token"], data["revision"]

    def control(kind, action, **kwargs):
        return Control(kind, "moderation", "roles_" + action, f"{token}~{revision}", **kwargs)

    target = f"<@{data['target']}>"
    current = data.get("roles", [])
    source = data.get("candidates", []) if data.get("view") == "choice" else current
    pages = max(1, (len(source) + PAGE_SIZE - 1) // PAGE_SIZE)
    number = min(data.get("page", 0), pages - 1)
    lines = [target]
    if data.get("notice"):
        lines.append(data["notice"])
    controls = []
    if data.get("view") == "choice":
        candidates = data.get("candidates", [])
        start = number * PAGE_SIZE
        controls.append(
            control(
                "string_select",
                "choose",
                placeholder="Choose a matching role",
                options=tuple(
                    Option(r["name"][:100], str(r["id"]), f"Role ID {r['id']}")
                    for r in candidates[start : start + PAGE_SIZE]
                ),
            )
        )
        pages = max(1, (len(candidates) + PAGE_SIZE - 1) // PAGE_SIZE)
        number = min(data.get("page", 0), pages - 1)
    elif data.get("view") != "receipt":
        lines.append(
            "**Current**\n"
            + (
                " · ".join(
                    mention(r) for r in current[number * PAGE_SIZE : (number + 1) * PAGE_SIZE]
                )
                or "None"
            )
        )
        selected = data.get("selected", {})
        for label, was_assigned in (("Add", False), ("Remove", True)):
            ids = [int(r) for r, before in selected.items() if before is was_assigned]
            if ids:
                lines.append(f"**{label}**\n" + " · ".join(mention(r) for r in ids))
        controls.extend(
            (
                control(
                    "role_select",
                    "stage",
                    placeholder="Choose roles to toggle",
                    min_values=0,
                    max_values=25,
                    default_ids=tuple(int(r) for r in selected),
                ),
                control("button", "apply", label="Apply", style="primary", disabled=not selected),
                control("button", "clear", label="Clear", disabled=not selected),
            )
        )
    if pages > 1 and data.get("view") != "receipt":
        lines.append(f"{number + 1}/{pages}")
        controls.extend(
            (
                control("button", "previous", label="Previous", disabled=number == 0),
                control("button", "next", label="Next", disabled=number == pages - 1),
            )
        )
    return Page(title="Roles", description="\n\n".join(lines), controls=tuple(controls))


def outcome(row):
    added, removed, uncertain = [], [], []
    for step in row.get("steps", {}).values():
        if step["status"] == "done":
            (added if step["desired"] else removed).append(mention(step["role"]))
        elif step["status"] == "uncertain":
            uncertain.append(mention(step["role"]))
    parts = []
    if added:
        parts.append("Added " + ", ".join(added) + ".")
    if removed:
        parts.append("Removed " + ", ".join(removed) + ".")
    if uncertain:
        parts.append("Unconfirmed: " + ", ".join(uncertain) + ".")
    if row.get("error"):
        parts.append(row["error"])
    return "\n".join(parts) or "No roles changed."
