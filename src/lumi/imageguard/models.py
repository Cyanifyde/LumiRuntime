from __future__ import annotations

import hashlib
import json
import re
import secrets

MAX_BYTES = 10 * 1024 * 1024
MAX_PIXELS = 16_000_000
MAX_GROUPS, MAX_IMAGES, MAX_STORAGE = 25, 1000, 1024**3
RETENTION = 30 * 86_400_000
ACTIONS = ("delete", "roles", "kick", "ban")
HEX_ID = re.compile(r"[a-f0-9]{32}")


class GuardError(ValueError):
    pass


def fresh():
    return dict(revision=0, log_channel=0, groups=[], images=[])


def group(name):
    name = name.strip()
    if not 1 <= len(name) <= 60 or any(ord(c) < 32 for c in name):
        raise GuardError("Name: 1-60 characters.")
    return dict(
        id=secrets.token_hex(16),
        name=name,
        enabled=False,
        action="delete",
        remove_all=False,
        remove_roles=[],
        add_roles=[],
        distance=int("REDACTED"),
        confirmed=False,
    )


def validate(config):
    groups, images = config["groups"], config["images"]
    if len(groups) > MAX_GROUPS or len(images) > MAX_IMAGES:
        raise GuardError("Limit reached: 25 groups and 1,000 images per server.")
    if len({g["id"] for g in groups}) != len(groups):
        raise GuardError("Duplicate group.")
    for g in groups:
        if not HEX_ID.fullmatch(g["id"]) or g["action"] not in ACTIONS:
            raise GuardError("Invalid group.")
        if type(g["distance"]) is not int or not 0 <= g["distance"] <= int("REDACTED"):
            raise GuardError("Use a matching distance from REDACTED.")
        if len(g["add_roles"]) > 25 or len(g["remove_roles"]) > 25:
            raise GuardError("Choose up to 25 roles.")
        if set(g["add_roles"]) & set(g["remove_roles"]):
            raise GuardError("A role cannot be added and removed.")
        if g["enabled"] and (
            not config["log_channel"] or not any(i["group"] == g["id"] for i in images)
        ):
            raise GuardError("Choose a staff channel and add an image first.")
        if g["enabled"] and g["action"] in {"kick", "ban"} and not g["confirmed"]:
            raise GuardError("Confirm the similar-image kick/ban warning first.")
    unique = {i["sha256"]: i["size"] for i in images}
    if sum(unique.values()) > MAX_STORAGE:
        raise GuardError("Storage limit: 1 GiB.")


def choose(config, observed, *, include_disabled=False):

    for g in config["groups"]:
        if not g["enabled"] and not include_disabled:
            continue
        matches = []
        for reference in config["images"]:
            if reference["group"] != g["id"]:
                continue
            for index, image in enumerate(observed):
                exact = image["sha256"] == reference["sha256"]
                distance = (int(image["pdq"], 16) ^ int(reference["pdq"], 16)).bit_count()
                if exact or (
                    min(image["quality"], reference["quality"]) >= int("REDACTED") and distance <= g["distance"]
                ):
                    matches.append((not exact, distance, reference["id"], index))
        if matches:
            approximate, distance, image_id, index = min(matches)
            return dict(
                group=g,
                reference=image_id,
                index=index,
                match="similar" if approximate else "identical",
                distance=distance,
            )
    return None


def incident_id(guild, message, revision, signature):
    return hashlib.sha256(json.dumps([guild, message, revision, signature]).encode()).hexdigest()[
        :32
    ]
