from lumi.shared.access_receipts import admitted_request

from .models import CATEGORIES, EVENTS

SETTINGS = {"destination": "destinations", "events": "events", "excluded": "excluded", "bots": "bots"}


async def configure(store, request, session, *, generation, now, before_commit=None):
    event = request.event
    action = request.route.rpartition(".")[2]
    guild, actor, channel = session["guild"], session["owner"], session["channel"]
    if (
        action not in SETTINGS or request.route != f"modlog.{action}"
        or request.handler != "modlog.panel" or event.get("dispatch") != "component"
        or event.get("_expires_at_ms", 0) <= now
        or event.get("_job_generation") != generation
        or not admitted_request(
            request, module="modlog", group="security", generation=generation,
            actor=actor, guild=guild, channel=channel, acl="modlog",
        )
        or (event.get("guild_id"), event.get("actor_id"), event.get("channel_id"))
        != (guild, actor, channel)
        or session["screen"] != "setup" or session["section"] != SETTINGS[action]
    ):
        raise ValueError("Access unavailable.")

    values = request.arguments.get("values", ())
    category = session["category"]
    if action in {"destination", "excluded"}:
        if (
            not isinstance(values, (list, tuple))
            or event.get("_selected_channels") != {"guild": guild, "ids": list(values)}
        ):
            raise ValueError("Channels unavailable.")

    def change(doc):
        if action == "destination":
            if category not in (*CATEGORIES, "all") or len(values) > 1:
                raise ValueError("Invalid destination.")
            doc["destinations"][category] = values[0] if values else 0
        elif action == "events":
            if category not in CATEGORIES or not set(values) <= set(EVENTS[category]):
                raise ValueError("Invalid events.")
            doc["events"] = sorted((set(doc["events"]) - set(EVENTS[category])) | set(values))
        elif action == "excluded":
            doc["excluded"] = list(values)
        else:
            doc["bots"] = not doc["bots"]

    return await store.configure(
        guild, session["config_revision"], change, before_commit=before_commit,
    )
