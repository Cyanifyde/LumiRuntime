from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import secrets

from lumi.contracts import (
    CaptureSpec,
    ComponentSpec,
    DiscordIntent,
    ModalSpec,
    PresentationPage,
)
from lumi.contracts import (
    ModalFieldSpec as Field,
)
from lumi.contracts import (
    PresentationControl as Control,
)
from lumi.contracts import (
    PresentationOption as Option,
)

from . import expanded as v2
from . import models as engine
from .protocol import (
    DRAW_DESTINATION, HOST_PERMISSIONS, MUTATE_GUILD, MUTATE_RENDER_GUILD,
    PUBLISH_GUILD, REPLY_GUILD, SOURCE_SCOPE,
)
from .source import observed_view

NAMESPACE = "giveawayui"
RECORD_PAGES = frozenset({"mine", "server", "joined"})
DISMISS_SELECTION = frozenset({"nav", "create", "pick_draft", "pick_preset"})
LEAVE_RECORD_PAGE = DISMISS_SELECTION | {
    "edit", "duplicate", "ask_end", "ask_cancel", "ask_reroll", "leave",
}
SECTIONS = ("Prizes", "Timing", "Entry", "Appearance", "Preview")


PAGE_DEPTH = dict(
    hub=0, builder=1, drafts=1, presets=1, mine=1, joined=1, server=1, setup=1, confirm=2
)
FORMS = {
    "custom": (
        "Custom prize",
        (
            Field(
                "name", "Prize · amount per winner", minimum=1, maximum=180, placeholder="100 gems"
            ),
            Field("copies", "Winner slots · 1-25", maximum=2, value="1"),
        ),
    ),
    "card": (
        "Verified card",
        (Field("view", "Fresh kv / sv / gv message link", minimum=20, maximum=200),),
    ),
    "copies": (
        "Custom prize",
        (
            Field(
                "name", "Prize · amount per winner", minimum=1, maximum=180, placeholder="100 gems"
            ),
            Field("copies", "Winner slots · 1-25", maximum=2),
        ),
    ),
    "timing": ("Duration", (Field("duration", "Duration · 20m, 2hs, 1d", maximum=50),)),
    "age": (
        "Minimum age",
        (
            Field("account", "Account age · 7d or 0", maximum=50, required=False),
            Field("member", "Time in server · 1d or 0", maximum=50, required=False),
        ),
    ),
    "appearance": (
        "Appearance",
        (
            Field("title", "Title", maximum=180, required=False),
            Field("description", "Description", style="paragraph", maximum=800, required=False),
        ),
    ),
    "save_preset": ("Save preset", (Field("name", "Preset name", minimum=1, maximum=100),)),
    "upload": (
        "Prize image",
        (Field("images", "Image", kind="file_upload", max_values=1, file_types=("image",)),),
    ),
    "banner": (
        "Giveaway banner",
        (Field("images", "Image", kind="file_upload", max_values=1, file_types=("image",)),),
    ),
    "setup_age": (
        "Server minimum age",
        (
            Field("account", "Account age · 7d or 0", maximum=50, required=False),
            Field("member", "Time in server · 1d or 0", maximum=50, required=False),
        ),
    ),
    "setup_defaults": (
        "Giveaway defaults",
        (Field("duration", "Default duration · 1d", maximum=50),),
    ),
}
INPUTS = {
    "nav": "string_values",
    "section": "string_values",
    "allocation": "string_values",
    "pick_prize": "string_values",
    "pick_draft": "string_values",
    "pick_preset": "string_values",
    "pick_record": "string_values",
    "mode": "string_values",
    "layout": "string_values",
    "roles": "role_ids",
    "exclude": "role_ids",
    "reroll_slots": "string_values",
    "setup_section": "string_values",
    "setup_channel": "channel_ids",
    "setup_host": "role_ids",
    "setup_roles": "role_ids",
    "setup_exclude": "role_ids",
    "setup_mode": "string_values",
    "setup_layout": "string_values",
}
ACTIONS = {
    "open",
    "create",
    "edit",
    "refresh",
    "previous",
    "next",
    "remove",
    "up",
    "down",
    "start",
    "apply",
    "clear_image",
    "clear_banner",
    "duplicate",
    "ask_end",
    "ask_cancel",
    "ask_reroll",
    "confirm_end",
    "confirm_cancel",
    "confirm_reroll",
    "back",
    "leave",
    "delete_preset",
    "confirm_delete_preset",
    "preview_back",
}


def components():
    result = []
    for action in sorted(ACTIONS | INPUTS.keys() | FORMS.keys()):
        form = FORMS.get(action)
        result.append(
            ComponentSpec(
                NAMESPACE,
                action,
                "giveaways.builder",
                r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,12})$",
                captures=(CaptureSpec("session"), CaptureSpec("revision", "integer", minimum=1)),
                acl_key="giveaway",
                public=True,
                ephemeral=action == "leave",
                session_bound=True,
                input_kind="modal" if form else INPUTS.get(action, "none"),
                modal=ModalSpec(*form) if form else None,
                modal_defaults=bool(form),
                channel_types=(0,) if action == "setup_channel" else (),
                channel_members=DRAW_DESTINATION if action in {"confirm_end", "confirm_reroll", "start"} else None,
                embed_sources=SOURCE_SCOPE if action in {"start", "apply"} else None,
                channel_publication=PUBLISH_GUILD if action == "start" else None,
                message_mutation=(
                    MUTATE_GUILD if action in {"start", "apply"}
                    else MUTATE_RENDER_GUILD if action in {"confirm_end", "confirm_cancel", "confirm_reroll"}
                    else None
                ),
                text_reply=REPLY_GUILD if action in {
                    "start", "apply", "confirm_end", "confirm_cancel", "confirm_reroll",
                } else None,
                required_permissions=("administrator",) if action.startswith("setup_") else (),
                deadline_ms=120_000,
            )
        )
    return tuple(result)


def button(action, payload, label, style="secondary", *, disabled=False):
    return Control(
        "button", NAMESPACE, action, payload, label=label, style=style, disabled=disabled
    )


def select(action, payload, choices, *, selected=None, placeholder=None, multi=False):
    return Control(
        "string_select",
        NAMESPACE,
        action,
        payload,
        placeholder=placeholder,
        min_values=1,
        max_values=len(choices) if multi else 1,
        options=tuple(
            Option(
                str(label)[:100],
                str(value),
                default=(value in selected if multi and selected else value == selected),
            )
            for label, value in choices
        ),
    )


def number(raw):
    if not str(raw).isascii() or not str(raw).isdigit():
        raise engine.GiveawayError("Use a whole number from 1 to 25.")
    return v2.integer(int(raw), 1, 25, "winner count")


def age(raw):
    from .durations import duration

    return duration(raw or "0", minimum=0, maximum=100 * 366 * v2.DAY)


class GiveawayBuilder:
    def __init__(self, module):
        self.m = module
        self.locks = {}

    async def launch(self, actor, guild, command, channel, *, job, setup=False, record=None):
        config = await self.m.store.config(guild)
        access = await self.m.actor_access(job, guild, actor, channel, (config["channel"],))
        channels, roles, permissions = self.access_sets(access)
        admin = "administrator" in permissions
        if setup and not admin:
            raise engine.GiveawayError("Administrator only.")
        can_host = config["channel"] in channels and self.can_host(config, roles, permissions)
        state = dict(
            kind="builder",
            revision=1,
            command=command,
            channel=channel,
            page="setup" if setup else "hub",
            section="Destination" if setup else "Prizes",
            draft=None,
            selected=None,
            giveaway=record,
            offset=0,
            selected_slots=[],
        )
        token = await self.m.session(actor, guild, **state)
        state.update(owner=actor, guild=guild)
        return await self.render(
            state,
            token + "~1",
            config,
            admin,
            can_host,
            None,
            channels=channels,
            kind="send",
        )

    async def mutate(self, request, job, operation, draft, config, access, *, session=None):
        event = request.event
        actor, guild = event["actor_id"], event["guild_id"]
        if (draft["owner"], draft["guild"]) != (actor, guild):
            raise engine.GiveawayError("Draft unavailable.")
        channels, roles, permissions = self.access_sets(access)
        spec = v2.validate_spec(draft["spec"], complete=True)
        now = self.m.clock()
        if operation == "quick":
            if (
                request.handler != "giveaways.command" or request.route != request.handler
                or draft["command"] != event.get("message_id")
                or draft["source_channel"] != event.get("channel_id")
                or len(spec["prizes"]) != 1 or spec["prizes"][0]["kind"] != "card"
                or spec["prizes"][0]["card"]["message"] != event.get("reference_message_id")
                or spec["duration"] != engine.parse_duration(request.arguments.get("raw", ""))
            ):
                raise engine.GiveawayError("Unavailable.")
        elif (
            operation not in {"start", "apply"}
            or request.handler != "giveaways.builder" or request.route != f"{NAMESPACE}.{operation}"
            or not session or session.get("draft") != draft["id"]
            or session.get("section") != "Preview"
            or session.get("draft_revision") != draft["revision"]
        ):
            raise engine.GiveawayError("Draft changed. Refresh it.")

        if operation == "apply":
            editing = draft.get("editing") or {}
            original = await self.m.store.get(guild, editing.get("id", ""))
            self.require_manage(
                original, guild, channels, actor,
                admin="administrator" in permissions, can_host=self.can_host(config, roles, permissions),
            )
            pending = original.get("pending_edit")
            if (
                pending and original.get("last_edit_job") == job
                and pending["draft"] == draft["id"] and pending["spec"] == spec
                and pending["base"] == editing.get("revision") == original["revision"]
            ):
                return original["id"], None
            proposed = self.m.workflow.prepare_edit(original, draft, actor, job, spec, now)
            observed = await self.m.observe_sources(proposed, job, "edit")
            if observed is None:
                raise engine.GiveawayError("Unavailable. Try again.")
            await self.m.workflow.approve_edit(draft, original, proposed)
            return original["id"], observed

        if config["channel"] not in channels or not self.can_host(config, roles, permissions):
            raise engine.GiveawayError("Moderator permission or the host role required.")
        if spec["baseline"] != v2.config_defaults(config)["baseline"] or draft.get("editing"):
            raise engine.GiveawayError("Server requirements changed. Start a fresh draft.")
        host_role, config_revision = config["host_role"], config["revision"]
        proposed = v2.new_record(
            secrets.token_hex(8), guild, actor, config["channel"], draft["command"],
            draft["source_channel"], draft["id"], spec, now,
        )
        observed = await self.m.observe_sources(proposed, job, "publish", config=config)
        if observed is None:
            raise engine.GiveawayError("Unavailable. Try again.")
        created, _ = await self.m.store.create_expanded(
            draft, self.m.clock(), host_role=host_role,
            expected_config=config_revision, identifier=proposed["id"],
        )
        return created["id"], observed

    @staticmethod
    def access_sets(access):
        return tuple(frozenset(access[field]) for field in ("channels", "roles", "permissions"))

    @staticmethod
    def can_host(config, roles, permissions):
        return bool(permissions & HOST_PERMISSIONS or config.get("host_role", 0) in roles)

    @staticmethod
    def needs_channel_listing(s, action, choice):
        if action == "nav":
            return choice in RECORD_PAGES
        if action in {"start", "apply"}:
            return True
        if action in {"back", "confirm_end", "confirm_cancel", "confirm_reroll"}:
            return s.get("return_page") in RECORD_PAGES
        return s["page"] in RECORD_PAGES and action not in LEAVE_RECORD_PAGE

    @staticmethod
    def require_visible(record, guild, channels):
        if not record or record["guild"] != guild or record["channel"] not in channels:
            raise engine.GiveawayError("Giveaway unavailable.")
        return record

    @classmethod
    def require_manage(cls, record, guild, channels, actor, *, admin, can_host):
        cls.require_visible(record, guild, channels)
        if not admin and record["host"] != actor:
            raise engine.GiveawayError("Host or administrator only.")
        if not admin and not can_host:
            raise engine.GiveawayError("Moderator permission or the host role required.")
        return record

    async def handle(self, request, job):
        token = str(request.arguments.get("session", ""))
        async with self.locks.setdefault(token, asyncio.Lock()):
            return await self._handle(request, job, token)

    async def _handle(self, request, job, token):
        actor, guild = int(request.event["actor_id"]), int(request.event["guild_id"])
        raw = await self.m.cache.get("lumi:giveaways:session:" + token)
        s = json.loads(raw) if raw else None
        if (
            not s
            or s.get("kind") != "builder"
            or s["owner"] != actor
            or s["guild"] != guild
            or s["channel"] != request.event.get("channel_id")
            or s["expires"] <= self.m.clock()
        ):
            raise engine.GiveawayError("Expired.")
        if request.arguments.get("revision") != s["revision"]:
            raise engine.GiveawayError("Menu changed. Use its latest controls.")
        action = request.route.split(".")[-1]
        fields = request.arguments.get("fields", {})
        values = request.arguments.get("values", ())
        choice = str(values[0]) if len(values) == 1 else ""
        config = await self.m.store.config(guild)
        draft = (
            await self.m.store.draft(guild, actor, s["draft"], self.m.clock())
            if s.get("draft") and action not in DISMISS_SELECTION
            else None
        )
        if (
            draft
            and s.get("draft_revision") != draft["revision"]
            and action != "refresh"
            and job not in draft.get("receipts", [])
        ):
            raise engine.GiveawayError("Draft changed. Reopen it from Drafts.")
        record = None
        has_selection = (
            (s["page"] in RECORD_PAGES | {"confirm"} or action == "leave")
            and s.get("giveaway") and action not in DISMISS_SELECTION
        )
        media_edit = action in {"card", "upload", "banner"} and draft and draft.get("editing")
        if media_edit:
            record = await self.m.store.get(guild, media_edit["id"])
        elif has_selection:
            record = await self.m.store.get(guild, s["giveaway"])
        targets = None if self.needs_channel_listing(s, action, choice) else (
            config["channel"], record["channel"] if record else 0,
        )
        access = await self.m.actor_access(
            job, guild, actor, int(request.event["channel_id"]), targets,
            link_field="view" if action == "card" and draft else None,
        )
        channels, roles, permissions = self.access_sets(access)
        admin = "administrator" in permissions
        host_role = self.can_host(config, roles, permissions)
        can_host = config["channel"] in channels and host_role
        if s["page"] == "setup" and not admin:
            raise engine.GiveawayError("Administrator only.")
        if has_selection:
            self.require_visible(record, guild, channels)
        notice = ""
        draft_id = hashlib.sha256(f"draft:{guild}:{job}".encode()).hexdigest()[:16]

        if action == "nav" and choice != "create":
            permitted = {"hub", "drafts", "presets", "mine", "joined"} | (
                {"server", "setup"} if admin else set()
            )
            if choice not in permitted:
                raise engine.GiveawayError("Unavailable.")
            s.update(page=choice, offset=0, draft=None, selected=None, giveaway=None, preset=None)
            draft = None
        elif action == "create" or (action == "nav" and choice == "create"):
            if not can_host:
                raise engine.GiveawayError(
                    "You need hosting access and an available giveaway channel."
                )
            draft = await self.m.store.new_draft(
                guild,
                actor,
                s["command"],
                s["channel"],
                v2.config_defaults(config),
                self.m.clock(),
                identifier=draft_id,
            )
            s.update(
                draft=draft["id"],
                page="builder",
                section="Prizes",
                selected=None,
                giveaway=None,
                preset=None,
            )
        elif action == "pick_draft":
            draft = await self.m.store.draft(guild, actor, choice, self.m.clock())
            s.update(
                draft=draft["id"],
                page="builder",
                section="Prizes",
                selected=None,
                giveaway=None,
                preset=None,
            )
        elif action == "pick_preset":
            presets = await self.m.store.presets(guild)
            chosen = next((p for p in presets if p["id"] == choice), None)
            if not chosen or not can_host:
                raise engine.GiveawayError("Preset unavailable.")
            spec = copy.deepcopy(chosen["spec"])
            spec["baseline"] = v2.config_defaults(config)["baseline"]
            draft = await self.m.store.new_draft(
                guild, actor, s["command"], s["channel"], spec, self.m.clock(), identifier=draft_id
            )
            s.update(
                draft=draft["id"],
                page="builder",
                section="Prizes",
                selected=None,
                preset=choice,
                giveaway=None,
            )
        elif action == "pick_record":
            if s["page"] not in RECORD_PAGES or not engine.ID.fullmatch(choice):
                raise engine.GiveawayError("Unavailable.")
            if not record or record["id"] != choice:
                record = await self.m.store.get(guild, choice)
            self.require_visible(record, guild, channels)
            s.update(
                giveaway=choice,
                selected_slots=[],
                record_revision=record["revision"],
            )
        elif action in {"previous", "next"}:
            s["offset"] = max(0, s["offset"] + (-5 if action == "previous" else 5))
        elif action == "section":
            if choice not in SECTIONS or not draft:
                raise engine.GiveawayError("Unavailable.")
            if choice == "Preview":
                s["preview_from"] = s["section"]
            s["section"] = choice
        elif action == "preview_back":
            s["section"] = s.get("preview_from", "Prizes")
            if s["section"] == "Preview":
                s["section"] = "Prizes"
        elif action == "back":
            s["page"] = s.get("return_page", "hub")
        elif action == "setup_section":
            if not admin or choice not in {"Destination", "Access", "Entry", "Defaults", "Presets"}:
                raise engine.GiveawayError("Administrator only.")
            s.update(page="setup", section=choice)
        elif action.startswith("setup_"):
            if not admin or s["page"] != "setup":
                raise engine.GiveawayError("Administrator only.")
            if action == "setup_channel":
                if len(values) != 1:
                    raise engine.GiveawayError("Select a channel.")
                if not self.m.workflow.selection_available(access, "channel_ids", values):
                    raise engine.GiveawayError("Giveaway channel unavailable.")
            elif action in {"setup_host", "setup_roles", "setup_exclude"}:
                if not self.m.workflow.selection_available(access, "role_ids", values):
                    raise engine.GiveawayError("Role unavailable.")
            await self.m.workflow.configure(guild, actor, action, values, fields, config)
            config = await self.m.store.config(guild)
            notice = "Saved."
        elif action == "duplicate":
            self.require_visible(record, guild, channels)
            spec = v2.preset(record["spec"] if v2.is_v2(record) else v2.upgrade(record)["spec"])
            spec["baseline"] = v2.config_defaults(config)["baseline"]
            draft = await self.m.store.new_draft(
                guild, actor, s["command"], s["channel"], spec, self.m.clock(), identifier=draft_id
            )
            s.update(
                draft=draft["id"],
                page="builder",
                section="Prizes",
                selected=None,
                giveaway=None,
                preset=None,
            )
        elif action == "edit":
            self.require_manage(record, guild, channels, actor, admin=admin, can_host=host_role)
            if record["status"] != "active" or record.get("pending_edit"):
                raise engine.GiveawayError("This giveaway changed.")
            spec = copy.deepcopy(record["spec"] if v2.is_v2(record) else v2.upgrade(record)["spec"])
            draft = await self.m.store.new_draft(
                guild,
                actor,
                s["command"],
                s["channel"],
                spec,
                self.m.clock(),
                editing=dict(id=record["id"], revision=record["revision"]),
                identifier=draft_id,
            )
            s.update(draft=draft["id"], page="builder", section="Prizes", selected=None)
        elif action == "refresh" and s["page"] in {"mine", "server", "joined"}:
            pass
        elif action == "ask_reroll" or action in {"ask_end", "ask_cancel"}:
            self.require_manage(record, guild, channels, actor, admin=admin, can_host=host_role)
            s.update(
                return_page=s["page"],
                page="confirm",
                operation=action.removeprefix("ask_"),
                record_revision=record["revision"],
            )
            if action == "ask_reroll":
                s["selected_slots"] = [a["slot"] for a in record.get("awards", []) if a["user"]]
        elif action == "reroll_slots":
            self.require_visible(record, guild, channels)
            valid = {a["slot"] for a in record.get("awards", []) if a["user"]}
            if not values or not set(values) <= valid:
                raise engine.GiveawayError("Select awarded prize slots.")
            s["selected_slots"] = list(values)
        elif action in {"confirm_end", "confirm_cancel", "confirm_reroll"}:
            operation = action.removeprefix("confirm_")
            if s["page"] != "confirm" or s.get("operation") != operation:
                raise engine.GiveawayError("Unavailable.")
            self.require_manage(record, guild, channels, actor, admin=admin, can_host=host_role)
            await self.m.workflow.control(
                request, job, record, operation, revision=s["record_revision"],
                selected_slots=s.get("selected_slots", ()),
            )
            await self.m.work(guild, s["giveaway"], job, request=request, access=access)
            record = await self.m.store.get(guild, s["giveaway"])
            s["page"] = s.get("return_page", "mine")
        elif action == "leave":
            self.require_visible(record, guild, channels)
            await self.m.workflow.control(request, job, record, "leave")
            notice = "Left."
            s["page"] = "hub"
        elif action == "delete_preset":
            if not admin or not s.get("preset"):
                raise engine.GiveawayError("Administrator only.")
            s.update(page="confirm", operation="delete_preset", return_page="builder")
        elif action == "confirm_delete_preset":
            if not admin or s["page"] != "confirm" or s.get("operation") != "delete_preset":
                raise engine.GiveawayError("Administrator only.")
            if not engine.ID.fullmatch(s.get("preset") or ""):
                raise engine.GiveawayError("Preset unavailable.")
            await self.m.store.delete_preset(guild, s["preset"])
            s.update(page="presets", draft=None)
            draft = None
        elif action == "open":
            if s["page"] == "setup" and not admin:
                raise engine.GiveawayError("Administrator only.")
        elif action == "refresh":
            pass
        else:
            if not draft or (not can_host and not (admin and draft.get("editing"))):
                raise engine.GiveawayError("Moderator permission or the host role required.")
            selected = next(
                (p for p in draft["spec"]["prizes"] if p["id"] == s.get("selected")), None
            )
            if action in {"start", "apply"}:
                if s["section"] != "Preview":
                    raise engine.GiveawayError("Preview the giveaway first.")
                identifier, observed = await self.mutate(
                    request, job, action, draft, config, access, session=s,
                )
                saved = await self.m.store.get(guild, identifier)
                s.update(
                    giveaway=identifier,
                    page="mine" if saved["host"] == actor else "server",
                    draft=None,
                    offset=0,
                )
                draft = None
                await self.m.work(guild, identifier, job, request=request, access=access, observed=observed)
                current = await self.m.store.get(guild, identifier)
                record = current
                if current["status"] == "failed":
                    raise engine.GiveawayError(current.get("error") or "Publication failed.")
                notice = (
                    "Updating."
                    if current.get("pending_edit")
                    else "Publication pending."
                    if current["status"] == "publishing"
                    else "Saved."
                )
            elif action == "save_preset":
                if not admin:
                    raise engine.GiveawayError("Administrator only.")
                preset = hashlib.sha256(f"preset:{guild}:{job}".encode()).hexdigest()[:16]
                await self.m.store.save_preset(guild, fields.get("name", ""), draft["spec"], identifier=preset)
                notice = "Preset saved. Cards need fresh proof when reused."
            elif action == "pick_prize":
                if choice not in {p["id"] for p in draft["spec"]["prizes"]}:
                    raise engine.GiveawayError("Prize unavailable.")
                s["selected"] = choice
            else:

                def change(value):
                    spec = value["spec"]
                    prizes = spec["prizes"]
                    if action == "custom":
                        prize = dict(
                            id=hashlib.sha256(f"prize:{job}".encode()).hexdigest()[:16],
                            kind="custom",
                            name=fields.get("name", ""),
                            copies=number(fields.get("copies", "1")),
                        )
                        prizes.append(prize)
                        s["selected"] = prize["id"]
                    elif action == "card":
                        prizes.append(added)
                        s["selected"] = added["id"]
                    elif action in {"upload", "banner"}:
                        if action == "banner":
                            spec["banner"] = image
                        elif not selected or selected["kind"] != "custom":
                            raise engine.GiveawayError("Select a custom prize.")
                        else:
                            next(p for p in prizes if p["id"] == selected["id"])["image"] = image
                    elif action == "clear_banner":
                        spec["banner"] = None
                    elif action in {"remove", "up", "down", "copies", "clear_image"}:
                        if not selected:
                            raise engine.GiveawayError("Select a prize.")
                        index = next(i for i, p in enumerate(prizes) if p["id"] == selected["id"])
                        if action == "remove":
                            prizes.pop(index)
                            s["selected"] = None
                        elif action in {"up", "down"}:
                            target = index + (-1 if action == "up" else 1)
                            if 0 <= target < len(prizes):
                                prizes[index], prizes[target] = prizes[target], prizes[index]
                        elif prizes[index]["kind"] != "custom":
                            raise engine.GiveawayError(
                                "Verified card images and quantity cannot be changed."
                            )
                        elif action == "copies":
                            prizes[index]["copies"] = number(fields.get("copies", ""))
                            prizes[index]["name"] = fields.get("name", "")
                        else:
                            prizes[index].pop("image", None)
                    elif action == "allocation" and choice in {"bundle", "separate"}:
                        spec["mode"] = choice
                    elif action == "timing":
                        spec["duration"] = engine.parse_duration(fields.get("duration", ""))
                        value["timing_changed"] = True
                    elif action in {"roles", "exclude"}:
                        spec["rules"]["roles" if action == "roles" else "excluded"] = [
                            int(v) for v in values
                        ]
                    elif action == "mode" and choice in {"all", "any"}:
                        spec["rules"]["mode"] = choice
                    elif action == "age":
                        spec["rules"].update(
                            account_ms=age(fields.get("account")),
                            member_ms=age(fields.get("member")),
                        )
                    elif action == "appearance":
                        spec.update(
                            title=fields.get("title", ""), description=fields.get("description", "")
                        )
                    elif action == "layout" and choice in {"image-first", "text-first"}:
                        spec["layout"] = choice
                    else:
                        raise engine.GiveawayError("Unavailable.")

                if action == "card":
                    if media_edit:
                        self.require_manage(record, guild, channels, actor, admin=admin, can_host=host_role)
                    view = access.get("source")
                    source = observed_view(view, guild, view.get("channel") if isinstance(view, dict) else None, self.m.clock())
                    if source["owner"] != (record["host"] if media_edit else actor):
                        raise engine.GiveawayError("The host does not own that card.")
                    image = await self.m.assets.ingest(source["image_url"])
                    card = dict(source)
                    card.pop("image_url")
                    added = dict(
                        id=hashlib.sha256(f"prize:{job}".encode()).hexdigest()[:16],
                        kind="card",
                        copies=1,
                        card=card,
                        image=image,
                        source_channel=view["channel"],
                        proof=engine.source_fingerprint(source),
                    )
                if action in {"upload", "banner"}:
                    if media_edit:
                        self.require_manage(record, guild, channels, actor, admin=admin, can_host=host_role)
                    files = request.arguments.get("files", {}).get("images", ())
                    if len(files) != 1 or not 0 < int(files[0].get("size", 0)) <= 10 * 1024 * 1024:
                        raise engine.GiveawayError("Upload one image up to 10 MiB.")

                    image = await self.m.assets.ingest(files[0]["url"], upload=True)
                draft = await self.m.store.update_draft(
                    guild,
                    actor,
                    draft["id"],
                    draft["revision"],
                    self.m.clock(),
                    change,
                    receipt=job,
                )
                if action in {"custom", "card"}:
                    s["selected"] = hashlib.sha256(f"prize:{job}".encode()).hexdigest()[:16]
                elif action == "remove":
                    s["selected"] = None

        s["revision"] += 1
        s["draft_revision"] = draft["revision"] if draft else None
        payload = token + "~" + str(s["revision"])
        intent = await self.render(s, payload, config, admin, can_host, draft, notice, channels=channels, record=record)
        await self.m.cache.setex(
            "lumi:giveaways:session:" + token,
            max(1, (s["expires"] - self.m.clock()) // 1000),
            json.dumps(s),
        )
        return intent

    async def render(
        self, s, payload, config, admin, can_host, draft, notice="", *, channels, record=None, kind="edit"
    ):
        page, controls, attachments = s["page"], [], ()
        body = "Giveaways"
        navigation = [
            ("Home", "hub"),
            ("Create", "create"),
            ("Drafts", "drafts"),
            ("Presets", "presets"),
            ("My giveaways", "mine"),
            ("Joined", "joined"),
        ]
        if admin:
            navigation += [("Server giveaways", "server"), ("Setup", "setup")]
        if page not in PAGE_DEPTH:
            raise engine.GiveawayError("Unavailable.")
        active_nav = (
            "create"
            if page == "builder"
            else s.get("return_page", "hub")
            if page == "confirm"
            else page
        )
        controls.append(
            select("nav", payload, navigation, selected=active_nav, placeholder="Giveaways")
        )
        pages = []
        if page == "hub":
            body += (
                f"\n<#{config['channel']}>"
                if config.get("channel")
                else "\nAn administrator must choose a channel."
            )
            controls += [button("create", payload, "Create", "primary", disabled=not can_host)]
            if not can_host:
                body += "\nModerator permission or the host role required."
        elif page in {"drafts", "presets", "mine", "joined", "server"}:
            body = dict(
                drafts="Drafts",
                presets="Presets",
                mine="My giveaways",
                joined="Joined",
                server="Server giveaways",
            )[page]
            if page == "drafts":
                rows = await self.m.store.drafts(s["guild"], s["owner"], self.m.clock())
                action = "pick_draft"
                choices = [(r["spec"]["title"] or "Untitled giveaway", r["id"]) for r in rows]
            elif page == "presets":
                rows = await self.m.store.presets(s["guild"])
                action, choices = "pick_preset", [(r["name"], r["id"]) for r in rows]
            else:
                if page == "server" and not admin:
                    raise engine.GiveawayError("Unavailable.")
                rows = await self.m.store.list_records(
                    s["guild"], host=s["owner"] if page == "mine" else None,
                    joined=s["owner"] if page == "joined" else None, channels=sorted(channels),
                    offset=s["offset"], limit=6,
                )
                choices = [
                    (
                        self.record_label(r),
                        r["id"],
                    )
                    for r in rows if r["guild"] == s["guild"] and r["channel"] in channels
                ]
                action = "pick_record"
            if page in {"drafts", "presets"}:
                choices = choices[s["offset"] : s["offset"] + 6]
            body += "\nChoose one." if choices else "\nNone."
            if choices:
                controls.append(select(action, payload, choices[:5], placeholder="Choose"))
            controls += [
                button("previous", payload, "Previous", disabled=s["offset"] == 0),
                button("next", payload, "Next", disabled=len(choices) <= 5),
            ]
            if page in {"mine", "server", "joined"} and s.get("giveaway"):
                self.require_visible(record, s["guild"], channels)
                detail, actions = self.record_panel(s, payload, admin, can_host, record)
                body = body.split("\n", 1)[0] + "\n" + detail
                controls.extend(actions)
        elif page == "setup":
            if not admin:
                raise engine.GiveawayError("Administrator only.")
            section = (
                s["section"]
                if s["section"] in {"Destination", "Access", "Entry", "Defaults", "Presets"}
                else "Destination"
            )
            controls.append(
                select(
                    "setup_section",
                    payload,
                    [(v, v) for v in ("Destination", "Access", "Entry", "Defaults", "Presets")],
                    selected=section,
                )
            )
            settings = config.get("settings") or {}
            rule = v2.rules(settings.get("rules"))
            body = "Giveaway setup · " + section
            if section == "Destination":
                body += f"\n<#{config['channel']}>" if config.get("channel") else "\nNot set."
                controls.append(
                    Control(
                        "channel_select",
                        NAMESPACE,
                        "setup_channel",
                        payload,
                        channel_types=(0,),
                        default_ids=(config["channel"],) if config.get("channel") else (),
                    )
                )
            elif section == "Access":
                body += (
                    f"\nModerators + <@&{config['host_role']}>"
                    if config.get("host_role")
                    else "\nModerators only."
                )
                controls.append(
                    Control(
                        "role_select",
                        NAMESPACE,
                        "setup_host",
                        payload,
                        min_values=0,
                        default_ids=(config["host_role"],) if config.get("host_role") else (),
                        placeholder="Additional host role · optional",
                    )
                )
            elif section == "Entry":
                body += "\n" + (
                    v2.requirements(
                        dict(baseline=v2.config_defaults(config)["baseline"], rules=v2.rules())
                    )
                    or "No mandatory entry rules."
                )
                if config.get("required_role") and not rule["roles"]:
                    rule["roles"] = [config["required_role"]]
                controls += self.rule_controls(payload, rule, setup=True)
            elif section == "Defaults":
                body += "\nDuration: " + v2.age_label(settings.get("duration", v2.DAY))
                controls += [
                    button("setup_defaults", payload, "Duration"),
                    select(
                        "setup_layout",
                        payload,
                        [("Image first", "image-first"), ("Text first", "text-first")],
                        selected=settings.get("layout", "image-first"),
                    ),
                ]
            else:
                body += "\nSave a configured draft as a preset. Reusing it requires new card proof."
        elif page == "builder" and draft:
            spec, section = draft["spec"], s["section"]
            controls.append(
                select("section", payload, [(v, v) for v in SECTIONS], selected=section)
            )
            body = "Giveaway · " + section
            if section == "Prizes":
                body += "\n" + (
                    "\n".join(v2.prize_name(p) for p in spec["prizes"]) or "Add a prize."
                )
                controls += [
                    button(
                        "card", payload, "Add card", disabled=len(spec["prizes"]) >= v2.MAX_PRIZES
                    ),
                    button(
                        "custom",
                        payload,
                        "Add custom prize",
                        disabled=len(spec["prizes"]) >= v2.MAX_PRIZES,
                    ),
                    select(
                        "allocation",
                        payload,
                        [
                            ("One winner takes the bundle", "bundle"),
                            ("Separate winners", "separate"),
                        ],
                        selected=spec["mode"],
                    ),
                ]
                if spec["prizes"]:
                    controls.append(
                        select(
                            "pick_prize",
                            payload,
                            [(v2.prize_name(p), p["id"]) for p in spec["prizes"]],
                            selected=s.get("selected"),
                            placeholder="Edit a prize",
                        )
                    )
                if s.get("selected"):
                    index = next(
                        i for i, p in enumerate(spec["prizes"]) if p["id"] == s["selected"]
                    )
                    selected = spec["prizes"][index]
                    controls += [
                        button("remove", payload, "Remove"),
                        button("up", payload, "Move up", disabled=index == 0),
                        button(
                            "down", payload, "Move down", disabled=index == len(spec["prizes"]) - 1
                        ),
                    ]
                    if selected["kind"] == "custom":
                        controls += [
                            button(
                                a,
                                payload,
                                label,
                                disabled=a == "clear_image" and not selected.get("image"),
                            )
                            for a, label in (
                                ("copies", "Edit prize"),
                                ("upload", "Image"),
                                ("clear_image", "Remove image"),
                            )
                        ]
            elif section == "Timing":
                body += "\nDuration: " + v2.age_label(spec["duration"])
                if draft.get("editing"):
                    body += "\nUnchanged unless you replace the duration."
                controls += [button("timing", payload, "Set duration")]
            elif section == "Entry":
                body += "\n" + (v2.requirements(spec) or "Everyone eligible. Equal chances.")
                controls += self.rule_controls(payload, spec["rules"])
            elif section == "Appearance":
                body += "\n" + (v2.escaped(spec["title"]) or "Automatic title")
                controls += [
                    button("appearance", payload, "Text"),
                    button("banner", payload, "Banner"),
                    button(
                        "clear_banner", payload, "Remove banner", disabled=not spec.get("banner")
                    ),
                    select(
                        "layout",
                        payload,
                        [("Image first", "image-first"), ("Text first", "text-first")],
                        selected=spec["layout"],
                    ),
                ]
            else:
                v2.validate_spec(spec, complete=True)
                preview = v2.new_record(
                    draft["id"],
                    s["guild"],
                    s["owner"],
                    config["channel"],
                    draft["command"],
                    draft["source_channel"],
                    draft["id"],
                    spec,
                    self.m.clock(),
                )
                preview.update(ends=self.m.clock() + spec["duration"], status="active")
                if draft.get("editing"):
                    original = await self.m.store.get(s["guild"], draft["editing"]["id"])
                    preview.update(
                        host=original["host"],
                        channel=original["channel"],
                        entries=original["entries"],
                    )
                    if not draft.get("timing_changed"):
                        preview["ends"] = original["ends"]
                attachments = tuple(
                    [
                        await self.m.assets.attachment(image, v2.file_name(preview, image))
                        for _, image, _ in v2.image_items(spec)
                    ]
                )
                pages.extend(v2.presentation(preview, preview=True).pages)
                controls += [
                    button(
                        "apply" if draft.get("editing") else "start",
                        payload,
                        "Apply" if draft.get("editing") else "Start",
                        "primary",
                    )
                ]
                controls.append(button("preview_back", payload, "Back"))
            if admin:
                controls += [button("save_preset", payload, "Save preset")]
                if s.get("preset"):
                    controls += [button("delete_preset", payload, "Delete preset", "danger")]
            defaults = {
                "appearance": {"title": spec["title"], "description": spec["description"]},
                "timing": {"duration": v2.age_label(spec["duration"])},
                "age": {
                    "account": v2.age_label(spec["rules"]["account_ms"]),
                    "member": v2.age_label(spec["rules"]["member_ms"]),
                },
            }
            selected = next(
                (
                    p
                    for p in spec["prizes"]
                    if p["id"] == s.get("selected") and p["kind"] == "custom"
                ),
                None,
            )
            if selected:
                defaults["copies"] = dict(
                    name=selected["name"], copies=str(selected.get("copies", 1))
                )
            for action, values in defaults.items():
                await self.m.cache.setex(
                    f"lumi:modal-context:giveaways:{payload}:{action}",
                    900,
                    json.dumps(
                        dict(
                            owner=s["owner"],
                            guild=s["guild"],
                            generation=self.m.generation,
                            values=values,
                        )
                    ),
                )
        elif page == "confirm":
            operation = s["operation"]
            body = {
                "end": "End now and draw winners?",
                "cancel": "Cancel without drawing?",
                "reroll": "Replace winners?",
                "delete_preset": "Delete preset?",
            }[operation]
            if operation == "reroll":
                self.require_visible(record, s["guild"], channels)
                if v2.is_v2(record):
                    choices = [
                        (f"Prize slot {i + 1} · {a['user']}", a["slot"])
                        for i, a in enumerate(record["awards"])
                        if a["user"]
                    ]
                    controls += [
                        select(
                            "reroll_slots",
                            payload,
                            choices,
                            selected=s["selected_slots"],
                            multi=True,
                        )
                    ]
            controls += [
                button("confirm_" + operation, payload, "Confirm", "danger"),
                button("back", payload, "Back"),
            ]
        if notice:
            body += "\n" + notice
        if page == "builder" and not draft:
            raise engine.GiveawayError("Draft unavailable.")
        pages.append(
            PresentationPage(
                title=body.split("\n", 1)[0],
                description=body.partition("\n")[2],
                controls=tuple(controls),
            )
        )
        return DiscordIntent(
            kind,
            session_id=payload.split("~")[0],
            pages=tuple(pages),
            attachments=attachments,
        )

    @staticmethod
    def record_label(record):
        spec = record.get("spec")
        title = (
            (
                spec["title"]
                or (
                    spec["prizes"][0].get("name")
                    or spec["prizes"][0].get("card", {}).get("character")
                )
            )
            if spec
            else record["card"].get("character")
        )
        return f"{title or 'Giveaway'} · {record['status']} · {record['id'][:4]}"

    def record_panel(self, s, payload, admin, can_host, record):
        body = f"{v2.escaped(self.record_label(record))}\n{engine.count(record)} entries"
        if record.get("pending_edit"):
            body += "\nUpdate pending."
        if record.get("error"):
            body += "\n" + v2.escaped(record["error"])
        if record["message"]:
            body += f"\n[View giveaway](https://discord.com/channels/{s['guild']}/{record['channel']}/{record['message']})"
        controls = []
        if admin or record["host"] == s["owner"]:
            if record["status"] == "active":
                controls += [
                    button("edit", payload, "Edit"),
                    button("ask_end", payload, "End now"),
                    button("ask_cancel", payload, "Cancel", "danger"),
                ]
            if engine.ready_to_reroll(record):
                controls += [button("ask_reroll", payload, "Reroll")]
            controls += [button("duplicate", payload, "Duplicate", disabled=not can_host)]
        elif record["status"] == "active" and record["entries"].get(str(s["owner"])):
            controls.append(button("leave", payload, "Leave"))
        controls.append(button("refresh", payload, "Refresh"))
        return body, controls

    @staticmethod
    def rule_controls(payload, rule, *, setup=False):
        prefix = "setup_" if setup else ""
        return [
            Control(
                "role_select",
                NAMESPACE,
                prefix + "roles",
                payload,
                min_values=0,
                max_values=10,
                default_ids=tuple(rule["roles"]),
                placeholder="Required roles",
            ),
            Control(
                "role_select",
                NAMESPACE,
                "setup_exclude" if setup else "exclude",
                payload,
                min_values=0,
                max_values=10,
                default_ids=tuple(rule["excluded"]),
                placeholder="Excluded roles",
            ),
            select(
                prefix + "mode",
                payload,
                [("All selected roles", "all"), ("Any selected role", "any")],
                selected=rule["mode"],
            ),
            button("setup_age" if setup else "age", payload, "Minimum age"),
        ]
