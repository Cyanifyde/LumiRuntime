from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import json
import logging
import secrets
import time

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts import (
    ChannelTransitionSpec,
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    GuildPolicySnapshot,
    HelpSpec,
    Manifest,
    MatcherExpr,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    PresentationBlock,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    RepeatedExtraction,
    ResultEnvelope,
    ScheduledCallbackSpec,
    TemporaryAttachment,
)
from lumi.contracts.staff_recovery import EDIT_RECOVERY, OBSERVE_RECOVERY, StaffRecoveryAccess
from lumi.contracts.staff_moderation import STAFF_EFFECT, StaffModerationAccess
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from .engine import Engine
from .enforcement import Enforcement
from .images import ImagePipeline
from .models import ACTIONS, HEX_ID, MAX_BYTES, GuardError, choose, group
from .policy import (
    ENFORCEMENT_RESOURCE_FACTS, PRIVILEGED_ROLE_PERMISSIONS, SELECTED_CHANNEL_FACTS, SELECTED_ROLE_FACTS,
    STAFF_CHANNEL_BOT_PERMISSIONS, STAFF_CHANNEL_VIEWER_PERMISSIONS,
)
from .recovery import ROUTE as RECOVER, Recovery
from .urls import IMAGE_ATTACHMENT, IMAGE_LINK, URL_PATTERN

LOGGER = logging.getLogger(__name__)
PREFIX = "lumi:imageguard:ui:"
TTL = 900_000


def escaped(value):
    text = str(value).replace("@", "@\u200b")
    for c in ("\\", "*", "_", "`", "~", "|", ">", "[", "]"):
        text = text.replace(c, "\\" + c)
    return text


class ImageGuardModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        cache_redis,
        store,
        artifact_root,
        temp_root,
        actions=None,
        pipeline=None,
        clock=None,
    ):
        self.generation, self.cache, self.store = generation, cache_redis, store
        self.queue = queue_redis
        self.actions = actions or EdgeActionClient(
            queue_redis, module="imageguard", group="security", generation=generation
        )
        self.pipeline = pipeline or ImagePipeline(artifact_root, temp_root)
        self.pipeline.cache = cache_redis
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.enforcement = Enforcement(store, queue_redis, self.actions, generation, self.clock)
        self.engine = Engine(store, self.pipeline, self.enforcement, self.clock)
        self.recovery = Recovery(
            store, ScheduledCallbackClient(queue_redis, module="imageguard", group="security", generation=generation),
            self.actions, self.clock,
        )
        self.task = None
        self.draining = False
        self._maintenance_busy = False

    @property
    def manifest(self):
        link_url = "(?P<url>" + URL_PATTERN + ")"
        attachment_url = "^" + link_url + "$"
        source_match = MatcherExpr("all", children=(
            MatcherExpr("not", children=(MatcherExpr("exact", "author_bot", "True"),)),
            MatcherExpr("not", children=(MatcherExpr("exact", "administrator", "True"),)),
            MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
            MatcherExpr("any", children=(
                MatcherExpr("regex", "attachments.url", IMAGE_ATTACHMENT),
                MatcherExpr("regex", "content", IMAGE_LINK),
            )),
        ))
        grammar = r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$"
        captures = (CaptureSpec("session"), CaptureSpec("revision", "integer", minimum=0))
        base = dict(
            namespace="imageguard",
            handler="imageguard.panel",
            grammar=grammar,
            captures=captures,
            acl_key="imageguard",
            required_permissions=("administrator",),
            ephemeral=True,
            session_bound=True,
            deadline_ms=120_000,
        )

        def component(action, **kwargs):
            return ComponentSpec(action=action, **base, **kwargs)

        def modal(action, title, fields, **kwargs):
            return component(action, input_kind="modal", modal=ModalSpec(title, fields), **kwargs)

        upload = (
            ModalFieldSpec(
                "images",
                "Images",
                kind="file_upload",
                min_values=1,
                max_values=10,
                file_types=("image",),
            ),
        )
        components = [
            *(
                component(a)
                for a in (
                    "open",
                    "refresh",
                    "back",
                    "up",
                    "down",
                    "disable",
                    "images",
                    "previous",
                    "next",
                    "remove_image",
                    "preview",
                )
            ),
            component("pick", input_kind="string_values"),
            component("pick_image", input_kind="string_values"),
            component("action", input_kind="string_values"),
            component("remove_mode", input_kind="string_values"),
            component("enable", guild_resources=ENFORCEMENT_RESOURCE_FACTS),
            component("add_roles", input_kind="role_ids", guild_resources=SELECTED_ROLE_FACTS),
            component("remove_roles", input_kind="role_ids", guild_resources=SELECTED_ROLE_FACTS),
            component(
                "channel", input_kind="channel_ids", channel_types=(0,),
                guild_resources=SELECTED_CHANNEL_FACTS,
            ),
            modal(
                "create",
                "New image group",
                (ModalFieldSpec("name", "Name", minimum=1, maximum=60),),
            ),
            modal(
                "delete",
                "Delete group",
                (ModalFieldSpec("confirmation", "Type DELETE", minimum=6, maximum=6),),
            ),
            modal(
                "edit",
                "Image group",
                (
                    ModalFieldSpec("name", "Name", required=False, maximum=60),
                    ModalFieldSpec("distance", "Distance · REDACTED", required=False, maximum=2),
                ),
            ),
            modal("upload", "Add blocked images", upload),
            modal("test", "Test without taking action", upload),
            modal(
                "confirm",
                "Confirm automatic kick or ban",
                (
                    ModalFieldSpec(
                        "confirmation",
                        "Similar matches can be wrong. Type ENABLE",
                        minimum=6,
                        maximum=6,
                    ),
                ),
                guild_resources=ENFORCEMENT_RESOURCE_FACTS,
            ),
        ]
        return Manifest(
            schema=2,
            module="imageguard",
            channel_transition=ChannelTransitionSpec(quiesce_guild=True),
            group="security",
            generation=self.generation,
            job_stream="lumi:jobs:security",
            display_name="ImageGuard",
            summary="Grouped image moderation.",
            version="1.0.2",
            presentation_mode="v2",
            commands=(
                CommandSpec(
                    root="imageguard",
                    handler="imageguard.open",
                    grammar=r"^\s*$",
                    acl_key="imageguard",
                    required_permissions=("administrator",),
                    context_fields=("used_prefix",),
                    queue_class="security",
                    help=HelpSpec(
                        ".imageguard", "Manage blocked images and matching rules."
                    ),
                ),
            ),
            components=tuple(components),
            passive=tuple(
                PassiveMatcherSpec(
                    route="imageguard." + event,
                    handler="imageguard.scan",
                    event=event,
                    expression=source_match,
                    queue_class="security",
                    repeated=(
                        RepeatedExtraction("attachments.url", attachment_url, 10, (CaptureSpec("url"),)),


                        RepeatedExtraction("content", link_url, 100, (CaptureSpec("url"),)),
                    ),
                    hydrate=event == "message_edit",
                    hydrate_matching_edit=event == "message_edit",
                    context_fields=(
                        "source_revision_ms",
                        "author_bot",
                        "webhook_id",
                    ),
                    staff_moderation=StaffModerationAccess(
                        STAFF_CHANNEL_VIEWER_PERMISSIONS, STAFF_CHANNEL_BOT_PERMISSIONS,
                        PRIVILEGED_ROLE_PERMISSIONS,
                        ("inspect", "observe_source", "send", "edit", "delete", "add_role", "remove_role", "kick", "ban"),
                    ),
                    deadline_ms=120_000,
                )
                for event in ("message_create", "message_edit")
            ),
            scheduled=(ScheduledCallbackSpec(
                RECOVER, RECOVER, deadline_ms=60_000, queue_class="security", scope="guild",
                staff_recovery=StaffRecoveryAccess(STAFF_CHANNEL_VIEWER_PERMISSIONS, STAFF_CHANNEL_BOT_PERMISSIONS),
            ),),
            capabilities=(STAFF_EFFECT, OBSERVE_RECOVERY, EDIT_RECOVERY),
        )

    async def start(self):
        self.task = asyncio.create_task(self._maintain(), name="imageguard-maintenance")

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.pipeline.close()
        await self.store.close()

    def begin_drain(self):
        self.draining = True

    def resume(self):
        self.draining = False

    def drain_ready(self):
        return self.pipeline.active == 0 and not self._maintenance_busy

    async def _maintain(self):
        while True:
            await asyncio.sleep(60)
            try:
                if not self.draining:
                    self._maintenance_busy = True
                    await self.recovery.schedule()
                    await self.store.cleanup(self.clock())
                    await self.pipeline.prune(await self.store.references())
                    cutoff = time.time() - 600
                    async for raw in self.cache.sscan_iter(PREFIX + "temporary", count=100):
                        name = raw.decode() if isinstance(raw, bytes) else raw
                        if not HEX_ID.fullmatch(name):
                            continue
                        path = self.pipeline.temp / name
                        if not path.exists():
                            await self.cache.srem(PREFIX + "temporary", name)
                        elif (
                            path.is_file()
                            and not path.is_symlink()
                            and path.stat().st_mtime < cutoff
                        ):
                            path.unlink(missing_ok=True)
                            await self.cache.srem(PREFIX + "temporary", name)
            except (MySQLError, RedisError, OSError, ValueError):
                LOGGER.warning("imageguard_maintenance_unavailable")
            finally:
                self._maintenance_busy = False

    def result(self, job, intent=None):
        return ResultEnvelope(
            1,
            job,
            "imageguard",
            "security",
            self.generation,
            "ok",
            intents=(intent,) if intent else (),
        )

    async def handle(self, request, job_id, generation):
        guild, owner = (int(request.event.get(k) or 0) for k in ("guild_id", "actor_id"))
        if (
            generation != self.generation
            or self.draining
            or request.event.get("_expires_at_ms", self.clock() + 1) <= self.clock()
        ):
            return self.result(job_id)
        try:
            if request.handler == RECOVER:
                await self.recovery.run(request, job_id)
                return self.result(job_id)
            if request.handler == "imageguard.scan":
                raw = await self.queue.hget("lumi:policy:current", str(guild))
                if not raw:
                    return self.result(job_id)
                policy = msgspec.json.decode(raw, type=GuildPolicySnapshot)
                if policy.guild_id != guild or "imageguard" not in policy.enabled_modules:
                    return self.result(job_id)
                await self.engine.observe(request.event, job_id, request.arguments, policy=policy)
                return self.result(job_id)
            if guild <= 0 or owner <= 0 or not request.event.get("administrator"):
                return self.result(job_id)
            if request.handler == "imageguard.open":
                session = self.session(guild, owner, int(request.event["channel_id"]), "opener")
                await self.save(session)
                control = self.control(session, "open", "Open ImageGuard", style="primary")
                return self.result(
                    job_id,
                    DiscordIntent(
                        "send",
                        pages=(PresentationPage(title="ImageGuard", controls=(control,)),),
                        session_id=session["token"],
                    ),
                )
            return self.result(job_id, await self.panel(request))
        except (
            RedisError,
            MySQLError,
            ActionUnavailable,
            TimeoutError,
            OSError,
            msgspec.DecodeError,
        ):
            LOGGER.warning("imageguard_unavailable")
            return self.result(
                job_id,
                None
                if request.handler in {"imageguard.scan", RECOVER}
                else DiscordIntent(
                    "ephemeral", pages=(PresentationPage(description="Unavailable. Try again."),)
                ),
            )

    def session(self, guild, owner, channel, screen="home"):
        return dict(
            token=secrets.token_hex(8),
            guild=guild,
            owner=owner,
            channel=channel,
            expires=self.clock() + TTL,
            revision=0,
            config_revision=0,
            group="",
            image="",
            page=0,
            screen=screen,
        )

    async def save(self, session):
        remaining = session["expires"] - self.clock()
        if remaining > 0:
            await self.cache.set(PREFIX + session["token"], json.dumps(session), px=remaining)

    def control(self, s, action, label=None, kind="button", **kwargs):
        return PresentationControl(
            kind, "imageguard", action, f"{s['token']}~{s['revision']}", label=label, **kwargs
        )

    async def panel(self, request):
        token = str(request.arguments.get("session") or "")
        if len(token) != 16 or any(c not in "0123456789abcdef" for c in token):
            return None
        async with self.cache.lock(PREFIX + token + ":lock", timeout=120, blocking_timeout=1):
            raw = await self.cache.get(PREFIX + token)
            s = json.loads(raw) if raw else None
            e = request.event
            bound = await self.cache.get(ui_key("message", token))
            if (
                not s
                or s["expires"] <= self.clock()
                or s["revision"] != request.arguments.get("revision")
                or (s["guild"], s["owner"], s["channel"])
                != (e.get("guild_id"), e.get("actor_id"), e.get("channel_id"))
                or not bound
                or int(bound) != e.get("message_id")
            ):
                return DiscordIntent(
                    "ephemeral",
                    pages=(PresentationPage(description="Panel expired. Run .imageguard again."),),
                )
            action = request.route.rpartition(".")[2]
            if action == "open":
                if s["screen"] != "opener":
                    return None
                s = self.session(s["guild"], s["owner"], s["channel"])
                config = await self.store.snapshot(s["guild"])
                s["config_revision"] = config["revision"]
                await self.save(s)
                return await self.render(s, config, kind="ephemeral")
            if s["screen"] == "opener":
                return None
            config = await self.store.snapshot(s["guild"])
            notice, preview = None, None
            try:
                notice, preview = await self.apply(s, action, request, config)
            except GuardError as exc:
                notice = str(exc)
            config = await self.store.snapshot(s["guild"])
            s.update(revision=s["revision"] + 1, config_revision=config["revision"])
            await self.save(s)
            return await self.render(s, config, notice=notice, preview=preview)

    async def apply(self, s, action, request, config):
        values = request.arguments.get("values", ())
        resources = request.event.get("_guild_resources") or {}
        selected = next((g for g in config["groups"] if g["id"] == s["group"]), None)
        preview = None

        def update(fn):
            return self.enforcement.change(s["guild"], fn, s["config_revision"])

        def selected_change(fn):
            def change(doc):
                current = next((g for g in doc["groups"] if g["id"] == s["group"]), None)
                if current is None:
                    raise GuardError("Group unavailable.")
                fn(current, doc)

            return update(change)

        if action == "refresh":
            return None, None
        if action == "back":
            if s["screen"] == "images":
                s["screen"] = "group"
            else:
                s.update(screen="home", group="")
        elif action == "pick":
            identifier = str(values[0]) if values else ""
            if not any(g["id"] == identifier for g in config["groups"]):
                raise GuardError("Group unavailable.")
            s.update(group=identifier, screen="group", page=0, image="")
        elif action == "create":
            new = group(str(values.get("name") or ""))
            await update(lambda doc: doc["groups"].append(new))
            s.update(group=new["id"], screen="group")
        elif action == "channel":
            identifier = int(values[0]) if values else 0
            if identifier and identifier not in resources.get("text_channel_ids", ()):
                raise GuardError("Choose a private staff channel Lumi can read and upload to.")

            def change(doc):
                doc["log_channel"] = identifier
                if not identifier:
                    for g in doc["groups"]:
                        g["enabled"] = False

            await update(change)
        elif action == "test":
            checked = config
            if selected:
                checked = {**config, "groups": [selected]}
            files = request.arguments.get("files", {}).get("images", ())
            observed = await self.upload(s["guild"], files, save=False)
            matched = choose(checked, observed, include_disabled=bool(selected))
            if matched:
                g = matched["group"]
                return (
                    f"Test · {escaped(g['name'])}\n"
                    f"{matched['match']} · distance {matched['distance']}\nWould delete"
                    + {
                        "delete": ".",
                        "roles": " and change roles.",
                        "kick": " and kick.",
                        "ban": " and ban.",
                    }[g["action"]]
                    + "\nNo action taken.",
                    None,
                )
            return "Test · No match.", None
        elif not selected:
            raise GuardError("Choose a group.")
        elif action == "edit":
            name = str(values.get("name") or "").strip()
            distance = str(values.get("distance") or "").strip()
            if name:
                group(name)
            if distance and (
                not distance.isascii() or not distance.isdigit() or not 0 <= int(distance) <= int("REDACTED")
            ):
                raise GuardError("Use a distance from REDACTED.")

            def change(g, _):
                if name:
                    g["name"] = name
                if distance and int(distance) != g["distance"]:
                    g["distance"] = int(distance)
                    if g["action"] in {"kick", "ban"}:
                        g.update(enabled=False, confirmed=False)

            await selected_change(change)
        elif action == "action":
            choice = str(values[0]) if values else ""
            if choice not in ACTIONS:
                raise GuardError("Invalid action.")
            await selected_change(
                lambda g, _: g.update(action=choice, enabled=False, confirmed=False)
            )
        elif action == "remove_mode":
            mode = str(values[0]) if values else ""
            if mode not in {"none", "selected", "all"}:
                raise GuardError("Invalid removal mode.")
            await selected_change(
                lambda g, _: g.update(
                    remove_all=mode == "all",
                    remove_roles=g["remove_roles"] if mode == "selected" else [],
                )
            )
        elif action in {"add_roles", "remove_roles"}:
            identifiers = list(dict.fromkeys(int(v) for v in values))
            roles = {r["id"]: r for r in resources.get("roles", ())}
            if any(
                r not in roles
                or not roles[r]["manageable"]
                or (action == "add_roles" and roles[r]["permissions"])
                for r in identifiers
            ):
                raise GuardError("Choose manageable roles without privileged grants.")

            def change(g, _):
                g[action] = identifiers
                if action == "remove_roles":
                    g["remove_all"] = False

            await selected_change(change)
        elif action in {"enable", "confirm", "disable"}:
            if action == "enable" and selected["action"] in {"kick", "ban"}:
                raise GuardError("Use the confirmation button to enable kick or ban.")
            if action == "confirm" and str(values.get("confirmation") or "") != "ENABLE":
                raise GuardError("Type ENABLE to confirm.")
            if action != "disable":
                self.check_resources(selected, config, resources)
            await selected_change(
                lambda g, _: g.update(
                    enabled=action != "disable", confirmed=action == "confirm" or g["confirmed"]
                )
            )
        elif action in {"up", "down"}:

            def change(doc):
                index = next(i for i, g in enumerate(doc["groups"]) if g["id"] == s["group"])
                target = max(0, min(len(doc["groups"]) - 1, index + (-1 if action == "up" else 1)))
                doc["groups"][index], doc["groups"][target] = (
                    doc["groups"][target],
                    doc["groups"][index],
                )

            await update(change)
        elif action == "delete":
            if str(values.get("confirmation") or "") != "DELETE":
                raise GuardError("Type DELETE.")

            def change(doc):
                doc["groups"] = [g for g in doc["groups"] if g["id"] != s["group"]]
                doc["images"] = [i for i in doc["images"] if i["group"] != s["group"]]

            await update(change)
            s.update(screen="home", group="")
        elif action == "upload":
            files = request.arguments.get("files", {}).get("images", ())
            added = await self.upload(s["guild"], files, save=True)

            def change(g, doc):
                hashes = {i["sha256"] for i in doc["images"] if i["group"] == g["id"]}
                for image in added:
                    if image["sha256"] not in hashes:
                        doc["images"].append(dict(**image, id=secrets.token_hex(16), group=g["id"]))
                        hashes.add(image["sha256"])

            await selected_change(change)
        elif action in {"images", "previous", "next", "pick_image", "remove_image", "preview"}:
            s["screen"] = "images"
            if action in {"previous", "next"}:
                s["page"] = max(0, s["page"] + (-1 if action == "previous" else 1))
            if action == "pick_image":
                s["image"] = str(values[0]) if values else ""
            image = next(
                (
                    i
                    for i in config["images"]
                    if i["group"] == selected["id"] and i["id"] == s["image"]
                ),
                None,
            )
            if action in {"remove_image", "preview"} and image is None:
                raise GuardError("Choose an image.")
            if action == "remove_image":

                def change(g, doc):
                    doc["images"] = [i for i in doc["images"] if i["id"] != image["id"]]
                    if not any(i["group"] == g["id"] for i in doc["images"]):
                        g["enabled"] = False

                await selected_change(change)
                s["image"] = ""
            elif action == "preview":
                data = await asyncio.to_thread(self.pipeline.path(image["sha256"]).read_bytes)
                if not self.pipeline.enter(s["guild"]):
                    raise GuardError("Scanner busy. Try again.")
                try:
                    meta, png = await self.pipeline.analyse(data)
                    if meta["sha256"] != image["sha256"]:
                        raise GuardError("Stored image is damaged.")
                    preview = await self.pipeline.attachment(png)
                    await self.cache.sadd(PREFIX + "temporary", preview["reference"][4:])
                finally:
                    self.pipeline.leave(s["guild"])
        return None, preview

    @staticmethod
    def check_resources(g, config, resources):
        if config["log_channel"] not in resources.get("text_channel_ids", ()):
            raise GuardError("Choose a private staff channel Lumi can read and upload to.")
        if g["action"] == "roles":
            roles = {r["id"]: r for r in resources.get("roles", ())}
            if any(
                r not in roles or not roles[r]["manageable"]
                for r in g["remove_roles"] + g["add_roles"]
            ) or any(roles[r]["permissions"] for r in g["add_roles"]):
                raise GuardError("A configured role is unavailable or privileged.")

    async def upload(self, guild, files, *, save):
        if not 1 <= len(files) <= 10 or any(
            not 0 < int(f.get("size") or 0) <= MAX_BYTES for f in files
        ):
            raise GuardError("1-10 images. Up to 10 MiB each.")
        if not self.pipeline.enter(guild):
            raise GuardError("Scanner busy. Try again.")
        try:
            added = []
            for f in files:
                data = await self.pipeline.fetch(f["url"])
                meta, _ = await self.pipeline.analyse(data)
                if save:
                    await self.pipeline.save_reference(meta, data)
                added.append(meta)
            return added
        finally:
            self.pipeline.leave(guild)

    async def render(self, s, config, *, kind="edit", notice=None, preview=None):
        def c(action, label=None, **kw):
            return self.control(s, action, label, **kw)

        controls, lines = [], []
        selected = next((g for g in config["groups"] if g["id"] == s["group"]), None)
        title = "ImageGuard"
        if not selected or s["screen"] == "home":
            s["screen"] = "home"
            stats = await self.store.status(s["guild"])
            health = str(stats.get("health") or "")
            lines.append("Needs attention · " + health.replace("_", " ") if health else "Ready")
            lines.extend(
                (
                    f"Groups · {len(config['groups'])} · Images · {len(config['images'])}",
                    f"Scanned · {stats.get('scanned', 0):,}"
                    f" · Matches · {stats.get('matched', 0):,}",
                    f"Skipped · {stats.get('skipped', 0):,} · Errors · {stats.get('errors', 0):,}",
                )
            )
            controls.append(
                c(
                    "channel",
                    kind="channel_select",
                    placeholder="Staff log channel",
                    channel_types=(0,),
                    min_values=0,
                    max_values=1,
                    default_ids=(config["log_channel"],) if config["log_channel"] else (),
                )
            )
            if config["groups"]:
                controls.append(
                    c(
                        "pick",
                        kind="string_select",
                        placeholder="Choose a group",
                        options=tuple(
                            PresentationOption(
                                g["name"],
                                g["id"],
                                description=f"{n} · {'On' if g['enabled'] else 'Off'}",
                            )
                            for n, g in enumerate(config["groups"], 1)
                        ),
                    )
                )
            controls.extend((c("create", "New group"), c("test", "Test"), c("refresh", "Refresh")))
        elif s["screen"] == "images":
            title = escaped(selected["name"])
            images = [i for i in config["images"] if i["group"] == selected["id"]]
            s["page"] = min(s["page"], max(0, (len(images) - 1) // 10))
            start = s["page"] * 10
            shown = images[start : start + 10]
            lines.append(
                f"{len(images)} images · Page {s['page'] + 1}/{max(1, (len(images) + 9) // 10)}"
            )
            if shown:
                controls.append(
                    c(
                        "pick_image",
                        kind="string_select",
                        placeholder="Choose an image",
                        options=tuple(
                            PresentationOption(
                                f"Image {start + n}", i["id"], default=i["id"] == s["image"]
                            )
                            for n, i in enumerate(shown, 1)
                        ),
                    )
                )
            controls.extend(
                (
                    c("previous", "Previous", disabled=s["page"] == 0),
                    c("next", "Next", disabled=start + 10 >= len(images)),
                    c("preview", "View", disabled=not s["image"]),
                    c("remove_image", "Remove image", style="danger", disabled=not s["image"]),
                    c("back", "Back"),
                )
            )
        else:
            title = escaped(selected["name"])
            count = sum(i["group"] == selected["id"] for i in config["images"])
            mode = (
                "all"
                if selected["remove_all"]
                else "selected"
                if selected["remove_roles"]
                else "none"
            )
            lines.extend(
                (
                    ("On" if selected["enabled"] else "Off") + f" · {count} images",
                    f"Distance · {selected['distance']}",
                )
            )
            if selected["action"] in {"kick", "ban"}:
                lines.append("Similar matches can be wrong.")
            controls.append(
                c(
                    "action",
                    kind="string_select",
                    placeholder="Action",
                    options=tuple(
                        PresentationOption(label, action, default=action == selected["action"])
                        for action, label in zip(
                            ACTIONS,
                            (
                                "Delete",
                                "Delete and change roles",
                                "Delete and kick",
                                "Delete and ban",
                            ),
                            strict=True,
                        )
                    ),
                )
            )
            if selected["action"] == "roles":
                controls.append(
                    c(
                        "remove_mode",
                        kind="string_select",
                        placeholder="Roles to remove",
                        options=tuple(
                            PresentationOption(label, value, default=value == mode)
                            for value, label in (
                                ("none", "Remove none"),
                                ("selected", "Remove selected roles"),
                                ("all", "Remove all removable roles"),
                            )
                        ),
                    )
                )
                controls.append(
                    c(
                        "remove_roles",
                        kind="role_select",
                        placeholder="Select roles to remove",
                        min_values=0,
                        max_values=25,
                        default_ids=tuple(selected["remove_roles"]),
                    )
                )
                controls.append(
                    c(
                        "add_roles",
                        kind="role_select",
                        placeholder="Replacement roles to give",
                        min_values=0,
                        max_values=25,
                        default_ids=tuple(selected["add_roles"]),
                    )
                )
            enable = (
                "disable"
                if selected["enabled"]
                else "confirm"
                if selected["action"] in {"kick", "ban"}
                else "enable"
            )
            controls.extend(
                (
                    c(
                        enable,
                        "Disable" if selected["enabled"] else "Enable",
                        style="danger" if enable == "confirm" else "primary",
                    ),
                    c("upload", "Add images"),
                    c("images", "Images"),
                    c("edit", "Name / matching"),
                    c("test", "Test"),
                    c("up", "Move up"),
                    c("down", "Move down"),
                    c("delete", "Delete group", style="danger"),
                    c("back", "Back"),
                )
            )
        if notice:
            lines.append(notice)
        page = PresentationPage(title=title, description="\n".join(lines), controls=tuple(controls))
        if preview:
            rows, buttons = [], []
            for control in controls:
                if control.kind != "button":
                    if buttons:
                        rows.append(PresentationBlock("action_row", controls=tuple(buttons)))
                        buttons = []
                    rows.append(PresentationBlock("action_row", controls=(control,)))
                else:
                    buttons.append(control)
                    if len(buttons) == 5:
                        rows.append(PresentationBlock("action_row", controls=tuple(buttons)))
                        buttons = []
            if buttons:
                rows.append(PresentationBlock("action_row", controls=tuple(buttons)))
            page = PresentationPage(
                blocks=(
                    PresentationBlock("heading", text=title, level=2),
                    PresentationBlock("text", text="\n".join(lines)),
                    PresentationBlock("media", file_name=preview["file_name"], spoiler=True),
                    *rows,
                )
            )
        return DiscordIntent(
            kind,
            pages=(page,),
            session_id=s["token"],
            attachments=(TemporaryAttachment(**preview),) if preview else (),
        )
