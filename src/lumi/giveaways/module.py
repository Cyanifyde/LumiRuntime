from __future__ import annotations

from lumi.transport import outbound

from lumi.ui_keys import ui_key

import asyncio
import hashlib
import json
import logging
import secrets
import time

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts import (
    ChannelTransitionSpec,
    Announcement,
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordActionResult,
    DiscordEffectGrant,
    DiscordIntent,
    GuildPolicySnapshot,
    HelpSpec,
    Manifest,
    MatcherExpr,
    MessageMutation,
    PassiveMatcherSpec,
    PresentationControl,
    PresentationPage,
    ResultEnvelope,
    ScheduledCallbackSpec,
    decode,
    encode,
)
from lumi.shared.discord_effects import (
    EFFECT_GRANT_SECONDS, MUTATE_MESSAGE, PUBLISH_MESSAGE, REPLY_TEXT,
    effect_key, mutation_lane, payload_digest, source_digest, stage_mutation,
)
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from . import expanded as v2
from . import models as engine
from .builder import GiveawayBuilder
from .builder import components as builder_components
from .protocol import (
    ACTOR_CHANNELS, CHANNEL_MEMBERS, DRAW_BATCH_SIZE, DRAW_DESTINATION, HOST_PERMISSIONS,
    MUTATE_CHANNEL, MUTATE_GUILD, MUTATE_RENDER_CHANNEL,
    PUBLISH_CHANNEL, PUBLISH_GUILD, REPLY_CHANNEL, REPLY_GUILD, pending_edit_digest, publication_binding,
    SOURCE_FACTS, SOURCE_SCOPE, revision_digest, source_query,
)
from .source import SourceObservation, observed_view, verify_views
from .recovery import READ_HISTORY, recover_history
from .workflow import GiveawayWorkflow

LOGGER = logging.getLogger(__name__)
CAPABILITIES = (
    ACTOR_CHANNELS, READ_HISTORY, CHANNEL_MEMBERS, SOURCE_FACTS, REPLY_TEXT, PUBLISH_MESSAGE, MUTATE_MESSAGE,
)


class GiveawaysModule:
    def __init__(
        self,
        *,
        generation,
        queue_redis,
        cache_redis,
        store,
        assets,
        actions=None,
        scheduler=None,
        clock=None,
    ):
        self.generation, self.queue, self.cache = generation, queue_redis, cache_redis
        self.store, self.assets = store, assets
        self.actions = actions or EdgeActionClient(
            queue_redis, module=engine.MODULE, group="cards", generation=generation
        )
        self.scheduler = scheduler or ScheduledCallbackClient(
            queue_redis, module=engine.MODULE, group="cards", generation=generation
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.stopped = self.draining = self.ticking = False
        self.task = None
        self.locks = {}
        self.builder = GiveawayBuilder(self)

    @property
    def workflow(self):
        return GiveawayWorkflow(
            self.store, self.clock, revoke_publication=self.revoke_publication,
            revoke_mutation=self.revoke_mutation,
        )

    async def request_discord(self, *, job_id, kind, guild_id, **kwargs):
        result = await self.actions.request(job_id=job_id, kind=kind, guild_id=guild_id, **kwargs)
        if kind == MUTATE_MESSAGE:
            return result
        return await self.apply_discord_receipt(result, kind=kind, guild_id=guild_id, **kwargs)

    async def actor_access(self, job, guild, actor, origin, targets, *, reference=None, link_field=None):
        if targets is not None:
            targets = tuple(dict.fromkeys(channel for channel in targets if channel))
        result = await self.request_discord(
            job_id=job, kind=ACTOR_CHANNELS, guild_id=guild, channel_id=origin, member_id=actor,
            target_channel_ids=targets, idempotency_key=f"gw-ui-access:{job}",
            message_id=reference, message_link_field=link_field,
        )
        if not result.ok:
            if result.error_code == "invalid_message_link":
                raise engine.GiveawayError("Use a card-view link from this server.")
            if result.error_code == "source_unavailable":
                raise engine.GiveawayError("Card view unavailable.")
            raise engine.GiveawayError(result.data.get("message", "Unavailable."))
        access = result.data.get("access")
        self.workflow.require_facts(access, job=job, guild=guild, actor=actor, origin=origin)
        if targets is not None and not set(access["channels"]) <= set(targets):
            raise engine.GiveawayError("Unavailable.")
        return access

    async def apply_discord_receipt(self, result, *, kind, guild_id, **kwargs):
        data = getattr(result, "data", None)
        deletion = data.get("deleted_post") if isinstance(data, dict) else None
        if deletion is None:
            return result
        if (result.ok and kind != "giveaway_apply") or (not result.ok and result.error_code != "message_not_found"):
            raise engine.GiveawayError("Unavailable.")
        self.workflow.require_facts(
            deletion, operation=kind, guild=guild_id, receipt=kwargs.get("idempotency_key"),
        )
        identifier = deletion.get("record")
        if not isinstance(identifier, str) or not engine.ID.fullmatch(identifier):
            raise engine.GiveawayError("Unavailable.")
        if kind in {"giveaway_apply", "giveaway_render", "giveaway_complete"} and identifier != kwargs.get("giveaway_id"):
            raise engine.GiveawayError("Unavailable.")
        await self.workflow.observe_deleted(
            guild_id, deletion.get("channel"), deletion.get("message"), identifier=identifier,
        )
        if not result.ok and result.error_code == "message_not_found":
            result = msgspec.structs.replace(result, data={**data, "message": "Giveaway was deleted."})
        return result

    @property
    def manifest(self):
        components = [
            ComponentSpec(
                "giveaway",
                action,
                "giveaways.component",
                r"^(?P<id>[a-f0-9]{16})$",
                captures=(CaptureSpec("id"),),
                public=True,
                ephemeral=True,
                channel_members=DRAW_DESTINATION,
            )
            for action in ("join", "reroll")
        ]
        for action in ("open", "channel", "role", "host_role", "confirm", "dismiss"):
            components.append(
                ComponentSpec(
                    "giveaway",
                    action,
                    "giveaways.component",
                    r"^(?P<session>[a-f0-9]{16})$",
                    captures=(CaptureSpec("session"),),
                    session_bound=True,
                    ephemeral=True,
                    channel_members=DRAW_DESTINATION if action == "confirm" else None,
                    text_reply=REPLY_CHANNEL if action == "confirm" else None,
                    message_mutation=MUTATE_RENDER_CHANNEL if action == "confirm" else None,
                    input_kind="channel_ids"
                    if action == "channel"
                    else "role_ids"
                    if action in {"role", "host_role"}
                    else "none",
                    channel_types=(0,) if action == "channel" else (),
                    required_permissions=("administrator",)
                    if action in {"open", "channel", "role", "host_role"}
                    else (),
                )
            )
        return Manifest(
            schema=2,
            module=engine.MODULE,
            channel_transition=ChannelTransitionSpec(quiesce_guild=True, rebuild_posts=True),
            group="cards",
            generation=self.generation,
            job_stream="lumi:jobs:cards",
            display_name="Giveaways",
            summary="Card and custom-prize giveaways.",
            presentation_mode="v2",
            version="2.0.3",
            action_presentation_modes=("legacy", "v2"),
            commands=(
                CommandSpec(
                    "giveaway",
                    "giveaways.command",
                    r"(?s)^\s*(?P<raw>.{0,160})$",
                    captures=(CaptureSpec("raw", required=False, maximum=160),),
                    public=True,
                    acl_key="giveaway",
                    channel_members=DRAW_DESTINATION,
                    embed_sources=SOURCE_SCOPE,
                    text_reply=REPLY_GUILD,
                    channel_publication=PUBLISH_GUILD,
                    message_mutation=MUTATE_GUILD,
                    context_fields=("reference_message_id", "reference_embed"),
                    help=HelpSpec(
                        ".giveaway\n.giveaway <duration>\n.giveaway setup",
                        "Open the giveaway hub; reply to a card view for the duration shortcut."
                    ),
                ),
            ),
            passive=(PassiveMatcherSpec(
                "giveaways.post_deleted", "giveaways.post_deleted", "message_delete",
                MatcherExpr("exists", field="message_id"), own_output_deleted=True,
                deadline_ms=engine.WINDOW,
            ),),
            components=tuple(components) + builder_components(),
            scheduled=(
                ScheduledCallbackSpec(
                    "giveaways.work", "giveaways.work", scope="guild", deadline_ms=engine.WINDOW,
                    own_history=True,
                    channel_members=DRAW_DESTINATION,
                    embed_sources=SOURCE_SCOPE,
                    text_reply=REPLY_CHANNEL,
                    channel_publication=PUBLISH_CHANNEL,
                    message_mutation=MUTATE_CHANNEL,
                ),
            ),
            capabilities=CAPABILITIES,
        )

    async def start(self):
        await self.assets.start()
        self.task = asyncio.create_task(self._run(), name="card-giveaways")

    async def close(self):
        self.stopped = True
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.store.close()

    def begin_drain(self):
        self.draining = True

    def resume(self):
        self.draining = False

    def drain_ready(self):
        return not self.ticking and not any(lock.locked() for lock in self.locks.values())

    async def enabled(self):
        raw = await self.queue.hget(outbound("lumi:manifests:current", "cards"), engine.MODULE)
        if not raw:
            return set()
        manifest = msgspec.json.decode(raw, type=Announcement).manifest
        if manifest.generation != self.generation or manifest.group != "cards":
            return set()
        guilds = set()
        for value in await self.queue.hvals("lumi:policy:current"):
            policy = msgspec.json.decode(value, type=GuildPolicySnapshot)
            if engine.MODULE in policy.enabled_modules:
                guilds.add(policy.guild_id)
        return guilds

    async def _run(self):
        while not self.stopped:
            if not self.draining:
                self.ticking = True
                try:
                    await self.tick()
                except (RedisError, MySQLError, TimeoutError, ValueError):
                    LOGGER.warning("giveaways_reconcile_unavailable")
                finally:
                    self.ticking = False
            await asyncio.sleep(5)

    async def tick(self):
        guilds = await self.enabled()
        now = self.clock()
        if hasattr(self.store, "expire_drafts"):
            await self.store.expire_drafts(now)
        gate = getattr(self, "handover_gate", None)
        for row in await self.store.due(now, guilds=guilds):
            if row["guild"] not in guilds or (gate and gate.critical(row["guild"])):
                continue


            await self.scheduler.publish(
                operation="upsert",
                schedule_key="giveaways:" + row["id"],
                route="giveaways.work",
                revision=max(1, now),
                guild_id=row["guild"],
                channel_id=row["channel"],
                actor_id=0,
                due_at_ms=now,
                expires_at_ms=now + engine.WINDOW,
                arguments={"id": row["id"]},
            )

    async def handle(self, request, job_id, generation):
        if generation != self.generation or self.clock() >= int(
            request.event.get("_expires_at_ms", self.clock() + 1)
        ):
            return ResultEnvelope(
                schema=1,
                job_id=job_id,
                module=engine.MODULE,
                group="cards",
                generation=self.generation,
                status="ok",
                intents=(),
                completed_at_ms=self.clock(),
            )
        try:
            if request.handler == "giveaways.command":
                intent = await self.command(request, job_id)
            elif request.handler == "giveaways.component":
                intent = await self.component(request, job_id)
            elif request.handler == "giveaways.builder":
                intent = await self.builder.handle(request, job_id)
            elif request.handler == "giveaways.work":
                await self.work(
                    int(request.event["guild_id"]), str(request.arguments["id"]), job_id
                )
                intent = None
            elif request.handler == "giveaways.post_deleted":
                event = request.event
                if event.get("dispatch") == "passive" and event.get("_own_output_deleted") is True:
                    await self.workflow.observe_deleted(
                        event.get("guild_id"), event.get("channel_id"), event.get("message_id"),
                    )
                intent = None
            else:
                raise engine.GiveawayError("Unavailable.")
        except (
            engine.GiveawayError,
            ActionUnavailable,
            TimeoutError,
            OSError,
            MySQLError,
            RedisError,
        ) as exc:
            text = str(exc) if isinstance(exc, engine.GiveawayError) else "Unavailable. Try again."
            if request.handler in {"giveaways.work", "giveaways.post_deleted"}:
                LOGGER.warning("giveaway_work_unavailable")
                intent = None
            else:
                intent = self.text(
                    "send" if request.handler == "giveaways.command" else "ephemeral", text
                )
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module=engine.MODULE,
            group="cards",
            generation=self.generation,
            status="ok",
            intents=(intent,) if intent else (),
            completed_at_ms=self.clock(),
        )

    @staticmethod
    def text(kind, value):
        return DiscordIntent(kind, pages=(PresentationPage(description=value),))

    async def session(self, owner, guild, **data):
        token = secrets.token_hex(8)
        value = dict(owner=owner, guild=guild, expires=self.clock() + 900_000, **data)
        pipeline = self.cache.pipeline(transaction=True)
        pipeline.setex("lumi:giveaways:session:" + token, 900, json.dumps(value))
        pipeline.setex(ui_key("owner", token), 900, str(owner))
        await pipeline.execute()
        return token

    async def command(self, request, job):
        actor, guild, channel = (
            int(request.event[key]) for key in ("actor_id", "guild_id", "channel_id")
        )
        raw = str(request.arguments.get("raw") or "").strip()
        if not raw:
            return await self.builder.launch(
                actor, guild, int(request.event["message_id"]), channel, job=job
            )
        if raw.casefold() == "help":
            return self.text(
                "send",
                "Reply to a card view: `.giveaway 2hs`\nSetup: `.giveaway setup`\n"
                "Delete the giveaway post to stop it.",
            )
        if raw.casefold() == "setup":
            if not request.event.get("administrator"):
                raise engine.GiveawayError("Administrator only.")
            return await self.builder.launch(
                actor, guild, int(request.event["message_id"]), channel, job=job, setup=True
            )
        wait = engine.parse_duration(raw)
        access = observed = None
        prior = await self.store.for_command(guild, int(request.event["message_id"]))
        if prior is None:
            reference = int(request.event.get("reference_message_id") or 0)
            if not reference:
                raise engine.GiveawayError("Reply to a fresh game-bot card view.")
            config = await self.store.config(guild)
            access = await self.actor_access(
                job, guild, actor, channel, (config["channel"],), reference=reference,
            )
            source = observed_view(
                access.get("source"), guild, channel, self.clock(), message=reference,
            )
            if source["owner"] != actor:
                raise engine.GiveawayError("That card is not yours.")
            channels, roles, permissions = self.builder.access_sets(access)
            if config["channel"] not in channels:
                raise engine.GiveawayError("Giveaway channel unavailable.")
            if not self.builder.can_host(config, roles, permissions):
                if not config.get("host_role"):
                    raise engine.GiveawayError("Hosting is limited to moderators until a host role is set.")
                raise engine.GiveawayError("Moderator permission or the host role required.")
            image = await self.assets.ingest(source["image_url"])
            spec = v2.config_defaults(config)
            card = dict(source)
            card.pop("image_url")
            spec.update(
                duration=wait,
                prizes=[
                    dict(
                        id=secrets.token_hex(8),
                        kind="card",
                        copies=1,
                        card=card,
                        image=image,
                        source_channel=channel,
                        proof=engine.source_fingerprint(source),
                    )
                ],
            )
            draft_id = hashlib.sha256(
                f"quick:{guild}:{request.event['message_id']}".encode()
            ).hexdigest()[:16]
            draft = await self.store.new_draft(
                guild,
                actor,
                int(request.event["message_id"]),
                channel,
                spec,
                self.clock(),
                identifier=draft_id,
            )
            created, observed = await self.builder.mutate(
                request, job, "quick", draft, config, access,
            )
            prior = await self.store.get(guild, created)
        await self.work(guild, prior["id"], job, request=request, access=access, observed=observed)
        current = await self.store.get(guild, prior["id"])
        if current["status"] == "failed":
            raise engine.GiveawayError(
                current.get("error") or "Could not publish. Run the view command again."
            )
        if current["message"]:
            return self.text(
                "send",
                f"Created. https://discord.com/channels/{guild}/{current['channel']}/{current['message']}",
            )
        return self.text("send", "Publication pending. Do not create another giveaway.")

    async def component(self, request, job):
        actor, guild = int(request.event["actor_id"]), int(request.event["guild_id"])
        action = request.route.split(".")[-1]
        identifier = str(request.arguments.get("id") or "")
        if action == "join":
            record, actor_facts = await self.legacy_member(request, job, identifier)
            await self.workflow.entry(request, job, record, actor_facts)
            from .builder import button

            token = await self.session(
                actor,
                guild,
                kind="builder",
                revision=1,
                command=int(request.event["message_id"]),
                channel=int(request.event["channel_id"]),
                page="hub",
                section="Prizes",
                draft=None,
                selected=None,
                giveaway=identifier,
                offset=0,
                selected_slots=[],
            )
            return DiscordIntent(
                "ephemeral",
                session_id=token,
                pages=(PresentationPage(description="Joined.", controls=(button("leave", token + "~1", "Leave"),)),),
            )
        if action == "reroll":
            record, actor_facts = await self.legacy_member(request, job, identifier)
            revision, message = await self.workflow.reroll(request, job, record, actor_facts)
            return await self.confirmation(actor, guild, action, identifier, revision, message)
        token = str(request.arguments.get("session") or "")
        raw = await self.cache.get("lumi:giveaways:session:" + token)
        saved = json.loads(raw) if raw else None
        if (
            not saved
            or saved["owner"] != actor
            or saved["guild"] != guild
            or saved["expires"] <= self.clock()
        ):
            raise engine.GiveawayError("Expired.")
        if action == "dismiss":
            await self.cache.delete("lumi:giveaways:session:" + token)
            return self.text("edit", "Closed.")
        if action in {"open", "channel", "role", "host_role"} and saved["kind"] == "setup":
            config = await self.store.config(guild)
            access = await self.actor_access(job, guild, actor, int(request.event["channel_id"]), ())
            if "administrator" not in access["permissions"]:
                raise engine.GiveawayError("Administrator only.")
            notice = ""
            if action == "channel":
                values = request.arguments.get("values", ())
                if len(values) != 1:
                    raise engine.GiveawayError("Select a channel.")
                if self.workflow.selection_available(access, "channel_ids", values):
                    await self.store.configure(guild, int(values[0]), actor, expected=config["revision"])
                    config = await self.store.config(guild)
                    notice = "Saved."
                else:
                    notice = "Channel unavailable."
            if action in {"role", "host_role"}:
                values = request.arguments.get("values", ())
                if len(values) > 1:
                    raise engine.GiveawayError("Select one role.")
                if self.workflow.selection_available(access, "role_ids", values):
                    await self.store.configure_role(
                        guild, int(values[0]) if values else 0, actor, host=action == "host_role",
                        expected=config["revision"],
                    )
                    config = await self.store.config(guild)
                    notice = "Saved."
                else:
                    notice = "Role unavailable."
            text = "Giveaways\n" + (
                f"<#{config['channel']}>" if config["channel"] else "Select a channel."
            )
            if notice:
                text += "\n" + notice
            role = config.get("required_role", 0)
            host_role = config.get("host_role", 0)
            text += f"\nRequired role <@&{role}>" if role else "\nRequired role: none"
            text += f"\nModerators + <@&{host_role}>" if host_role else "\nModerators only"
            return DiscordIntent(
                "ephemeral" if action == "open" else "edit",
                session_id=token,
                pages=(
                    PresentationPage(
                        description=text,
                        controls=(
                            PresentationControl(
                                "channel_select",
                                "giveaway",
                                "channel",
                                token,
                                placeholder="Giveaway channel",
                                channel_types=(0,),
                                default_ids=(config["channel"],) if config["channel"] else (),
                            ),
                            PresentationControl(
                                "role_select",
                                "giveaway",
                                "role",
                                token,
                                placeholder="Required role · optional",
                                min_values=0,
                                default_ids=(role,) if role else (),
                            ),
                            PresentationControl(
                                "role_select",
                                "giveaway",
                                "host_role",
                                token,
                                placeholder="Additional host role · optional",
                                min_values=0,
                                default_ids=(host_role,) if host_role else (),
                            ),
                        ),
                    ),
                ),
            )
        if action != "confirm" or saved["kind"] != "confirm":
            raise engine.GiveawayError("Unavailable.")
        record, actor_facts = await self.legacy_member(request, job, saved["id"], confirmation=saved)
        await self.workflow.reroll(request, job, record, actor_facts, confirmation=saved)
        await self.work(guild, saved["id"], job, request=request)
        await self.cache.delete("lumi:giveaways:session:" + token)
        current = await self.store.get(guild, saved["id"])
        return self.text(
            "edit",
            current.get("error")
            or ("Winner updated." if current["status"] == "ended" else "Drawing…"),
        )

    async def legacy_member(self, request, job, identifier, *, confirmation=None):
        if not engine.ID.fullmatch(identifier):
            raise engine.GiveawayError("Unavailable.")
        record = await self.store.get(int(request.event["guild_id"]), identifier)
        self.workflow.require_legacy_post(request, record, confirmation=confirmation)
        result = await self.request_discord(
            job_id=job, kind=CHANNEL_MEMBERS, guild_id=record["guild"], channel_id=record["channel"],
            member_ids=(int(request.event["actor_id"]),), idempotency_key=f"gw-member:{job}",
        )
        if not result.ok:
            raise engine.GiveawayError("Unavailable.")
        return record, result.data

    async def confirmation(self, actor, guild, operation, identifier, revision, message):
        token = await self.session(
            actor,
            guild,
            kind="confirm",
            operation=operation,
            id=identifier,
            revision=revision,
            message=message,
        )
        return DiscordIntent(
            "ephemeral",
            session_id=token,
            pages=(
                PresentationPage(
                    description="Reroll?",
                    controls=(
                        PresentationControl(
                            "button", "giveaway", "confirm", token, label="Confirm", style="danger"
                        ),
                        PresentationControl("button", "giveaway", "dismiss", token, label="Back"),
                    ),
                ),
            ),
        )

    async def work(self, guild, identifier, job, *, request=None, access=None, observed=None):
        if not engine.ID.fullmatch(identifier):
            raise engine.GiveawayError("Unavailable.")
        gate = getattr(self, "handover_gate", None)
        if gate and gate.critical(guild):
            return
        lock = self.locks.setdefault((guild, identifier), asyncio.Lock())
        async with lock:
            record = await self.store.get(guild, identifier)
            if (
                not record
                or record["status"] == "failed"
                or (record["status"] == "cancelled" and not record.get("cancel_pending"))
            ):
                return
            if record["status"] == "publishing" or (record.get("cancel_pending") and not record["message"]):
                await self.publish(record, job, observed=observed)
                record = await self.store.get(guild, identifier)
                if not record or not record["message"]:
                    return
            if record.get("pending_edit") or record.get("cancel_pending"):
                if not await self.apply_edit(record, job, observed=observed):
                    return
                record = await self.store.get(guild, identifier)
                if not record or record["status"] == "cancelled":
                    return
            async with self.store.locked(guild, identifier) as current:
                engine.begin_draw(current, self.clock())
            for _ in range(10):
                record = await self.store.get(guild, identifier)
                if record["status"] != "drawing":
                    break
                if not record["draw"]["order"]:
                    async with self.store.locked(guild, identifier) as current:
                        if current["status"] == "drawing":
                            engine.finish_empty(current, self.clock())
                    break
                cursor = record["draw"]["cursor"]
                users = tuple(record["draw"]["order"][cursor : cursor + DRAW_BATCH_SIZE])
                grant = None
                if request is not None and request.event.get("channel_id") != record["channel"]:
                    if access is None and request.event.get("dispatch") == "command":
                        access = await self.actor_access(
                            job, guild, request.event["actor_id"], request.event["channel_id"],
                            (record["channel"],),
                        )
                    grant = (access or {}).get("member_read_grants", {}).get(str(record["channel"]))
                    if not grant:
                        break
                result = await self.request_discord(
                    job_id=job,
                    kind=CHANNEL_MEMBERS,
                    guild_id=guild,
                    channel_id=record["channel"],
                    member_ids=users,
                    read_proof=grant,
                    idempotency_key=(
                        f"gw-draw-facts:{job}:{record['revision']}:{cursor}"
                    ),
                    wait_seconds=30,
                )
                if not result.ok or not await self.workflow.draw(
                    record, job, cursor, result.data,
                ):
                    break
            record = await self.store.get(guild, identifier)
            if not record or not record["message"] or record["status"] in {"cancelled", "failed"}:
                return
            await self.render(record, job)
            record = await self.store.get(guild, identifier)
            key = f"result:{record['round']}"
            if record["status"] in {"cancelled", "failed"}:
                return
            if (
                record["status"] == "ended"
                and record["deliveries"].get(key, {}).get("state") != "sent"
                and key in record["deliveries"]
            ):
                await self.complete(record, job)
                await self.render(await self.store.get(guild, identifier), job)
            async with self.store.locked(guild, identifier) as current:
                pending = current["rendered"] != engine.signature(current) or any(
                    d["state"] in {"pending", "sending", "ambiguous"}
                    for d in current["deliveries"].values()
                )
                current["next_ms"] = (
                    self.clock() + 30_000
                    if pending or current["status"] in {"publishing", "drawing"}
                    else current["ends"]
                    if current["status"] == "active"
                    else 9_000_000_000_000_000
                )

    async def delivery_receipt(self, attempt):
        raw = await self.queue.get(f"lumi:edge-action-receipt:{engine.MODULE}:{attempt}")
        try:
            return decode(raw, DiscordActionResult) if raw else None
        except ValueError:
            return None

    async def observe_sources(self, record, job, purpose, *, config=None):
        edit = record.get("pending_edit") if purpose == "edit" else None
        query = source_query(record, purpose)
        result = await self.request_discord(
            job_id=job, kind=SOURCE_FACTS, guild_id=record["guild"], channel_id=record["channel"],
            message_id=record["message"] or None, embed_sources=query,
            idempotency_key=f"gw-source:{record['id']}:{purpose}:{job}", wait_seconds=30,
        )
        if not result.ok:
            message = {
                "source_unavailable": "Card view unavailable.",
                "viewer_access_unavailable": "Host unavailable.",
                "destination_access_unavailable": "Giveaway channel unavailable.",
            }.get(result.error_code)
            if message:
                raise engine.GiveawayError(message)
            return None
        facts = result.data
        self.workflow.require_facts(
            facts, job=job, guild=record["guild"], channel=record["channel"],
            viewer=query.viewer_id, binding=query.binding,
        )
        actor = facts.get("actor")
        self.workflow.require_facts(actor, user=query.actor_id)
        permissions = set(actor["permissions"])
        if edit and actor["user"] != record["host"] and "administrator" not in permissions:
            raise engine.GiveawayError("Host or administrator only.")
        if not HOST_PERMISSIONS & permissions:
            if edit or v2.is_v2(record):
                if config is None:
                    config = await self.store.config(record["guild"])
                host_role = config.get("host_role", 0)
            else:
                host_role = record.get("host_role", 0)
            if not host_role:
                raise engine.GiveawayError("Hosting is limited to moderators until a host role is set.")
            if host_role not in actor["roles"]:
                raise engine.GiveawayError("Moderator permission or the host role required.")
        if facts.get("available_roles") != list(query.role_ids):
            raise engine.GiveawayError("A selected role is unavailable.")
        views = facts.get("views")
        if purpose == "edit":
            verify_views(
                views, record["guild"], record["host"], record["pending_edit"]["spec"],
                self.clock(), previous=record["spec"],
            )
        elif v2.is_v2(record):
            verify_views(views, record["guild"], record["host"], record["spec"], self.clock())
        else:
            if not isinstance(views, (list, tuple)) or len(views) != 1:
                raise engine.GiveawayError("Card view unavailable.")
            view = observed_view(
                views[0], record["guild"], record["source_channel"], self.clock(),
                message=record["card"]["message"],
            )
            if view["owner"] != record["host"] or engine.source_fingerprint(view) != record["source_hash"]:
                raise engine.GiveawayError("Card details changed. Run the view command again.")
        proof, valid_until = facts.get("read_proof"), facts.get("read_until_ms")
        if not isinstance(proof, str) or not proof or type(valid_until) is not int or valid_until <= self.clock():
            return None
        if views:
            valid_until = min(valid_until, *(view["created"] + engine.FRESH_MS for view in views))
        return SourceObservation(
            query, job, record["guild"], record["channel"], record["message"], proof, valid_until,
        )

    async def checked_sources(self, record, job, purpose, *, observed=None):
        edit = record.get("pending_edit") if purpose == "edit" else None
        if purpose == "edit" and (
            not edit or not record["message"] or record["status"] != "active"
            or record["revision"] != edit["base"]
        ):
            return None
        if purpose == "publish" and (record["status"] != "publishing" or record["message"]):
            return None
        if (
            observed is not None and observed.valid_until > self.clock()
            and (observed.job, observed.guild, observed.channel, observed.message)
            == (job, record["guild"], record["channel"], record["message"])
            and observed.query == source_query(record, purpose)
        ):
            return observed.proof
        try:
            latest = await self.observe_sources(record, job, purpose)
        except engine.GiveawayError as exc:
            if purpose == "edit":
                await self.workflow.reject_edit(record, str(exc))
            else:
                await self.workflow.reject_publication(record, str(exc))
            return None
        return latest.proof if latest is not None else None

    async def apply_edit(self, record, job, *, observed=None):
        intent = read_proof = query = None
        assets = ()
        if record.get("cancel_pending"):
            reason = "cancel"
        else:
            reason = pending_edit_digest(record)
            try:
                flight = record.get("edit_in_flight", {})
                for prior in dict.fromkeys((flight.get("latest"), flight.get("first"))):
                    if not prior or (known := await self.delivery_receipt(prior)) is None:
                        continue
                    known = await self.apply_discord_receipt(
                        known, kind="giveaway_apply", guild_id=record["guild"],
                        giveaway_id=record["id"], idempotency_key=prior,
                    )
                    if known.error_code == "message_not_found" and known.data.get("deleted_post"):
                        return False
                    if await self.reconcile_mutation(record, prior, known) == "missing":
                        return False
                    if await self.workflow.settle_edit(record, prior, known):
                        return True
                    await self.workflow.release_unstarted_edit(record, prior, known)
                read_proof = await self.checked_sources(record, job, "edit", observed=observed)
            except (ActionUnavailable, TimeoutError, RedisError, OSError):
                return False
            if not read_proof:
                return False
            query = source_query(record, "edit")
            shown = self.workflow.edited_record(record)
            assets = tuple([
                await self.assets.attachment(image, v2.file_name(shown, image))
                for _, image, _ in v2.image_items(shown["spec"])
            ])
            intent = msgspec.structs.replace(v2.presentation(shown), kind="edit", attachments=assets)
        receipt = f"gw-apply:{record['id']}:{record['revision']}:{reason}:{self.clock() // 30000}"
        operation = "delete" if reason == "cancel" else "edit"
        mutation = self.post_mutation(record, operation)

        async def stage():
            return await self.stage_effect(
                record, job, MUTATE_MESSAGE, receipt, payload_digest(intent, ()),
                message_id=record["message"], query=query, mutation=mutation,
            )

        try:
            staged = (
                await self.workflow.stage_post_effect(record, stage)
                if operation == "delete"
                else await self.workflow.begin_edit_effect(record, receipt, stage=stage)
            )
            if not staged:
                if assets:
                    await self.assets.discard(assets)
                return False
            result = await self.request_discord(
                job_id=job, kind=MUTATE_MESSAGE, guild_id=record["guild"],
                channel_id=record["channel"], message_id=record["message"],
                intent=intent, idempotency_key=receipt, wait_seconds=EFFECT_GRANT_SECONDS,
                read_proof=read_proof, embed_sources=query, message_mutation=mutation,
            )
        except (ActionUnavailable, TimeoutError, RedisError, OSError):
            return False
        disposition = await self.reconcile_mutation(
            record, receipt, result, legacy_kind="giveaway_apply", deleting=operation == "delete",
        )
        if operation == "delete" or disposition == "missing":
            return disposition == "deleted"
        return await self.workflow.settle_edit(record, receipt, result)

    async def publish(self, record, job, *, observed=None):
        delivery = record["deliveries"]["publish"]
        intent = read_proof = None
        try:
            if delivery["state"] == "pending" and record["status"] == "publishing":
                if record["next_ms"] > self.clock():
                    return
                read_proof = await self.checked_sources(record, job, "publish", observed=observed)
                if not read_proof:
                    return
                images = (
                    [(image, v2.file_name(record, image)) for _, image, _ in v2.image_items(record["spec"])]
                    if v2.is_v2(record) else [(record["image"], engine.filename(record))]
                )
                attachments = [await self.assets.attachment(image, name) for image, name in images]
                shown = dict(record, ends=self.clock() + record["duration"])
                if v2.is_v2(record):
                    shown["status"] = "active"
                intent = msgspec.structs.replace(engine.presentation(shown), attachments=tuple(attachments))
                query = source_query(record, "publish")

                async def stage(attempt, digest):
                    return await self.stage_effect(
                        record, job, PUBLISH_MESSAGE, attempt, digest, query=query,
                    )

                claimed = await self.workflow.claim_publication(
                    record, intent, rendered_signature=engine.signature(record),
                    job=job, stage=stage,
                )
                if claimed is None:


                    await self.assets.discard(attachments)
                    return
                record = claimed
                key = publication_binding(record)["attempt"]
            elif delivery["state"] in {"sending", "ambiguous"}:
                attempt = delivery.get("attempt")
                if attempt and (known := await self.delivery_receipt(attempt)) is not None:
                    if await self.workflow.settle_publication(record, known, durable=True) not in {False, "ambiguous"}:
                        return
                result = await recover_history(self.actions.request, record, job)
                if result is not None:
                    await self.workflow.settle_publication(record, result)
                return
            elif record["status"] == "cancelled":
                await self.workflow.finish_unpublished_cancel(record)
                return
            else:
                return
            result = await self.request_discord(
                job_id=job, kind=PUBLISH_MESSAGE, guild_id=record["guild"], channel_id=record["channel"],
                intent=intent, idempotency_key=key, wait_seconds=EFFECT_GRANT_SECONDS,
                read_proof=read_proof, embed_sources=query,
            )
        except (ActionUnavailable, TimeoutError, RedisError, OSError):
            return
        if result is not None:
            await self.workflow.settle_publication(record, result)

    def post_mutation(self, record, operation, *, preserve=False):
        return MessageMutation(operation, preserve, f"lumi:giveaway:join:{record['id']}")

    async def observe_missing_output(self, record):
        await self.workflow.observe_deleted(
            record["guild"], record["channel"], record["message"], identifier=record["id"],
        )

    async def reconcile_mutation(self, record, receipt, result, *, legacy_kind=None, deleting=False):
        disposition = self.workflow.mutation_outcome(record, receipt, result)
        legacy = result.data.get("deleted_post")
        if disposition is None and legacy_kind and legacy is not None:
            if (
                (result.ok and deleting)
                or (not result.ok and result.error_code == "message_not_found")
            ):
                self.workflow.require_facts(
                    legacy, operation=legacy_kind, receipt=receipt, guild=record["guild"],
                    record=record["id"], channel=record["channel"], message=record["message"],
                )
                disposition = "deleted" if result.ok else "missing"
        if disposition in {"deleted", "missing"}:
            await self.observe_missing_output(record)
        return disposition

    @staticmethod
    def mutation_lane(record):
        return mutation_lane("cards", engine.MODULE, record["guild"], record["channel"], record["message"])

    async def stage_effect(self, record, job, kind, attempt, digest, *, message_id=None, query=None, mutation=None):
        key = effect_key("cards", job, attempt)
        grant = DiscordEffectGrant(
            engine.MODULE, self.generation, kind, record["guild"], record["channel"],
            message_id, digest, self.clock() + EFFECT_GRANT_SECONDS * 1000,
            source_digest(query), source_digest(mutation),
        )
        async with asyncio.timeout(EFFECT_GRANT_SECONDS):
            if mutation is not None:
                return await stage_mutation(self.queue, key, self.mutation_lane(record), encode(grant))
            return await self.queue.set(key, encode(grant), nx=True, ex=EFFECT_GRANT_SECONDS)

    async def revoke_publication(self, job, attempt):
        async with asyncio.timeout(EFFECT_GRANT_SECONDS):
            return bool(await self.queue.delete(effect_key("cards", job, attempt)))

    async def revoke_mutation(self, record):
        async with asyncio.timeout(EFFECT_GRANT_SECONDS):
            await self.queue.delete(self.mutation_lane(record))

    async def stage_completion(self, record, job):
        delivery = record["deliveries"][f"result:{record['round']}"]
        return await self.stage_effect(
            record, job, REPLY_TEXT, delivery["attempt"], delivery["digest"], message_id=record["message"],
        )

    async def complete(self, record, job):
        round_number = record["round"]
        delivery = record["deliveries"].get(f"result:{round_number}", {})
        if delivery.get("state") == "pending":
            claimed = await self.workflow.claim_completion(record)
            if claimed is None:
                return
            record, intent, recipients = claimed
            delivery = record["deliveries"][f"result:{round_number}"]
            receipt = delivery["attempt"]
        elif delivery.get("state") in {"sending", "ambiguous"}:
            attempt = delivery.get("attempt")
            if attempt:
                known = await self.delivery_receipt(attempt)
                if known is not None:
                    known = await self.apply_discord_receipt(
                        known, kind="giveaway_complete", guild_id=record["guild"],
                        giveaway_id=record["id"], idempotency_key=attempt,
                    )
                    deletion = known.data.get("deleted_post")
                    if deletion and (deletion.get("channel"), deletion.get("message")) == (record["channel"], record["message"]):
                        return
                    if await self.workflow.settle_completion(record, known, durable=True) in {"sent", "pending", "deleted"}:
                        return


            intent, _ = self.workflow.completion_payload(record)
            try:
                result = await recover_history(self.actions.request, record, job, reply=intent.content)
            except (ActionUnavailable, TimeoutError, RedisError, OSError):
                return
            if result is not None:
                await self.workflow.settle_completion(record, result)
            return
        else:
            return
        try:
            if not await self.stage_completion(record, job):
                return
            result = await self.request_discord(
                job_id=job, kind=REPLY_TEXT, guild_id=record["guild"], channel_id=record["channel"],
                message_id=record["message"], intent=intent, member_ids=recipients,
                idempotency_key=receipt, wait_seconds=EFFECT_GRANT_SECONDS,
            )
        except (ActionUnavailable, TimeoutError, RedisError, OSError):
            return
        if result is not None:
            await self.workflow.settle_completion(record, result)

    async def render(self, record, job):
        if not record or not record["message"] or record["status"] in {"cancelled", "failed"} or record.get("pending_edit"):
            return
        signature = engine.signature(record)
        if record["rendered"] == signature:
            return
        digest = revision_digest(signature)
        receipt = f"gw-ui:{record['id']}:{digest}:{self.clock() // 30000}"
        intent = msgspec.structs.replace(engine.presentation(record), kind="edit")
        mutation = self.post_mutation(record, "edit", preserve=True)

        async def stage():
            return await self.stage_effect(
                record, job, MUTATE_MESSAGE, receipt, payload_digest(intent, ()),
                message_id=record["message"], mutation=mutation,
            )

        if not await self.workflow.stage_post_effect(record, stage, signature=signature):
            return
        result = await self.request_discord(
            job_id=job,
            kind=MUTATE_MESSAGE,
            guild_id=record["guild"],
            channel_id=record["channel"],
            message_id=record["message"],
            intent=intent,
            message_mutation=mutation,
            idempotency_key=receipt,
            wait_seconds=30,
        )
        disposition = await self.reconcile_mutation(
            record, receipt, result, legacy_kind="giveaway_render",
        )
        if disposition == "edited":
            await self.workflow.observe_rendered(
                record["guild"], record["id"], receipt, dict(
                    guild=record["guild"], record=record["id"], receipt=receipt,
                    channel=record["channel"], message=record["message"], signature=signature,
                ),
            )
        elif result.ok and result.data.get("rendered_post") is not None:
            await self.workflow.observe_rendered(
                record["guild"], record["id"], receipt, result.data["rendered_post"],
            )
