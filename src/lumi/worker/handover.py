from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import secrets
import time

import msgspec

from lumi.contracts import DiscordIntent, encode
from lumi.moderation.handover_blockers import inspect_group
from lumi.moderation.handover_status import CHECK_PHASES, UNAVAILABLE
from lumi.moderation.handover_store import CRITICAL, DEFAULT_TRANSITION, TERMINAL, blocked
from lumi.shared.attachment_payloads import LIFETIME_MS, transition_scope
from lumi.transport import enabled
from lumi.ui_keys import session_keys, session_tokens

from .attachment_delivery import ACTIVE_DELIVERY

LOGGER = logging.getLogger(__name__)
SESSIONS = {
    "platform": ("lumi:settings:session:*",),
    "security": (
        "lumi:verification:session:*", "lumi:imageguard:ui:*", "lumi:modlog:session:*",
        "lumi:moderation:role:*", "lumi:ui-role-target:security:*",
    ),
    "community": (
        "lumi:reaction_roles:session:*",
        "lumi:halloween:session:*",
        "lumi:fun:session:*",
        "lumi:personal-ui:notes:*",
        "lumi:personal-ui:reminders:*",
    ),
    "cards": (
        "lumi:karuta:session:*",
        "lumi:sofi:session:*",
        "lumi:glitch:session:*",
        "lumi:pogdrop:session:*",
        "lumi:giveaways:session:*",
    ),
}


def plan(module, kind, reference, position, intent, *, sources=(), revision=0, **extra):
    return dict(
        key=f"{kind}:{reference}:{position}",
        module=module,
        kind=kind,
        reference=reference,
        position=position,
        revision=revision,
        intent=msgspec.to_builtins(intent),
        sources=list(sources),
        **extra,
    )


async def stage_transferred_panels(uploads, record, plans, now):

    scope = transition_scope(record["guild"], record["id"])
    for item in plans:
        intent = msgspec.convert(item["intent"], type=DiscordIntent)
        await uploads.stage(scope, intent, deadline=now + LIFETIME_MS)


def document_parts(document):


    from lumi.edge.intents import IntentValidator

    validator = IntentValidator()
    files = {item.file_name: item for item in document.attachments}
    pages, attachments, count, size = [], [], 0, 0
    for page in document.pages:
        selected = [files[name] for name in validator._page_file_names(page)]
        components = validator.component_count(page)
        if pages and (
            count + components > 40
            or len(attachments) + len(selected) > 10
            or size + sum(f.size for f in selected) > 25 * 1024 * 1024
        ):
            yield DiscordIntent("send", pages=tuple(pages), attachments=tuple(attachments))
            pages, attachments, count, size = [], [], 0, 0
        pages.append(page)
        attachments.extend(selected)
        count += components
        size += sum(f.size for f in selected)
    if pages:
        yield DiscordIntent("send", pages=tuple(pages), attachments=tuple(attachments))


async def public_plans(modules, record):

    guild, old = record["guild"], record["old"]
    result = []
    for module in modules:
        name = module.manifest.module
        if name == "verification":
            for panel in await module._store.list_verification(guild):
                if panel.channel_id != old or panel.lifecycle not in {"active", "needs_repair"}:
                    continue
                if panel.ownership != "lumi":
                    raise ValueError("Adopt the verification panel before resetting this channel.")
                result.append(
                    plan(
                        name,
                        "verification",
                        panel.panel_id,
                        0,
                        DiscordIntent("send", pages=(module._public_panel(panel),)),
                        sources=(panel.message_id,) if panel.message_id else (),
                        revision=panel.route_revision,
                    )
                )
        elif name == "reaction_roles":
            for panel in await module._store.list_reaction_panels(guild):
                if panel.channel_id != old or panel.lifecycle not in {"active", "needs_repair"}:
                    continue
                parts = await module._store.list_reaction_parts(panel.panel_id)
                if panel.ownership != "lumi" or any(part.ownership != "lumi" for part in parts):
                    raise ValueError("Adopt the reaction panel before resetting this channel.")
                mappings = await module._store.list_mappings(panel.panel_id)
                for position, page in enumerate(module._reaction_public_pages(mappings)):
                    selected = mappings[position * 20 : (position + 1) * 20]
                    result.append(
                        plan(
                            name,
                            "reaction",
                            panel.panel_id,
                            position,
                            DiscordIntent("send", pages=(page,)),
                            sources=tuple(part.message_id for part in parts),
                            revision=panel.route_revision,
                            reactions=[m.emoji_key for m in selected],
                            mappings=[
                                dict(
                                    id=m.mapping_id,
                                    emoji=m.emoji_key,
                                    role=m.role_id,
                                    position=m.position,
                                )
                                for m in selected
                            ],
                        )
                    )
            for panel in await module._store.list_button_panels(guild):
                if panel.channel_id != old or panel.lifecycle not in {"active", "needs_repair"}:
                    continue
                document = await module._button_roles.document(panel)
                for position, intent in enumerate(document_parts(document)):
                    result.append(
                        plan(
                            name,
                            "button",
                            panel.panel_id,
                            position,
                            intent,
                            sources=document.message_ids,
                            revision=panel.desired_revision,
                        )
                    )
        elif name == "giveaways":
            from lumi.giveaways import expanded as v2
            from lumi.giveaways.models import filename, presentation

            for giveaway in await module.store.in_channel(guild, old):
                if (
                    giveaway["status"] not in {"active", "ended"}
                    or not giveaway["message"]
                    or giveaway.get("pending_edit")
                    or giveaway.get("cancel_pending")
                ):
                    raise ValueError("A giveaway needs to settle before reset.")
                if v2.is_v2(giveaway) and giveaway.get("ui_version") == 2:
                    attachments = tuple(
                        [
                            await module.assets.attachment(image, v2.file_name(giveaway, image))
                            for _, image, _ in v2.image_items(giveaway["spec"])
                        ]
                    )
                else:
                    attachments = (
                        await module.assets.attachment(giveaway["image"], filename(giveaway)),
                    )
                intent = msgspec.structs.replace(presentation(giveaway), attachments=attachments)
                result.append(
                    plan(
                        name,
                        "giveaway",
                        giveaway["id"],
                        0,
                        intent,
                        sources=(giveaway["message"],),
                        revision=giveaway["revision"],
                    )
                )
        elif name == "halloween":
            if guild not in await module.store.active_guilds():
                continue
            async with module._locks.setdefault(guild, asyncio.Lock()):
                async with module.store.transaction(guild) as state:
                    snapshot = copy.deepcopy(state)
            if old not in {
                snapshot["config"].get("channel"),
                snapshot["config"].get("setup_channel"),
            }:
                continue
            result.extend(await event_plans(module, record, snapshot))
    if len(result) > 500:
        raise ValueError("Too many saved panels for one reset.")
    return result


async def event_plans(module, record, original):
    guild, old, new = record["guild"], record["old"], record["new"]
    state = copy.deepcopy(original)
    for key in ("channel", "setup_channel"):
        if state["config"].get(key) == old:
            state["config"][key] = new
    result, seen = [], set()
    for collection in (
        "standings_parts",
        "contribution_panels",
        "contribution_portal",
        "prize_prompt",
    ):
        entries = (
            [original.get(collection)]
            if collection in {"prize_prompt", "contribution_portal"}
            else original.get(collection, [])
        )
        for index, entry in enumerate(entries):
            if (
                not entry
                or entry.get("channel") != old
                or entry.get("status") not in {"applied", "pending"}
            ):
                continue
            source = entry.get("message", 0)
            if source and source in seen:
                continue
            if collection in {"contribution_panels", "contribution_portal"}:
                if state["stage"] in {"running", "finished"}:
                    continue
                intent = await module._contribution_intent(state, "send")
                digest = module._contribution_digest(state)
            elif collection == "standings_parts":
                groups = module._standings(state)
                if index >= len(groups):
                    continue
                pages = groups[index]
                intent = DiscordIntent(
                    "send",
                    presentation_mode="legacy",
                    pages=pages,
                    attachments=(await module._prize_attachment(state),)
                    if state.get("main_prize") and state["stage"] != "finished"
                    else (),
                )
                digest = hashlib.sha256(encode(pages)).hexdigest()
            else:
                intent = DiscordIntent(
                    "send",
                    presentation_mode="legacy",
                    pages=(module._prize_prompt_page(state),),
                    attachments=(await module._prize_attachment(state),)
                    if state.get("main_prize")
                    else (),
                )
                digest = module._prize_prompt_hash(state)
            seen.add(source)
            result.append(
                plan(
                    "halloween",
                    "event",
                    state["run"],
                    len(result),
                    intent,
                    sources=(source,) if source else (),
                    revision=original["revision"],
                    collection=collection,
                    index=index,
                    old_message=source,
                    digest=digest,
                )
            )
    if (
        state["stage"] == "setup"
        and original["config"].get("setup_channel") == old
        and state.get("organizer_id")
    ):
        session = dict(
            id=secrets.token_hex(8),
            owner=state["organizer_id"],
            guild=guild,
            channel=new,
            expires=module.clock() + 900000,
            screen="overview",
            page=0,
            selected="new",
            presentation_mode=None,
            run=state["run"],
            revision=original["revision"],
        )
        intent = await module._dashboard(session, state, True, "send")
        await module.cache.set(
            "lumi:halloween:session:" + session["id"], json.dumps(session), px=900000
        )
        result.append(
            plan(
                "halloween",
                "event_setup",
                state["run"],
                0,
                intent,
                revision=original["revision"],
                owner=state["organizer_id"],
                session=session,
            )
        )
    return result


class HandoverParticipant:
    def __init__(self, *, store, owner, group, generation, modules, cache, clock=None):
        self.store, self.owner, self.group, self.generation = store, owner, group, generation
        self.modules, self.cache = tuple(modules), cache
        self.transitions = {
            manifest.module: getattr(manifest, "channel_transition", None)
            for manifest in (module.manifest for module in self.modules)
        }
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.runtime, self.task, self.gates = None, None, []
        self.loaded, self.stopped, self.expired, self.released = False, False, set(), set()
        self.changed = asyncio.Event()
        for module in self.modules:
            module.handover_gate = self

    def critical(self, guild):
        return not self.loaded or any(
            g["guild"] == guild and g["phase"] in CRITICAL for g in self.gates
        )

    def busy(self, guild):
        return not self.loaded or any(
            g["guild"] == guild and g["phase"] not in TERMINAL for g in self.gates
        )

    def blocked(self, job):
        if not self.loaded:
            return True
        return blocked(tuple(gate for gate in self.gates if not self.reset_job(job, gate)),
                       job, spec=self.transition(job.module))

    def reset_job(self, job, gate):
        from lumi.moderation.reset_runner import ROUTE

        return (
            self.group == job.group == "security" and job.module == "moderation"
            and job.request.route == job.request.handler == ROUTE and job.request.event.get("dispatch") == "scheduled"
            and (gate["guild"], gate["old"], gate["id"]) == (
                job.guild_id, job.channel_id, job.request.arguments.get("batch"),
            )
        )

    def transition(self, module):
        return self.transitions.get(module) or DEFAULT_TRANSITION

    async def wait(self, job):
        try:
            await asyncio.wait_for(self.changed.wait(), timeout=1)
        except TimeoutError:
            pass

    def quiet(self, gate):
        if self.runtime:
            for job in self.runtime._handover_jobs.values():
                if job.guild_id == gate["guild"] and not self.reset_job(job, gate) and (
                    self.transition(job.module).quiesce_guild
                    or job.channel_id in {gate["old"], gate.get("new")}
                ):
                    return False
        for module in self.modules:
            if module.manifest.module == "giveaways" and not module.drain_ready():
                return False
            if module.manifest.module == "halloween":
                lock = module._locks.get(gate["guild"])
                if lock and lock.locked():
                    return False
            if module.manifest.module == "reminders" and module.ticking:
                return False
        return True

    async def start(self):
        self.gates = await self.store.index()
        self.loaded = True
        await self.store.heartbeat(self.group, self.generation, self.clock())
        self.task = asyncio.create_task(self.run(), name="channel-handover")

    async def close(self):
        self.stopped = True
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.owner.close()

    async def run(self):
        while not self.stopped:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("handover_participant_unavailable")
            await asyncio.sleep(1)

    async def expire_sessions(self, guild):
        tokens = await session_tokens(self.cache, self.group) if enabled() else None
        for pattern in (*SESSIONS[self.group], f"lumi:ui-response-revision:{self.group}:*"):
            async for key in session_keys(self.cache, self.group, pattern, tokens=tokens):
                raw = await self.cache.get(key)
                try:
                    value = json.loads(raw) if raw else None
                except (ValueError, UnicodeError):
                    continue
                if (
                    isinstance(value, dict)
                    and (value.get("guild_id") or value.get("guild")) == guild
                ):
                    await self.cache.delete(key)

    async def tick(self):
        gates = await self.store.index()
        if gates != self.gates:
            self.gates = gates
            previous, self.changed = self.changed, asyncio.Event()
            previous.set()
        await self.store.heartbeat(self.group, self.generation, self.clock())
        for gate in self.gates:
            if gate["phase"] in TERMINAL:
                if gate["id"] not in self.released:
                    await self.publish(gate)
                    self.released.add(gate["id"])
                continue
            checking = gate["phase"] in CHECK_PHASES
            if not checking and not self.quiet(gate):
                continue
            record = await self.store.get(gate["id"])
            if record is None or record["phase"] != gate["phase"]:
                continue
            epoch = record.get("epoch", 0)
            ack = record.get("acks", {}).get(self.group)
            if not checking and ack and (ack["phase"], ack["generation"], ack.get("epoch", 0)) == (
                gate["phase"],
                self.generation,
                epoch,
            ):
                continue
            plans = None
            facts = {}
            if checking:
                try:
                    blocker = await inspect_group(self.owner, self.group, gate["guild"], gate["old"])
                except Exception:
                    LOGGER.warning("handover_preflight_unavailable")
                    blocker = UNAVAILABLE


                facts = dict(blocker=blocker, idle=gate["phase"] == "quiesced" and self.quiet(gate))
            observed_at = self.clock()
            if gate["phase"] == "quiesced" and facts["idle"] and gate["id"] not in self.expired:
                await self.expire_sessions(gate["guild"])
                self.expired.add(gate["id"])
            if gate["phase"] == "building":
                try:
                    plans = await public_plans(self.modules, record)
                    uploads = ACTIVE_DELIVERY.get()
                    if uploads is not None:
                        await stage_transferred_panels(uploads, record, plans, observed_at)
                except Exception:
                    await self.store.mutate(
                        gate["id"],
                        lambda r: r.setdefault("errors", {}).update(
                            {self.group: "Saved panels could not be prepared."}
                        ),
                    )
                    continue
            if gate["phase"] == "publishing":
                await self.publish(gate)

            def acknowledge(row, gate=gate, epoch=epoch, plans=plans, facts=facts, at=observed_at):
                if row["phase"] != gate["phase"] or row.get("epoch", 0) != epoch:
                    return
                row.setdefault("acks", {})[self.group] = dict(
                    phase=gate["phase"], generation=self.generation, epoch=epoch, at=at, **facts,
                )
                if plans is not None:
                    row.setdefault("plans", {})[self.group] = plans

            await self.store.mutate(gate["id"], acknowledge)

    async def publish(self, gate):
        for module in self.modules:
            if module.manifest.module == "reminders":
                await module.publish_pending({gate["guild"]})
            publisher = getattr(module, "publisher", None) or getattr(module, "_publisher", None)
            if publisher:
                flush = getattr(publisher, "flush", None) or getattr(
                    publisher, "flush_safely", None
                )
                if flush:
                    await flush()
            if module.manifest.module == "halloween":
                if gate["guild"] not in await module.store.active_guilds():
                    continue
                async with module._locks.setdefault(gate["guild"], asyncio.Lock()):
                    async with module.store.transaction(gate["guild"]) as state:
                        snapshot = copy.deepcopy(state)
                    await module._sync(gate["guild"], snapshot, handover=True)
