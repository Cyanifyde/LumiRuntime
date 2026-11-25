from __future__ import annotations

import asyncio
import base64
import copy
import csv
import hashlib
import io
import json
import logging
import secrets
import time
from pathlib import Path

import msgspec

from lumi.contracts import (
    ChannelTransitionSpec,
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordActionResult,
    DiscordIntent,
    FieldExtraction,
    HelpSpec,
    Manifest,
    MatcherExpr,
    ModalChoice,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    PrivateFeedbackSpec,
    ResultEnvelope,
    ScheduledCallbackSpec,
    TemporaryAttachment,
    TypedRequest,
    decode,
    encode,
)
from lumi.contracts.activity_observations import admission_key
from lumi.shared.access_receipts import admitted_request
from lumi.shared.receipts import ComponentReceipts
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.activity_inbox import ActivityInbox
from lumi.worker.scheduler import ScheduledCallbackClient

from . import engine
from .activity import ActivityAggregator, ActivityRules
from .feedback import TEXT as CLAIM_FEEDBACK_TEXT
from .feedback import project_feedback
from .pool_view import EventPoolViewer
from .prize import PrizeLoader
from .recovery import observe_publication, snowflake

LOGGER = logging.getLogger(__name__)
MODULE = "halloween"
SESSION = r"(?P<session>[a-f0-9]{16})~(?P<arg>[A-Za-z0-9_-]{1,32})"
KINDS = ("tickets", "gems", "cards", "frames", "other")


def _claim_receipt(raw: bytes | str, drop: str) -> dict | None:

    try:
        receipt = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(receipt, dict):
        return None
    if "payload" in receipt:
        payload = receipt["payload"]
        if not isinstance(payload, str):
            return None
        scope, separator, slot = payload.partition("~")
        if not separator or scope != drop or slot not in {"0", "1", "2", "3"}:
            return None
        receipt["slot"] = slot
    return receipt


EVENT_ABOUT = (
    "Random drops appear while people chat after the game starts. After a short warning, "
    "four hidden buttons appear: two treats, one spider, and a prize or blank, "
    "shuffled each time.\n\n"
    "Choose once per drop. Treats add 1 point; spiders remove 1 point, without going below zero. "
    "Prizes can include tickets, gems, frames, cards and other items. Blanks give nothing.\n\n"
    "Each button goes to its first eligible claimer. Drops close after 30 seconds, "
    "or when all four buttons are claimed. Results stay hidden until the drop closes.\n\n"
    "The highest score wins the main event prize."
)
CONTRIBUTIONS_ABOUT = (
    "Submit your card codes using Add Cards below, then gift them to the organiser "
    "when they ask for them. "
    "You can add or remove submitted codes until contributions close. "
    "Keep your contributions secret so others don't know whether you're ahead.\n\n"
    "Contributors are ranked by the total worth of their gifted cards. "
    "Contributions on another person's wishlist are worth 2x their value.\n\n"
    "First place receives half the starting-point pool, then each following place "
    "receives half of what remains.\n\n"
    "For example, with 800 points: first gets 400, second gets 200, and third gets 100."
)


def event_about(state: dict) -> str:
    profile = state["config"].get("point_scaling")
    if profile not in (engine.LEGACY_POINT_SCALING, engine.POINT_SCALING):
        return EVENT_ABOUT
    text = EVENT_ABOUT.replace(
        "Treats add 1 point; spiders remove 1 point, without going below zero.",
        "Both treats add the same amount; the spider removes that amount. "
        "Scores never go below zero. "
        "The usual amount grows from 1 to 20, with bigger random swings near the end.",
    )
    if profile == engine.POINT_SCALING:
        text = text.replace(
            "The usual amount grows from 1 to 20, with bigger random swings near the end.",
            "The usual amount grows from 1 to 20, and the random range widens. "
            "Both reach their maximum about five days before the event ends "
            "and stay there until the finish.",
        )
    return text


def safe(text: str) -> str:
    return "".join("\\" + char if char in "\\*_`~|<>" else char for char in text).replace(
        "@", "@\u200b"
    )


def select_field(key: str, label: str, values: tuple[str, ...]) -> ModalFieldSpec:
    return ModalFieldSpec(
        key,
        label,
        kind="string_select",
        choices=tuple(ModalChoice(value.title(), value) for value in values),
    )


def modal(action: str) -> ModalSpec:
    if action == "tally":
        return ModalSpec(
            "Participant tally",
            (
                ModalFieldSpec("person", "Person", kind="user_select", minimum=1, maximum=1),
                ModalFieldSpec("worth", "Total worth", minimum=1, maximum=12),
            ),
        )
    if action in {"add_cards", "remove_cards"}:
        return ModalSpec(
            "Card contributions",
            (
                ModalFieldSpec(
                    "codes",
                    "Card codes",
                    style="paragraph",
                    minimum=1,
                    placeholder="abc123, def456",
                ),
            ),
        )
    if action == "add_stock":
        return ModalSpec(
            "Add prize stock",
            (
                ModalFieldSpec("amount", "Extra amount (not cards or frames)", required=False, maximum=7),
                ModalFieldSpec(
                    "codes",
                    "Extra card codes / frame names",
                    required=False,
                    style="paragraph",
                    maximum=4000,
                ),
            ),
        )
    if action == "config":
        return ModalSpec(
            "Halloween setup",
            (
                ModalFieldSpec("points", "Starting points pool", minimum=1, maximum=12),
                ModalFieldSpec(
                    "drop_time", "Drop stage duration (e.g. 30d)", minimum=2, maximum=32
                ),
                ModalFieldSpec(
                    "contribution_time",
                    "Contribution time limit (e.g. 30m, 2h, 3d)",
                    minimum=2,
                    maximum=32,
                ),
                ModalFieldSpec(
                    "instructions",
                    "Gift instructions",
                    required=False,
                    style="paragraph",
                    maximum=1000,
                ),
            ),
        )
    if action == "ranking":
        return ModalSpec(
            "Ranked participants · highest first",
            (
                select_field("operation", "Change", ("replace", "append", "remove")),
                ModalFieldSpec(
                    "people",
                    "Discord user IDs or mentions",
                    style="paragraph",
                    minimum=1,
                    placeholder="<@-7>, <@-8>",
                ),
            ),
        )
    return ModalSpec(
        "Prize pool",
        (
            select_field("type", "Prize type", KINDS),
            ModalFieldSpec(
                "name",
                "Item name / card codes / frame names",
                style="paragraph",
                required=False,
                maximum=4000,
            ),
            ModalFieldSpec(
                "stock", "Stock (cards/frames follow entries)", minimum=1, maximum=7
            ),
            ModalFieldSpec("cap", "Number per drop (one for cards or frames)", minimum=1, maximum=7),
            ModalFieldSpec("weight", "Draw weight (legacy events only)", required=False, maximum=4),
        ),
    )


class HalloweenModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis,
        cache_redis,
        store,
        temp_root: Path,
        actions=None,
        clock=None,
        rng=None,
        owner_id: int = 0,
    ) -> None:
        self.generation = generation
        self.owner_id = owner_id
        self.queue, self.cache, self.store, self.temp_root = (
            queue_redis,
            cache_redis,
            store,
            temp_root,
        )
        self.actions = actions or EdgeActionClient(
            queue_redis, module=MODULE, group="community", generation=generation
        )
        self.scheduler = ScheduledCallbackClient(
            queue_redis, module=MODULE, group="community", generation=generation
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.rng = rng or secrets.SystemRandom()
        self._locks: dict[int, asyncio.Lock] = {}
        self._task = None
        self._draining = False
        self._activity_task = None
        self.activity_rules = ActivityRules()
        self.activity = ActivityAggregator(queue_redis)
        self.activity_inbox = ActivityInbox(
            queue_redis, "community", MODULE, generation, self.clock,
            ready=lambda: not self._draining, observe=self._observe_activity,
        )
        self.prize_loader = PrizeLoader()
        self.pool_viewer = EventPoolViewer(
            generation=generation, cache=cache_redis, store=store, clock=self.clock
        )

    @property
    def manifest(self) -> Manifest:
        components = []
        admin = {
            "config",
            "channel",
            "pools",
            "ranking",
            "tally",
            "review",
            "handoffs",
            "confirm",
            "start",
            "stop",
            "finish",
            "new",
            "new_confirm",
            "pool",
            "add_stock",
            "pick_pool",
            "pool_remove",
            "delivered",
            "pick_handoff",
            "export",
            "repair",
            "main_prize",
            "wizard_next",
            "wizard_back",
        }
        for action in sorted(
            admin | {"overview", "leaderboard", "previous", "next", "add_cards", "remove_cards"}
        ):
            form = action in {
                "config",
                "ranking",
                "tally",
                "pool",
                "add_stock",
                "add_cards",
                "remove_cards",
            }
            input_kind = (
                "modal"
                if form
                else (
                    "channel_ids"
                    if action == "channel"
                    else "string_values"
                    if action in {"pick_pool", "pick_handoff"}
                    else "none"
                )
            )
            components.append(
                ComponentSpec(
                    "halloween",
                    action,
                    "halloween.dashboard",
                    rf"^{SESSION}$",
                    captures=(CaptureSpec("session"), CaptureSpec("arg")),
                    deadline_ms=120_000,
                    public=True,
                    acl_key="events",
                    session_bound=True,
                    required_permissions=("administrator",) if action in admin else (),
                    own_history_footers=action == "repair",
                    input_kind=input_kind,
                    channel_types=(0, 5, 11, 12) if action == "channel" else (),
                    modal=modal(action) if form else None,
                    modal_defaults=form,
                    presentation_mode="plain" if action == "export" else "v2",
                )
            )
        components.append(
            ComponentSpec(
                "halloween",
                "settings",
                "halloween.settings",
                rf"^{SESSION}$",
                captures=(CaptureSpec("session"), CaptureSpec("arg")),
                public=True,
                acl_key="events",
                session_bound=True,
            )
        )
        components.extend(
            ComponentSpec(
                "halloween",
                action,
                "halloween.contribute",
                r"^(?P<run>[a-f0-9]{16})$",
                captures=(CaptureSpec("run"),),
                public=True,
                acl_key="events",
                input_kind="modal",
                modal=modal(form),
                ephemeral=True,
                presentation_mode="legacy",
                deadline_ms=120_000,
                source_message_facts=True,
                human_guild_member=True,
            )
            for action, form in (("submit_cards", "add_cards"), ("withdraw_cards", "remove_cards"))
        )
        components.append(
            ComponentSpec(
                "halloween",
                "claim",
                "halloween.claim",
                r"^(?P<drop>[a-f0-9]{16})~(?P<slot>[0-3])$",
                captures=(CaptureSpec("drop"), CaptureSpec("slot")),
                public=True,
                acl_key="events",
                ephemeral=True,
                durable_receipts=True,
                private_loading=True,
                private_feedback=PrivateFeedbackSpec(
                    timeout_text=CLAIM_FEEDBACK_TEXT["timeout"],
                    rejected_text=CLAIM_FEEDBACK_TEXT["unavailable"],
                ),
                presentation_mode="legacy",
                deadline_ms=120_000,
            )
        )
        components.extend(
            ComponentSpec(
                "halloween",
                action,
                "halloween.dashboard",
                rf"^{SESSION}$",
                captures=(CaptureSpec("session"), CaptureSpec("arg")),
                public=True,
                acl_key="event_leaderboard",
                session_bound=True,
            )
            for action in ("lb_previous", "lb_next")
        )
        components.extend(
            ComponentSpec(
                "halloween",
                action,
                "halloween.pool_view",
                r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,10})$",
                captures=(
                    CaptureSpec("session"),
                    CaptureSpec("revision", "integer", minimum=0, maximum=2_147_483_647),
                ),
                public=True,
                acl_key="event_leaderboard",
                session_bound=True,
                presentation_mode="legacy",
                deadline_ms=120_000,
            )
            for action in ("pool_ready", "pool_schedule", "pool_previous", "pool_next")
        )
        components.extend(
            ComponentSpec(
                "halloween",
                action,
                "halloween.personal_contributions",
                rf"^{SESSION}$",
                captures=(CaptureSpec("session"), CaptureSpec("arg")),
                public=True,
                acl_key="events",
                session_bound=True,
                ephemeral=True,
                input_kind="modal" if form else "none",
                modal=modal(form) if form else None,
                deadline_ms=120_000,
            )
            for action, form in (
                ("contributions_open", None),
                ("contributions_add", "add_cards"),
                ("contributions_remove", "remove_cards"),
                ("contributions_previous", None),
                ("contributions_next", None),
            )
        )
        return Manifest(
            schema=2,
            module=MODULE,
            channel_transition=ChannelTransitionSpec(
                quiesce_guild=True,
                drain_handlers=("halloween.tick", "halloween.claim"),
                suppress_handlers=("halloween.activity",),
                rebuild_posts=True,
            ),
            group="community",
            generation=self.generation,
            job_stream="lumi:jobs:community",
            display_name="Halloween Event",
            summary="Contributions, starting points and chat prize drops.",
            commands=(
                CommandSpec(
                    "events",
                    "halloween.open",
                    r"(?i)^\s+halloween(?:\s+(?:(?P<help>help)|(?P<operation>settings|tally|start|stop|finish)))?\s*$",
                    captures=(
                        CaptureSpec("help", required=False),
                        CaptureSpec("operation", required=False),
                    ),
                    public=True,
                    acl_key="events",
                    context_fields=("prefix",),
                    deadline_ms=120_000,
                    help=HelpSpec(".events halloween", "Open the Halloween event dashboard."),
                    presentation_modes=("v2", "legacy"),
                ),
                CommandSpec(
                    "event",
                    "halloween.event",
                    r"(?i)^\s+(?P<operation>leaderboard|lb|contributions|pool)(?:\s+(?P<help>help))?\s*$",
                    captures=(CaptureSpec("operation"), CaptureSpec("help", required=False)),
                    public=True,
                    acl_key="event_leaderboard",
                    context_fields=("prefix",),
                    deadline_ms=120_000,
                    presentation_modes=("v2", "legacy"),
                    help=HelpSpec(
                        ".event contributions\n.event lb\n.event pool",
                        "View your contributions, player rankings and prizes.",
                    ),
                ),
                CommandSpec(
                    "eventdrop",
                    "halloween.force_drop",
                    r"^\s*$",
                    public=True,
                    acl_key="events",
                    owner_scope="server",
                    configure_in_guild=False,
                    queue_class="control",
                    presentation_mode="legacy",
                    help=HelpSpec("eventdrop", "Start an event drop.", hidden=True),
                ),
            ),
            components=tuple(components),
            passive=tuple(
                PassiveMatcherSpec(
                    route=f"halloween.prize_{event}",
                    handler="halloween.prize",
                    event=f"message_{event}",
                    bot_ids=(-2,),
                    expression=MatcherExpr(
                        "regex", "reply_content", r"(?i)^\s*kv\s+[a-z0-9]{1,32}\s*$"
                    ),
                    extractions=(
                        FieldExtraction(
                            "reply_content",
                            r"(?i)^\s*kv\s+(?P<code>[a-z0-9]{1,32})\s*$",
                            (CaptureSpec("code", "normalized_text", maximum=32),),
                        ),
                    ),
                    subject_field="reply_author_id",
                    subject_required=True,
                    reference_author_max_age_ms=120_000,
                    watch_ttl_ms=120_000 if event == "create" else 0,
                    hydrate=event == "edit",
                    reply_prompt=True,
                    source_cleanup=True,
                    context_fields=(
                        "primary_embed_image_url",
                        "reply_author_id",
                        "reference_message_id",
                        "reply_reference_message_id",
                        "reply_created_at_ms",
                        "reply_prompt_module",
                    ),
                    presentation_mode="legacy",
                )
                for event in ("create", "edit")
            ) + (PassiveMatcherSpec(
                "halloween.post_deleted", "halloween.post_deleted", "message_delete",
                MatcherExpr("exists", field="message_id"), own_output_deleted=True,
                deadline_ms=120_000,
            ),),
            scheduled=tuple(
                ScheduledCallbackSpec(
                    f"halloween.{name}",
                    f"halloween.{name}",
                    scope="guild",
                    acl_key="events",
                    presentation_mode="legacy",
                    deadline_ms=120_000,
                )
                for name in ("tick", "activity")
            ),
            activity_observations=True,
            action_presentation_modes=("legacy",),
            capabilities=(
                "send_message",
                "send_role_message",
                "edit_message",
                "resolve_members",
                "read_owned_history",
                "send_dm",
                "delete_source_messages",
                "delete_message",
            ),
            version="1.19.4",
        )

    async def start(self) -> None:
        self._task = asyncio.create_task(self._replay(), name="halloween-outbox")
        self._activity_task = asyncio.create_task(
            self.activity_inbox.run(), name="halloween-activity",
        )

    async def close(self) -> None:
        tasks = [task for task in (self._task, self._activity_task) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.prize_loader.close()

    def begin_drain(self) -> None:
        self._draining = True

    def resume(self) -> None:
        self._draining = False

    def drain_ready(self) -> bool:
        return not self.activity_inbox.active and not any(lock.locked() for lock in self._locks.values())

    async def _observe_activity(self, observation, run):
        gate = getattr(self, "handover_gate", None)
        if gate and gate.busy(observation.guild):
            return
        if not await self.activity.count(
            MODULE, observation.guild, observation.actor, observation.message,
            observation.occurred_at_ms, self.activity_rules, run=run,
        ):
            return
        now = self.clock()
        await self.scheduler.publish(
            operation="upsert", schedule_key=f"{MODULE}:activity:{observation.guild}",
            route="halloween.activity", revision=now, guild_id=observation.guild,
            channel_id=observation.channel, actor_id=0, due_at_ms=now,
            expires_at_ms=now + 120_000, arguments={"run": run},
        )

    async def _replay(self) -> None:
        while True:
            try:
                await self._replay_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("event_outbox_replay_unavailable")
            await asyncio.sleep(20)

    async def _replay_once(self) -> None:
        if self._draining:
            return
        for guild in await self.store.active_guilds():
            async with self._locks.setdefault(guild, asyncio.Lock()):
                gate = getattr(self, "handover_gate", None)
                if gate and gate.critical(guild):
                    continue
                async with self.store.transaction(guild) as state:
                    if state.get("generation", 0) > self.generation:
                        continue
                    state["generation"] = self.generation
                    if any(item["status"] == "sending" for item in state["outbox"]):
                        state["paused"] = True
                    if any(
                        part["status"] in {"sending", "ambiguous", "failed"}
                        for part in state.get("standings_parts", [])
                    ):
                        state["paused"] = True
                await self._project_private_feedback(guild, state)
                await self._sync(guild, state)

    async def _project_private_feedback(self, guild: int, state: dict) -> None:
        try:
            await project_feedback(self.queue, guild, state, self.clock())
        except Exception:


            LOGGER.warning("event_private_feedback_projection_unavailable")

    def _result(self, job: str, intent=None) -> ResultEnvelope:
        return ResultEnvelope(
            1,
            job,
            MODULE,
            "community",
            self.generation,
            "ok",
            intents=(intent,) if intent else (),
            completed_at_ms=self.clock(),
        )

    def _notice(
        self, job: str, text: str, *, legacy=False, plain=False, kind="ephemeral"
    ) -> ResultEnvelope:
        return self._result(
            job,
            DiscordIntent(
                kind=kind,
                pages=() if plain else (PresentationPage(description=text),),
                content=text if plain else None,
                presentation_mode="plain" if plain else "legacy" if legacy else "v2",
            ),
        )

    async def _members(self, guild: int, identifiers: list[int], job: str) -> dict[int, dict]:
        if not identifiers:
            return {}
        result = await self.actions.request(
            job_id=job,
            kind="resolve_members",
            guild_id=guild,
            member_ids=tuple(identifiers),
            idempotency_key=f"members:{job}:{secrets.token_hex(4)}",
        )
        if not result.ok:
            raise ActionUnavailable("member resolution unavailable")
        return {int(member["id"]): member for member in result.data["members"]}

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        if request.handler == "halloween.event":
            operation = str(request.arguments.get("operation", "")).casefold()
            request = msgspec.structs.replace(
                request,
                handler="halloween.personal_contributions"
                if operation == "contributions"
                else "halloween.pool_view"
                if operation == "pool"
                else "halloween.leaderboard",
            )
        guild, actor = (
            int(request.event.get("guild_id") or 0),
            int(request.event.get("actor_id") or 0),
        )
        if generation != self.generation or not guild:
            return self._result(job_id)
        legacy = request.handler in {
            "halloween.tick",
            "halloween.activity",
            "halloween.claim",
            "halloween.contribute",
            "halloween.pool_view",
        }
        notice_kind = "send" if request.event.get("dispatch") == "command" else "ephemeral"
        try:
            if request.handler == "halloween.pool_view":
                return self._result(job_id, await self.pool_viewer.handle(request))
            async with self._locks.setdefault(guild, asyncio.Lock()):
                if request.handler == "halloween.post_deleted":
                    await self._observe_post_deleted(guild, request.event)
                    return self._result(job_id)
                if await self._acknowledge_rehearsal(request):
                    return self._result(job_id)
                if request.handler == "halloween.force_drop":
                    return await self._force_drop(request, job_id, guild, actor)
                if request.handler == "halloween.prize":
                    return await self._capture_prize(request, job_id, guild, actor)
                if request.handler == "halloween.contribute":
                    return await self._contribute(request, job_id, guild, actor)
                if request.handler == "halloween.personal_contributions":
                    return await self._personal_contributions(request, job_id, guild, actor)
                return await self._handle(request, job_id, guild, actor)
        except engine.EventError as exc:
            return self._notice(
                job_id,
                str(exc),
                legacy=legacy,
                plain=request.route.endswith(".export"),
                kind=notice_kind,
            )
        except Exception:
            LOGGER.warning("event_request_unavailable", extra={"handler": request.handler})
            if request.handler == "halloween.prize":
                return self._result(job_id)
            return (
                self._notice(
                    job_id,
                    "Unavailable.",
                    legacy=legacy,
                    plain=request.route.endswith(".export"),
                    kind=notice_kind,
                )
                if actor
                else self._result(job_id)
            )

    async def _observe_post_deleted(self, guild: int, event: dict) -> None:
        if event.get("dispatch") != "passive" or event.get("_own_output_deleted") is not True:
            return
        async with self.store.transaction(guild) as state:
            if state.get("generation") != self.generation:
                return
            drop = state["drop"]
            if (
                not drop or not drop["message"] or drop.get("message_deleted")
                or (drop["channel"], drop["message"]) != (event.get("channel_id"), event.get("message_id"))
            ):
                return
            self._drop_message_deleted(state)
            updated = copy.deepcopy(state)
        await self._sync(guild, updated)

    async def _acknowledge_rehearsal(self, request: TypedRequest) -> bool:

        if request.handler != "halloween.claim" or request.event.get("dispatch") != "component":
            return False
        message = int(request.event.get("message_id") or 0)
        raw = await self.queue.get(f"lumi:halloween:rehearsal:{message}")
        if not raw or len(raw) > 2048:
            return False
        try:
            lease = json.loads(raw)
            now = self.clock()
            if (
                not isinstance(lease, dict)
                or lease.get("generation") != self.generation
                or lease.get("guild") != int(request.event.get("guild_id") or 0)
                or lease.get("channel") != int(request.event.get("channel_id") or 0)
                or lease.get("message") != message
                or lease.get("drop") != request.arguments.get("drop")
                or not now < lease["expires"] <= now + 600_000
            ):
                return False
            raw_receipt = await self.queue.hget(
                ComponentReceipts.key(MODULE, message), str(request.event.get("interaction_id"))
            )
            receipt = _claim_receipt(raw_receipt, lease["drop"]) if raw_receipt else None
            if receipt is None:
                return False
            return (
                receipt.get("actor_id") == str(request.event.get("actor_id"))
                and receipt.get("slot") == str(request.arguments.get("slot"))
                and receipt.get("timestamp") == request.event.get("interaction_created_at_ms")
            )
        except (KeyError, TypeError, ValueError):
            return False

    async def _handle(
        self, request: TypedRequest, job: str, guild: int, actor: int
    ) -> ResultEnvelope:
        now = self.clock()
        legacy = request.handler in {"halloween.tick", "halloween.activity", "halloween.claim"}
        session = None
        if request.handler in {"halloween.open", "halloween.leaderboard"}:
            if request.arguments.get("help"):
                return self._result(
                    job,
                    DiscordIntent(
                        kind="send",
                        pages=(
                            PresentationPage(
                                description=f"{request.event.get('prefix', '.')}"
                                + (
                                    "event lb"
                                    if request.handler == "halloween.leaderboard"
                                    else "events halloween"
                                )
                            ),
                        ),
                    ),
                )
            session = {
                "id": secrets.token_hex(8),
                "owner": actor,
                "guild": guild,
                "channel": int(request.event.get("channel_id") or 0),
                "expires": now + 900_000,
                "screen": (
                    "leaderboard"
                    if request.handler == "halloween.leaderboard"
                    else "review"
                    if request.arguments.get("operation") == "tally"
                    else "settings"
                    if request.arguments.get("operation") == "settings"
                    else "overview"
                ),
                "page": 0,
                "selected": "new",
                "presentation_mode": None,
            }
        elif request.handler in {"halloween.dashboard", "halloween.settings"}:
            raw = await self.cache.get(
                f"lumi:halloween:session:{request.arguments.get('session', '')}"
            )
            session = json.loads(raw) if raw else None
            if session is not None:

                session.setdefault("presentation_mode", "v2")
            if (
                not session
                or session["owner"] != actor
                or session["guild"] != guild
                or session["expires"] <= now
            ):
                raise engine.EventError("This menu has expired.")
            if request.handler == "halloween.settings":

                session = {
                    **session,
                    "id": secrets.token_hex(8),
                    "screen": "settings",
                    "page": 0,
                    "expires": now + 900_000,
                    "presentation_mode": "v2",
                }
            elif session.get("presentation_mode") == "legacy":
                raise engine.EventError("Open event settings.")

        action = request.route.rpartition(".")[2]
        if request.handler == "halloween.open" and request.arguments.get("operation") in {
            "start",
            "stop",
            "finish",
        }:
            action = request.arguments["operation"]
        admin = bool(request.event.get("administrator"))
        admin_actions = {
            "config",
            "channel",
            "pools",
            "ranking",
            "tally",
            "review",
            "handoffs",
            "confirm",
            "start",
            "stop",
            "finish",
            "new",
            "new_confirm",
            "pool",
            "add_stock",
            "pick_pool",
            "pool_remove",
            "delivered",
            "pick_handoff",
            "export",
            "repair",
            "main_prize",
            "wizard_next",
            "wizard_back",
        }
        if session and action in admin_actions:
            current = await self._members(guild, [actor], job)
            if not current.get(actor, {}).get("administrator") or current.get(actor, {}).get(
                "blocked"
            ):
                raise engine.EventError("Unavailable.")
            admin = True
        ranking_members = None
        if action in {"ranking", "tally"}:
            ranking_ids = (
                engine.users(request.arguments["values"]["people"])
                if action == "ranking"
                else [int(request.arguments["values"]["person"][0])]
            )
            ranking_members = await self._members(guild, ranking_ids, job)
            if any(
                user not in ranking_members
                or ranking_members[user]["bot"]
                or ranking_members[user]["blocked"]
                for user in ranking_ids
            ):
                raise engine.EventError("Ranking contains an unavailable person.")
        if action == "confirm":
            async with self.store.transaction(guild) as snapshot:
                ranked = list(snapshot["ranking"])
            current = await self._members(guild, ranked, job)
            if any(
                person not in current
                or current[person]["bot"]
                or current[person]["blocked"]
                or not current[person].get("allowed", True)
                for person in ranked
            ):
                raise engine.EventError("A participant is no longer available.")

        receipts, excluded = [], set()
        if legacy:
            async with self.store.transaction(guild) as snapshot:
                drop_snapshot = copy.deepcopy(snapshot["drop"])
            if drop_snapshot and drop_snapshot["message"]:
                raw = await self.queue.hvals(
                    ComponentReceipts.key(MODULE, drop_snapshot["message"])
                )
                receipts = [
                    receipt for value in raw
                    if (receipt := _claim_receipt(value, drop_snapshot["id"])) is not None
                ]
                actors = list({int(item["actor_id"]) for item in receipts})
                if actors:
                    current = await self._members(guild, actors, job)
                    excluded = {
                        user
                        for user in actors
                        if user not in current
                        or current[user]["blocked"]
                        or current[user]["bot"]
                        or not current[user].get("allowed", True)
                    }

        async with self.store.transaction(guild) as state:
            if state.get("generation", 0) > self.generation:
                return self._result(job)
            state["generation"] = self.generation
            if request.handler == "halloween.open" and request.arguments.get("operation") in {
                "tally",
                "start",
                "stop",
                "finish",
            }:
                if actor != state.get("organizer_id"):
                    raise engine.EventError("Only the event organizer can use this event command.")
                admin = True
            if request.handler == "halloween.open" and request.event.get("prefix"):
                state["config"]["prefix"] = request.event["prefix"]
            home = state["config"].get("setup_channel", 0)
            channel = int(request.event.get("channel_id") or 0)
            leaderboard_navigation = (
                request.handler == "halloween.dashboard"
                and session["screen"] == "leaderboard"
                and action in {"lb_previous", "lb_next"}
            )
            if leaderboard_navigation and channel != session["channel"]:
                raise engine.EventError("This menu has expired.")
            if (
                session
                and home
                and channel != home
                and request.handler != "halloween.leaderboard"
                and not leaderboard_navigation
            ):
                raise engine.EventError(f"Use <#{home}>.")
            if request.event.get("dispatch") == "scheduled" and request.arguments.get(
                "run"
            ) not in {None, state["run"]}:
                return self._result(job)
            if request.handler == "halloween.claim" and (
                not state["drop"]
                or state["drop"]["id"] != request.arguments.get("drop")
                or state["drop"]["message"] != int(request.event.get("message_id") or 0)
            ):
                return self._notice(job, "This drop has closed.", legacy=True)
            engine.advance(state, now)
            if (
                request.handler == "halloween.open"
                and not request.arguments.get("operation")
                and admin
                and state["stage"] == "setup"
            ):
                session["screen"] = "settings"
            if (
                state["stage"] in {"tally", "ready", "running"}
                and state.get("organizer_id")
                and not state.get("submissions_dm")
            ):
                state["submissions_dm"] = {
                    "status": "pending",
                    "key": f"submissions:{state['run']}",
                }
            if request.handler == "halloween.activity":
                if (
                    state["stage"] == "running"
                    and not state["paused"]
                    and state["drop"] is None
                    and now >= state["last_settled"] + engine.drop_gap_ms(state)
                ):
                    engine.begin_drop(state, now, self.rng)
                    self._enqueue(state, "warning")
            elif legacy:
                drop = state["drop"]
                if (
                    drop
                    and drop["status"] == "warning"
                    and not state["paused"]
                    and now >= drop["reveal_at"]
                ):
                    self._enqueue(state, "reveal")
                elif drop and drop["status"] == "open":
                    engine.update_claims(state, receipts, now, excluded)
                    engine.settle(state, receipts, now, excluded)
                    if not any(item["status"] != "pending" for item in state["outbox"]):
                        self._enqueue(state, "settled" if drop["status"] == "settled" else "claims")
                elif drop and drop["status"] == "cancelled":
                    self._enqueue(state, "settled")
            elif session:
                if session.get("run") and session["run"] != state["run"]:
                    raise engine.EventError("This event has changed. Open a new menu.")
                stock_receipt = (
                    "stock:"
                    + hashlib.sha256(
                        str(request.event.get("interaction_id") or job).encode()
                    ).hexdigest()
                )
                if action == "add_stock" and stock_receipt in state.get("processed", []):
                    return self._notice(job, "Stock already saved.")
                if (
                    state["stage"] in {"setup", "tally", "ready"}
                    and admin
                    and request.handler == "halloween.dashboard"
                    and request.event.get("message_id")
                    and session["screen"] != "leaderboard"
                ):
                    message = int(request.event["message_id"])
                    saved = state.setdefault("setup_messages", [])
                    if not any(part["message"] == message for part in saved):
                        if len(saved) >= 100:
                            raise engine.EventError("Too many setup menus. Use the latest menu.")
                        saved.append(
                            dict(
                                message=message,
                                channel=int(request.event["channel_id"]),
                                status="pending",
                                retire_after="contributions"
                                if state["stage"] == "setup"
                                else "running",
                            )
                        )
                if action in {
                    "config",
                    "pool",
                    "add_stock",
                    "ranking",
                    "tally",
                    "confirm",
                    "pool_remove",
                    "delivered",
                }:
                    if session.get("revision") != state["revision"]:
                        raise engine.EventError("The event changed. Refresh the menu first.")
                values = request.arguments.get("values", {})
                if session["screen"] == "setup" and action in {
                    "config",
                    "channel",
                    "pool",
                    "start",
                }:
                    expected = {
                        "config": "details",
                        "channel": "channel",
                        "pool": "pools",
                        "start": "review",
                    }[action]
                    if (
                        state.get("setup_step", "details") != expected
                        or session.get("setup_step") != expected
                    ):
                        raise engine.EventError("The setup changed. Open a new menu.")
                if action in {"overview", "settings", "pools", "review", "handoffs", "leaderboard"}:
                    session["screen"], session["page"] = action, 0
                    if action == "overview":
                        session["screen"] = "settings"
                elif action in {"previous", "next", "lb_previous", "lb_next"}:
                    session["page"] = max(
                        0, session["page"] + (-1 if action in {"previous", "lb_previous"} else 1)
                    )
                elif action in {"add_cards", "remove_cards"}:
                    engine.contribute(state, actor, values["codes"], action == "remove_cards", now)
                elif action in {"wizard_next", "wizard_back"}:
                    if state["stage"] != "setup" or session["screen"] != "setup":
                        raise engine.EventError("Unavailable.")
                    steps = ("details", "channel", "prize", "pools", "review")
                    step = state.get("setup_step", "details")
                    if session.get("setup_step") != step:
                        raise engine.EventError("The setup changed. Open a new menu.")
                    if action == "wizard_next" and step not in {"prize", "pools"}:
                        raise engine.EventError("Complete this step first.")
                    state["setup_step"] = steps[
                        max(0, min(4, steps.index(step) + (1 if action == "wizard_next" else -1)))
                    ]
                elif action == "config":
                    if state["stage"] != "setup":
                        raise engine.EventError("Setup is locked after contributions open.")
                    try:
                        points = int(values["points"])
                    except ValueError as exc:
                        raise engine.EventError("Use whole numbers.") from exc
                    drop_ms = engine.duration(
                        values.get("drop_time", f"{values.get('hours', '0')}h"), label="Drop stage"
                    )
                    if not 1 <= points <= 1_000_000:
                        raise engine.EventError("Invalid points or duration.")
                    contribution_ms = engine.duration(values.get("contribution_time", "24h"))
                    if not state.get("organizer_id"):
                        state["organizer_id"] = actor
                    home = state["config"].get("setup_channel") or int(
                        request.event.get("channel_id") or session.get("channel") or 0
                    )
                    if not home:
                        raise engine.EventError("Unavailable.")
                    state["config"].update(
                        points=points,
                        drop_ms=drop_ms,
                        contribution_ms=contribution_ms,
                        instructions=values.get("instructions", ""),
                        setup_channel=home,
                    )
                    if session["screen"] == "setup":
                        state["setup_step"] = "channel"
                elif action == "channel":
                    if state["drop"] or state["outbox"]:
                        raise engine.EventError(
                            "The drop destination is locked while a drop is active."
                        )


                    for part in state.get("standings_parts", []):
                        part.setdefault("channel", state["config"]["channel"])
                    if "setup_channel" not in state["config"]:
                        state["config"]["setup_channel"] = engine.event_channel(state)
                    state["config"]["channel"] = int(values[0])
                    if session["screen"] == "setup":
                        state["setup_step"] = "prize"
                elif action == "ranking":
                    if actor != state.get("organizer_id"):
                        raise engine.EventError("Only the event organizer can enter the tally.")
                    if not engine.tally_open(state):
                        raise engine.EventError("Contributions must close first.")
                    mode = values["operation"][0]
                    existing = state["ranking"]
                    ranked = (
                        ranking_ids
                        if mode == "replace"
                        else (
                            [*existing, *ranking_ids]
                            if mode == "append"
                            else [user for user in existing if user not in ranking_ids]
                        )
                    )
                    if len(ranked) > engine.MAX_USERS or len(set(ranked)) != len(ranked):
                        raise engine.EventError("Ranking must contain unique people, up to 1000.")
                    state["ranking"] = ranked
                    session["screen"] = "review"
                elif action == "tally":
                    if actor != state.get("organizer_id"):
                        raise engine.EventError("Only the event organizer can enter the tally.")
                    engine.tally_person(state, ranking_ids[0], values["worth"])
                    session["screen"] = "review"
                elif action == "confirm":
                    if actor != state.get("organizer_id"):
                        raise engine.EventError("Only the event organizer can confirm the tally.")
                    engine.confirm_ranking(state)
                elif action == "pool":
                    key = session["selected"]
                    existing_frames = next((source for source, pool in state["pools"].items()
                                            if pool["type"] == "frames"), None)
                    new_pool = key == "new"
                    if key == "new":
                        key = secrets.token_hex(8)
                    form_values = dict(values)
                    form_values["type"] = values["type"][0]
                    engine.put_pool(state, key, form_values, now=now)
                    if new_pool and form_values["type"] == "frames" and existing_frames:
                        engine.merge_frame_pools(state, existing_frames, now)
                        key = existing_frames
                    session["selected"] = "new" if session["screen"] == "setup" else key
                    if session["screen"] != "setup":
                        session["screen"] = "pools"
                elif action == "add_stock":
                    if len(state.setdefault("processed", [])) >= 4096:
                        raise engine.EventError("Too many stock changes.")
                    engine.add_stock(state, session["selected"], values, now)
                    state["processed"].append(stock_receipt)
                    session["screen"] = "pools"
                elif action == "pick_pool":
                    if values[0] != "new" and values[0] not in state["pools"]:
                        raise engine.EventError("Unavailable pool.")
                    session["selected"] = values[0]
                elif action == "pool_remove":
                    pool = state["pools"].get(session["selected"])
                    if pool and pool["reserved"]:
                        raise engine.EventError("This pool has a reserved prize.")
                    state["pools"].pop(session["selected"], None)
                    session["selected"] = "new"
                elif action == "pick_handoff":
                    if values[0] not in state["handoffs"]:
                        raise engine.EventError("Unavailable handoff.")
                    session["selected"] = values[0]
                elif action == "delivered":
                    if session["selected"] in state["handoffs"]:
                        state["handoffs"][session["selected"]]["delivered"] = True
                elif action == "start":
                    if state["stage"] == "setup" and state.get("setup_step", "review") != "review":
                        raise engine.EventError("Complete setup first.")
                    if (
                        state["stage"] in {"ready", "running"}
                        and request.handler != "halloween.open"
                    ):
                        raise engine.EventError(
                            f"Use {state['config'].get('prefix', '.')}events halloween start "
                            "to start chat drops."
                        )
                    if state["outbox"] or any(
                        part["status"] in {"sending", "ambiguous", "failed"}
                        for part in state.get("standings_parts", [])
                    ):
                        raise engine.EventError("Repair the current drop first.")
                    was_setup = state["stage"] == "setup"
                    engine.start(state, now)
                    if was_setup:
                        state["contribution_portal"] = {
                            "channel": engine.event_channel(state),
                            "message": 0,
                            "status": "pending",
                            "key": f"contribution-portal:{state['run']}",
                        }
                        session["screen"] = "setup_complete"
                elif action == "stop":
                    if state["stage"] in {"contributions", "tally", "ready", "running"}:
                        state["paused"] = True
                        drop = state["drop"]
                        if drop and drop["status"] in {"warning", "publishing"} and drop["open_at"]:
                            drop["status"] = "open"
                        if drop and drop["status"] in {"warning", "publishing"}:
                            engine.refund(state, drop)
                            drop["status"] = "cancelled"
                            self._enqueue(state, "settled")
                        elif drop and drop["status"] == "open":
                            drop["close_at"] = min(now, drop["close_at"])
                            drop["settle_at"] = drop["close_at"] + engine.SETTLEMENT_DELAY_MS
                elif action == "finish":
                    engine.finish(state, now)
                    if state["drop"] and state["drop"]["status"] == "cancelled":
                        self._enqueue(state, "settled")
                elif action == "new":
                    session["screen"] = "new_confirm"
                elif action == "new_confirm":
                    if state["stage"] != "finished" or state["drop"]:
                        raise engine.EventError("Finish the current event and its drop first.")
                    config, handoffs, organizer = (
                        state["config"],
                        state["handoffs"],
                        state.get("organizer_id", 0),
                    )
                    config = {**config, "prize_pacing": True, "prize_release_policy": engine.RELEASE_POLICY,
                              "automatic_start": "after_contributions_24h_v1", "card_timetable": "ordered_v1"}
                    state.clear()
                    state.update(engine.new_event())
                    state.update(
                        config=config,
                        handoffs=handoffs,
                        organizer_id=organizer,
                        generation=self.generation,
                    )
                    state["config"]["setup_channel"] = int(request.event.get("channel_id") or 0)
                    session["screen"], session["page"] = "settings", 0
                elif action == "repair":
                    session["screen"] = "settings"
                elif action == "main_prize":
                    if actor != state.get("organizer_id"):
                        raise engine.EventError("Only the event organizer can set the main prize.")
                    if state["stage"] not in {"setup", "contributions", "tally", "ready"}:
                        raise engine.EventError("The main prize is locked after chat drops start.")
                    prompt = state.get("prize_prompt")
                    if prompt and prompt["status"] == "applied":
                        text = f"Reply with kv <card code>: https://discord.com/channels/{guild}/{prompt['channel']}/{prompt['message']}"
                        return self._notice(job, text)
                    if not prompt or prompt["status"] == "removed":
                        state["prize_prompt"] = dict(
                            message=0,
                            channel=engine.event_channel(state),
                            hash="",
                            status="pending",
                            retire_with_setup=False,
                        )
                elif action == "export":
                    if actor != state.get("organizer_id"):
                        raise engine.EventError(
                            "Only the event organizer can view card submissions."
                        )
                    exported = copy.deepcopy(state)

        await self._project_private_feedback(guild, state)
        await self._flush(guild, job)
        if action == "repair":
            await self._repair(guild, job)
            await self._repair_standings(guild, job)
            await self._repair_prize_prompt(guild, job)
            await self._repair_contribution_portal(guild, job)
            async with self.store.transaction(guild) as state:
                for part in state.get("contribution_panels", []):
                    if part["status"] == "failed":
                        part["status"] = "pending"
        await self._publish_updates(guild, job)
        async with self.store.transaction(guild) as state:
            view_state = copy.deepcopy(state)
        await self._sync(guild, view_state)
        if legacy:


            if request.handler == "halloween.claim":
                return self._result(job)
            return (
                self._notice(job, "Attempt recorded.", legacy=True) if actor else self._result(job)
            )
        if action == "export":
            return self._result(job, await self._export(exported))
        if (
            action in {"start", "finish"}
            and request.handler == "halloween.open"
            and view_state["stage"] in {"running", "finished"}
        ):
            return self._result(job)
        if session and any(
            part["message"] == int(request.event.get("message_id") or 0)
            and part["status"] == "removed"
            for part in view_state.get("setup_messages", [])
        ):
            return self._result(job)
        session["run"], session["revision"] = view_state["run"], view_state["revision"]
        intent = await self._dashboard(
            session,
            view_state,
            admin,
            "send"
            if request.handler in {"halloween.open", "halloween.leaderboard", "halloween.settings"}
            else "edit",
        )
        if intent.session_id:
            await self.cache.set(
                f"lumi:halloween:session:{session['id']}",
                json.dumps(session),
                px=max(1, session["expires"] - now),
            )
        return self._result(job, intent)

    async def _force_drop(
        self, request: TypedRequest, job: str, guild: int, actor: int
    ) -> ResultEnvelope:
        if (
            self.owner_id <= 0
            or actor != self.owner_id
            or request.event.get("dispatch") != "command"
            or request.route != "halloween.force_drop"
        ):
            return self._result(job)
        source = int(request.event.get("message_id") or 0)
        if not source:
            raise engine.EventError("Unavailable.")
        key = f"force-drop:{source}"
        async with self.store.transaction(guild) as state:
            if state.get("generation", 0) > self.generation:
                return self._result(job)
            state["generation"] = self.generation
            if key not in state["processed"]:
                now = self.clock()
                if state["stage"] != "running" or state["paused"] or now >= state["finish_at"]:
                    raise engine.EventError("The event is not running.")
                if state["drop"] or state["outbox"]:
                    raise engine.EventError("A drop is already active.")
                if len(state["processed"]) >= 4096:
                    raise engine.EventError("Unavailable.")
                engine.begin_drop(state, now, self.rng)
                state["processed"].append(key)
                self._enqueue(state, "warning")
        await self._flush(guild, job)
        async with self.store.transaction(guild) as state:
            snapshot = copy.deepcopy(state)
        await self._sync(guild, snapshot)
        return self._notice(job, "Drop started.", legacy=True, kind="send")

    async def _personal_contributions(
        self, request: TypedRequest, job: str, guild: int, actor: int
    ) -> ResultEnvelope:

        now = self.clock()
        channel = int(request.event.get("channel_id") or 0)
        command = request.event.get("dispatch") == "command"
        if not actor or not channel:
            raise engine.EventError("Unavailable.")
        if command and request.arguments.get("help"):
            return self._notice(
                job, f"{request.event.get('prefix', '.')}event contributions", kind="send"
            )
        action = request.route.rpartition(".")[2]
        if command:
            session = dict(
                id=secrets.token_hex(8),
                owner=actor,
                guild=guild,
                channel=channel,
                expires=now + 900_000,
                generation=self.generation,
                screen="contributions_launcher",
                page=0,
            )
        else:
            if request.event.get("dispatch") != "component":
                raise engine.EventError("Unavailable.")
            raw = await self.cache.get(
                f"lumi:halloween:session:{request.arguments.get('session', '')}"
            )
            session = json.loads(raw) if raw else None
            if (
                not session
                or session.get("owner") != actor
                or session.get("guild") != guild
                or session.get("channel") != channel
                or session.get("expires", 0) <= now
                or session.get("generation") != self.generation
                or session.get("screen") not in {"contributions_launcher", "personal_contributions"}
            ):
                raise engine.EventError("This menu has expired.")
            opening = action == "contributions_open"
            if opening != (session["screen"] == "contributions_launcher"):
                raise engine.EventError("Unavailable.")
            members = await self._members(guild, [actor], job)
            member = members.get(actor, {})
            if (
                not member
                or member.get("bot")
                or member.get("blocked")
                or not member.get("allowed", True)
            ):
                raise engine.EventError("Unavailable.")
            if opening:
                session = {
                    **session,
                    "id": secrets.token_hex(8),
                    "screen": "personal_contributions",
                    "page": 0,
                }
            elif action not in {
                "contributions_add",
                "contributions_remove",
                "contributions_previous",
                "contributions_next",
            }:
                raise engine.EventError("Unavailable.")

        changed = False
        async with self.store.transaction(guild) as state:
            if state.get("generation", 0) > self.generation:
                return self._result(job)
            if not command and session.get("run") != state["run"]:
                raise engine.EventError("This menu has expired.")
            if not state.get("organizer_id"):
                raise engine.EventError("No event.")
            session["run"] = state["run"]
            if not command and action in {"contributions_add", "contributions_remove"}:
                values = request.arguments.get("values", {})
                if not isinstance(values, dict) or not isinstance(values.get("codes"), str):
                    raise engine.EventError("Invalid input.")
                engine.contribute(
                    state, actor, values["codes"], action == "contributions_remove", now
                )
                state["generation"] = self.generation
                changed = True
            codes = list(state["contributions"].get(str(actor), ()))
            editable = (
                state["stage"] == "contributions"
                and not state["paused"]
                and now < state["contribution_end"]
            )

        if changed:
            try:
                await self._publish_updates(guild, job)
                async with self.store.transaction(guild) as state:
                    snapshot = copy.deepcopy(state)
                await self._sync(guild, snapshot)
            except Exception:
                LOGGER.warning("event_contribution_publication_unavailable")

        def button(name: str, label: str, disabled=False):
            return PresentationControl(
                "button",
                "halloween",
                name,
                f"{session['id']}~x",
                label=label,
                disabled=disabled,
            )

        if command:
            page = PresentationPage(
                description="Contributions",
                controls=(button("contributions_open", "Open Contributions"),),
            )
            kind = "send"
        else:
            total = max(1, (len(codes) + 19) // 20)
            offset = (
                -1 if action == "contributions_previous" else int(action == "contributions_next")
            )
            session["page"] = min(total - 1, max(0, session["page"] + offset))
            start = session["page"] * 20
            controls = [
                button("contributions_add", "Add Cards", not editable),
                button("contributions_remove", "Remove Cards", not editable or not codes),
            ]
            if total > 1:
                controls.extend(
                    (
                        button("contributions_previous", "Previous", session["page"] == 0),
                        button("contributions_next", "Next", session["page"] == total - 1),
                    )
                )
            description = f"{len(codes)} {'card' if len(codes) == 1 else 'cards'}"
            if total > 1:
                description += f" · {session['page'] + 1}/{total}"
            description += "\n\n" + ("\n".join(codes[start : start + 20]) or "No cards added.")
            if not editable:
                description += "\n\nContributions are closed."
            page = PresentationPage(
                title="Your contributions", description=description, controls=tuple(controls)
            )
            kind = "ephemeral" if action == "contributions_open" else "edit"
        await self.cache.set(
            f"lumi:halloween:session:{session['id']}",
            json.dumps(session),
            px=max(1, session["expires"] - now),
        )
        return self._result(job, DiscordIntent(kind=kind, pages=(page,), session_id=session["id"]))

    async def _contribute(
        self, request: TypedRequest, job: str, guild: int, actor: int
    ) -> ResultEnvelope:

        action = request.route.rpartition(".")[2]
        run = request.arguments.get("run")
        channel = int(request.event.get("channel_id") or 0)
        message = int(request.event.get("message_id") or 0)
        values = request.arguments.get("values", {})
        source = request.event.get("_component_message")
        if (
            action not in {"submit_cards", "withdraw_cards"}
            or not actor
            or not channel
            or not message
            or request.event.get("dispatch") != "component"
            or not isinstance(values, dict)
            or not isinstance(values.get("codes"), str)
            or not isinstance(run, str)
            or not admitted_request(
                request, module=MODULE, group="community", generation=self.generation,
                actor=actor, guild=guild, channel=channel, acl="events",
            )
            or not isinstance(source, dict)
        ):
            raise engine.EventError("Unavailable.")
        required = {f"lumi:halloween:{name}:{run}" for name in ("submit_cards", "withdraw_cards")}
        component_ids = source.get("component_custom_ids")
        if (
            source.get("current_lumi") is not True
            or source.get("webhook") is not False
            or source.get("components_v2") is not False
            or source.get("guild_id") != guild
            or source.get("channel_id") != channel
            or source.get("message_id") != message
            or not isinstance(component_ids, list)
            or any(not isinstance(value, str) for value in component_ids)
            or not required.issubset(component_ids)
        ):
            raise engine.EventError("Unavailable.")
        async with self.store.transaction(guild) as state:
            if state.get("generation", 0) > self.generation:
                return self._result(job)
            if run != state["run"]:
                raise engine.EventError("Contributions are closed.")
            if engine.event_channel(state) and channel != engine.event_channel(state):
                raise engine.EventError("Unavailable.")
            state["generation"] = self.generation
            engine.advance(state, self.clock())
            closed = state["stage"] != "contributions" or state["paused"]
            if not closed:
                engine.contribute(
                    state, actor, values["codes"], action == "withdraw_cards", self.clock()
                )
            if (
                state["stage"] in {"tally", "ready", "running"}
                and state.get("organizer_id")
                and not state.get("submissions_dm")
            ):
                state["submissions_dm"] = {"status": "pending", "key": f"submissions:{run}"}
            panels = state.setdefault("contribution_panels", [])
            if not any(
                part["message"] == message and part["channel"] == channel for part in panels
            ):

                if len(panels) >= 100:
                    panels.pop(0)
                panels.append(
                    {"channel": channel, "message": message, "hash": "", "status": "pending"}
                )


        try:
            await self._publish_updates(guild, job)
            async with self.store.transaction(guild) as state:
                snapshot = copy.deepcopy(state)
            await self._sync(guild, snapshot)
        except Exception:
            LOGGER.warning("event_contribution_publication_unavailable")
        return self._notice(
            job,
            "Contributions are closed."
            if closed
            else "Removed."
            if action == "withdraw_cards"
            else "Added.",
            legacy=True,
        )

    async def _prize_attachment(self, state: dict) -> TemporaryAttachment:
        asset = state["main_prize"]
        data = base64.b64decode(asset["data"], validate=True)
        if len(data) != asset["size"] or hashlib.sha256(data).hexdigest() != asset["sha256"]:
            raise ValueError("invalid saved prize image")
        identifier = secrets.token_hex(16)
        await asyncio.to_thread(self.temp_root.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread((self.temp_root / identifier).write_bytes, data)
        return TemporaryAttachment(
            reference=f"tmp:{identifier}",
            file_name=asset["file_name"],
            sha256=asset["sha256"],
            size=asset["size"],
            expires_at_ms=self.clock() + 120_000,
        )

    async def _capture_prize(self, request, job, guild, actor) -> ResultEnvelope:
        event = request.event
        command = int(event.get("reference_message_id") or 0)
        source = int(event.get("message_id") or 0)
        code = str(request.arguments["code"]).casefold()
        if not command or not source or event.get("reply_prompt_module") != MODULE:
            return self._result(job)
        async with self.store.transaction(guild) as state:
            prompt = state.get("prize_prompt")
            if (
                state.get("generation", 0) > self.generation
                or actor != state.get("organizer_id")
                or state["stage"] not in {"setup", "contributions", "tally", "ready"}
                or not prompt
                or prompt["status"] != "applied"
                or prompt["message"] != event.get("reply_reference_message_id")
                or prompt["channel"] != event.get("channel_id")
                or command < int((state.get("prize_lookup") or {}).get("command", 0))
            ):
                return self._result(job)
            if code in engine.occupied_cards(state):
                raise engine.EventError("This card is already pooled, reserved or awarded.")
            state["generation"] = self.generation
            state["prize_lookup"] = {
                "command": command,
                "source": source,
                "code": code,
                "status": "waiting",
            }
            run = state["run"]
        members = await self._members(guild, [actor], job)
        person = members.get(actor, {})
        if (
            not person.get("administrator")
            or person.get("blocked")
            or person.get("bot")
            or person.get("allowed") is False
        ):
            return self._result(job)
        url = event.get("primary_embed_image_url")
        if not url:
            return self._result(job)
        image = await self.prize_loader.fetch(str(url))
        async with self.store.transaction(guild) as state:
            if (
                state["run"] != run
                or state["generation"] != self.generation
                or state["prize_lookup"]["command"] != command
                or state["stage"] not in {"setup", "contributions", "tally", "ready"}
            ):
                return self._result(job)
            if code in engine.occupied_cards(state):
                raise engine.EventError("This card is already pooled, reserved or awarded.")
            state["main_prize"] = {
                **image,
                "code": code,
                "command": command,
                "source": source,
            }
            state["prize_lookup"]["status"] = "saved"
        await self._publish_updates(guild, job)
        async with self.store.transaction(guild) as state:
            prompt = state["prize_prompt"]
            parts = state.get("standings_parts", [])
            visible = prompt["status"] == "applied" and prompt["hash"] == self._prize_prompt_hash(
                state
            )
            if prompt["status"] == "removed":
                visible = any(
                    part["status"] == "applied" and part["hash"] == self._contribution_digest(state)
                    for part in state.get("contribution_panels", [])
                )
            if parts:
                visible = (
                    visible
                    and parts[0]["status"] == "applied"
                    and parts[0]["hash"]
                    == hashlib.sha256(encode(self._standings(state)[0])).hexdigest()
                )
        if visible:
            result = await self.actions.request(
                job_id=job,
                kind="delete_source_messages",
                guild_id=guild,
                channel_id=event["channel_id"],
                idempotency_key=f"prize-cleanup:{run}:{command}:{job}",
            )
            async with self.store.transaction(guild) as state:
                if state["run"] == run and state["prize_lookup"]["command"] == command:
                    state["prize_lookup"]["status"] = "complete" if result.ok else "needs_cleanup"
        return self._result(job)

    def _prize_prompt_page(self, state: dict) -> PresentationPage:
        prize = state.get("main_prize")
        return PresentationPage(
            title="Main event prize",
            description=""
            if prize
            else (
                "Organizer: reply to this message with kv <card code>. "
                "Your latest lookup becomes the main prize, not a random-drop item."
            ),
            file_name=prize["file_name"] if prize else None,
        )

    def _prize_prompt_hash(self, state: dict) -> str:
        return hashlib.sha256(encode(self._prize_prompt_page(state))).hexdigest()

    async def _publish_prize_prompt(self, guild: int, job: str) -> None:
        async with self.store.transaction(guild) as state:
            if state.get("generation") != self.generation:
                return
            finished = state["stage"] == "finished"
            prompt = copy.deepcopy(state.get("prize_prompt"))
        if finished:
            if prompt and prompt.get("message") and prompt["status"] != "removed":
                result = await self.actions.request(
                    job_id=job,
                    kind="delete_message",
                    guild_id=guild,
                    channel_id=prompt["channel"],
                    message_id=prompt["message"],
                    idempotency_key=f"prize-prompt-remove:{state['run']}:{prompt['message']}",
                )
                if result.ok:
                    async with self.store.transaction(guild) as state:
                        state["prize_prompt"]["status"] = "removed"
            return
        async with self.store.transaction(guild) as state:
            if (
                state.get("generation") != self.generation
                or not state.get("organizer_id")
                or not engine.event_channel(state)
            ):
                return
            prompt = state.get("prize_prompt")
            if prompt and prompt["status"] == "removed":
                return
            if not prompt:
                if state["stage"] != "setup":
                    return
                prompt = state["prize_prompt"] = {
                    "message": 0,
                    "channel": engine.event_channel(state),
                    "hash": "",
                    "status": "pending",
                }
            digest = self._prize_prompt_hash(state)
            if prompt["hash"] == digest or prompt["status"] in {"sending", "failed", "ambiguous"}:
                return
            prompt["status"] = "sending"
            prompt["markerless"] = True
            prompt["key"] = f"prize-prompt:{state['run']}:{digest[:24]}" + (
                f":{prompt['repair']}" if prompt.get("repair") else ""
            )
            snapshot = copy.deepcopy(state)
        try:
            result = await self.actions.request(
                job_id=job,
                kind="edit_message" if prompt["message"] else "send_message",
                guild_id=guild,
                channel_id=prompt["channel"],
                message_id=prompt["message"] or None,
                idempotency_key=prompt["key"],
                intent=DiscordIntent(
                    kind="edit" if prompt["message"] else "send",
                    presentation_mode="legacy",
                    pages=(self._prize_prompt_page(snapshot),),
                    attachments=(await self._prize_attachment(snapshot),)
                    if snapshot.get("main_prize")
                    else (),
                ),
            )
        except Exception:
            result = None
        async with self.store.transaction(guild) as state:
            if state["run"] != snapshot["run"] or state.get("generation") != self.generation:
                return
            current = state["prize_prompt"]
            if result and result.ok:
                current.update(
                    message=prompt["message"] or int(result.data["message_id"]),
                    hash=digest,
                    status="applied",
                )
            else:
                current["status"] = "failed" if result else "ambiguous"
                if prompt["message"] and result and result.data.get("deleted") is True:
                    current["message_deleted"] = True

    async def _repair_prize_prompt(self, guild: int, job: str) -> None:
        async with self.store.transaction(guild) as state:
            snapshot = copy.deepcopy(state)
        prompt = snapshot.get("prize_prompt")
        if not prompt or prompt["status"] not in {"sending", "ambiguous", "failed"}:
            return
        message = await self._repair_publication_message(
            guild, job, prompt,
            marker=f"Event {snapshot['run']} · Prize prompt",
            key=f"prize-prompt-repair:{job}",
            error="Could not confirm the prize-prompt send.",
        )
        async with self.store.transaction(guild) as state:
            current = state.get("prize_prompt")
            if state["run"] != snapshot["run"] or current != prompt:
                return
            current.pop("message_deleted", None)
            current.update(
                message=message,
                status="pending",
                hash="",
                repair=job,
            )

    def _enqueue(self, state: dict, kind: str) -> None:
        drop = state["drop"]
        suffix = f":{state['revision'] + 1}" if kind == "claims" else ""
        identifier = f"drop:{drop['id']}:{kind}{suffix}"
        if not any(item["id"] == identifier for item in state["outbox"]):
            state["outbox"].append({"id": identifier, "kind": kind, "status": "pending"})

    def _drop_intent(self, state: dict, kind: str) -> DiscordIntent:
        drop = state["drop"]
        controls = []
        missed_prize = (
            kind == "settled"
            and drop["status"] == "settled"
            and bool(drop["open_at"])
            and bool(drop["prize"])
            and drop["refunded"]
        )
        quiet_settlement = kind == "settled" and not drop["results"] and not missed_prize
        if quiet_settlement and not drop["open_at"]:
            return DiscordIntent(
                kind="edit",
                presentation_mode="legacy",
                pages=(
                    PresentationPage(title="Halloween", description="A Halloween drop is coming…"),
                ),
            )
        if kind == "warning":
            description = "A Halloween drop is coming…"
        elif kind == "settled":
            description = "\n".join(
                f"<@{entry['actor']}> · {safe(entry['text'])}" for entry in drop["results"]
            )
            if missed_prize:
                prize = drop["prize"]
                label = f"card {prize['code']}" if prize.get("code") else engine.prize_label(prize)
                notice = f"You missed {safe(label)}."
                description = f"{description}\n\n{notice}" if description else notice
        if kind not in {"warning", "settled"} or quiet_settlement:
            description = "Choose once. Beware the spiders."
            for index in range(4):
                controls.append(
                    PresentationControl(
                        "button",
                        "halloween",
                        "claim",
                        f"{drop['id']}~{index}",
                        label=drop["blank"],
                        disabled=quiet_settlement
                        or str(index) in drop["claims"]
                        or self.clock() >= drop["close_at"],
                    )
                )
        return DiscordIntent(
            kind="send" if kind == "warning" else "edit",
            pages=(
                PresentationPage(
                    title="Halloween",
                    description=description,
                ),
            ),
            controls=tuple(controls),
            presentation_mode="legacy",
            public_controls=True,
        )

    async def _flush(self, guild: int, job: str) -> None:


        while True:
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation:
                    return
                pending = next(
                    (item for item in state["outbox"] if item["status"] == "pending"), None
                )
                if not pending or not state["drop"]:
                    return
                if pending["kind"] in {"reveal", "claims"} and state["drop"]["status"] in {
                    "settled",
                    "cancelled",
                }:
                    state["outbox"].remove(pending)
                    continue
                if state["drop"].get("message_deleted"):
                    self._drop_message_deleted(state)
                    return
                pending["status"] = "sending"
                pending["markerless"] = True
                first_reveal = pending["kind"] == "reveal" and not state["drop"]["open_at"]
                if first_reveal:
                    state["drop"]["open_at"] = self.clock()
                    state["drop"]["close_at"] = self.clock() + 30_000
                    state["drop"]["settle_at"] = state["drop"]["close_at"] + engine.SETTLEMENT_DELAY_MS
                snapshot, item = copy.deepcopy(state), copy.deepcopy(pending)
            drop = snapshot["drop"]
            if item["kind"] == "reveal":
                await self._receipt_context(guild, snapshot)
            if item["kind"] != "warning" and not drop["message"]:
                result = None
            else:
                try:
                    notification_role = None
                    if item["kind"] == "warning":
                        configured = snapshot["config"].get("drop_notification_role_id")
                        if (
                            type(configured) is int
                            and configured > 0
                            and snapshot["config"].get("drop_notification_run") == snapshot["run"]
                        ):
                            notification_role = configured
                    action_kind = "edit_message"
                    if item["kind"] == "warning":
                        action_kind = "send_role_message" if notification_role else "send_message"
                    result = await self.actions.request(
                        job_id=job,
                        kind=action_kind,
                        guild_id=guild,
                        channel_id=drop["channel"],
                        message_id=drop["message"] or None,
                        **({"role_id": notification_role} if notification_role else {}),
                        idempotency_key=item["id"],
                        intent=self._drop_intent(snapshot, item["kind"]),
                    )
                except Exception:
                    result = None
            async with self.store.transaction(guild) as state:
                if (
                    state.get("generation") != self.generation
                    or not state["drop"]
                    or state["drop"]["id"] != drop["id"]
                ):
                    return
                pending = next(value for value in state["outbox"] if value["id"] == item["id"])
                if result is None or not result.ok:
                    state["paused"] = True
                    if item["kind"] != "warning" and result and result.data.get("deleted") is True:
                        if first_reveal:

                            state["drop"]["open_at"] = 0
                        self._drop_message_deleted(state)
                        return
                    pending["status"] = (
                        "ambiguous"
                        if result is None
                        or result.error_code
                        in {"ambiguous", "action_in_progress", "discord_action_failed"}
                        else "failed"
                    )
                    return
                state["outbox"].remove(pending)
                current = state["drop"]
                if item["kind"] == "warning":
                    current["message"] = int(result.data["message_id"])
                    current["status"] = "warning"
                    current["reveal_at"] = self.clock() + current["wait"]
                elif item["kind"] == "reveal":
                    current["status"] = "open"
                elif item["kind"] == "settled":
                    state["last_drop"] = copy.deepcopy(current)
                    state["drop"] = None
                    state["outbox"] = []

    def _drop_message_deleted(self, state: dict) -> None:
        current = state["drop"]
        if current["status"] in {"warning", "publishing"} and current["open_at"]:

            current["status"] = "open"
        if current["status"] in {"warning", "publishing", "cancelled"}:
            engine.refund(state, current)
            state["drop"] = None
        elif current["status"] == "open":
            current["message_deleted"] = True
            current["close_at"] = min(current["close_at"], self.clock())
            current["settle_at"] = current["close_at"] + engine.SETTLEMENT_DELAY_MS
        else:
            state["last_drop"] = copy.deepcopy(current)
            state["drop"] = None
        state["outbox"] = []

    async def _receipt_context(self, guild: int, state: dict) -> None:
        drop = state["drop"]
        context = dict(
            guild=str(guild),
            channel=str(drop["channel"]),
            message=str(drop["message"]),
            members={str(user): True for user in engine.participants(state)},
            payloads={f"{drop['id']}~{slot}": True for slot in range(4)},
            open_at=drop["open_at"],
            close_at=drop["close_at"],
            settle_at=drop["settle_at"],
        )
        await self.queue.set(
            f"{ComponentReceipts.key(MODULE, drop['message'])}:context", json.dumps(context), ex=600
        )

    def _standings(self, state: dict) -> list[tuple[PresentationPage, ...]]:
        if state["stage"] not in {"tally", "ready", "running", "finished"} or not engine.participants(state):
            return []
        scores = (
            engine.allocations(state["config"]["points"], state["ranking"])
            if state["stage"] == "tally" and not engine.automatic(state)
            else state["scores"]
        )
        ordered = sorted(scores.items(), key=lambda item: (-item[1], int(item[0])))
        lines, rank, previous = [], 0, None
        for index, (person, score) in enumerate(ordered):
            if score != previous:
                rank = index + 1
            previous = score
            lines.append(f"{rank}. <@{person}> · {score} points")
        pages = [
            PresentationPage(
                title="Points · provisional"
                if state["stage"] == "tally"
                else "Final leaderboard"
                if state["stage"] == "finished"
                else "Points",
                description="\n".join(lines[:10]),
            )
        ]
        if state["stage"] != "finished":
            if state.get("main_prize"):
                pages.insert(
                    0,
                    PresentationPage(
                        title="Main event prize",
                        file_name=state["main_prize"]["file_name"],
                    ),
                )
            if state.get("recent_claims"):
                pages.append(
                    PresentationPage(
                        title="Latest 5 claimed items",
                        description="\n".join(
                            f"<@{entry['actor']}> · {safe(entry['text'])}"
                            for entry in state["recent_claims"][:5]
                        ),
                    )
                )
        return [tuple(pages)]

    async def _publish_updates(self, guild: int, job: str) -> None:
        await self._publish_contribution_portal(guild, job)
        await self._publish_contribution_panels(guild, job)
        await self._publish_prize_prompt(guild, job)
        await self._cleanup_event_ui(guild, job)
        async with self.store.transaction(guild) as state:
            if state.get("generation") != self.generation:
                return
            notification = state.get("submissions_dm")
            send_dm = notification and notification["status"] == "pending"
            if send_dm:
                notification["status"] = "sending"
            snapshot = copy.deepcopy(state)
        if send_dm:
            try:
                exported = await self._export(snapshot)
                result = await self.actions.request(
                    job_id=job,
                    kind="send_dm",
                    guild_id=guild,
                    member_id=snapshot["organizer_id"],
                    idempotency_key=notification["key"],
                    intent=DiscordIntent(
                        kind="send",
                        presentation_mode="legacy",
                        attachments=exported.attachments,
                        pages=(
                            PresentationPage(
                                title="Contributions closed",
                                description=(
                                    f"Your private submissions for server {guild} are attached.\n"
                                    f"Run {snapshot['config'].get('prefix', '.')}"
                                    "events halloween tally in the server to enter "
                                    "one person's tally at a time, in any order."
                                ),
                            ),
                        ),
                    ),
                )
            except Exception:
                result = None
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation or state["run"] != snapshot["run"]:
                    return
                state["submissions_dm"]["status"] = (
                    "sent" if result and result.ok else "ambiguous" if result is None else "failed"
                )


        for _ in range(5):
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation or state["outbox"]:
                    return
                if state["stage"] not in {"running", "finished"}:
                    return
                pages = self._standings(state)
                if not pages or not engine.event_channel(state):
                    return
                parts = state.setdefault("standings_parts", [])
                if not parts:
                    source = next(
                        (
                            part
                            for part in state.get("contribution_panels", [])
                            if part["status"] == "applied"
                            and part["channel"] == engine.event_channel(state)
                        ),
                        None,
                    )
                    if source:
                        parts.append(
                            dict(
                                message=source["message"],
                                channel=source["channel"],
                                hash="",
                                status="pending",
                            )
                        )
                        source["status"] = "replaced"
                for part in parts:
                    part.setdefault("channel", state["config"]["channel"])
                if len(parts) > len(pages):
                    surplus = copy.deepcopy(parts[-1])
                    snapshot = copy.deepcopy(state)
                else:
                    surplus = None
                if any(part["status"] in {"sending", "ambiguous", "failed"} for part in parts):
                    return
                target = None
                for position, group in enumerate(pages):
                    if surplus:
                        break
                    digest = hashlib.sha256(encode(group)).hexdigest()
                    if position == len(parts):
                        if state["stage"] == "finished":
                            return
                        parts.append(
                            {
                                "message": 0,
                                "channel": engine.event_channel(state),
                                "hash": "",
                                "status": "pending",
                            }
                        )
                    if parts[position]["hash"] != digest:
                        if state["stage"] == "finished" and not parts[position]["message"]:
                            return
                        target = position, group, digest
                        break
                if target is None and not surplus:
                    return
                if surplus:
                    publication = None
                else:
                    position, group, digest = target
                    part = parts[position]
                    part.update(status="sending", desired_hash=digest, markerless=True)
                    part["key"] = f"standings:{state['run']}:{position}:{digest[:24]}" + (
                        f":repair:{part['repair']}" if part.get("repair") else ""
                    )
                    snapshot, publication = copy.deepcopy(state), copy.deepcopy(part)
            if surplus:
                if surplus["message"]:
                    result = await self.actions.request(
                        job_id=job,
                        kind="delete_message",
                        guild_id=guild,
                        channel_id=surplus["channel"],
                        message_id=surplus["message"],
                        idempotency_key=f"standings-remove:{snapshot['run']}:{surplus['message']}",
                    )
                    if not result.ok:
                        return
                async with self.store.transaction(guild) as state:
                    state["standings_parts"].pop()
                continue
            try:
                result = await self.actions.request(
                    job_id=job,
                    kind="edit_message" if publication["message"] else "send_message",
                    guild_id=guild,
                    channel_id=publication["channel"],
                    message_id=publication["message"] or None,
                    idempotency_key=publication["key"],
                    intent=DiscordIntent(
                        kind="edit" if publication["message"] else "send",
                        presentation_mode="legacy",
                        pages=group,
                        attachments=(await self._prize_attachment(snapshot),)
                        if position == 0
                        and snapshot.get("main_prize")
                        and snapshot["stage"] != "finished"
                        else (),
                    ),
                )
            except Exception:
                result = None
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation or state["run"] != snapshot["run"]:
                    return
                part = state["standings_parts"][position]
                if result is None or not result.ok:
                    part["status"] = "ambiguous" if result is None else "failed"
                    if publication["message"] and result and result.data.get("deleted") is True:
                        part["message_deleted"] = True
                    state["paused"] = True
                    return
                part.update(
                    message=publication["message"] or int(result.data["message_id"]),
                    hash=digest,
                    status="applied",
                )
            await self._cleanup_event_ui(guild, job)

    async def _cleanup_event_ui(self, guild: int, job: str) -> None:

        for _ in range(105):
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation:
                    return
                portal = state.get("contribution_portal") or {}
                board = state.get("standings_parts", [])
                contribution_visible = portal.get("status") == "applied"
                board_visible = bool(board) and all(part["status"] == "applied" for part in board)
                if not (contribution_visible or board_visible):
                    return
                target = next(
                    (
                        part
                        for part in state.get("setup_messages", [])
                        if part["status"] == "pending"
                        and (
                            board_visible
                            if part.get("retire_after") == "running"
                            else contribution_visible or board_visible
                        )
                    ),
                    None,
                )
                prompt = state.get("prize_prompt")
                if (
                    target is None
                    and prompt
                    and prompt["status"] == "applied"
                    and prompt.get("message")
                    and (board_visible or prompt.get("retire_with_setup", True))
                ):
                    target = prompt
                if target is None and state["stage"] in {"running", "finished"} and board_visible:
                    target = next(
                        (
                            part
                            for part in state.get("contribution_panels", [])
                            if part["status"] not in {"removed", "deleted", "replaced"}
                            and not any(
                                saved["message"] == part["message"]
                                and saved["channel"] == part["channel"]
                                for saved in board
                            )
                        ),
                        None,
                    )
                if target is None:
                    return
                run, publication = state["run"], copy.deepcopy(target)
            try:
                result = await self.actions.request(
                    job_id=job,
                    kind="delete_message",
                    guild_id=guild,
                    channel_id=publication["channel"],
                    message_id=publication["message"],
                    idempotency_key=f"event-ui-remove:{run}:{publication['message']}:{job}",
                )
            except Exception:
                return
            if not result.ok and not result.data.get("deleted"):
                return
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation or state["run"] != run:
                    return
                candidates = [
                    *state.get("setup_messages", []),
                    *state.get("contribution_panels", []),
                ]
                if state.get("prize_prompt"):
                    candidates.append(state["prize_prompt"])
                for part in candidates:
                    if (
                        part["message"] == publication["message"]
                        and part["channel"] == publication["channel"]
                    ):
                        part["status"] = "removed"

    async def _publish_contribution_portal(self, guild: int, job: str) -> None:
        async with self.store.transaction(guild) as state:
            portal = state.get("contribution_portal")
            if (
                state.get("generation") != self.generation
                or not portal
                or portal["status"] != "pending"
                or state["stage"] not in {"contributions", "tally", "ready"}
            ):
                return
            portal["status"] = "sending"
            snapshot = copy.deepcopy(state)
        try:
            result = await self.actions.request(
                job_id=job,
                kind="send_message",
                guild_id=guild,
                channel_id=portal["channel"],
                idempotency_key=portal["key"],
                intent=await self._contribution_intent(snapshot, "send"),
            )
        except Exception:
            result = None
        async with self.store.transaction(guild) as state:
            if state.get("generation") != self.generation or state["run"] != snapshot["run"]:
                return
            portal = state["contribution_portal"]
            if result and result.ok:
                portal.update(message=int(result.data["message_id"]), status="applied")
                state.setdefault("contribution_panels", []).append(
                    {
                        "channel": portal["channel"],
                        "message": portal["message"],
                        "hash": self._contribution_digest(snapshot),
                        "status": "applied",
                    }
                )
            else:
                portal["status"] = (
                    "ambiguous" if result is None or result.error_code == "ambiguous" else "failed"
                )

    async def _repair_contribution_portal(self, guild: int, job: str) -> None:
        async with self.store.transaction(guild) as state:
            snapshot = copy.deepcopy(state)
        portal = snapshot.get("contribution_portal")
        if not portal or portal["status"] not in {"sending", "ambiguous", "failed"}:
            return
        message = await self._recover_sent_message(portal["key"], portal["channel"])
        async with self.store.transaction(guild) as state:
            if state.get("generation") != self.generation or state["run"] != snapshot["run"]:
                return
            state["contribution_portal"].update(message=message, status="applied")
            if not any(part["message"] == message for part in state["contribution_panels"]):
                state["contribution_panels"].append(
                    {
                        "channel": portal["channel"],
                        "message": message,
                        "hash": "",
                        "status": "pending",
                    }
                )

    async def _publish_contribution_panels(self, guild: int, job: str) -> None:
        for _ in range(5):
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation:
                    return
                if state["stage"] in {"running", "finished"}:
                    return
                digest = self._contribution_digest(state)
                target = next(
                    (
                        part
                        for part in state.get("contribution_panels", [])
                        if part["status"] not in {"deleted", "removed", "replaced"}
                        and (part["status"] == "pending" or part["hash"] != digest)
                        and part["status"] != "failed"
                    ),
                    None,
                )
                if target is None:
                    return
                snapshot, publication = copy.deepcopy(state), copy.deepcopy(target)
            try:
                result = await self.actions.request(
                    job_id=job,
                    kind="edit_message",
                    guild_id=guild,
                    channel_id=publication["channel"],
                    message_id=publication["message"],
                    idempotency_key=f"contribution-edit:{job}:{publication['message']}:{digest[:16]}",
                    intent=await self._contribution_intent(snapshot, "edit"),
                )
            except Exception:
                result = None
            async with self.store.transaction(guild) as state:
                if state.get("generation") != self.generation or state["run"] != snapshot["run"]:
                    return
                part = next(
                    part
                    for part in state["contribution_panels"]
                    if part["message"] == publication["message"]
                    and part["channel"] == publication["channel"]
                )
                if result and result.ok:
                    part.update(hash=digest, status="applied")
                else:
                    part["status"] = (
                        "deleted" if result and result.data.get("deleted") else "failed"
                    )

    async def _repair_standings(self, guild: int, job: str) -> None:
        async with self.store.transaction(guild) as state:
            snapshot = copy.deepcopy(state)
        for position, part in enumerate(snapshot.get("standings_parts", [])):
            if part["status"] not in {"sending", "ambiguous", "failed"}:
                continue
            message = await self._repair_publication_message(
                guild, job, part,
                marker=f"Event {snapshot['run']} · Standings {position}",
                key=f"standings-repair:{job}:{position}",
                error="Could not confirm the points-list send.",
                fallback_channel=snapshot["config"]["channel"],
            )
            async with self.store.transaction(guild) as state:
                current_parts = state["standings_parts"]
                if state["run"] != snapshot["run"] or position >= len(current_parts):
                    return
                current = current_parts[position]
                if current != part:
                    continue
                current.pop("message_deleted", None)
                current["message"] = message
                current["status"] = (
                    "applied"
                    if state["stage"] == "finished" and not current["message"]
                    else "pending"
                )


                current["hash"] = ""
                current["repair"] = job

    async def _repair_publication_message(
        self, guild: int, job: str, post: dict, *, marker: str, key: str, error: str,
        fallback_channel: int | None = None,
    ) -> int:
        if post["message"]:

            return 0 if post.get("message_deleted") is True else post["message"]
        channel = post.get("channel", fallback_channel)
        existing = await self._confirmed_send(post.get("key"), channel)
        if existing:
            return existing
        if post.get("markerless"):
            raise engine.EventError("Could not confirm the send. Inspect the destination channel.")
        message = await observe_publication(
            self.actions, job=job, guild=guild, channel=channel, marker=marker,
            attempt=key, now_ms=self.clock(),
        )
        if message is None:
            raise engine.EventError(error)
        return message

    async def _confirmed_send(self, key: str | None, channel: int | None) -> int | None:
        if not key or not snowflake(channel):
            return None
        raw = await self.queue.get(f"lumi:edge-action-receipt:{MODULE}:{key}")
        try:
            receipt = decode(raw, DiscordActionResult) if raw else None
        except ValueError:
            return None
        if (
            receipt and receipt.ok and receipt.status == "sent"
            and receipt.data.get("channel_id") == channel
            and snowflake(receipt.data.get("message_id"))
        ):
            return receipt.data["message_id"]
        return None

    async def _recover_sent_message(self, key: str, channel: int) -> int:
        message = await self._confirmed_send(key, channel)
        if message is None:

            raise engine.EventError("Could not confirm the send. Inspect the destination channel.")
        return message

    async def _sync(self, guild: int, state: dict, *, handover=False) -> None:
        if state.get("generation") != self.generation:
            return
        gate = getattr(self, "handover_gate", None)
        if gate and gate.critical(guild) and not handover:
            return
        keys = ActivityAggregator.keys(MODULE, guild)
        enabled = (
            state["stage"] == "running"
            and not state["paused"]
            and not state["drop"]
            and not (gate and gate.busy(guild))
        )
        control = {
            "enabled": enabled,
            "token": state["run"],
            "not_before": state["last_settled"] + engine.drop_gap_ms(state),
        }
        previous = await self.queue.get(keys[0])
        previous_control = json.loads(previous) if previous else None
        pipeline = self.queue.pipeline(transaction=True)
        if previous_control is None or any(
            previous_control.get(field) != control[field] for field in ("not_before", "token")
        ):
            pipeline.delete(*keys[1:])
        pipeline.set(keys[0], json.dumps(control), ex=7 * 86400)
        ingress = admission_key("community", MODULE, guild)
        if enabled:
            pipeline.set(ingress, f"{self.generation}:{state['run']}", ex=60)
        else:
            pipeline.delete(ingress)
        drop = state["drop"]
        if drop and drop["message"] and drop["status"] == "open":
            await self._receipt_context(guild, state)
        elif drop and drop["message"]:
            pipeline.delete(f"{ComponentReceipts.key(MODULE, drop['message'])}:context")
        await pipeline.execute()
        deadlines = []
        if (state.get("submissions_dm") or {}).get("status") == "pending":
            deadlines.append(self.clock() + 1000)
        if (state.get("contribution_portal") or {}).get("status") == "pending":
            deadlines.append(self.clock() + 1000)
        if any(part["status"] == "pending" for part in state.get("setup_messages", [])) and (
            state["stage"] in {"running", "finished"}
            or (state.get("contribution_portal") or {}).get("status") == "applied"
        ):
            deadlines.append(self.clock() + 10_000)
        projection = self._standings(state)
        parts = state.get("standings_parts", [])
        if (
            projection
            and state["stage"] in {"running", "finished"}
            and engine.event_channel(state)
            and (state["stage"] != "finished" or any(part["message"] for part in parts))
            and not any(part["status"] in {"sending", "ambiguous", "failed"} for part in parts)
            and (
                len(parts) < len(projection)
                or len(parts) > len(projection)
                or any(part["status"] == "pending" for part in parts)
                or any(
                    part["hash"] != hashlib.sha256(encode(pages)).hexdigest()
                    for part, pages in zip(parts, projection, strict=False)
                )
            )
        ):
            deadlines.append(self.clock() + 1000)
        if state["stage"] == "contributions":
            deadlines.append(state["contribution_end"])
        if (engine.automatic(state) and state["stage"] in {"tally", "ready"}
                and not state["paused"]):
            deadlines.append(engine.planned_start(state))
        contribution_digest = self._contribution_digest(state)
        if any(
            part["status"] not in {"deleted", "failed"}
            and (part["status"] == "pending" or part["hash"] != contribution_digest)
            for part in state.get("contribution_panels", [])
        ):
            deadlines.append(self.clock() + 1000)
        if state["stage"] == "running":
            deadlines.append(state["finish_at"])
        if drop:
            if drop["status"] == "warning" and not state["paused"]:
                deadlines.append(drop["reveal_at"])
            elif drop["status"] == "open":
                deadlines.append(
                    min(drop["close_at"], drop["settle_at"])
                    if self.clock() < drop["close_at"]
                    else drop["settle_at"]
                )
            elif state["outbox"] and any(item["status"] == "pending" for item in state["outbox"]):
                deadlines.append(self.clock() + 1000)
        if deadlines:
            due = max(self.clock() + 100, min(deadlines))
            await self.scheduler.publish(
                operation="upsert",
                schedule_key=f"halloween:{guild}:tick",
                route="halloween.tick",
                revision=await self.queue.incr(f"lumi:events:schedule_revision:{guild}"),
                guild_id=guild,
                channel_id=(drop or {}).get("channel") or engine.event_channel(state) or None,
                actor_id=0,
                due_at_ms=due,
                expires_at_ms=due + 300_000,
                arguments={"run": state["run"]},
            )

    async def _repair(self, guild: int, job: str) -> None:
        async with self.store.transaction(guild) as state:
            snapshot = copy.deepcopy(state)
        drop = snapshot["drop"]
        if not drop:
            return
        if not drop["message"]:
            warning = next((item for item in snapshot["outbox"] if item["kind"] == "warning"), None)
            message = await self._confirmed_send(warning["id"], drop["channel"]) if warning else None
            if message is None:
                if warning and warning.get("markerless"):
                    raise engine.EventError(
                        "Could not confirm the send. Inspect the destination channel."
                    )
                now = self.clock()
                message = await observe_publication(
                    self.actions, job=job, guild=guild, channel=drop["channel"],
                    marker=f"Event {snapshot['run']} · Drop {drop['id']}",
                    attempt=f"repair:{job}", now_ms=now,
                    window_ms=max(60, (now - drop["created"]) // 1000 + 60) * 1000,
                )
                if message is None:
                    raise engine.EventError(
                        "Could not confirm the send. Inspect the destination channel."
                    )
            async with self.store.transaction(guild) as state:
                current = state["drop"]
                if state["run"] != snapshot["run"] or not current or any(
                    current[key] != drop[key] for key in ("id", "channel", "message")
                ):
                    return
                if message:
                    current["message"] = int(message)
                    if current["status"] != "cancelled":
                        current["status"] = "warning"
                        current["reveal_at"] = self.clock() + current["wait"]
                else:
                    engine.refund(state, current)
                    state["drop"] = None
                state["outbox"] = []
                if state["drop"] and state["drop"]["status"] == "cancelled":
                    self._enqueue(state, "settled")
            await self._flush(guild, job)
        else:


            async with self.store.transaction(guild) as state:
                current = state["drop"]
                if state["run"] != snapshot["run"] or not current or any(
                    current[key] != drop[key] for key in ("id", "channel", "message")
                ):
                    return
                if current.get("message_deleted") is True:
                    self._drop_message_deleted(state)
                else:
                    for item in state["outbox"]:
                        if item["status"] != "pending":
                            item["status"] = "pending"
                            item["id"] = f"repair:{job}:{item['kind']}"
            await self._flush(guild, job)
        async with self.store.transaction(guild) as state:
            repaired = copy.deepcopy(state)
        await self._sync(guild, repaired)

    async def _defaults(self, session: dict, action: str, values: dict) -> None:
        payload = f"{session['id']}~x"
        await self.cache.set(
            f"lumi:modal-context:{MODULE}:{payload}:{action}",
            json.dumps(
                {
                    "owner": session["owner"],
                    "guild": session["guild"],
                    "generation": self.generation,
                    "values": values,
                }
            ),
            px=max(1, session["expires"] - self.clock()),
        )

    async def _dashboard(self, session: dict, state: dict, admin: bool, kind: str) -> DiscordIntent:
        if state["stage"] == "setup" and admin and session["screen"] == "settings":
            session["screen"] = "setup"
        if session["screen"] in {"setup", "setup_complete"}:
            return await self._setup_dashboard(session, state, kind)
        screen, page, owner = session["screen"], session["page"], session["owner"]
        presentation_mode = session.get("presentation_mode") or (
            "legacy" if screen == "overview" else "v2"
        )
        session["presentation_mode"] = presentation_mode
        if screen == "overview" and presentation_mode == "legacy":
            if state["stage"] in {"running", "finished"} and self._standings(state):
                return DiscordIntent(
                    kind=kind,
                    presentation_mode="legacy",
                    pages=self._standings(state)[0],
                    attachments=(await self._prize_attachment(state),)
                    if state.get("main_prize") and state["stage"] != "finished"
                    else (),
                )
            return await self._contribution_intent(state, kind)
        controls = []

        def button(action, label, disabled=False):
            controls.append(
                PresentationControl(
                    "button",
                    "halloween",
                    action,
                    f"{session['id']}~x",
                    label=label,
                    disabled=disabled,
                )
            )

        stage = state["stage"]
        text = [f"## Halloween · {stage.replace('_', ' ').title()}"]
        if state["paused"]:
            text.append("Paused")
        if stage == "contributions":
            text.append(f"Contributions close <t:{state['contribution_end'] // 1000}:R>")
        elif stage == "running":
            text.append(f"Finishes <t:{state['finish_at'] // 1000}:R>")
        if screen == "overview":
            text = []
            if stage == "contributions":
                text.append(f"Contributions close <t:{state['contribution_end'] // 1000}:R>")
            counts = engine.contribution_counts(state)[:10]
            text += [
                safe(state["config"]["instructions"]),
                *(f"<@{user}> has added {count} cards" for user, count in counts),
            ]
            button("settings", "Settings")
        elif screen == "settings":
            text[0] = "## Halloween"
            button("leaderboard", "Leaderboard")
            if admin:
                button("config", "Setup", stage != "setup")
                button(
                    "main_prize",
                    "Main Prize",
                    stage not in {"setup", "contributions", "tally", "ready"},
                )
                lookup = state.get("prize_lookup") or {}
                if lookup.get("status") == "needs_cleanup":
                    text.append(
                        "Main prize saved. Lookup cleanup needs Manage Messages permission."
                    )
                button("review", "Contributions / Tally")
                button("pools", "Prize Pools")
                button("handoffs", "Handoffs")
                button(
                    "start",
                    "Open / Resume Contributions",
                    stage not in {"setup", "contributions"}
                    or (stage == "contributions" and not state["paused"]),
                )
                if engine.automatic(state) and stage in {"contributions", "tally", "ready"}:
                    text.append(f"Drops start <t:{engine.planned_start(state) // 1000}:F>")
                elif stage in {"ready", "running"} and not engine.automatic(state):
                    text.append(
                        f"Start chat drops: {state['config'].get('prefix', '.')}"
                        "events halloween start"
                    )
                button("stop", "Stop", stage not in {"contributions", "tally", "ready", "running"} or state["paused"])
                button("finish", "Finish", stage == "finished")
                button("new", "New Event", stage != "finished" or bool(state["drop"]))
                needs_repair = bool(state["outbox"]) or any(
                    part["status"] in {"sending", "ambiguous", "failed"}
                    for part in state.get("standings_parts", [])
                )
                needs_repair = needs_repair or (state.get("prize_prompt") or {}).get("status") in {
                    "sending",
                    "ambiguous",
                    "failed",
                }
                needs_repair = needs_repair or any(
                    part["status"] == "failed" for part in state.get("contribution_panels", [])
                )
                needs_repair = needs_repair or (state.get("contribution_portal") or {}).get(
                    "status"
                ) in {"sending", "ambiguous", "failed"}
                button("repair", "Repair", not needs_repair)
                text.append(
                    "Contribution time limit: "
                    + engine.duration_label(state["config"].get("contribution_ms", engine.DAY))
                )
                configured_drop = (
                    state["config"].get("drop_ms") or state["config"].get("hours", 0) * 3_600_000
                )
                text.append(
                    "Drop duration: "
                    + (engine.duration_label(configured_drop) if configured_drop else "Not set")
                )
                controls.append(
                    PresentationControl(
                        "channel_select",
                        "halloween",
                        "channel",
                        f"{session['id']}~x",
                        placeholder="Drop destination",
                        channel_types=(0, 5, 11, 12),
                        default_ids=(state["config"]["channel"],)
                        if state["config"]["channel"]
                        else (),
                    )
                )
                await self._defaults(
                    session,
                    "config",
                    {
                        "contribution_time": engine.duration_label(
                            state["config"].get("contribution_ms", engine.DAY)
                        ),
                        "drop_time": engine.duration_label(configured_drop)
                        if configured_drop
                        else "",
                        **{
                            key: str(value)
                            for key, value in state["config"].items()
                            if key in {"points", "instructions"}
                        },
                    },
                )
        elif screen == "new_confirm":
            text.append(
                "Archive these standings and start a new contribution period? Handoffs remain."
            )
            button("new_confirm", "Confirm New Event")
        elif screen == "pools":
            pools = list(state["pools"].items())
            page = min(page, max(0, (len(pools) - 1) // 20))
            visible = pools[page * 20 : (page + 1) * 20]
            text += [
                f"{safe(pool['name'])} · Available {engine.pool_available(pool)}"
                f" · Reserved {pool['reserved']}"
                f" · Awarded {pool['awarded']} · Number per drop {pool['cap']}"
                + ("" if engine.paced(state) else f" · Weight {pool['weight']}")
                for _, pool in visible
            ]
            if engine.paced(state):
                text.append("Automatic pacing · unclaimed stock stays in the pool.")
                if stage == "running":
                    for _, pool in visible:
                        if not engine.pool_available(pool):
                            continue
                        plan = engine.pool_pacing(state, pool, self.clock())
                        timing = (
                            "Rare · available throughout"
                            if plan.get("rare")
                            else (
                                "Ready"
                                if plan["ready"]
                                else f"Next release <t:{plan['next_at'] // 1000}:R>"
                            )
                        )
                        text.append(
                            f"{safe(pool['name'])} · {timing} · {plan['allowance']} left this hour"
                        )
            else:
                weight = sum(pool["weight"] for _, pool in pools if engine.pool_available(pool))
                text += [
                    f"{safe(pool['name'])}: "
                    f"{engine.PRIZE_CHANCE_PERCENT * pool['weight'] / weight:.2f}% per fourth button"
                    for _, pool in visible
                    if engine.pool_available(pool) and weight
                ]
            controls.append(
                PresentationControl(
                    "string_select",
                    "halloween",
                    "pick_pool",
                    f"{session['id']}~x",
                    placeholder="Choose a pool",
                    options=(
                        PresentationOption("New Pool", "new"),
                        *(PresentationOption(pool["name"][:100], key) for key, pool in visible),
                    ),
                )
            )
            button("pool", "Add / Edit Pool")
            button("add_stock", "Add Stock", session["selected"] == "new" or stage == "finished")
            button("pool_remove", "Remove Pool", session["selected"] == "new")
            selected = state["pools"].get(session["selected"])
            values = (
                {
                    "type": selected["type"],
                    "name": ", ".join(selected["codes"])
                    if selected["type"] == "cards"
                    else ", ".join(selected["items"]) if "items" in selected else selected["name"],
                    "stock": str(engine.pool_available(selected)),
                    "cap": str(selected["cap"]),
                    "weight": "" if engine.paced(state) else str(selected["weight"]),
                }
                if selected
                else {"type": "tickets", "stock": "1", "cap": "1", "weight": "", "name": ""}
            )
            await self._defaults(session, "pool", values)
            await self._defaults(session, "add_stock", {"amount": "", "codes": ""})
        elif screen == "review":
            ranked = state["ranking"]
            awards = engine.allocations(state["config"]["points"], ranked)
            page = min(page, max(0, (max(len(ranked), len(state["contributions"])) - 1) // 20))
            if ranked:
                text += [
                    f"{index + 1}. <@{user}> · {awards[str(user)]} points"
                    f" · {100 * awards[str(user)] / max(1, state['config']['points']):.3f}%"
                    for index, user in enumerate(ranked)
                    if page * 20 <= index < (page + 1) * 20
                ]
            text.append("### Submissions")
            for user, count in engine.contribution_counts(state)[page * 20 : (page + 1) * 20]:
                text.append(f"<@{user}> has added {count} cards")
            if (state.get("submissions_dm") or {}).get("status") in {
                "failed",
                "ambiguous",
                "sending",
            }:
                text.append("Private DM delivery needs attention. Download submissions here.")
            if owner == state.get("organizer_id"):
                button("tally", "Enter Person's Tally", not engine.tally_open(state))
            button("confirm", "Confirm Participants & Points", not engine.tally_open(state) or not ranked)
            if owner == state.get("organizer_id"):
                button("export", "Download Submissions")
            await self._defaults(session, "ranking", {"operation": "replace", "people": ""})
        elif screen == "handoffs":
            pending = [
                (key, item) for key, item in state["handoffs"].items() if not item["delivered"]
            ]
            page = min(page, max(0, (len(pending) - 1) // 20))
            visible = pending[page * 20 : (page + 1) * 20]
            text += [
                f"<@{item['actor']}> · {safe(engine.prize_label(item))}" for _, item in visible
            ]
            if visible:
                controls.append(
                    PresentationControl(
                        "string_select",
                        "halloween",
                        "pick_handoff",
                        f"{session['id']}~x",
                        placeholder="Choose a handoff",
                        options=tuple(
                            PresentationOption(
                                f"{item['actor']} · {engine.prize_label(item)}"[:100], key
                            )
                            for key, item in visible
                        ),
                    )
                )
                button("delivered", "Mark Delivered")
        elif screen == "leaderboard":
            text = ["## Points"]
            entries = sorted(state["scores"].items(), key=lambda pair: (-pair[1], int(pair[0])))
            page = min(page, max(0, (len(entries) - 1) // 20))
            rank, previous = 0, None
            for index, (user, score) in enumerate(entries):
                if score != previous:
                    rank = index + 1
                previous = score
                if page * 20 <= index < (page + 1) * 20:
                    text.append(f"{rank}. <@{user}> · {score} points")
        if screen not in {"overview", "settings", "new_confirm"} and (
            screen != "leaderboard" or len(entries) > 20
        ):
            button("lb_previous" if screen == "leaderboard" else "previous", "Previous", page == 0)
            button(
                "lb_next" if screen == "leaderboard" else "next",
                "Next",
                screen == "leaderboard" and (page + 1) * 20 >= len(entries),
            )
        if screen not in {"overview", "settings", "leaderboard"}:
            button("overview", "Back")
        if screen != "overview" and (
            state["outbox"]
            or any(
                part["status"] in {"sending", "ambiguous", "failed"}
                for part in state.get("standings_parts", [])
            )
        ):
            text.append("Needs repair · prize reservations remain protected.")
        session["page"] = page
        pages = [
            PresentationPage(
                description="\n".join(line for line in text if line)[:3900],
                controls=tuple(controls) if presentation_mode == "v2" else (),
            )
        ]
        attachments = ()
        if screen == "overview":
            pages[:0] = [
                PresentationPage(title="About the event", description=event_about(state)),
                PresentationPage(
                    title="About contributions",
                    description=(
                        f"{state['config']['points']:,} starting points are available.\n\n"
                        + CONTRIBUTIONS_ABOUT
                    ),
                ),
            ]
            if state.get("main_prize") and stage != "finished":
                pages.insert(
                    0,
                    PresentationPage(
                        title="Main event prize", file_name=state["main_prize"]["file_name"]
                    ),
                )
                attachments = (await self._prize_attachment(state),)
        return DiscordIntent(
            kind=kind,
            session_id=session["id"],
            presentation_mode=presentation_mode,
            pages=tuple(pages),
            controls=tuple(controls) if presentation_mode == "legacy" else (),
            attachments=attachments,
        )

    async def _setup_dashboard(self, session: dict, state: dict, kind: str) -> DiscordIntent:
        session["presentation_mode"] = "v2"
        payload, controls = f"{session['id']}~x", []

        def button(action, label):
            controls.append(
                PresentationControl("button", "halloween", action, payload, label=label)
            )

        config = state["config"]
        step = state.get("setup_step", "details")
        session["setup_step"] = step
        if session["screen"] == "setup_complete":
            title, text = "Contributions", "Contributions are open."
            if (state.get("contribution_portal") or {}).get("status") != "applied":
                text = "Contributions are open. The public panel needs repair."
                button("repair", "Repair")
        elif step == "details":
            title, text = "Event details", "Set the starting points and time limits."
            button("config", "Set Details")
            drop_ms = config.get("drop_ms") or config.get("hours", 0) * 3_600_000
            await self._defaults(
                session,
                "config",
                {
                    "points": str(config["points"]) if config["points"] else "",
                    "drop_time": engine.duration_label(drop_ms) if drop_ms else "",
                    "contribution_time": engine.duration_label(
                        config.get("contribution_ms", engine.DAY)
                    ),
                    "instructions": config["instructions"],
                },
            )
        elif step == "channel":
            title, text = "Drop channel", "Choose where random drops will appear."
            controls.append(
                PresentationControl(
                    "channel_select",
                    "halloween",
                    "channel",
                    payload,
                    placeholder="Drop channel",
                    channel_types=(0, 5, 11, 12),
                    default_ids=(config["channel"],) if config["channel"] else (),
                )
            )
        elif step == "prize":
            title, text = (
                "Main prize",
                "Reply to the Main event prize message with kv <card code>, then continue.",
            )
            if state.get("main_prize"):
                text = "Main prize saved."
            else:
                text += "\nOptional."
            button("wizard_next", "Continue")
        elif step == "pools":
            title, text = "Prize pools", "Add the prizes for random drops, then continue."
            if state["pools"]:
                text += "\n\n" + "\n".join(
                    f"{safe(pool['name']) if pool['name'] else pool['type'].title()}"
                    f" · {engine.pool_available(pool)}"
                    for pool in list(state["pools"].values())[:10]
                )
            button("pool", "Add Prize")
            button("wizard_next", "Continue")
            await self._defaults(
                session,
                "pool",
                {"type": "tickets", "stock": "1", "cap": "1", "weight": "", "name": ""},
            )
        else:
            title = "Ready to open contributions"
            contribution_time = engine.duration_label(config.get("contribution_ms", engine.DAY))
            drop_time = engine.duration_label(
                config.get("drop_ms") or config.get("hours", 0) * 3_600_000
            )
            text = (
                f"Starting points: {config['points']:,}\n"
                f"Contributions: {contribution_time}\n"
                f"Drop stage: {drop_time}\n"
                f"Drop channel: <#{config['channel']}>\n"
                f"Prize pools: {len(state['pools'])}\n\n"
                + ("Chat drops start automatically one day after contributions close."
                   if engine.automatic(state) else "Chat drops start later when you run the start command.")
            )
            button("start", "Open Contributions")
        if session["screen"] != "setup_complete" and step != "details":
            button("wizard_back", "Back")
        return DiscordIntent(
            kind=kind,
            session_id=session["id"],
            presentation_mode="v2",
            pages=(PresentationPage(title=title, description=text, controls=tuple(controls)),),
        )

    @staticmethod
    def _contribution_pages(state: dict) -> tuple[PresentationPage, ...]:
        text = []
        if state["stage"] == "contributions":
            text.append(f"Contributions close <t:{state['contribution_end'] // 1000}:R>")
        text.extend(
            [
                safe(state["config"]["instructions"]),
                *(
                    f"<@{user}> has added {count} cards"
                    for user, count in engine.contribution_counts(state)[:10]
                ),
            ]
        )
        pages = [
            PresentationPage(title="About the event", description=event_about(state)),
            PresentationPage(
                title="About contributions",
                description=f"{state['config']['points']:,} starting points are available.\n\n"
                + CONTRIBUTIONS_ABOUT,
            ),
            PresentationPage(
                description="\n".join(line for line in text if line) or "Contributions are closed."
            ),
        ]
        if state.get("main_prize") and state["stage"] != "finished":
            pages.insert(
                0,
                PresentationPage(
                    title="Main event prize", file_name=state["main_prize"]["file_name"]
                ),
            )
        return tuple(pages)

    @staticmethod
    def _contribution_controls(state: dict) -> tuple[PresentationControl, ...]:
        return tuple(
            PresentationControl(
                "button",
                "halloween",
                action,
                state["run"],
                label=label,
                disabled=state["stage"] != "contributions" or state["paused"],
            )
            for action, label in (("submit_cards", "Add Cards"), ("withdraw_cards", "Remove Cards"))
        )

    def _contribution_digest(self, state: dict) -> str:
        return hashlib.sha256(
            encode(
                (
                    self._contribution_pages(state),
                    self._contribution_controls(state),
                    (state.get("main_prize") or {}).get("sha256")
                    if state["stage"] != "finished"
                    else None,
                )
            )
        ).hexdigest()

    async def _contribution_intent(self, state: dict, kind: str) -> DiscordIntent:
        return DiscordIntent(
            kind=kind,
            presentation_mode="legacy",
            pages=self._contribution_pages(state),
            controls=self._contribution_controls(state),
            public_controls=True,
            attachments=(await self._prize_attachment(state),)
            if state.get("main_prize") and state["stage"] != "finished"
            else (),
        )

    async def _export(self, state: dict) -> DiscordIntent:
        output = io.StringIO(newline="")
        writer = csv.writer(output)
        writer.writerow(("user_id", "card_code", "duplicate_submission"))
        owners = {}
        for user, entries in state["contributions"].items():
            for code in entries:
                owners.setdefault(code, set()).add(user)
        for user, entries in state["contributions"].items():
            for code in entries:
                writer.writerow((user, code, len(owners[code]) > 1))
        data = output.getvalue().encode("utf-8")
        identifier = secrets.token_hex(16)
        self.temp_root.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread((self.temp_root / identifier).write_bytes, data)
        return DiscordIntent(
            kind="ephemeral",
            presentation_mode="plain",
            attachments=(
                TemporaryAttachment(
                    f"tmp:{identifier}",
                    "halloween-contributions.csv",
                    hashlib.sha256(data).hexdigest(),
                    len(data),
                    self.clock() + 900_000,
                ),
            ),
        )
