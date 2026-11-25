from __future__ import annotations

from lumi.ui_keys import ui_key

import asyncio
import contextlib
import hashlib
import json
import logging
import random
import re
import secrets
import time
from pathlib import Path

from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts import (
    CaptureSpec,
    ChannelWatchSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    MatcherExpr,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
    ScheduledCallbackSpec,
    TemporaryAttachment,
)
from lumi.karuta_collections.artifacts import ArtifactStore
from lumi.worker.actions import ActionUnavailable, EdgeActionClient
from lumi.worker.scheduler import ScheduledCallbackClient

from .dispatcher import GamesDispatcher
from .filters import ClueRenderer
from .kguess import (
    ROUNDS,
    TIMERS,
    GameError,
    answer_key,
    choose,
    mask_answer,
    matches,
    next_effect,
    parse_setup,
    parse_wishlist,
    points,
    pool_size,
    reveal_order,
    validate_settings,
)
from .store import TERMINAL

LOGGER = logging.getLogger(__name__)
MODULE = "games"
PREFIX = "lumi:games:"
UI_MS = 900_000
ROUTE = "games.kguess_messages"
SAFE_TOKEN = re.compile(r"^[a-f0-9]{16}$")


def safe(value):
    return re.sub(r"([\\`*_~|\[\]()>#])", r"\\\1", str(value).replace("@", "@\u200b"))


class GamesModule:
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
        scheduler=None,
        renderer=None,
        clock=None,
    ):
        self.generation, self.queue, self.cache, self.store = (
            generation,
            queue_redis,
            cache_redis,
            store,
        )
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.actions = actions or EdgeActionClient(
            queue_redis, module=MODULE, group="cards", generation=generation
        )
        self.scheduler = scheduler or ScheduledCallbackClient(
            queue_redis, module=MODULE, group="cards", generation=generation
        )
        self.temp_root = Path(temp_root)
        self.renderer = renderer or ClueRenderer(
            ArtifactStore(Path(artifact_root)), Path(artifact_root) / "game-clues"
        )
        self.draining = False
        self.maintenance = None
        self._maintenance_busy = False
        self.dispatcher = GamesDispatcher()
        self.dispatcher.register("kguess", "Karuta guessing", self.kguess_command)

    @property
    def manifest(self):
        def component(action, kind="none", modal=None):
            return ComponentSpec(
                MODULE,
                action,
                "games.component",
                r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$",
                captures=(CaptureSpec("session"), CaptureSpec("revision", "integer", minimum=0)),
                input_kind=kind,
                modal=modal,
                modal_defaults=modal is not None,
                acl_key="games",
                public=True,
                session_bound=True,
                deadline_ms=120_000,
            )

        return Manifest(
            2,
            MODULE,
            "cards",
            self.generation,
            "lumi:jobs:cards",
            display_name="Games",
            summary="Channel games and server leaderboards.",
            version="1.1.2",
            commands=(
                CommandSpec(
                    "games",
                    "games.command",
                    r"^\s*(?P<raw>[^\r\n]{0,256})$",
                    captures=(CaptureSpec("raw", required=False, maximum=256),),
                    acl_key="games",
                    public=True,
                    context_fields=("prefix",),
                    help=HelpSpec(
                        ".games\n.games kguess [WL] [-rounds N] [-time 3m]\n"
                        ".games kguess <stop|lb>\n.games kguess stats [@user|ID]",
                        "Guess Karuta characters or series; WL accepts 2< or <2."
                    ),
                ),
            ),
            components=(
                component("mode", "string_values"),
                component("rounds", "string_values"),
                component("timer", "string_values"),
                component("start"),
                component("close"),
                component("previous"),
                component("next"),
                component(
                    "wl",
                    "modal",
                    ModalSpec(
                        "Wishlist",
                        (
                            ModalFieldSpec(
                                "query",
                                "WL filter",
                                required=False,
                                maximum=128,
                                placeholder="5<<10 or 20>=>2",
                            ),
                        ),
                    ),
                ),
            ),
            passive=(
                PassiveMatcherSpec(
                    ROUTE,
                    "games.channel",
                    "message_create",
                    MatcherExpr("regex", "content", r"^[\s\S]{1,240}$"),
                    channel_scoped=True,
                    acl_key="games",
                ),
            ),
            scheduled=(
                ScheduledCallbackSpec(
                    "games.tick", "games.tick", scope="guild", acl_key="games", deadline_ms=120_000
                ),
            ),
            capabilities=("channel_watch", "send_message", "edit_message"),
        )

    async def start(self):
        self.temp_root.mkdir(parents=True, exist_ok=True)
        await self.store.cleanup(self.clock())
        active = await self.store.active_sessions()
        if isinstance(self.renderer, ClueRenderer):
            self.renderer.protected.update(s["round"]["cache"] for s in active if s.get("round"))
            await self.renderer.cleanup()
        for state in active:
            await self.schedule(state, self.clock() + 1000)
        if self.maintenance is None:
            self.maintenance = asyncio.create_task(self.maintain(), name="games-maintenance")

    async def maintain(self):
        while True:
            await asyncio.sleep(3600)
            if self.draining:
                continue
            self._maintenance_busy = True
            try:
                await self.store.cleanup(self.clock())
                if isinstance(self.renderer, ClueRenderer):
                    await self.renderer.cleanup()
            except (OSError, RedisError, MySQLError):
                LOGGER.exception("games_cleanup_unavailable")
            finally:
                self._maintenance_busy = False

    async def close(self):
        if self.maintenance is not None:
            self.maintenance.cancel()
            await asyncio.gather(self.maintenance, return_exceptions=True)
            self.maintenance = None
        await self.renderer.close()
        await self.store.close()

    def begin_drain(self):
        self.draining = True

    def resume(self):
        self.draining = False

    def drain_ready(self):
        return not self._maintenance_busy

    def current(self, request, generation):
        return generation == self.generation and self.clock() < int(
            request.event.get("_expires_at_ms", self.clock() + 1)
        )

    def result(self, job, intent=None):
        return ResultEnvelope(
            1,
            job,
            MODULE,
            "cards",
            self.generation,
            "ok",
            intents=(intent,) if intent else (),
            completed_at_ms=self.clock(),
        )

    @staticmethod
    def notice(request, text):
        return DiscordIntent(
            "ephemeral" if request.event.get("dispatch") == "component" else "send",
            pages=(PresentationPage(description=text),),
        )

    async def handle(self, request, job_id, generation):
        if not self.current(request, generation) or self.draining:
            return self.result(job_id)
        try:
            if request.handler == "games.command":
                intent = await self.command(request, job_id)
            elif request.handler == "games.component":
                intent = await self.component(request, job_id)
            else:
                guild, channel = (
                    int(request.event.get(k) or 0) for k in ("guild_id", "channel_id")
                )
                state = await self.store.active(guild, channel)
                if state and (
                    request.handler != "games.channel"
                    or request.arguments.get("token") == (state.get("round") or {}).get("id")
                ):
                    if (
                        request.handler != "games.tick"
                        or request.arguments.get("session") == state["id"]
                    ):
                        await self.drive(state["id"], request, job_id)
                intent = None
            return self.result(job_id, intent if self.current(request, generation) else None)
        except GameError as exc:
            return self.result(
                job_id,
                self.notice(request, str(exc))
                if request.handler in {"games.command", "games.component"}
                else None,
            )
        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            RedisError,
            MySQLError,
            ActionUnavailable,
            TimeoutError,
        ):
            LOGGER.exception("games_unavailable", extra={"handler": request.handler})
            if request.handler in {"games.command", "games.component"}:
                return self.result(job_id, self.notice(request, "Unavailable. Try again."))

            state = await self.store.active(
                int(request.event["guild_id"]), int(request.event["channel_id"])
            )
            if state:
                await self.schedule(state, self.clock() + 2000)
            return self.result(job_id)

    async def command(self, request, job):
        return await self.dispatcher.dispatch(request, job)

    async def kguess_command(self, tail, request, job):
        prefix = str(request.event.get("prefix") or ".")
        if tail.lower() == "help":
            return self.notice(
                request,
                f"`{prefix}games kguess 2< -rounds 25 -time 3m`\n`{prefix}games kguess stop`\n"
                f"`{prefix}games kguess lb` · `stats [@user|ID]`",
            )
        if tail.lower() == "stop":
            state = await self.store.active(
                int(request.event["guild_id"]), int(request.event["channel_id"])
            )
            if state is None:
                return self.notice(request, "No active game.")
            if int(request.event["actor_id"]) != state["host"]:
                response = await self.watch(
                    state,
                    job,
                    "authorize_stop",
                    token=(state.get("round") or {}).get("id", state["id"]),
                )
                if not response.ok:
                    raise GameError("Host or Manage Messages required.")
            async with self.run_lock(state):
                state = await self.store.get(state["id"])
                await self.cancel(state, job)
            return self.notice(request, "Stopped.")
        if tail.lower() == "lb":
            return await self.new_ui(request, "lb", "")
        if tail.lower() == "stats" or tail.lower().startswith("stats "):
            query = tail[5:].strip()
            target = int(request.event["actor_id"])
            if query:
                match = re.fullmatch(r"(?:<@!?)?(\d{17,20})>?", query)
                if not match:
                    raise GameError("Use a mention or user ID.")
                target = int(match[1])
            rows = await self.store.stats(int(request.event["guild_id"]), target)
            text = "\n".join(
                f"{mode.title()} · {rows.get(mode, {}).get('points', 0):,} points · "
                f"{rows.get(mode, {}).get('wins', 0):,} wins"
                for mode in ("character", "series")
            )
            return DiscordIntent(
                "send",
                pages=(PresentationPage(title="KGuess", description=f"<@{target}>\n{text}"),),
            )
        options = parse_setup(tail)
        return await self.new_ui(
            request, "setup", options.query, rounds=options.rounds, timer=options.timer
        )

    async def new_ui(self, request, kind, query, *, rounds=5, timer=60):
        validate_settings(rounds, timer)
        ui = dict(
            token=secrets.token_hex(8),
            owner=int(request.event["actor_id"]),
            guild=int(request.event["guild_id"]),
            channel=int(request.event["channel_id"]),
            kind=kind,
            query=query,
            mode="character",
            rounds=rounds,
            timer=timer,
            revision=0,
            page=0,
            expires=self.clock() + UI_MS,
            game_id=secrets.token_hex(16),
        )
        await self.save_ui(ui)
        return await self.ui_intent(ui, "send")

    async def save_ui(self, ui):
        ttl = ui["expires"] - self.clock()
        if ttl <= 0:
            raise GameError("Menu expired.")
        pipe = self.cache.pipeline(transaction=True)
        pipe.set(PREFIX + "ui:" + ui["token"], json.dumps(ui), px=ttl)
        pipe.set(ui_key("owner", ui["token"]), str(ui["owner"]), px=ttl)
        payload = f"{ui['token']}~{ui['revision']}"
        pipe.set(
            f"lumi:modal-context:games:{payload}:wl",
            json.dumps(
                dict(
                    owner=ui["owner"],
                    guild=ui["guild"],
                    generation=self.generation,
                    values={"query": ui["query"]},
                )
            ),
            px=ttl,
        )
        await pipe.execute()

    @staticmethod
    def control(ui, action, label=None, *, options=None, disabled=False):
        return PresentationControl(
            "string_select" if options else "button",
            MODULE,
            action,
            f"{ui['token']}~{ui['revision']}",
            label=label if not options else None,
            placeholder=label if options else None,
            disabled=disabled,
            style="success" if action == "start" else "secondary",
            options=tuple(
                PresentationOption(str(text), str(value), default=value == ui.get(action))
                for value, text in (options or ())
            ),
        )

    async def ui_intent(self, ui, kind):
        controls = [
            self.control(
                ui, "mode", "Mode", options=(("character", "Character"), ("series", "Series"))
            )
        ]
        if ui["kind"] == "lb":
            rows, total = await self.store.leaderboard(ui["guild"], ui["mode"], ui["page"])
            controls += [
                self.control(ui, "previous", "Previous", disabled=ui["page"] == 0),
                self.control(ui, "next", "Next", disabled=(ui["page"] + 1) * 10 >= total),
            ]
            text = (
                "\n".join(
                    f"{r['rank']}. <@{r['user']}> · {r['points']:,} · {r['wins']:,} wins"
                    for r in rows
                )
                or "No scores."
            )
            page = PresentationPage(
                title=f"KGuess · {ui['mode'].title()}", description=text, controls=tuple(controls)
            )
        else:
            bounds = parse_wishlist(ui["query"])
            count = pool_size(await self.store.candidates(bounds), ui["mode"])
            controls += [
                self.control(
                    ui,
                    "rounds",
                    "Rounds",
                    options=tuple(
                        (n, f"{n} round" + ("s" if n != 1 else ""))
                        for n in sorted({*ROUNDS, ui["rounds"]})
                    ),
                ),
                self.control(
                    ui,
                    "timer",
                    "Timer",
                    options=tuple((n, f"{n} seconds") for n in sorted({*TIMERS, ui["timer"]})),
                ),
                self.control(ui, "wl", "WL"),
                self.control(ui, "start", "Start", disabled=count < ui["rounds"]),
                self.control(ui, "close", "Close"),
            ]
            page = PresentationPage(
                title="KGuess",
                description=f"{bounds.label} · {count:,} available",
                controls=tuple(controls),
            )
        return DiscordIntent(kind, pages=(page,), session_id=ui["token"])

    async def component(self, request, job):
        token = str(request.arguments.get("session") or "")
        if not SAFE_TOKEN.fullmatch(token):
            raise GameError("Menu expired.")
        lock = self.cache.lock(PREFIX + "ui-lock:" + token, timeout=120, blocking_timeout=1)
        async with lock:
            raw = await self.cache.get(PREFIX + "ui:" + token)
            ui = json.loads(raw) if raw else None
            bound = await self.cache.get(ui_key("message", token))
            if (
                not ui
                or ui["expires"] <= self.clock()
                or (ui["owner"], ui["guild"], ui["channel"])
                != tuple(
                    int(request.event.get(k) or 0) for k in ("actor_id", "guild_id", "channel_id")
                )
                or not bound
                or int(bound) != int(request.event.get("message_id") or 0)
            ):
                raise GameError("Menu expired.")
            if int(request.arguments.get("revision", -1)) != ui["revision"]:
                raise GameError("Menu updated. Try again.")
            if ui["kind"] not in {"setup", "lb"}:
                raise GameError("Menu closed.")
            action = request.route.rsplit(".", 1)[-1]
            values = request.arguments.get("values")
            if action in {"mode", "rounds", "timer"}:
                if not isinstance(values, (list, tuple)) or len(values) != 1:
                    raise GameError("Invalid selection.")
                value = values[0] if action == "mode" else int(values[0])
                allowed = (
                    ("character", "series")
                    if action == "mode"
                    else (*ROUNDS, ui["rounds"])
                    if action == "rounds"
                    else (*TIMERS, ui["timer"])
                )
                if value not in allowed:
                    raise GameError("Invalid selection.")
                ui[action] = value
                validate_settings(ui["rounds"], ui["timer"])
                ui["page"] = 0
            elif action == "wl":
                if not isinstance(values, dict):
                    raise GameError("Invalid filter.")
                ui["query"] = str(values.get("query") or "")
                parse_wishlist(ui["query"])
            elif action in {"previous", "next"}:
                _, total = await self.store.leaderboard(ui["guild"], ui["mode"], 0)
                ui["page"] = max(
                    0,
                    min(
                        max(0, (total - 1) // 10), ui["page"] + (-1 if action == "previous" else 1)
                    ),
                )
            elif action == "close":
                await self.cache.delete(PREFIX + "ui:" + token)
                return DiscordIntent("edit", pages=(PresentationPage(title="KGuess"),))
            elif action == "start":
                if ui["kind"] != "setup":
                    raise GameError("Invalid menu.")
                await self.begin(ui, request, job)
                ui["kind"] = "started"
                ui["revision"] += 1
                await self.save_ui(ui)
                return DiscordIntent(
                    "edit",
                    pages=(
                        PresentationPage(
                            title="KGuess",
                            description=f"{ui['mode'].title()} · {ui['rounds']} rounds",
                        ),
                    ),
                )
            else:
                raise GameError("Invalid action.")
            ui["revision"] += 1
            await self.save_ui(ui)
            return await self.ui_intent(ui, "edit")

    def run_lock(self, state):
        return self.cache.lock(
            PREFIX + f"run:{state['guild']}:{state['channel']}", timeout=120, blocking_timeout=2
        )

    async def begin(self, ui, request, job):
        validate_settings(ui["rounds"], ui["timer"])
        bounds = parse_wishlist(ui["query"])
        pool = await self.store.candidates(bounds)
        if pool_size(pool, ui["mode"]) < ui["rounds"]:
            raise GameError("Not enough available answers.")
        now = self.clock()
        state = dict(
            id=ui["game_id"],
            guild=ui["guild"],
            channel=ui["channel"],
            host=ui["owner"],
            mode=ui["mode"],
            count=ui["rounds"],
            timer=ui["timer"],
            query=ui["query"],
            pool=pool,
            used=[],
            seed=secrets.randbits(63),
            number=0,
            last_effect="",
            effect_queue=[],
            effect_cycle=0,
            phase="preparing",
            round=None,
            revision=0,
            updated=now,
            next=now,
            scores={},
            silent=0,
        )
        if not await self.store.create(state):
            existing = await self.store.active(state["guild"], state["channel"])
            if not existing or existing["id"] != state["id"]:
                raise GameError("A game is already active here.")
        await self.schedule(state, now + 1000)
        await self.drive(state["id"], request, job)

    async def save(self, state):
        state["updated"] = self.clock()
        if not await self.store.save(state, state["revision"]):
            raise GameError("Game updated.")

    async def schedule(self, state, due=None):
        if state["phase"] in TERMINAL:
            return
        due = due if due is not None else state.get("next", self.clock() + 1000)

        revision = await self.cache.incr(PREFIX + "schedule:" + state["id"])
        await self.cache.expire(PREFIX + "schedule:" + state["id"], 31 * 86400)
        await self.scheduler.publish(
            operation="upsert",
            schedule_key="games:" + state["id"],
            route="games.tick",
            revision=revision,
            guild_id=state["guild"],
            channel_id=state["channel"],
            actor_id=state["host"],
            due_at_ms=due,
            expires_at_ms=due + 120_000,
            arguments={"session": state["id"]},
        )

    async def watch(
        self,
        state,
        job,
        operation,
        *,
        token=None,
        intent=None,
        cursor="0-0",
        member=None,
        message=None,
        resolved=False,
    ):
        token = token or state["round"]["id"]
        return await self.actions.request(
            job_id=job,
            kind="channel_watch",
            guild_id=state["guild"],
            channel_id=state["channel"],
            idempotency_key=f"games:{token}:{operation}:{job}:{cursor}",
            intent=intent,
            member_id=member,
            message_id=message,
            channel_watch=ChannelWatchSpec(
                operation,
                ROUTE,
                token,
                state["timer"] * 1000,
                cursor,
                revision=state["revision"],
                resolved=resolved,
            ),
            wait_seconds=25,
        )

    async def attachment(self, path):
        extension = path.suffix.lower()
        if extension not in {".webp", ".png"}:
            raise ValueError("invalid clue format")
        data = await asyncio.to_thread(path.read_bytes)
        token = secrets.token_hex(32)
        target = self.temp_root / token
        await asyncio.to_thread(target.write_bytes, data)
        target.chmod(0o640)
        return TemporaryAttachment(
            "tmp:" + token,
            "clue-" + secrets.token_hex(8) + extension,
            hashlib.sha256(data).hexdigest(),
            len(data),
            self.clock() + 120_000,
        )

    async def round_intent(self, state, kind="edit", resolved=False):
        round_ = state["round"]
        elapsed = max(0, self.clock() - round_["start"]) if round_.get("start") else 0
        hint = min(5, elapsed * 6 // (state["timer"] * 1000))
        stage = 3 if resolved else min(2, elapsed * 3 // (state["timer"] * 1000))
        attachment = await self.attachment(self.renderer.path(round_["cache"], stage))
        if resolved:
            text = safe(round_["answer"])
            outcome = round_.get("outcome") or {}
            if outcome.get("winner"):
                text += f"\n<@{outcome['winner']}> · +{outcome['points']:,}"
            elif state["phase"] == "cancelled":
                text += "\nStopped."
        else:
            text = "`" + mask_answer(round_["reveal"], hint).replace("`", "\u02cb") + "`"
            if round_.get("end"):
                text += f"\n<t:{round_['end'] // 1000}:R>"
        return DiscordIntent(
            kind,
            pages=(
                PresentationPage(
                    title=f"KGuess · {state['number']}/{state['count']} · {state['mode'].title()}",
                    description=text,
                    file_name=attachment.file_name,
                ),
            ),
            attachments=(attachment,),
        )

    async def edit_round(self, state, job, resolved=False):
        intent = await self.round_intent(state, resolved=resolved)
        return await self.watch(
            state,
            job,
            "edit",
            intent=intent,
            message=state["round"]["message"],
            resolved=resolved,
        )

    async def prepare(self, state):
        rng = random.Random(state["seed"] + state["number"])
        while True:
            candidate = choose(state["pool"], state["mode"], state["used"], rng)
            if candidate is None:
                state["phase"] = "done"
                return
            images = list(await self.store.images(candidate["key"]))
            rng.shuffle(images)
            effect, effect_queue, effect_cycle = next_effect(
                state["seed"],
                state.get("effect_queue", ()),
                state["last_effect"],
                state.get("effect_cycle", 0),
            )
            seed = rng.randrange(2**63)
            for edition, blob in images:
                try:
                    cache = await self.renderer.prepare(blob, effect, seed)
                except (OSError, ValueError):
                    continue
                answer = (
                    candidate["character"] if state["mode"] == "character" else candidate["series"]
                )
                aliases = await self.store.aliases(candidate["key"], state["mode"])
                state["round"] = dict(
                    id=secrets.token_hex(16),
                    key=candidate["key"],
                    edition=edition,
                    wl=candidate["wl"],
                    answer=answer,
                    answers=[answer, *aliases],
                    cache=cache,
                    effect=effect,
                    seed=seed,
                    reveal=reveal_order(answer, seed),
                    cursor="0-0",
                    message=0,
                    start=0,
                    end=0,
                    had_message=False,
                    last_stage=-1,
                )
                state["number"] += 1
                state["last_effect"] = effect
                state["effect_queue"] = effect_queue
                state["effect_cycle"] = effect_cycle
                state["used"].append(answer_key(candidate, state["mode"]))
                state["phase"] = "publishing"
                return

            state["pool"] = [row for row in state["pool"] if row["key"] != candidate["key"]]
            remaining = [
                row for row in state["pool"] if answer_key(row, state["mode"]) not in state["used"]
            ]
            if pool_size(remaining, state["mode"]) < state["count"] - state["number"]:
                state["phase"] = "done"
                return

    async def drive(self, identifier, request, job):
        original = await self.store.get(identifier)
        if original is None or original["phase"] in TERMINAL:
            return
        async with self.run_lock(original):
            state = await self.store.get(identifier)
            if state is None or state["phase"] in TERMINAL:
                return

            await self.schedule(state, self.clock() + 2000)
            if state["phase"] == "finishing":
                await self.finish(state, job)
                return
            if state["phase"] == "between" and self.clock() < state["next"]:
                return
            if state.get("round") and state["phase"] in {"publishing", "active", "resolving"}:
                if not all(
                    self.renderer.path(state["round"]["cache"], i).is_file() for i in range(4)
                ):
                    await self.cancel(state, job, edit=False)
                    return
            if state["phase"] in {"preparing", "between"}:
                if state.get("round") and isinstance(self.renderer, ClueRenderer):
                    self.renderer.release(state["round"]["cache"])
                if state["number"] >= state["count"] or state["silent"] >= 2:
                    await self.finish(state, job)
                    return
                await self.prepare(state)
                if state["phase"] == "done":
                    await self.finish(state, job)
                    return
                await self.save(state)
            if state["phase"] == "publishing":
                response = await self.watch(
                    state, job, "publish", intent=await self.round_intent(state, "send")
                )
                if not response.ok or not response.data.get("message"):
                    await self.cancel(state, job)
                    return
                state["round"].update(
                    message=response.data["message"],
                    start=response.data["start"],
                    end=response.data["end"],
                )
                state["phase"] = "active"
                await self.save(state)
            if state["phase"] == "active":
                await self.evaluate(state, job)
            if state["phase"] == "resolving":
                response = await self.edit_round(state, job, resolved=True)
                if response.ok:
                    await self.watch(state, job, "forget")
                    if isinstance(self.renderer, ClueRenderer):
                        self.renderer.release(state["round"]["cache"])
                    state["phase"] = "between"
                    state["next"] = self.clock() + 5000
                    await self.save(state)
                else:
                    await self.cancel(state, job, edit=False)
                    return
            if state["phase"] == "active":
                round_ = state["round"]
                stage = min(
                    5, max(0, self.clock() - round_["start"]) * 6 // (state["timer"] * 1000)
                )
                if stage != round_["last_stage"]:
                    if not (await self.edit_round(state, job)).ok:
                        await self.cancel(state, job, edit=False)
                        return
                    round_["last_stage"] = stage
                    await self.save(state)
                state["next"] = min(
                    round_["end"], round_["start"] + (stage + 1) * state["timer"] * 1000 // 6
                )
                await self.save(state)
            await self.schedule(state)

    async def evaluate(self, state, job):
        round_ = state["round"]
        closing = self.clock() >= round_["end"]
        if closing:
            await self.watch(state, job, "close")
        winner, elapsed = 0, 0
        while True:
            response = await self.watch(state, job, "read", cursor=round_["cursor"])
            if not response.ok:
                await self.cancel(state, job)
                return
            if response.data.get("deleted") or response.data.get("error"):
                await self.cancel(state, job, edit=not response.data.get("deleted"))
                return
            entries = response.data.get("entries", [])
            for entry in entries:
                round_["cursor"] = entry["cursor"]
                if (
                    not entry["eligible"]
                    or entry["message"] <= round_["message"]
                    or entry["at"] > round_["end"]
                ):
                    continue
                round_["had_message"] = True
                if not matches(entry["text"], round_["answers"]):
                    continue
                verified = await self.watch(
                    state,
                    job,
                    "verify",
                    cursor=entry["cursor"],
                    member=entry["actor"],
                    message=entry["message"],
                )
                if not verified.ok:
                    if verified.error_code == "candidate_ineligible":
                        continue
                    raise ActionUnavailable("candidate verification unavailable")
                winner = entry["actor"]
                elapsed = max(0, entry["at"] - round_["start"])
                break
            if winner or not response.data.get("has_more"):
                break
        if not winner and not closing:
            await self.save(state)
            return
        await self.watch(state, job, "close")
        outcome = dict(
            round=round_["id"],
            winner=winner,
            elapsed=elapsed,
            points=points(elapsed, state["timer"] * 1000) if winner else 0,
            ended=self.clock(),
        )
        round_["outcome"] = outcome
        state["phase"] = "resolving"
        state["silent"] = 0 if round_["had_message"] else state["silent"] + 1
        if winner:
            score = state["scores"].setdefault(str(winner), dict(points=0, wins=0))
            score["points"] += outcome["points"]
            score["wins"] += 1
        state["updated"] = self.clock()
        if not await self.store.settle(state, state["revision"], outcome):
            raise GameError("Round updated.")

    async def finish(self, state, job):

        state["phase"] = "finishing"
        await self.save(state)
        rows = sorted(state["scores"].items(), key=lambda r: (-r[1]["points"], int(r[0])))
        standings, rank, previous = [], 0, None
        for index, (user, row) in enumerate(rows[:20], 1):
            if row["points"] != previous:
                rank, previous = index, row["points"]
            standings.append(f"{rank}. <@{user}> · {row['points']:,} · {row['wins']} wins")
        text = "\n".join(standings) or "No scores."
        response = await self.actions.request(
            job_id=job,
            kind="send_message",
            guild_id=state["guild"],
            channel_id=state["channel"],
            idempotency_key="games:final:" + state["id"],
            intent=DiscordIntent(
                "send", pages=(PresentationPage(title="KGuess", description=text),)
            ),
            wait_seconds=25,
        )
        if response.ok:
            state["phase"] = "done"
            await self.save(state)
        else:
            await self.cancel(state, job, edit=False)

    async def cancel(self, state, job, edit=True):
        if state["phase"] in TERMINAL:
            return
        state["phase"] = "cancelled"
        await self.save(state)
        if state.get("round") and state["round"].get("message"):
            with contextlib.suppress(ActionUnavailable, RedisError, TimeoutError):
                await self.watch(state, job, "close")
            if edit:
                with contextlib.suppress(ActionUnavailable, RedisError, TimeoutError, OSError):
                    await self.edit_round(state, job, resolved=True)
            with contextlib.suppress(ActionUnavailable, RedisError, TimeoutError):
                await self.watch(state, job, "forget")
        if state.get("round") and isinstance(self.renderer, ClueRenderer):
            self.renderer.release(state["round"]["cache"])
