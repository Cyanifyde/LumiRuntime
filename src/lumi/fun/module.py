from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
import weakref
from pathlib import Path

import msgspec
from asyncmy.errors import Error as DatabaseError
from redis.exceptions import RedisError

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    GuildCommandSourceSpec,
    HelpSpec,
    Manifest,
    ModalChoice,
    ModalFieldSpec,
    ModalSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
    ResponseRevision,
    TypedRequest,
    decode,
    encode,
)
from lumi.shared.response_revisions import publish_revision
from lumi.worker.actions import ActionUnavailable, EdgeActionClient

from .catalogue import Catalogue, Mutation
from .media import ATTRIBUTION, KlipyMediaService, MediaUnavailable
from .publisher import PERFORM_GRAMMAR, CataloguePublisher
from .rules import RESERVED, TARGET

LOGGER = logging.getLogger(__name__)
SELECT = """
local cached = redis.call('GET', KEYS[1])
if cached then return cached end
if not redis.call('SET', KEYS[2], '1', 'NX', 'EX', 3) then return '' end
local previous = redis.call('GET', KEYS[3])
local choices = cjson.decode(ARGV[1])
local available = {}
for _,id in ipairs(choices) do
  if #choices == 1 or id ~= previous then table.insert(available, id) end
end
local index = (tonumber(ARGV[2]) % #available) + 1
local chosen = available[index]
redis.call('SET', KEYS[1], chosen, 'EX', 180)
redis.call('SET', KEYS[3], chosen)
return chosen
"""
RATE = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('EXPIRE', KEYS[1], 60) end
return count <= 5
"""


class Session(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    token: str
    owner_id: int
    guild_id: int
    channel_id: int
    expires_at_ms: int
    revision: int = 0
    catalogue_revision: int = 0
    category: str = ""
    gif: str = ""
    page: int = 0
    query: str = ""
    removed: bool = False
    gif_removed: bool = False


class FunModule:
    def __init__(
        self, *, generation: int, queue_redis, cache_redis, store, media_service=None
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._store = store
        self._media = (
            media_service
            if media_service is not None
            else KlipyMediaService(
                store,
                key_file=Path(os.getenv("LUMI_KLIPY_API_KEY_FILE", "/run/secrets/klipy_api_key")),
            )
        )
        self._actions = EdgeActionClient(
            queue_redis, module="fun", group="community", generation=generation
        )
        self.publisher = CataloguePublisher(store, queue_redis)
        self._task = None
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    @property
    def manifest(self) -> Manifest:
        grammar = r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,8})$"
        components = []
        modals = {
            "create": ModalSpec(
                "Add category",
                (
                    ModalFieldSpec("name", "Name", maximum=32, minimum=1),
                    ModalFieldSpec(
                        "mode",
                        "Type",
                        kind="string_select",
                        choices=(
                            ModalChoice("Solo", "solo"),
                            ModalChoice("Targeted", "targeted"),
                        ),
                    ),
                ),
            ),
            "add": ModalSpec(
                "Add GIFs",
                (
                    ModalFieldSpec(
                        "urls", "KLIPY URLs · up to 25", style="paragraph", maximum=4000, minimum=1
                    ),
                ),
            ),
            "search": ModalSpec(
                "Find category", (ModalFieldSpec("query", "Name", maximum=32, required=False),)
            ),
        }
        for action in (
            "home",
            "pick",
            "previous",
            "next",
            "create",
            "add",
            "search",
            "remove_gif",
            "remove_category",
            "removed",
            "restore_gif",
            "restore_category",
        ):
            components.append(
                ComponentSpec(
                    "fun",
                    action,
                    "fun.editor",
                    grammar,
                    captures=(
                        CaptureSpec("session"),
                        CaptureSpec("revision", "integer", minimum=0, maximum=99_999_999),
                    ),
                    acl_key="fun",
                    public=True,
                    session_bound=True,
                    presentation_mode="v2",
                    deadline_ms=30_000,
                    input_kind="modal"
                    if action in modals
                    else ("string_values" if action == "pick" else "none"),
                    modal=modals.get(action),
                )
            )
        return Manifest(
            2,
            "fun",
            "community",
            self._generation,
            "lumi:jobs:community",
            display_name="Fun",
            summary="GIF actions.",
            version="1.1.0",
            presentation_mode="legacy",
            commands=(
                CommandSpec(
                    "fun",
                    "fun.editor",
                    r"^(?:\s+(?P<section>gifs))?$",
                    captures=(CaptureSpec("section", required=False, maximum=4),),
                    help=HelpSpec(".fun gifs", "Add and manage GIF categories."),
                    acl_key="fun",
                    public=True,
                    context_fields=("prefix",),
                    deadline_ms=30_000,
                    presentation_mode="v2",
                ),
            ),
            components=tuple(components),
            capabilities=("read_interaction_context",),
            guild_commands=GuildCommandSourceSpec(
                "fun.perform",
                "fun",
                (
                    "prefix",
                    "reply_author_id",
                    "reference_message_id",
                ),
                grammar=PERFORM_GRAMMAR,
                captures=(CaptureSpec("target", required=False, maximum=100),),
            ),
        )

    async def start(self) -> None:
        await self.publisher.restore()
        self._task = asyncio.create_task(self.publisher.run(), name="fun-catalogue-outbox")

    def begin_drain(self):
        self.publisher.draining = True

    def resume(self):
        self.publisher.draining = False

    def drain_ready(self):
        return not self.publisher.busy

    async def close(self) -> None:
        self.publisher.stop()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._media.close()


    def _result(self, job: str, intent: DiscordIntent | None = None, session=None):
        return ResultEnvelope(
            1,
            job,
            "fun",
            "community",
            self._generation,
            "ok",
            intents=(intent,) if intent else (),
            completed_at_ms=int(time.time() * 1000),
            response_revision=ResponseRevision(session.token, session.revision) if session else None,
        )

    @staticmethod
    def _message(kind: str, text: str, *, editor: bool = False) -> DiscordIntent:
        if editor:
            return DiscordIntent(kind, pages=(PresentationPage(description=text),))
        return DiscordIntent(kind, content=text)

    async def _context(self, request, job):
        target_id, reference_id, target_valid = self._target_parameters(request)
        result = await self._actions.request(
            job_id=job,
            kind="read_interaction_context",
            guild_id=int(request.event.get("guild_id") or 0),
            channel_id=int(request.event.get("channel_id") or 0),
            member_id=target_id,
            message_id=reference_id,
            idempotency_key=f"context:{job}",
            wait_seconds=8,
        )
        if not result.ok:
            if request.handler == "fun.perform" and result.error_code == "target_unavailable":
                return msgspec.structs.replace(result, error_code="usage")
            return result
        facts = result.data
        resolved_target = facts.get("target_id")
        if (
            not target_valid
            or (target_id is not None and resolved_target != target_id)
            or (reference_id is not None and (not resolved_target or resolved_target == facts["actor_id"]))
        ):
            return msgspec.structs.replace(result, ok=False, status="failed", error_code="usage")
        return msgspec.structs.replace(result, data=dict(
            actor_id=facts["actor_id"], target_id=resolved_target,
            moderator=bool(facts["guild_administrator"] or facts["guild_manage_messages"]),
            media=bool(facts["actor_can_embed"] and facts["bot_can_embed"]),
            conflicts=sorted(set(facts["registered_roots"]) | RESERVED),
        ))

    @staticmethod
    def _target_parameters(request):
        if request.handler != "fun.perform":
            return None, None, True
        mode = request.arguments.get("mode")
        explicit = str(request.arguments.get("target") or "").strip()
        if mode == "solo":
            return None, None, not explicit
        if mode != "targeted":
            return None, None, False
        if explicit:
            match = TARGET.fullmatch(explicit)
            if not match:
                return None, None, False
            target = int(match.group(1) or match.group(2))
        else:
            target = int(request.event.get("reply_author_id") or 0)
            if not target:
                reference = int(request.event.get("reference_message_id") or 0)
                return None, reference or None, bool(reference)
        if target == request.event.get("actor_id"):
            return None, None, False
        return target, None, True

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        try:
            return await self._handle(request, job_id, generation)
        except (DatabaseError, OSError, RedisError, ActionUnavailable, TimeoutError):
            LOGGER.exception("fun_request_unconfirmed")
            kind = "edit" if request.event.get("dispatch") == "component" else "send"
            return self._result(
                job_id,
                self._message(
                    kind,
                    "Unavailable. Try again.",
                    editor=request.handler == "fun.editor",
                ),
            )

    async def _handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        if (
            int(request.event.get("_expires_at_ms") or (time.time() * 1000 + 1))
            <= time.time() * 1000
        ):
            return self._result(job_id)
        try:
            context = await self._context(request, job_id)
        except (ActionUnavailable, TimeoutError):
            kind = "edit" if request.event.get("dispatch") == "component" else "send"
            return self._result(
                job_id,
                self._message(
                    kind,
                    "Unavailable.",
                    editor=request.handler == "fun.editor",
                ),
            )
        if not context.ok:
            if request.handler == "fun.perform" and context.error_code == "usage":
                prefix = str(request.event.get("prefix") or ".")
                name = str(request.arguments.get("category_name") or "fun")
                usage = (
                    prefix
                    + name
                    + (" @member" if request.arguments.get("mode") == "targeted" else "")
                )
                return self._result(job_id, self._message("send", usage))
            return self._result(job_id)
        cached = await self._cache.get(f"lumi:fun:result:{job_id}")
        if cached:
            return decode(cached, ResultEnvelope)
        if request.handler == "fun.perform":
            return await self._perform(request, job_id, context.data)
        if request.handler != "fun.editor":
            return self._result(job_id)
        if request.event.get("dispatch") == "command":
            state = await self._store.read(int(request.event["guild_id"]))
            session = Session(
                secrets.token_hex(8),
                int(request.event["actor_id"]),
                int(request.event["guild_id"]),
                int(request.event["channel_id"]),
                int(time.time() * 1000) + 900_000,
                catalogue_revision=state.revision,
            )
            result = self._result(
                job_id, self._render(session, state, context.data, "send"), session
            )
            await self._save(session, result)
            return result
        token = str(request.arguments.get("session") or "")
        async with self._locks.setdefault(token, asyncio.Lock()):
            raw = await self._cache.get(f"lumi:fun:session:{token}")
            session = decode(raw, Session) if raw else None
            if (
                session is None
                or session.owner_id != request.event.get("actor_id")
                or session.guild_id != request.event.get("guild_id")
                or session.channel_id != request.event.get("channel_id")
                or session.expires_at_ms <= int(time.time() * 1000)
            ):
                return self._result(job_id)

            if session.revision != request.arguments.get("revision"):
                return self._result(job_id)
            return await self._editor(request, job_id, session, context.data)

    async def _perform(self, request, job, context):
        state = await self._store.read(int(request.event["guild_id"]))
        category = next(
            (
                c
                for c in state.categories
                if c.id == request.arguments.get("category_id")
                and c.name == request.arguments.get("category_name")
                and c.mode == request.arguments.get("mode")
                and not c.hidden
            ),
            None,
        )
        gifs = (
            [g for g in category.gifs if not g.hidden and g.canonical not in state.blocked_urls]
            if category
            else []
        )
        if not gifs:
            return self._result(job, self._message("send", "No GIFs."))
        ids = [g.id for g in gifs]
        chosen = await self._cache.eval(
            SELECT,
            3,
            f"lumi:fun:selection:{job}",
            f"lumi:fun:cooldown:{request.event['guild_id']}:{request.event['actor_id']}",
            f"lumi:fun:last:{request.event['guild_id']}:{category.id}",
            msgspec.json.encode(ids),
            secrets.randbelow(2**31),
        )
        if not chosen:
            return self._result(job, self._message("send", "Wait 3s."))
        identifier = chosen.decode() if isinstance(chosen, bytes) else chosen
        gif = next((g for g in gifs if g.id == identifier), None)
        if gif is None:

            return self._result(job)
        result = self._result(job, self._message("send", gif.url))
        if context.get("media"):
            try:
                media = await self._media.resolve(gif.url)
            except (MediaUnavailable, DatabaseError, OSError, msgspec.DecodeError):
                LOGGER.warning("fun_media_unavailable")
            else:
                result = self._result(
                    job,
                    DiscordIntent(
                        "send",
                        pages=(PresentationPage(image_url=media.media_url, footer=ATTRIBUTION),),
                    ),
                )
        if (
            int(request.event.get("_expires_at_ms") or (time.time() * 1000 + 1))
            <= time.time() * 1000
        ):
            return self._result(job)
        await self._cache.set(f"lumi:fun:result:{job}", encode(result), ex=180)
        return result

    async def _save(self, session, result):
        ttl = max(1, (session.expires_at_ms - int(time.time() * 1000) + 999) // 1000)
        pipeline = self._cache.pipeline(transaction=True)
        pipeline.set(f"lumi:fun:session:{session.token}", encode(session), ex=ttl)
        publish_revision(
            pipeline, "community", session.token, module="fun", actor_id=session.owner_id,
            guild_id=session.guild_id, channel_id=session.channel_id, revision=session.revision,
            expires_at_ms=session.expires_at_ms, ttl=ttl,
        )
        pipeline.set(f"lumi:fun:result:{result.job_id}", encode(result), ex=180)
        await pipeline.execute()

    async def _editor(self, request, job, session, context):
        state = await self._store.read(session.guild_id)
        action = request.route.partition(".")[2]
        notice = ""
        args = request.arguments
        values = args.get("values", {})
        try:
            if action == "home":
                session = msgspec.structs.replace(
                    session, category="", gif="", page=0, gif_removed=False
                )
            elif action == "removed":
                if not context["moderator"]:
                    raise ValueError("Moderator only.")
                if session.category:
                    session = msgspec.structs.replace(
                        session, gif_removed=not session.gif_removed, gif="", page=0
                    )
                else:
                    session = msgspec.structs.replace(
                        session, removed=not session.removed, category="", gif="", page=0
                    )
            elif action == "search":
                session = msgspec.structs.replace(
                    session, query=str(values.get("query", ""))[:32], category="", gif="", page=0
                )
            elif action == "pick":
                selected = str(values[0]) if values else ""
                if selected not in {c.id for c in state.categories if c.hidden == session.removed}:
                    raise ValueError("Category unavailable.")
                session = msgspec.structs.replace(session, category=selected, gif="", page=0)
            elif action in {"previous", "next"}:
                session = msgspec.structs.replace(
                    session, page=max(0, session.page + (1 if action == "next" else -1))
                )
            elif action in {
                "create",
                "add",
                "remove_gif",
                "remove_category",
                "restore_gif",
                "restore_category",
            }:


                fresh = await self._context(request, job)
                if not fresh.ok:
                    return self._result(job)
                context = fresh.data
                if action in {"create", "add"}:
                    allowed = await self._cache.eval(
                        RATE, 1, f"lumi:fun:uploads:{session.guild_id}:{session.owner_id}"
                    )
                    if not allowed:
                        raise ValueError("Upload limit. Try later.")
                mode = values.get("mode", "") if isinstance(values, dict) else ""
                if isinstance(mode, (tuple, list)):
                    mode = mode[0] if mode else ""
                change = Mutation(
                    action,
                    session.owner_id,
                    session.catalogue_revision,
                    category=session.category,
                    gif=session.gif,
                    name=str(values.get("name", "")) if isinstance(values, dict) else "",
                    mode=str(mode),
                    urls=str(values.get("urls", "")) if isinstance(values, dict) else "",
                )
                state, notice = await self._store.change(
                    session.guild_id,
                    job,
                    change,
                    moderator=context["moderator"],
                    conflicts=frozenset(context["conflicts"]),
                )
                if action == "create" and notice == "Added.":
                    category = next(
                        c for c in state.categories if c.name == change.name.strip().casefold()
                    )
                    session = msgspec.structs.replace(
                        session, category=category.id, page=0, removed=False
                    )
                if action in {"remove_category", "restore_category"} and notice in {
                    "Removed.",
                    "Restored.",
                }:
                    session = msgspec.structs.replace(session, category="", gif="", page=0)
                try:
                    await self.publisher.flush()
                except Exception:


                    LOGGER.warning("fun_catalogue_publish_delayed")
        except ValueError as exc:
            notice = str(exc)
        except (DatabaseError, OSError, RedisError, ActionUnavailable, TimeoutError):
            LOGGER.exception("fun_editor_update_unconfirmed")
            notice = "Couldn't confirm. Try again."
        session = msgspec.structs.replace(
            session, revision=session.revision + 1, catalogue_revision=state.revision
        )
        session = self._normalize(session, state, context)
        result = self._result(job, self._render(session, state, context, "edit", notice), session)
        await self._save(session, result)
        return result

    def _normalize(self, session, state, context):
        if (session.removed or session.gif_removed) and not context["moderator"]:
            session = msgspec.structs.replace(
                session, removed=False, gif_removed=False, category="", gif="", page=0
            )
        category = next(
            (
                c
                for c in state.categories
                if c.id == session.category and c.hidden == session.removed
            ),
            None,
        )
        if session.category and category is None:
            return msgspec.structs.replace(session, category="", gif="", page=0)
        if category:
            gifs = self._gifs(category, state, session)
            page = min(session.page, max(0, len(gifs) - 1))
            return msgspec.structs.replace(session, page=page, gif=gifs[page].id if gifs else "")
        count = len(self._categories(session, state))
        return msgspec.structs.replace(session, page=min(session.page, max(0, (count - 1) // 25)))

    @staticmethod
    def _categories(session, state):
        return sorted(
            (
                c
                for c in state.categories
                if c.hidden == session.removed and session.query.casefold() in c.name
            ),
            key=lambda c: c.name,
        )

    @staticmethod
    def _gifs(category, state, session):
        if session.gif_removed:
            return [g for g in category.gifs if g.hidden or g.canonical in state.blocked_urls]
        return [g for g in category.gifs if not g.hidden and g.canonical not in state.blocked_urls]

    def _render(self, session, state: Catalogue, context, kind, notice=""):
        payload = f"{session.token}~{session.revision}"

        def button(action, label, disabled=False, style="secondary"):
            return PresentationControl(
                "button", "fun", action, payload, label=label, disabled=disabled, style=style
            )

        controls = []
        category = next((c for c in state.categories if c.id == session.category), None)
        if category:
            gifs = self._gifs(category, state, session)
            gif = next((g for g in gifs if g.id == session.gif), None)
            if gif is None and gifs:
                gif = gifs[min(session.page, len(gifs) - 1)]
            text = f"**{category.name}** · {category.mode.capitalize()}"
            if category.name in context["conflicts"]:
                text += " · Command unavailable"
            text += "\n" + (
                f"{session.page + 1}/{len(gifs)} · "
                + ("Starter" if gif.starter else f"<@{gif.contributor}>")
                + "\n"
                + gif.url
                if gif
                else "No GIFs."
            )
            controls.extend(
                (
                    button("home", "Back"),
                    button("previous", "Previous", session.page == 0),
                    button("next", "Next", session.page >= len(gifs) - 1),
                )
            )
            if not category.hidden:
                controls.append(button("add", "Add GIFs"))
                if context["moderator"]:
                    controls.append(
                        button("removed", "Active GIFs" if session.gif_removed else "Removed GIFs")
                    )
            if (
                not category.hidden
                and gif
                and (
                    context["moderator"]
                    or (not gif.starter and gif.contributor == session.owner_id)
                )
            ):
                restore = gif.hidden or gif.canonical in state.blocked_urls
                controls.append(
                    button(
                        "restore_gif" if restore else "remove_gif",
                        "Restore GIF" if restore else "Remove GIF",
                    )
                )
            if context["moderator"] or (
                not category.starter
                and category.creator == session.owner_id
                and not any(not g.hidden for g in category.gifs)
            ):
                controls.append(
                    button(
                        "restore_category" if category.hidden else "remove_category",
                        "Restore category" if category.hidden else "Remove category",
                    )
                )
        else:
            categories = self._categories(session, state)
            page = categories[session.page * 25 : session.page * 25 + 25]
            text = "**Fun GIFs**" + (" · Removed" if session.removed else "")
            if page:
                controls.append(
                    PresentationControl(
                        "string_select",
                        "fun",
                        "pick",
                        payload,
                        placeholder="Category",
                        options=tuple(
                            PresentationOption(c.name, c.id, c.mode.capitalize()) for c in page
                        ),
                    )
                )
            else:
                text += "\nNo categories."
            if len(categories) > 25:
                text += f" · {session.page + 1}/{(len(categories) + 24) // 25}"
                controls.extend(
                    (
                        button("previous", "Previous", session.page == 0),
                        button("next", "Next", (session.page + 1) * 25 >= len(categories)),
                    )
                )
            controls.extend((button("create", "Add category"), button("search", "Search")))
            if context["moderator"]:
                controls.append(button("removed", "Active" if session.removed else "Removed"))
        if notice:
            text += "\n" + notice
        title, _, description = text.partition("\n")
        return DiscordIntent(
            kind,
            pages=(
                PresentationPage(
                    title=title.replace("**", ""),
                    description=description[:2000],
                    controls=tuple(controls),
                ),
            ),
            session_id=session.token,
        )
