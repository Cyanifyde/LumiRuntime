from __future__ import annotations

import logging
import re
import secrets
import time
from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    MemberSearchResult,
    MemberSearchSpec,
    PresentationControl,
    PresentationOption,
    PresentationPage,
    ResultEnvelope,
)
from lumi.members.search import MAX_QUERY, SESSION_MS, TOKEN, SearchSession
from lumi.ui_keys import search_key, ui_key
from lumi.worker.actions import ActionUnavailable, EdgeActionClient

MODULE = "local_time"
LOGGER = logging.getLogger(__name__)


def safe_name(value):

    value = value.replace("\r", " ").replace("\n", " ").replace("@", "@\u200b")
    return re.sub(r"([\\`*_~|\[\]()>#])", r"\\\1", value)


class LocalTimeModule:
    def __init__(
        self, *, generation, queue_redis, cache_redis, region_provider, actions=None, clock=None
    ):
        self.generation, self.cache, self.region_provider = generation, cache_redis, region_provider
        self.actions = actions or EdgeActionClient(
            queue_redis, module=MODULE, group="community", generation=generation
        )
        self.clock = clock or (lambda: int(time.time() * 1000))

    @property
    def manifest(self):
        return Manifest(
            schema=2,
            module=MODULE,
            group="community",
            generation=self.generation,
            job_stream="lumi:jobs:community",
            display_name="Local Time",
            summary="Local time from saved regions.",
            presentation_mode="legacy",
            version="1.0.1",
            commands=(
                CommandSpec(
                    "time",
                    "local_time.show",
                    r"^\s*(?P<query>[^\r\n]{0,100})$",
                    captures=(CaptureSpec("query", required=False, maximum=MAX_QUERY),),
                    help=HelpSpec(
                        ".time [name|@user|ID]", "Show local time; omit the user for yours."
                    ),
                    acl_key="time",
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
            components=(
                ComponentSpec(
                    MODULE,
                    "select",
                    "local_time.select",
                    r"^(?P<session>[a-f0-9]{16})$",
                    captures=(CaptureSpec("session"),),
                    input_kind="string_values",
                    acl_key="time",
                    public=True,
                    session_bound=True,
                    native_selection=True,
                    deadline_ms=60_000,
                ),
            ),
            capabilities=("search_members",),
        )

    def current(self, request, generation):
        return generation == self.generation and self.clock() < int(
            request.event.get("_expires_at_ms", self.clock() + 1)
        )

    async def close(self):
        pass

    async def handle(self, request, job_id, generation):
        if not self.current(request, generation):
            return self.result(job_id)
        try:
            if request.handler == "local_time.show":
                intent = await self.show(request, job_id)
            elif request.handler == "local_time.select":
                intent = await self.select(request, job_id)
            else:
                intent = None
            return self.result(job_id, intent if self.current(request, generation) else None)
        except (ActionUnavailable, RedisError, MySQLError, TimeoutError, ValueError, TypeError):
            LOGGER.warning("local_time_unavailable")
            return self.result(
                job_id,
                self.notice(request, "Unavailable. Try again.")
                if self.current(request, generation)
                else None,
            )

    def result(self, job_id, intent=None):
        return ResultEnvelope(
            1,
            job_id,
            MODULE,
            "community",
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
            presentation_mode="legacy",
        )

    async def lookup(self, request, job_id, query, selected_id=None):
        response = await self.actions.request(
            job_id=job_id,
            kind="search_members",
            guild_id=int(request.event["guild_id"]),
            channel_id=int(request.event["channel_id"]),
            member_search=MemberSearchSpec(query, selected_id),
            idempotency_key=f"time:{job_id}:{selected_id or 'search'}",
            wait_seconds=30,
        )
        if not response.ok:
            raise ActionUnavailable()
        return msgspec.convert(response.data, type=MemberSearchResult)

    async def text(self, candidate):
        saved = await self.region_provider(candidate.id)
        try:
            zone = ZoneInfo(saved) if saved else None
        except (ZoneInfoNotFoundError, ValueError):
            zone = None
        if zone is None:
            return "Region not set."
        local = datetime.fromtimestamp(self.clock() / 1000, UTC).astimezone(zone)
        return f"{safe_name(candidate.display_name)} · {local:%a %d %b, %H:%M}"

    async def show(self, request, job_id):
        query = str(request.arguments.get("query") or "").strip()
        if query.casefold() == "help":
            prefix = str(request.event.get("prefix") or ".")
            return self.notice(request, f"`{prefix}time [name | @user | ID]`")
        found = await self.lookup(request, job_id, query)
        if found.status == "matched" and len(found.candidates) == 1:
            return self.notice(request, await self.text(found.candidates[0]))
        if found.status != "choose":
            return self.notice(request, self.search_notice(found.status))
        if not 1 <= len(found.candidates) <= 25:
            raise ValueError("invalid search candidates")
        session = SearchSession(
            secrets.token_hex(8),
            MODULE,
            int(request.event["actor_id"]),
            int(request.event["guild_id"]),
            int(request.event["channel_id"]),
            query,
            tuple(c.id for c in found.candidates),
            self.clock() + SESSION_MS,
        )
        await self.save(session)
        return DiscordIntent(
            "send",
            pages=(PresentationPage(description="Choose a member."),),
            controls=(
                PresentationControl(
                    "string_select",
                    MODULE,
                    "select",
                    session.token,
                    placeholder="Member",
                    options=tuple(
                        PresentationOption(
                            c.display_name[:100], str(c.id), f"@{c.username} · {c.id}"[:100]
                        )
                        for c in found.candidates
                    ),
                ),
            ),
            session_id=session.token,
            presentation_mode="legacy",
        )

    async def save(self, session):
        remaining = session.expires - self.clock()
        if remaining <= 0:
            raise ValueError("expired search")
        pipeline = self.cache.pipeline(transaction=True)
        pipeline.set(search_key(session.token), msgspec.json.encode(session), px=remaining)
        pipeline.set(ui_key("owner", session.token), str(session.owner), px=remaining)
        await pipeline.execute()

    async def select(self, request, job_id):
        token = str(request.arguments.get("session") or "")
        if not TOKEN.fullmatch(token):
            return self.notice(request, "This menu has expired.")
        async with self.cache.lock(search_key(token) + ":lock", timeout=45, blocking_timeout=1):
            raw = await self.cache.get(search_key(token))
            session = msgspec.json.decode(raw, type=SearchSession) if raw else None
            bound = await self.cache.get(ui_key("message", token))
            if (
                not session
                or (session.module, session.owner, session.guild, session.channel)
                != (
                    MODULE,
                    int(request.event["actor_id"]),
                    int(request.event["guild_id"]),
                    int(request.event["channel_id"]),
                )
                or session.expires <= self.clock()
                or not bound
                or int(bound) != int(request.event.get("message_id") or 0)
            ):
                return self.notice(request, "This menu has expired.")
            if session.text is not None:
                if session.last_job == job_id:
                    return self.selected(session)
                return self.notice(request, "Already selected.")
            values = request.arguments.get("values")
            if (
                not isinstance(values, (list, tuple))
                or len(values) != 1
                or not isinstance(values[0], str)
                or not values[0].isascii()
                or not values[0].isdigit()
                or (int(values[0]) not in session.ids)
            ):
                return self.notice(request, "Invalid selection.")
            found = await self.lookup(request, job_id, session.query, int(values[0]))
            if found.status != "matched" or len(found.candidates) != 1:
                return self.notice(request, self.search_notice(found.status))
            if found.candidates[0].id != int(values[0]):
                raise ValueError("unexpected selected member")
            text = await self.text(found.candidates[0])
            if not self.current(request, self.generation):
                return None
            session = msgspec.structs.replace(session, last_job=job_id, text=text)
            await self.save(session)
            return self.selected(session)

    @staticmethod
    def selected(session):
        return DiscordIntent(
            "edit",
            pages=(PresentationPage(description=session.text),),
            session_id=session.token,
            presentation_mode="legacy",
        )

    @staticmethod
    def search_notice(status):
        return {
            "not_found": "Member not found.",
            "too_many": "Too many matches. Use a longer name.",
            "changed": "Member changed. Search again.",
        }.get(status, "Unavailable. Try again.")
