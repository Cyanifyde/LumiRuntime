from __future__ import annotations

import asyncio
import html
import json
import os
import re
import time
import unicodedata
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import quote

import aiohttp

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
)

API_URL = "https://api.urbandictionary.com/v0/define"
MAX_QUERY_CHARS = 200
MAX_CAPTURE_CHARS = 1_900
MAX_RESULTS = 3
MAX_RESPONSE_BYTES = 1024 * 1024
CACHE_TTL_SECONDS = 10 * 60
CACHE_MAX_ENTRIES = 256
REQUEST_TIMEOUT_SECONDS = 15
MAX_PENDING_QUERIES = 32

_ESCAPES = {"n": "\n", "r": "\n", "t": " ", '"': '"', "'": "'"}
_LITERAL_ESCAPE_RE = re.compile(r"\\([nrt\"'])")
_MENTION_RE = re.compile(r"@(everyone|here|[!&]?\d{17,20})")
_MARKDOWN_RE = re.compile(
    r"(?P<markdown>[_\\~|*`]|^>(?:>>)?\s|\[.+\]\(.+\)|^#{1,3}|^\s*-)",
    re.MULTILINE,
)


class UrbanError(RuntimeError):
    pass


class UrbanNoResultsError(UrbanError):
    pass


class UrbanUnavailableError(UrbanError):
    pass


class UrbanRateLimitedError(UrbanError):
    pass


@dataclass(frozen=True, slots=True)
class UrbanDefinition:
    word: str
    meaning: str
    example: str = ""


def _without_controls(text: str) -> str:
    return "".join(
        character
        for character in text
        if character.isspace() or unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )


def normalize_query(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    return " ".join(_without_controls(normalized).split())


def clean_source_text(text: str) -> str:
    text = html.unescape(text)
    text = _LITERAL_ESCAPE_RE.sub(lambda match: _ESCAPES[match[1]], text)
    text = _without_controls(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = [" ".join(line.split()) for line in text.split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def parse_definitions(payload: object, query: str) -> tuple[UrbanDefinition, ...]:
    if not isinstance(payload, dict):
        raise UrbanUnavailableError()
    rows = payload.get("list")
    if not isinstance(rows, list):
        raise UrbanUnavailableError()
    if not rows:
        raise UrbanNoResultsError()

    expected_word = normalize_query(query).casefold()
    results: list[UrbanDefinition] = []
    seen: set[tuple[str, str]] = set()
    valid_rows = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        word, meaning = row.get("word"), row.get("definition")
        if not isinstance(word, str) or not isinstance(meaning, str):
            continue
        word, meaning = normalize_query(word), clean_source_text(meaning)
        if not word or not meaning:
            continue
        valid_rows += 1
        if word.casefold() != expected_word:
            continue
        raw_example = row.get("example")
        example = clean_source_text(raw_example) if isinstance(raw_example, str) else ""
        identity = (meaning, example)
        if identity in seen:
            continue
        seen.add(identity)
        results.append(UrbanDefinition(word, meaning, example))
        if len(results) == MAX_RESULTS:
            break
    if not results:
        if not valid_rows:
            raise UrbanUnavailableError()
        raise UrbanNoResultsError()
    return tuple(results)


class UrbanDictionaryService:
    def __init__(
        self,
        *,
        endpoint: str = API_URL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._endpoint = endpoint
        self._clock = clock
        self._session: aiohttp.ClientSession | None = None
        self._cache: OrderedDict[str, tuple[float, tuple[UrbanDefinition, ...]]] = (
            OrderedDict()
        )
        self._inflight: dict[str, asyncio.Task[tuple[UrbanDefinition, ...]]] = {}
        self._request_gate = asyncio.Semaphore(4)

    @property
    def ready(self) -> bool:
        return self._session is not None and not self._session.closed

    async def start(self) -> None:
        if self.ready:
            return
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS, connect=5),
            connector=aiohttp.TCPConnector(limit=4, ttl_dns_cache=300),
        )

    async def close(self) -> None:
        session, self._session = self._session, None
        tasks = tuple(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._inflight.clear()
        self._cache.clear()
        if session is not None:
            await session.close()

    async def search(self, query: str) -> tuple[UrbanDefinition, ...]:
        query = normalize_query(query)
        if not query or len(query) > MAX_QUERY_CHARS:
            raise ValueError("invalid query")
        if not self.ready:
            raise UrbanUnavailableError()
        key = query.casefold()
        now = self._clock()
        for expired_key, (expiry, _) in tuple(self._cache.items()):
            if expiry <= now:
                del self._cache[expired_key]
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached[1]
        task = self._inflight.get(key)
        if task is None:
            if len(self._inflight) >= MAX_PENDING_QUERIES:
                raise UrbanRateLimitedError()
            task = asyncio.create_task(self._fetch_and_cache(query, key))
            self._inflight[key] = task
            task.add_done_callback(lambda completed: self._forget_task(key, completed))
        return await asyncio.shield(task)

    def _forget_task(self, key: str, task: asyncio.Task[tuple[UrbanDefinition, ...]]) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]
        if not task.cancelled():
            task.exception()

    async def _fetch_and_cache(
        self, query: str, key: str
    ) -> tuple[UrbanDefinition, ...]:
        definitions = await self._fetch(query)
        self._cache[key] = (self._clock() + CACHE_TTL_SECONDS, definitions)
        self._cache.move_to_end(key)
        while len(self._cache) > CACHE_MAX_ENTRIES:
            self._cache.popitem(last=False)
        return definitions

    async def _fetch(self, query: str) -> tuple[UrbanDefinition, ...]:
        session = self._session
        if session is None:
            raise UrbanUnavailableError()
        params = {"term": query}
        try:
            async with (
                asyncio.timeout(REQUEST_TIMEOUT_SECONDS),
                self._request_gate,
                session.get(self._endpoint, params=params, allow_redirects=False) as response,
            ):
                if response.status == 404:
                    raise UrbanNoResultsError()
                if response.status == 429:
                    raise UrbanRateLimitedError()
                if response.status != 200 or response.content_type != "application/json":
                    raise UrbanUnavailableError()
                if (response.content_length or 0) > MAX_RESPONSE_BYTES:
                    raise UrbanUnavailableError()
                body = bytearray()
                async for chunk in response.content.iter_chunked(65_536):
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE_BYTES:
                        raise UrbanUnavailableError()
                return parse_definitions(json.loads(body), query)
        except (aiohttp.ClientError, TimeoutError, ValueError):
            raise UrbanUnavailableError() from None


def _discord_character_count(text: str) -> int:
    return sum(2 if ord(character) > 0xFFFF else 1 for character in text)


def _safe_text(text: str) -> str:
    text = _MENTION_RE.sub(lambda match: f"@\u200b{match[1]}", text)
    text = _MARKDOWN_RE.sub(lambda match: "\\" + match["markdown"], text)
    return text.replace("<", "\\<").replace(">", "\\>")


def _excerpt(text: str, budget: int) -> str:
    if _discord_character_count(text) <= budget:
        return text
    used = 0
    end = 0
    for index, character in enumerate(text):
        used += 2 if ord(character) > 0xFFFF else 1
        if used > budget - 1:
            break
        end = index + 1
    short = text[:end].rstrip()
    boundaries = list(re.finditer(r"(?<=[.!?])\s", short))
    boundary = boundaries[-1].start() if boundaries else short.rfind(" ")
    if boundary >= len(short) * 0.75:
        short = short[:boundary]
    return short.rstrip().rstrip("\\") + "…"


def build_definition_page(definition: UrbanDefinition, query: str) -> PresentationPage:
    link = (
        "[Urban Dictionary](https://www.urbandictionary.com/define.php?term="
        + quote(query, safe="")
        + ")"
    )
    meaning = _safe_text(definition.meaning)
    example = _safe_text(definition.example)
    budget = 3_900 - _discord_character_count(link) - 2
    if example:
        budget -= 2
        example_budget = min(_discord_character_count(example), 900, budget // 3)
        meaning = _excerpt(meaning, budget - example_budget)
        example = _excerpt(example, budget - _discord_character_count(meaning))
    else:
        meaning = _excerpt(meaning, budget)
    description = "\n\n".join(part for part in (meaning, example, link) if part)
    return PresentationPage(description=description)


class UrbanModule:
    def __init__(
        self,
        *,
        generation: int,
        group: str = "tools",
        service: UrbanDictionaryService | None = None,
    ) -> None:
        self._generation = generation
        self._group = group
        self._service = service or UrbanDictionaryService(
            endpoint=os.getenv("LUMI_URBAN_ENDPOINT", API_URL)
        )

    @property
    def manifest(self) -> Manifest:
        return Manifest(
            schema=2,
            module="urban",
            group=self._group,
            generation=self._generation,
            job_stream=f"lumi:jobs:{self._group}",
            display_name="Urban Dictionary",
            summary="Slang definitions.",
            presentation_mode="v2",
            version="1.0.2",
            commands=(
                CommandSpec(
                    root="urban",
                    handler="urban.lookup",
                    grammar=r"^(?:\s+(?P<query>[\s\S]+))?$",
                    captures=(
                        CaptureSpec(
                            "query",
                            "normalized_text",
                            required=False,
                            maximum=MAX_CAPTURE_CHARS,
                        ),
                    ),
                    help=HelpSpec(
                        ".urban <word or phrase>", "Look up an Urban Dictionary definition."
                    ),
                    acl_key="urban",
                    deadline_ms=20_000,
                    public=True,
                    context_fields=("prefix",),
                ),
            ),
        )

    async def start(self) -> None:
        await self._service.start()

    async def handle(
        self,
        request: TypedRequest,
        job_id: str,
        generation: int,
    ) -> ResultEnvelope:
        del generation
        query = normalize_query(str(request.arguments.get("query") or ""))
        prefix = str(request.event.get("prefix") or ".")
        if not query or query.casefold() == "help":
            return self._result(job_id, self._send(f"{prefix}urban <word or phrase>"))
        if len(query) > MAX_QUERY_CHARS:
            return self._result(job_id, self._send("Maximum 200 characters."))
        try:
            definitions = await self._service.search(query)
        except UrbanNoResultsError:
            return self._result(job_id, self._send("No definitions found."))
        except UrbanRateLimitedError:
            return self._result(
                job_id,
                self._send("Try again later."),
                error_code="rate_limited",
            )
        except Exception:
            return self._result(
                job_id,
                self._send("Unavailable."),
                error_code="provider",
            )
        pages = tuple(build_definition_page(definition, query) for definition in definitions)
        return self._result(job_id, DiscordIntent(kind="paginator", pages=pages))

    @staticmethod
    def _send(text: str) -> DiscordIntent:
        return DiscordIntent(kind="send", pages=(PresentationPage(description=text),))

    def _result(
        self,
        job_id: str,
        intent: DiscordIntent,
        *,
        error_code: str | None = None,
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="urban",
            group=self._group,
            generation=self._generation,
            status="ok" if error_code is None else "error",
            intents=(intent,),
            error_code=error_code,
            completed_at_ms=int(time.time() * 1_000),
        )

    async def close(self) -> None:
        await self._service.close()
