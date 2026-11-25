from __future__ import annotations

import asyncio
import re
import time
import unicodedata
import uuid
from collections import defaultdict

from redis.asyncio import Redis

from lumi.ai import AIClient, AIUnavailable, profile_hash
from lumi.contracts import (
    AICallRequest,
    AIProfileSpec,
    AITextVariant,
    CaptureSpec,
    CommandSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    PresentationPage,
    ResultEnvelope,
    TypedRequest,
)
from lumi.contracts.runtime_control import BLOCK_CONFIRMATION

from .actor_block import ActorBlockClient
from .translate_safety import contains_dangerous, safety_views

MAX_SOURCE = 1_000
MAX_TARGET = 64
COOLDOWN_SECONDS = 10
_TO_MARKER = re.compile(r"(?<!\S)--to(?:\s+|=|$)", re.IGNORECASE)
_MENTION = re.compile(r"<@!?\d+>|<@&\d+>|<#\d+>|@everyone|@here", re.IGNORECASE)

TRANSLATE_PROFILE = AIProfileSpec(
    key="translate.v1",
    version="1.4.0",
    model="gemma4:31b",
    system_prompt=(
        "REDACTED"
    ),
    input_schema={
        "type": "object",
        "properties": {
            "source_text": {"type": "string", "minLength": 1, "maxLength": 1_000},
            "target_language": {"type": "string", "minLength": 1, "maxLength": 64},
            "safety_views": {
                "type": "array",
                "items": {"type": "string", "minLength": 1, "maxLength": 1_000},
                "minItems": 0,
                "maxItems": 4,
            },
        },
        "required": ["source_text", "target_language", "safety_views"],
        "additionalProperties": False,
    },
    response_mode="text",
    text_variants=(
        AITextVariant(
            outcome="workaround",
            pattern=r"^(?is:\s*WORKAROUND(?:\n[\s\S]*)?\s*)$",
            terminal=True,
        ),
        AITextVariant(
            outcome="yes",
            pattern=r"^(?is:\s*YES(?:\n[\s\S]*)?\s*)$",
            terminal=True,
        ),
        AITextVariant(
            outcome="no",
            pattern=(
                r"^(?is:\s*NO[ \t]*\n"
                r"(?P<language>[^\n<>:{}\[\]`]{1,64})\n"
                r"(?P<translation>[\s\S]+?)\s*)$"
            ),
            captures=(
                CaptureSpec("language", "text", maximum=64),
                CaptureSpec("translation", "text", maximum=16_000),
            ),
        ),
    ),
    repair_prompt=(
        "REDACTED"
    ),
    deadline_ms=65_000,
    max_attempts=2,
    temperature=0.0,
    thinking=False,
    max_output_tokens=1_200,
)


class TranslateModule:
    def __init__(
        self,
        *,
        generation: int,
        queue_redis: Redis,
        group: str = "tools",
        ai_client: AIClient | None = None,
    ) -> None:
        self._generation = generation
        self._group = group
        self._ai = ai_client or AIClient()
        self._actor_block = ActorBlockClient(queue_redis)
        self._cooldowns: dict[int, float] = {}
        self._locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._locally_blocked_until: dict[int, float] = {}

    @property
    def manifest(self) -> Manifest:
        return Manifest(
            schema=2,
            module="translate",
            group=self._group,
            generation=self._generation,
            job_stream=f"lumi:jobs:{self._group}",
            display_name="Translate",
            summary="Translate text.",
            presentation_mode="v2",
            commands=(
                CommandSpec(
                    root="translate",
                    handler="translate.translate",
                    grammar=r"^(?:\s+(?P<body>[\s\S]+))?$",
                    captures=(CaptureSpec("body", "text", required=False, maximum=2_000),),
                    help=HelpSpec(
                        ".translate <text> [--to <language>]\n"
                        ".translate [language | --to <language>]",
                        "Translate text or a reply; omit the language for English.",
                    ),
                    acl_key="translate",
                    deadline_ms=75_000,
                    public=True,
                    actor_admission=True,
                    context_fields=("prefix", "reference_message_id", "reply_content"),
                ),
            ),
            version="1.0.2",
        )

    @property
    def ai_profiles(self) -> tuple[AIProfileSpec, ...]:
        return (TRANSLATE_PROFILE,)

    async def start(self) -> None:
        await self._ai.start()

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        del generation
        actor_id = int(request.event.get("actor_id") or 0)
        async with self._locks[actor_id]:
            if self._locally_blocked_until.get(actor_id, 0) > time.time():
                return self._result(job_id, (), error_code="blocked")
            source, target = self._parse_input(request)
            if not source:
                return self._text(job_id, "No text to translate.")
            if len(source) > MAX_SOURCE:
                return self._text(job_id, "Maximum 1000 characters.")
            if target is None:
                return self._text(job_id, "Use --to <language>.", error_code="target")
            if contains_dangerous(source) or contains_dangerous(target):
                return self._text(job_id, "Translation declined.", error_code="declined")
            now = time.monotonic()
            cooldown = self._cooldowns.get(actor_id, 0)
            if cooldown > now:
                remaining = max(1, int(cooldown - now + 0.999))
                return self._text(job_id, f"Try again in {remaining} seconds.")
            self._cooldowns[actor_id] = now + COOLDOWN_SECONDS
            expires_at_ms = int(request.event.get("_expires_at_ms") or 0)
            if expires_at_ms <= int(time.time() * 1_000):
                return self._text(job_id, "Unavailable.", error_code="expired")
            call = AICallRequest(
                schema=1,
                call_id=uuid.uuid4().hex,
                profile_key=TRANSLATE_PROFILE.key,
                profile_hash=profile_hash(TRANSLATE_PROFILE),
                caller_module="translate",
                caller_group=self._group,
                caller_generation=self._generation,
                actor_id=actor_id,
                expires_at_ms=expires_at_ms,
                input={
                    "source_text": source,
                    "target_language": target,
                    "safety_views": list(safety_views(source)),
                },
            )
            try:
                response = await self._ai.call(call)
            except AIUnavailable:
                return self._text(job_id, "Unavailable.", error_code="ai_unavailable")
            if response.status == "busy":
                return self._text(job_id, "Busy. Try again.", error_code="busy")
            if response.status != "ok":
                return self._text(job_id, "Unavailable.", error_code=response.status)
            if response.outcome == "workaround":
                blocked = await self._actor_block.request(job_id, expires_at_ms)
                if blocked is not None:
                    self._locally_blocked_until[actor_id] = blocked.expires / 1000
                    return self._text(
                        job_id,
                        "Blacklisted for 20 minutes.",
                        error_code=BLOCK_CONFIRMATION,
                    )
                return self._text(job_id, "Translation declined.", error_code="declined")
            if response.outcome == "yes":
                return self._text(job_id, "Translation declined.", error_code="declined")
            if response.outcome != "no":
                return self._text(job_id, "Unavailable.", error_code="invalid_outcome")
            language = self._clean_language(str(response.captures.get("language") or ""))
            translation = str(response.captures.get("translation") or "").strip()
            if not language or not translation:
                return self._text(job_id, "Unavailable.", error_code="invalid_output")
            if contains_dangerous(translation):
                return self._text(job_id, "Translation declined.", error_code="declined")
            shown_target = self._clean_language(target)
            if not shown_target:
                return self._text(job_id, "Unavailable.", error_code="invalid_output")
            chunks = self._split_translation(translation)
            pages = tuple(
                PresentationPage(
                    title=f"{language} - {shown_target}" if index == 0 else None,
                    description=chunk,
                )
                for index, chunk in enumerate(chunks)
            )
            return self._result(
                job_id,
                (
                    DiscordIntent(
                        kind="send" if len(pages) == 1 else "send_parts",
                        pages=pages,
                    ),
                ),
            )

    @classmethod
    def _parse_input(cls, request: TypedRequest) -> tuple[str, str | None]:
        body = str(request.arguments.get("body") or "").strip()
        matches = tuple(_TO_MARKER.finditer(body))
        marker = matches[-1] if matches else None
        reference_id = request.event.get("reference_message_id")
        if reference_id:
            source = str(request.event.get("reply_content") or "").strip()
            target = cls._normalize_target(body[marker.end() :] if marker else body or "English")
            return source, target
        if marker is None:
            return body, "English"
        source = body[: marker.start()].strip()
        target = cls._normalize_target(body[marker.end() :])
        return source, target

    @staticmethod
    def _normalize_target(value: str) -> str | None:
        normalized = unicodedata.normalize("NFKC", value)
        normalized = "".join(
            character for character in normalized if unicodedata.category(character)[0] != "C"
        )
        normalized = " ".join(normalized.split())
        if not normalized or len(normalized) > MAX_TARGET:
            return None
        return normalized

    @staticmethod
    def _clean_language(value: str) -> str:
        cleaned = _MENTION.sub("", value)
        cleaned = re.sub(r"[*_~`|><#\\\[\]]", "", cleaned)
        return " ".join(cleaned.split())[:MAX_TARGET].strip()

    @staticmethod
    def _split_translation(value: str, maximum: int = 3_800) -> tuple[str, ...]:
        remaining = value
        parts: list[str] = []
        while len(remaining) > maximum:
            split_at = remaining.rfind("\n", 0, maximum + 1)
            if split_at < maximum // 2:
                split_at = remaining.rfind(" ", 0, maximum + 1)
            if split_at < maximum // 2:
                split_at = maximum
            parts.append(remaining[:split_at].rstrip())
            remaining = remaining[split_at:].lstrip("\n")
        if remaining:
            parts.append(remaining)
        return tuple(parts)

    def _text(self, job_id: str, text: str, *, error_code: str | None = None) -> ResultEnvelope:
        return self._result(
            job_id,
            (DiscordIntent(kind="send", pages=(PresentationPage(description=text),)),),
            error_code=error_code,
        )

    def _result(
        self,
        job_id: str,
        intents: tuple[DiscordIntent, ...],
        *,
        error_code: str | None = None,
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="translate",
            group=self._group,
            generation=self._generation,
            status="ok" if error_code is None else "error",
            intents=intents,
            error_code=error_code,
            completed_at_ms=int(time.time() * 1_000),
        )

    async def close(self) -> None:
        await self._ai.close()
