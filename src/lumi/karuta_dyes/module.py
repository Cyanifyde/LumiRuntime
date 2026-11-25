from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import re
import secrets
import time
from pathlib import Path

from redis.asyncio import Redis

from lumi.contracts import (
    CaptureSpec,
    DiscordIntent,
    HelpSpec,
    Manifest,
    ReactionOfferSpec,
    ResultEnvelope,
    TemporaryAttachment,
    TypedRequest,
)

from .service import DyeReportService, DyeView

LOGGER = logging.getLogger(__name__)
KARUTA_BOT_ID = -2
ATTACHMENT_TTL_MS = 120_000
RELEASE_RECEIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class KarutaDyesModule:
    def __init__(
        self,
        *,
        generation: int,
        cache_redis: Redis,
        temp_root: Path,
        cache_root: Path,
        service: DyeReportService | None = None,
    ) -> None:
        self._generation = generation
        self._cache = cache_redis
        self._temp_root = temp_root.resolve()
        self._service = service or DyeReportService(cache_root)

    @property
    def manifest(self) -> Manifest:
        return Manifest(
            schema=2,
            module="karuta_dyes",
            group="cards",
            generation=self._generation,
            job_stream="lumi:jobs:cards",
            display_name="Karuta Dyes",
            summary="Dye purity, colours, brightness and glow.",
            help=HelpSpec(
                "React 🥦 on a Karuta dye view.",
                "Analyse dye purity, colours, brightness and Mystic glow.",
            ),
            presentation_mode="plain",
            reaction_offers=(
                ReactionOfferSpec(
                    key="dye_view",
                    callback="karuta_dyes.analyse",
                    pattern=(
                        r"(?s)^\s*Dye Details\s+\*\*(?P<dye_name>[^\r\n*]{1,100})\*\*\s+"
                        r"Owned by <@!?(?P<owner_id>[0-9]{17,20})>\s+"
                        r"Code:[ \t]*`\$(?P<dye_code>[a-z0-9]{1,20})`\s+"
                        r"Charges:[ \t]*`(?P<charges>[0-9]{1,9})`\s*$"
                    ),
                    emoji="🥦",
                    bot_ids=(KARUTA_BOT_ID,),
                    requires_embeds=True,
                    requires_image=True,
                    captures=(
                        CaptureSpec("dye_name", maximum=100),
                        CaptureSpec("owner_id", "snowflake"),
                        CaptureSpec("dye_code", maximum=20),
                        CaptureSpec("charges", "integer", minimum=0, maximum=999_999_999),
                    ),
                    context_fields=("primary_image_url",),


                    audience_fields=(),
                    claim_if_missing=True,
                    ttl_ms=900_000,
                    remove_unauthorized=True,
                    remove_after_dispatch=True,
                    presentation_mode="plain",
                ),
            ),
            version="1.0.1",
        )

    async def start(self) -> None:
        await asyncio.to_thread(self._temp_root.mkdir, parents=True, exist_ok=True)
        await self._service.start()

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        event = request.event
        deadline = event.get("_expires_at_ms", 0)
        if (
            generation != self._generation
            or request.handler != "karuta_dyes.analyse"
            or request.route != "karuta_dyes.dye_view"
            or event.get("dispatch") != "reaction_offer"
            or event.get("author_id") != KARUTA_BOT_ID
            or str(event.get("emoji", "")).replace("\ufe0f", "") != "🥦"
            or not re.fullmatch(r"[a-f0-9]{32}", str(event.get("offer_id", "")))
            or any(
                type(event.get(field)) is not int or event[field] <= 0
                for field in ("guild_id", "channel_id", "message_id", "actor_id")
            )
            or event.get("offer_owner_id") != event.get("actor_id")
            or type(deadline) is not int
            or deadline <= int(time.time() * 1000)
        ):
            return self._result(job_id)
        try:
            if (
                type(request.arguments.get("owner_id")) is not int
                or request.arguments["owner_id"] <= 0
                or type(request.arguments.get("charges")) is not int
                or not 0 <= request.arguments["charges"] <= 999_999_999
            ):
                raise ValueError("Invalid dye metadata")
            view = DyeView(
                request.arguments.get("dye_name"),
                "$" + str(request.arguments.get("dye_code", "")),
                event.get("primary_image_url"),
            )
        except (ValueError, TypeError):
            return self._result(job_id)

        try:
            async with asyncio.timeout(max(0, (deadline - time.time() * 1000) / 1000)):
                receipt = f"lumi:karuta-dyes:offer:{event['guild_id']}:{event['offer_id']}"
                first = await self._cache.set(receipt, job_id, nx=True, ex=86_400)
                if not first:
                    previous = await self._cache.get(receipt)
                    if isinstance(previous, bytes):
                        previous = previous.decode("utf-8")
                    if previous != job_id:
                        return self._result(job_id)
                data = await self._service.image(view)
                identifier = secrets.token_hex(24)
                await asyncio.to_thread((self._temp_root / identifier).write_bytes, data)
                attachment = TemporaryAttachment(
                    reference=f"tmp:{identifier}",
                    file_name=f"dye-{view.code[1:]}.png",
                    sha256=hashlib.sha256(data).hexdigest(),
                    size=len(data),
                    expires_at_ms=int(time.time() * 1000) + ATTACHMENT_TTL_MS,
                )
                return self._result(
                    job_id,
                    DiscordIntent(
                        kind="send", attachments=(attachment,), presentation_mode="plain"
                    ),
                )
        except Exception:


            with contextlib.suppress(Exception):
                await self._cache.eval(RELEASE_RECEIPT, 1, receipt, job_id)
            LOGGER.warning("karuta_dye_analysis_unavailable", exc_info=True)
            return self._result(
                job_id,
                DiscordIntent(kind="send", content="Unavailable.", presentation_mode="plain"),
                error_code="dye_unavailable",
            )

    def _result(
        self,
        job_id: str,
        *intents: DiscordIntent,
        error_code: str | None = None,
    ) -> ResultEnvelope:
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="karuta_dyes",
            group="cards",
            generation=self._generation,
            status="error" if error_code else "ok",
            intents=intents,
            error_code=error_code,
            completed_at_ms=int(time.time() * 1000),
        )

    async def close(self) -> None:
        await self._service.close()
