from __future__ import annotations

import time
from decimal import Decimal

import msgspec

from lumi.contracts import (
    DiscordIntent,
    HelpSpec,
    Manifest,
    MatcherExpr,
    PassiveMatcherSpec,
    PreparedReplyOffer,
    PresentationPage,
    ReactionOfferSpec,
    ResultEnvelope,
    TypedRequest,
)

from .engine import CalculatorEvaluator
from .models import CANDIDATE_PATTERN, DATE_PATTERN, EMOJI, TTL_MS, CalculatorOfferSpec


SOURCE_MATCH = MatcherExpr("all", children=(
    MatcherExpr("regex", field="content", value=CANDIDATE_PATTERN),
    MatcherExpr("regex", field="content", value=r"[+\-−*/×÷^%()＋－＊／＾％（）]"),
    MatcherExpr("not", children=(MatcherExpr(
        "regex", field="content",
        value=rf"^[ \t`]*{DATE_PATTERN}[ \t`]*$",
    ),)),
))


def answer_page(offer: CalculatorOfferSpec) -> PresentationPage:
    value = Decimal(offer.value)
    if not value.is_finite() or abs(value) > Decimal("9e99"):
        raise ValueError("Invalid calculator result")
    text = format(value, ",f") if value.is_zero() or -12 <= value.adjusted() <= 30 else str(value)
    if "E" not in text and "." in text:
        text = text.rstrip("0").rstrip(".")
    return PresentationPage(
        description=f"{offer.expression} {'≈' if offer.approximate else '='} {text}"
    )


class CalculatorModule:
    def __init__(self, *, generation: int, group: str = "tools", evaluator=None) -> None:
        self._generation, self._group = generation, group
        self._evaluator = evaluator or CalculatorEvaluator()

    @property
    def manifest(self) -> Manifest:
        return Manifest(
            schema=2,
            module="calculator",
            group=self._group,
            generation=self._generation,
            job_stream=f"lumi:jobs:{self._group}",
            display_name="Calculator",
            summary="Reaction maths.",
            version="1.0.2",
            help=HelpSpec("React 🧮 on maths.", "React 🧮 on maths."),
            passive=tuple(
                PassiveMatcherSpec(
                    route=f"calculator.prepare_{kind}",
                    handler="calculator.prepare",
                    event=f"message_{kind}",
                    expression=SOURCE_MATCH,
                    context_fields=("content", "source_revision_ms"),
                    prepare_reply="expression",
                    deadline_ms=5_000,
                )
                for kind in ("create", "edit")
            ),
            reaction_offers=(
                ReactionOfferSpec(
                    key="expression",
                    callback="calculator.reply",
                    pattern=r"^$",
                    emoji=EMOJI,
                    prepared_only=True,
                    preparation_cooldown_ms=3_000,
                    audience_fields=(),
                    claim_if_missing=True,
                    ttl_ms=TTL_MS,
                    deadline_ms=5_000,
                ),
            ),
        )

    async def start(self) -> None:
        await self._evaluator.start()

    async def close(self) -> None:
        await self._evaluator.close()

    async def handle(self, request: TypedRequest, job_id: str, generation: int) -> ResultEnvelope:
        del generation
        offer = None
        intents = ()
        if int(request.event.get("_expires_at_ms") or 2**63) <= int(time.time() * 1_000):
            return self._result(job_id)
        if request.handler == "calculator.prepare" and request.event.get("dispatch") == "passive":
            try:
                raw = str(request.event.get("content") or "")
                evaluated = await self._evaluator.calculate(raw)
                if evaluated is not None:
                    offer = PreparedReplyOffer(
                        evaluated.source_hash,
                        int(request.event.get("source_revision_ms") or 0),
                        DiscordIntent(kind="send", pages=(answer_page(evaluated),)),
                    )
            except Exception:
                pass
        elif (
            request.handler == "calculator.reply"
            and request.event.get("dispatch") == "reaction_offer"
        ):
            try:
                saved = msgspec.convert(request.arguments, type=PreparedReplyOffer, strict=True)
                intents = (saved.reply,)
            except ValueError:
                pass
        return self._result(job_id, offer=offer, intents=intents)

    def _result(self, job_id, *, offer=None, intents=()):
        return ResultEnvelope(
            schema=1,
            job_id=job_id,
            module="calculator",
            group=self._group,
            generation=self._generation,
            status="ok",
            intents=intents,
            prepared_reply_offer=offer,
            completed_at_ms=int(time.time() * 1_000),
        )
