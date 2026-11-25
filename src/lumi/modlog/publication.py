import asyncio
import secrets
import time
from contextlib import suppress
from dataclasses import dataclass

from redis.exceptions import RedisError

from lumi.contracts import (
    DiscordActionResult, DiscordEffectGrant, DiscordIntent, MessageMutation, PresentationPage,
    TemporaryAttachment, decode, encode,
)
from lumi.contracts.models import EffectDelivery
from lumi.shared.discord_effects import (
    MUTATE_MESSAGE, PUBLISH_MESSAGE, effect_key, effect_receipt_key, mutation_lane, payload_digest,
    source_digest, stage_mutation,
)
from lumi.worker.actions import ActionUnavailable

from .evidence import FILE_PAGE, participants
from .models import profile_image, role_changes
from .presentation import render

APPROVAL_MS = 15_000


@dataclass(frozen=True, slots=True)
class Publication:
    job: str
    guild: int
    channel: int
    record: str
    revision: int
    config_revision: int
    generation: int
    expires: int
    nonce: str
    message: int
    nsfw: bool
    participants: tuple[int, ...]
    mode: str
    page: PresentationPage
    attachments: tuple[TemporaryAttachment, ...]


def normalized(row, bot_id):
    kind = row["kind"]
    if kind == "webhook" and (
        (bot_id and row.get("actor") == bot_id)
        or (
            not row.get("audit") and not row.get("actor") and row.get("operation") == "updated"
            and row.get("subject") == row.get("channel")
        )
    ):
        return None
    if kind.startswith("role_"):
        changes = role_changes(kind, row.get("changes", {}))
        if kind == "role_update" and not changes:
            return None
        return {**row, "changes": changes}
    if kind == "avatar":
        before, after = (profile_image(row.get(key)) for key in ("image_before", "image_after"))
        if before == after:
            return None
        return {**row, "image_before": before, "image_after": after}
    return row


async def submit(store, queue, actions, evidence, request, job, *, guild, record, revision, generation, clock):
    candidate = await store.publication_candidate(guild, record, revision, clock())
    if not candidate:
        return
    row, out, cfg = candidate
    if (
        row["kind"] not in cfg["events"] or row.get("channel") in cfg["excluded"]
        or row.get("parent_id") in cfg["excluded"]
    ):
        return
    bot_id = request.event.get("discord_application_user_id")
    if row["kind"] == "webhook" and row.get("actor") and (
        type(bot_id) is not int or bot_id <= 0
    ):
        return
    people = participants(row)
    relevant = people - {evidence.owner_id}
    if relevant and (
        evidence.block_reader is None or await evidence.block_reader.blocked(relevant)
    ):
        return
    target = cfg["destinations"].get(row["category"]) or cfg["destinations"].get("all") or 0
    row = normalized(row, bot_id)
    if row is None or (target and out.get("destination") not in {None, 0, target}):
        await store.transition(
            guild, record, {"ready"}, previous=out, expected_revision=revision, state="archived", due=0,
        )
        return
    if not target:
        return
    if target != request.event.get("channel_id"):
        return
    deadline = min(request.event["_expires_at_ms"], row["expires"])
    staged = await evidence.stage_files(job, guild, row.get("files", ())[:FILE_PAGE], deadline, optional=True)
    files = tuple(item for _, item in staged)
    try:
        expires = min(deadline, clock() + APPROVAL_MS, *(item.expires_at_ms for item in files))
        if expires <= clock():
            return
        plan = Publication(
            job, guild, target, record, revision, cfg["revision"], generation, expires,
            "ml:" + record[:21], out.get("message") or 0, bool(row.get("nsfw")),
            tuple(sorted(people)), row["format"],
            render(row, files={identifier: item.file_name for identifier, item in staged}), files,
        )
        await dispatch(store, queue, actions, plan, out, clock)
    finally:
        evidence.discard(job, files)


async def dispatch(store, queue, actions, plan, previous, clock):
    attempt = "effect:" + secrets.token_hex(16)
    key = effect_key("security", plan.job, attempt)
    message = plan.message or None
    kind = MUTATE_MESSAGE if message else PUBLISH_MESSAGE
    intent = DiscordIntent(
        "edit" if message else "send", pages=(plan.page,), attachments=plan.attachments,
        presentation_mode=plan.mode,
    )
    mutation = MessageMutation("edit") if message else None
    grant = DiscordEffectGrant(
        "modlog", plan.generation, kind, plan.guild, plan.channel, message,
        payload_digest(intent, ()), plan.expires,
        mutation_digest=source_digest(mutation),
        delivery=EffectDelivery(plan.participants, plan.nsfw, None if message else plan.nonce),
    )

    async def issue():
        remaining = plan.expires - clock()
        if remaining <= 0:
            return False
        async with asyncio.timeout(remaining / 1000):
            if message:
                lane = mutation_lane("security", "modlog", plan.guild, plan.channel, message)
                return await stage_mutation(queue, key, lane, encode(grant))
            return await queue.set(key, encode(grant), nx=True, px=remaining)

    out = await store.claim_publication(previous, plan, attempt, clock=clock, issue=issue)
    if not out:
        return
    try:
        response = await actions.request(
            kind=kind, job_id=plan.job, guild_id=plan.guild, channel_id=plan.channel,
            message_id=message, message_mutation=mutation, intent=intent, idempotency_key=attempt,
        )
        if await settle(store, out, response, clock()):
            return
    except (ActionUnavailable, RedisError, TimeoutError, OSError):
        pass
    finally:
        with suppress(RedisError):
            await queue.delete(key)
    await store.transition(
        plan.guild, plan.record, {"sending"}, previous=out,
        state="uncertain", due=max(out["due"], clock() + 30000),
    )


async def settle(store, out, response, now):
    disposition = response.status
    bound = response.data.get("delivery")
    if bound is not None:
        if not isinstance(bound, dict) or (
            bound.get("guild_id"), bound.get("channel_id"), bound.get("parent_id"),
            bound.get("attempt"), bound.get("outcome"),
        ) != (out["guild"], out["destination"], out.get("message") or None, out["attempt"], disposition):
            return False
        if response.ok and disposition in {"sent", "edited"}:
            message = bound.get("message_id")
            if type(message) is not int or message <= 0 or (out.get("message") and out["message"] != message):
                return False
            if disposition != ("edited" if out.get("message") else "sent"):
                return False
            await store.delivered(out["guild"], out["id"], out["revision"], message, now, attempt=out["attempt"])
            return True
        if response.ok or disposition not in {"rejected", "missing"}:
            return False
    elif response.ok or response.error_code not in {
        "unknown_job", "expired_job", "action_scope_mismatch", "capability_denied",
        "stale_generation", "effect_not_authorized", "policy_denied", "blocked", "guild_unavailable",
    }:
        return False
    await store.transition(
        out["guild"], out["id"], {"sending", "uncertain"}, previous=out,
        state="archived" if disposition == "missing" else "ready",
        due=0 if disposition == "missing" else now + 30000,
    )
    return True


async def receipt(queue, out):
    if not out.get("attempt"):
        return None
    try:
        raw = await queue.get(effect_receipt_key("security", "modlog", out["attempt"]))
        return decode(raw, DiscordActionResult) if raw else None
    except ValueError:
        return None


class Publisher:
    def __init__(self, store, queue, actions, evidence, clock):
        self.store, self.queue, self.actions, self.evidence, self.clock = store, queue, actions, evidence, clock
        self.lock = asyncio.Lock()
        self.next_send = 0.0

    async def send(self, request, job, **scope):
        async with self.lock:
            await asyncio.sleep(max(0.0, self.next_send - time.monotonic()))
            try:
                await submit(
                    self.store, self.queue, self.actions, self.evidence, request, job,
                    clock=self.clock, **scope,
                )
            finally:
                self.next_send = time.monotonic() + 1.0

    async def revoke(self, tx):
        offset = 0
        now = self.clock()
        while rows := await tx.scan("outbox", filters=[("state", "in", ("sending", "uncertain"))], offset=offset):
            keys = [
                effect_key("security", row["attempt_job"], row["attempt"])
                for row in rows
                if row.get("attempt") and row.get("attempt_job") and row.get("attempt_expires", 0) > now
            ]
            if keys:
                await self.queue.delete(*keys)
            offset += len(rows)
