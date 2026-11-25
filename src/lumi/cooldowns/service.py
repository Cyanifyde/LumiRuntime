from __future__ import annotations

import asyncio
import logging
import re
import time
import weakref

from lumi.contracts import (
    DiscordActionResult,
    DiscordIntent,
    MentionBatchPlan,
    MentionBatchRef,
    MentionRecipient,
    MentionTemplate,
    ScheduledCallbackSpec,
    decode,
)
from lumi.shared.mention_batches import projected_receipt_key, publish_plan, receipt_identity
from lumi.worker.actions import ActionUnavailable
from lumi.worker.scheduler import ScheduledCallbackClient

from .models import CAPTIONS, DM_FAILURES, MAX_RECIPIENTS, TRACKERS

LOGGER = logging.getLogger(__name__)
IDENTIFIER = re.compile(r"^[a-f0-9]{32}$")


UNCERTAIN_GRACE_MS = 30_000
ACTION_WINDOW_MS = 15_000
DEFINITE_DELIVERY_FAILURES = DM_FAILURES | {
    "action_scope_mismatch", "blocked", "capability_denied", "expired_job", "stale_generation", "unknown_job",
}


def flush_spec(tracker):
    return ScheduledCallbackSpec(
        tracker + ".cooldown_flush",
        tracker + ".cooldown_flush",
        presentation_mode="plain",
        scope="guild",
        acl_key=TRACKERS[tracker][0],
        mention_templates=tuple(
            MentionTemplate(kind, CAPTIONS[tracker, kind]) for kind in TRACKERS[tracker][1]
        ),
    )


async def deliver_reminder(
    *, tracker, store, actions, coalescer, guild, user, kind, revision, job, text
):
    state = await store.scheduled_subscription(guild, user, kind, revision)
    if state is None:
        return
    delivered = uncertain = False
    if state.delivery_mode == "dm":
        try:
            result = await actions.request(
                job_id=job,
                kind="send_dm",
                guild_id=guild,
                member_id=user,
                channel_id=state.channel_id,
                intent=DiscordIntent("send", content=text, presentation_mode="plain"),
                idempotency_key=f"reminder:{kind}:{guild}:{user}:{revision}:dm",
            )
            delivered = result.ok
            uncertain = not delivered and result.error_code not in DM_FAILURES
        except ActionUnavailable:
            uncertain = True
    if not delivered and not uncertain and state.channel_id:
        if coalescer is None:
            raise RuntimeError("channel cooldown coalescer unavailable")
        await coalescer.enqueue(tracker, guild, user, kind, revision, state.delivery_mode)


        return
    await store.finish_timer(guild, user, kind, revision)


class CooldownCoalescer:
    def __init__(self, store, queue, generation, *, clock=None):
        self.store, self.queue = store, queue
        self.generation = generation
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.clients = {
            tracker: ScheduledCallbackClient(
                queue, module=tracker, group="cards", generation=generation
            )
            for tracker in TRACKERS
        }
        self._task = None
        self._drained = self._working = False
        self._last_recovery = self._last_cleanup = 0
        self._cursors = {}
        self._flush_locks = weakref.WeakValueDictionary()

    async def start(self):
        self._task = asyncio.create_task(self.run(), name="card-cooldown-coalescer")

    async def close(self):
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def begin_drain(self):
        self._drained = True

    def resume(self):
        self._drained = False

    def drain_ready(self):
        return not self._working

    async def run(self):
        while True:
            if not self._drained:
                self._working = True
                try:
                    await self.tick()
                except Exception:
                    LOGGER.exception("card_cooldown_reconcile_failed")
                finally:
                    self._working = False
            await asyncio.sleep(1)

    async def enqueue(self, tracker, guild, user, kind, revision, mode):
        identifier = await self.store.enroll(tracker, guild, user, kind, revision, mode)
        if identifier:
            try:
                await self.publish()
            except Exception:

                LOGGER.exception("card_cooldown_schedule_failed")
        return identifier

    async def publish(self):
        for batch, dispatch_revision in await self.store.schedules():
            await self.clients[batch.tracker].publish(
                operation="upsert",
                schedule_key=f"{batch.tracker}:batch:{batch.id}",
                route=batch.tracker + ".cooldown_flush",
                revision=dispatch_revision,
                guild_id=batch.guild_id,
                channel_id=batch.channel_id,
                actor_id=0,
                due_at_ms=max(batch.flush_at_ms, self.clock()),
                expires_at_ms=batch.expires_at_ms,
                arguments={"batch": batch.id, "revision": batch.revision},
            )

    async def flush(self, tracker, request, job, actions):
        identifier = request.arguments.get("batch")
        revision = request.arguments.get("revision")
        if (
            not isinstance(identifier, str) or not IDENTIFIER.fullmatch(identifier)
            or type(revision) is not int or revision <= 0 or tracker not in TRACKERS
        ):
            return
        async with self._flush_locks.setdefault(identifier, asyncio.Lock()):
            await self._flush(tracker, request.event, job, actions, identifier, revision)

    async def _flush(self, tracker, event, job, actions, identifier, revision):
        batch = await self.store.get(identifier)
        if (
            batch is None
            or batch.tracker != tracker
            or batch.revision != revision
            or event.get("dispatch") != "scheduled"
            or event.get("actor_id") != 0
            or event.get("guild_id") != batch.guild_id
            or event.get("channel_id") != batch.channel_id
            or event.get("schedule_key") != f"{tracker}:batch:{batch.id}"
        ):
            return
        batch = await self.store.seal(identifier)
        if batch is None or batch.state != "ready" or self.clock() >= batch.expires_at_ms:
            await self.store.settle()
            return
        candidates = await self.store.eligible(batch)
        if len(candidates) > MAX_RECIPIENTS:
            raise ValueError("oversized cooldown batch")
        permitted = [
            item for item in candidates
            if item.mode == "channel" or (item.mode == "dm" and await self.definite_dm_failure(
                tracker, batch.kind, batch.guild_id, item.user_id, item.revision,
            ))
        ]
        reference = MentionBatchRef(batch.id, batch.revision)
        if permitted:
            wanted = tuple(dict.fromkeys(item.user_id for item in permitted))
            try:
                ready = await actions.request(
                    job_id=job,
                    kind="read_mention_batch_members",
                    guild_id=batch.guild_id,
                    channel_id=batch.channel_id,
                    member_ids=wanted,
                    mention_batch=reference,
                    idempotency_key=f"probe:{receipt_identity(reference)}",
                )
            except (ActionUnavailable, TimeoutError):
                return
            found = ready.data.get("member_ids")
            if not ready.ok or not isinstance(found, list) or any(
                type(uid) is not int or uid not in wanted for uid in found
            ):
                return
            users = set(found)
            permitted = [item for item in permitted if item.user_id in users]
        deadline = min(batch.expires_at_ms, self.clock() + ACTION_WINDOW_MS)
        included = await self.store.begin_send(batch, permitted)
        if not included:
            await self.store.settle()
            return
        plan = MentionBatchPlan(
            reference, tracker, "cards", job, self.generation, batch.guild_id, batch.channel_id,
            batch.kind, batch.flush_at_ms, deadline,
            tuple(MentionRecipient(item.user_id, min(deadline, item.expires_at_ms)) for item in included),
        )
        if not await publish_plan(self.queue, plan, self.clock()):
            return
        try:
            result = await actions.request(
                job_id=job,
                kind="send_mention_batch",
                guild_id=batch.guild_id,
                channel_id=batch.channel_id,
                mention_batch=reference,
                idempotency_key=receipt_identity(reference),
            )
        except (ActionUnavailable, TimeoutError):
            return
        await self.record_receipt(batch, result, {item.user_id for item in included})
        await self.store.settle()

    async def definite_dm_failure(self, tracker, kind, guild, user, revision):
        receipt = await self.queue.get(
            f"lumi:edge-action-receipt:{tracker}:reminder:{kind}:{guild}:{user}:{revision}:dm"
        )
        try:
            result = decode(receipt, DiscordActionResult) if receipt else None
        except ValueError:
            return False
        return result is not None and not result.ok and result.error_code in DM_FAILURES

    async def record_receipt(self, batch, result, claimed):
        users = result.data.get("recipient_ids", [])
        if not isinstance(users, list) or any(type(user) is not int or user not in claimed for user in users):
            return False
        if len(users) != len(set(users)):
            return False
        if result.status == "sent" and result.ok:
            if (
                not users or result.data.get("channel_id") != batch.channel_id
                or type(result.data.get("message_id")) is not int or result.data["message_id"] <= 0
            ):
                return False
            await self.store.record_result(batch, "sent", result.data)
        elif result.status == "skipped" and result.ok and not users:
            await self.store.record_result(batch, "skipped", {"recipient_ids": []})
        elif not result.ok and result.error_code in DEFINITE_DELIVERY_FAILURES:
            await self.store.record_result(batch, "failed", dict(recipient_ids=users, error_code=result.error_code))
        elif not result.ok and result.error_code == "ambiguous":
            await self.store.record_result(batch, "uncertain", dict(recipient_ids=users or sorted(claimed), error_code="ambiguous"))
        else:
            return False
        return True

    async def reconcile_sending(self):
        for batch, attempted_at in await self.store.sending():
            reference = MentionBatchRef(batch.id, batch.revision)
            receipt = await self.queue.get(projected_receipt_key("cards", batch.tracker, receipt_identity(reference)))
            if receipt is None:

                receipt = await self.queue.get(
                    f"lumi:edge-action-receipt:{batch.tracker}:cooldown-batch:{batch.id}:{batch.revision}"
                )
            try:
                result = decode(receipt, DiscordActionResult) if receipt else None
            except ValueError:
                result = None
            claimed = set((batch.outcome or {}).get("recipient_ids", ()))
            if result is not None and await self.record_receipt(batch, result, claimed):
                continue
            if self.clock() - attempted_at >= UNCERTAIN_GRACE_MS:
                await self.store.record_result(batch, "uncertain", dict(
                    recipient_ids=sorted(claimed), error_code="ambiguous",
                ))

    async def recover_sources(self):
        for tracker, (_acl, kinds) in TRACKERS.items():
            for kind in kinds:
                for mode in ("channel", "dm"):
                    key = tracker, kind, mode
                    candidates = await self.store.candidates(
                        tracker, kind, mode, self._cursors.get(key, 0)
                    )
                    self._cursors[key] = int(candidates[-1][1]) if len(candidates) == 100 else 0
                    for guild, user, revision in candidates:
                        if mode == "dm" and not await self.definite_dm_failure(tracker, kind, guild, user, revision):
                            continue
                        await self.store.enroll(
                            tracker, int(guild), int(user), kind, int(revision), mode
                        )

    async def tick(self):
        await self.reconcile_sending()
        await self.store.settle()
        if self.clock() - self._last_recovery >= 5_000:
            await self.recover_sources()
            self._last_recovery = self.clock()
        await self.publish()
        if self.clock() - self._last_cleanup >= 60_000:
            await self.store.cleanup()
            self._last_cleanup = self.clock()
