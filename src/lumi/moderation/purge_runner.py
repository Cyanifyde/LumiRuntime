import asyncio
import logging
import re
import secrets
from contextlib import suppress

import msgspec
from asyncmy.errors import MySQLError
from redis.exceptions import RedisError

from lumi.contracts import PurgeSpec
from lumi.contracts.channel_messages import MESSAGE_PAGE
from lumi.ui_keys import ui_key
from lumi.worker.actions import ActionUnavailable

from .purge import duplicate_observation, matches_observation

ROUTE = "moderation.purge_work"
CALLBACK_MS = 30_000
PACE_MS = 1000
RECONCILE_MS = 15000
BULK_AGE_MS = 14 * 86400000 - 60000
RUNNABLE = {"queued", "running", "cancelling"}
TRANSIENT = {"rate_limited", "access_changed", "expired_job", "effect_not_authorized"}
UNCERTAIN = {"uncertain", "unavailable", "action_in_progress", "discord_action_failed", "invalid_action", "ambiguous"}
LOGGER = logging.getLogger(__name__)


class LostLease(RuntimeError):
    pass


class NativeFailure(RuntimeError):
    def __init__(self, code, data):
        super().__init__(code)
        self.code, self.data = code, data


def observations(result, key):
    if not result.ok:
        raise NativeFailure(result.error_code, result.data)
    return result.data[key]


class PurgeRunner:
    mode = "purge"
    route = ROUTE
    runnable = RUNNABLE

    def __init__(self, store, transport, scheduler, cache, group, clock):
        self.store, self.transport, self.scheduler, self.cache = store, transport, scheduler, cache
        self.group, self.clock = group, clock
        self.wake = asyncio.Event()
        self.pending = None
        self.active = self.draining = False
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self.run(), name=f"moderation-{self.mode}")

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def run(self):
        while True:
            self.wake.clear()
            if not self.draining:
                try:
                    await self.schedule_due()
                except (MySQLError, OSError, TimeoutError, RedisError):
                    LOGGER.warning("purge_schedule_unavailable")
            with suppress(TimeoutError):
                await asyncio.wait_for(self.wake.wait(), timeout=PACE_MS / 1000)

    async def schedule_due(self):
        now = self.clock()
        if self.active or self.draining or (self.pending and now < self.pending[2]):
            return
        rows = await self.store.ready(now, mode=self.mode)
        if not rows:
            return
        row = rows[0]
        expires = now + CALLBACK_MS
        pending = row["id"], row["revision"], expires
        self.pending = pending
        label = "work" if self.mode == "purge" else f"{self.mode}-work"
        await self.scheduler.publish(
            operation="upsert", schedule_key=f"moderation:{label}:{row['id']}:{secrets.token_hex(16)}",
            route=self.route, revision=1, guild_id=row["guild"], channel_id=row["channel"], actor_id=row["actor"],
            due_at_ms=now, expires_at_ms=expires, arguments={"batch": row["id"], "revision": row["revision"]},
        )

    async def callback(self, request, job):
        event, arguments = request.event, request.arguments
        revision, batch = arguments.get("revision"), arguments.get("batch")
        deadline = event.get("_expires_at_ms", 0)
        if (self.draining or event.get("dispatch") != "scheduled" or self.clock() >= deadline
                or not isinstance(batch, str) or not re.fullmatch(r"[a-f0-9]{8}", batch)
                or type(revision) is not int or revision <= 0):
            return
        async with self.store.exclusive() as acquired:
            if not acquired or self.draining or self.clock() >= deadline:
                return
            self.active = True
            try:
                if self.pending and self.pending[:2] == (batch, revision):
                    self.pending = None
                row = await self.store.get(event.get("guild_id"), event.get("channel_id"), batch)
                if (not row or row["mode"] != self.mode or row["revision"] != revision
                        or row["actor"] != event.get("actor_id") or row["channel"] != event.get("channel_id")
                        or row["state"] not in self.runnable or row["next"] > self.clock()):
                    return
                row = await self.store.claim(row, self.clock())
                if row:
                    await self.tick(row, job, deadline)
            finally:
                self.active = False
                self.wake.set()

    async def checkpoint(self, row, *, release=False):
        if not await self.store.save(row, self.clock(), release=release):
            raise LostLease

    async def fence(self, row, states=None):
        current = await self.store.get(row["guild"], row["channel"], row["id"])
        if (not current or current["revision"] != row["revision"] or current["lease_token"] != row["lease_token"]
                or current["lease_until"] <= self.clock() or current["state"] not in (self.runnable if states is None else states)):
            raise LostLease

    async def tick(self, row, job, deadline):
        row["served"] = self.clock()
        try:
            if row["state"] == "cancelling":
                if row.get("sending"):
                    await self.reconcile(row, job)
                if not row.get("sending"):
                    row["state"] = "cancelled"
            else:
                if not row["message"]:
                    value = await self.cache.get(ui_key("message", row["token"], self.group))
                    if not value:
                        row["next"] = self.clock() + 5000
                        await self.checkpoint(row, release=True)
                        return
                    row["message"] = int(value)
                    await self.checkpoint(row)
                row.pop("error", None)
                row["state"] = "running"
                await self.step(row, job, deadline, msgspec.convert(row["spec"], type=PurgeSpec))
        except LostLease:
            return
        except NativeFailure as exc:
            if exc.code in TRANSIENT:
                row["next"] = self.clock() + max(PACE_MS, exc.data.get("retry_after_ms", 0))
            elif exc.code in UNCERTAIN:
                row["next"] = self.clock() + RECONCILE_MS
            else:
                row.update(state="paused", error="Missing permission." if exc.code == "permission_denied" else "Could not confirm. Use purge resume.")
        except (ActionUnavailable, OSError, TimeoutError, RedisError):
            row["next"] = self.clock() + RECONCILE_MS
        with suppress(LostLease):
            row["next"] = max(row["next"], self.clock() + PACE_MS)
            await self.checkpoint(row)
            await self.progress(row, job, deadline)
            await self.checkpoint(row, release=True)

    async def window(self, row, job, before, spec, *, limit=MESSAGE_PAGE):
        result = await self.transport.history(row, job, before, limit=limit, content=True,
                                              author_roles=bool(spec.roles or spec.exclude_roles))
        messages = observations(result, "messages")
        return sorted(messages, key=lambda message: message["id"], reverse=True)

    async def exact(self, row, job, message, *, spec=None):
        result = await self.transport.observe(row, job, message, content=True,
                                              author_roles=bool(spec and (spec.roles or spec.exclude_roles)))
        hits = observations(result, "messages")
        return next((hit for hit in hits if hit["id"] == message), None)

    async def step(self, row, job, deadline, spec):
        if row.get("sending"):
            await self.reconcile(row, job)
            return
        observed = None
        if not row["pending"] and not row["exhausted"]:
            remaining = MESSAGE_PAGE if spec.limit is None else min(MESSAGE_PAGE, spec.limit - row["inspected"])
            messages = await self.window(row, job, row["cursor"], spec, limit=remaining) if remaining > 0 else []
            row["individual"] = []
            for message in messages:
                identifier = message["id"]
                row["cursor"] = min(row["cursor"], identifier)
                if identifier <= row["after"]:
                    row["exhausted"] = True
                    break
                row["inspected"] += 1
                if matches_observation(message, spec):
                    row["pending"].append(identifier)
                    if message["created_at_ms"] <= self.clock() - BULK_AGE_MS:
                        row["individual"].append(identifier)
                else:
                    row["skipped"] += 1
            if remaining <= 0 or len(messages) < remaining or (spec.limit is not None and row["inspected"] >= spec.limit):
                row["exhausted"] = True
            await self.checkpoint(row)
            if not (spec.keep_pins or spec.roles or spec.exclude_roles or spec.flags or spec.text):


                observed = {message["id"]: message for message in messages}
        if row["pending"]:
            await self.delete_pending(row, job, deadline, spec, observed=observed)
        elif row["exhausted"]:
            await self.finish(row, job, deadline, spec)

    async def delete_pending(self, row, job, deadline, spec, *, observed=None):
        individual = set(row["individual"])
        first = row["pending"][0]
        if observed is not None:
            fresh, candidates = observed, [first] if first in individual else row["pending"][:MESSAGE_PAGE]
        elif first in individual:
            found = await self.exact(row, job, first, spec=spec)
            fresh = {first: found} if found else {}
            candidates = [first]
        else:
            fresh = {message["id"]: message for message in await self.window(row, job, max(row["pending"]) + 1, spec)}
            candidates = row["pending"][:MESSAGE_PAGE]
        duplicates = any(flag.name == "duplicates" and flag.enabled for flag in spec.flags)
        known, available, updates = {}, set(), {}
        if duplicates:
            known = await self.store.keepers(row["id"], {d for hit in fresh.values() if (d := duplicate_observation(hit))})
        targets, skipped = [], set()
        for target in candidates:
            message = fresh.get(target)
            if not message or not matches_observation(message, spec):
                skipped.add(target)
                continue
            if duplicates:
                digest = duplicate_observation(message)
                if not digest:
                    skipped.add(target)
                    continue
                if digest not in available:
                    keeper = known.get(digest)
                    saved = (fresh.get(keeper) or await self.exact(row, job, keeper, spec=spec)) if keeper and keeper > target else None
                    available.add(digest)
                    if not saved or duplicate_observation(saved) != digest or not matches_observation(saved, spec):
                        updates[digest] = target
                        skipped.add(target)
                        continue
            recent = target not in individual and message["created_at_ms"] > self.clock() - BULK_AGE_MS
            if not recent:
                individual.add(target)
                if targets:
                    break
            targets.append(target)
            if not recent:
                break
        row["pending"] = [identifier for identifier in row["pending"] if identifier not in skipped]
        row["individual"] = sorted(individual.intersection(row["pending"]), reverse=True)
        row["skipped"] += len(skipped)
        if updates:
            await self.store.remember(row["id"], updates)
        if targets:
            await self.delete(row, job, deadline, targets, "message")

    async def delete(self, row, job, deadline, ids, kind):
        await self.fence(row, {"running", "queued"})
        due = await self.store.pace(self.clock())
        if due:
            row["next"] = due
            return
        try:
            row["sending"] = self.transport.checkpoint_attempt(row, ids, kind=kind, deadline=deadline)
            await self.checkpoint(row)
            result = await self.transport.delete(row, job, lambda row: self.fence(row, {"running", "queued"}))
            await self.settle(row, result)
        finally:
            await self.store.pace(self.clock(), complete=True)

    async def settle(self, row, result):
        attempt = row["sending"]
        targets, kind = set(attempt["ids"]), attempt["kind"]
        if result.ok:
            if kind == "command":
                row["invocation_removed"] = True
            else:
                row["pending"] = [identifier for identifier in row["pending"] if identifier not in targets]
                row["deleted" if result.status == "deleted" else "skipped"] += len(targets)
        elif result.error_code in UNCERTAIN:
            row["next"] = self.clock() + RECONCILE_MS
            return
        elif result.error_code == "too_old":
            row["individual"] = sorted(set(row["individual"]) | targets, reverse=True)
        elif result.error_code == "permission_denied" and kind == "command":
            row["invocation_removed"] = False
        else:
            row["sending"] = None
            if result.error_code == "rate_limited":
                row["next"] = self.clock() + max(PACE_MS, result.data.get("retry_after_ms", 0))
                await self.store.defer(row["next"])
            raise NativeFailure(result.error_code, result.data)
        row["sending"] = None

    async def reconcile(self, row, job):
        attempt = row["sending"]
        targets = attempt.get("ids", [attempt.get("id")])
        if native := attempt.get("native_attempt"):
            receipt = await self.transport.receipt(native)
            if receipt and receipt.error_code not in UNCERTAIN:
                await self.settle(row, receipt)
                return
            if not receipt and await self.transport.pending(attempt):
                row["next"] = self.clock() + RECONCILE_MS
                return
        if len(targets) > 1:
            result = await self.transport.history(row, job, max(targets) + 1)
        else:
            result = await self.transport.observe(row, job, targets[0])
        missing = set(targets) - set(observations(result, "ids"))
        if attempt["kind"] == "command" and missing:
            row["invocation_removed"] = True
        elif missing:
            row["unconfirmed"] += len(missing)
            row["pending"] = [identifier for identifier in row["pending"] if identifier not in missing]


        row["sending"] = None

    async def finish(self, row, job, deadline, spec):
        if "invocation_removed" not in row:
            source = await self.exact(row, job, row["source"])
            if (source and not source.get("restricted") and source["author"] == row["actor"]
                    and source["deletable"] and (not source["pinned"] or not spec.keep_pins)):
                await self.delete(row, job, deadline, [row["source"]], "command")
                return
            row["invocation_removed"] = source is None
        row["state"] = "done"

    async def progress(self, row, job, deadline):
        if not row.get("message") or row.get("progress_missing") or self.clock() >= deadline:
            return
        text = f"Deleted {row['deleted']} messages."
        if row["state"] not in {"done", "cancelled"}:
            text = f"Purge {row['id']} · {row['state'].capitalize()}\n" + text
        if row["unconfirmed"]:
            text += f" {row['unconfirmed']} could not be confirmed."
        if row.get("error"):
            text += " " + row["error"]
        if text == row.get("rendered") or (
            self.clock() < row.get("last_rendered", 0) + 5000 and row["state"] in RUNNABLE
        ):
            return
        with suppress(ActionUnavailable, RedisError, OSError, TimeoutError):
            result = await self.transport.progress(row, job, text, deadline, lambda row: self.fence(row, {row["state"]}))
            if result and result.ok:
                row.update(rendered=text, last_rendered=self.clock())
            elif result and (result.error_code == "message_not_owned" or result.data.get("deleted") is True):
                row["progress_missing"] = True
