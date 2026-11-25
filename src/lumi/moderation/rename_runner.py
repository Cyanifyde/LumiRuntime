from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from datetime import datetime

from redis.exceptions import RedisError

from .rename import RUNNABLE, TTL, declined, page
from .rename_store import member_record

LOGGER = logging.getLogger(__name__)
IN_FLIGHT = {"apply_sending", "undo_sending"}
BUSY = {"running", "undoing", "listing", "queued"}


class Changed(Exception):
    pass


class NativeFailure(Exception):
    def __init__(self, code, data=None):
        super().__init__(code)
        self.code = code
        self.data = data or {}


def observed(result):
    if not result.ok:
        raise NativeFailure(result.error_code, result.data)
    return result.data


class RenameRunner:
    def __init__(self, store, transport, clock):
        self.store, self.transport, self.clock = store, transport, clock
        self.wake = asyncio.Event()
        self.rendered = {}
        self.pending = {}
        self.active = False
        self.draining = False
        self.task = None

    async def notify(self):
        self.wake.set()

    def start(self):
        self.task = asyncio.create_task(self._run(), name="moderation-rename")

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def _run(self):
        while True:
            self.wake.clear()
            delay = 15
            if not self.draining:
                try:
                    delay = await self.schedule_due()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.warning("rename_schedule_unavailable")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.wake.wait(), timeout=delay)

    async def schedule_due(self):
        if self.active:
            return 1
        now = self.clock()
        self.pending = {key: expires for key, expires in self.pending.items() if expires > now}
        if self.pending:
            return 1
        rows = await self.store.ready(now)
        for row in rows[:1]:
            key = row["id"], row["revision"]
            if key not in self.pending:
                self.pending[key] = await self.transport.schedule(row)
        if rows:
            return 1
        due = await self.store.next_due()
        return 30 if due is None else max(0.05, min(30, (due - now) / 1000))

    async def run_callback(self, request, job_id):
        args, event = request.arguments, request.event
        batch = args.get("batch")
        if (
            event.get("dispatch") != "scheduled" or self.draining or not self.transport.live(event)
            or not isinstance(batch, str) or not re.fullmatch(r"[a-f0-9]{8}", batch)
            or type(args.get("revision")) is not int or args["revision"] <= 0
        ):
            return
        async with self.store.exclusive() as acquired:
            if not acquired or self.draining:
                return
            row = await self.store.get(event["guild_id"], batch)
            self.pending.pop((batch, args["revision"]), None)
            if not row or (row["revision"], row["actor"], row["channel"]) != (
                args.get("revision"), event["actor_id"], event["channel_id"],
            ) or row["state"] not in RUNNABLE or row["next"] > self.clock():
                return
            self.active = True
            try:


                while not self.draining and row["state"] in RUNNABLE:
                    if row["next"] > self.clock() or not self.transport.live(event):
                        break
                    row = await self.step(row, job_id, event)
                    if row["state"] in BUSY and self.transport.live(event):

                        await asyncio.sleep(0.05)
                    if row["actor"] != event["actor_id"] or row["channel"] != event["channel_id"]:
                        break
            finally:
                self.active = False
                await self.notify()

    async def patch(self, row, **changes):
        updated = await self.store.update(row, changes, self.clock())
        if not updated:
            raise Changed
        return updated

    async def save(self, member, **changes):
        saved = await self.store.save_member(member, changes)
        if not saved:
            raise Changed
        return saved

    async def fence(self, row, *, write=True):
        current = await self.store.get(row["guild"], row["id"])
        if (
            not current or current["revision"] != row["revision"]
            or (write and (
                current["state"] not in RUNNABLE
                or (current["mode"] == "apply" and not current["approved"])
            ))
        ):
            raise Changed

    async def step(self, row, job, event):
        try:
            if row["state"] in {"confirm", "undo_confirm"}:
                row = await self.patch(row, state=declined(row), next=self.clock(), expires=0)
            else:
                if not row["message"]:
                    bound = await self.transport.panel_id(row)
                    if not bound:
                        return await self.patch(row, next=self.clock() + 1000)
                    row = await self.patch(row, message=bound, native_panel=True)
                elif not row.get("native_panel"):
                    return await self.patch(row, state="paused", error="Use rename resume to reopen this panel.")
                if row["state"] == "listing":
                    row = await self.list_page(row, job)
                else:
                    row = await self.patch(row, state="undoing" if row["mode"] == "undo" else "running")
                    member = await self.store.next_member(row)
                    if member is None:
                        row = await self.patch(row, state="done" if row["mode"] == "undo" else "ready")
                    else:
                        await self.edit(row, member, job, event)
                        row = await self.patch(row, next=self.clock() + 1, retry=False)
        except Changed:
            current = await self.store.get(row["guild"], row["id"])
            if current:
                row = current
        except NativeFailure as exc:
            with contextlib.suppress(Changed):
                if exc.code in {"rate_limited", "access_changed", "expired_job", "effect_not_authorized"}:
                    row = await self.patch(row, next=self.clock() + exc.data.get("retry_after_ms", 1000))
                elif exc.code in {"permission_denied", "policy_denied", "bot_permission_denied",
                                  "blocked", "target_protected", "guild_unavailable"}:
                    row = await self.patch(row, state="paused", error="Missing permission.")
                else:
                    row = await self.patch(row, state="paused", error="Could not confirm. Use rename resume.")
        except (OSError, TimeoutError, RedisError):
            with contextlib.suppress(Changed):
                row = await self.patch(row, state="paused", error="Could not confirm. Use rename resume.")
        await self.progress(row, job, event)
        return row

    async def list_page(self, row, job):
        native = observed(await self.transport.list_members(row, job))
        records = [
            member_record(row["id"], member, joined)
            for member, joined in native["members"]
            if not joined or int(datetime.fromisoformat(joined).timestamp() * 1000) <= row["created"]
        ]
        complete = not native["more"]
        updated = await self.store.update(row, dict(
            cursor=native["cursor"], listed=complete,
            state="confirm" if complete else "listing",
            next=self.clock() + (TTL if complete else 1),
            expires=self.clock() + TTL if complete else 0,
        ), self.clock(), members=records)
        if not updated:
            raise Changed
        return updated

    async def edit(self, row, record, job, event):
        attempt = record.get("native_attempt")
        if record["state"] in IN_FLIGHT and attempt:
            receipt = await self.transport.receipt(attempt)
            if receipt is not None:


                if receipt.ok or receipt.error_code != "uncertain":
                    await self.settle(row, record, receipt)
                    return
        result = await self.transport.read_member(row, job, record["member"], fresh=record["state"] in IN_FLIGHT)
        if not result.ok and result.error_code == "member_unavailable":
            await self.save(record, state="left")
            return
        target = observed(result)
        if not record["joined"] or target["joined_at"] != record["joined"]:
            await self.save(record, state="left")
            return
        if not target["manageable"]:
            if record["state"] in IN_FLIGHT | {"applied", "restore_failed"}:
                raise NativeFailure("target_protected")
            await self.save(record, state="skipped")
            return
        nickname = target["nickname"]
        if record["state"] in IN_FLIGHT:
            before = record.get("native_before", record["original"] if record["state"] == "apply_sending"
                                else record.get("accepted_restore", record["applied"]))
            if nickname == record["expected"]:
                await self.accepted(row, record, nickname)
                return
            if nickname != before or not row["retry"] or (
                attempt and await self.transport.pending(attempt, record.get("native_until", 0))
            ):
                error = ("Could not confirm. Name kept for review." if nickname != before
                         else "Could not confirm. Use rename resume.")
                await self.patch(row, state="paused", error=error)
                raise Changed
            record = await self.save(record, state="pending" if record["state"] == "apply_sending" else "applied")
        if row["mode"] == "undo":
            if record["state"] == "pending":
                return
            if nickname != record.get("accepted_restore", record["applied"]):
                await self.save(record, state="changed")
                return
            requested, sending = record["original"], "undo_sending"
        else:
            if record["state"] == "applied":
                return
            if "original" in record and nickname != record["original"]:
                await self.save(record, state="changed")
                return
            if nickname == row["name"]:
                await self.save(record, state="unchanged")
                return
            requested, sending = row["name"], "apply_sending"
        await self.fence(row)
        attempt = self.transport.attempt()
        changes = dict(state=sending, expected=requested, native_attempt=attempt, native_before=nickname,
                       native_until=self.transport.deadline(event))
        if "original" not in record:
            changes["original"] = nickname
        record = await self.save(record, **changes)
        result = await self.transport.write_member(row, job, event, record, self.fence)
        await self.settle(row, record, result)

    async def accepted(self, row, record, actual):
        if record["state"] == "apply_sending":
            await self.save(record, applied=actual, state="unchanged" if actual == record["original"] else "applied")
        elif actual != record["original"]:
            await self.save(record, state="restore_failed", accepted_restore=actual)
            await self.patch(row, state="paused", error="Discord did not restore the exact name.")
            raise Changed
        else:
            await self.save(record, state="restored")

    async def settle(self, row, record, result):
        if result.ok:
            await self.accepted(row, record, result.data["nickname"])
            return
        code = result.error_code
        if code in {"member_unavailable", "member_changed", "value_changed"}:
            await self.save(record, state="changed" if code == "value_changed" else "left")
            return
        if code == "discord_rejected":
            if record["state"] == "undo_sending":
                await self.save(record, state="restore_failed", accepted_restore=record["native_before"])
            else:
                await self.save(record, state="failed")
            if result.data.get("http_status") == 400:
                await self.patch(row, state="paused", error="Discord rejected the name.")
                raise Changed
        elif code in {
            "rate_limited", "access_changed", "effect_not_authorized", "expired_job",
            "permission_denied", "policy_denied", "bot_permission_denied", "target_protected",
            "blocked", "guild_unavailable",
        }:
            await self.save(record, state="applied" if record["state"] == "undo_sending" else "pending")
        raise NativeFailure(code, result.data)

    async def progress(self, row, job, event):
        if not row.get("message") or (row["actor"], row["channel"]) != (
            event["actor_id"], event["channel_id"],
        ) or not self.transport.live(event):
            return
        rendered = page(row, await self.store.counts(row))
        previous, at = self.rendered.get(row["id"], (None, 0))
        if rendered == previous or (self.clock() < at + 5000 and row["state"] in BUSY):
            return
        if await self.transport.progress(row, job, event, rendered, lambda row: self.fence(row, write=False)):
            self.rendered[row["id"]] = rendered, self.clock()
            if len(self.rendered) > 64:
                self.rendered.pop(next(iter(self.rendered)))
