import asyncio
import secrets
from contextlib import suppress

from redis.exceptions import RedisError

from lumi.contracts import DiscordIntent, PresentationPage
from lumi.contracts.channel_fence import request_refresh
from lumi.contracts.channel_resources import (
    ROLLBACK_POST_LIMIT, ChannelResourcePost, ChannelResourceRequest, TextChannelEdit, configuration_digest,
)
from lumi.contracts.handover_reference import PARTICIPANTS, REQUEST_MS, ReferenceRequest
from lumi.ui_keys import ui_key

from .channel_panel_transfer import ChannelPanelTransfer
from .purge import PurgeError
from .handover_store import MySQLHandoverStore
from .purge_runner import LostLease, PACE_MS, RECONCILE_MS, PurgeRunner

ROUTE = "moderation.reset_work"
TRANSITION_WINDOW_MS = 15 * 60_000
EDIT_FIELDS = ("name", "topic", "position", "nsfw", "slowmode", "archive", "thread_slowmode")
PENDING_CODES = {"unavailable", "ambiguous", "action_in_progress", "discord_action_failed"}


class LeasedJournal:
    def __init__(self, runner, row):
        self.runner, self.row = runner, row

    async def get(self, code):
        return await self.runner.journal.get(code)

    async def mutate(self, code, change):
        row, journal = self.row, self.runner.journal
        if isinstance(journal, MySQLHandoverStore):
            changed = await journal.mutate(code, change, reset_lease=(row["revision"], row["lease_token"], self.runner.clock))
        else:
            await self.runner.fence(row)
            changed = await journal.mutate(code, change)
        if changed is None:
            raise LostLease
        return changed


class ResetRunner(PurgeRunner):
    mode = "reset"
    route = ROUTE
    runnable = frozenset({"queued", "running", "handover", "cancelling"})

    def __init__(self, store, transport, scheduler, cache, group, clock, journal, references, generation):
        super().__init__(store, transport, scheduler, cache, group, clock)
        self.journal, self.references, self.generation = journal, references, generation

    async def ref(self, op, row, record, *, workers=None):
        now = self.clock()
        request = ReferenceRequest(
            secrets.token_hex(16), op, self.generation, row["id"], record["phase"], record.get("epoch", 0),
            record.get("revision", 1), row["revision"], row["lease_token"], min(now + REQUEST_MS, row["lease_until"]),
            workers,
        )
        if not request.valid(now):
            raise LostLease
        async with asyncio.timeout((request.deadline - now) / 1000):
            return await self.references.execute(request, self.clock)

    async def update(self, row, record, **changes):
        def change(current):
            if current["phase"] != record["phase"] or current.get("epoch", 0) != record.get("epoch", 0):
                raise LostLease
            current.update(changes)

        updated = await LeasedJournal(self, row).mutate(record["id"], change)
        if any(name in changes for name in ("new", "phase")):
            with suppress(RedisError):
                await request_refresh(self.transport.queue)
        return updated

    async def tick(self, row, job, deadline):
        row["served"] = self.clock()
        try:
            record = await self.journal.get(row["id"])
            if record:
                await self.advance(row, record, job)
                await self.original_progress(row, record, job, deadline)
        except LostLease:
            return
        except (OSError, TimeoutError, RedisError):
            row["next"] = self.clock() + RECONCILE_MS
        with suppress(LostLease):
            row["next"] = max(row.get("next", 0), self.clock() + PACE_MS)
            await self.checkpoint(row, release=True)

    async def original_progress(self, row, record, job, deadline):
        if not row.get("token") or not row.get("message") or row.get("original_deleted") or row.get("progress_missing"):
            return
        text = f"Reset {row['id']} · {row['state'].capitalize()}"
        if row.get("error"):
            text += "\n" + row["error"]
        now = self.clock()
        if row.get("rendered") == text or now >= deadline or (
            row.get("last_rendered", 0) + 5000 > now and row["state"] not in {"failed", "cancelled", "paused"}
        ):
            return
        result = await self.transport.request(row, job, ChannelResourceRequest(
            "edit_post", record["id"], record["old"], post=ChannelResourcePost(
                message=row["message"], module=self.transport.module, group=self.group, session=row["token"],
            ),
        ), attempt=self.transport.attempt(), authorize=self.fence, intent=DiscordIntent(
            "edit", pages=(PresentationPage(description=text),), presentation_mode="v2",
        ))
        if result.ok:
            row.update(rendered=text, last_rendered=now)
        elif result.error_code in {"message_not_found", "message_not_owned"}:
            row["progress_missing"] = True

    async def advance(self, row, record, job):
        phase = record["phase"]
        row.update(replacement=record.get("new", 0), original_deleted=bool(record.get("original_deleted")),
                   references_moved=bool(record.get("applied")) and not record.get("references_rolled_back"))
        if phase == "complete":
            row.update(state="done", message=record["progress_message"], progress_channel=record["new"])
            return
        if phase == "rolled_back":
            text = record.get("error") or "Reset cancelled."
            if record.get("copy_preserved"):
                text += " Replacement kept for review."
            row.update(state="failed" if record.get("error") else "cancelled", error=text)
            return
        if not row.get("message"):
            bound = await self.cache.get(ui_key("message", row["token"], self.group))
            if not bound:
                return
            row["message"] = int(bound)
        if row["state"] != "cancelling":
            row["state"] = "handover"
        status = await self.transport.request(row, job, ChannelResourceRequest(
            "transport_state", record["id"], record["old"], delivery_state=phase in {"waiting", "quiesced"},
        ))
        if not status.ok or (status.data["phase"], status.data["peer"]) != (phase, record.get("new", 0)):
            return
        native = status.data


        original_pending = phase == "deleting" and (record.get("effect") or {}).get("operation") == "delete"
        clone_pending = phase == "creating" and not record.get("new") and (record.get("effect") or {}).get("operation") == "clone"
        stopping = row["state"] == "cancelling" or self.clock() > record["started"] + TRANSITION_WINDOW_MS
        if not (record.get("original_deleted") or original_pending) and phase != "rollback" and (
            stopping and not clone_pending
        ):
            await self.update(row, record, phase="rollback", effect=None,
                              **({} if row["state"] == "cancelling" else {"error": "Handover timed out."}))
            return
        try:
            if clone_pending and stopping:
                return await self.create_or_configure(row, record, job)
            if phase == "rollback":
                return await self.rollback(row, record, job)
            if phase in {"waiting", "quiesced", "creating", "building", "applying", "publishing", "deleting"}:
                workers = {group: native["generations"][group] for group in PARTICIPANTS
                           if group in native["generations"]}
                if workers.keys() != PARTICIPANTS or workers["security"] != self.generation:
                    return
                report = await self.ref("preflight", row, record, workers=workers)
                if report.outcome == "pending":
                    return
                if report.outcome == "blocked":
                    if phase == "waiting":
                        row["error"] = report.blocker
                        return
                    raise PurgeError(report.blocker)
            if phase in {"waiting", "quiesced"}:
                if not native["settled"]:
                    row["error"] = "A Discord delivery is unconfirmed."
                    return
                if not native["quiet"]:
                    return
                row.pop("error", None)
                if phase == "waiting":
                    await self.update(row, record, phase="quiesced")
                else:
                    await self.update(row, record, phase="creating", create_marker=secrets.token_hex(12))
                return
            if phase == "creating":
                return await self.create_or_configure(row, record, job)
            if phase == "building":
                await self.ref("validate", row, record)
                if await ChannelPanelTransfer(LeasedJournal(self, row), self.transport).step(row, record, job, self.fence):
                    await self.update(row, record, phase="applying")
                return
            if phase == "applying":
                await self.ref("apply", row, record)
                with suppress(RedisError):
                    await request_refresh(self.transport.queue)
                return
            if phase in {"publishing", "deleting"}:
                if not all(native[key] >= record.get(key, 0) for key in ("policy_revision", "reaction_revision")):
                    return
                if not await self.ref("verify", row, record):
                    raise PurgeError("A moved setting changed. Reset stopped.")
                if phase == "publishing":
                    before, copy = await self.layout(row, record, job)
                    self.equal_settings(record, before, copy)
                    progress = await ChannelPanelTransfer(LeasedJournal(self, row), self.transport).progress(row, record, job, self.fence)
                    if progress:
                        await self.update(row, record, phase="deleting")
                else:
                    await self.delete_original(row, record, job)
        except PurgeError as exc:
            actual = await self.journal.get(row["id"])
            if actual.get("original_deleted") or phase == "rollback" or original_pending:
                row.update(state="paused", error=str(exc))
            else:
                await self.update(row, actual, phase="rollback", effect=None, error=str(exc))

    async def layout(self, row, record, job):
        result = await self.transport.request(row, job, ChannelResourceRequest(
            "observe", record["id"], record["old"], relative_to_peer=bool(record.get("new")),
            companion=bool(record.get("new")),
        ))
        if not result.ok:
            if result.error_code == "permission_denied":
                raise PurgeError("Missing permission.")
            raise TimeoutError("Channel observation not available")

        def settings(facts):
            if not facts:
                return None
            if not facts["ordinary_text"] or facts["system_destination"]:
                raise PurgeError("This channel is protected.")
            return facts["settings"]

        return settings(result.data.get("configuration")), settings(result.data.get("companion"))

    @staticmethod
    def equal_settings(record, *observations):
        expected = configuration_digest(record["settings"])
        if any(value is None or configuration_digest(value) != expected for value in observations):
            raise PurgeError("Channel settings changed. Reset stopped.")
        return expected

    async def effect(self, row, record, job, query):
        saved = record.get("effect")
        if saved:
            if (saved["operation"], saved["target"]) != (query.operation, query.target):
                return None
            return await self.transport.receipt(saved["attempt"])
        if query.operation == "delete" and await self.store.pace(self.clock()):
            return None
        attempt = self.transport.attempt()
        await self.update(row, record, effect={"operation": query.operation, "target": query.target, "attempt": attempt})
        try:
            return await self.transport.request(row, job, query, attempt=attempt, authorize=self.fence)
        finally:
            if query.operation == "delete":
                await self.store.pace(self.clock(), complete=True)

    @staticmethod
    def accepted(result):
        if result and result.ok:
            return True
        if result and result.status != "uncertain" and result.error_code not in PENDING_CODES:
            raise PurgeError("Missing permission." if result.error_code == "permission_denied" else "Discord rejected the change.")
        return False

    async def create_or_configure(self, row, record, job):
        reason = f"Lumi channel reset {record['id']}"
        if not record.get("new"):
            if record.get("effect"):


                result = await self.transport.receipt(record["effect"]["attempt"])
                if not self.accepted(result):
                    found = await self.transport.request(row, job, ChannelResourceRequest(
                        "find_created", record["id"], record["old"], reason=reason,
                    ))
                    if not found.ok or len(found.data["channels"]) != 1:
                        return
                    identifier = found.data["channels"][0]
                else:
                    identifier = result.data["channel_id"]
            else:
                before, _ = await self.layout(row, record, job)
                digest = self.equal_settings(record, before)
                result = await self.effect(row, record, job, ChannelResourceRequest(
                    "clone", record["id"], record["old"], compare=digest,
                    reason=reason, name="lumi-reset-" + record["create_marker"],
                ))
                if not self.accepted(result):
                    return
                identifier = result.data["channel_id"]
            await self.update(row, record, new=identifier, effect=None)
            return
        origin, copy = await self.layout(row, record, job)
        self.equal_settings(record, origin)
        if copy is None:
            raise PurgeError("Replacement channel unavailable.")
        expected = record["settings"]


        desired = TextChannelEdit(*(origin[key] for key in EDIT_FIELDS))
        needs_edit = any(origin[key] != copy[key] for key in EDIT_FIELDS if key != "position") or any(
            expected[key] != copy[key] for key in ("previous", "following")
        )
        pending = (record.get("effect") or {}).get("operation")
        if pending == "edit" and not needs_edit or pending == "flags" and (copy["flags"] == expected["flags"]):
            await self.update(row, record, effect=None)
            return
        operation = "edit" if needs_edit else "flags" if copy["flags"] != expected["flags"] else None
        if operation:
            result = await self.effect(row, record, job, ChannelResourceRequest(
                operation, record["id"], record["new"], relative_to_peer=True,
                compare=configuration_digest(copy), reason=reason,
                **({"edit": desired} if operation == "edit" else {"flags": expected["flags"]}),
            ))
            if self.accepted(result):
                await self.update(row, record, effect=None)
            return
        self.equal_settings(record, copy)
        await self.update(row, record, phase="building", effect=None)

    async def delete_original(self, row, record, job):
        if not record.get("original_deleted"):
            saved = record.get("effect")
            result = await self.transport.receipt(saved["attempt"]) if saved else None
            if not self.accepted(result):
                origin, copy = await self.layout(row, record, job)
                if origin is not None:
                    if saved:
                        return
                    digest = self.equal_settings(record, origin, copy)
                    effect = await self.effect(row, record, job, ChannelResourceRequest(
                        "delete", record["id"], record["old"], relative_to_peer=True, compare=digest,
                        peer_compare=digest, reason=f"Lumi channel reset {record['id']} by {row['actor']}",
                    ))
                    if not self.accepted(effect):
                        return
            await self.update(row, record, original_deleted=True, effect=None)
            return
        message = record["progress_message"]
        result = await self.transport.request(row, job, ChannelResourceRequest(
            "edit_post", record["id"], record["new"], post=ChannelResourcePost(
                message=message, module=self.transport.module, group=self.group,
            ),
        ), attempt=self.transport.attempt(), authorize=self.fence, intent=DiscordIntent(
            "edit", pages=(PresentationPage(description="Channel reset."),), presentation_mode="v2",
        ))
        if not result.ok and result.error_code in PENDING_CODES and result.status != "uncertain":
            return
        await self.update(row, record, phase="complete", completed=self.clock())
        row.update(state="done", original_deleted=True, message=message, progress_channel=record["new"])

    async def rollback(self, row, record, job):
        moving_back = record.get("applied") and not record.get("references_rolled_back")
        if moving_back and not record.get("new"):
            raise PurgeError("Replacement unavailable. Review the channel references.")
        if record.get("new"):
            found = await self.transport.request(row, job, ChannelResourceRequest(
                "observe", record["id"], record["new"], cleanup=True,
            ))
            if found.error_code == "peer_missing":
                raise PurgeError("The original channel is missing. Review the replacement.")
            if not found.ok and found.error_code not in {"permission_denied", "channel_unavailable"}:
                return
            if moving_back:


                await self.ref("rollback", row, record)
                return
            preserved = found.error_code == "permission_denied" or (found.ok and found.data["compare"] is None)
            if found.ok and not preserved:
                owned = {item.get("message") for items in record.get("plans", {}).values() for item in items}
                owned.add(record.get("progress_message"))
                owned = tuple(sorted(i for i in owned if i))
                if len(owned) > ROLLBACK_POST_LIMIT:
                    await self.update(row, record, phase="rolled_back", completed=self.clock(), copy_preserved=True)
                    return
                result = await self.effect(row, record, job, ChannelResourceRequest(
                    "delete", record["id"], record["new"], compare=found.data["compare"],
                    only_owned_posts=owned, reason=f"Rollback Lumi reset {record['id']}",
                ))
                if result and result.error_code == "peer_missing":
                    raise PurgeError("The original channel is missing. Review the replacement.")
                preserved = bool(result and result.error_code in {"foreign_posts", "permission_denied"})
                if not preserved and not self.accepted(result):
                    return
        else:
            preserved = False
        await self.update(row, record, phase="rolled_back", completed=self.clock(), copy_preserved=preserved)
