from contextlib import suppress
from dataclasses import dataclass, field

from redis.exceptions import RedisError

from lumi.contracts.channel_fence import request_refresh
from lumi.ui_keys import ui_key

from .handover_store import GROUPS, TERMINAL
from .operation_tags import tag_operation
from .purge import PurgeError, build_plan
from .purge_store import ACTIVE

BOT_CHANNEL_FACTS = ("view_channel", "read_message_history", "manage_messages")


@dataclass(slots=True)
class ControlResult:
    status: str
    data: dict = field(default_factory=dict)
    ok: bool = True
    error_code: str | None = None


def reject(code):
    return ControlResult("failed", ok=False, error_code=code)


def describe(status, row, *, bind=False):
    data = {key: row[key] for key in ("id", "state", "mode", "inspected", "deleted", "skipped", "unconfirmed")}
    if bind:
        data["token"] = row["token"]
    if row.get("error"):
        data["error"] = row["error"]
    return ControlResult(status, data)


def reset_settings(result):
    facts = result.data.get("configuration") or {}
    if facts.get("ordinary_text") is not True:
        raise PurgeError("Only ordinary text channels can be reset.")
    if facts.get("system_destination") is True:
        raise PurgeError("This channel is protected.")
    if not isinstance(facts.get("settings"), dict):
        raise PurgeError("Channel settings unavailable.")
    return facts["settings"]


class PurgeControls:
    def __init__(self, runner, queue, cache, handovers, group, clock):
        self.runner, self.store, self.queue, self.cache = runner, runner.store, queue, cache
        self.handovers, self.group, self.clock = handovers, group, clock
        self.reset_revoke = None

    async def recover_unprepared(self):
        if self.handovers is not None:
            await self.store.recover_unprepared(self.handovers, self.clock())

    async def handle(self, request, job, operation, code, spec):
        event, args = request.event, request.arguments
        guild, channel, actor, source = (event.get(k) for k in ("guild_id", "channel_id", "actor_id", "message_id"))
        if any(type(i) is not int or not 0 < i < 2**64 for i in (guild, channel, actor, source)):
            return reject("permission_denied")
        flags = event.get("_bot_channel_facts", {})
        if self.clock() >= event.get("_expires_at_ms", 0) or not all(flags.get(key) is True for key in BOT_CHANNEL_FACTS):
            return reject("permission_denied")
        if operation == "start":
            if self.handovers is not None and any(
                gate["phase"] not in TERMINAL for gate in await self.handovers.index(guild)
            ):
                return ControlResult("purge_busy")
            now = self.clock()
            record = build_plan(
                spec, guild=guild, channel=channel, actor=actor, source=source,
                created=event.get("created_at_ms"), admitted=event.get("_admitted_at_ms", now), now=now,
            )
            if record["mode"] == "reset":
                if self.handovers is None or set(await self.handovers.ready(self.clock())) != GROUPS:
                    return ControlResult("purge_reset_unavailable")
                facts = await self.runner.transport.configuration(job, guild, channel)
                if not facts.ok:
                    return reject(facts.error_code)
                record["settings"] = reset_settings(facts)
            await tag_operation(self.queue, record, source_key=record["key"])
            if self.clock() >= event["_expires_at_ms"]:
                return reject("expired_job")
            row, status = await self.store.create(record)
            if row is None:
                return ControlResult("purge_" + status)
            if (row["actor"], row["spec"], row["mode"]) != (actor, record["spec"], record["mode"]):
                return reject("action_scope_mismatch")
            self.runner.wake.set()
            return describe("purge_confirmation" if row["state"] == "confirm" else "purge_queued", row,
                            bind=not row["message"])
        row = await self.store.get(guild, channel, code)
        if operation == "confirm":
            token, verb = args.get("session"), request.route.rpartition(".")[2]
            if (not row or row["state"] != "confirm" or row["expires"] <= self.clock()
                    or (row["token"], row["actor"]) != (token, actor)
                    or verb not in {"purge_confirm", "purge_cancel"}):
                return reject("panel_expired")
            bound = await self.cache.get(ui_key("message", token, self.group))
            if bound is None or int(bound) != source or self.clock() >= event["_expires_at_ms"]:
                return reject("panel_expired")
            accept = verb == "purge_confirm"
            if accept and row["mode"] == "reset":
                if self.handovers is None:
                    return ControlResult("purge_reset_unavailable")
                facts = await self.runner.transport.configuration(job, guild, channel)
                if not facts.ok:
                    return reject(facts.error_code)
                if reset_settings(facts) != row.get("settings"):
                    raise PurgeError("Channel settings changed. Start again.")
                if self.clock() >= min(event["_expires_at_ms"], row["expires"]):
                    return reject("panel_expired")
            changed = await self.store.control(
                row, "queued" if accept else "cancelled", self.clock(), requester=actor,
                prepare_handover=self.handovers if accept and row["mode"] == "reset" else None,
            )
            if changed is None:
                return reject("panel_expired")
            if accept and row["mode"] == "reset":
                with suppress(RedisError):
                    await request_refresh(self.queue)
            self.runner.wake.set()
            return describe("purge_queued" if accept else "purge_status", changed)
        if not row:
            return ControlResult("purge_missing")
        if operation == "resume":
            if actor != row["actor"] or row["state"] not in {"paused", "uncertain"}:
                return reject("permission_denied")
            if row["mode"] == "reset":
                target = row.get("replacement") if row.get("references_moved") else row["channel"]
                if self.handovers is None or not target:
                    return ControlResult("purge_reset_unavailable")
                facts = await self.runner.transport.configuration(job, guild, channel, target=target, layout=False)
                if not facts.ok:
                    return reject(facts.error_code)
                if self.clock() >= event["_expires_at_ms"]:
                    return reject("expired_job")
            row = await self.store.control(
                row, "queued", self.clock(), requester=actor,
                prepare_handover=self.handovers if row["mode"] == "reset" else None,
            )
        elif operation == "cancel" and row["state"] in ACTIVE:
            previous = row
            row = await self.store.control(row, "cancelling", self.clock())
            if row and previous["mode"] == "purge":
                await self.runner.transport.revoke(previous)
            elif row and self.reset_revoke is not None:
                await self.reset_revoke(previous)
        if row is None:
            return ControlResult("purge_changed")
        if operation == "resume" and row["mode"] == "reset":
            with suppress(RedisError):
                await request_refresh(self.queue)
        self.runner.wake.set()
        return describe("purge_status", row, bind=actor == row["actor"] and not row["message"])
