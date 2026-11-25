import secrets

import msgspec
from redis.exceptions import RedisError

from lumi.contracts.staff_recovery import EDIT_RECOVERY, OBSERVE_RECOVERY, StaffRecoveryTarget

from .log_entry import evidence_page, summary
from .models import HEX_ID

ROUTE = "imageguard.recover"
RECOVERY_MS = 60_000


def unsettled(row):
    return {
        name for name, item in row.get("steps", {}).items() if item.get("state") in {"sending", "uncertain"}
    }


def target(row, group):
    steps = row.get("steps", {})
    evidence = steps.get("evidence", {})
    evidence_id, evidence_mode = 0, "none"
    if evidence.get("state") == "done" and evidence.get("message"):
        if evidence.get("channel") in (None, 0, row["log_channel"]):
            evidence_id = evidence["message"]
            evidence_mode = "confirmed" if evidence.get("channel") else "legacy"
    elif evidence.get("state") in {"sending", "uncertain"}:
        evidence_mode = "uncertain"
    pending = unsettled(row)
    return StaffRecoveryTarget(
        row["log_channel"], row["message"], row["user"], "ImageGuard " + row["id"],
        evidence=evidence_mode, evidence_id=evidence_id,
        check_roles=any(name.startswith(("add_", "remove_")) for name in pending),
        check_ban="terminal" in pending and row.get("action", group.get("action")) == "ban",
    )


class Recovery:
    def __init__(self, store, scheduler, actions, clock):
        self.store, self.scheduler, self.actions, self.clock = store, scheduler, actions, clock

    async def group(self, row):
        config = await self.store.snapshot(row["guild"])
        return next((group for group in config["groups"] if group["id"] == row["group"]), {"name": "Removed group"})

    async def deletion_confirmed(self, row):
        attempt = row["steps"]["delete"].get("attempt")
        if not isinstance(attempt, str):
            return False
        try:
            receipt = await self.actions.read_effect_receipt(attempt)
        except (ValueError, msgspec.DecodeError):
            return False
        return bool(
            receipt and receipt.ok and receipt.status == "deleted"
            and (receipt.data or {}).get("deleted_source")
            == [row["guild"], row["channel"], row["message"]]
        )

    async def schedule(self):
        now = self.clock()
        legacy_config = {}
        for row in await self.store.recoverable(now):
            if row.get("lease_until", 0) > now:
                continue
            group = {}
            if not row.get("action") and "terminal" in unsettled(row):
                guild = row["guild"]
                if guild not in legacy_config:
                    legacy_config[guild] = {
                        group["id"]: group for group in (await self.store.snapshot(guild))["groups"]
                    }
                group = legacy_config[guild].get(row.get("group"), {})
            try:
                scope = target(row, group)
            except (KeyError, TypeError, ValueError):
                continue
            if not scope.valid(row["channel"]):
                continue
            await self.scheduler.publish(
                operation="upsert", schedule_key="imageguard:recover:" + row["id"],
                route=ROUTE, revision=max(1, now), guild_id=row["guild"], channel_id=row["channel"],
                actor_id=0, due_at_ms=now, expires_at_ms=now + RECOVERY_MS,
                arguments=msgspec.to_builtins(scope),
            )

    async def run(self, request, job_id):
        try:
            scope = msgspec.convert(request.arguments, StaffRecoveryTarget)
        except (TypeError, ValueError):
            return
        event = request.event
        incident = scope.marker.removeprefix("ImageGuard ")
        if (
            scope.marker != "ImageGuard " + incident or not HEX_ID.fullmatch(incident)
            or not scope.valid(event.get("channel_id")) or event.get("dispatch") != "scheduled"
            or event.get("actor_id") != 0
            or event.get("schedule_key") != "imageguard:recover:" + incident
        ):
            return
        row = await self.store.incident(incident)
        if not row or row.get("reconciled") or (
            row["guild"], row["channel"]
        ) != (event.get("guild_id"), event.get("channel_id")):
            return
        group = await self.group(row)
        if target(row, group) != scope:
            return
        owner = secrets.token_hex(16)
        row = await self.store.claim(incident, owner, self.clock(), recovery=True)
        if not row or target(row, group) != scope:
            return
        base = dict(job_id=job_id, guild_id=row["guild"], channel_id=row["channel"])
        pending = unsettled(row)
        try:
            deleted = "delete" in pending and await self.deletion_confirmed(row)
        except RedisError:
            return
        facts = {}
        if scope.evidence != "none" or scope.check_roles or scope.check_ban:
            result = await self.actions.request(
                kind=OBSERVE_RECOVERY, idempotency_key="recovery-read:" + job_id, **base,
            )
            if not result.ok:
                return
            facts = result.data or {}
        message = facts.get("evidence")
        roles = set(facts["roles"]) if "roles" in facts else None
        for name in sorted(pending):
            if name == "evidence_update":
                continue
            details = {}
            if name == "evidence":
                resolved = bool(message)
                if resolved:
                    details = dict(message=message, channel=scope.log)
            elif name == "delete":
                resolved = deleted
            elif name == "terminal":
                resolved = scope.check_ban and facts.get("banned") is True
            elif name.startswith("add_"):
                resolved = roles is not None and int(name[4:]) in roles
            elif name.startswith("remove_"):
                resolved = roles is not None and int(name[7:]) not in roles
            else:
                resolved = False
            await self.store.step(
                incident, owner, name, "done" if resolved else "uncertain", recovered=True, **details,
            )
        if message and facts.get("proof"):
            settled = await self.store.incident(incident)
            edited = await self.actions.request(
                kind=EDIT_RECOVERY, idempotency_key="recovery-edit:" + job_id,
                message_id=message, read_proof=facts["proof"],
                page=evidence_page(row, group, summary(settled, group) or "Uncertain"), **base,
            )
            if not edited.ok and edited.error_code not in {
                "discord_rejected", "message_not_owned", "staff_log_unavailable",
            }:
                return
            if "evidence_update" in pending:
                await self.store.step(
                    incident, owner, "evidence_update", "done" if edited.ok else "failed", recovered=True,
                )

        def finish(current):
            if current.get("lease_owner") == owner and current.get("lease_until", 0) > self.clock():
                current.update(reconciled=True, status="done", lease_until=0, outcome="reconciled")
                if current.get("modlog_id"):
                    current["modlog_revision"] = self.clock()
            return current

        await self.store.update_incident(incident, finish)
