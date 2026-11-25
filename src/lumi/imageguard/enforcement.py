import asyncio
import contextlib
import secrets
from collections import defaultdict
from types import SimpleNamespace

from redis.exceptions import RedisError

from lumi.contracts import DiscordEffectGrant, DiscordIntent, TemporaryAttachment, encode
from lumi.contracts.staff_moderation import STAFF_EFFECT, STAFF_READS, StaffEffect, policy_lane, policy_token
from lumi.shared.discord_effects import EFFECT_GRANT_SECONDS, effect_key, source_digest
from lumi.worker.actions import ActionUnavailable

from .log_entry import evidence_page, summary
from .urls import observed_sources, signature


def result(status, *, ok=False):
    return SimpleNamespace(ok=ok, status=status, error_code=None if ok else status, data={})


class Enforcement:
    def __init__(self, store, redis, actions, generation, clock):
        self.store, self.redis, self.actions = store, redis, actions
        self.generation, self.clock = generation, clock
        self.locks = defaultdict(asyncio.Lock)

    def lane(self, guild):
        return policy_lane("security", "imageguard", guild)

    async def change(self, guild, operation, expected):
        async with self.locks[guild]:

            await self.redis.delete(self.lane(guild))
            return await self.store.change(guild, operation, expected)

    async def current(self, row, deadline):
        return (
            self.clock() < deadline
            and (await self.store.snapshot(row["guild"]))["revision"] == row["config_revision"]
        )

    async def effect(self, row, owner, job, deadline, operation, *, step=None, intent=None, **fields):
        data = StaffEffect(
            operation, row["log_channel"], row["config_revision"],
            reason="" if operation in STAFF_READS else "ImageGuard · " + row["id"], **fields,
        )
        attempt = "effect:" + secrets.token_hex(16)
        key = None
        if operation not in STAFF_READS:
            async with self.locks[row["guild"]]:
                if not await self.current(row, deadline):
                    if step:
                        await self.store.step(row["id"], owner, step, "skipped", reason="stale_policy")
                    return result("stale_policy")
                if step:
                    receipt = {"attempt": attempt} if operation == "delete" else {}
                    state = await self.store.step(row["id"], owner, step, "sending", **receipt)
                    if state.get("state") != "sending":
                        return result("incident_in_progress")
                key = effect_key("security", job, attempt)
                grant = DiscordEffectGrant(
                    "imageguard", self.generation, STAFF_EFFECT, row["guild"], row["channel"],
                    row["message"], source_digest((data, intent)),
                    min(deadline, self.clock() + EFFECT_GRANT_SECONDS * 1000),
                )
                try:
                    async with self.redis.pipeline(transaction=True) as transaction:
                        transaction.set(key, encode(grant), ex=EFFECT_GRANT_SECONDS)
                        transaction.set(
                            self.lane(row["guild"]), policy_token(self.generation, row["config_revision"]),
                            ex=EFFECT_GRANT_SECONDS,
                        )
                        await transaction.execute()
                except RedisError:
                    return result("uncertain")
        try:
            answer = await self.actions.request(
                kind=STAFF_EFFECT, job_id=job, guild_id=row["guild"], channel_id=row["channel"],
                message_id=row["message"], member_id=row["user"], idempotency_key=attempt,
                staff_effect=data, intent=intent,
            )
            if not answer.ok and getattr(answer, "error_code", None) in {
                "discord_action_failed", "action_in_progress",
            }:
                return result("uncertain")
            return answer
        except (ActionUnavailable, RedisError, TimeoutError, OSError):
            return result("uncertain")
        finally:
            if key:
                with contextlib.suppress(RedisError):
                    await self.redis.delete(key)

    async def run(self, row, event, job, arguments):
        if (
            event.get("dispatch") != "passive"
            or (row["guild"], row["channel"], row["message"], row["user"]) != (
                event.get("guild_id"), event.get("channel_id"), event.get("message_id"), event.get("actor_id"),
            )
            or signature(observed_sources(arguments)) != row["signature"]
            or int(event.get("source_revision_ms") or event.get("created_at_ms") or 0) != row["source_revision"]
        ):
            return result("action_scope_mismatch")
        if row.get("status") == "done":
            return result("already_completed", ok=True)
        if row.get("steps"):
            return result("incident_in_progress")
        owner = secrets.token_hex(16)
        row = await self.store.claim(row["id"], owner, self.clock())
        if not row:
            return result("incident_in_progress")
        if row.get("steps"):
            return result("incident_in_progress")
        deadline = min(row["lease_until"], event.get("_expires_at_ms", row["lease_until"]))
        outcome = await self.execute(row, owner, event, job, deadline)
        await self.store.finish(row["id"], owner, outcome)
        await self.store.metric(row["guild"], "matched", "" if outcome == "completed" else outcome)
        return result(outcome, ok=outcome == "completed")

    async def execute(self, row, owner, event, job, deadline):
        config = await self.store.snapshot(row["guild"])
        group = next((item for item in config["groups"] if item["id"] == row["group"] and item["enabled"]), None)
        if (
            not group or config["revision"] != row["config_revision"]
            or config["log_channel"] != row["log_channel"]
            or not any(i["id"] == row["reference"] and i["group"] == group["id"] for i in config["images"])
            or row["match"] not in {"identical", "similar"}
            or (row["match"] == "similar" and not 0 <= row["distance"] <= group["distance"])
        ):
            return "stale_policy"
        mode = group["action"]
        if mode in {"kick", "ban"} and not group["confirmed"]:
            return "confirmation_required"

        async def effect(operation, **fields):
            return await self.effect(row, owner, job, deadline, operation, **fields)

        async def settle(name, response):
            state = (
                "done" if response.ok else
                "uncertain" if response.status == "uncertain" else
                "skipped" if (response.data or {}).get("skipped") or response.error_code == "stale_policy" else
                "failed"
            )
            await self.store.step(row["id"], owner, name, state)
            return state

        roles = (*group["add_roles"], *group["remove_roles"]) if mode == "roles" else ()
        readiness = await effect("inspect", inspect_roles=roles)
        if not readiness.ok:
            return readiness.error_code or "enforcement_unavailable"
        facts = readiness.data or {}
        if mode == "roles" and (
            not facts.get("manage_roles") or not set(group["add_roles"]) <= set(facts.get("grantable", ()))
            or (not group["remove_all"] and not set(group["remove_roles"]) <= set(facts.get("manageable", ())))
        ):
            return "role_unavailable"
        item = TemporaryAttachment(**row["attachment"])
        if item.expires_at_ms <= self.clock() or item.file_name != "SPOILER_imageguard.png":
            return "evidence_unavailable"
        delivered = await effect("send", step="evidence", intent=DiscordIntent(
            "send", pages=(evidence_page(row, group, "Pending"),), attachments=(item,), presentation_mode="legacy",
        ))
        evidence = (delivered.data or {}).get("evidence") if delivered.ok else None
        if not evidence:
            if delivered.ok:
                delivered = result("uncertain")
            await settle("evidence", delivered)
            return "evidence_uncertain" if delivered.status == "uncertain" else (
                delivered.error_code or "evidence_unavailable"
            )
        await self.store.step(row["id"], owner, "evidence", "done", message=evidence, channel=row["log_channel"])
        outcome = "delete_failed"
        try:
            observed = await effect("observe_source")
            source = observed.data or {}
            if (
                not observed.ok or not source.get("proof") or source.get("revision") != row["source_revision"]
                or signature(observed_sources(source.get("arguments", {}))) != row["signature"]
            ):
                await self.store.step(row["id"], owner, "delete", "skipped", reason="eligibility_changed")
                return outcome
            removed = await effect("delete", step="delete", proof=source["proof"])
            deletion = await settle("delete", removed)
            if deletion != "done":
                outcome = "delete_uncertain" if deletion == "uncertain" else "delete_failed"
                return outcome
            outcome = "completed"
            if mode == "roles":
                for role in group["add_roles"]:
                    if (state := await settle(f"add_{role}", await effect("add_role", role=role, step=f"add_{role}"))) != "done":
                        outcome = "roles_uncertain" if state == "uncertain" else "roles_partial"
                        return outcome
                if group["remove_all"] and group["add_roles"]:
                    current = await effect("inspect")
                    if not current.ok:
                        outcome = "roles_partial"
                        return outcome
                    facts = current.data or {}
                for role in (facts.get("roles", ()) if group["remove_all"] else group["remove_roles"]):
                    if role in group["add_roles"]:
                        continue
                    step = f"remove_{role}"
                    if role not in facts.get("manageable", ()):
                        await self.store.step(row["id"], owner, step, "skipped", reason="unmanageable")
                        continue
                    state = await settle(step, await effect("remove_role", role=role, step=step))
                    if state == "uncertain":
                        outcome = "roles_uncertain"
                    elif state != "done" and outcome != "roles_uncertain":
                        outcome = "roles_partial"
            elif mode in {"kick", "ban"}:
                if not await self.store.reserve_terminal(row["id"], row["guild"], self.clock()):
                    await self.store.step(row["id"], owner, "terminal", "skipped", reason="hourly_limit")
                    outcome = "terminal_limit"
                else:
                    response = await effect(mode, step="terminal")
                    state = await settle("terminal", response)
                    if state != "done":
                        outcome = "terminal_uncertain"
                        if state in {"failed", "skipped"}:
                            await self.store.release_terminal(row["id"])
                            if response.error_code == "terminal_unavailable":
                                outcome = response.error_code
            return outcome
        finally:
            current = await self.store.incident(row["id"])
            edited = await effect("edit", evidence=evidence, intent=DiscordIntent(
                "edit", pages=(evidence_page(row, group, summary(current, group) or outcome),),
                presentation_mode="legacy",
            ))
            if not edited.ok:


                await self.store.step(row["id"], owner, "evidence_update", "uncertain")
