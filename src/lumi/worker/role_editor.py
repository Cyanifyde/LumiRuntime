import asyncio
import json
import re
import secrets
from contextlib import nullcontext

import msgspec
from redis.exceptions import LockError, RedisError

from lumi.contracts import (
    DiscordActionResult,
    DiscordIntent,
    PresentationPage,
    ResponseRevision,
    decode,
    encode,
)
from lumi.contracts.role_access import (
    READ_GUILD_ROLES,
    SET_ROLE_MEMBERSHIP,
    RoleFact,
    RoleFactsQuery,
    RoleMembership,
    RoleTargetBinding,
    role_target_key,
    role_write_key,
    role_write_receipt,
)
from lumi.moderation.roles import HELP, PAGE_SIZE, PREFIX, TTL, match_roles, outcome, page
from lumi.moderation.rules import identifier
from lumi.shared.response_revisions import publish_revision
from lumi.ui_keys import write_keys

from .actions import ActionUnavailable

TOKEN = re.compile(r"[a-f0-9]{16}")
ERRORS = {
    "permission_denied": "Missing permission.",
    "policy_denied": "Missing permission.",
    "bot_permission_denied": "Lumi needs Manage Roles.",
    "guild_unavailable": "Server unavailable.",
    "member_unavailable": "Member unavailable.",
    "member_changed": "Member changed.",
    "role_changed": "Roles changed.",
    "role_unavailable": "Missing permission or role unavailable.",
    "discord_rejected": "Role edit rejected.",
    "expired_job": "Expired.",
    "action_scope_mismatch": "Menu changed or expired.",
}


class RoleError(ValueError):
    pass


class RoleEditor:
    def __init__(self, actions, clock, *, cache, queue, store, group, generation):
        self.actions, self.clock, self.cache, self.queue = actions, clock, cache, queue
        self.store, self.group, self.generation = store, group, generation

    async def handle(self, request, job_id):
        event, args = request.event, request.arguments
        component = event.get("dispatch") == "component"
        try:
            if component:
                return await self.component(request, job_id)
            target = args.get("mention") or args.get("user_id")
            if not target or args.get("help"):
                raise RoleError(HELP.format(p=event.get("prefix") or "."))
            return await self.open(
                request, job_id, target, str(args.get("role_query") or "").strip()
            )
        except (LockError, RoleError) as exc:
            text = "Applying…" if isinstance(exc, LockError) else str(exc)
            return DiscordIntent(
                "ephemeral" if component else "send",
                pages=(PresentationPage(description=text),),
            ), None

    def lock(self, key):
        return self.cache.lock(PREFIX + key + ":lock", timeout=120, blocking_timeout=2)

    async def facts(self, request, job_id, target, *, fresh=False):
        event, roles, revision = request.event, {}, None
        number = 0
        while self.clock() < event["_expires_at_ms"]:
            result = await self.actions.request(
                job_id=job_id,
                kind=READ_GUILD_ROLES,
                guild_id=event["guild_id"],
                channel_id=event["channel_id"],
                message_id=event["message_id"],
                member_id=target,
                role_facts=RoleFactsQuery(number, fresh and not number),
                idempotency_key=f"role-facts:{job_id}:{number}",
            )
            if not result.ok:
                raise RoleError(ERRORS.get(result.error_code, "Unavailable."))
            data = result.data
            if revision is not None and revision != data["revision"]:
                raise RoleError("Roles changed.")
            revision = data["revision"]
            for raw in data["roles"]:
                info = msgspec.convert(raw, type=RoleFact)
                roles[info.id] = info
            if not data["more"]:
                return roles, set(data["assigned"]), data["joined"]
            number += 1
        raise RoleError("Roles unavailable.")

    async def load(self, token):
        if not isinstance(token, str) or TOKEN.fullmatch(token) is None:
            raise RoleError("Menu expired.")
        raw = await self.cache.get(PREFIX + token)
        session = json.loads(raw) if raw else None
        if not session or session["expires"] <= self.clock():
            raise RoleError("Menu expired.")
        return session

    async def save(self, session, *, publish=False):
        remaining, token = session["expires"] - self.clock(), session["token"]
        if remaining <= 0:
            raise RoleError("Menu expired.")
        binding = RoleTargetBinding(
            "moderation",
            session["guild"],
            session["channel"],
            session["actor"],
            session["target"],
            session["expires"],
        )
        pipe = self.cache.pipeline(transaction=True)
        pipe.set(PREFIX + token, json.dumps(session), px=remaining)
        pipe.set(role_target_key(self.group, token), encode(binding), px=remaining)
        for key in write_keys("owner", token, self.group):
            pipe.set(key, str(session["actor"]), px=remaining)
        if publish:
            publish_revision(
                pipe,
                self.group,
                token,
                module="moderation",
                actor_id=session["actor"],
                guild_id=session["guild"],
                channel_id=session["channel"],
                revision=session["revision"],
                expires_at_ms=session["expires"],
                ttl=(remaining + 999) // 1000,
            )
        await pipe.execute()

    async def finish(self, session, roles, assigned, component):
        session["revision"] += 1
        await self.save(session, publish=True)
        data = {k: session[k] for k in ("token", "revision", "target", "page", "view", "selected")}
        data["roles"] = [rid for rid in roles if rid in assigned]
        data["candidates"] = [
            dict(id=rid, name=roles[rid].name)
            for rid in session.get("candidates", [])
            if rid in roles
        ]
        if session.get("notice"):
            data["notice"] = session["notice"]
        token = session["token"]
        return DiscordIntent(
            "edit" if component else "send",
            pages=(page(data),),
            session_id=token,
        ), ResponseRevision(token, session["revision"])

    @staticmethod
    def require_roles(roles, selected):
        for rid in selected:
            if rid not in roles:
                raise RoleError("Role unavailable.")
            if not roles[rid].assignable:
                raise RoleError(f"Cannot manage <@&{rid}>.")

    async def open(self, request, job_id, target, query):
        event = request.event
        guild, channel, actor, source = (
            event[k]
            for k in (
                "guild_id",
                "channel_id",
                "actor_id",
                "message_id",
            )
        )
        key = "source:" + identifier(guild, source, "role")
        async with self.lock(key), self.lock(f"member:{guild}:{target}"):
            token = await self.cache.get(PREFIX + key)
            roles, assigned, joined = await self.facts(request, job_id, target)
            if token:
                session = await self.load(token.decode() if isinstance(token, bytes) else token)
                fields = "actor", "guild", "channel", "target", "query", "joined"
                if tuple(session[k] for k in fields) != (
                    actor,
                    guild,
                    channel,
                    target,
                    query,
                    joined,
                ):
                    raise RoleError("Command or member changed.")
            else:
                session = dict(
                    token=secrets.token_hex(8),
                    revision=-1,
                    guild=guild,
                    channel=channel,
                    actor=actor,
                    target=target,
                    joined=joined,
                    source=source,
                    expires=self.clock() + TTL,
                    view="picker",
                    selected={},
                    page=0,
                    query=query,
                )
                found = match_roles(query, roles.values()) if query else []
                if query and not found:
                    raise RoleError("Role not found.")
                if len(found) > 1:
                    session.update(view="choice", candidates=[role.id for role in found])
                if len(found) == 1:
                    self.require_roles(roles, (found[0].id,))
                    session["view"] = "receipt"
                await self.save(session)
                await self.cache.set(PREFIX + key, session["token"], px=TTL)
                if len(found) == 1:
                    rid = found[0].id
                    await self.apply(
                        session, request, job_id, roles, assigned, {rid: rid in assigned}
                    )
            if session.get("operation"):
                await self.refresh_receipt(session, assigned)
            return await self.finish(session, roles, assigned, False)

    async def component(self, request, job_id):
        args, event = request.arguments, request.event
        token = args.get("session")
        if not isinstance(token, str) or TOKEN.fullmatch(token) is None:
            raise RoleError("Menu expired.")
        operation = request.route.rpartition(".")[2].removeprefix("roles_")
        async with self.lock(token):
            session = await self.load(token)
            scope = "actor", "guild", "channel"
            if tuple(session[k] for k in scope) != tuple(event[k + "_id"] for k in scope):
                raise RoleError("Menu changed or expired.")
            revision = args.get("revision")
            if revision != session["revision"] and not (
                session.get("operation") and revision == session["revision"] - 1
            ):
                raise RoleError("Menu changed or expired.")
            mutating = operation in {"apply", "choose"}
            member_lock = (
                self.lock(f"member:{session['guild']}:{session['target']}")
                if mutating
                else nullcontext()
            )
            async with member_lock:
                return await self.advance(session, operation, request, job_id)

    async def advance(self, session, operation, request, job_id):
        args = request.arguments
        await self.save(session)
        roles, assigned, joined = await self.facts(
            request,
            job_id,
            session["target"],
            fresh=operation in {"apply", "choose"},
        )
        if joined != session["joined"]:
            raise RoleError("Member changed.")
        if session.get("operation"):
            await self.refresh_receipt(session, assigned)
        elif operation in {"stage", "choose"}:
            values = args.get("values") or ()
            if any(re.fullmatch(r"[0-9]{1,20}", str(v)) is None for v in values):
                raise RoleError("Invalid selection.")
            selected = tuple(map(int, values))
            if len(selected) > 25 or len(selected) != len(set(selected)):
                raise RoleError("Invalid selection.")
            self.require_roles(roles, selected)
            if operation == "stage" and session["view"] == "picker":
                session["selected"] = {str(rid): rid in assigned for rid in selected}
                session.pop("notice", None)
            elif operation == "choose" and session["view"] == "choice" and len(selected) == 1:
                found = [role.id for role in match_roles(session["query"], roles.values())]
                if found != session["candidates"] or selected[0] not in found:
                    raise RoleError("Matches changed.")
                session["view"] = "receipt"
                rid = selected[0]
                await self.apply(session, request, job_id, roles, assigned, {rid: rid in assigned})
            else:
                raise RoleError("Invalid selection.")
        elif operation == "apply":
            if session["view"] != "picker" or not session["selected"]:
                raise RoleError("Choose roles.")
            before = {int(rid): present for rid, present in session["selected"].items()}
            await self.apply(session, request, job_id, roles, assigned, before)
        elif operation == "clear":
            session["selected"] = {}
            session.pop("notice", None)
        elif operation in {"previous", "next"}:
            count = len(session["candidates"]) if session["view"] == "choice" else len(assigned)
            maximum = max(0, (count - 1) // PAGE_SIZE)
            session["page"] = max(
                0, min(maximum, session["page"] + (1 if operation == "next" else -1))
            )
        else:
            raise RoleError("Invalid role request.")
        return await self.finish(session, roles, assigned, True)

    async def apply(self, session, request, job_id, roles, assigned, before):
        event = request.event
        guild, target = session["guild"], session["target"]
        source = (
            event.get("interaction_id")
            if event.get("dispatch") == "component"
            else session["source"]
        )
        if re.fullmatch(r"[1-9][0-9]{0,19}", str(source)) is None:
            raise RoleError("Invalid source interaction.")
        self.require_roles(roles, before)
        deadline = min(event["_expires_at_ms"] - 3000, session["expires"])
        if self.clock() >= deadline:
            raise RoleError("Expired.")
        data = dict(
            actor=session["actor"],
            target=target,
            channel=session["channel"],
            roles=list(before),
            expected={str(k): v for k, v in before.items()},
        )
        row = dict(
            id=identifier(guild, int(source), "role"),
            type="roles",
            guild=guild,
            created=self.clock(),
            status="running",
            deadline=deadline,
            generation=self.generation,
            job=job_id,
            token=session["token"],
            joined=session["joined"],
            steps={},
            request=data,
        )
        stored, new = await self.store.begin(row)
        if stored["token"] != row["token"] or stored["request"] != data:
            raise RoleError("Invalid repeated role request.")
        session["operation"] = row["id"]
        await self.save(session)
        if new:
            if any((rid in assigned) != present for rid, present in before.items()):
                row.update(stale=True, error="Roles changed.")
            else:
                for rid, present in before.items():
                    if not await self.step(row, rid, present, event, assigned):
                        break
            row["status"] = "done"
            await self.store.save(row)
        await self.refresh_receipt(session, assigned)

    async def step(self, row, rid, present, event, assigned):
        step = row["steps"][str(rid)] = dict(role=rid, desired=not present, status="pending")
        attempt = 0
        while self.clock() < row["deadline"]:
            key = role_write_key(row["job"], rid, attempt)
            step.update(status="sending", receipt=key)
            await self.store.save(row)
            remaining = (row["deadline"] - self.clock()) / 1000
            if remaining <= 0:
                break
            try:
                result = await self.actions.request(
                    job_id=row["job"],
                    kind=SET_ROLE_MEMBERSHIP,
                    guild_id=row["guild"],
                    channel_id=event["channel_id"],
                    message_id=event["message_id"],
                    member_id=row["request"]["target"],
                    role_id=rid,
                    idempotency_key=key,
                    role_membership=RoleMembership(row["joined"], present, not present),
                    wait_seconds=min(15, remaining),
                )
            except (ActionUnavailable, RedisError, TimeoutError, OSError):
                result = None
            if result and result.ok and result.status == ("removed" if present else "added"):
                step["status"] = "done"
                (assigned.discard if present else assigned.add)(rid)
                await self.store.save(row)
                return True
            error = result.error_code if result is not None else "uncertain"
            if error == "rate_limited":
                step["status"] = "pending"
                await self.store.save(row)
                wait = max(0.05, int(result.data["retry_after_ms"]) / 1000)
                if self.clock() + wait * 1000 < row["deadline"]:
                    await asyncio.sleep(wait)
                    attempt += 1
                    continue
                row["error"] = "Rate limited."
            else:
                step["status"] = "failed" if error in ERRORS else "uncertain"
                row["error"] = ERRORS.get(error, "Incomplete.")
                if error == "role_changed":
                    row["stale"] = True
            return False
        step["status"], row["error"] = "pending", "Expired."
        return False

    async def recover(self, row):
        if row["status"] == "done":
            return row
        for step in row["steps"].values():
            if step["status"] not in {"sending", "uncertain"}:
                continue
            step["status"] = "uncertain"
            if step.get("receipt"):
                raw = await self.queue.get(
                    role_write_receipt(self.group, "moderation", step["receipt"])
                )
                native = decode(raw, DiscordActionResult) if raw else None
                if (
                    native
                    and native.ok
                    and native.status == ("added" if step["desired"] else "removed")
                ):
                    step["status"] = "done"
        planned = len(row["request"]["roles"])
        confirmed = sum(step["status"] == "done" for step in row["steps"].values())
        row.update(status="done", error="Incomplete." if planned != confirmed else "")
        await self.store.save(row)
        return row

    async def refresh_receipt(self, session, assigned):
        row = await self.store.operation(session["guild"], session["operation"])
        if row is None or row["token"] != session["token"]:
            raise RoleError("Unavailable.")
        row = await self.recover(row)
        session.pop("operation", None)
        session["notice"] = outcome(row)
        session["selected"] = (
            {str(rid): rid in assigned for rid in row["request"]["roles"]}
            if row.get("stale")
            else {}
        )

    async def recover_stopped(self):
        for row in await self.store.pending_roles():
            if row["generation"] != self.generation or row["deadline"] <= self.clock():
                try:
                    async with self.lock(f"member:{row['guild']}:{row['request']['target']}"):
                        latest = await self.store.operation(row["guild"], row["id"])
                        if latest:
                            await self.recover(latest)
                except LockError:
                    continue
