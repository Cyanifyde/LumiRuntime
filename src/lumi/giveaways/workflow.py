from __future__ import annotations

import copy
import secrets

from lumi.contracts import DiscordIntent, PresentationControl
from lumi.shared.discord_effects import payload_digest

from . import expanded as v2
from . import models as engine
from .durations import duration
from .protocol import DRAW_BATCH_SIZE, pending_edit_digest, post_location, publication_binding


NOT_DISPATCHED = frozenset({
    "unknown_job", "expired_job", "stale_generation", "capability_denied", "action_scope_mismatch",
    "policy_denied", "blocked", "guild_unavailable", "channel_unavailable", "giveaway_rejected",
    "effect_not_authorized",
})


class GiveawayWorkflow:


    def __init__(self, store, clock, *, revoke_publication=None, revoke_mutation=None):
        self.store = store
        self.clock = clock
        self.revoke_publication = revoke_publication
        self.revoke_mutation = revoke_mutation

    @staticmethod
    def require_facts(proof, **expected):
        if not isinstance(proof, dict) or any(
            proof.get(key) != value for key, value in expected.items()
        ):
            raise engine.GiveawayError("Unavailable.")

    def require_legacy_post(self, request, record, *, confirmation=None):
        event = request.event
        if (
            not record or not record["message"] or event.get("dispatch") != "component"
            or request.handler != "giveaways.component"
            or (event.get("guild_id"), event.get("channel_id")) != (record["guild"], record["channel"])
        ):
            raise engine.GiveawayError("Unavailable.")
        if request.route == "giveaway.confirm":
            self.require_facts(
                confirmation, kind="confirm", operation="reroll", owner=event.get("actor_id"),
                guild=record["guild"], id=record["id"], message=record["message"],
            )
            if confirmation["expires"] <= self.clock():
                raise engine.GiveawayError("Expired.")
            return
        if request.route == "giveaway.join":
            origin = record["message"]
        elif request.route == "giveaway.reroll":
            origin = record["deliveries"].get(f"result:{record['round']}", {}).get("message")
        else:
            raise engine.GiveawayError("Unavailable.")
        if not origin or event.get("message_id") != origin or request.arguments.get("id") != record["id"]:
            raise engine.GiveawayError("Unavailable.")

    def interaction_member(self, request, job, record, proof):
        actor = int(request.event["actor_id"])
        self.require_facts(proof, job=job, guild=record["guild"], channel=record["channel"], users=[actor])
        members = proof.get("members")
        if not isinstance(members, list) or len(members) != 1:
            raise engine.GiveawayError("Unavailable.")
        self.require_facts(members[0], user=actor)
        return members[0]

    async def observe_deleted(self, guild, channel, message, *, identifier=None):

        if any(type(value) is not int or value <= 0 for value in (guild, channel, message)):
            return False
        record = (
            await self.store.for_message(guild, message) if identifier is None
            else await self.store.get(guild, identifier)
        )
        if not record or (record["channel"], record["message"]) != (channel, message):
            return False
        async with self.store.locked(guild, record["id"]) as current:
            if (current["guild"], current["channel"], current["message"]) != (guild, channel, message):
                return False
            if current["status"] == "cancelled" and current.get("cancel_pending"):
                current.update(cancel_pending=False, next_ms=v2.NEVER)
            else:
                engine.stop_deleted(current, self.clock())
        return True

    async def observe_rendered(self, guild, identifier, receipt, proof):
        self.require_facts(proof, guild=guild, record=identifier, receipt=receipt)
        async with self.store.locked(guild, identifier) as current:
            if (
                current["status"] in {"cancelled", "failed"} or current.get("pending_edit")
                or (current["channel"], current["message"]) != (proof.get("channel"), proof.get("message"))
                or engine.signature(current) != proof.get("signature")
            ):
                return False
            current["rendered"] = proof["signature"]
        return True

    @staticmethod
    def edited_record(record):
        pending = record["pending_edit"]
        return dict(
            record, spec=pending["spec"], ends=pending["ends"], duration=pending["spec"]["duration"],
            ui_version=2, revision=record["revision"] + 1, pending_edit=None, error="",
        )

    async def settle_edit(self, record, receipt, result):
        proof = result.data.get("applied_edit")
        expected = {
            **post_location(record), "revision": record["revision"],
            "edit": pending_edit_digest(record), "receipt": receipt,
        }
        if self.mutation_outcome(record, receipt, result) == "edited":
            proof = {**expected, "disposition": "edited"}
        if not expected["edit"] or not isinstance(proof, dict) or any(
            proof.get(key) != value for key, value in expected.items()
        ):
            return False
        if not result.ok or proof.get("disposition") != "edited":
            return False
        return await self._finish_edit(record)

    async def reject_edit(self, record, error):
        return await self._finish_edit(record, error=error)

    @staticmethod
    def _same_pending_edit(current, record):
        digest = pending_edit_digest(record)
        return (
            current["status"] == "active" and current["revision"] == record["revision"]
            and post_location(current) == post_location(record)
            and digest is not None and pending_edit_digest(current) == digest
        )

    @staticmethod
    def mutation_outcome(record, receipt, result):
        delivery = result.data.get("delivery")
        if not isinstance(delivery, dict) or (
            delivery.get("guild_id"), delivery.get("channel_id"),
            delivery.get("parent_id"), delivery.get("attempt"),
        ) != (record["guild"], record["channel"], record["message"], receipt):
            return None
        outcome = delivery.get("outcome")
        if result.ok and outcome in {"edited", "deleted"} and delivery.get("message_id") == record["message"]:
            return outcome
        if not result.ok and outcome == "missing" and result.error_code == "message_not_found":
            return outcome
        return None

    @staticmethod
    def published_target(current, record):
        delivery = current["deliveries"].get("publish", {})
        return (
            current["message"] and post_location(current) == post_location(record)
            and delivery.get("state") == "sent" and delivery.get("message") == current["message"]
        )

    async def stage_post_effect(self, record, stage, *, signature=None):
        async with self.store.locked(record["guild"], record["id"]) as current:
            if not self.published_target(current, record) or current["revision"] != record["revision"]:
                return False
            if signature is None:
                if current["status"] != "cancelled" or not current.get("cancel_pending"):
                    return False
            elif (
                current["status"] in {"cancelled", "failed"} or current.get("pending_edit")
                or engine.signature(current) != signature or current["rendered"] == signature
            ):
                return False
            return await stage()

    async def begin_edit_effect(self, record, receipt, *, stage=None):
        async with self.store.locked(record["guild"], record["id"]) as current:
            if not self._same_pending_edit(current, record):
                return False
            if stage is not None and (not self.published_target(current, record) or not await stage()):
                return False


            flight = current.setdefault("edit_in_flight", {"first": receipt})
            if flight["first"] != receipt:
                flight["multiple"] = True
            flight["latest"] = receipt
        return True

    async def release_unstarted_edit(self, record, receipt, outcome):
        if outcome.ok or outcome.error_code not in NOT_DISPATCHED | {"source_verification_expired"}:
            return
        async with self.store.locked(record["guild"], record["id"]) as current:
            flight = current.get("edit_in_flight", {})
            if (
                self._same_pending_edit(current, record) and not flight.get("multiple")
                and flight.get("first") == flight.get("latest") == receipt
            ):
                current.pop("edit_in_flight")

    async def _finish_edit(self, record, *, error=None):
        async with self.store.locked(record["guild"], record["id"]) as current:
            if not self._same_pending_edit(current, record) or (error is not None and current.get("edit_in_flight")):
                return False
            if self.revoke_mutation:
                await self.revoke_mutation(current)
            if error is None:
                current.update(self.edited_record(current))
                current["rendered"] = engine.signature(current)
            else:
                current.update(pending_edit=None, error=error)
            current.pop("edit_in_flight", None)
            current["next_ms"] = self.clock()
        return True

    async def reject_publication(self, record, error):
        async with self.store.locked(record["guild"], record["id"]) as current:
            delivery = current["deliveries"]["publish"]
            if (
                current["status"] != "publishing" or current["message"] or delivery["state"] != "pending"
                or post_location(current) != post_location(record)
                or engine.signature(current) != engine.signature(record)
            ):
                return False
            delivery["state"] = "failed"
            current.update(status="failed", error=error, next_ms=v2.NEVER)
        return True

    async def claim_publication(self, record, intent, *, rendered_signature, job=None, stage=None):
        async with self.store.locked(record["guild"], record["id"]) as current:
            delivery = current["deliveries"]["publish"]
            if (
                current["status"] != "publishing" or current["message"]
                or current["channel"] != record["channel"]
                or engine.signature(current) != rendered_signature or delivery["state"] != "pending"
                or current["next_ms"] > self.clock()
            ):
                return None
            attempt = f"gw-publication:{current['id']}:{secrets.token_hex(8)}"
            digest = payload_digest(intent, ())
            if stage is not None:


                if not await stage(attempt, digest):
                    return None
                delivery["grant_job"] = job
            delivery.update(
                state="sending", at=self.clock(), attempt=attempt, revision=current["revision"],
                digest=digest,
            )
            return copy.deepcopy(current)

    async def settle_publication(self, record, result, *, durable=False):
        expected = publication_binding(record)
        proof = result.data.get("publication")
        transport = result.data.get("delivery")
        if transport is not None:
            exact = dict(
                guild_id=record["guild"], channel_id=record["channel"], parent_id=None,
                attempt=expected["attempt"],
            )
            if not isinstance(transport, dict) or not expected["attempt"] or any(
                key not in transport or transport[key] != value for key, value in exact.items()
            ):
                return False
            proof = {**expected, "disposition": transport.get("outcome"), "message_id": transport.get("message_id")}
        if durable and proof is None and not result.ok and result.error_code in NOT_DISPATCHED:
            proof = {**expected, "disposition": "rejected"}
        if not isinstance(proof, dict) or any(proof.get(k) != v for k, v in expected.items()):
            return False
        outcome = proof.get("disposition")
        message = proof.get("message_id")
        if result.ok:
            if outcome != "sent" or type(message) is not int or not 0 < message < 2**64:
                return False
        elif outcome not in {"rejected", "uncertain"}:
            return False
        async with self.store.locked(record["guild"], record["id"]) as current:
            delivery = current["deliveries"]["publish"]
            cancelled = current["status"] == "cancelled" and current.get("cancel_pending")
            if (
                (current["status"] != "publishing" and not cancelled) or current["message"]
                or publication_binding(current) != expected or delivery["state"] not in {"sending", "ambiguous"}
            ):
                return False
            if outcome == "sent":
                delivery.pop("grant_job", None)
                delivery.update(state="sent", message=message)
                current.update(message=message, error="", next_ms=self.clock())
                if not cancelled:
                    current.update(
                        status="active",
                        ends=(message >> 22) + engine.DISCORD_EPOCH + current["duration"],
                    )
            elif cancelled and outcome == "rejected":
                delivery.pop("grant_job", None)
                delivery["state"] = "cancelled"
                current.update(cancel_pending=False, next_ms=v2.NEVER)
            elif outcome == "rejected":
                delivery.pop("grant_job", None)
                delivery.update(state="pending", attempt=None, digest=None)
                current["next_ms"] = self.clock() + 30_000
            else:
                delivery["state"] = "ambiguous"
                current.update(error="Publication needs review.", next_ms=self.clock() + 30_000)
            return delivery["state"]

    async def finish_unpublished_cancel(self, record):
        async with self.store.locked(record["guild"], record["id"]) as current:
            if (
                current["status"] == "cancelled" and current.get("cancel_pending")
                and not current["message"] and current["deliveries"]["publish"]["state"] == "cancelled"
            ):
                current.update(cancel_pending=False, next_ms=v2.NEVER)

    @staticmethod
    def completion_payload(record):
        if v2.is_v2(record):
            delivery = record["deliveries"][f"result:{record['round']}"]
            winners = [award["user"] for award in delivery.get("awards", record["awards"]) if award["user"]]
        else:
            winners = [record["winner"]] if record["winner"] else []
        recipients = tuple(dict.fromkeys([record["host"], *winners]))
        controls = (
            (PresentationControl("button", "giveaway", "reroll", record["id"], label="Reroll"),)
            if record["winner"] and not v2.is_v2(record) else ()
        )
        return DiscordIntent(
            "send", content=engine.completion(record), controls=controls, presentation_mode="legacy",
        ), recipients

    async def claim_completion(self, record):
        guild, identifier, round_number = record["guild"], record["id"], record["round"]
        async with self.store.locked(guild, identifier) as current:
            delivery = current["deliveries"].get(f"result:{round_number}", {})
            publish = current["deliveries"].get("publish", {})
            if (
                current["status"] != "ended" or current["round"] != round_number
                or post_location(current) != post_location(record) or not current["message"]
                or current["rendered"] != engine.signature(current) or delivery.get("state") != "pending"
                or publish.get("state") != "sent" or publish.get("message") != current["message"]
            ):
                return None
            intent, recipients = self.completion_payload(current)
            attempt = f"gw-result:{identifier}:{round_number}:{secrets.token_hex(8)}"
            delivery.update(state="sending", at=self.clock(), attempt=attempt, digest=payload_digest(intent, recipients))
            return copy.deepcopy(current), intent, recipients

    async def settle_completion(self, record, result, *, durable=False):
        proof = result.data.get("completion")
        round_number = record["round"]
        key = f"result:{round_number}"
        attempt = record["deliveries"][key].get("attempt")
        expected = {**post_location(record), "round": round_number, "attempt": attempt}
        transport = result.data.get("delivery")
        if transport is not None:
            exact = dict(
                guild_id=record["guild"], channel_id=record["channel"],
                parent_id=record["message"], attempt=attempt,
            )
            if not isinstance(transport, dict) or not attempt or any(
                transport.get(name) != value for name, value in exact.items()
            ):
                return False
            outcome = transport.get("outcome")
            if outcome == "missing" and (result.ok or result.error_code != "message_not_found"):
                return False
            proof = {**expected, "disposition": outcome, "message_id": transport.get("message_id")}
        if durable and proof is None and not result.ok and result.error_code in NOT_DISPATCHED:
            proof = {**expected, "disposition": "rejected"}
        if not isinstance(proof, dict) or any(proof.get(k) != v for k, v in expected.items()):
            return False
        outcome = proof.get("disposition")
        if result.ok:
            if outcome != "sent" or type(proof.get("message_id")) is not int or proof["message_id"] <= 0:
                return False
        elif outcome not in {"rejected", "uncertain"} and not (outcome == "missing" and transport):
            return False
        async with self.store.locked(record["guild"], record["id"]) as current:
            delivery = current["deliveries"].get(key, {})
            expected_status = {"ended", "cancelled"} if outcome == "missing" else {"ended"}
            if (
                current["status"] not in expected_status or current["round"] != round_number
                or post_location(current) != post_location(record) or delivery.get("attempt") != attempt
                or delivery.get("state") not in {"sending", "ambiguous"}
            ):
                return False
            if outcome == "missing":


                delivery["state"] = "cancelled"
                engine.stop_deleted(current, self.clock())
                return "deleted"
            if outcome == "sent":
                delivery.update(state="sent", message=proof["message_id"])
                current["next_ms"] = self.clock()
            elif outcome == "rejected":
                delivery.update(state="pending", attempt=None, digest=None)
            else:
                delivery["state"] = "ambiguous"
            return delivery["state"]

    @staticmethod
    def eligible_rules(record, member, now):
        roles = member["roles"]
        required_role = record.get("required_role", 0)
        if required_role and required_role not in roles:
            return False
        return not v2.is_v2(record) or v2.eligible_rules(
            record["spec"],
            roles,
            member["created"],
            member["joined"],
            now,
        )

    async def entry(self, request, job, record, proof):
        if request.route != "giveaway.join":
            raise engine.GiveawayError("Unavailable.")
        actor = int(request.event["actor_id"])
        member = self.interaction_member(request, job, record, proof)
        async with self.store.locked(record["guild"], record["id"]) as current:
            self.require_legacy_post(request, current)
            if post_location(current) != post_location(record):
                raise engine.GiveawayError("This giveaway changed.")
            now = self.clock()
            if not self.eligible_rules(current, member, now):
                if current.get("required_role") and current["required_role"] not in member["roles"]:
                    raise engine.GiveawayError("You need the required role.")
                raise engine.GiveawayError("You do not meet the entry requirements.")
            key = str(request.event.get("interaction_id") or job)
            v2.set_entry(current, actor, key, now)

    @staticmethod
    def selection_available(access, kind, values):
        selected = access.get("selection")
        return bool(
            isinstance(selected, dict) and selected.get("kind") == kind
            and selected.get("values") == list(values)
            and selected.get("available") == list(values)
        )

    async def reroll(self, request, job, record, proof, *, confirmation=None):
        if request.route != ("giveaway.confirm" if confirmation else "giveaway.reroll"):
            raise engine.GiveawayError("Unavailable.")
        member = self.interaction_member(request, job, record, proof)
        async with self.store.locked(record["guild"], record["id"]) as current:
            self.require_legacy_post(request, current, confirmation=confirmation)
            if post_location(current) != post_location(record):
                raise engine.GiveawayError("This giveaway changed.")
            if member["user"] != current["host"] and not member["administrator"]:
                raise engine.GiveawayError("Host or administrator only.")
            if confirmation is not None and current.get("control_job") == job:
                return current["revision"], current["message"]
            if current["revision"] != record["revision"] or (
                confirmation is not None and current["revision"] != confirmation["revision"]
            ):
                raise engine.GiveawayError("This giveaway changed.")
            if not engine.ready_to_reroll(current):
                raise engine.GiveawayError("Reroll unavailable.")
            if confirmation is not None:
                engine.begin_draw(current, self.clock(), reroll=True, expected=confirmation["revision"])
                current["control_job"] = job
            return current["revision"], current["message"]

    async def draw(self, record, job, cursor, proof):
        guild, identifier = record["guild"], record["id"]
        self.require_facts(proof, guild=guild, job=job, channel=record["channel"])
        async with self.store.locked(guild, identifier) as current:
            if (
                current["status"] != "drawing"
                or current["revision"] != record["revision"]
                or post_location(current) != post_location(record)
                or current["draw"]["cursor"] != cursor
            ):
                return False
            users = current["draw"]["order"][cursor : cursor + DRAW_BATCH_SIZE]
            if not users or users != proof.get("users"):
                raise engine.GiveawayError("Invalid draw result.")
            now = self.clock()
            eligible = {
                member["user"]
                for member in proof["members"]
                if member["user"] in users
                and member["user"] != current["host"]
                and self.eligible_rules(current, member, now)
            }
            engine.accept_candidates(current, users, eligible, now)
        return True

    async def control(self, request, job, record, operation, *, revision=None, selected_slots=()):
        guild, actor = int(request.event["guild_id"]), int(request.event["actor_id"])
        if not record or record["guild"] != guild or operation not in {"leave", "end", "cancel", "reroll"}:
            raise engine.GiveawayError("Unavailable.")
        async with self.store.locked(guild, record["id"]) as current:
            if post_location(current) != post_location(record):
                raise engine.GiveawayError("This giveaway changed.")
            now = self.clock()
            if operation == "leave":
                key = str(request.event.get("interaction_id") or job)
                v2.set_entry(current, actor, key, now, entered=False)
            else:
                if current.get("control_job") == job:
                    return {}
                if (
                    current["revision"] != revision
                    or (current.get("pending_edit") and operation != "cancel")
                ):
                    raise engine.GiveawayError("This giveaway changed.")
                self.manage(current, operation, now, {"selected_slots": selected_slots}, revision)
                if operation == "cancel" and current["message"] and self.revoke_mutation:
                    await self.revoke_mutation(current)
                if operation == "cancel" and current["cancel_pending"] and not current["message"] and self.revoke_publication:
                    publication = current["deliveries"]["publish"]
                    if publication.get("grant_job") and await self.revoke_publication(
                        publication["grant_job"], publication["attempt"],
                    ):
                        publication.pop("grant_job")
                        publication["state"] = "cancelled"
                        current.update(cancel_pending=False, next_ms=v2.NEVER)
                current["control_job"] = job
        return {}

    @staticmethod
    def prepare_edit(record, draft, actor, job, spec, now):
        editing = draft.get("editing") or {}
        if not record or editing.get("id") != record["id"]:
            raise engine.GiveawayError("Giveaway unavailable.")
        if (
            record["revision"] != editing.get("revision")
            or record["status"] != "active"
            or record["ends"] <= now
            or record.get("pending_edit")
        ):
            raise engine.GiveawayError("This giveaway changed.")
        previous = record if v2.is_v2(record) else v2.upgrade(record)
        if spec["baseline"] != previous["spec"]["baseline"]:
            raise engine.GiveawayError("Server requirements cannot be removed.")
        return dict(
            previous,
            pending_edit=dict(
                spec=spec,
                actor=actor,
                draft=draft["id"],
                ends=now + spec["duration"] if draft.get("timing_changed") else previous["ends"],
                base=previous["revision"],
                approved=now,
            ),
            last_edit_job=job,
        )

    async def approve_edit(self, draft, record, proposed):
        guild, identifier = record["guild"], record["id"]
        pending = dict(proposed["pending_edit"])
        expected = pending["base"]
        current_draft = await self.store.draft(guild, draft["owner"], draft["id"], self.clock())
        if current_draft != draft:
            raise engine.GiveawayError("Draft changed. Refresh it.")
        if not v2.is_v2(record):
            await self.store.upgrade_expanded(guild, identifier, expected)
        async with self.store.locked(guild, identifier) as current:
            now = self.clock()
            if current.get("last_edit_job") == proposed["last_edit_job"]:
                return
            if (
                post_location(current) != post_location(record)
                or current["revision"] != expected
                or current["status"] != "active"
                or current["ends"] <= now
                or current.get("pending_edit")
                or current["spec"] != proposed["spec"]
            ):
                raise engine.GiveawayError("This giveaway changed.")
            pending["approved"] = now
            pending["ends"] = now + pending["spec"]["duration"] if draft.get("timing_changed") else current["ends"]
            if self.revoke_mutation:
                await self.revoke_mutation(current)
            current["pending_edit"] = pending
            current["last_edit_job"] = proposed["last_edit_job"]
            current["next_ms"] = now

    @staticmethod
    def manage(current, operation, now, proof, revision):
        if operation == "cancel":
            if current["status"] not in {"active", "publishing"}:
                raise engine.GiveawayError("Giveaway already closed.")
            current["pending_edit"] = None
            current.pop("edit_in_flight", None)
            engine.stop_deleted(current, now)
            current.update(cancel_pending=True, next_ms=now)
        elif operation == "end":
            if current["status"] != "active" or current["ends"] <= now:
                raise engine.GiveawayError("Giveaway already closed.")
            current["ends"] = now
            engine.begin_draw(current, now)
        else:
            engine.begin_draw(
                current,
                now,
                reroll=True,
                expected=revision,
                selected=proof.get("selected_slots") or None,
            )

    async def configure(self, guild, actor, route, values, fields, config):
        revision = config["revision"]
        if route == "setup_channel":
            if len(values) != 1:
                raise engine.GiveawayError("Select a channel.")
            await self.store.configure(guild, int(values[0]), actor, expected=revision)
            return
        if route == "setup_host":
            if len(values) > 1:
                raise engine.GiveawayError("Role unavailable.")
            await self.store.configure_role(
                guild,
                int(values[0]) if values else 0,
                actor,
                host=True,
                expected=revision,
            )
            return
        settings = copy.deepcopy(config.get("settings") or {})
        rule = v2.rules(settings.get("rules"))
        if route in {"setup_roles", "setup_exclude"}:
            if len(values) > 10:
                raise engine.GiveawayError("Role unavailable.")
            rule["roles" if route == "setup_roles" else "excluded"] = [int(role) for role in values]
        elif route == "setup_mode" and len(values) == 1 and values[0] in {"all", "any"}:
            rule["mode"] = values[0]
        elif route == "setup_age":
            rule.update(
                account_ms=duration(
                    fields.get("account") or "0", minimum=0, maximum=100 * 366 * v2.DAY
                ),
                member_ms=duration(
                    fields.get("member") or "0", minimum=0, maximum=100 * 366 * v2.DAY
                ),
            )
        elif route == "setup_defaults":
            settings["duration"] = duration(fields.get("duration", ""))
        elif (
            route == "setup_layout"
            and len(values) == 1
            and values[0] in {"image-first", "text-first"}
        ):
            settings["layout"] = values[0]
        else:
            raise engine.GiveawayError("Unavailable.")
        settings["rules"] = v2.rules(rule)
        await self.store.save_settings(
            guild, settings, revision, clear_legacy=route == "setup_roles"
        )
