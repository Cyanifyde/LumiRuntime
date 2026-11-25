from __future__ import annotations

import re

import msgspec

from lumi.contracts.gateway import GUILD_CAPTURE_EVENTS
from lumi.contracts.native_mutations import MUTATION_ATTEMPT, EditNickname
from lumi.shared.capture_admission import CaptureAdmission, publish_admission, refresh_publication
from lumi.shared.capture_packets import decode_batch, decode_coverage
from lumi.shared.capture_relay import CaptureRelay

from .capture_wire import CAPTURE_OWNER
from .journal_store import MAX_SUBJECTS
from .models import TTL
from .mutations import observed_attempt, scope, suppressions
from .observations import snapshot_key

REASONS = {
    "gateway_started": "Gateway restarted; offline activity may be unavailable",
    "session_replaced": "Gateway session replaced; offline activity may be unavailable",
    "spool_full": "Capture queue full",
    "capture_error": "Capture unavailable",
    "baseline_error": "Member baseline unavailable",
}


_MODERATION_REASON = re.compile(
    r"Lumi (?:ban|kick|unban|timeout|untimeout) by ([0-9]{1,20}): (.*)", re.DOTALL,
)


def skip_webhook_observation(name: str, data: dict, bot_id: int | None) -> bool:

    if name == "WEBHOOKS_UPDATE":
        return True
    if name != "GUILD_AUDIT_LOG_ENTRY_CREATE" or not bot_id:
        return False
    return (
        int(data.get("action_type") or 0) in {50, 51, 52}
        and int(data.get("user_id") or 0) == bot_id
    )


def attribute_moderation_actor(obs, client_id):

    data = obs.data
    if (
        obs.kind != "GUILD_AUDIT_LOG_ENTRY_CREATE" or not client_id
        or int(data.get("user_id") or 0) != client_id
        or "moderator" in data
    ):
        return obs
    match = _MODERATION_REASON.fullmatch(str(data.get("reason") or ""))
    if match is None or not 0 < (moderator := int(match[1])) < 2**64:
        return obs
    return msgspec.structs.replace(obs, data={**data, "moderator": moderator, "reason": match[2]})


def capture_admission(config):
    return CaptureAdmission(
        1, config["guild"], config["revision"],
        tuple(sorted(set(config["excluded"]))), bool(config["bots"]),
    )


async def publish_capture_admission(redis, config):
    await publish_admission(redis, CAPTURE_OWNER, capture_admission(config))


async def rehydrate_capture_admission(redis, configs):
    await refresh_publication(
        redis, CAPTURE_OWNER,
        (capture_admission(config) for config in configs if config["revision"] > 0),
    )


class CaptureReceiver:
    def __init__(self, redis, archive, block_reader, *, owner_id, mutation_reader=None):
        self.relay = CaptureRelay(redis, *CAPTURE_OWNER)
        self.coverage = CaptureRelay(redis, *CAPTURE_OWNER, coverage=True)
        self.archive, self.block_reader, self.owner_id = archive, block_reader, owner_id
        self.mutation_reader = mutation_reader

    async def receive(self, enabled, now):
        await self.receive_coverage(enabled, now)
        frame, settled = await self.relay.read()
        if frame is None or settled:
            return
        batch = decode_batch(frame)
        entries, identities, subjects = [], {}, set()
        for obs in batch.records:
            if (
                obs.guild not in enabled
                or obs.occurred + TTL <= now
                or skip_webhook_observation(obs.kind, obs.data, batch.client_id)
            ):
                continue
            if obs.kind == MUTATION_ATTEMPT:
                if self.mutation_reader is None:
                    continue
                try:
                    mutation = observed_attempt(obs)
                except (ValueError, TypeError):
                    await self.archive.gap(obs.guild, now, REASONS["capture_error"])
                    continue
                participants = {mutation.attempt.actor}
                if isinstance(mutation.attempt, EditNickname):
                    participants.add(mutation.attempt.member)
                subjects.update(participants - {self.owner_id})
                entries.append((obs, participants, mutation))
                continue
            if obs.kind not in GUILD_CAPTURE_EVENTS:
                continue
            obs = attribute_moderation_actor(obs, batch.client_id)
            key = obs.guild, ":".join(map(str, snapshot_key(obs)))
            subject = int(obs.data.get("subject") or 0)
            if not subject and key not in identities:
                identities[key] = int((await self.archive.snapshot(*key) or {}).get("subject") or 0)
            subject = subject or identities.get(key, 0)
            if subject:
                identities[key] = subject
            participants = {subject}
            if obs.kind == "GUILD_AUDIT_LOG_ENTRY_CREATE":
                participants.update(int(obs.data.get(field) or 0) for field in (
                    "user_id", "moderator", "target_id",
                ))
            subjects.update(participants - {0, self.owner_id})
            entries.append((obs, participants, None))

        blocked = set()
        subjects = sorted(subjects)
        for start in range(0, len(subjects), MAX_SUBJECTS):
            batch = set(subjects[start:start + MAX_SUBJECTS])
            blocked.update(await self.block_reader.blocked(batch))
        admitted = [
            (obs, mutation) for obs, people, mutation in entries if people.isdisjoint(blocked)
        ]
        attempts = tuple(mutation.attempt for _, mutation in admitted if mutation is not None)
        tracked = await self.mutation_reader.tracked_mutations(attempts) if attempts else set()
        for obs, mutation in admitted:
            if mutation is None:
                await self.archive.ingest(obs, captured=True)
            elif scope(mutation.attempt) in tracked:
                await self.archive.record_suppressions(
                    obs, suppressions(obs, mutation.attempt, mutation.application_id),
                )

        await self.relay.acknowledge(frame)

    async def receive_coverage(self, enabled, now):
        frame, settled = await self.coverage.read()
        if frame is None or settled:
            return
        latest = {}
        for signal in decode_coverage(frame).signals:
            if signal.guild in enabled and signal.occurred + TTL > now:
                before = latest.get(signal.guild)
                if before is None or signal.occurred >= before.occurred:
                    latest[signal.guild] = signal
        for signal in latest.values():
            await self.archive.gap(signal.guild, signal.occurred, REASONS[signal.kind])
        await self.coverage.acknowledge(frame)
