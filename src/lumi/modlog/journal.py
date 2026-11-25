import asyncio

from .models import Observation, identifier

SCAN_INTERVAL_MS = 5000


class JournalReconciler:
    def __init__(self, reader, archive, *, owner_id):
        self.reader, self.archive, self.owner_id = reader, archive, owner_id
        self.next_scan = 0

    async def reconcile(self, guilds, now):
        if not guilds or now < self.next_scan:
            return
        self.next_scan = now + SCAN_INTERVAL_MS
        async with asyncio.timeout(SCAN_INTERVAL_MS / 1000):
            await self._capture(guilds, now)

    async def _capture(self, guilds, now):
        entries, subjects = [], set()
        for source, row in await self.reader.entries(now, guilds):
            if row["guild"] not in guilds:
                continue
            counts, warning = None, None
            if source == "rename":
                counts = await self.reader.rename_counts(row)
            if source == "moderation" and row["result"]["status"] == "revoked":
                subject = await self.reader.warning_subject(row["guild"], row["request"]["warning_id"])
                warning = {"user": subject}
            obs = committed(source, row, now, counts=counts, warning=warning)
            if obs is not None:
                subjects.update({obs.actor, obs.data.get("subject", 0)} - {0, self.owner_id})
            key = row["modlog_id"] + (":" + row["mode"] if source == "rename" else "")
            revision = row.get("revision", row.get("modlog_revision", row["created"]))
            entries.append((row["guild"], key, revision, obs))

        blocked = await self.reader.blocked(subjects) if subjects else frozenset()
        for guild, key, revision, obs in entries:
            if obs is not None and {obs.actor, obs.data.get("subject", 0)}.isdisjoint(blocked):


                await self.archive.ingest(obs)
            await self.archive.journal_done(guild, key, revision, now)


def committed(source, row, now, *, counts=None, warning=None):
    key = row["modlog_id"] + (":" + row["mode"] if source == "rename" else "")
    operation = identifier("operation", key)
    data = dict(id=operation, journal_operation=operation, known_actor=True, files=[])
    actor, channel, subject = row.get("actor", 0), row.get("channel", 0), 0
    if source == "moderation":
        request, result = row["request"], row["result"]
        event = {
            "warned": "warn",
            "revoked": "unwarn",
            "kicked": "kick",
            "unbanned": "unban",
            "timed_out": "timeout",
            "timeout_removed": "untimeout",
        }.get(result["status"])
        if not event or result.get("error"):
            return None
        actor, channel = request["actor"], request["channel"]
        subject = request.get("target") or (warning or {}).get("user", 0)
        detail = result.get("data", {})
        if event == "timeout" and row.get("before_until", 0) > row["created"]:
            event = "timeout_change"
        data.update(
            event=event,
            reason=request["reason"],
            warning=detail.get("warning_id"),
            name=row.get("target_name") or str(subject),
            until=row.get("until", 0),
            duration=request.get("duration", 0) * 1000,
            before=row.get("before_until", 0),
        )
    elif source == "purge":
        data.update(
            event="purge",
            name="Channel reset" if row["mode"] == "reset" else "Purge",
            counts={k: row[k] for k in ("deleted", "skipped", "unconfirmed")},
            changes={"State": [None, row["state"]]},
            body="Channel recreated." if row["mode"] == "reset" else "",
        )
    elif source == "rename":
        data.update(
            event="rename_undo" if row["mode"] == "undo" else "rename",
            name="Server nicknames",
            counts={
                k: (counts or {}).get(k, 0) for k in ("applied", "restored", "skipped", "failed")
            },
            changes={"State": [None, row["state"]]},
        )
    elif source == "imageguard":
        subject = row["user"]
        steps = row.get("steps", {})
        evidence = steps.get("evidence", {})
        data.update(
            event="imageguard",
            group=row.get("group_name") or row["group"],
            name=str(subject),
            counts={k: v["state"] for k, v in steps.items() if k != "evidence"},
        )
        if evidence.get("message") and row.get("log_channel"):
            data["incident_url"] = (
                f"https://discord.com/channels/{row['guild']}/{row['log_channel']}/{evidence['message']}"
            )

        actor = 0
    else:
        return None
    data["subject"] = subject
    version = row.get("revision", row.get("modlog_revision", row["created"]))
    return Observation(
        identifier("journal", key, version),
        row["guild"],
        "LUMI_OPERATION",
        subject,
        row.get("updated", now),
        version,
        channel,
        actor,
        data,
    )
