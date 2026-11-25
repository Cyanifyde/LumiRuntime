from __future__ import annotations

import time

from lumi.contracts import TemporaryAttachment
from lumi.moderation.operation_tags import tag_from_policy

from .models import GuardError, choose, incident_id
from .urls import observed_sources, signature


class Engine:
    def __init__(self, store, pipeline, enforcement, clock=None):
        self.store, self.pipeline, self.enforcement = store, pipeline, enforcement
        self.clock = clock or (lambda: int(time.time() * 1000))

    async def observe(self, event, job_id, arguments, *, policy=None):
        guild, channel, message, user = (
            int(event.get(k) or 0) for k in ("guild_id", "channel_id", "message_id", "actor_id")
        )
        candidates = observed_sources(arguments)
        if (
            min(guild, channel, message, user) <= 0
            or event.get("dispatch") != "passive"
            or event.get("administrator")
            or event.get("author_bot")
            or event.get("webhook_id")
            or not candidates
        ):
            return
        config = await self.store.snapshot(guild)
        if not config["log_channel"] or not any(g["enabled"] for g in config["groups"]):
            return
        if not self.pipeline.enter(guild):
            await self.store.metric(guild, "skipped", "busy")
            return
        try:
            observed, previews = [], []
            for candidate in candidates:
                if event.get("_expires_at_ms", self.clock() + 1) <= self.clock():
                    await self.store.metric(guild, "skipped", "expired")
                    return
                data = await self.pipeline.fetch(candidate["url"])
                meta, png = await self.pipeline.analyse(data)
                observed.append(meta)
                previews.append(png)
            await self.store.metric(guild, "scanned", "")
            matched = choose(config, observed)
            if not matched:
                return
            source_signature = signature(candidates)
            revision = int(event.get("source_revision_ms") or event.get("created_at_ms") or 0)
            if revision <= 0:
                return
            identifier = incident_id(guild, message, revision, source_signature)
            existing = await self.store.incident(identifier)
            if existing and existing.get("status") == "done":
                return
            record = dict(
                id=identifier,
                guild=guild,
                channel=channel,
                message=message,
                user=user,
                created=self.clock(),
                source_revision=revision,
                signature=source_signature,
                config_revision=config["revision"],
                log_channel=config["log_channel"],
                group=matched["group"]["id"],
                group_name=matched["group"]["name"],
                action=matched["group"]["action"],
                reference=matched["reference"],
                match=matched["match"],
                distance=matched["distance"],
                status="pending",
                steps={},
            )
            if policy is not None:
                tag_from_policy(policy, record, source_key="imageguard:" + identifier)
            attachment = record["attachment"] = await self.pipeline.attachment(previews[matched["index"]])
            saved = await self.store.open_incident(record)
            if saved["attachment"] != attachment:
                self.unlink(attachment)
            try:
                outcome = await self.enforcement.run(saved, event, job_id, arguments)
                if not outcome.ok:
                    await self.store.metric(
                        guild, "errors", outcome.error_code or "enforcement_failed"
                    )
            except (TimeoutError, OSError):

                await self.store.metric(guild, "errors", "delivery_uncertain")
        except (TimeoutError, GuardError):
            await self.store.metric(guild, "errors", "image_unavailable")
        finally:
            self.pipeline.leave(guild)

    def unlink(self, attachment):
        item = TemporaryAttachment(**attachment)
        path = self.pipeline.temp / item.reference.removeprefix("tmp:")
        path.unlink(missing_ok=True)
