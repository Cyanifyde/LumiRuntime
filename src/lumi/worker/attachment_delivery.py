from __future__ import annotations

import asyncio
from contextvars import ContextVar
from pathlib import Path

from lumi.shared.attachment_payloads import (
    BYTE_LIMIT,
    AttachmentUnavailable,
    address,
    file_job,
    references,
)
from lumi.shared.job_files import job_file_name
from lumi.shared.local_upload_file import read_temporary

ACTIVE_DELIVERY: ContextVar[AttachmentDelivery | None] = ContextVar(
    "lumi_worker_attachment_delivery",
    default=None,
)


class AttachmentDelivery:
    def __init__(self, payloads, group: str, root: Path):
        self.payloads = payloads
        self.group = group
        self.root = root


        self._uploads = asyncio.Semaphore(2)

    def _load(self, job_id, intent, refs):
        remaining = BYTE_LIMIT
        contents = []
        for index, ref in enumerate(refs):
            name = job_file_name(ref, file_job(job_id)) if ref.startswith("job-file:") else ref[4:]
            maximum = (
                min(remaining, intent.attachments[index].size) if intent.attachments else remaining
            )
            if maximum <= 0:
                raise AttachmentUnavailable("Invalid upload size.")
            data = read_temporary(self.root / name, maximum=maximum)
            remaining -= len(data)
            contents.append(data)
        return tuple(contents)

    async def stage(self, job_id, intent, *, deadline):
        refs = references(intent)
        if not refs:
            return
        address(self.group, job_id, refs)
        async with self._uploads:
            try:
                contents = await asyncio.to_thread(self._load, job_id, intent, refs)
            except (OSError, ValueError):
                raise AttachmentUnavailable("Worker upload unavailable.") from None
            await self.payloads.publish(self.group, job_id, intent, contents, deadline=deadline)
