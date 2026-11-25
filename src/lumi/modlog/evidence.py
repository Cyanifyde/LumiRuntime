import asyncio
import secrets
import stat
from contextlib import suppress
from pathlib import Path

import msgspec

from lumi.contracts import TemporaryAttachment
from lumi.shared.job_files import JOB_FILE_NAME, job_file_name

from .models import public_record
from .presentation import detail_fields
from .journal_store import MAX_SUBJECTS

FILE_PAGE = 3
DETAIL_PAGE = 4
PAYLOAD_LIMIT = 25 * 1024**2
COPY_LIMIT = 64
SPOOL_LIMIT = 128 * 1024**2


def participants(row):
    return {value for key in ("subject", "actor") if (value := row.get(key, 0))}


class Evidence:
    def __init__(self, store, assets, temp_root, block_reader, *, owner_id, clock):
        self.store, self.assets = store, assets
        self.temp_root = Path(temp_root) if temp_root is not None else None
        self.block_reader, self.owner_id, self.clock = block_reader, owner_id, clock
        self.lock = asyncio.Lock()

    async def read(self, request, session, context, *, witness=None):
        target, nsfw = context.get("history_destination"), context.get("history_nsfw")
        if type(target) is not int or target <= 0 or type(nsfw) is not bool or self.block_reader is None:
            raise ValueError("Evidence access unavailable.")
        posted_from = None
        if request.handler == "modlog.record":
            source = request.event.get("message_id")
            if not source or request.arguments.get("record") != session["record"]:
                raise ValueError("Evidence source unavailable.")
            posted_from = (source, session["channel"])
        row, snapshots = await self.store.public_evidence(
            session["guild"], session["record"], self.clock(), destination=target,
            revision=context["config"]["revision"], allow_nsfw=nsfw, posted_from=posted_from,
            witness=witness,
        )
        subjects = participants(row)
        observed = sorted(subjects.union(*(participants(snapshot) for snapshot in snapshots)) - {self.owner_id})
        blocked = set()
        for start in range(0, len(observed), MAX_SUBJECTS):
            blocked.update(await self.block_reader.blocked(observed[start : start + MAX_SUBJECTS]))
        if not subjects.isdisjoint(blocked):
            raise ValueError("Evidence access unavailable.")
        snapshots = [s for s in snapshots if participants(s).isdisjoint(blocked)]
        subjects.update(*(participants(snapshot) for snapshot in snapshots))
        if row["kind"] == "bulk_delete":
            row = {
                **row,
                "files": [file for snapshot in snapshots for file in snapshot.get("files", ())],
                "nsfw": bool(row.get("nsfw") or any(snapshot.get("nsfw") for snapshot in snapshots)),
            }
        return row, snapshots, subjects

    @staticmethod
    def detail(row, snapshots, page):
        result = public_record(row)
        if row["kind"] == "bulk_delete":
            body = "\n\n".join(
                str(message.get("name", "Unknown")) + "\n" + str(message.get("content", "Unavailable"))
                for message in snapshots
            )
            fields = detail_fields({"body": body, "kind": "bulk_delete", "category": "messages"})
        else:
            fields = detail_fields(row)
        for key in ("before", "after", "body", "reason", "changes", "added", "removed"):
            result.pop(key, None)
        result["detail_fields"] = msgspec.to_builtins(
            tuple(fields[page * DETAIL_PAGE : (page + 1) * DETAIL_PAGE])
        )
        more = (
            len(fields) > (page + 1) * DETAIL_PAGE
            or len(row.get("files", ())) > (page + 1) * FILE_PAGE
        )
        return result, more

    def spool_usage(self):
        count = used = 0
        for path in self.temp_root.iterdir():
            if not JOB_FILE_NAME.fullmatch(path.name):
                continue
            with suppress(FileNotFoundError):
                entry = path.lstat()
                if stat.S_ISREG(entry.st_mode):
                    count += 1
                    used += entry.st_size
        return count, used

    @staticmethod
    def save_file(path, payload):

        output = path.open("xb")
        try:
            with output:
                path.chmod(0o640)
                output.write(payload)
        except BaseException:
            with suppress(OSError):
                path.unlink()
            raise

    async def files(self, request, job, session, row):
        start = session["detail_page"] * FILE_PAGE
        staged = await self.stage_files(
            job, session["guild"], row.get("files", ())[start : start + FILE_PAGE],
            min(row["expires"], request.event.get("_expires_at_ms", 0)),
        )
        return tuple(item for _, item in staged)

    def discard(self, job, attachments):
        for item in attachments:
            with suppress(OSError):
                (self.temp_root / job_file_name(item.reference, job)).unlink(missing_ok=True)

    async def stage_files(self, job, guild, sources, deadline, *, optional=False):
        if not sources:
            return ()
        attachments, paths = [], []
        try:
            async with self.lock:
                if self.temp_root is None or self.temp_root.is_symlink() or not self.temp_root.is_dir():
                    raise ValueError("Evidence delivery unavailable.")
                count, used = await asyncio.to_thread(self.spool_usage)
                total = 0
                for file in sources:
                    now = self.clock()
                    saved = await self.store.file(guild, file["id"])
                    if not saved or saved.get("status") != "saved" or saved["expires"] <= now:
                        continue
                    try:
                        payload = await asyncio.to_thread(self.assets.read, guild, saved)
                    except (ValueError, OSError):
                        if optional:
                            continue
                        raise
                    if total + len(payload) > PAYLOAD_LIMIT:
                        break
                    if count >= COPY_LIMIT or used + len(payload) > SPOOL_LIMIT:
                        raise ValueError("Evidence delivery queue full.")
                    reference = "job-file:" + secrets.token_hex(16)
                    path = self.temp_root / job_file_name(reference, job)
                    expires = min(deadline, saved["expires"], now + 120000)
                    if expires <= now:
                        raise ValueError("Evidence request expired.")
                    write = asyncio.create_task(asyncio.to_thread(self.save_file, path, payload))
                    try:
                        await asyncio.shield(write)
                    except asyncio.CancelledError:

                        await write
                        paths.append(path)
                        raise
                    paths.append(path)
                    filename = f"SPOILER_modlog-{saved['id']}" + Path(saved["name"]).suffix.lower()
                    attachments.append((
                        str(saved["id"]),
                        TemporaryAttachment(reference, filename, saved["sha"], len(payload), expires),
                    ))
                    count, used, total = count + 1, used + len(payload), total + len(payload)
            return tuple(attachments)
        except BaseException:
            for path in paths:
                with suppress(OSError):
                    path.unlink(missing_ok=True)
            raise
