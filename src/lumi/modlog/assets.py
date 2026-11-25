from __future__ import annotations

import asyncio
import hashlib
import io
import os
import re
import secrets
import struct
import zipfile
from pathlib import Path

import aiohttp

from .models import MAX_FILE, TTL, attachment_url

EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".avif",
    ".mp4",
    ".mov",
    ".webm",
    ".mp3",
    ".m4a",
    ".wav",
    ".ogg",
    ".opus",
    ".flac",
    ".pdf",
    ".txt",
    ".csv",
    ".docx",
    ".xlsx",
    ".pptx",
}
IMAGE_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".avif": "image/avif",
}


def valid(data: bytes, filename: str) -> bool:
    suffix = Path(filename).suffix.lower()
    if not data or len(data) > MAX_FILE or suffix not in EXTENSIONS:
        return False
    head = data[:64]
    if suffix in {".txt", ".csv"}:
        try:
            data.decode("utf-8-sig")
            return b"\0" not in data
        except UnicodeError:
            return False
    signatures = {
        ".png": head.startswith(b"\x89PNG\r\n\x1a\n"),
        ".jpg": head.startswith(b"\xff\xd8\xff"),
        ".jpeg": head.startswith(b"\xff\xd8\xff"),
        ".gif": head.startswith((b"GIF87a", b"GIF89a")),
        ".webp": head.startswith(b"RIFF") and head[8:12] == b"WEBP",
        ".wav": head.startswith(b"RIFF") and head[8:12] == b"WAVE",
        ".avif": head[4:8] == b"ftyp" and b"avif" in head[8:],
        ".pdf": head.startswith(b"%PDF-"),
        ".webm": head.startswith(b"\x1aE\xdf\xa3"),
        ".mp3": head.startswith(b"ID3")
        or (len(head) > 1 and head[0] == 255 and head[1] & 0xE0 == 0xE0),
        ".ogg": head.startswith(b"OggS"),
        ".opus": head.startswith(b"OggS"),
        ".flac": head.startswith(b"fLaC"),
    }
    if suffix in {".mp4", ".mov", ".m4a"}:
        return head[4:8] == b"ftyp"
    if suffix in {".docx", ".xlsx", ".pptx"}:

        ending = data.rfind(b"PK\x05\x06", max(0, len(data) - 65557))
        if (
            ending < 0
            or ending + 22 > len(data)
            or struct.unpack_from("<H", data, ending + 10)[0] > 2048
        ):
            return False
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as package:
                entries = package.infolist()
                names = {f.filename.lower() for f in entries}
                required = {
                    "[content_types].xml",
                    {
                        ".docx": "word/document.xml",
                        ".xlsx": "xl/workbook.xml",
                        ".pptx": "ppt/presentation.xml",
                    }[suffix],
                }
                return required <= names and not any(
                    f.flag_bits & 1
                    or "vba" in f.filename.lower()
                    or Path(f.filename).suffix.lower()
                    in {
                        ".exe",
                        ".com",
                        ".dll",
                        ".js",
                        ".vbs",
                        ".ps1",
                        ".bat",
                        ".cmd",
                        ".sh",
                        ".py",
                        ".jar",
                    }
                    for f in entries
                )
        except (ValueError, zipfile.BadZipFile, OSError):
            return False
    return signatures.get(suffix, False)


class Assets:
    def __init__(self, root: Path, store, *, clock):
        self.root, self.store, self.clock = root, store, clock
        self.limit = asyncio.Semaphore(2)
        self.guild_locks = {}

    def path(self, guild, sha):
        if type(guild) is not int or guild <= 0 or not re.fullmatch("[a-f0-9]{64}", sha):
            raise ValueError("Invalid archive reference")
        path = self.root / str(guild) / sha
        if self.root.is_symlink() or path.parent.is_symlink() or path.is_symlink():
            raise ValueError("Invalid archive path")
        return path

    async def ingest(self, file):
        guild = file["guild"]
        async with self.limit, self.guild_locks.setdefault(guild, asyncio.Lock()):
            lease = await self.store.download_slot(guild, self.clock())
            if not lease:
                return
            try:
                await self._ingest(file)
            finally:
                await self.store.release_download(lease)

    async def _ingest(self, file):
        guild, fid = file["guild"], file["id"]
        f = await self.store.reserve(guild, fid, self.clock())
        if not f:
            return
        status = "retry"
        delay = 0
        finished = False
        try:
            if not 0 < f["size"] <= MAX_FILE:
                status = "too large"
                return
            if Path(f["name"]).suffix.lower() not in EXTENSIONS:
                status = "unsupported"
                return
            if not attachment_url(f.get("url", ""), f["channel"], fid):
                status = "unavailable"
                return
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(f["url"], allow_redirects=False) as response:
                    if response.status == 429 or response.status >= 500:
                        try:
                            delay = max(
                                0,
                                min(
                                    TTL,
                                    int(float(response.headers.get("Retry-After", 0)) * 1000),
                                ),
                            )
                        except (ValueError, OverflowError, TypeError):
                            pass
                        return
                    if response.status != 200:
                        status = "unavailable"
                        return
                    if (response.content_length or 0) > MAX_FILE:
                        status = "too large"
                        return
                    chunks = bytearray()
                    async for part in response.content.iter_chunked(65536):
                        chunks.extend(part)
                        if len(chunks) > MAX_FILE:
                            status = "too large"
                            return
            data = bytes(chunks)
            name, media_type = f["name"], f.get("type", "")
            image = Path(name).suffix.lower() in IMAGE_TYPES
            if image:


                suffix = next((ext for ext in IMAGE_TYPES if valid(data, "image" + ext)), None)
                if suffix:
                    media_type = IMAGE_TYPES[suffix]
                    if IMAGE_TYPES[Path(name).suffix.lower()] != media_type:
                        name = str(Path(name).with_suffix(suffix))
                else:
                    status = "invalid"
                    return
            if not valid(data, name) or (not image and len(data) != f["size"]):
                status = "invalid"
                return
            sha = hashlib.sha256(data).hexdigest()
            await self.store.finish_file(
                guild,
                fid,
                status="saved",
                sha=sha,
                size=len(data),
                media_type=media_type,
                name=name,
                attempt=f["attempts"],
                now=self.clock(),
                save=lambda: asyncio.to_thread(self.save, guild, sha, data),
            )
            finished = True
        except (OSError, ValueError, aiohttp.ClientError, TimeoutError):
            pass
        finally:
            if not finished:
                await self.store.finish_file(
                    guild, fid, status=status, now=self.clock(), delay=delay, attempt=f["attempts"]
                )

    def save(self, guild, sha, data):
        target = self.path(guild, sha)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
        if target.exists():
            if hashlib.sha256(target.read_bytes()).hexdigest() != sha:
                raise ValueError("Archive checksum mismatch")
            return
        temporary = target.parent / ("." + secrets.token_hex(16))
        try:
            with temporary.open("xb") as output:
                os.chmod(temporary, 0o640)
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def read(self, guild, file):
        data = self.path(guild, file["sha"]).read_bytes()
        if (
            len(data) > MAX_FILE
            or len(data) != file["size"]
            or hashlib.sha256(data).hexdigest() != file["sha"]
        ):
            raise ValueError("Archive unavailable")
        return data

    async def cleanup(self, references):
        for guild, sha in references:
            async with self.store.transaction(guild) as tx:
                blob = await tx.get("blob", sha)
                if blob and blob["expires"] <= self.clock():
                    await asyncio.to_thread(self.path(guild, sha).unlink, missing_ok=True)
                    await tx.remove("blob", sha)
