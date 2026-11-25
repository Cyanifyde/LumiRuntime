from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path

MAX_RECORD = 128 * 1024
TARGET_CHUNK = 256 * 1024
MAX_CHUNK = 512 * 1024
MAX_SEGMENT = 16 * 1024 * 1024
SEGMENT = re.compile(r"[a-f0-9]{32}\.lz4c\Z")


class ArchiveUnavailable(RuntimeError):
    pass


class ArchiveFull(ArchiveUnavailable):
    pass


class ArchiveFiles:
    def __init__(self, root: Path):
        self.root = root

    def path(self, name):
        if not isinstance(name, str) or not SEGMENT.fullmatch(name):
            raise ArchiveUnavailable("Invalid archive segment")
        return self.root / name

    def append(self, name, records):
        import lz4.frame

        if not isinstance(records, list) or not records:
            raise ArchiveUnavailable("Archive chunk must contain records")
        payload = json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode()
        if len(payload) > MAX_CHUNK:
            raise ArchiveUnavailable("Archive chunk exceeds its bound")
        frame = lz4.frame.compress(
            payload,
            compression_level=lz4.frame.COMPRESSIONLEVEL_MAX,
            block_size=lz4.frame.BLOCKSIZE_MAX256KB,
            block_linked=True,
            content_checksum=True,
            block_checksum=True,
            store_size=True,
        )
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.root.is_symlink():
            raise ArchiveUnavailable("Archive root must not be a symbolic link")
        target = self.path(name)
        fd = os.open(
            target,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_APPEND
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_BINARY", 0),
            0o600,
        )
        try:
            details = os.fstat(fd)
            if not stat.S_ISREG(details.st_mode):
                raise ArchiveUnavailable("Archive segment must be a regular file")
            offset = details.st_size
            if offset + len(frame) > MAX_SEGMENT:
                raise ArchiveFull("Archive segment is full")
            pending = memoryview(frame)
            while pending:
                written = os.write(fd, pending)
                if written <= 0:
                    raise OSError("Archive write did not advance")
                pending = pending[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        if os.name == "posix":
            directory = os.open(self.root, os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return offset, len(frame), len(payload), hashlib.sha256(payload).hexdigest()

    def read(self, name, offset, size, raw_size, digest):
        import lz4.frame

        if (
            type(offset) is not int
            or type(size) is not int
            or type(raw_size) is not int
            or offset < 0
            or not 0 < size <= MAX_CHUNK + 4096
            or not 0 < raw_size <= MAX_CHUNK
            or offset + size > MAX_SEGMENT
        ):
            raise ArchiveUnavailable("Invalid archive index")
        fd = os.open(
            self.path(name),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0),
        )
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ArchiveUnavailable("Archive segment must be a regular file")

            os.lseek(fd, offset, os.SEEK_SET)
            chunks, remaining = [], size
            while remaining:
                block = os.read(fd, remaining)
                if not block:
                    raise ArchiveUnavailable("Archive frame is truncated")
                chunks.append(block)
                remaining -= len(block)
        finally:
            os.close(fd)
        frame = b"".join(chunks)
        info = lz4.frame.get_frame_info(frame)
        if info["content_size"] != raw_size:
            raise ArchiveUnavailable("Archive size does not match its index")
        payload, consumed = lz4.frame.decompress(frame, return_bytes_read=True)
        if (
            consumed != size
            or len(payload) != raw_size
            or hashlib.sha256(payload).hexdigest() != digest
        ):
            raise ArchiveUnavailable("Archive checksum does not match its index")
        return json.loads(payload)

    def remove(self, name):
        try:
            self.path(name).unlink()
        except FileNotFoundError:
            pass
