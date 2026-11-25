from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
import json
import os
import secrets
import sys
import time
import warnings
from importlib import import_module
from pathlib import Path

import aiohttp
from PIL import Image, ImageOps

from .models import MAX_BYTES, MAX_PIXELS, GuardError
from .urls import canonical

PROCESS_TIMEOUT = 2
STARTUP_TIMEOUT = 10
WORKER_READY = b"LUMI_IMAGEGUARD_READY\n"


def process(data):

    from ._pdq.hasher.pdq_hasher import PDQHasher

    if not data or len(data) > MAX_BYTES:
        raise GuardError("Image exceeds 10 MiB.")
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(io.BytesIO(data)) as image:
            if image.format not in {"PNG", "JPEG", "WEBP", "GIF"}:
                raise GuardError("Use PNG, JPEG, WebP or GIF.")
            if image.width * image.height > MAX_PIXELS or max(image.size) > 8192:
                raise GuardError("Image dimensions exceeded.")
            image.seek(0)
            image.load()
            frame = ImageOps.exif_transpose(image).convert("RGBA")
            rgb = Image.new("RGB", frame.size, "white")
            rgb.paste(frame, mask=frame.getchannel("A"))

    rgb.thumbnail((512, 512))
    encoded = io.BytesIO()
    rgb.save(encoded, "PNG")
    hashed = PDQHasher().fromBufferedImage(io.BytesIO(encoded.getvalue()))
    return dict(
        sha256=hashlib.sha256(data).hexdigest(),
        pdq=str(hashed.hash),
        quality=hashed.quality,
        size=len(data),
    ), encoded.getvalue()


class ImagePipeline:
    def __init__(self, root, temp_root):
        self.root, self.temp = Path(root).resolve(), Path(temp_root).resolve()
        self.session = None
        self.children = set()
        self.active = 0
        self.guilds = set()

    def enter(self, guild):
        if self.active >= 2 or guild in self.guilds:
            return False
        self.active += 1
        self.guilds.add(guild)
        return True

    def leave(self, guild):
        self.active -= 1
        self.guilds.discard(guild)

    async def fetch(self, url):
        if not canonical(url):
            raise GuardError("Use an image uploaded to Discord.")
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
                trust_env=False,
                cookie_jar=aiohttp.DummyCookieJar(),
                connector=aiohttp.TCPConnector(limit=2),
            )
        try:
            async with self.session.get(url, allow_redirects=False) as response:
                if response.status != 200 or (response.content_length or 0) > MAX_BYTES:
                    raise GuardError("Image unavailable or too large.")
                data = bytearray()
                async for part in response.content.iter_chunked(65536):
                    data.extend(part)
                    if len(data) > MAX_BYTES:
                        raise GuardError("Image exceeds 10 MiB.")
                return bytes(data)
        except (aiohttp.ClientError, TimeoutError):
            raise GuardError("Image unavailable.") from None

    async def analyse(self, data):
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "lumi.imageguard.images",
            "--worker",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        self.children.add(child)
        try:


            try:
                async with asyncio.timeout(STARTUP_TIMEOUT):
                    ready = await child.stdout.readexactly(len(WORKER_READY))
            except TimeoutError:
                raise GuardError("Image processor unavailable.") from None
            if ready != WORKER_READY:
                raise GuardError("Image processor unavailable.")
            async with asyncio.timeout(PROCESS_TIMEOUT):
                output, _ = await child.communicate(data)
            if child.returncode or len(output) > 4 * MAX_BYTES:
                raise GuardError("Image could not be processed.")
            metadata, png = output.split(b"\n", 1)
            return json.loads(metadata), png
        except TimeoutError:
            raise GuardError("Image processing timed out.") from None
        except asyncio.IncompleteReadError:
            raise GuardError("Image processor unavailable.") from None
        finally:
            if child.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    child.kill()
                await child.wait()
            self.children.discard(child)

    def path(self, sha):
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise ValueError("invalid reference")
        path = self.root / sha[:2] / (sha + ".image")
        if self.root not in path.resolve().parents or path.is_symlink():
            raise ValueError("unsafe reference")
        return path

    async def save_reference(self, meta, data):
        def save():
            destination = self.path(meta["sha256"])
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if hashlib.sha256(destination.read_bytes()).hexdigest() != meta["sha256"]:
                    raise GuardError("Stored image is damaged.")
                return
            temporary = destination.with_name(".new-" + secrets.token_hex(16))
            try:
                with temporary.open("xb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)

        await asyncio.to_thread(save)

    async def attachment(self, png):
        token = secrets.token_hex(16)
        self.temp.mkdir(parents=True, exist_ok=True)
        if getattr(self, "cache", None) is not None:
            await self.cache.sadd("lumi:imageguard:ui:temporary", token)
        await asyncio.to_thread((self.temp / token).write_bytes, png)
        return dict(
            reference="tmp:" + token,
            file_name="SPOILER_imageguard.png",
            sha256=hashlib.sha256(png).hexdigest(),
            size=len(png),
            expires_at_ms=int(time.time() * 1000) + 300_000,
        )

    async def prune(self, referenced):
        def prune():
            cutoff = time.time() - 3600
            for path in self.root.glob("[0-9a-f][0-9a-f]/*.image"):
                if (
                    path.stem not in referenced
                    and not path.is_symlink()
                    and path.stat().st_mtime < cutoff
                ):
                    path.unlink()

        await asyncio.to_thread(prune)

    async def close(self):
        for child in tuple(self.children):
            with contextlib.suppress(ProcessLookupError):
                child.kill()
            await child.wait()
        if self.session:
            await self.session.close()


def main():
    if os.name == "posix":
        import resource

        resource.setrlimit(resource.RLIMIT_CPU, (2, 2))
        resource.setrlimit(resource.RLIMIT_AS, (512 * 1024**2, 512 * 1024**2))
        os.nice(10)
    try:
        if sys.argv[1:] == ["--worker"]:
            for name in (
                "PIL.PngImagePlugin",
                "PIL.JpegImagePlugin",
                "PIL.WebPImagePlugin",
                "PIL.GifImagePlugin",
                "lumi.imageguard._pdq.hasher.pdq_hasher",
            ):
                import_module(name)
            sys.stdout.buffer.write(WORKER_READY)
            sys.stdout.buffer.flush()
        meta, png = process(sys.stdin.buffer.read(MAX_BYTES + 1))
        sys.stdout.buffer.write(json.dumps(meta).encode() + b"\n" + png)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
