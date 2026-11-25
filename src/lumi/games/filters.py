from __future__ import annotations

import asyncio
import hashlib
import io
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from PIL import Image
from scipy.ndimage import map_coordinates
from scipy.spatial import cKDTree

from lumi.karuta_collections.artifacts import ArtifactStore

VERSION = "kguess-3-lossy-webp-q85-m4"


def _image(array):
    return Image.fromarray(np.clip(np.rint(array), 0, 255).astype(np.uint8))


def _cell_means(array, labels):
    labels = labels.ravel()
    counts = np.bincount(labels)
    colours = np.column_stack(
        [
            np.bincount(labels, weights=array[..., channel].ravel()) / np.maximum(counts, 1)
            for channel in range(3)
        ]
    )
    return _image(colours[labels].reshape(array.shape))


def _axes(xx, yy, angle):
    x, y = xx - (xx.shape[1] - 1) / 2, yy - (yy.shape[0] - 1) / 2
    cosine, sine = np.cos(angle), np.sin(angle)
    return cosine * x + sine * y, -sine * x + cosine * y


def _adaptive(array, xx, yy, rng, stage):


    u, v = _axes(xx, yy, rng.uniform(0, np.pi))
    spans = np.array([max(float(np.ptp(u)), 1), max(float(np.ptp(v)), 1)], dtype=np.float64)
    margins = rng.uniform(0.01, 0.12, (2, 2)) * spans[:, None]
    spans += margins.sum(axis=1)
    u = ((u - u.min() + margins[0, 0]) / spans[0]).ravel()
    v = ((v - v.min() + margins[1, 0]) / spans[1]).ravel()
    minimum = int(rng.integers(26, 39)) / (2**stage)
    threshold = rng.uniform(700, 1100)
    pixels = array.reshape(-1, 3)
    result = np.empty_like(pixels)
    active = np.arange(len(pixels))
    scale = 1
    while len(active):
        labels = (u[active] * scale).astype(np.int64) + scale * (v[active] * scale).astype(np.int64)
        counts = np.bincount(labels)
        colours = pixels[active]
        means = np.column_stack(
            [
                np.bincount(labels, weights=colours[:, channel]) / np.maximum(counts, 1)
                for channel in range(3)
            ]
        )
        squares = np.bincount(labels, weights=np.einsum("ij,ij->i", colours, colours))
        variance = (squares / np.maximum(counts, 1) - np.einsum("ij,ij->i", means, means)) / 3
        split = (variance >= threshold) & (min(spans) / scale > minimum)
        continuing = split[labels]
        leaves = ~continuing
        result[active[leaves]] = means[labels[leaves]]
        active = active[continuing]
        scale *= 2
    return _image(result.reshape(array.shape))


def render(image: Image.Image, effect: str, seed: int, stage: int) -> Image.Image:
    if stage not in (0, 1, 2, 3):
        raise ValueError("invalid stage")
    image = image.convert("RGB")
    if stage == 3:
        return image.copy()
    rng = np.random.default_rng(seed)
    a = np.asarray(image, dtype=np.float64)
    h, w = a.shape[:2]
    yy, xx = np.indices((h, w), dtype=np.float64)
    if effect == "mosaic":
        width = w / int(rng.integers(9, 15))
        height = width * rng.uniform(0.8, 1.2)
        offset_x, offset_y = rng.uniform((0, 0), (width, height))
        x = np.floor((xx + offset_x) / (width / (2**stage))).astype(np.int64)
        y = np.floor((yy + offset_y) / (height / (2**stage))).astype(np.int64)
        return _cell_means(a, x + y * (int(x.max()) + 1))
    if effect == "glass":

        base = int(rng.integers(160, 241))
        sites = rng.uniform((0, 0), (w, h), (base * 9, 2))[: base * (1, 3, 9)[stage]]
        _, labels = cKDTree(sites).query(np.column_stack((xx.ravel(), yy.ravel())))
        return _cell_means(a, labels)
    if effect == "spectral":
        strength = rng.uniform(0.9, 1.1)
        phase = np.angle(np.fft.fft2(rng.normal(size=(h, w))))
        phase[0, 0] = 0
        field = np.exp(1j * phase * strength * (0.6, 0.35, 0.15)[stage])
        return _image(
            np.stack([np.fft.ifft2(np.fft.fft2(a[..., c]) * field).real for c in range(3)], -1)
        )
    if effect == "adaptive":
        return _adaptive(a, xx, yy, rng, stage)
    if effect == "polar":
        angular, radial = _axes(xx, yy, rng.uniform(0, np.pi * 2))
        angular = (angular - angular.min()) / max(float(np.ptp(angular)), 1)
        radial = (radial - radial.min()) / max(float(np.ptp(radial)), 1)
        winding = rng.choice((-1, 1))
        if rng.integers(2):
            radial = 1 - radial
        angle = winding * angular * np.pi * 2 + rng.uniform(0, np.pi * 2)
        radius = radial * min(w, h) * rng.uniform(0.48, 0.78)
        centre_x, centre_y = rng.uniform((0.3, 0.25), (0.7, 0.65)) * (w - 1, h - 1)
        sx, sy = centre_x + radius * np.cos(angle), centre_y + radius * np.sin(angle)
        amount = (1, 0.6, 0.25)[stage]
    elif effect == "ribbons":
        angle = rng.uniform(0, np.pi * 2)
        along, across = _axes(xx, yy, angle)
        strip = max(2, min(w, h) * rng.uniform(0.07, 0.18))
        indices = np.floor((across - across.min() + rng.uniform(0, strip)) / strip).astype(np.int64)
        count = int(indices.max()) + 1
        span = max(float(np.ptp(along)), 1)
        offsets = rng.uniform(-span / 3, span / 3, count)
        mirrored = rng.random(count) < rng.uniform(0.1, 0.35)
        shift = offsets[indices] - np.where(mirrored[indices], 2 * along, 0)
        sx, sy = xx + np.cos(angle) * shift, yy + np.sin(angle) * shift
        amount = (1, 0.55, 0.15)[stage]
    else:
        raise ValueError("unknown clue effect")
    sx, sy = xx + amount * (sx - xx), yy + amount * (sy - yy)
    return _image(
        np.stack(
            [map_coordinates(a[..., c], [sy, sx], order=1, mode="reflect") for c in range(3)], -1
        )
    )


class ClueRenderer:
    def __init__(self, artifacts: ArtifactStore, root: Path):
        self.artifacts, self.root = artifacts, root
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kguess-render")
        self.slots = asyncio.Semaphore(4)
        self.protected = set()
        self.prune_lock = threading.Lock()

    async def prepare(self, blob, effect, seed):

        async with self.slots:
            data = await self.artifacts.read(blob)
            return await asyncio.get_running_loop().run_in_executor(
                self.executor, self._prepare, data, blob.sha256, effect, seed
            )

    def _prepare(self, data, digest, effect, seed):
        key = hashlib.sha256(f"{VERSION}:{digest}:{effect}:{seed}".encode()).hexdigest()
        self.protected.add(key)
        directory = self.root / key
        directory.mkdir(parents=True, exist_ok=True)
        with Image.open(io.BytesIO(data)) as opened:
            image = opened.convert("RGB")
            image.thumbnail((1040, 1440))
            image.info.clear()
        for stage in range(4):
            path = directory / f"{stage}.webp"
            if not path.exists():
                result = render(image, effect, seed, stage)
                result.info.clear()
                temporary = None
                try:
                    with tempfile.NamedTemporaryFile(
                        dir=directory, suffix=".incoming", delete=False
                    ) as stream:
                        temporary = Path(stream.name)
                        result.save(stream, format="WEBP", lossless=False, quality=85, method=4)
                    temporary.replace(path)
                finally:
                    if temporary is not None:
                        temporary.unlink(missing_ok=True)
        self._prune()
        return key

    def release(self, key):
        self.protected.discard(key)

    async def cleanup(self):
        await asyncio.get_running_loop().run_in_executor(self.executor, self._prune)

    def _prune(self):
        with self.prune_lock:
            if not self.root.exists():
                return
            entries = []
            for directory in self.root.iterdir():
                if (
                    not directory.is_dir()
                    or directory.is_symlink()
                    or len(directory.name) != 64
                    or any(c not in "0123456789abcdef" for c in directory.name)
                ):
                    continue
                files = [
                    p
                    for p in directory.iterdir()
                    if p.name
                    in {
                        f"{stage}.{extension}"
                        for stage in range(4)
                        for extension in ("png", "webp")
                    }
                    and p.is_file()
                    and not p.is_symlink()
                ]
                size = sum(p.stat().st_size for p in files)
                stamp = max((p.stat().st_mtime for p in files), default=0)
                entries.append((stamp, size, directory, files))
            total = sum(r[1] for r in entries)
            for stamp, size, directory, files in sorted(entries):
                if directory.name in self.protected or time.time() - stamp < 60:
                    continue
                if total <= 512 * 1024 * 1024 and time.time() - stamp <= 86400:
                    continue

                if directory.resolve().parent != self.root.resolve():
                    continue
                for path in files:
                    path.unlink(missing_ok=True)
                try:
                    directory.rmdir()
                except OSError:
                    pass
                total -= size

    def path(self, key, stage):
        if len(key) != 64 or any(c not in "0123456789abcdef" for c in key) or stage not in range(4):
            raise ValueError("invalid clue reference")
        directory = self.root / key
        webp, legacy = directory / f"{stage}.webp", directory / f"{stage}.png"
        return legacy if not webp.is_file() and legacy.is_file() else webp

    async def close(self):
        self.executor.shutdown(wait=True, cancel_futures=True)
