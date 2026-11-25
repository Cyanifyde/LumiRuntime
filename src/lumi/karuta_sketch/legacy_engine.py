from __future__ import annotations

import hashlib
import io
import math
import threading
from collections import OrderedDict
from dataclasses import dataclass

import numpy as np
from PIL import Image
from scipy import ndimage as ndi
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import maximum_flow
from skimage.color import deltaE_ciede2000, rgb2lab
from skimage.segmentation import slic

from .image import PreparedImage
from .palette import PALETTE, PALETTE_VERSION, palette_lab, palette_rgb

RENDERER_VERSION = "ksketch-boundary-opt-v1"


@dataclass(frozen=True, slots=True)
class SketchSettings:
    detail_scale: int
    edge_protection: int
    minimum_region: int
    colour_limit: int
    preset: str = "Custom"

    def validate(self) -> None:
        if not 1 <= self.detail_scale <= 16:
            raise ValueError("detail scale")
        if not 0 <= self.edge_protection <= 100:
            raise ValueError("edge protection")
        if not 0 <= self.minimum_region <= 1000:
            raise ValueError("minimum region")
        if not 2 <= self.colour_limit <= 64:
            raise ValueError("colour limit")


PRESETS = {
    "Detailed": SketchSettings(2, 85, 16, 64, "Detailed"),
    "Balanced": SketchSettings(4, 80, 40, 48, "Balanced"),
    "Clean": SketchSettings(7, 75, 80, 32, "Clean"),
}


@dataclass(frozen=True, slots=True)
class SketchResult:
    labels: np.ndarray
    alpha: np.ndarray
    counts: np.ndarray
    settings: SketchSettings
    source_hash: str

    @property
    def used_labels(self) -> tuple[int, ...]:
        indices = np.flatnonzero(self.counts)
        return tuple(
            int(index)
            for index in sorted(indices, key=lambda value: (-float(self.counts[value]), int(value)))
        )

    @property
    def visible_pixels(self) -> float:
        return float(np.sum(self.alpha, dtype=np.float64) / 255.0)


class SketchEngine:


    def __init__(self, *, cache_size: int = 8) -> None:
        self._rgb = palette_rgb()
        self._lab = palette_lab()
        self._cache_size = cache_size
        self._cache: OrderedDict[str, SketchResult] = OrderedDict()
        self._analysis_cache: OrderedDict[
            str,
            tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray],
        ] = OrderedDict()
        self._render_cache: OrderedDict[str, bytes] = OrderedDict()
        self._render_lock = threading.Lock()

    def optimize(self, source: PreparedImage, settings: SketchSettings) -> SketchResult:
        settings.validate()
        key = self.cache_key(source.sha256, settings)
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached
        rgba = source.rgba
        visible = rgba[..., 3] > 0
        alpha = rgba[..., 3].astype(np.uint8)
        rgb = rgba[..., :3].astype(np.float32) / 255.0
        lab = rgb2lab(rgb).astype(np.float32)
        analysis_key = hashlib.sha256(
            (
                f"{source.sha256}:{settings.detail_scale}:{settings.edge_protection}:"
                f"{PALETTE_VERSION}:{RENDERER_VERSION}"
            ).encode()
        ).hexdigest()
        cached_analysis = self._analysis_cache.get(analysis_key)
        if cached_analysis is None:
            edge = self._structural_edges(lab, visible, settings)
            regions = self._atomic_regions(lab, edge, visible, settings)
            region_count = int(regions.max()) + 1
            unary = self._region_costs(lab, alpha, regions, region_count)
            pairs, pair_weights = self._region_graph(regions, edge, visible, settings)
            self._analysis_cache[analysis_key] = (edge, regions, unary, pairs, pair_weights)
            self._analysis_cache.move_to_end(analysis_key)
            while len(self._analysis_cache) > 2:
                self._analysis_cache.popitem(last=False)
        else:
            edge, regions, unary, pairs, pair_weights = cached_analysis
            self._analysis_cache.move_to_end(analysis_key)
        labels = np.argmin(unary, axis=1).astype(np.int16)
        active = self._bounded_palette(unary, pairs, pair_weights, labels, settings)
        labels = self._alpha_expansion(unary, pairs, pair_weights, labels, active)
        pixel_labels = np.full(visible.shape, -1, dtype=np.int16)
        pixel_labels[visible] = labels[regions[visible]]
        pixel_labels = self._refine_boundaries(lab, visible, pixel_labels, edge, settings, active)
        pixel_labels = self._repair_small_components(lab, visible, pixel_labels, edge, settings)
        if np.any(pixel_labels[visible] < 0) or np.any(pixel_labels[visible] >= len(PALETTE)):
            raise RuntimeError("optimizer left visible pixels unassigned")
        counts = np.bincount(
            pixel_labels[visible],
            weights=alpha[visible].astype(np.float64) / 255.0,
            minlength=len(PALETTE),
        ).astype(np.float64)
        result = SketchResult(
            labels=pixel_labels,
            alpha=alpha.copy(),
            counts=counts,
            settings=settings,
            source_hash=source.sha256,
        )
        self._cache[key] = result
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return result

    @staticmethod
    def cache_key(source_hash: str, settings: SketchSettings) -> str:
        value = (
            f"{source_hash}:{settings.detail_scale}:{settings.edge_protection}:"
            f"{settings.minimum_region}:{settings.colour_limit}:"
            f"{PALETTE_VERSION}:{RENDERER_VERSION}"
        )
        return hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _structural_edges(
        lab: np.ndarray,
        visible: np.ndarray,
        settings: SketchSettings,
    ) -> np.ndarray:
        base = max(0.55, settings.detail_scale / 3.5)
        responses: list[np.ndarray] = []
        for sigma in (base, base * 1.8, base * 3.0):
            squared = np.zeros(visible.shape, dtype=np.float32)
            for channel in range(3):
                smooth = ndi.gaussian_filter(lab[..., channel], sigma=sigma, mode="nearest")
                gx = ndi.sobel(smooth, axis=1, mode="nearest")
                gy = ndi.sobel(smooth, axis=0, mode="nearest")
                squared += gx * gx + gy * gy
            response = np.sqrt(squared, dtype=np.float32) * math.sqrt(sigma)
            values = response[visible]
            scale = float(np.percentile(values, 97.5)) if values.size else 1.0
            responses.append(np.clip(response / max(scale, 1e-6), 0.0, 1.0))
        stack = np.stack(responses)
        persistence = np.quantile(stack, 0.35, axis=0).astype(np.float32)
        persistence *= visible
        return persistence

    @staticmethod
    def _atomic_regions(
        lab: np.ndarray,
        edge: np.ndarray,
        visible: np.ndarray,
        settings: SketchSettings,
    ) -> np.ndarray:
        height, width = visible.shape
        target_area = max(9.0, (settings.detail_scale * 3.0) ** 2)
        segments = int(np.count_nonzero(visible) / target_area)
        segments = max(32, min(4096, segments))
        sigma = max(0.45, settings.detail_scale / 4.0)
        simplified = ndi.gaussian_filter(lab, sigma=(sigma, sigma, 0), mode="nearest")
        features = np.concatenate(
            (
                (simplified / np.asarray([100.0, 128.0, 128.0], dtype=np.float32)),
                edge[..., None] * 1.6,
            ),
            axis=2,
        )
        raw = slic(
            features,
            n_segments=segments,
            compactness=7.5,
            sigma=0,
            start_label=0,
            mask=visible,
            enforce_connectivity=True,
            slic_zero=True,
            max_num_iter=5,
            convert2lab=False,
            channel_axis=-1,
        ).astype(np.int32)
        threshold = 0.72 - settings.edge_protection * 0.0035
        protected = (edge >= threshold) & visible
        open_pixels = visible & ~protected
        components, _ = ndi.label(open_pixels, structure=np.ones((3, 3), dtype=np.uint8))
        if np.any(protected) and np.any(open_pixels):
            nearest = ndi.distance_transform_edt(
                protected, return_distances=False, return_indices=True
            )
            components[protected] = components[tuple(nearest[:, protected])]
        keys = raw.astype(np.int64) * (int(components.max()) + 1) + components
        _, inverse = np.unique(keys[visible], return_inverse=True)
        regions = np.full((height, width), -1, dtype=np.int32)
        regions[visible] = inverse.astype(np.int32)
        return regions

    def _region_costs(
        self,
        lab: np.ndarray,
        alpha: np.ndarray,
        regions: np.ndarray,
        region_count: int,
    ) -> np.ndarray:
        flat_regions = regions.ravel()
        visible_positions = np.flatnonzero(flat_regions >= 0)
        result = np.zeros((region_count, len(PALETTE)), dtype=np.float64)
        flat_lab = lab.reshape(-1, 3)
        flat_alpha = alpha.ravel().astype(np.float32) / 255.0
        chunk = 32_768
        for start in range(0, len(visible_positions), chunk):
            positions = visible_positions[start : start + chunk]
            pixels = flat_lab[positions, None, :]
            distances = deltaE_ciede2000(pixels, self._lab[None, :, :]).astype(np.float32)
            distances *= flat_alpha[positions, None]
            region_ids = flat_regions[positions]
            for colour in range(len(PALETTE)):
                result[:, colour] += np.bincount(
                    region_ids, weights=distances[:, colour], minlength=region_count
                )
        return result

    @staticmethod
    def _region_graph(
        regions: np.ndarray,
        edge: np.ndarray,
        visible: np.ndarray,
        settings: SketchSettings,
    ) -> tuple[np.ndarray, np.ndarray]:
        pairs: list[np.ndarray] = []
        strengths: list[np.ndarray] = []
        for left, right, edge_left, edge_right, mask in (
            (
                regions[:, :-1],
                regions[:, 1:],
                edge[:, :-1],
                edge[:, 1:],
                visible[:, :-1] & visible[:, 1:],
            ),
            (
                regions[:-1, :],
                regions[1:, :],
                edge[:-1, :],
                edge[1:, :],
                visible[:-1, :] & visible[1:, :],
            ),
        ):
            differing = mask & (left != right)
            if not np.any(differing):
                continue
            pairs.append(np.stack((left[differing], right[differing]), axis=1))
            strengths.append(np.maximum(edge_left[differing], edge_right[differing]))
        if not pairs:
            return np.empty((0, 2), dtype=np.int32), np.empty(0, dtype=np.float64)
        pair_array = np.concatenate(pairs).astype(np.int32)
        strength_array = np.concatenate(strengths).astype(np.float64)
        pair_array.sort(axis=1)
        packed = pair_array[:, 0].astype(np.int64) << 32 | pair_array[:, 1].astype(np.int64)
        unique, inverse = np.unique(packed, return_inverse=True)
        boundary_count = np.bincount(inverse).astype(np.float64)
        edge_sum = np.bincount(inverse, weights=strength_array)
        mean_edge = edge_sum / np.maximum(boundary_count, 1.0)
        result_pairs = np.column_stack((unique >> 32, unique & 0xFFFFFFFF)).astype(np.int32)
        flat_strength = 1.8 + settings.edge_protection / 45.0
        weights = boundary_count * (0.15 + flat_strength * (1.0 - mean_edge) ** 2)
        return result_pairs, weights

    def _bounded_palette(
        self,
        unary: np.ndarray,
        pairs: np.ndarray,
        weights: np.ndarray,
        labels: np.ndarray,
        settings: SketchSettings,
    ) -> tuple[int, ...]:
        active = set(int(value) for value in np.unique(labels))
        label_cost = settings.minimum_region * 0.25 + settings.detail_scale
        while len(active) > 2:
            base_energy = self._energy(unary, pairs, weights, labels)
            removal: list[tuple[float, int, np.ndarray]] = []
            for colour in active:
                regions = np.flatnonzero(labels == colour)
                alternatives = np.asarray(sorted(active - {colour}), dtype=np.int32)
                if not len(regions) or not len(alternatives):
                    removal.append((-label_cost, colour, labels.copy()))
                    continue
                candidate = labels.copy()
                candidate[regions] = alternatives[
                    np.argmin(unary[np.ix_(regions, alternatives)], axis=1)
                ]
                delta = self._energy(unary, pairs, weights, candidate) - base_energy - label_cost
                removal.append((delta, colour, candidate))
            delta, removed, candidate = min(removal, key=lambda item: (item[0], item[1]))
            if len(active) <= settings.colour_limit and delta >= 0:
                break
            active.remove(removed)
            labels[:] = candidate
        if len(active) < 2:
            best = np.argsort(np.sum(unary, axis=0))[:2]
            active.update(int(value) for value in best)
        return tuple(sorted(active))

    def _alpha_expansion(
        self,
        unary: np.ndarray,
        pairs: np.ndarray,
        weights: np.ndarray,
        labels: np.ndarray,
        active: tuple[int, ...],
    ) -> np.ndarray:
        labels = labels.copy()
        for _ in range(3):
            changed = False
            for alpha in active:
                candidate = self._expansion_move(unary, pairs, weights, labels, alpha)
                if self._energy(unary, pairs, weights, candidate) + 1e-6 < self._energy(
                    unary, pairs, weights, labels
                ):
                    labels = candidate
                    changed = True
            if not changed:
                break
        return labels

    @staticmethod
    def _energy(
        unary: np.ndarray,
        pairs: np.ndarray,
        weights: np.ndarray,
        labels: np.ndarray,
    ) -> float:
        value = float(np.sum(unary[np.arange(len(labels)), labels]))
        if len(pairs):
            value += float(np.sum(weights[labels[pairs[:, 0]] != labels[pairs[:, 1]]]))
        return value

    @staticmethod
    def _expansion_move(
        unary: np.ndarray,
        pairs: np.ndarray,
        weights: np.ndarray,
        labels: np.ndarray,
        alpha: int,
    ) -> np.ndarray:
        count = len(labels)
        d0 = unary[np.arange(count), labels].astype(np.float64).copy()
        d1 = unary[:, alpha].astype(np.float64).copy()
        graph_edges: list[tuple[int, int, float]] = []
        for (first, second), weight in zip(pairs, weights, strict=True):
            first = int(first)
            second = int(second)
            e00 = float(weight) if labels[first] != labels[second] else 0.0
            e01 = float(weight) if labels[first] != alpha else 0.0
            e10 = float(weight) if alpha != labels[second] else 0.0
            c = max(0.0, (e01 + e10 - e00) / 2.0)
            d1[first] += e10 - e00 - c
            d1[second] += e01 - e00 - c
            if c > 0:
                graph_edges.append((first, second, c))
                graph_edges.append((second, first, c))
        minimum = np.minimum(d0, d1)
        d0 -= minimum
        d1 -= minimum
        maximum = max(
            float(np.max(d0, initial=0.0)),
            float(np.max(d1, initial=0.0)),
            max((edge[2] for edge in graph_edges), default=0.0),
            1.0,
        )
        scale = min(10_000.0, 1_000_000_000.0 / maximum)
        source = count
        sink = count + 1
        rows: list[int] = []
        cols: list[int] = []
        data: list[int] = []

        def add(start: int, end: int, capacity: float) -> None:
            fixed = round(max(0.0, capacity) * scale)
            if fixed:
                rows.append(start)
                cols.append(end)
                data.append(fixed)

        for index in range(count):
            add(source, index, float(d1[index]))
            add(index, sink, float(d0[index]))
        for start, end, capacity in graph_edges:
            add(start, end, capacity)
        if not data:
            return labels.copy()
        capacity_graph = coo_matrix(
            (np.asarray(data, dtype=np.int64), (rows, cols)),
            shape=(count + 2, count + 2),
            dtype=np.int64,
        ).tocsr()
        flow = maximum_flow(capacity_graph, source, sink, method="dinic").flow.tocsr()
        residual = (capacity_graph - flow).tocsr()
        reachable = np.zeros(count + 2, dtype=bool)
        reachable[source] = True
        stack = [source]
        while stack:
            node = stack.pop()
            begin, end = residual.indptr[node], residual.indptr[node + 1]
            for target, value in zip(
                residual.indices[begin:end], residual.data[begin:end], strict=True
            ):
                if value > 0 and not reachable[target]:
                    reachable[target] = True
                    stack.append(int(target))
        result = labels.copy()
        result[~reachable[:count]] = alpha
        return result

    def _refine_boundaries(
        self,
        lab: np.ndarray,
        visible: np.ndarray,
        labels: np.ndarray,
        edge: np.ndarray,
        settings: SketchSettings,
        active: tuple[int, ...],
    ) -> np.ndarray:
        boundary = np.zeros(visible.shape, dtype=bool)
        boundary[:, 1:] |= visible[:, 1:] & visible[:, :-1] & (labels[:, 1:] != labels[:, :-1])
        boundary[1:, :] |= visible[1:, :] & visible[:-1, :] & (labels[1:, :] != labels[:-1, :])
        band = (
            ndi.binary_dilation(boundary, iterations=max(1, settings.detail_scale // 2)) & visible
        )
        positions = np.flatnonzero(band)
        if not len(positions):
            return labels
        height, width = visible.shape
        active_mask = np.zeros(len(PALETTE), dtype=bool)
        active_mask[np.asarray(active, dtype=np.int32)] = True
        for _ in range(2):
            updated = labels.copy()
            for offset in range(0, len(positions), 8192):
                selected = positions[offset : offset + 8192]
                y, x = np.divmod(selected, width)
                candidate_columns = [labels[y, x]]
                for dy, dx in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                    ny = np.clip(y + dy, 0, height - 1)
                    nx = np.clip(x + dx, 0, width - 1)
                    candidate_columns.append(labels[ny, nx])
                candidates = np.stack(candidate_columns, axis=1)
                scores = np.full(candidates.shape, np.inf, dtype=np.float32)
                for column in range(candidates.shape[1]):
                    colours = candidates[:, column]
                    valid = colours >= 0
                    valid &= active_mask[np.maximum(colours, 0)]
                    safe_colours = np.maximum(colours, 0)
                    colour_cost = deltaE_ciede2000(lab[y, x], self._lab[safe_colours]).astype(
                        np.float32
                    )
                    smooth = np.zeros(len(selected), dtype=np.float32)
                    for dy, dx in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                        ny = np.clip(y + dy, 0, height - 1)
                        nx = np.clip(x + dx, 0, width - 1)
                        neighbour = labels[ny, nx]
                        smooth += (neighbour >= 0) & (neighbour != colours)
                    score = colour_cost + smooth * (1.8 * (1.0 - edge[y, x]))
                    scores[:, column] = np.where(valid, score, np.inf)
                chosen = np.argmin(scores, axis=1)
                updated[y, x] = candidates[np.arange(len(selected)), chosen]
            labels = updated
        return labels

    def _repair_small_components(
        self,
        lab: np.ndarray,
        visible: np.ndarray,
        labels: np.ndarray,
        edge: np.ndarray,
        settings: SketchSettings,
    ) -> np.ndarray:
        minimum = settings.minimum_region
        if minimum <= 0:
            return labels
        result = labels.copy()
        structure = np.ones((3, 3), dtype=np.uint8)
        for colour in tuple(int(value) for value in np.unique(result[visible])):
            components, count = ndi.label((result == colour) & visible, structure=structure)
            sizes = np.bincount(components.ravel())
            for component in range(1, count + 1):
                size = int(sizes[component])
                if size >= minimum:
                    continue
                mask = components == component
                border = ndi.binary_dilation(mask, structure=structure) & visible & ~mask
                neighbours = tuple(int(value) for value in np.unique(result[border]) if value >= 0)
                if not neighbours or float(np.mean(edge[mask])) >= 0.58:
                    continue
                current_cost = (
                    float(np.sum(deltaE_ciede2000(lab[mask], self._lab[colour][None, :])))
                    + minimum
                    - size
                )
                best: tuple[float, int] | None = None
                for neighbour in neighbours:
                    colour_cost = float(
                        np.sum(deltaE_ciede2000(lab[mask], self._lab[neighbour][None, :]))
                    )
                    boundary_penalty = (
                        float(np.count_nonzero(border & (result != neighbour))) * 0.35
                    )
                    candidate = colour_cost + boundary_penalty
                    if best is None or candidate < best[0]:
                        best = (candidate, neighbour)
                if best is not None and best[0] < current_cost:
                    result[mask] = best[1]
        return result

    def render_composite(self, result: SketchResult) -> bytes:
        key = f"{self.cache_key(result.source_hash, result.settings)}:composite"
        with self._render_lock:
            cached = self._render_cache.get(key)
            if cached is not None:
                self._render_cache.move_to_end(key)
                return cached
        rgba = np.zeros((*result.labels.shape, 4), dtype=np.uint8)
        visible = result.alpha > 0
        rgba[visible, :3] = self._rgb[result.labels[visible]]
        rgba[..., 3] = result.alpha
        return self._remember_render(key, self._webp(rgba))

    def render_layer(self, result: SketchResult, colour: int) -> bytes:
        key = f"{self.cache_key(result.source_hash, result.settings)}:layer:{colour}"
        with self._render_lock:
            cached = self._render_cache.get(key)
            if cached is not None:
                self._render_cache.move_to_end(key)
                return cached
        rgba = np.zeros((*result.labels.shape, 4), dtype=np.uint8)
        selected = (result.labels == colour) & (result.alpha > 0)
        rgba[selected, :3] = self._rgb[colour]
        rgba[selected, 3] = result.alpha[selected]
        return self._remember_render(key, self._webp(rgba))

    def _remember_render(self, key: str, data: bytes) -> bytes:
        with self._render_lock:
            self._render_cache[key] = data
            self._render_cache.move_to_end(key)
            while len(self._render_cache) > 64:
                self._render_cache.popitem(last=False)
        return data

    @staticmethod
    def _webp(rgba: np.ndarray) -> bytes:
        output = io.BytesIO()
        Image.fromarray(rgba, "RGBA").save(
            output, "WEBP", lossless=True, quality=100, method=4, exact=True
        )
        return output.getvalue()

    @staticmethod
    def serialize(result: SketchResult) -> bytes:
        output = io.BytesIO()
        np.savez_compressed(
            output,
            labels=result.labels,
            alpha=result.alpha,
            counts=result.counts,
            settings=np.asarray(
                [
                    result.settings.detail_scale,
                    result.settings.edge_protection,
                    result.settings.minimum_region,
                    result.settings.colour_limit,
                ],
                dtype=np.int16,
            ),
            preset=np.asarray(result.settings.preset),
            source_hash=np.asarray(result.source_hash),
        )
        return output.getvalue()

    @staticmethod
    def deserialize(data: bytes) -> SketchResult:
        with np.load(io.BytesIO(data), allow_pickle=False) as stored:
            values = stored["settings"].astype(int).tolist()
            settings = SketchSettings(*values, preset=str(stored["preset"]))
            return SketchResult(
                labels=stored["labels"].astype(np.int16),
                alpha=stored["alpha"].astype(np.uint8),
                counts=stored["counts"].astype(np.float64),
                settings=settings,
                source_hash=str(stored["source_hash"]),
            )
