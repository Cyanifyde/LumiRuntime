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
from skimage.color import deltaE_ciede2000, lab2rgb, rgb2lab
from skimage.measure import label as connected_labels

from .image import PreparedImage
from .legacy_engine import RENDERER_VERSION as LEGACY_RENDERER_VERSION
from .legacy_engine import SketchEngine as LegacySketchEngine
from .legacy_engine import SketchSettings as LegacySketchSettings
from .palette import PALETTE, PALETTE_VERSION, palette_lab, palette_rgb

RENDERER_VERSION = "ksketch-detail-opt-v2.6"
COVERAGE_CUTOFF_PERCENT = 0.25
COVERAGE_VERSION = "coverage-config-v2"
TEMPERATURE_VERSION = "lab-temperature-v1"
TEMPERATURES = ("Normal", "Warm", "Cold")
MODES = ("Preserve Detail", "Simplify")


@dataclass(frozen=True, slots=True)
class SketchSettings:
    detail_scale: int
    edge_protection: int
    minimum_region: int
    colour_limit: int
    preset: str = "Custom"
    mode: str = "Preserve Detail"
    cutoff_percent: float = COVERAGE_CUTOFF_PERCENT
    temperature: str = "Normal"

    def validate(self) -> None:
        if not 1 <= self.detail_scale <= 16:
            raise ValueError("detail scale")
        if not 0 <= self.edge_protection <= 100:
            raise ValueError("edge protection")
        if not 0 <= self.minimum_region <= 1000:
            raise ValueError("minimum region")
        if not 2 <= self.colour_limit <= 64:
            raise ValueError("colour limit")
        if self.mode not in MODES:
            raise ValueError("mode")
        if self.temperature not in TEMPERATURES:
            raise ValueError("temperature")
        if not math.isfinite(self.cutoff_percent) or not 0 <= self.cutoff_percent <= 100:
            raise ValueError("cutoff percent")
        if not math.isclose(
            self.cutoff_percent * 100, round(self.cutoff_percent * 100), rel_tol=0, abs_tol=1e-8
        ):
            raise ValueError("cutoff precision")


PRESETS = {
    "Detailed": SketchSettings(2, 85, 16, 64, "Detailed"),
    "Balanced": SketchSettings(4, 80, 40, 48, "Balanced"),
    "Clean": SketchSettings(7, 75, 80, 32, "Clean"),
}

SIMPLIFY_PRESETS = {


    "Detailed": SketchSettings(1, 95, 4, 64, "Detailed", "Simplify"),
    "Balanced": SketchSettings(2, 90, 12, 64, "Balanced", "Simplify"),
    "Clean": SketchSettings(3, 85, 24, 56, "Clean", "Simplify"),
}


def presets_for(mode: str) -> dict[str, SketchSettings]:
    return SIMPLIFY_PRESETS if mode == "Simplify" else PRESETS


@dataclass(frozen=True, slots=True)
class SketchResult:
    labels: np.ndarray
    alpha: np.ndarray
    counts: np.ndarray
    settings: SketchSettings
    source_hash: str
    version: str = RENDERER_VERSION

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
        self._analysis_cache: OrderedDict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = (
            OrderedDict()
        )
        self._render_cache: OrderedDict[str, bytes] = OrderedDict()
        self._render_lock = threading.Lock()
        self._legacy = LegacySketchEngine(cache_size=2)

    def optimize(self, source: PreparedImage, settings: SketchSettings) -> SketchResult:
        settings.validate()
        key = self.cache_key(source.sha256, settings)
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached
        source_hash = source.sha256
        source = self._temperature_source(source, settings.temperature)
        if settings.mode == "Simplify":


            old = self._legacy.optimize(
                source,
                LegacySketchSettings(
                    settings.detail_scale,
                    settings.edge_protection,
                    settings.minimum_region,
                    settings.colour_limit,
                    settings.preset,
                ),
            )
            labels = self._merge_low_coverage(
                old.labels,
                old.alpha,
                rgb2lab(source.rgba[..., :3].astype(np.float32) / 255.0),
                settings.cutoff_percent,
            )
            counts = np.bincount(
                labels[old.alpha > 0],
                weights=old.alpha[old.alpha > 0].astype(np.float64) / 255.0,
                minlength=len(PALETTE),
            )
            result = SketchResult(
                labels,
                old.alpha,
                counts,
                settings,
                source_hash,
                self.expected_version(settings.mode),
            )
            self._cache[key] = result
            self._cache.move_to_end(key)
            while len(self._cache) > self._cache_size:
                self._cache.popitem(last=False)
            return result
        rgba = source.rgba
        visible = rgba[..., 3] > 0
        alpha = rgba[..., 3].astype(np.uint8)
        cached_analysis = self._analysis_cache.get(source.sha256)
        if cached_analysis is None:
            rgb = rgba[..., :3].astype(np.float32) / 255.0
            lab = rgb2lab(rgb).astype(np.float32)
            baseline = self.nearest_palette(lab, visible)
            edge = self._structural_edges(lab, visible, settings)
            edge = self._contour_support(edge, baseline, visible)
            self._analysis_cache[source.sha256] = (lab, baseline, edge)
            self._analysis_cache.move_to_end(source.sha256)
            while len(self._analysis_cache) > 2:
                self._analysis_cache.popitem(last=False)
        else:
            lab, baseline, edge = cached_analysis
            self._analysis_cache.move_to_end(source.sha256)
        rgb = rgba[..., :3].astype(np.float32) / 255.0
        guided = self._guided_colour(rgb, alpha, settings.detail_scale)
        guided_lab = rgb2lab(guided).astype(np.float32)


        protection = np.clip((edge - 0.45) * (settings.edge_protection / 25.0), 0, 1)
        working_lab = guided_lab * (1 - protection[..., None]) + lab * protection[..., None]
        initial = self.nearest_palette(working_lab, visible)
        active = self._choose_palette(working_lab, alpha, edge, initial, baseline, settings)
        nearest = (
            initial if len(active) == 64 else self.nearest_palette(working_lab, visible, active)
        )


        pixel_labels = nearest.copy()
        pixel_labels = self._refine_boundaries(
            working_lab,
            visible,
            pixel_labels,
            edge,
            settings,
            active,
            alpha=alpha,
            nearest=nearest,
            source_nearest=baseline,
        )
        pixel_labels = self._repair_small_components(
            working_lab, visible, pixel_labels, edge, settings, alpha=alpha
        )
        pixel_labels = self._merge_low_coverage(pixel_labels, alpha, lab, settings.cutoff_percent)
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
            source_hash=source_hash,
        )
        self._cache[key] = result
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return result

    @staticmethod
    def _temperature_source(source: PreparedImage, temperature: str) -> PreparedImage:

        if temperature not in TEMPERATURES:
            raise ValueError("temperature")
        if temperature == "Normal":
            return source
        lab = rgb2lab(source.rgba[..., :3].astype(np.float32) / 255)
        lightness = np.clip(lab[..., 0] / 100, 0, 1)
        strength = 4 * lightness * (1 - lightness)
        lab[..., 2] += (8 if temperature == "Warm" else -8) * strength
        adjusted = np.rint(np.clip(lab2rgb(lab), 0, 1) * 255).astype(np.uint8)
        rgba = source.rgba.copy()
        visible = rgba[..., 3] > 0
        rgba[visible, :3] = adjusted[visible]
        analysis_hash = hashlib.sha256(
            f"{source.sha256}:{temperature}:{TEMPERATURE_VERSION}".encode()
        ).hexdigest()
        return PreparedImage(rgba, analysis_hash, source.source_format)

    @staticmethod
    def expected_version(mode: str) -> str:
        return (
            f"{LEGACY_RENDERER_VERSION}-{COVERAGE_VERSION}-{TEMPERATURE_VERSION}"
            if mode == "Simplify"
            else RENDERER_VERSION
        )

    def _merge_low_coverage(
        self,
        labels: np.ndarray,
        alpha: np.ndarray,
        lab: np.ndarray,
        cutoff_percent: float = COVERAGE_CUTOFF_PERCENT,
    ) -> np.ndarray:

        cutoff_hundredths = round(cutoff_percent * 100)
        if cutoff_hundredths == 0:
            return labels
        visible = alpha > 0
        mass = np.bincount(labels[visible], weights=alpha[visible], minlength=len(PALETTE))
        discarded = (mass > 0) & (mass * 10_000 < mass.sum() * cutoff_hundredths)
        if not np.any(discarded):
            return labels


        if np.all(discarded[mass > 0]):
            discarded[int(np.argmax(mass))] = False
        retained = (mass > 0) & ~discarded
        rare = visible & discarded[np.maximum(labels, 0)]
        allowed = np.flatnonzero(retained).astype(np.int16)
        if len(allowed) == 1:
            result = labels.copy()
            result[rare] = allowed[0]
            return result
        columns = np.full(len(PALETTE), -1, dtype=np.int16)
        columns[allowed] = np.arange(len(allowed))
        regions = (
            connected_labels(np.where(rare, labels + 1, 0), background=0, connectivity=1).astype(
                np.int32
            )
            - 1
        )
        count = int(regions.max()) + 1


        islands, island_count = ndi.label(rare)
        region_islands = np.zeros(count, dtype=np.int32)
        region_islands[regions[rare]] = islands[rare]
        direct = np.zeros((count, len(allowed)), dtype=bool)
        for source, neighbour in (
            ((slice(None), slice(None, -1)), (slice(None), slice(1, None))),
            ((slice(None), slice(1, None)), (slice(None), slice(None, -1))),
            ((slice(None, -1), slice(None)), (slice(1, None), slice(None))),
            ((slice(1, None), slice(None)), (slice(None, -1), slice(None))),
        ):
            boundary = rare[source] & visible[neighbour] & ~rare[neighbour]
            direct[regions[source][boundary], columns[labels[neighbour][boundary]]] = True
        shared = np.zeros((island_count + 1, len(allowed)), dtype=bool)
        nodes, colours = np.nonzero(direct)
        shared[region_islands[nodes], colours] = True
        candidates = direct.copy()
        indirect = ~np.any(candidates, axis=1)
        candidates[indirect] = shared[region_islands[indirect]]


        candidates[~np.any(candidates, axis=1)] = True
        replacements = np.argmax(candidates, axis=1)
        ambiguous = np.flatnonzero(np.sum(candidates, axis=1) > 1)
        if len(ambiguous):


            compact = np.full(count, -1, dtype=np.int32)
            compact[ambiguous] = np.arange(len(ambiguous))
            selected_pixels = np.flatnonzero(rare & (compact[regions] >= 0))
            costs = np.zeros((len(ambiguous), len(allowed)), dtype=np.float64)
            flat_lab = lab.reshape(-1, 3)
            flat_alpha = alpha.ravel().astype(np.float32) / 255
            for offset in range(0, len(selected_pixels), 8192):
                positions = selected_pixels[offset : offset + 8192]
                pixels, inverse = np.unique(flat_lab[positions], axis=0, return_inverse=True)
                distances = deltaE_ciede2000(pixels[:, None], self._lab[allowed][None])[inverse]
                np.add.at(
                    costs,
                    compact[regions.ravel()[positions]],
                    distances * flat_alpha[positions, None],
                )
            replacements[ambiguous] = np.argmin(
                np.where(candidates[ambiguous], costs, np.inf), axis=1
            )
        result = labels.copy()
        result[rare] = allowed[replacements[regions[rare]]]
        return result

    @staticmethod
    def cache_key(source_hash: str, settings: SketchSettings) -> str:
        value = (
            f"{source_hash}:{settings.detail_scale}:{settings.edge_protection}:"
            f"{settings.minimum_region}:{settings.colour_limit}:{settings.mode}:"
            f"{round(settings.cutoff_percent * 100)}:"
            f"{settings.temperature}:"
            f"{PALETTE_VERSION}:{SketchEngine.expected_version(settings.mode)}"
        )
        return hashlib.sha256(value.encode()).hexdigest()

    def nearest_palette(
        self, lab: np.ndarray, visible: np.ndarray, active: tuple[int, ...] | None = None
    ) -> np.ndarray:
        allowed = np.asarray(active if active is not None else range(64), dtype=np.int16)
        result = np.full(visible.shape, -1, dtype=np.int16)
        positions = np.flatnonzero(visible)
        pixels = lab.reshape(-1, 3)
        for offset in range(0, len(positions), 8192):
            selected = positions[offset : offset + 8192]
            unique, inverse = np.unique(pixels[selected], axis=0, return_inverse=True)
            distances = deltaE_ciede2000(unique[:, None], self._lab[allowed][None])
            result.ravel()[selected] = allowed[np.argmin(distances, axis=1)][inverse]
        return result

    @staticmethod
    def _guided_colour(rgb: np.ndarray, alpha: np.ndarray, radius: int) -> np.ndarray:

        weight = alpha.astype(np.float32) / 255.0
        size = 2 * radius + 1
        mass = ndi.uniform_filter(weight, size=size, mode="reflect")
        denominator = np.maximum(mass, 1e-6)

        def mean(value: np.ndarray) -> np.ndarray:
            trailing = (1,) * (value.ndim - 2)
            expanded = weight.reshape(weight.shape + trailing)
            normalizer = denominator.reshape(denominator.shape + trailing)
            return (
                ndi.uniform_filter(value * expanded, size=(size, size, *trailing), mode="reflect")
                / normalizer
            )

        mu = mean(rgb)
        covariance = np.empty((*rgb.shape[:2], 3, 3), dtype=np.float32)
        for first in range(3):
            for second in range(3):
                covariance[..., first, second] = (
                    mean(rgb[..., first] * rgb[..., second]) - mu[..., first] * mu[..., second]
                )
        strength = 0.00035
        regularized = covariance + np.eye(3, dtype=np.float32) * strength * radius**2
        coefficients = np.linalg.solve(regularized, covariance)
        intercept = mu - np.einsum("...ij,...i->...j", coefficients, mu)
        filtered = np.einsum("...ij,...i->...j", mean(coefficients), rgb) + mean(intercept)
        return np.clip(filtered, 0, 1).astype(np.float32)

    def _choose_palette(
        self,
        lab: np.ndarray,
        alpha: np.ndarray,
        edge: np.ndarray,
        initial: np.ndarray,
        baseline: np.ndarray,
        settings: SketchSettings,
    ) -> tuple[int, ...]:
        if settings.colour_limit == 64:
            return tuple(range(64))
        visible = alpha > 0
        active = set(int(value) for value in np.unique(initial[visible]))
        active.update(int(value) for value in np.unique(baseline[visible]))
        if len(active) <= settings.colour_limit:
            return tuple(sorted(active))


        protected_alpha = alpha.astype(np.float32) * (1 + 3 * edge)
        unary = self._region_costs(lab, protected_alpha, initial, 64)
        while len(active) > settings.colour_limit:
            scores = []
            for colour in sorted(active):
                remaining = tuple(sorted(active - {colour}))
                scores.append((float(np.min(unary[:, remaining], axis=1).sum()), colour))
            active.remove(min(scores)[1])
        return tuple(sorted(active))

    @staticmethod
    def _structural_edges(
        lab: np.ndarray,
        visible: np.ndarray,
        settings: SketchSettings,
    ) -> np.ndarray:
        responses: list[np.ndarray] = []
        tensor_x = np.zeros(visible.shape, dtype=np.float32)
        tensor_y = tensor_x.copy()
        tensor_xy = tensor_x.copy()
        mass = ndi.gaussian_filter(visible.astype(np.float32), 0.6)
        for sigma in (0.6, 1.2, 2.4):
            squared = np.zeros(visible.shape, dtype=np.float32)
            for channel in range(3):
                weight = ndi.gaussian_filter(visible.astype(np.float32), sigma)
                smooth = ndi.gaussian_filter(lab[..., channel] * visible, sigma) / np.maximum(
                    weight, 1e-6
                )
                gx = ndi.sobel(smooth, axis=1, mode="nearest")
                gy = ndi.sobel(smooth, axis=0, mode="nearest")
                squared += gx * gx + gy * gy
                if sigma == 0.6:
                    tensor_x += gx * gx
                    tensor_y += gy * gy
                    tensor_xy += gx * gy
            response = np.sqrt(squared, dtype=np.float32) * math.sqrt(sigma)
            values = response[visible]
            scale = float(np.percentile(values, 97.5)) if values.size else 1.0
            responses.append(np.clip(response / max(scale, 1e-6), 0.0, 1.0))
        tensor_x = ndi.gaussian_filter(tensor_x, 1.2)
        tensor_y = ndi.gaussian_filter(tensor_y, 1.2)
        tensor_xy = ndi.gaussian_filter(tensor_xy, 1.2)
        coherence = np.sqrt((tensor_x - tensor_y) ** 2 + 4 * tensor_xy**2) / np.maximum(
            tensor_x + tensor_y, 1e-6
        )


        directional = np.clip((coherence - 0.25) / 0.65, 0, 1)
        fine = responses[0] * directional
        broad = np.minimum(responses[1], responses[2]) * (0.25 + 0.55 * directional)
        neighbourhood = np.stack(
            [
                ndi.gaussian_filter(lab[..., c] * visible, 0.8)
                / np.maximum(ndi.gaussian_filter(visible.astype(np.float32), 0.8), 1e-6)
                for c in range(3)
            ],
            axis=-1,
        )


        contrast = deltaE_ciede2000(lab, neighbourhood)
        point = np.clip((contrast - 8) / 12, 0, 1)


        thin = np.sqrt(directional) * np.clip((contrast - 1.5) / 4, 0, 1)
        confidence = np.maximum(np.maximum(np.maximum(fine, broad), point), thin)
        confidence[(mass < 0.05) | ~visible] = 0
        return confidence.astype(np.float32)

    @staticmethod
    def _contour_support(
        edge: np.ndarray, source_labels: np.ndarray, visible: np.ndarray
    ) -> np.ndarray:


        result = edge.copy()
        height, width = edge.shape
        for _ in range(2):
            previous = result.copy()
            for dy, dx in ((-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)):
                target = (
                    slice(max(0, -dy), min(height, height - dy)),
                    slice(max(0, -dx), min(width, width - dx)),
                )
                neighbour = (
                    slice(max(0, dy), min(height, height + dy)),
                    slice(max(0, dx), min(width, width + dx)),
                )
                same = (source_labels[target] == source_labels[neighbour]) & visible[target]
                supported = same & (previous[neighbour] >= 0.7)
                result[target] = np.maximum(
                    result[target], np.where(supported, previous[neighbour] * 0.88, 0)
                )
        return result

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
        chunk = 8192
        for start in range(0, len(visible_positions), chunk):
            positions = visible_positions[start : start + chunk]
            pixels, inverse = np.unique(flat_lab[positions], axis=0, return_inverse=True)
            distances = deltaE_ciede2000(pixels[:, None], self._lab[None]).astype(np.float32)[
                inverse
            ]
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
        *,
        alpha: np.ndarray | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        pairs: list[np.ndarray] = []
        strengths: list[np.ndarray] = []
        coverage: list[np.ndarray] = []
        alpha = (visible * 255).astype(np.uint8) if alpha is None else alpha
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
            if left.shape[0] == regions.shape[0]:
                coverage.append(np.minimum(alpha[:, :-1], alpha[:, 1:])[differing] / 255)
            else:
                coverage.append(np.minimum(alpha[:-1], alpha[1:])[differing] / 255)
        if not pairs:
            return np.empty((0, 2), dtype=np.int32), np.empty(0, dtype=np.float64)
        pair_array = np.concatenate(pairs).astype(np.int32)
        strength_array = np.concatenate(strengths).astype(np.float64)
        pair_array.sort(axis=1)
        packed = pair_array[:, 0].astype(np.int64) << 32 | pair_array[:, 1].astype(np.int64)
        unique, inverse = np.unique(packed, return_inverse=True)
        result_pairs = np.column_stack((unique >> 32, unique & 0xFFFFFFFF)).astype(np.int32)


        local = (
            SketchEngine._smooth_strength(settings)
            * np.exp(-strength_array * (2 + settings.edge_protection / 12))
            * np.concatenate(coverage)
        )
        weights = np.bincount(inverse, weights=local).astype(np.float64)
        return result_pairs, weights

    @staticmethod
    def _smooth_strength(settings: SketchSettings) -> float:
        return 0.65 + settings.detail_scale**1.5 * 0.32

    def _refine_boundaries(
        self,
        lab: np.ndarray,
        visible: np.ndarray,
        labels: np.ndarray,
        edge: np.ndarray,
        settings: SketchSettings,
        active: tuple[int, ...],
        *,
        alpha: np.ndarray | None = None,
        nearest: np.ndarray | None = None,
        source_nearest: np.ndarray | None = None,
    ) -> np.ndarray:
        nearest = self.nearest_palette(lab, visible, active) if nearest is None else nearest
        source_nearest = nearest if source_nearest is None else source_nearest
        alpha = (visible * 255).astype(np.uint8) if alpha is None else alpha
        height, width = visible.shape
        active_mask = np.zeros(len(PALETTE), dtype=bool)
        active_mask[np.asarray(active, dtype=np.int32)] = True
        yy, xx = np.indices(visible.shape)
        features = edge >= (0.55 - settings.edge_protection * 0.003)
        eligible = visible.copy()
        positions_by_parity = [np.flatnonzero(eligible & ((yy + xx) % 2 == p)) for p in (0, 1)]
        labels = labels.copy()


        for _ in range(3):
            changed = False
            for positions in positions_by_parity:
                for offset in range(0, len(positions), 8192):
                    selected = positions[offset : offset + 8192]
                    y, x = np.divmod(selected, width)
                    recover = features[y, x]
                    candidate_columns = [
                        labels[y, x],
                        nearest[y, x],
                        np.where(recover, source_nearest[y, x], labels[y, x]),
                    ]
                    neighbours = []
                    weights = []
                    for dy, dx in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                        ny = np.clip(y + dy, 0, height - 1)
                        nx = np.clip(x + dx, 0, width - 1)
                        candidate_columns.append(labels[ny, nx])
                        neighbours.append(labels[ny, nx])
                        weight = (
                            self._smooth_strength(settings)
                            * np.exp(
                                -np.maximum(edge[y, x], edge[ny, nx])
                                * (2 + settings.edge_protection / 12)
                            )
                            * np.minimum(alpha[y, x], alpha[ny, nx])
                            / 255
                        )
                        weight *= (ny != y) | (nx != x)
                        weights.append(weight)
                    candidates = np.stack(candidate_columns, axis=1)
                    scores = np.full(candidates.shape, np.inf, dtype=np.float32)
                    for column in range(candidates.shape[1]):
                        colours = candidates[:, column]
                        valid = colours >= 0
                        valid &= active_mask[np.maximum(colours, 0)]
                        safe_colours = np.maximum(colours, 0)
                        colour_cost = deltaE_ciede2000(lab[y, x], self._lab[safe_colours]).astype(
                            np.float32
                        ) * (alpha[y, x] / 255)
                        smooth = np.zeros(len(selected), dtype=np.float32)
                        for neighbour, weight in zip(neighbours, weights, strict=True):
                            smooth += ((neighbour >= 0) & (neighbour != colours)) * weight
                        score = colour_cost + smooth
                        scores[:, column] = np.where(valid, score, np.inf)
                    chosen = np.argmin(scores, axis=1)
                    selected_labels = candidates[np.arange(len(selected)), chosen]
                    changed |= bool(np.any(labels[y, x] != selected_labels))
                    labels[y, x] = selected_labels
            if not changed:
                break
        return labels

    def _repair_small_components(
        self,
        lab: np.ndarray,
        visible: np.ndarray,
        labels: np.ndarray,
        edge: np.ndarray,
        settings: SketchSettings,
        *,
        alpha: np.ndarray | None = None,
    ) -> np.ndarray:
        minimum = settings.minimum_region
        if minimum <= 0:
            return labels
        alpha = (visible * 255).astype(np.uint8) if alpha is None else alpha
        regions = connected_labels(labels + 1, background=0, connectivity=1).astype(np.int32) - 1
        count = int(regions.max()) + 1
        ids = regions[visible]
        sizes = np.bincount(ids, minlength=count)
        mass = np.bincount(ids, weights=alpha[visible] / 255, minlength=count)
        confidence = np.bincount(
            ids, weights=edge[visible] * alpha[visible] / 255, minlength=count
        ) / np.maximum(mass, 1e-6)
        eligible = (sizes < minimum) & (confidence < 0.45)
        if not np.any(eligible):
            return labels
        colours = np.zeros(count, dtype=np.int16)
        colours[ids] = labels[visible]
        unary = self._region_costs(lab, alpha, regions, count)
        pairs, weights = self._region_graph(regions, edge, visible, settings, alpha=alpha)
        graph = coo_matrix(
            (
                np.tile(weights, 2),
                (
                    np.concatenate((pairs[:, 0], pairs[:, 1])),
                    np.concatenate((pairs[:, 1], pairs[:, 0])),
                ),
            ),
            shape=(count, count),
        ).tocsr()


        for node in np.flatnonzero(eligible):
            start, end = graph.indptr[node : node + 2]
            neighbours = colours[graph.indices[start:end]]
            if not len(neighbours):
                continue
            boundary = graph.data[start:end]
            candidates = np.unique(neighbours)
            current = colours[node]
            current_cost = unary[node, current] + np.sum(boundary[neighbours != current])
            costs = unary[node, candidates] + np.sum(
                boundary[:, None] * (neighbours[:, None] != candidates[None]), axis=0
            )
            best = int(np.argmin(costs))
            if costs[best] + 1e-6 < current_cost:
                colours[node] = candidates[best]
        result = labels.copy()
        result[visible] = colours[ids]
        return result

    def render_composite(self, result: SketchResult) -> bytes:
        key = f"{self.cache_key(result.source_hash, result.settings)}:{result.version}:composite"
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
        key = (
            f"{self.cache_key(result.source_hash, result.settings)}:{result.version}:layer:{colour}"
        )
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
            cutoff_percent=np.asarray(result.settings.cutoff_percent, dtype=np.float64),
            temperature=np.asarray(result.settings.temperature),
            mode=np.asarray(result.settings.mode),
            version=np.asarray(result.version),
            source_hash=np.asarray(result.source_hash),
        )
        return output.getvalue()

    @staticmethod
    def deserialize(data: bytes) -> SketchResult:
        with np.load(io.BytesIO(data), allow_pickle=False) as stored:
            values = stored["settings"].astype(int).tolist()
            settings = SketchSettings(
                *values,
                preset=str(stored["preset"]),
                mode=str(stored["mode"]) if "mode" in stored else "Simplify",
                cutoff_percent=(
                    float(stored["cutoff_percent"])
                    if "cutoff_percent" in stored
                    else COVERAGE_CUTOFF_PERCENT
                ),
                temperature=str(stored["temperature"]) if "temperature" in stored else "Normal",
            )
            return SketchResult(
                labels=stored["labels"].astype(np.int16),
                alpha=stored["alpha"].astype(np.uint8),
                counts=stored["counts"].astype(np.float64),
                settings=settings,
                source_hash=str(stored["source_hash"]),
                version=str(stored["version"]) if "version" in stored else LEGACY_RENDERER_VERSION,
            )
