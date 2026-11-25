from __future__ import annotations

import math

import numpy as np

MODEL_VERSION = "single-hue-shade-preserving-deok-v1"
JND = 0.02
CLEAR_DIFFERENCE = 2 * JND
BACKGROUND_RGB = (36, 40, 47)
BACKGROUND_HEX = "#24282f"


def matching_strength(distance):

    t = np.clip((np.asarray(distance, dtype=float) - JND) / (CLEAR_DIFFERENCE - JND), 0, 1)
    return 1 - t * t * (3 - 2 * t)


def _distance_squared(ab, chroma, theta):
    theta = np.atleast_1d(theta)
    projected = ab.real[:, None] * np.cos(theta) + ab.imag[:, None] * np.sin(theta)


    return np.maximum(0.0, 2 * chroma[:, None] ** 2 - 2 * chroma[:, None] * projected)


def perceived_purity(lab, area_weights):
    lab = np.asarray(lab, dtype=float)
    area_weights = np.asarray(area_weights, dtype=float)
    if lab.ndim != 2 or lab.shape[1] != 3 or area_weights.shape != (len(lab),):
        raise ValueError("Expected N x 3 OKLab and N area weights")
    if (
        not np.all(np.isfinite(lab))
        or not np.all(np.isfinite(area_weights))
        or np.any(area_weights < 0)
    ):
        raise ValueError("Perception inputs must be finite with nonnegative weights")
    active = area_weights > 0
    if not np.any(active):
        return {
            "model": MODEL_VERSION,
            "purity": None,
            "reference_hue": None,
            "delta_e_mean": None,
            "delta_e_p95": None,
            "matching_area": None,
            "clearly_different_area": None,
            "jnd": JND,
            "clear_difference": CLEAR_DIFFERENCE,
        }

    samples = lab[active, 1] + 1j * lab[active, 2]
    ab, inverse = np.unique(samples, return_inverse=True)
    weights = np.bincount(inverse, weights=area_weights[active], minlength=len(ab))
    weights /= weights.sum()
    chroma = np.abs(ab)
    moment = np.sum(weights * chroma * ab)
    preferred = float(np.angle(moment)) if abs(moment) > 1e-18 else 0.0

    def objective(theta):
        d2 = _distance_squared(ab, chroma, theta)
        match = np.sum(weights[:, None] * matching_strength(np.sqrt(d2)), axis=0)


        return match - 1e-6 * np.sum(weights[:, None] * d2, axis=0)


    step = math.radians(2)
    seeds = np.arange(180) * step
    values = objective(seeds)
    maxima = np.flatnonzero((values >= np.roll(values, 1)) & (values >= np.roll(values, -1)))
    if not len(maxima):
        maxima = np.array([int(np.argmax(values))])
    maxima = maxima[np.argsort(values[maxima], kind="stable")[-4:]]
    best_theta = preferred
    best = float(objective([preferred])[0])
    ratio = (math.sqrt(5) - 1) / 2
    for i in maxima:
        lo, hi = seeds[i] - step, seeds[i] + step
        left, right = hi - ratio * (hi - lo), lo + ratio * (hi - lo)
        fl, fr = float(objective([left])[0]), float(objective([right])[0])
        for _ in range(22):
            if fl < fr:
                lo, left, fl = left, right, fr
                right = lo + ratio * (hi - lo)
                fr = float(objective([right])[0])
            else:
                hi, right, fr = right, left, fl
                left = hi - ratio * (hi - lo)
                fl = float(objective([left])[0])
        theta = (lo + hi) / 2
        value = float(objective([theta])[0])
        if value > best:
            best, best_theta = value, theta
    distance = np.sqrt(_distance_squared(ab, chroma, [best_theta])[:, 0])
    match = float(np.sum(weights * matching_strength(distance)))
    order = np.argsort(distance, kind="stable")
    p95_index = min(int(np.searchsorted(np.cumsum(weights[order]), 0.95)), len(order) - 1)
    return {
        "model": MODEL_VERSION,
        "purity": round(float(np.clip(100 * match, 0, 100)), 3),
        "reference_hue": round(float(np.degrees(best_theta) % 360), 3)
        if np.max(chroma) >= 0.005
        else None,
        "delta_e_mean": round(float(np.sum(weights * distance)), 7),
        "delta_e_p95": round(float(distance[order[p95_index]]), 7),
        "matching_area": round(float(100 * np.sum(weights[distance <= JND])), 3),
        "clearly_different_area": round(
            float(100 * np.sum(weights[distance >= CLEAR_DIFFERENCE])), 3
        ),
        "jnd": JND,
        "clear_difference": CLEAR_DIFFERENCE,
    }


def displayed_glow(rgb, alpha, rgb_to_lab, appearance_summary=None):

    rgb = np.asarray(rgb, dtype=float) / 255
    alpha = np.asarray(alpha, dtype=float)
    background = np.asarray(BACKGROUND_RGB, dtype=float) / 255
    shown = rgb * alpha[:, None] + background * (1 - alpha[:, None])
    shown_lab = rgb_to_lab(shown)
    backdrop_lab = rgb_to_lab(background[None])[0]
    contrast = np.linalg.norm(shown_lab - backdrop_lab, axis=1)

    visibility = np.clip(contrast / CLEAR_DIFFERENCE, 0, 1)
    result = perceived_purity(shown_lab, visibility)
    result.update(
        {
            "background": BACKGROUND_HEX,
            "visible_area_share": round(float(100 * np.mean(contrast >= JND)), 3),
            "mean_backdrop_delta_e": round(float(np.mean(contrast)), 7),
        }
    )
    if appearance_summary is not None:
        result["appearance"] = appearance_summary(shown_lab, visibility)
    return result
