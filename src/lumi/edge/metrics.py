from __future__ import annotations

import collections
import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProcessingSummary:
    average_ms: float | None
    samples: int


class RollingProcessingMetrics:
    def __init__(self, window_seconds: float = 900.0) -> None:
        self._window = window_seconds
        self._samples: collections.deque[tuple[float, float]] = collections.deque()
        self._totals = 0.0

    def observe(self, duration_ms: float, *, now: float | None = None) -> None:
        timestamp = time.monotonic() if now is None else now
        self._samples.append((timestamp, duration_ms))
        self._totals += duration_ms
        self._trim(timestamp)

    def summary(self, *, now: float | None = None) -> ProcessingSummary:
        timestamp = time.monotonic() if now is None else now
        self._trim(timestamp)
        count = len(self._samples)
        return ProcessingSummary(self._totals / count if count else None, count)

    def _trim(self, now: float) -> None:
        threshold = now - self._window
        while self._samples and self._samples[0][0] < threshold:
            _, duration = self._samples.popleft()
            self._totals -= duration
