"""Stored averages of the averaged windows at coarser resolutions.

The Dashboard folds averaged windows into display buckets on every request.
Over a long range that is tens of thousands of windows (86,400 for a day of
1 s windows), all read and averaged per request. These tiers keep running
sums per tier period instead: each window is added as it is stored, and when
a period ends its row is written and the sums reset. A long range then reads
one row per period.

Pure arithmetic, no I/O: ``TierAccumulator`` turns windows into finished
``TierRow``s; the database owns writing them and reading them back.

Only data from when the accumulator runs is covered (no backfill). A query
uses a tier only for ranges that start after the tier's coverage start.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np

if TYPE_CHECKING:
    from collections.abc import Callable

# Tier periods, finest first. A Dashboard range uses the coarsest tier whose
# period fits its display bucket (see choose_tier).
TIER_SECONDS: tuple[int, ...] = (10, 60, 600)

# The colour range of a tier row is taken at this many bins (the Dashboard's
# default), matching what the raw aggregation reports for the same request.
COLOR_BINS = 512

# config-table key holding the epoch from which a tier is complete.
COVERAGE_KEY = "psd_tier_since_{}"


class Tuning(NamedTuple):
    sdr_center_freq_hz: float
    sample_rate_hz: float
    gain_db: float | None
    num_bins: int
    freq_start_hz: float
    freq_step_hz: float


@dataclass
class TierRow:
    """One stored tier period for one tuning."""

    level_sec: int
    bucket_epoch: float
    tuning: Tuning
    n: int = 0
    n_psd: int = 0
    sum_avg: float = 0.0
    sum_median: float = 0.0
    sum_std: float = 0.0
    sum_kurtosis: float = 0.0
    max_max: float | None = None
    # Smallest pwr_avg among windows with a pwr_max: the stats chart's floor.
    min_avg: float | None = None
    psd_min: float | None = None
    psd_max: float | None = None
    psd_sum: np.ndarray[Any, Any] | None = field(default=None, repr=False)  # float64, native bins

    def psd_mean(self) -> np.ndarray[Any, Any] | None:
        if self.psd_sum is None or self.n_psd == 0:
            return None
        return (self.psd_sum / self.n_psd).astype(np.float32)

    def merge(self, other: TierRow) -> None:
        """Fold another partial row of the same period and tuning into this one
        (a period split by a restart)."""
        self.n += other.n
        self.n_psd += other.n_psd
        self.sum_avg += other.sum_avg
        self.sum_median += other.sum_median
        self.sum_std += other.sum_std
        self.sum_kurtosis += other.sum_kurtosis
        self.max_max = _opt(max, self.max_max, other.max_max)
        self.min_avg = _opt(min, self.min_avg, other.min_avg)
        self.psd_min = _opt(min, self.psd_min, other.psd_min)
        self.psd_max = _opt(max, self.psd_max, other.psd_max)
        if other.psd_sum is not None:
            if self.psd_sum is None:
                self.psd_sum = other.psd_sum.copy()
            elif self.psd_sum.size == other.psd_sum.size:
                self.psd_sum += other.psd_sum


def _opt(fn: Callable[[float, float], float], a: float | None, b: float | None) -> float | None:
    if a is None:
        return b
    if b is None:
        return a
    return float(fn(a, b))


def color_range(powers: np.ndarray[Any, Any], bins: int = COLOR_BINS) -> tuple[float, float] | None:
    """Min and max of a PSD row after the Dashboard's downsample to ``bins``."""
    p = np.asarray(powers, dtype=np.float32)
    if p.size > bins:
        factor = p.size // bins
        p = p[: factor * bins].reshape(bins, factor).mean(axis=1)
    finite = p[np.isfinite(p)]
    if finite.size == 0:
        return None
    return float(finite.min()), float(finite.max())


class TierAccumulator:
    """Running sums for every tier and tuning; emits rows as periods end."""

    def __init__(self, tiers: tuple[int, ...] = TIER_SECONDS) -> None:
        self.tiers = tiers
        self._open: dict[tuple[int, Tuning], TierRow] = {}

    def add(
        self,
        *,
        epoch: float,
        tuning: Tuning,
        pwr_avg: float,
        pwr_max: float | None,
        pwr_median: float,
        pwr_std: float,
        kurtosis: float,
        powers: np.ndarray[Any, Any] | None,
    ) -> list[TierRow]:
        """Add one window (by its start time). Returns the rows whose period
        ended before this window, ready to store."""
        done = self._close_before(epoch)
        crange = color_range(powers) if powers is not None else None
        for level in self.tiers:
            key = (level, tuning)
            row = self._open.get(key)
            if row is None:
                row = TierRow(level, math.floor(epoch / level) * level, tuning)
                self._open[key] = row
            elif epoch < row.bucket_epoch:
                # A late window for a period already closed: skipped here
                # rather than counted in the wrong period.
                continue
            row.n += 1
            row.sum_avg += pwr_avg
            row.sum_median += pwr_median
            row.sum_std += pwr_std
            row.sum_kurtosis += kurtosis
            if pwr_max is not None:
                row.max_max = pwr_max if row.max_max is None else max(row.max_max, pwr_max)
                row.min_avg = pwr_avg if row.min_avg is None else min(row.min_avg, pwr_avg)
            if powers is not None:
                p = np.asarray(powers, dtype=np.float64)
                if row.psd_sum is None:
                    row.psd_sum = p.copy()
                    row.n_psd = 1
                elif row.psd_sum.size == p.size:
                    row.psd_sum += p
                    row.n_psd += 1
                if crange is not None:
                    row.psd_min = _opt(min, row.psd_min, crange[0])
                    row.psd_max = _opt(max, row.psd_max, crange[1])
        return done

    def _close_before(self, epoch: float) -> list[TierRow]:
        done = [r for r in self._open.values() if r.bucket_epoch + r.level_sec <= epoch]
        for r in done:
            del self._open[(r.level_sec, r.tuning)]
        return done

    def drain(self) -> list[TierRow]:
        """Every open (partial) row, for shutdown. A later run merges the rest
        of the period into the stored row."""
        rows = list(self._open.values())
        self._open.clear()
        return rows


def choose_tier(
    span_sec: float, max_rows: int, forced_bucket: float | None = None
) -> tuple[int, float] | None:
    """The tier and display bucket for a range, or None to use raw windows.

    Picks the coarsest tier whose period, rounded up to a whole multiple,
    stays within 1.25x of the ideal bucket (span / max_rows), so a range keeps
    at least ~80% of its rows. With ``forced_bucket`` (the live tail poll) the
    grid is given: the coarsest tier that divides it.
    """
    if forced_bucket is not None and forced_bucket > 0:
        for level in sorted(TIER_SECONDS, reverse=True):
            ratio = forced_bucket / level
            if ratio >= 1 and abs(ratio - round(ratio)) < 1e-9:
                return level, forced_bucket
        return None
    target = span_sec / max_rows
    for level in sorted(TIER_SECONDS, reverse=True):
        if level > target:
            continue
        bucket = math.ceil(target / level - 1e-9) * level
        if bucket <= 1.25 * target:
            return level, float(bucket)
    return None
