"""Fold averaged windows, and stored tier rows, into Dashboard time buckets.

The Dashboard's aggregated waterfall and stats timeline are a fixed grid of
buckets anchored to epoch multiples of ``bucket_sec``. A bucket can be fed by
raw ``avg_windows`` rows (a chunk at a time, in numpy) and by ``avg_tiers``
rows (one stored period each, already summed); both give the same means, since
a tier row carries its sums and counts.
"""

from __future__ import annotations

import math
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from collections.abc import Sequence

# Column lists the folds expect, in order.
WINDOW_WF_COLUMNS = (
    "start_time, num_bins, freq_start_hz, freq_step_hz, psd_powers, "
    "pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis"
)
TIER_WF_COLUMNS = (
    "bucket_epoch, num_bins, freq_start_hz, freq_step_hz, psd_mean, "
    "n, n_psd, sum_avg, max_max, sum_median, sum_std, sum_kurtosis, psd_min, psd_max"
)
WINDOW_STATS_COLUMNS = "start_time, pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis"
TIER_STATS_COLUMNS = "bucket_epoch, n, sum_avg, max_max, sum_median, sum_std, sum_kurtosis, min_avg"


def decode_psd_rows(
    rows: Sequence[Sequence[Any]], blob_col: int, max_bins: int
) -> tuple[np.ndarray[Any, np.dtype[np.float32]], np.ndarray[Any, np.dtype[np.bool_]]]:
    """Decode a chunk's PSD blobs to a (rows, max_bins) float32 array.

    Each row is downsampled by group-mean when wider than ``max_bins`` and
    NaN-padded when narrower, as ``SensorDatabase._ds_psd`` does for one row.
    Rows of the same width are decoded together. Returns the array and a mask
    of the rows that had a blob (the others are all-NaN).
    """
    out = np.full((len(rows), max_bins), np.nan, dtype=np.float32)
    has = np.zeros(len(rows), dtype=bool)
    groups: dict[int, list[int]] = {}
    for i, r in enumerate(rows):
        blob = r[blob_col]
        if blob is not None and len(blob) >= 4:
            groups.setdefault(len(blob) // 4, []).append(i)
    for width, members in groups.items():
        sel = np.asarray(members)
        arr = np.frombuffer(
            b"".join(rows[i][blob_col][: width * 4] for i in members), dtype="<f4"
        ).reshape(len(members), width)
        if width > max_bins:
            # Group-mean of each `factor` adjacent bins, as strided slice sums:
            # mean(axis=2) over a short last axis is several times slower.
            factor = width // max_bins
            acc = arr[:, 0 : factor * max_bins : factor].astype(np.float32)
            for j in range(1, factor):
                acc += arr[:, j : factor * max_bins : factor]
            out[sel] = acc / np.float32(factor)
        else:
            out[sel, :width] = arr
        has[sel] = True
    return out, has


def _runs(idx: np.ndarray[Any, Any]) -> tuple[np.ndarray[Any, Any], np.ndarray[Any, Any]]:
    """Start offsets and bucket indexes of the runs of equal ``idx`` (rows come
    in time order, so each bucket is one contiguous run)."""
    starts = np.flatnonzero(np.r_[True, idx[1:] != idx[:-1]])
    return starts, idx[starts]


def _col(rows: Sequence[Sequence[Any]], col: int, none: float = 0.0) -> np.ndarray[Any, Any]:
    return np.fromiter(
        (none if r[col] is None else float(r[col]) for r in rows), np.float64, len(rows)
    )


def _window_epochs(rows: Sequence[Sequence[Any]]) -> np.ndarray[Any, Any]:
    return np.fromiter(
        (datetime.fromisoformat(r[0]).timestamp() for r in rows), np.float64, len(rows)
    )


class _Grid:
    def __init__(self, since: datetime, until: datetime, bucket_sec: float) -> None:
        self.bucket_sec = bucket_sec
        self.anchor = math.floor(since.timestamp() / bucket_sec) * bucket_sec
        self.n = max(1, math.ceil((until.timestamp() - self.anchor) / bucket_sec))

    def index(self, epochs: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
        return np.clip(((epochs - self.anchor) / self.bucket_sec).astype(np.int64), 0, self.n - 1)


class WaterfallFold:
    """Per-bucket PSD means and scalar stats over the Dashboard grid."""

    def __init__(self, since: datetime, until: datetime, bucket_sec: float, max_bins: int) -> None:
        self.grid = _Grid(since, until, bucket_sec)
        n = self.grid.n
        self.max_bins = max_bins
        # PSD per-bin sums and counts, so a NaN-padded (short) row never
        # poisons a bucket's mean.
        self.psd_sum = np.zeros((n, max_bins), dtype=np.float64)
        self.psd_cnt = np.zeros((n, max_bins), dtype=np.int64)
        self.stat_n = np.zeros(n, dtype=np.int64)
        self.stat_avg = np.zeros(n)
        self.stat_med = np.zeros(n)
        self.stat_std = np.zeros(n)
        self.stat_kurt = np.zeros(n)
        self.stat_max = np.full(n, -np.inf)
        self.total_windows = 0
        self.gmin, self.gmax = math.inf, -math.inf
        self.axis: tuple[int, float, float] | None = None

    def _take_axis(self, row: Sequence[Any]) -> None:
        if self.axis is None:
            self.axis = (int(row[1]), float(row[2]), float(row[3]))

    def _add_psd(
        self,
        psd: np.ndarray[Any, Any],
        idx: np.ndarray[Any, Any],
        weight: np.ndarray[Any, Any] | None = None,
    ) -> None:
        valid = ~np.isnan(psd)
        vals = np.where(valid, psd, 0.0)
        cnt = valid.astype(np.int64)
        if weight is not None:
            vals = vals * weight[:, None]
            cnt = cnt * weight[:, None].astype(np.int64)
        starts, at = _runs(idx)
        self.psd_sum[at] += np.add.reduceat(vals, starts, axis=0)
        self.psd_cnt[at] += np.add.reduceat(cnt, starts, axis=0)

    def add_windows(self, rows: Sequence[Sequence[Any]]) -> None:
        """A chunk of avg_windows rows in WINDOW_WF_COLUMNS order, time-sorted."""
        if not rows:
            return
        self._take_axis(rows[0])
        idx = self.grid.index(_window_epochs(rows))
        starts, at = _runs(idx)
        np.add.at(self.stat_n, at, np.diff(np.r_[starts, len(rows)]))
        for arr, col in (
            (self.stat_avg, 5),
            (self.stat_med, 7),
            (self.stat_std, 8),
            (self.stat_kurt, 9),
        ):
            arr[at] += np.add.reduceat(_col(rows, col), starts)
        pmax = np.maximum.reduceat(_col(rows, 6, none=-np.inf), starts)
        self.stat_max[at] = np.maximum(self.stat_max[at], pmax)
        psd, has = decode_psd_rows(rows, 4, self.max_bins)
        if not has.any():
            return
        self.total_windows += int(has.sum())
        psd, pidx = psd[has], idx[has]
        with np.errstate(invalid="ignore"), warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)  # an all-NaN row
            lo, hi = np.nanmin(psd), np.nanmax(psd)
        if np.isfinite(lo):
            self.gmin, self.gmax = min(self.gmin, float(lo)), max(self.gmax, float(hi))
        self._add_psd(psd, pidx)

    def add_tiers(self, rows: Sequence[Sequence[Any]]) -> None:
        """avg_tiers rows in TIER_WF_COLUMNS order, time-sorted. Each tier
        period lies inside one display bucket (the bucket is a multiple of
        the tier period)."""
        if not rows:
            return
        self._take_axis(rows[0])
        idx = self.grid.index(_col(rows, 0))
        starts, at = _runs(idx)
        n = _col(rows, 5)
        self.stat_n[at] += np.add.reduceat(n, starts).astype(np.int64)
        for arr, col in (
            (self.stat_avg, 7),
            (self.stat_med, 9),
            (self.stat_std, 10),
            (self.stat_kurt, 11),
        ):
            arr[at] += np.add.reduceat(_col(rows, col), starts)
        pmax = np.maximum.reduceat(_col(rows, 8, none=-np.inf), starts)
        self.stat_max[at] = np.maximum(self.stat_max[at], pmax)
        for r in rows:
            if r[12] is not None:
                self.gmin = min(self.gmin, float(r[12]))
            if r[13] is not None:
                self.gmax = max(self.gmax, float(r[13]))
        psd, has = decode_psd_rows(rows, 4, self.max_bins)
        n_psd = _col(rows, 6)
        has &= n_psd > 0
        if not has.any():
            return
        self.total_windows += int(n_psd[has].sum())
        self._add_psd(psd[has], idx[has], weight=n_psd[has])

    def result(self) -> dict[str, Any]:
        num_bins, freq_start_hz, freq_step_hz = self.axis or (0, 0.0, 0.0)
        # The downsampled axis is still uniform: group-mean of a uniform axis
        # shifts the start by (factor-1)*step/2 and multiplies the step.
        if num_bins > self.max_bins and freq_step_hz > 0:
            factor = num_bins // self.max_bins
            freq_start_hz = freq_start_hz + (factor - 1) * freq_step_hz / 2.0
            freq_step_hz = factor * freq_step_hz
        psd_rows: list[list[float]] = []
        for i in range(self.grid.n):
            if self.psd_cnt[i].any():
                with np.errstate(invalid="ignore", divide="ignore"):
                    mean_row = self.psd_sum[i] / self.psd_cnt[i]
                psd_rows.append([float(x) for x in mean_row])
            else:
                psd_rows.append([float("nan")] * self.max_bins)
        g, sn = self.grid, self.stat_n
        buckets = [
            {
                "start_epoch": g.anchor + i * g.bucket_sec,
                "duration_sec": g.bucket_sec,
                "count": int(sn[i]),
                "pwr_avg": float(self.stat_avg[i] / sn[i]) if sn[i] else 0.0,
                # -inf when every window in the bucket had no pwr_max.
                "pwr_max": float(self.stat_max[i]) if sn[i] else 0.0,
                "pwr_median": float(self.stat_med[i] / sn[i]) if sn[i] else 0.0,
                "pwr_std": float(self.stat_std[i] / sn[i]) if sn[i] else 0.0,
                "kurtosis": float(self.stat_kurt[i] / sn[i]) if sn[i] else 0.0,
            }
            for i in range(g.n)
        ]
        return {
            "bucket_sec": g.bucket_sec,
            "num_bins": self.max_bins,
            "min_db": self.gmin if self.gmin != math.inf else 0.0,
            "max_db": self.gmax if self.gmax != -math.inf else 0.0,
            "total_windows": self.total_windows,
            "freq_start_hz": freq_start_hz,
            "freq_step_hz": freq_step_hz,
            "mode": 1,
            "buckets": buckets,
            "psd_rows": psd_rows,
        }


class StatsFold:
    """Per-bucket scalar stats (the power and kurtosis timelines)."""

    def __init__(self, since: datetime, until: datetime, bucket_sec: float) -> None:
        self.grid = _Grid(since, until, bucket_sec)
        self.tz = since.tzinfo
        n = self.grid.n
        self.n = np.zeros(n, dtype=np.int64)
        self.avg = np.zeros(n)
        self.med = np.zeros(n)
        self.std = np.zeros(n)
        self.kurt = np.zeros(n)
        self.mx = np.full(n, -np.inf)
        self.gmin, self.gmax = math.inf, -math.inf

    def _fold(
        self,
        idx: np.ndarray[Any, Any],
        counts: np.ndarray[Any, Any],
        sums: Sequence[np.ndarray[Any, Any]],
        pmax: np.ndarray[Any, Any],
    ) -> None:
        starts, at = _runs(idx)
        self.n[at] += np.add.reduceat(counts, starts).astype(np.int64)
        for arr, vals in zip((self.avg, self.med, self.std, self.kurt), sums, strict=True):
            arr[at] += np.add.reduceat(vals, starts)
        self.mx[at] = np.maximum(self.mx[at], np.maximum.reduceat(pmax, starts))

    def add_windows(self, rows: Sequence[Sequence[Any]]) -> None:
        """avg_windows rows in WINDOW_STATS_COLUMNS order, time-sorted."""
        if not rows:
            return
        avg = _col(rows, 1)
        pmax = _col(rows, 2, none=-np.inf)
        self._fold(
            self.grid.index(_window_epochs(rows)),
            np.ones(len(rows)),
            (avg, _col(rows, 3), _col(rows, 4), _col(rows, 5)),
            pmax,
        )
        # The timeline's range: pwr_avg to pwr_max over windows with a max.
        has = np.isfinite(pmax)
        if has.any():
            self.gmin = min(self.gmin, float(avg[has].min()))
            self.gmax = max(self.gmax, float(pmax[has].max()))

    def add_tiers(self, rows: Sequence[Sequence[Any]]) -> None:
        """avg_tiers rows in TIER_STATS_COLUMNS order, time-sorted."""
        if not rows:
            return
        pmax = _col(rows, 3, none=-np.inf)
        self._fold(
            self.grid.index(_col(rows, 0)),
            _col(rows, 1),
            (_col(rows, 2), _col(rows, 4), _col(rows, 5), _col(rows, 6)),
            pmax,
        )
        for r in rows:
            if r[3] is not None and r[7] is not None:
                self.gmin = min(self.gmin, float(r[7]))
                self.gmax = max(self.gmax, float(r[3]))

    def result(self) -> dict[str, Any]:
        g, n = self.grid, self.n
        points = [
            {
                "start_time": datetime.fromtimestamp(
                    g.anchor + i * g.bucket_sec, tz=self.tz
                ).isoformat(),
                "count": int(n[i]),
                "pwr_avg": float(self.avg[i] / n[i]) if n[i] else None,
                "pwr_max": float(self.mx[i]) if n[i] and self.mx[i] != -np.inf else None,
                "pwr_median": float(self.med[i] / n[i]) if n[i] else None,
                "pwr_std": float(self.std[i] / n[i]) if n[i] else None,
                "kurtosis": float(self.kurt[i] / n[i]) if n[i] else None,
            }
            for i in range(g.n)
        ]
        return {
            "bucket_sec": g.bucket_sec,
            "min_pwr": self.gmin if self.gmin != math.inf else 0.0,
            "max_pwr": self.gmax if self.gmax != -math.inf else 0.0,
            "points": points,
        }
