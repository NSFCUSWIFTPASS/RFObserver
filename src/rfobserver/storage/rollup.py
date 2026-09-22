"""Per-minute rollup of averaged windows, and peak selection over it.

Pure functions: no database, no clock, no I/O. The rollup loop and the peaks
endpoint own all the I/O and call in here for the arithmetic, so the two rules
most likely to be wrong (which minute a window belongs to, and which peaks are
far enough apart to count as separate events) can be tested on their own.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

# 'YYYY-MM-DDTHH:MM' is the first 16 characters of an ISO-8601 timestamp, and
# sorts lexicographically in the same order as the timestamps themselves, which
# is what lets the rollup be range-filtered without parsing any dates.
MINUTE_KEY_LEN = 16

# How far the rollup has folded forward, and how far back its backfill has
# reached. Stored in the `config` table so a restart resumes instead of
# starting over. Defined here so both the pipeline loop that writes them and
# the web endpoint that reports coverage can use the same names.
ROLLUP_NEWEST_KEY = "rollup_newest"
ROLLUP_OLDEST_KEY = "rollup_oldest"

METRICS: tuple[str, ...] = ("pwr_max", "pwr_snr", "pwr_avg")

# Each metric records the timestamp of the window that achieved it, so the
# Dashboard can centre on the real peak rather than on a minute boundary.
PEAK_TIME_COLUMN: dict[str, str] = {
    "pwr_max": "peak_max_time",
    "pwr_snr": "peak_snr_time",
    "pwr_avg": "peak_avg_time",
}


class WindowRow(NamedTuple):
    """One `avg_windows` row, light columns only."""

    start_time: str
    sdr_center_freq_hz: float
    sample_rate_hz: float | None
    gain_db: float | None
    pwr_max: float | None
    pwr_median: float | None
    pwr_avg: float | None


class MinuteSummary(NamedTuple):
    """One `avg_minutes` row."""

    minute_start: str
    sdr_center_freq_hz: float
    n: int
    sample_rate_hz: float | None
    gain_db: float | None
    pwr_max: float | None
    pwr_snr: float | None
    pwr_avg: float | None
    peak_max_time: str | None
    peak_snr_time: str | None
    peak_avg_time: str | None


class Candidate(NamedTuple):
    """A rollup row considered for selection by `select_peaks`."""

    peak_time: datetime
    value: float
    pwr_max: float | None
    pwr_snr: float | None
    pwr_avg: float | None


class Peak(NamedTuple):
    """A selected peak and the time range the Dashboard should open for it."""

    rank: int
    peak_time: datetime
    since: datetime
    until: datetime
    value: float
    pwr_max: float | None
    pwr_snr: float | None
    pwr_avg: float | None


def minute_key(start_time: str) -> str:
    """Minute bucket for an ISO-8601 window start, as stored in `avg_windows`."""
    return start_time[:MINUTE_KEY_LEN]


def _metric_value(row: WindowRow, metric: str) -> float | None:
    if metric == "pwr_max":
        return row.pwr_max
    if metric == "pwr_avg":
        return row.pwr_avg
    # pwr_snr is how far the peak rose above that window's own noise floor.
    if row.pwr_max is None or row.pwr_median is None:
        return None
    return row.pwr_max - row.pwr_median


@dataclass
class _Acc:
    """Running maxima for one (minute, centre frequency) bucket."""

    n: int = 0
    sample_rate_hz: float | None = None
    gain_db: float | None = None
    values: dict[str, float] = field(default_factory=dict)
    times: dict[str, str] = field(default_factory=dict)


def fold_windows(rows: Iterable[WindowRow]) -> list[MinuteSummary]:
    """Collapse windows into one summary per (minute, centre frequency).

    Each metric keeps its own maximum and the timestamp of the window that
    achieved it; a window missing the statistics a metric needs is skipped for
    that metric but still counted in `n`.
    """
    best: dict[tuple[str, float], _Acc] = {}
    for row in rows:
        key = (minute_key(row.start_time), row.sdr_center_freq_hz)
        acc = best.get(key)
        if acc is None:
            acc = _Acc(sample_rate_hz=row.sample_rate_hz, gain_db=row.gain_db)
            best[key] = acc
        acc.n += 1
        for metric in METRICS:
            value = _metric_value(row, metric)
            if value is None:
                continue
            current = acc.values.get(metric)
            if current is None or value > current:
                acc.values[metric] = value
                acc.times[metric] = row.start_time

    out: list[MinuteSummary] = []
    for (minute, center), acc in best.items():
        out.append(
            MinuteSummary(
                minute_start=minute,
                sdr_center_freq_hz=center,
                n=acc.n,
                sample_rate_hz=acc.sample_rate_hz,
                gain_db=acc.gain_db,
                pwr_max=acc.values.get("pwr_max"),
                pwr_snr=acc.values.get("pwr_snr"),
                pwr_avg=acc.values.get("pwr_avg"),
                peak_max_time=acc.times.get("pwr_max"),
                peak_snr_time=acc.times.get("pwr_snr"),
                peak_avg_time=acc.times.get("pwr_avg"),
            )
        )
    return out


def select_peaks(
    candidates: Sequence[Candidate],
    *,
    window_sec: int,
    count: int,
    now: datetime,
) -> list[Peak]:
    """Pick up to `count` peaks no closer together than `window_sec`.

    Without the separation rule the top N over a week is usually N samples of
    one burst. Candidates are ranked by `value` descending before selection,
    so the first acceptable candidate always wins over a rejected one that is
    weaker.
    """
    half = timedelta(seconds=window_sec / 2)
    gap = timedelta(seconds=window_sec)
    ordered = sorted(candidates, key=lambda c: c.value, reverse=True)
    chosen: list[Candidate] = []
    for candidate in ordered:
        if len(chosen) == count:
            break
        if any(abs(candidate.peak_time - c.peak_time) < gap for c in chosen):
            continue
        chosen.append(candidate)

    peaks: list[Peak] = []
    for rank, candidate in enumerate(chosen, start=1):
        until = candidate.peak_time + half
        peaks.append(
            Peak(
                rank=rank,
                peak_time=candidate.peak_time,
                since=candidate.peak_time - half,
                until=min(until, now),
                value=candidate.value,
                pwr_max=candidate.pwr_max,
                pwr_snr=candidate.pwr_snr,
                pwr_avg=candidate.pwr_avg,
            )
        )
    return peaks
