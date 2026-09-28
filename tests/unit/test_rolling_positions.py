"""Completed bursts carry the absolute stream samples they came from."""

from __future__ import annotations

import numpy as np

from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import PSDGridResult

BINS = 64
SLICE = 100  # samples per PSD row


def _grid(rows: int, burst_rows: range | None = None) -> PSDGridResult:
    g = np.full((rows, BINS), -100.0, dtype=np.float32)
    g += np.random.default_rng(0).normal(0, 0.5, g.shape).astype(np.float32)
    if burst_rows is not None:
        g[burst_rows.start : burst_rows.stop, 30:34] = -40.0
    return PSDGridResult(
        grid=g,
        time_axis=np.arange(rows) * 1e-4,
        freq_axis=np.linspace(-5e5, 5e5, BINS),
        ffts_per_slice=1,
        total_ffts=rows,
    )


def _det() -> RollingBurstDetector:
    return RollingBurstDetector(
        window_rows=256,
        eval_interval_rows=64,
        num_bins=BINS,
        burst_config=BurstDetectionConfig(threshold_high_db=20.0),
        center_freq_hz=915e6,
        freq_axis=np.linspace(-5e5, 5e5, BINS),
        time_resolution_s=1e-4,
    )


def _run(det, grids):
    out = []
    for g, start in grids:
        out += det.feed(g, chunk_start=start, slice_samples=SLICE)
    for k in range(8):  # flush
        out += det.feed(_grid(64), chunk_start=10**7 + k * 64 * SLICE, slice_samples=SLICE)
    return out


def test_burst_samples_match_the_rows_they_span():
    det = _det()
    bursts = _run(
        det,
        [(_grid(64), 0), (_grid(64, range(10, 30)), 64 * SLICE), (_grid(64), 128 * SLICE)],
    )
    (b,) = [b for b in bursts if b.peak_power_db > -60]
    # Rows 10..29 of the grid starting at stream sample 64*SLICE.
    assert b.start_sample == 64 * SLICE + 10 * SLICE
    assert b.stop_sample == 64 * SLICE + 30 * SLICE


def test_burst_samples_survive_a_dropped_chunk():
    det = _det()
    # The chunk at 64*SLICE was dropped: the next grid starts at 128*SLICE, not 64.
    bursts = _run(
        det,
        [(_grid(64), 0), (_grid(64, range(5, 15)), 128 * SLICE), (_grid(64), 192 * SLICE)],
    )
    (b,) = [b for b in bursts if b.peak_power_db > -60]
    assert b.start_sample == 128 * SLICE + 5 * SLICE
    assert b.stop_sample == 128 * SLICE + 15 * SLICE


def test_without_positions_the_fields_are_none():
    det = _det()
    out = []
    for g in [_grid(64), _grid(64, range(10, 30)), _grid(64)] + [_grid(64)] * 8:
        out += det.feed(g)
    (b,) = [b for b in out if b.peak_power_db > -60]
    assert b.start_sample is None and b.stop_sample is None


def test_reset_forgets_positions():
    det = _det()
    det.feed(_grid(64), chunk_start=0, slice_samples=SLICE)
    det.reset()
    assert (det._row_pos == -1).all()
