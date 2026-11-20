"""The burst detector reads each grid on that grid's own frequency axis.

After a bandwidth change, grids computed at the old rate are still queued.
Both rates may have the same bin count, so they pass the stale-grid check; the
detector built from one of them must not keep the old axis for the new grids
(docs/debugging/2026-11-20_stale-burst-freq-axis.md)."""

from __future__ import annotations

import numpy as np

from rfobserver.pipeline.streaming import _STOP
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import PSDGridResult
from tests.unit.test_recording_gaps import _proc


def _grid(bins: int, rate: float) -> PSDGridResult:
    rows = 10
    return PSDGridResult(
        grid=np.full((rows, bins), -90.0, dtype=np.float32),
        time_axis=np.arange(rows) * 2e-4,
        freq_axis=np.fft.fftshift(np.fft.fftfreq(bins, 1.0 / rate)),
        ffts_per_slice=1,
        total_ffts=rows,
    )


def test_a_grid_at_another_rate_rebuilds_the_detector_on_its_axis(tmp_path, monkeypatch):
    proc = _proc(tmp_path)
    proc._running = True
    bins = proc._settings.NUM_FFT_BINS
    ss = proc._slice_samples
    axes = []  # the axis of the detector each grid was fed to
    monkeypatch.setattr(
        RollingBurstDetector, "feed", lambda self, *a, **k: axes.append(self._freq_axis) or []
    )
    rates = [56e6, 26e6, 26e6]
    for i, rate in enumerate(rates):
        proc._burst_queue.put((_grid(bins, rate), 915e6, i, i * 10 * ss))
    proc._burst_queue.put(_STOP)
    proc._burst_detection_loop()
    assert len(axes) == 3
    for axis, rate in zip(axes, rates, strict=True):
        assert np.isclose(axis[1] - axis[0], rate / bins)
