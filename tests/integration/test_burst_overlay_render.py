"""Visual / pixel-level test of the burst overlay alignment.

Runs synthetic IQ through the real PSD + rolling-detector pipeline, builds a
PNG of waterfall + overlay rectangles using the exact same render logic as
the WebUI dashboard, and asserts that each planted burst's overlay
rectangle lies on top of the bright streak it came from.

The PNG is written to ``/tmp/rfobs_burst_overlay_<bandwidth>.png`` so the
result can be eyeballed without spinning up the dashboard.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

# Pillow is only used to save the eyeball PNG; CI runners may not have it.
# Skip the whole module rather than tank an unrelated test job.
pytest.importorskip("PIL")

from rfobserver.processing.burst import BurstDetectionConfig  # noqa: E402
from rfobserver.processing.iq_utils import convert_sc16_to_complex  # noqa: E402
from rfobserver.processing.rolling_burst import RollingBurstDetector  # noqa: E402
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid  # noqa: E402

from ._synth import Burst, iq_to_sc16_int32, make_iq_with_bursts  # noqa: E402

# Same canvas dimensions as the dashboard <canvas> elements.
WF_WIDTH = 920
WF_HEIGHT = 260


def _power_to_color(val: float, vmin: float, vmax: float) -> tuple[int, int, int]:
    """Mirror of ``powerToColor()`` in src/rfobserver/web/static/shared-charts.js."""
    t = (val - vmin) / (vmax - vmin) if vmax > vmin else 0.0
    t = max(0.0, min(1.0, t))
    r = int(t * 2 * 80) if t < 0.5 else int(80 + (t - 0.5) * 2 * 175)
    if t < 0.25:
        g = int(t * 4 * 20)
    elif t < 0.75:
        g = int(20 + (t - 0.25) * 2 * 235)
    else:
        g = 255
    if t < 0.5:
        b = int(80 + (1 - t * 2) * 175)
    elif t < 0.75:
        b = int(80 - (t - 0.5) * 4 * 80)
    else:
        b = 0
    return (r, g, b)


def _render_waterfall(
    rows: list[np.ndarray],
    vmin: float,
    vmax: float,
    width: int = WF_WIDTH,
    height: int = WF_HEIGHT,
) -> np.ndarray:
    """Build an HxWx3 RGB array using the JS waterfall row mapping.

    rows[0] is the newest (top) row; the array is truncated to ``height``.
    Bin index for a pixel ``x`` is ``floor(x*N/width)`` — same as
    renderWaterfallRow() in shared-charts.js.
    """
    img = np.full((height, width, 3), (15, 15, 20), dtype=np.uint8)
    span = max(1, vmax - vmin)
    for y, powers in enumerate(rows[:height]):
        n = len(powers)
        if n == 0:
            continue
        bin_idx = np.minimum((np.arange(width) * n // width), n - 1)
        vals = np.asarray(powers, dtype=np.float64)[bin_idx]
        t = np.clip((vals - vmin) / span, 0.0, 1.0)
        r = np.where(t < 0.5, t * 2 * 80, 80 + (t - 0.5) * 2 * 175).astype(np.uint8)
        g = np.where(
            t < 0.25,
            t * 4 * 20,
            np.where(t < 0.75, 20 + (t - 0.25) * 2 * 235, 255),
        ).astype(np.uint8)
        b = np.where(
            t < 0.5,
            80 + (1 - t * 2) * 175,
            np.where(t < 0.75, 80 - (t - 0.5) * 4 * 80, 0),
        ).astype(np.uint8)
        img[y, :, 0] = r
        img[y, :, 1] = g
        img[y, :, 2] = b
    return img


def _row_for_time(row_times: list[float], target_ms: float) -> int:
    """Mirror of rowForTime() in dashboard.html — newest-first sorted."""
    if not row_times:
        return -1
    if target_ms > row_times[0]:
        return -1
    for i, t in enumerate(row_times):
        if t <= target_ms:
            return i
    return len(row_times)


def _overlay_burst_rect(
    burst: dict,
    row_times: list[float],
    freq_low_hz: float,
    freq_high_hz: float,
    width: int = WF_WIDTH,
    height: int = WF_HEIGHT,
) -> tuple[int, int, int, int] | None:
    """Compute (x, y, w, h) for a burst rectangle using the dashboard's logic.

    Returns None if the burst is fully outside the visible waterfall.
    """
    f_span = freq_high_hz - freq_low_hz
    if f_span <= 0:
        return None
    y_top_row = _row_for_time(row_times, burst["stop_time_ms"])
    y_bot_row = _row_for_time(row_times, burst["start_time_ms"])
    y_top = 0 if y_top_row < 0 else y_top_row
    y_bot = height if y_bot_row >= height else y_bot_row
    if y_bot <= 0 or y_top >= height:
        return None
    natural_h = y_bot - y_top
    h = max(3, natural_h)
    y_center = (y_top + y_bot) / 2
    y = max(0, int(round(y_center - h / 2)))

    x_lo_f = (burst["freq_low_hz"] - freq_low_hz) / f_span
    x_hi_f = (burst["freq_high_hz"] - freq_low_hz) / f_span
    natural_w = (x_hi_f - x_lo_f) * width
    w = max(3, natural_w)
    x_center = ((x_lo_f + x_hi_f) / 2) * width
    x = int(round(x_center - w / 2))
    return (x, y, int(round(w)), int(h))


def _draw_overlay(
    img: np.ndarray, rects: list[tuple[int, int, int, int]], thickness: int = 2
) -> None:
    """Paint red rectangle outlines on the image (mutates in place).

    The dashboard uses a 1.5 px stroke; we use 2 px here so the outline is
    clearly visible in the small saved PNG and easy to eyeball against the
    bright streaks.
    """
    h, w, _ = img.shape
    for x, y, rw, rh in rects:
        x0, x1 = max(0, x), min(w, x + rw)
        y0, y1 = max(0, y), min(h, y + rh)
        if x0 >= x1 or y0 >= y1:
            continue
        t = max(1, thickness)
        # Top + bottom edges
        img[y0 : min(h, y0 + t), x0:x1] = (255, 60, 60)
        img[max(0, y1 - t) : y1, x0:x1] = (255, 60, 60)
        # Left + right edges
        img[y0:y1, x0 : min(w, x0 + t)] = (255, 60, 60)
        img[y0:y1, max(0, x1 - t) : x1] = (255, 60, 60)


def _run_pipeline(
    iq: np.ndarray,
    bandwidth_hz: int,
    num_fft_bins: int,
    psd_time_resolution_ms: float,
    chunk_samples: int,
    rf_center_hz: int,
    burst_threshold_high_db: float,
) -> tuple[list[np.ndarray], list[float], list[dict], float, float]:
    """Slice ``iq`` into ``chunk_samples`` blocks, run each through
    ``compute_psd_grid``, feed the grid to a rolling burst detector, and
    collect per-chunk waterfall rows + overlay payloads in the same shape
    the live broadcast emits.

    Returns ``(rows, row_times_ms, bursts, freq_low_hz, freq_high_hz)`` where
    rows[0] is the newest (top of waterfall).
    """
    sc16 = iq_to_sc16_int32(iq)
    cfg = PSDGridConfig(num_bins=num_fft_bins, time_resolution_ms=psd_time_resolution_ms)

    detector: RollingBurstDetector | None = None
    rows: list[np.ndarray] = []  # newest-first
    row_times_ms: list[float] = []
    seen: dict[str, dict] = {}
    freq_low_hz: float | None = None
    freq_high_hz: float | None = None

    # Walk the buffer in chunks. Time advances by chunk_duration each step.
    chunk_duration = chunk_samples / bandwidth_hz
    epoch = datetime(2026, 5, 6, tzinfo=timezone.utc)
    for ci, start in enumerate(range(0, len(sc16) - chunk_samples + 1, chunk_samples)):
        chunk_int32 = sc16[start : start + chunk_samples]
        complex_iq = convert_sc16_to_complex(chunk_int32)
        grid = compute_psd_grid(complex_iq, sampling_rate=bandwidth_hz, config=cfg)
        # Per-chunk summary PSD (mean across grid rows) — what the WebUI
        # plots as one waterfall line.
        summary = grid.grid.mean(axis=0)
        rows.insert(0, summary)
        chunk_time = epoch + timedelta(seconds=(ci + 1) * chunk_duration)
        row_times_ms.insert(0, chunk_time.timestamp() * 1000.0)

        if freq_low_hz is None:
            freq_low_hz = float(grid.freq_axis[0]) + rf_center_hz
            freq_high_hz = float(grid.freq_axis[-1]) + rf_center_hz

        if detector is None:
            detector = RollingBurstDetector(
                window_rows=500,
                eval_interval_rows=250,
                num_bins=num_fft_bins,
                burst_config=BurstDetectionConfig(threshold_high_db=burst_threshold_high_db),
                center_freq_hz=float(rf_center_hz),
                freq_axis=grid.freq_axis,
                time_resolution_s=psd_time_resolution_ms / 1000.0,
            )

        detector.feed(grid)
        # Pull current bursts in the same shape streaming.py emits.
        det = detector.last_detection
        if det and det.bursts:
            window_dur = detector._rows_filled * detector._time_resolution_s
            for b in det.bursts:
                real_start = b.start_time - timedelta(seconds=window_dur)
                real_stop = b.stop_time - timedelta(seconds=window_dur)
                # Project the relative-to-now window onto our synthetic clock.
                # Detector's "now" == chunk_time, so timestamps are already
                # consistent with row_times_ms.
                offset = (chunk_time - b.detection_timestamp).total_seconds()
                seen[b.burst_id] = {
                    "id": b.burst_id,
                    "freq_low_hz": float(b.center_freq_hz - b.bandwidth_hz / 2),
                    "freq_high_hz": float(b.center_freq_hz + b.bandwidth_hz / 2),
                    "start_time_ms": (real_start + timedelta(seconds=offset)).timestamp() * 1000.0,
                    "stop_time_ms": (real_stop + timedelta(seconds=offset)).timestamp() * 1000.0,
                }

    bursts = list(seen.values())
    assert freq_low_hz is not None
    assert freq_high_hz is not None
    return rows, row_times_ms, bursts, freq_low_hz, freq_high_hz


@pytest.mark.asyncio
async def test_burst_overlay_aligns_with_streaks(tmp_path: Path) -> None:
    """End-to-end: planted bursts → streaming PSD → overlay rectangles → PNG.

    Verifies (a) at least one detected-burst rectangle lands on top of each
    planted burst's expected position, and (b) the rectangle's interior
    contains bright (high-power) waterfall pixels — i.e. the box covers the
    streak rather than sitting next to it.
    """
    bandwidth = 1_000_000
    rf_center = 915_000_000
    num_fft_bins = 256
    psd_time_res_ms = 0.5
    chunk_samples = 3840  # matches default streaming chunk size at 1 MHz / 256 / 0.5
    # Total duration is sized so chunks * chunk_duration ~ WF_HEIGHT * chunk_duration
    # — keeps every planted burst on the visible waterfall.
    duration_sec = 0.95

    # Three well-separated bursts inside the visible 1-second window.
    planted = [
        Burst(start_sec=0.15, duration_sec=0.05, freq_offset_hz=bandwidth * 0.20, amplitude=0.20),
        Burst(start_sec=0.45, duration_sec=0.05, freq_offset_hz=-bandwidth * 0.15, amplitude=0.20),
        Burst(start_sec=0.75, duration_sec=0.05, freq_offset_hz=bandwidth * 0.30, amplitude=0.20),
    ]
    iq = make_iq_with_bursts(duration_sec=duration_sec, bandwidth_hz=bandwidth, bursts=planted)

    rows, row_times_ms, bursts, freq_low_hz, freq_high_hz = _run_pipeline(
        iq=iq,
        bandwidth_hz=bandwidth,
        num_fft_bins=num_fft_bins,
        psd_time_resolution_ms=psd_time_res_ms,
        chunk_samples=chunk_samples,
        rf_center_hz=rf_center,
        burst_threshold_high_db=25.0,
    )

    # Dynamic range mirroring updateDynamicRange() — use percentiles of the
    # newest row's data.
    all_powers = np.concatenate(rows[: min(len(rows), WF_HEIGHT)])
    vmin = float(np.percentile(all_powers, 5))
    vmax = float(np.percentile(all_powers, 99))

    rects = []
    for b in bursts:
        rect = _overlay_burst_rect(b, row_times_ms, freq_low_hz, freq_high_hz)
        if rect is not None:
            rects.append(rect)

    img = _render_waterfall(rows, vmin=vmin, vmax=vmax)
    _draw_overlay(img, rects)

    out_path = Path(f"/tmp/rfobs_burst_overlay_{bandwidth // 1_000_000}mhz.png")
    from PIL import Image

    Image.fromarray(img).save(out_path)
    print(f"\nrendered: {out_path}")

    assert rects, "no detected-burst rectangles to draw"

    # Per-burst alignment check: for each planted burst, find a rectangle
    # whose center-x is within tolerance of the expected x and whose
    # interior contains at least one bright (above-mid) pixel.
    span = freq_high_hz - freq_low_hz
    bright_threshold = vmin + (vmax - vmin) * 0.5
    misses = []
    for p in planted:
        expected_freq_hz = rf_center + p.freq_offset_hz
        expected_xc = (expected_freq_hz - freq_low_hz) / span * WF_WIDTH
        # Find a rectangle whose center-x is within ~30 px of expected.
        candidates = [r for r in rects if abs((r[0] + r[2] / 2) - expected_xc) < 30]
        if not candidates:
            misses.append(f"no rect near x={expected_xc:.1f} for {p.freq_offset_hz / 1e3:+.1f} kHz")
            continue
        # At least one candidate's interior should have a bright pixel.
        any_bright = False
        for x, y, w, h in candidates:
            x0, x1 = max(0, x), min(WF_WIDTH, x + w)
            y0, y1 = max(0, y), min(WF_HEIGHT, y + h)
            if x0 >= x1 or y0 >= y1:
                continue
            # Reconstruct interior power values from rows
            interior_powers = []
            for ry in range(y0, y1):
                if ry < len(rows):
                    n_bins = len(rows[ry])
                    bin_lo = x0 * n_bins // WF_WIDTH
                    bin_hi = max(bin_lo + 1, x1 * n_bins // WF_WIDTH)
                    interior_powers.extend(rows[ry][bin_lo:bin_hi])
            if interior_powers and max(interior_powers) >= bright_threshold:
                any_bright = True
                break
        if not any_bright:
            misses.append(
                f"rect at x~{expected_xc:.0f} for {p.freq_offset_hz / 1e3:+.1f} kHz "
                f"has no pixel above {bright_threshold:.1f} dB"
            )

    assert not misses, "burst overlay misalignment:\n  " + "\n  ".join(misses)
