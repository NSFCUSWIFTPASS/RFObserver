"""Burst detection via connected-component labeling on PSD grids.

Operates directly on the high-resolution PSD grid produced by spectral.py.
Each cell in the grid is an averaged PSD at a specific time slice and
frequency bin. Detection thresholds against the per-bin noise floor,
then groups detections via 8-connectivity CCL and extracts five-parameter
fingerprints.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np
from scipy import ndimage

from rfobserver.models import BurstFingerprint
from rfobserver.processing.spectral import PSDGridResult, compute_noise_floor


@dataclass
class BurstDetectionConfig:
    threshold_high_db: float = 10.0
    threshold_low_ratio: float = 0.6  # T_L = T_H * ratio
    min_duration_sec: float = 0.001
    merge_freq_bins: int = 5
    merge_time_sec: float = 0.003
    noise_floor_percentile: float = 50.0

    @property
    def threshold_low_db(self) -> float:
        return self.threshold_high_db * self.threshold_low_ratio


@dataclass
class BurstDetectionResult:
    bursts: list[BurstFingerprint] = field(default_factory=list)
    noise_floor_db: float = 0.0
    noise_floor_per_bin: np.ndarray | None = None
    num_raw_detections: int = 0
    num_merged: int = 0


def detect_bursts(
    psd_grid: PSDGridResult,
    config: BurstDetectionConfig | None = None,
    center_freq_hz: float = 0.0,
    capture_time: datetime | None = None,
) -> BurstDetectionResult:
    """Detect RF bursts in a PSD grid using dual-threshold hysteresis + CCL.

    Algorithm:
    1. Estimate noise floor per frequency bin at config.noise_floor_percentile
       (default median; a lower percentile like 10 sits below the true floor and
       fragments detections, a strong signal on a broadband pedestal needs the
       median to keep the pedestal sub-threshold).
    2. Create high-threshold mask: cells > noise_floor + T_H.
    3. Create low-threshold mask: cells > noise_floor + T_L.
    4. Label connected components in the low-threshold mask (8-connectivity).
    5. Keep only components that contain at least one high-threshold cell.
    6. Extract five-parameter fingerprint for each valid component.
    7. Merge nearby bursts (within merge_freq_bins and merge_time_sec).
    """
    if config is None:
        config = BurstDetectionConfig()

    if capture_time is None:
        capture_time = datetime.utcnow()

    grid = psd_grid.grid
    time_axis = psd_grid.time_axis
    freq_axis = psd_grid.freq_axis
    noise_floor = compute_noise_floor(grid, config.noise_floor_percentile)
    avg_noise = float(np.mean(noise_floor))

    # Dual-threshold masks
    mask_high = grid > (noise_floor + config.threshold_high_db)
    mask_low = grid > (noise_floor + config.threshold_low_db)

    # 8-connectivity CCL on low-threshold mask
    structure = ndimage.generate_binary_structure(2, 2)
    labeled, num_labels = ndimage.label(mask_low, structure=structure)

    # Find labels that contain at least one high-threshold cell
    valid_labels = np.unique(labeled[mask_high])
    valid_labels = valid_labels[valid_labels > 0]

    if len(valid_labels) == 0:
        return BurstDetectionResult(noise_floor_db=avg_noise, noise_floor_per_bin=noise_floor)

    raw_bursts = _extract_fingerprints(
        labeled,
        mask_low,
        valid_labels,
        grid,
        time_axis,
        freq_axis,
        center_freq_hz,
        capture_time,
        config.min_duration_sec,
    )

    num_raw = len(raw_bursts)

    # Merge nearby bursts
    if len(raw_bursts) > 1:
        freq_bin_width = abs(freq_axis[1] - freq_axis[0]) if len(freq_axis) > 1 else 0
        freq_tolerance = freq_bin_width * config.merge_freq_bins
        raw_bursts = _merge_bursts(raw_bursts, config.merge_time_sec, freq_tolerance)

    return BurstDetectionResult(
        bursts=raw_bursts,
        noise_floor_db=avg_noise,
        noise_floor_per_bin=noise_floor,
        num_raw_detections=num_raw,
        num_merged=num_raw - len(raw_bursts),
    )


def _extract_fingerprints(
    labeled: np.ndarray,
    labeled_mask: np.ndarray,
    valid_labels: np.ndarray,
    grid: np.ndarray,
    time_axis: np.ndarray,
    freq_axis: np.ndarray,
    center_freq_hz: float,
    capture_time: datetime,
    min_duration_sec: float,
) -> list[BurstFingerprint]:
    """Extract five-parameter fingerprints from labeled regions.

    ``labeled_mask`` is the mask ``labeled`` was labeled from (nonzero cells).
    Every region is measured at once from the cells of the valid labels only,
    with numpy reductions. ``ndimage.find_objects`` (one C call over the whole
    grid, building a slice per label, noise labels included) and a per-label
    loop held the GIL for 55 to 68 ms at a time on an Orin Nano and made the
    receiver overflow under heavy burst load; see
    docs/debugging/2026-11-06_overflows-under-burst-load.md.
    """
    flat = labeled.ravel()
    cells = np.flatnonzero(labeled_mask)
    cell_labels = flat[cells]
    lut = np.zeros(int(valid_labels.max()) + 1, dtype=bool)
    lut[valid_labels] = True
    keep = lut[np.minimum(cell_labels, len(lut) - 1)] & (cell_labels < len(lut))
    # Group the cells by label; the stable sort keeps each region's cells in
    # row-major order, so its first maximum is the same peak cell as before.
    order = np.argsort(cell_labels[keep], kind="stable")
    idx = cells[keep][order]
    lbl = cell_labels[keep][order]
    starts = np.flatnonzero(np.r_[True, lbl[1:] != lbl[:-1]])

    n_cols = labeled.shape[1]
    rows = idx // n_cols
    cols = idx % n_cols
    row_min = np.minimum.reduceat(rows, starts)
    row_max = np.maximum.reduceat(rows, starts)
    col_min = np.minimum.reduceat(cols, starts)
    col_max = np.maximum.reduceat(cols, starts)
    powers = grid.ravel()[idx]
    peak = np.maximum.reduceat(powers, starts)
    group = np.repeat(np.arange(len(starts)), np.diff(np.r_[starts, len(idx)]))
    at_peak = np.flatnonzero(powers == peak[group])
    first_peak = at_peak[np.r_[True, group[at_peak][1:] != group[at_peak][:-1]]]

    bursts: list[BurstFingerprint] = []
    last_row = len(time_axis) - 1
    for k in range(len(starts)):
        # Time bounds
        t_start = float(time_axis[row_min[k]])
        t_end = float(time_axis[min(int(row_max[k]) + 1, last_row)])

        duration_sec = t_end - t_start
        if duration_sec < min_duration_sec:
            continue

        # Frequency bounds
        f_min = float(freq_axis[col_min[k]])
        f_max = float(freq_axis[col_max[k]])
        bandwidth = f_max - f_min

        # Peak power; center frequency = geometric middle of (f_min, f_max).
        # The peak's exact bin location is intentionally NOT used for
        # center_freq_hz: visualizations and span queries assume it sits
        # at the geometric center so that center +/- bandwidth/2 reproduces
        # the actual occupied range.
        peak_power = float(peak[k])
        peak_freq_hz = center_freq_hz + float(freq_axis[cols[first_peak[k]]])

        burst_center_freq = center_freq_hz + (f_min + f_max) / 2

        bursts.append(
            BurstFingerprint(
                start_time=capture_time + timedelta(seconds=t_start),
                stop_time=capture_time + timedelta(seconds=t_end),
                center_freq_hz=burst_center_freq,
                peak_freq_hz=peak_freq_hz,
                bandwidth_hz=max(bandwidth, 0.0),
                peak_power_db=peak_power,
                duration_ms=duration_sec * 1000.0,
                detection_timestamp=capture_time,
            )
        )

    bursts.sort(key=lambda b: b.start_time)
    return bursts


def _merge_bursts(
    bursts: list[BurstFingerprint],
    max_time_gap: float,
    freq_tolerance: float,
) -> list[BurstFingerprint]:
    """Merge bursts that are close in frequency and time."""
    if len(bursts) < 2:
        return bursts

    merged: list[BurstFingerprint] = []
    current = bursts[0]

    for next_burst in bursts[1:]:
        curr_f_lo = current.center_freq_hz - current.bandwidth_hz / 2
        curr_f_hi = current.center_freq_hz + current.bandwidth_hz / 2
        next_f_lo = next_burst.center_freq_hz - next_burst.bandwidth_hz / 2
        next_f_hi = next_burst.center_freq_hz + next_burst.bandwidth_hz / 2

        freq_overlap = (
            curr_f_lo - freq_tolerance <= next_f_hi and curr_f_hi + freq_tolerance >= next_f_lo
        )

        time_gap = (next_burst.start_time - current.stop_time).total_seconds()

        if freq_overlap and time_gap <= max_time_gap:
            new_stop = max(current.stop_time, next_burst.stop_time)
            new_f_lo = min(curr_f_lo, next_f_lo)
            new_f_hi = max(curr_f_hi, next_f_hi)
            new_bw = new_f_hi - new_f_lo
            new_center = (new_f_lo + new_f_hi) / 2
            new_peak = max(current.peak_power_db, next_burst.peak_power_db)
            # Carry the peak frequency of the stronger constituent (matches the
            # rolling tracker's rule), not the geometric midpoint.
            if current.peak_power_db >= next_burst.peak_power_db:
                new_peak_freq = current.peak_freq_hz
            else:
                new_peak_freq = next_burst.peak_freq_hz
            new_duration = (new_stop - current.start_time).total_seconds() * 1000.0

            current = BurstFingerprint(
                burst_id=current.burst_id,
                start_time=current.start_time,
                stop_time=new_stop,
                center_freq_hz=new_center,
                peak_freq_hz=new_peak_freq,
                bandwidth_hz=new_bw,
                peak_power_db=new_peak,
                duration_ms=new_duration,
                detection_timestamp=current.detection_timestamp,
            )
        else:
            merged.append(current)
            current = next_burst

    merged.append(current)
    return merged
