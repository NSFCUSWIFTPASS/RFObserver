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

    @property
    def threshold_low_db(self) -> float:
        return self.threshold_high_db * self.threshold_low_ratio


@dataclass
class BurstDetectionResult:
    bursts: list[BurstFingerprint] = field(default_factory=list)
    noise_floor_db: float = 0.0
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
    1. Estimate noise floor as 10th percentile per frequency bin.
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
    noise_floor = compute_noise_floor(grid)
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
        return BurstDetectionResult(noise_floor_db=avg_noise)

    raw_bursts = _extract_fingerprints(
        labeled,
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
        num_raw_detections=num_raw,
        num_merged=num_raw - len(raw_bursts),
    )


def _extract_fingerprints(
    labeled: np.ndarray,
    valid_labels: np.ndarray,
    grid: np.ndarray,
    time_axis: np.ndarray,
    freq_axis: np.ndarray,
    center_freq_hz: float,
    capture_time: datetime,
    min_duration_sec: float,
) -> list[BurstFingerprint]:
    """Extract five-parameter fingerprints from labeled regions."""
    bursts: list[BurstFingerprint] = []

    for label_id in valid_labels:
        rows, cols = np.where(labeled == label_id)
        if len(rows) == 0:
            continue

        # Time bounds
        start_row, end_row = int(rows.min()), int(rows.max())
        t_start = float(time_axis[start_row])
        t_end = float(time_axis[min(end_row + 1, len(time_axis) - 1)])

        duration_sec = t_end - t_start
        if duration_sec < min_duration_sec:
            continue

        # Frequency bounds
        min_col, max_col = int(cols.min()), int(cols.max())
        f_min = float(freq_axis[min_col])
        f_max = float(freq_axis[max_col])
        bandwidth = f_max - f_min

        # Peak power and center frequency
        region_powers = grid[rows, cols]
        peak_idx = int(np.argmax(region_powers))
        peak_power = float(region_powers[peak_idx])
        peak_freq = float(freq_axis[cols[peak_idx]])

        burst_center_freq = center_freq_hz + peak_freq

        bursts.append(
            BurstFingerprint(
                start_time=capture_time + timedelta(seconds=t_start),
                stop_time=capture_time + timedelta(seconds=t_end),
                center_freq_hz=burst_center_freq,
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
            new_duration = (new_stop - current.start_time).total_seconds() * 1000.0

            current = BurstFingerprint(
                burst_id=current.burst_id,
                start_time=current.start_time,
                stop_time=new_stop,
                center_freq_hz=new_center,
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
