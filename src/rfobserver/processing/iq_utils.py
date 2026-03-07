"""SC16 conversion and IQ power statistics.

Ported from rf_processor.iq_utils with rf-shared models vendored into rfobserver.models.
"""

from __future__ import annotations

import numpy as np

from rfobserver.models import IQStatistics


def convert_bytes_to_complex(iq_data_bytes: bytes) -> np.ndarray:
    """Convert raw SC16 (interleaved int16 I/Q) bytes to complex64 numpy array.

    Normalizes to [-1, 1] range by dividing by 32768.
    """
    raw = np.frombuffer(iq_data_bytes, dtype=np.int16).astype(np.float32)
    raw *= 1.0 / 32768.0
    pairs = raw.reshape(-1, 2)
    return pairs[:, 0] + 1j * pairs[:, 1]


def calculate_iq_statistics(data: np.ndarray) -> IQStatistics:
    """Compute power statistics from complex IQ data.

    Power is calculated assuming 50-ohm impedance: P = |z|^2 / 50.
    Median is approximated from a subsample to avoid O(n log n) sort on 13M elements.
    """
    # Compute |z|^2 once -- avoids sqrt from np.abs then squaring again
    power_sq = data.real**2 + data.imag**2  # |z|^2
    power = power_sq * (1.0 / 50.0)

    mean_db = float(10.0 * np.log10(np.mean(power)))
    max_db = float(10.0 * np.log10(np.max(power)))

    # Approximate median from subsample (1/64 of data) -- 50x faster than full sort
    step = max(1, len(power) // (1 << 16))  # ~65K samples
    median_db = float(10.0 * np.log10(np.median(power[::step])))

    variance = np.mean(power_sq) - np.mean(data.real) ** 2 - np.mean(data.imag) ** 2
    standard_dev = float(np.sqrt(variance))

    # Spectral kurtosis estimator: k = M * S2/S1^2 - 1, scaled by (M+1)/(M-1)
    m = len(power_sq)
    s1 = np.sum(power_sq)
    s2 = float(np.dot(power_sq, power_sq))  # dot avoids allocating power_sq^2
    k = m * s2 / (float(s1) ** 2) - 1.0
    spec_kurtosis = float(k * (m + 1.0) / (m - 1.0))

    return IQStatistics(
        average=mean_db,
        max=max_db,
        median=median_db,
        std=standard_dev,
        kurtosis=spec_kurtosis,
    )
