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
    data_int16 = np.frombuffer(iq_data_bytes, dtype=np.int16)
    data_float = data_int16.astype(np.float32) / 32768.0
    return data_float[0::2] + 1j * data_float[1::2]


def calculate_iq_statistics(data: np.ndarray) -> IQStatistics:
    """Compute power statistics from complex IQ data.

    Power is calculated assuming 50-ohm impedance: P = |z|^2 / 50.
    Spectral kurtosis uses the normalized estimator: k * (m+1)/(m-1).
    """
    power = np.abs(data) ** 2 / 50.0

    mean_db = float(10.0 * np.log10(np.mean(power)))
    max_db = float(10.0 * np.log10(np.max(power)))
    median_db = float(10.0 * np.log10(np.median(power)))
    standard_dev = float(np.std(np.abs(data)))

    # Spectral kurtosis estimator
    dataset = np.abs(data) ** 2
    m = len(dataset)
    s1 = np.sum(dataset)
    s2 = np.sum(dataset**2)
    k = m * s2 / s1**2 - 1.0
    spec_kurtosis = float(k * (m + 1.0) / (m - 1.0))

    return IQStatistics(
        average=mean_db,
        max=max_db,
        median=median_db,
        std=standard_dev,
        kurtosis=spec_kurtosis,
    )
