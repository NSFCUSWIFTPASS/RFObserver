"""SC16 conversion and IQ power statistics.

Ported from rf_processor.iq_utils with rf-shared models vendored into rfobserver.models.
"""

from __future__ import annotations

import numpy as np

from rfobserver.models import IQStatistics


def convert_sc16_to_complex(sc16_data: np.ndarray, out: np.ndarray | None = None) -> np.ndarray:
    """Convert SC16 int32 array to complex64, normalizing to [-1, 1].

    Each int32 element packs two int16 values (I in low 16 bits, Q in high 16).
    This avoids a bytes round-trip when working with numpy arrays directly.

    Pass a pre-allocated *out* buffer (complex64) to avoid per-call allocation.
    """
    raw16 = sc16_data.view(np.int16).reshape(-1, 2)
    n = raw16.shape[0]
    if out is None or len(out) != n:
        out = np.empty(n, dtype=np.complex64)
    out.real = raw16[:, 0]
    out.imag = raw16[:, 1]
    out *= np.float32(1.0 / 32768.0)
    return out


def convert_bytes_to_complex(iq_data_bytes: bytes) -> np.ndarray:
    """Convert raw SC16 (interleaved int16 I/Q) bytes to complex64 numpy array.

    Normalizes to [-1, 1] range by dividing by 32768.
    """
    sc16 = np.frombuffer(iq_data_bytes, dtype=np.int32)
    return convert_sc16_to_complex(sc16)


def calculate_iq_statistics(data: np.ndarray) -> IQStatistics:
    """Compute power statistics from complex IQ data.

    Power is |z|^2 / 50. The /50 is a constant -17 dB offset applied after
    log10 to avoid allocating and writing a second full-length array.

    All statistics are computed on a subsample (~262K samples) to keep this
    function fast (~5ms) without meaningful accuracy loss for monitoring.
    """
    # Subsample for all stats — 262K samples is plenty for monitoring accuracy
    step = max(1, len(data) // (1 << 18))
    sub = data[::step]

    # |z|^2 via abs+square (faster than real**2 + imag**2 due to memory access)
    power_sq = np.abs(sub)
    np.square(power_sq, out=power_sq)

    # dB relative to 50 ohm: 10*log10(|z|^2/50) = 10*log10(|z|^2) - 16.99
    db_offset = -16.989700043360187  # 10*log10(50)

    mean_db = float(10.0 * np.log10(np.mean(power_sq)) + db_offset)
    max_db = float(10.0 * np.log10(np.max(power_sq)) + db_offset)
    median_db = float(10.0 * np.log10(np.median(power_sq)) + db_offset)

    variance = np.mean(power_sq) - np.mean(sub.real) ** 2 - np.mean(sub.imag) ** 2
    standard_dev = float(np.sqrt(max(0.0, variance)))

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
