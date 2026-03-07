"""Tests for rfobserver.processing.iq_utils."""

import numpy as np
import pytest

from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_bytes_to_complex


def test_convert_bytes_to_complex_basic():
    # Two complex samples: (1000+2000j), (3000+4000j) normalized by 32768
    data = np.array([1000, 2000, 3000, 4000], dtype=np.int16).tobytes()
    result = convert_bytes_to_complex(data)
    assert result.dtype == np.complex64
    assert len(result) == 2
    np.testing.assert_allclose(result[0].real, 1000 / 32768.0, rtol=1e-5)
    np.testing.assert_allclose(result[0].imag, 2000 / 32768.0, rtol=1e-5)


def test_convert_bytes_to_complex_zeros():
    data = np.zeros(8, dtype=np.int16).tobytes()
    result = convert_bytes_to_complex(data)
    assert len(result) == 4
    assert np.all(result == 0)


def test_convert_bytes_to_complex_roundtrip():
    rng = np.random.default_rng(42)
    original = rng.integers(-32768, 32767, size=200, dtype=np.int16)
    result = convert_bytes_to_complex(original.tobytes())
    assert len(result) == 100


def test_calculate_iq_statistics_shape():
    rng = np.random.default_rng(42)
    data = (rng.standard_normal(1000) + 1j * rng.standard_normal(1000)).astype(np.complex64)
    stats = calculate_iq_statistics(data)
    assert isinstance(stats.average, float)
    assert isinstance(stats.max, float)
    assert isinstance(stats.median, float)
    assert isinstance(stats.std, float)
    assert isinstance(stats.kurtosis, float)


def test_calculate_iq_statistics_max_ge_avg():
    rng = np.random.default_rng(42)
    data = (rng.standard_normal(500) + 1j * rng.standard_normal(500)).astype(np.complex64)
    stats = calculate_iq_statistics(data)
    assert stats.max >= stats.average


def test_calculate_iq_statistics_gaussian_kurtosis():
    """Gaussian data should have spectral kurtosis near 1.0."""
    rng = np.random.default_rng(42)
    data = (rng.standard_normal(10000) + 1j * rng.standard_normal(10000)).astype(np.complex64)
    stats = calculate_iq_statistics(data)
    # Spectral kurtosis of Gaussian should be close to 1.0
    assert 0.5 < stats.kurtosis < 2.0


def test_calculate_iq_statistics_deterministic():
    """Same input should produce same output."""
    data = np.array([0.5 + 0.5j, 0.1 + 0.2j, -0.3 + 0.4j], dtype=np.complex64)
    stats1 = calculate_iq_statistics(data)
    stats2 = calculate_iq_statistics(data)
    assert stats1.average == stats2.average
    assert stats1.kurtosis == stats2.kurtosis
