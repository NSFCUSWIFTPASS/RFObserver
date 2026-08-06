import time

import numpy as np
import pytest

from rfobserver.processing.iq_utils import (
    HIST_EDGES,
    IQMoments,
    calculate_iq_statistics,
    finalize_moments,
    moments_from_iq,
)


def _refproc(data):
    # verbatim copy of reference_software/rf-processor/src/rf_processor/iq_utils.py
    mean_db = 10 * np.log10(np.mean(np.abs(data) ** 2 / 50))
    max_db = 10 * np.log10(np.max(np.abs(data) ** 2 / 50))
    median_db = 10 * np.log10(np.median(np.abs(data) ** 2 / 50))
    std = np.std(np.abs(data))
    p = np.abs(data) ** 2
    m = len(p)
    s1 = np.sum(p)
    s2 = np.sum(p**2)
    k = m * s2 / s1**2 - 1.0
    kurt = k * (m + 1.0) / (m - 1.0)
    return dict(
        average=float(mean_db),
        max=float(max_db),
        median=float(median_db),
        std=float(std),
        kurtosis=float(kurt),
    )


def _signals():
    rng = np.random.default_rng(0)
    n = 1_000_000
    yield (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64) * 0.05
    t = np.arange(n)
    yield (
        (rng.standard_normal(n) + 1j * rng.standard_normal(n)) * 0.03
        + 0.02 * np.exp(2j * np.pi * 0.11 * t)
    ).astype(np.complex64)


def _full_moments(data):
    mag = np.abs(data).astype(np.float64)
    p = mag * mag
    hist, _ = np.histogram(p, bins=HIST_EDGES)
    return IQMoments(
        n=int(mag.size),
        s_abs=float(mag.sum()),
        s_pow=float(p.sum()),
        s_pow2=float(np.dot(p, p)),
        max_pow=float(p.max()),
        hist=hist.astype("int64"),
    )


def test_finalize_exact_on_full_moments():
    # _refproc computes np.abs() on complex64 input (float32), which accumulates
    # ~1e-6 dB of its own rounding noise over 1M samples independent of our
    # float64 math; tolerances are set just above that reference-side noise floor.
    for iq in _signals():
        ref = _refproc(iq)
        got = finalize_moments(_full_moments(iq))
        assert abs(got.average - ref["average"]) < 5e-6
        assert abs(got.std - ref["std"]) < 1e-6
        assert abs(got.kurtosis - ref["kurtosis"]) / (abs(ref["kurtosis"]) + 1e-9) < 5e-6
        assert abs(got.max - ref["max"]) < 1e-6


@pytest.mark.parametrize("nchunks", [1, 7, 64])
def test_folded_moments_match_refproc(nchunks):
    for iq in _signals():
        ref = _refproc(iq)
        parts = np.array_split(iq, nchunks)
        acc = moments_from_iq(parts[0])
        for pc in parts[1:]:
            acc = acc.add(moments_from_iq(pc))
        got = finalize_moments(acc)
        assert abs(got.average - ref["average"]) < 0.2  # dB, subsample noise
        assert abs(got.std - ref["std"]) / (ref["std"] + 1e-12) < 0.05
        assert abs(got.kurtosis - ref["kurtosis"]) / (abs(ref["kurtosis"]) + 1e-9) < 0.05
        assert abs(got.median - ref["median"]) < 0.1  # dB
        # full-res -> exact, modulo the reference's own float32 abs() noise (see
        # test_finalize_exact_on_full_moments)
        assert abs(got.max - ref["max"]) < 1e-6


def test_calculate_iq_statistics_delegates():
    iq = (
        np.random.default_rng(1).standard_normal(50000)
        + 1j * np.random.default_rng(2).standard_normal(50000)
    ).astype(np.complex64) * 0.1
    a = calculate_iq_statistics(iq)
    b = finalize_moments(moments_from_iq(iq))
    assert (a.average, a.std, a.max, a.kurtosis) == (b.average, b.std, b.max, b.kurtosis)


def test_add_is_order_independent():
    rng = np.random.default_rng(3)
    a = moments_from_iq(
        (rng.standard_normal(1000) + 1j * rng.standard_normal(1000)).astype(np.complex64)
    )
    b = moments_from_iq(
        (rng.standard_normal(2000) + 1j * rng.standard_normal(2000)).astype(np.complex64)
    )
    ab = finalize_moments(a.add(b))
    ba = finalize_moments(b.add(a))
    assert abs(ab.average - ba.average) < 1e-9 and ab.max == ba.max


def test_moments_from_iq_is_cheap():
    rng = np.random.default_rng(0)
    iq = (rng.standard_normal(2_000_000) + 1j * rng.standard_normal(2_000_000)).astype(np.complex64)
    moments_from_iq(iq)  # warm
    t = time.perf_counter()
    for _ in range(5):
        moments_from_iq(iq)
    assert time.perf_counter() - t < 1.0  # 5 chunks < 1s (guards against full-array histogram)
