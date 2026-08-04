"""Tests for the SigMF reader + file-replay receiver."""

from __future__ import annotations

import json

import numpy as np

from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.capture.replay_receiver import FileReplayReceiver
from rfobserver.capture.sigmf_reader import load_raw, load_sigmf, to_sc16_int32


def _write_sigmf(base, iq_complex, datatype, sample_rate, center):
    """Write a minimal SigMF pair; iq_complex is complex64 in [-1, 1]."""
    meta = {
        "global": {
            "core:datatype": datatype,
            "core:sample_rate": sample_rate,
            "core:version": "1.0.0",
        },
        "captures": [{"core:sample_start": 0, "core:frequency": center}],
        "annotations": [],
    }
    base.with_suffix(".sigmf-meta").write_text(json.dumps(meta))
    if datatype == "cf32_le":
        inter = np.empty(iq_complex.size * 2, dtype=np.float32)
        inter[0::2] = iq_complex.real
        inter[1::2] = iq_complex.imag
    elif datatype == "ci16_le":
        inter = np.empty(iq_complex.size * 2, dtype=np.int16)
        inter[0::2] = np.clip(iq_complex.real * 32767, -32768, 32767)
        inter[1::2] = np.clip(iq_complex.imag * 32767, -32768, 32767)
    inter.tofile(base.with_suffix(".sigmf-data"))


def test_load_sigmf_cf32(tmp_path):
    iq = (0.5 * np.exp(1j * np.linspace(0, 10, 1000))).astype(np.complex64)
    base = tmp_path / "cap"
    _write_sigmf(base, iq, "cf32_le", 4_000_000.0, 915_000_000.0)

    cap = load_sigmf(base.with_suffix(".sigmf-data"))
    assert cap.datatype == "cf32_le"
    assert cap.sample_rate_hz == 4_000_000.0
    assert cap.center_freq_hz == 915_000_000.0
    assert cap.num_samples == 1000


def test_ci16_is_zero_copy_sc16(tmp_path):
    iq = (0.3 * np.exp(1j * np.linspace(0, 5, 500))).astype(np.complex64)
    base = tmp_path / "cap16"
    _write_sigmf(base, iq, "ci16_le", 4_000_000.0, 915_000_000.0)
    cap = load_sigmf(base)

    sc16 = to_sc16_int32(cap.raw, cap.datatype)
    assert sc16.dtype == np.int32
    assert sc16.size == cap.num_samples
    # low 16 bits = I, high 16 = Q; recover and compare to what we wrote
    raw16 = sc16.view(np.int16).reshape(-1, 2)
    assert np.array_equal(raw16[:, 0], np.clip(iq.real * 32767, -32768, 32767).astype(np.int16))


def test_load_raw_ci16_no_sidecar(tmp_path):
    """A headerless .dat is loaded with caller-supplied params (no SigMF meta)."""
    iq = (0.3 * np.exp(1j * np.linspace(0, 5, 500))).astype(np.complex64)
    inter = np.empty(iq.size * 2, dtype=np.int16)
    inter[0::2] = np.clip(iq.real * 32767, -32768, 32767)
    inter[1::2] = np.clip(iq.imag * 32767, -32768, 32767)
    path = tmp_path / "capture.dat"
    inter.tofile(path)

    cap = load_raw(path, datatype="ci16_le", sample_rate_hz=26_000_000.0, center_freq_hz=915e6)
    assert cap.datatype == "ci16_le"
    assert cap.sample_rate_hz == 26_000_000.0
    assert cap.center_freq_hz == 915e6
    assert cap.num_samples == 500
    # zero-copy view matches what we wrote
    sc16 = to_sc16_int32(cap.raw, cap.datatype)
    assert np.array_equal(sc16.view(np.int16).reshape(-1, 2)[:, 0], inter[0::2])


def test_replay_receiver_max_samples_caps_stream(tmp_path):
    """max_samples limits how much of the capture is served before drain."""
    iq = (0.4 * np.exp(1j * np.linspace(0, 20, 300))).astype(np.complex64)
    base = tmp_path / "capm"
    _write_sigmf(base, iq, "cf32_le", 2_000_000.0, 0.0)
    cap = load_sigmf(base)

    rx = FileReplayReceiver(
        cap, ReceiverConfig(gain_db=40, bandwidth_hz=2_000_000, duration_sec=0.5), max_samples=100
    )
    rx.initialize()
    out = np.zeros(64, dtype=np.int32)
    rx.recv_chunk(out)  # 0..64 (within the 100 cap)
    assert not rx.exhausted
    rx.recv_chunk(out)  # 64..128 -> crosses the 100-sample cap
    assert rx.exhausted


def test_replay_receiver_reproduces_then_exhausts(tmp_path):
    iq = (0.4 * np.exp(1j * np.linspace(0, 20, 300))).astype(np.complex64)
    base = tmp_path / "capr"
    _write_sigmf(base, iq, "cf32_le", 2_000_000.0, 0.0)
    cap = load_sigmf(base)

    rx = FileReplayReceiver(
        cap, ReceiverConfig(gain_db=40, bandwidth_hz=2_000_000, duration_sec=0.5)
    )
    rx.initialize()
    out = np.zeros(128, dtype=np.int32)

    rx.recv_chunk(out)  # first 128 samples
    expected = to_sc16_int32(cap.raw[: 128 * 2], cap.datatype)
    assert np.array_equal(out, expected)
    assert not rx.exhausted

    # Drain the rest; after the 300 samples it must report exhausted.
    for _ in range(10):
        rx.recv_chunk(out)
    assert rx.exhausted
