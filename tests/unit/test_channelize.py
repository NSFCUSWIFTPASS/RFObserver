import numpy as np

from rfobserver.processing.channelize import (
    TIER_BANDWIDTH_HZ,
    channelize_to_cs16,
    resample_ratio,
    select_rate_and_protocols,
)


def test_resample_ratio_reduces():
    assert resample_ratio(26_000_000, 1_600_000) == (4, 65)
    assert resample_ratio(56_000_000, 1_600_000) == (1, 35)


def test_channelize_shifts_offset_to_dc():
    # A pure tone at +300 kHz in a 26 Msps band must land at DC after channelizing
    # with offset_hz=300k, so its energy concentrates in the lowest FFT bins.
    fs = 26_000_000
    n = 260_000  # 10 ms
    t = np.arange(n)
    tone = np.exp(2j * np.pi * (300_000 / fs) * t).astype(np.complex64)
    cs16 = channelize_to_cs16(tone, fs, 300_000.0, 1_600_000)
    iq = np.frombuffer(cs16, dtype="<i2").astype(np.float32)
    ch = iq[0::2] + 1j * iq[1::2]
    spec = np.abs(np.fft.fftshift(np.fft.fft(ch)))
    peak = int(np.argmax(spec))
    center = len(spec) // 2
    assert abs(peak - center) <= 2  # peak sits at DC (center bin)


def test_channelize_output_is_interleaved_int16_at_target_len():
    fs = 26_000_000
    iq = np.ones(26_000, dtype=np.complex64)  # 1 ms
    cs16 = channelize_to_cs16(iq, fs, 0.0, 1_600_000)
    arr = np.frombuffer(cs16, dtype="<i2")
    assert arr.dtype == np.dtype("<i2")
    assert len(arr) % 2 == 0
    # 1 ms at 1.6 Msps ~= 1600 complex samples => ~3200 int16 (allow FIR edge slack)
    assert 3000 <= len(arr) <= 3400


def test_policy_two_tier_by_bandwidth():
    rate_wide, passes_wide = select_rate_and_protocols(TIER_BANDWIDTH_HZ + 1)
    assert rate_wide == 1_600_000
    assert ["-R", "383"] in passes_wide
    rate_narrow, passes_narrow = select_rate_and_protocols(100_000)
    assert rate_narrow == 1_000_000
    assert passes_narrow == [[]]  # one pass, full default set


def _unblocked_reference(iq, fs, offset_hz, target):
    # The mixer as it was before blocking: one int64 n, one complex128 exp.
    from scipy import signal as sig

    n = np.arange(len(iq))
    mixer = np.exp(-2j * np.pi * (offset_hz / fs) * n).astype(np.complex64)
    up, down = resample_ratio(int(fs), int(target))
    res = sig.resample_poly(iq.astype(np.complex64) * mixer, up, down)
    scaled = res * (30000.0 / float(np.max(np.abs(res))))
    out = np.empty(len(res) * 2, dtype="<i2")
    out[0::2] = scaled.real.astype("<i2")
    out[1::2] = scaled.imag.astype("<i2")
    return out


def test_blocked_mixer_matches_one_big_mix_across_block_edges(monkeypatch):
    from rfobserver.processing import channelize

    monkeypatch.setattr(channelize, "MIX_BLOCK_SAMPLES", 10_007)  # many uneven blocks
    fs = 26_000_000
    n = 260_000
    t = np.arange(n)
    tone = np.exp(2j * np.pi * (1_234_567 / fs) * t).astype(np.complex64)
    mixed = channelize.mix_to_dc(tone, fs, 1_234_567.0)
    assert mixed.dtype == np.complex64 and mixed.shape == (n,)
    ref = tone * np.exp(-2j * np.pi * (1_234_567 / fs) * t)
    assert float(np.max(np.abs(mixed - ref))) < 1e-5
    # Phase is continuous across block edges: the tone lands exactly on DC.
    assert float(np.max(np.abs(mixed - 1.0))) < 1e-5


def test_blocked_channelize_matches_the_unblocked_output(monkeypatch):
    from rfobserver.processing import channelize

    monkeypatch.setattr(channelize, "MIX_BLOCK_SAMPLES", 50_000)
    fs = 26_000_000
    rng = np.random.default_rng(1)
    n = 520_000
    t = np.arange(n)
    iq = (
        0.5 * np.exp(2j * np.pi * (-4_400_000 / fs) * t)
        + 0.01 * (rng.standard_normal(n) + 1j * rng.standard_normal(n))
    ).astype(np.complex64)
    got = np.frombuffer(channelize_to_cs16(iq, fs, -4_400_000.0, 1_600_000), dtype="<i2")
    ref = _unblocked_reference(iq, fs, -4_400_000.0, 1_600_000)
    assert got.shape == ref.shape
    assert int(np.max(np.abs(got.astype(np.int32) - ref.astype(np.int32)))) <= 2
