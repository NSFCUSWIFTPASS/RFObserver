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
