import numpy as np
import pytest

from rfobserver.processing.channelize import (
    TIER_BANDWIDTH_HZ,
    channelize,
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


def _resample_poly_reference(iq, fs, offset_hz, target):
    """The time-domain channelizer the FFT one replaced: mix to DC, then
    resample_poly (its polyphase FIR is the low-pass). Kept here only to
    compare against."""
    from scipy import signal as sig

    n = np.arange(len(iq))
    mixer = np.exp(-2j * np.pi * (offset_hz / fs) * n).astype(np.complex64)
    up, down = resample_ratio(int(fs), int(target))
    return sig.resample_poly(iq.astype(np.complex64) * mixer, up, down)


def _fsk(fs, offset_hz, n, seed=3):
    # +-50 kHz FSK at 20 kbaud around offset_hz, plus a little noise.
    rng = np.random.default_rng(seed)
    per = int(fs / 20_000)
    sym = rng.choice([-1.0, 1.0], n // per + 1).repeat(per)[:n]
    phase = 2 * np.pi * np.cumsum(offset_hz + 50e3 * sym) / fs
    noise = 0.01 * (rng.standard_normal(n) + 1j * rng.standard_normal(n))
    return (np.exp(1j * phase) + noise).astype(np.complex64)


@pytest.mark.parametrize(
    ("fs", "target", "offset"),
    [
        (26e6, 1_600_000, 1_234_567.0),
        (26e6, 1_000_000, -12_700_000.0),  # the kept band wraps past -fs/2
        (56e6, 1_600_000, 20_000_123.0),
        (2e6, 1_600_000, 100_000.0),
    ],
)
def test_fft_channelizer_matches_the_resample_poly_path_in_band(fs, target, offset):
    # The two low-pass filters differ (a Kaiser FIR against a tapered
    # brick wall), so the old <= 2 LSB bitwise comparison cannot hold. For a
    # signal well inside the band both are flat: the outputs agree to better
    # than -40 dB (measured -53 to -59 dB), edges aside.
    n = 1_300_000 if fs > 2e6 else 100_000
    iq = _fsk(fs, offset, n)
    ref = _resample_poly_reference(iq, fs, offset, target)
    got = channelize(iq, fs, offset, target)
    assert got.dtype == np.complex64 and got.shape == ref.shape
    e = 200
    err = np.linalg.norm(got[e:-e] - ref[e:-e]) / np.linalg.norm(ref[e:-e])
    assert 20 * np.log10(err) < -40


def _tone_spectrum_db(y):
    w = np.blackman(len(y))
    spec = np.abs(np.fft.fft(y * w))
    return 20 * np.log10(spec / spec.max() + 1e-30)


@pytest.mark.parametrize(
    ("fs", "target", "offset"),
    [
        (26e6, 1_600_000, 1_234_567.0),  # not on any FFT bin
        (26e6, 1_600_000, -3_300_001.0),
        (26e6, 1_000_000, 12_950_000.0),  # the kept band wraps past +fs/2
        (26_000_007.0, 1_600_000, 3_000_000.0),  # no short block: one whole FFT
        (1e6, 1_600_000, 100_000.0),  # upsampling
    ],
)
def test_a_tone_at_the_offset_lands_on_dc_with_spurs_40_db_down(fs, target, offset):
    n = int(0.05 * fs)  # 50 ms: many overlap-save blocks
    t = np.arange(n)
    tone = np.exp(2j * np.pi * (offset / fs) * t).astype(np.complex64)
    y = channelize(tone, fs, offset, target)
    up, down = resample_ratio(int(fs), int(target))
    assert len(y) == -(-n * up // down)
    y = y[200:-200]  # edge transients
    db = _tone_spectrum_db(y)
    assert int(np.argmax(db)) == 0  # the tone sits on DC
    assert float(db[8:-8].max()) < -40.0  # outside the window's main lobe
    # Phase is continuous across block edges: the tone is a constant.
    assert float(np.std(np.angle(y))) < 0.01


def test_an_out_of_band_tone_does_not_alias_into_the_output():
    # A 1.6 Msps output keeps +-800 kHz. A tone 1.0 MHz from the burst would
    # alias to -600 kHz without the low-pass; it must be 40 dB below an equal
    # in-band tone.
    fs, off = 26e6, 2_000_000.0
    t = np.arange(int(0.02 * fs))
    iq = np.exp(2j * np.pi * (off + 200e3) / fs * t) + np.exp(2j * np.pi * (off + 1.0e6) / fs * t)
    y = channelize(iq.astype(np.complex64), fs, off, 1_600_000)[200:-200]
    db = _tone_spectrum_db(y)
    f = np.fft.fftfreq(len(y), 1 / 1_600_000)
    assert abs(f[int(np.argmax(db))] - 200e3) < 1_000
    alias = np.abs(f + 600e3) < 5_000
    assert float(db[alias].max()) < -40.0


def test_empty_input_gives_empty_output():
    assert channelize_to_cs16(np.zeros(0, dtype=np.complex64), 26e6, 1e6, 1_600_000) == b""


def test_output_is_peak_normalized_int16():
    iq = _fsk(26e6, 500e3, 260_000)
    v = np.frombuffer(channelize_to_cs16(iq, 26e6, 500e3, 1_600_000), dtype="<i2")
    mag = np.abs(v[0::2].astype(np.float64) + 1j * v[1::2])
    assert 29_990 <= float(mag.max()) <= 30_000  # peak |I + jQ| is 30000, truncated
