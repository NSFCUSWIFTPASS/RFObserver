"""Low-level tone detector DSP: stage A flags bins of the averaged PSD that rise
over their own recent level; stage B confirms a tone in a narrow channel cut
from the IQ ring and measures it. Levels here are what a 2048-bin, 26 MS/s
setup gives: 12.7 kHz bins, about 0.04 dB window-to-window noise per bin."""

from __future__ import annotations

import numpy as np

from rfobserver.processing.tone_detect import BinBaseline, confirm_tones

BINS = 2048
NOISE_DB = 0.04
CH_RATE = 50_000
BIN_HZ = 12_695.3


def _windows(n: int, seed: int = 1) -> list[np.ndarray]:
    """Averaged-PSD windows: a fixed ripple (+-0.6 dB) plus per-window noise."""
    rng = np.random.default_rng(seed)
    ripple = -100.0 + 0.6 * np.sin(np.arange(BINS) / 7.0)
    return [ripple + NOISE_DB * rng.normal(size=BINS) for _ in range(n)]


def _warm(b: BinBaseline, key=(915e6, BINS), n: int = 30) -> None:
    for w in _windows(n, seed=99):
        assert b.update(key, w) == []


def test_no_candidates_until_the_baseline_has_history() -> None:
    b = BinBaseline(threshold_db=0.25)
    w = _windows(1)[0]
    w[1000] += 3.0
    assert b.update((915e6, BINS), w) == []


def test_noise_alone_raises_no_candidates() -> None:
    b = BinBaseline(threshold_db=0.25)
    flagged = sum(len(b.update((915e6, BINS), w)) for w in _windows(600, seed=2))
    assert flagged == 0


def test_a_bin_rising_half_a_db_is_a_candidate() -> None:
    """Twenty 100 ms pulses about 5 dB over the bin floor raised their bin by
    0.55 dB in the 2026-11-18 over-the-air test."""
    b = BinBaseline(threshold_db=0.25)
    _warm(b)
    w = _windows(1, seed=3)[0]
    w[1200] += 0.55
    c = b.update((915e6, BINS), w)
    assert len(c) == 1
    assert c[0].lo_bin <= 1200 <= c[0].hi_bin
    assert c[0].peak_bin == 1200
    assert abs(c[0].rise_db - 0.55) < 0.1


def test_a_tone_between_two_bins_is_one_candidate() -> None:
    b = BinBaseline(threshold_db=0.25)
    _warm(b)
    w = _windows(1, seed=4)[0]
    w[700] += 0.6
    w[701] += 0.5
    c = b.update((915e6, BINS), w)
    assert len(c) == 1
    assert (c[0].lo_bin, c[0].hi_bin) == (700, 701)


def test_a_floor_step_is_not_a_tone_and_is_learned() -> None:
    """A gain or temperature step lifts every bin: no candidates, and the
    baseline moves to the new floor."""
    b = BinBaseline(threshold_db=0.25)
    _warm(b)
    for w in _windows(80, seed=5):
        assert b.update((915e6, BINS), w + 1.0) == []
    w = _windows(1, seed=6)[0] + 1.0
    w[300] += 0.6
    assert [c.peak_bin for c in b.update((915e6, BINS), w)] == [300]


def test_a_steady_tone_stays_a_candidate() -> None:
    """Flagged windows do not feed the baseline, so a tone that stays on is
    found in every window instead of becoming the floor."""
    b = BinBaseline(threshold_db=0.25)
    _warm(b)
    for w in _windows(100, seed=7):
        w[500] += 2.0
        assert [c.peak_bin for c in b.update((915e6, BINS), w)] == [500]


def test_a_retune_starts_the_baseline_over() -> None:
    b = BinBaseline(threshold_db=0.25)
    _warm(b, key=(915e6, BINS))
    w = _windows(1, seed=8)[0]
    w[10] += 3.0
    assert b.update((925e6, BINS), w) == []


def test_candidates_are_capped_strongest_first() -> None:
    b = BinBaseline(threshold_db=0.25, max_candidates=2)
    _warm(b)
    w = _windows(1, seed=9)[0]
    for k, rise in ((100, 0.5), (400, 2.0), (900, 1.0)):
        w[k] += rise
    assert [c.peak_bin for c in b.update((915e6, BINS), w)] == [400, 900]


def test_a_noisy_bin_needs_a_larger_rise() -> None:
    """A bin that wanders (a nearby intermittent signal) must rise well over
    its own spread, not just the threshold."""
    b = BinBaseline(threshold_db=0.25)
    rng = np.random.default_rng(10)
    for w in _windows(40, seed=11):
        w[50] += 0.5 * rng.normal()
        b.update((915e6, BINS), w)
    w = _windows(1, seed=12)[0]
    w[50] += 0.6
    assert b.update((915e6, BINS), w) == []


# -- Stage B --------------------------------------------------------------


def _noise(n: int, rng: np.random.Generator, power: float = 1.0) -> np.ndarray:
    return np.sqrt(power / 2) * (rng.normal(size=n) + 1j * rng.normal(size=n))


def _channel(
    seconds: float = 1.0,
    pulses: tuple[tuple[float, float], ...] = ((0.4, 0.1),),
    offset_hz: float = 3_000.0,
    bin_snr_db: float = 5.0,
    seed: int = 1,
    noise_power: float = 1e-6,
) -> np.ndarray:
    """A 50 kHz channel: noise, plus a tone ``bin_snr_db`` over the noise in a
    12.7 kHz PSD bin, on for each (start s, duration s) in ``pulses``."""
    rng = np.random.default_rng(seed)
    n = int(seconds * CH_RATE)
    x = _noise(n, rng, noise_power)
    amp = np.sqrt(noise_power * BIN_HZ / CH_RATE * 10 ** (bin_snr_db / 10))
    t = np.arange(n) / CH_RATE
    for start, dur in pulses:
        on = (t >= start) & (t < start + dur)
        x[on] += amp * np.exp(2j * np.pi * offset_hz * t[on])
    return x.astype(np.complex64)


def test_a_5_db_tone_is_confirmed_and_measured() -> None:
    segs = confirm_tones(_channel(), CH_RATE, bin_hz=BIN_HZ)
    assert len(segs) == 1
    s = segs[0]
    assert abs(s.offset_hz - 3_000.0) < 2.0
    assert abs(s.start_s - 0.4) < 0.003
    assert abs(s.duration_s - 0.1) < 0.005
    assert abs(s.snr_db - 5.0) < 1.5
    assert not s.touches_start and not s.touches_end


def test_noise_alone_confirms_nothing() -> None:
    rng = np.random.default_rng(3)
    for seed in range(5):
        x = _noise(CH_RATE, np.random.default_rng(seed + 10)).astype(np.complex64)
        assert confirm_tones(x, CH_RATE, bin_hz=BIN_HZ) == []
    assert rng is not None


def test_a_modulated_signal_is_not_a_tone() -> None:
    """FSK at the same power as a 15 dB tone: no single line."""
    rng = np.random.default_rng(4)
    n = CH_RATE
    bits = rng.integers(0, 2, size=n // 50).repeat(50)[:n] * 2 - 1
    fsk = np.exp(2j * np.pi * np.cumsum(4_000.0 * bits) / CH_RATE)
    amp = np.sqrt(1e-6 * BIN_HZ / CH_RATE * 10**1.5)
    x = (_noise(n, rng, 1e-6) + amp * fsk).astype(np.complex64)
    assert confirm_tones(x, CH_RATE, bin_hz=BIN_HZ) == []


def test_the_receiver_gain_step_does_not_split_or_move_the_level() -> None:
    """The B200mini's 3.4 ms, 5 dB step inside a 1 s tone: one segment, and
    the level within 0.2 dB of the same tone without the step."""
    clean = _channel(pulses=((0.1, 0.8),), bin_snr_db=15.0, seed=5)
    stepped = clean.copy()
    for t0 in (0.3, 0.4024, 0.5048):
        a = int(t0 * CH_RATE)
        stepped[a : a + int(0.0034 * CH_RATE)] *= 0.56
    a = confirm_tones(clean, CH_RATE, bin_hz=BIN_HZ)
    b = confirm_tones(stepped, CH_RATE, bin_hz=BIN_HZ)
    assert len(a) == 1 and len(b) == 1
    assert abs(a[0].level_db - b[0].level_db) < 0.2
    assert abs(b[0].duration_s - 0.8) < 0.005


def test_levels_track_transmit_power() -> None:
    """For a linearity check: 10 dB less transmitted reads 10 dB less."""
    hi = confirm_tones(_channel(bin_snr_db=25.0, seed=6), CH_RATE, bin_hz=BIN_HZ)
    lo = confirm_tones(_channel(bin_snr_db=15.0, seed=6), CH_RATE, bin_hz=BIN_HZ)
    assert abs((hi[0].level_db - lo[0].level_db) - 10.0) < 0.3


def test_a_tone_on_at_both_edges_says_so() -> None:
    segs = confirm_tones(_channel(pulses=((0.0, 1.0),), seed=7), CH_RATE, bin_hz=BIN_HZ)
    assert len(segs) == 1
    assert segs[0].touches_start and segs[0].touches_end


def test_two_pulses_are_two_segments() -> None:
    x = _channel(pulses=((0.1, 0.1), (0.6, 0.15)), seed=8)
    segs = confirm_tones(x, CH_RATE, bin_hz=BIN_HZ)
    assert [round(s.start_s, 2) for s in segs] == [0.1, 0.6]
    assert [round(s.duration_s, 2) for s in segs] == [0.1, 0.15]


def test_a_short_blip_is_not_a_tone() -> None:
    x = _channel(pulses=((0.5, 0.008),), bin_snr_db=20.0, seed=9)
    assert confirm_tones(x, CH_RATE, bin_hz=BIN_HZ) == []
