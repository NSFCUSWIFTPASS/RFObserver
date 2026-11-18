"""Tone tracker: one PSD bin's power, row by row from the grid, into tone
segments against a rolling floor. Rows are 0.2 ms, as at 26 MS/s; a cell's
noise is that of about 4 averaged periodograms (relative spread 0.5)."""

from __future__ import annotations

import numpy as np
import pytest

from rfobserver.processing.tone_detect import ToneTracker, parse_tone_freqs

ROW = 0.0002
T0 = 1_795_000_000.0


def _rows(seconds: float, rng: np.random.Generator, floor: float = 1e-10) -> np.ndarray:
    """Noise rows: mean of 4 exponential periodograms per cell (linear power)."""
    n = round(seconds / ROW)
    return floor * rng.exponential(size=(n, 4)).mean(axis=1)


def _with_tone(rows: np.ndarray, start: float, dur: float, snr_db: float, floor: float = 1e-10):
    out = rows.copy()
    a, b = round(start / ROW), round((start + dur) / ROW)
    out[a:b] += floor * 10 ** (snr_db / 10)
    return out


def _feed(
    tracker: ToneTracker,
    rows: np.ndarray,
    chunk: int = 197,
    t0: float = T0,
    guard: np.ndarray | None = None,
) -> list:
    """Feed in chunk-sized pieces (197 rows is one 39 ms chunk at 26 MS/s);
    the guard bins are noise unless given."""
    if guard is None:
        guard = _rows(len(rows) * ROW, np.random.default_rng(999))[: len(rows)]
    found = []
    for i in range(0, len(rows), chunk):
        found += tracker.feed(rows[i : i + chunk], guard[i : i + chunk], t0 + i * ROW, ROW)
    return found + tracker.flush()


def test_parse_tone_freqs() -> None:
    assert parse_tone_freqs("915.5e6, 915500000,  ,902000000.5") == [915.5e6, 915.5e6, 902000000.5]
    assert parse_tone_freqs("") == []
    with pytest.raises(ValueError):
        parse_tone_freqs("915 MHz")


def test_a_5_db_tone_is_found_and_measured() -> None:
    rng = np.random.default_rng(1)
    rows = _with_tone(_rows(3.0, rng), 2.0, 0.1, 5.0)
    found = _feed(ToneTracker(threshold_db=3.0), rows)
    assert len(found) == 1
    t = found[0]
    assert abs(t.start - (T0 + 2.0)) < 0.002
    assert abs((t.stop - t.start) - 0.1) < 0.003
    assert abs(t.snr_db - 5.0) < 0.5
    assert abs(t.level_db - (-100.0 + 5.0)) < 0.5


def test_noise_alone_finds_nothing() -> None:
    rng = np.random.default_rng(2)
    assert _feed(ToneTracker(threshold_db=3.0), _rows(60.0, rng)) == []


def test_nothing_is_found_before_the_floor_is_known() -> None:
    rng = np.random.default_rng(3)
    rows = _with_tone(_rows(1.0, rng), 0.1, 0.1, 10.0)
    assert _feed(ToneTracker(threshold_db=3.0), rows) == []


def test_a_tone_of_seconds_across_many_chunks_is_one_tone() -> None:
    rng = np.random.default_rng(4)
    rows = _with_tone(_rows(8.0, rng), 2.0, 3.0, 5.0)
    found = _feed(ToneTracker(threshold_db=3.0), rows)
    assert len(found) == 1
    assert abs((found[0].stop - found[0].start) - 3.0) < 0.003


def test_the_receiver_gain_step_neither_splits_a_tone_nor_moves_its_level() -> None:
    """The B200mini drops 5 dB for 3.4 ms every 102.4 ms."""
    rng = np.random.default_rng(5)
    base = _rows(5.0, rng)
    clean = _with_tone(base, 2.0, 1.0, 15.0)
    stepped = base.copy()
    amp = 1e-10 * 10**1.5
    a, b = round(2.0 / ROW), round(3.0 / ROW)
    tone = np.full(b - a, amp)
    t = np.arange(b - a) * ROW
    tone[(t % 0.1024) < 0.0034] *= 10 ** (-5 / 10)
    stepped[a:b] += tone
    c = _feed(ToneTracker(threshold_db=3.0), clean)
    s = _feed(ToneTracker(threshold_db=3.0), stepped)
    assert len(c) == 1 and len(s) == 1
    assert abs(c[0].level_db - s[0].level_db) < 0.2
    assert abs((s[0].stop - s[0].start) - 1.0) < 0.003


def test_levels_track_transmit_power() -> None:
    """For the linearity check: 10 dB less transmitted reads 10 dB less."""
    rng = np.random.default_rng(6)
    base = _rows(4.0, rng)
    hi = _feed(ToneTracker(threshold_db=3.0), _with_tone(base, 2.0, 0.5, 25.0))
    lo = _feed(ToneTracker(threshold_db=3.0), _with_tone(base, 2.0, 0.5, 15.0))
    assert abs((hi[0].level_db - lo[0].level_db) - 10.0) < 0.2


def test_a_short_blip_is_not_a_tone() -> None:
    rng = np.random.default_rng(7)
    rows = _with_tone(_rows(3.0, rng), 2.0, 0.008, 20.0)
    assert _feed(ToneTracker(threshold_db=3.0), rows) == []


def test_tones_do_not_raise_the_floor() -> None:
    """A tone on half the time for a minute: the floor stays the noise's, so
    every pulse is still found at its level."""
    rng = np.random.default_rng(8)
    rows = _rows(62.0, rng)
    for k in range(60):
        rows = _with_tone(rows, 2.0 + k, 0.5, 6.0)
    found = _feed(ToneTracker(threshold_db=3.0), rows)
    assert len(found) == 60
    assert all(abs(t.snr_db - 6.0) < 0.6 for t in found)


def test_two_pulses_are_two_tones() -> None:
    rng = np.random.default_rng(9)
    rows = _with_tone(_with_tone(_rows(4.0, rng), 2.0, 0.1, 6.0), 2.5, 0.2, 6.0)
    found = _feed(ToneTracker(threshold_db=3.0), rows)
    assert [round(t.start - T0, 2) for t in found] == [2.0, 2.5]
    assert [round(t.stop - t.start, 2) for t in found] == [0.1, 0.2]


def test_a_gap_in_the_rows_ends_a_tone() -> None:
    """Rows that do not follow on in time (a stall, a retune) close an open tone."""
    rng = np.random.default_rng(10)
    tr = ToneTracker(threshold_db=3.0)
    rows = _with_tone(_rows(3.0, rng), 2.5, 0.5, 10.0)
    found = tr.feed(rows, _rows(3.0, rng), T0, ROW)
    found += tr.feed(_with_tone(_rows(0.5, rng), 0.0, 0.5, 10.0), _rows(0.5, rng), T0 + 10.0, ROW)
    found += tr.flush()
    assert len(found) == 2
    assert abs((found[0].stop - found[0].start) - 0.5) < 0.003


def test_broadband_energy_through_the_bin_is_not_a_tone() -> None:
    """Something wideband sweeping through lifts the guard bins (3 bins away)
    as much as the tone's bin; a CW tone leaks under -30 dB there."""
    rng = np.random.default_rng(11)
    rows = _with_tone(_rows(3.0, rng), 2.0, 0.04, 8.0)
    guard = _with_tone(_rows(3.0, rng), 2.0, 0.04, 7.0)
    assert _feed(ToneTracker(threshold_db=3.0), rows, guard=guard) == []


def test_a_strong_tone_leaking_into_the_guard_bins_is_kept() -> None:
    rng = np.random.default_rng(12)
    rows = _with_tone(_rows(3.0, rng), 2.0, 0.5, 50.0)
    guard = _with_tone(_rows(3.0, rng), 2.0, 0.5, 50.0 - 31.0)
    assert len(_feed(ToneTracker(threshold_db=3.0), rows, guard=guard)) == 1
