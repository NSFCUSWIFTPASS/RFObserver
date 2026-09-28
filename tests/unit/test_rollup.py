"""Tests for the per-minute rollup arithmetic and peak selection."""

from datetime import datetime, timedelta, timezone

from rfobserver.storage.rollup import (
    Candidate,
    WindowRow,
    fold_windows,
    minute_key,
    select_peaks,
)

UTC = timezone.utc


def _w(ts: str, pwr_max: float, pwr_median: float, pwr_avg: float, center: float = 2.437e9):
    return WindowRow(
        start_time=ts,
        sdr_center_freq_hz=center,
        sample_rate_hz=56e6,
        gain_db=40.0,
        pwr_max=pwr_max,
        pwr_median=pwr_median,
        pwr_avg=pwr_avg,
    )


def test_minute_key_truncates_to_the_minute():
    assert minute_key("2026-09-19T03:14:22.108192+00:00") == "2026-09-19T03:14"
    # isoformat() omits microseconds when they are zero; the key must not shift.
    assert minute_key("2026-09-19T03:14:22+00:00") == "2026-09-19T03:14"


def test_fold_takes_the_maximum_of_each_metric_with_its_own_timestamp():
    rows = [
        _w("2026-09-19T03:14:01+00:00", pwr_max=-30.0, pwr_median=-50.0, pwr_avg=-45.0),
        # highest pwr_max, but a smaller rise above its own noise floor
        _w("2026-09-19T03:14:30+00:00", pwr_max=-20.0, pwr_median=-25.0, pwr_avg=-44.0),
        # highest pwr_snr (40 dB) and highest pwr_avg
        _w("2026-09-19T03:14:59+00:00", pwr_max=-22.0, pwr_median=-62.0, pwr_avg=-40.0),
    ]
    (s,) = fold_windows(rows)
    assert s.minute_start == "2026-09-19T03:14"
    assert s.n == 3
    assert s.pwr_max == -20.0
    assert s.peak_max_time == "2026-09-19T03:14:30+00:00"
    assert s.pwr_snr == 40.0
    assert s.peak_snr_time == "2026-09-19T03:14:59+00:00"
    assert s.pwr_avg == -40.0
    assert s.peak_avg_time == "2026-09-19T03:14:59+00:00"


def test_fold_separates_minutes_and_centre_frequencies():
    rows = [
        _w("2026-09-19T03:14:01+00:00", -30.0, -50.0, -45.0, center=2.437e9),
        _w("2026-09-19T03:15:01+00:00", -31.0, -50.0, -45.0, center=2.437e9),
        _w("2026-09-19T03:14:02+00:00", -32.0, -50.0, -45.0, center=5.8e9),
    ]
    out = {(s.minute_start, s.sdr_center_freq_hz) for s in fold_windows(rows)}
    assert out == {
        ("2026-09-19T03:14", 2.437e9),
        ("2026-09-19T03:15", 2.437e9),
        ("2026-09-19T03:14", 5.8e9),
    }


def test_fold_tolerates_missing_statistics():
    # A window with no pwr_median cannot contribute a pwr_snr, but still counts.
    rows = [
        _w("2026-09-19T03:14:01+00:00", -30.0, -50.0, -45.0),
        WindowRow("2026-09-19T03:14:02+00:00", 2.437e9, 56e6, 40.0, -10.0, None, None),
    ]
    (s,) = fold_windows(rows)
    assert s.n == 2
    assert s.pwr_max == -10.0
    assert s.peak_max_time == "2026-09-19T03:14:02+00:00"
    assert s.pwr_snr == 20.0
    assert s.peak_snr_time == "2026-09-19T03:14:01+00:00"


def test_fold_returns_nothing_for_no_rows():
    assert fold_windows([]) == []


def _c(minutes_ago: float, value: float, now: datetime) -> Candidate:
    t = now - timedelta(minutes=minutes_ago)
    return Candidate(peak_time=t, value=value, pwr_max=value, pwr_snr=30.0, pwr_avg=-45.0)


def test_select_rejects_peaks_closer_together_than_the_window():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    # Ten candidates inside one minute: one real event, not ten peaks.
    candidates = [_c(600 + i * 0.1, -20.0 - i, now) for i in range(10)]
    peaks = select_peaks(candidates, window_sec=1800, count=10, now=now)
    assert len(peaks) == 1
    assert peaks[0].rank == 1


def test_select_returns_separated_peaks_in_descending_order():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    candidates = [
        _c(60, -25.0, now),
        _c(600, -20.0, now),  # strongest
        _c(1200, -30.0, now),
    ]
    peaks = select_peaks(candidates, window_sec=1800, count=10, now=now)
    assert [p.rank for p in peaks] == [1, 2, 3]
    assert [p.value for p in peaks] == [-20.0, -25.0, -30.0]


def test_select_centres_the_window_on_the_peak():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    (peak,) = select_peaks([_c(600, -20.0, now)], window_sec=1800, count=1, now=now)
    assert peak.since == peak.peak_time - timedelta(seconds=900)
    assert peak.until == peak.peak_time + timedelta(seconds=900)


def test_select_clamps_the_window_to_now():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    # A peak one minute old cannot have fifteen minutes of future in its window.
    (peak,) = select_peaks([_c(1, -20.0, now)], window_sec=1800, count=1, now=now)
    assert peak.until == now
    assert peak.since == peak.peak_time - timedelta(seconds=900)


def test_select_stops_at_count():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    candidates = [_c(60 * (i + 1), -20.0 - i, now) for i in range(20)]
    assert len(select_peaks(candidates, window_sec=60, count=5, now=now)) == 5


def test_select_returns_fewer_than_count_when_candidates_run_out():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    assert len(select_peaks([_c(600, -20.0, now)], window_sec=1800, count=10, now=now)) == 1


def test_select_returns_nothing_for_no_candidates():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    assert select_peaks([], window_sec=1800, count=10, now=now) == []
