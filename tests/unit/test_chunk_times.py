"""Capture-clock times for stream samples, from recent chunks' capture times."""

from __future__ import annotations

import pytest

from rfobserver.pipeline.streaming import _ChunkTimes

RATE = 1000.0  # samples per second


def test_sample_time_within_its_chunk():
    ct = _ChunkTimes()
    ct.add(0, 100.0)
    ct.add(1000, 101.5)  # 0.5 s gap: chunks need not be back to back
    assert ct.time_ms(250, RATE) == pytest.approx(100_250.0)
    assert ct.time_ms(1000, RATE) == pytest.approx(101_500.0)
    assert ct.time_ms(1100, RATE) == pytest.approx(101_600.0)


def test_exclusive_end_on_a_boundary_belongs_to_the_chunk_before():
    ct = _ChunkTimes()
    ct.add(0, 100.0)
    ct.add(1000, 101.5)
    assert ct.time_ms(1000, RATE, end=True) == pytest.approx(101_000.0)
    assert ct.time_ms(1100, RATE, end=True) == pytest.approx(101_600.0)


def test_before_the_kept_chunks_is_unknown():
    ct = _ChunkTimes()
    assert ct.time_ms(5, RATE) is None
    ct.add(1000, 101.0)
    assert ct.time_ms(999, RATE) is None


def test_a_counter_that_goes_back_starts_over():
    ct = _ChunkTimes()
    ct.add(5000, 100.0)
    ct.add(0, 200.0)  # restart
    assert ct.time_ms(100, RATE) == pytest.approx(200_100.0)
    ct.add(0, 300.0)  # the same start again is ignored
    assert ct.time_ms(100, RATE) == pytest.approx(200_100.0)


def test_keeps_a_bounded_history(monkeypatch):
    monkeypatch.setattr(_ChunkTimes, "KEEP", 4)
    ct = _ChunkTimes()
    for i in range(20):
        ct.add(i * 1000, float(i))
    assert len(ct._starts) <= 8
    assert ct.time_ms(19_500, RATE) == pytest.approx(19_500.0)
    assert ct.time_ms(0, RATE) is None


def test_a_decoded_label_carries_its_bursts_capture_times(tmp_path):
    from datetime import datetime, timezone

    from rfobserver.models import BurstFingerprint
    from rfobserver.pipeline.attribution import AttributionItem, AttributionResult
    from tests.unit.test_recording_gaps import _proc

    proc = _proc(tmp_path)
    rate = float(proc._settings.BANDWIDTH)
    proc._chunk_times.add(0, 100.0)
    now = datetime.now(timezone.utc)
    b = BurstFingerprint(
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        bandwidth_hz=1e5,
        peak_power_db=-40.0,
        start_sample=int(rate * 0.25),
        stop_sample=int(rate * 0.5),
    )
    proc._note_capture_times([b])

    def result(burst_id, outcome):
        item = AttributionItem(burst_id, b"", 0, [], -40.0, meta={"start_time_ms": 1.0})
        return AttributionResult(item, "SCMplus", None, "{}", outcome)

    proc._add_label(result(b.burst_id, "decoded"))
    (label,) = proc._labels
    assert label["capture_start_ms"] == pytest.approx(100_250.0)
    assert label["capture_stop_ms"] == pytest.approx(100_500.0)
    assert label["start_time_ms"] == 1.0  # the sensor's own time base, unchanged
    assert not proc._burst_capture_ms  # used once, then dropped

    # Without capture times (an unknown id) the label is as before.
    proc._add_label(result("other", "decoded"))
    assert "capture_start_ms" not in proc._labels[-1]
