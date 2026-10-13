"""Capture priority: the rule, the eviction order, and the recorder's marking."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import numpy as np
import pytest

from rfobserver.storage.local import LocalStorage
from rfobserver.storage.priority import BackgroundPower, MeanPower, classify
from tests.unit.test_recording_storage import _proc

if TYPE_CHECKING:
    from pathlib import Path

MB = 1024**2


def _cap(ls: LocalStorage, name: str, size: int, mtime: float, priority: bool = False) -> Path:
    """An auto/ capture of ``size`` bytes last written at ``mtime``."""
    d = ls.auto_dir
    sc16 = d / f"{name}.sc16"
    sc16.write_bytes(b"\0" * size)
    os.utime(sc16, (mtime, mtime))
    if priority:
        (d / f"{name}.priority").touch()
    ls.track(sc16)
    return sc16


def _names(ls: LocalStorage) -> list[str]:
    return sorted(p.stem for p in ls.auto_dir.glob("*.sc16"))


# --- the rule ---


@pytest.mark.parametrize(
    ("duration", "mean", "background", "expected"),
    [
        (2.0, None, None, "duration"),
        (29.2, -21.0, -47.0, "duration"),
        (0.14, -36.0, -47.0, "power"),  # 11 dB over
        (0.14, -38.0, -47.0, None),  # 9 dB over
        (0.14, -20.0, None, None),  # no background yet
        (1.99, None, -47.0, None),
    ],
)
def test_classify(duration, mean, background, expected):
    got = classify(
        duration_sec=duration,
        mean_power_db=mean,
        background_db=background,
        min_sec=2.0,
        excess_db=10.0,
    )
    assert got == expected


def test_a_rule_set_to_zero_is_off():
    kw = dict(duration_sec=30.0, mean_power_db=0.0, background_db=-50.0)
    assert classify(**kw, min_sec=0.0, excess_db=10.0) == "power"
    assert classify(**kw, min_sec=0.0, excess_db=0.0) is None


def test_background_is_a_median_and_waits_for_enough_windows():
    bg = BackgroundPower(maxlen=100)
    for _ in range(29):
        bg.add(-47.0)
    assert bg.median() is None
    bg.add(-47.0)
    for _ in range(20):
        bg.add(-15.0)  # an event filling part of the history
    assert bg.median() == -47.0
    bg.add(float("nan"))
    assert bg.median() == -47.0


def test_mean_power_averages_linear_power():
    m = MeanPower()
    assert m.db() is None
    m.add_db(-20.0)
    m.add_db(-40.0)
    assert m.db() == pytest.approx(10 * np.log10((0.01 + 0.0001) / 2))
    m.reset()
    assert m.db() is None


# --- storage ---


def test_scan_finds_the_mark_and_counts_priority_bytes(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1)
    _cap(ls, "A", 1000, 1, priority=True)
    _cap(ls, "B", 500, 2)
    again = LocalStorage(str(tmp_path), max_gb=1)
    assert again.get_priority_usage_bytes() == 1000
    assert again.get_usage_bytes() == 1500
    (a,) = [e for e in again.captures_newest_first() if e.path.stem == "A"]
    assert a.priority


def test_cap_evicts_ordinary_captures_before_priority_ones(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=10 / 1024)  # 10 MB
    ls.priority_share = 0.5
    _cap(ls, "old_prio", 2 * MB, 1, priority=True)
    _cap(ls, "a", 3 * MB, 2)
    _cap(ls, "b", 3 * MB, 3)
    _cap(ls, "c", 3 * MB, 4)  # 11 MB: one over
    ls.enforce_cap()
    assert _names(ls) == ["b", "c", "old_prio"]
    assert not (ls.auto_dir / "a.priority").exists()


def test_priority_over_its_share_is_evicted_first(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=10 / 1024)
    ls.priority_share = 0.5  # 5 MB for priority
    _cap(ls, "p1", 3 * MB, 1, priority=True)
    _cap(ls, "p2", 3 * MB, 2, priority=True)
    _cap(ls, "a", 3 * MB, 3)
    _cap(ls, "b", 2 * MB, 4)  # 11 MB, priority 6 MB > 5 MB
    ls.enforce_cap()
    assert _names(ls) == ["a", "b", "p2"]
    assert not (ls.auto_dir / "p1.priority").exists()  # the mark goes with it
    assert ls.get_priority_usage_bytes() == 3 * MB


def test_priority_is_taken_when_nothing_ordinary_is_left(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=4 / 1024)
    ls.priority_share = 0.9
    _cap(ls, "p1", 2 * MB, 1, priority=True)
    _cap(ls, "p2", 2 * MB, 2, priority=True)
    _cap(ls, "new", 2 * MB, 3, priority=True)
    ls.enforce_cap()
    assert _names(ls) == ["new", "p2"]


def test_share_zero_is_plain_oldest_first(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=6 / 1024)
    ls.priority_share = 0.0
    _cap(ls, "p", 3 * MB, 1, priority=True)
    _cap(ls, "a", 3 * MB, 2)
    _cap(ls, "b", 3 * MB, 3)
    ls.enforce_cap()
    assert _names(ls) == ["a", "b"]


def test_the_newest_capture_is_never_evicted(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1 / 1024)
    ls.priority_share = 0.5
    _cap(ls, "p", 3 * MB, 1, priority=True)
    _cap(ls, "big", 5 * MB, 2)
    ls.enforce_cap()
    assert _names(ls) == ["big"]


def test_floor_eviction_takes_ordinary_captures_first(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    ls.priority_share = 0.5
    _cap(ls, "p", 1000, 1, priority=True)
    _cap(ls, "a", 1000, 2)
    _cap(ls, "b", 1000, 3)
    freed = []
    ls.evict_until_free(10**12, free_bytes=lambda: 0, on_evict=lambda p, age: freed.append(p.stem))
    assert freed == ["a", "b", "p"]


def test_mark_priority_skips_unknown_and_marked_captures(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1)
    a = _cap(ls, "a", 100, 1)
    b = _cap(ls, "b", 100, 2, priority=True)
    gone = ls.auto_dir / "gone.sc16"
    assert ls.mark_priority([a, b, gone]) == [a]
    assert (ls.auto_dir / "a.priority").exists()
    assert ls.get_priority_usage_bytes() == 200
    assert ls.auto_captures_since(2) == [b]


def test_sample_reports_priority_bytes(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1)
    _cap(ls, "p", 1000, 1, priority=True)
    _cap(ls, "a", 300, 2)
    s = ls.sample(db_path=tmp_path / "x.db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert (s.auto_bytes, s.priority_bytes) == (1300, 1000)


# --- the recorder ---


def _finalized(proc, name: str, duration: float, start: float, mean_db: float | None):
    """Run the priority decision and marking for a capture already on disk, as
    _finalize_recording does."""
    sc16 = _cap(proc._storage, name, 1000, start + duration)
    proc._trigger_initiated = True
    proc._rec_power.reset()
    if mean_db is not None:
        proc._rec_power.add_db(mean_db)
    result = proc._decide_priority(duration, start, start + duration)
    proc._apply_priority(sc16)
    return result, sc16


def _marked(proc) -> list[str]:
    return sorted(p.stem for p in proc._storage.auto_dir.glob("*.priority"))


def test_long_capture_is_priority_and_takes_its_event_with_it(tmp_path):
    proc = _proc(tmp_path)
    for _ in range(40):
        proc._background.add(-47.0)
    _finalized(proc, "early", 0.14, 900.0, -46.0)  # ended 140 s before: not part of it
    _finalized(proc, "before", 0.14, 1000.0, -46.0)
    (mean, bg, reason), _ = _finalized(proc, "long", 29.0, 1030.0, -21.0)
    assert (reason, bg) == ("duration", -47.0)
    assert mean == pytest.approx(-21.0)
    (_, _, reason), _ = _finalized(proc, "after", 0.14, 1100.0, -46.0)
    assert reason == "event"
    (_, _, reason), _ = _finalized(proc, "later", 0.14, 1200.0, -46.0)
    assert reason is None  # past the 60 s span, which an event capture does not extend
    assert _marked(proc) == ["after", "before", "long"]


def test_strong_short_capture_is_priority(tmp_path):
    proc = _proc(tmp_path)
    for _ in range(40):
        proc._background.add(-47.0)
    (_, _, reason), _ = _finalized(proc, "strong", 0.14, 1000.0, -30.0)
    (_, _, weak), _ = _finalized(proc, "weak", 0.14, 2000.0, -45.0)
    assert (reason, weak) == ("power", None)
    assert _marked(proc) == ["strong"]


def test_manual_captures_are_not_classified(tmp_path):
    proc = _proc(tmp_path)
    proc._trigger_initiated = False
    assert proc._decide_priority(30.0, 1000.0, 1030.0) == (None, None, None)


def test_trigger_path_feeds_the_capture_power(tmp_path):
    proc = _proc(tmp_path, TRIGGER_THRESHOLD_DB=-200.0, RECORDING_RAM_BUFFER=True)
    proc.arm_trigger()
    chunk = np.full(4096, (1000 << 16) | 1000, dtype=np.int32)  # I = Q = 1000
    proc._check_trigger_and_record(chunk)
    assert proc.recording_status()["state"] == "recording"
    proc._check_trigger_and_record(chunk)
    expected = 10 * np.log10(2 * (1000 / 32768) ** 2 / 50)
    assert proc._rec_power.db() == pytest.approx(expected, abs=0.01)
    assert proc._rec_power._n == 2


def test_metadata_records_power_and_priority(tmp_path):
    proc = _proc(tmp_path, PRIORITY_MIN_SEC=0.0)
    for _ in range(40):
        proc._background.add(-47.0)
    proc._trigger_initiated = True
    proc._recording_dir = proc._storage.auto_dir
    proc._rec_power.add_db(-20.0)
    proc._recording_bytes = 4000
    proc._write_recording_metadata("x.sc16", 0.01)
    meta = json.loads((proc._storage.auto_dir / "x.json").read_text())
    assert meta["mean_power_db"] == -20.0
    assert meta["background_power_db"] == -47.0
    assert meta["priority"] == "power"


# --- the database record ---


def _row(**kw):
    from datetime import datetime, timedelta, timezone

    start = datetime(2026, 10, 12, 20, 19, 19, tzinfo=timezone.utc)
    base = dict(
        origin="auto",
        start_time=start,
        stop_time=start + timedelta(seconds=29.2),
        duration_sec=29.2,
        sdr_center_freq_hz=915e6,
        sample_rate_hz=26e6,
        gain_db=35.0,
        total_samples=759_200_000,
        trigger_initiated=True,
    )
    base.update(kw)
    return base


async def _query(db):
    from datetime import datetime, timezone

    rows = await db.query_iq_captures(
        since=datetime(2026, 10, 12, tzinfo=timezone.utc),
        until=datetime(2026, 10, 13, tzinfo=timezone.utc),
    )
    return {r["filename"]: r for r in rows}


async def test_capture_power_and_priority_are_stored_and_marked(tmp_path):
    from rfobserver.storage.database import SensorDatabase

    db = SensorDatabase(str(tmp_path / "t.db"))
    await db.connect()
    try:
        await db.insert_iq_capture(
            filename="long.sc16", mean_power_db=-21.0, background_db=-47.1, priority=True, **_row()
        )
        await db.insert_iq_capture(filename="short.sc16", **_row())
        rows = await _query(db)
        assert rows["long.sc16"]["priority"] is True
        assert (rows["long.sc16"]["mean_power_db"], rows["long.sc16"]["background_db"]) == (
            -21.0,
            -47.1,
        )
        assert rows["short.sc16"]["priority"] is False
        await db.mark_iq_captures_priority(["short.sc16", "missing.sc16"])
        assert (await _query(db))["short.sc16"]["priority"] is True
    finally:
        await db.close()


async def test_an_existing_iq_captures_table_gains_the_columns(tmp_path):
    import sqlite3

    from rfobserver.storage.database import SensorDatabase

    path = tmp_path / "old.db"
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE iq_captures (
            id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL UNIQUE,
            origin TEXT NOT NULL, start_time TEXT NOT NULL, stop_time TEXT NOT NULL,
            duration_sec REAL NOT NULL, sdr_center_freq_hz REAL, sample_rate_hz REAL,
            gain_db REAL, total_samples INTEGER, trigger_initiated INTEGER,
            created_at TEXT DEFAULT (datetime('now')))"""
    )
    con.execute(
        "INSERT INTO iq_captures (filename, origin, start_time, stop_time, duration_sec) "
        "VALUES ('old.sc16', 'auto', '2026-10-12T20:00:00+00:00', '2026-10-12T20:00:01+00:00', 1)"
    )
    con.commit()
    con.close()
    db = SensorDatabase(str(path))
    await db.connect()
    try:
        old = (await _query(db))["old.sc16"]
        assert (old["mean_power_db"], old["priority"]) == (None, None)
        await db.insert_iq_capture(filename="new.sc16", priority=True, **_row())
        assert (await _query(db))["new.sc16"]["priority"] is True
    finally:
        await db.close()
