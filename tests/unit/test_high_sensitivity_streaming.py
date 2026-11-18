"""The streaming pipeline's side of high-sensitivity mode: a live tone
detector for the run, the run ended by a retune, routine state restored."""

from __future__ import annotations

import asyncio

from tests.unit.test_recording_gaps import _proc


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_hs_start_runs_a_tone_detector_and_end_stops_only_its_own(tmp_path):
    proc = _proc(tmp_path)
    proc._loop = asyncio.get_running_loop()
    proc._start_tone_detector()
    assert proc.tone_detector_status()["enabled"] is False
    proc.high_sensitivity.start(30)
    await _settle()
    assert proc.tone_detector_status()["enabled"] is True
    proc.high_sensitivity.end_now("deleted")
    await _settle()
    assert proc.tone_detector_status()["enabled"] is False


async def test_routine_tone_detector_survives_hs_end(tmp_path):
    proc = _proc(tmp_path, TONE_DETECTOR_ENABLED=True)
    proc._loop = asyncio.get_running_loop()
    proc._start_tone_detector()
    routine = proc._tone_detector
    proc.high_sensitivity.start(30)
    await _settle()
    assert proc._tone_detector is routine
    proc.high_sensitivity.end_now("deleted")
    await _settle()
    assert proc._tone_detector is routine


async def test_reconfigure_ends_the_run_with_retune(tmp_path):
    proc = _proc(tmp_path)
    proc._loop = asyncio.get_running_loop()
    run = proc.high_sensitivity.start(30)
    proc.reconfigure()
    assert proc.high_sensitivity.current() is None
    assert run.end_reason == "retune"


def test_burst_threshold_follows_the_run(tmp_path):
    """The burst thread's per-grid check: the HS threshold while the run is on,
    the routine one back as soon as it ends (Review Focus 1)."""
    proc = _proc(tmp_path, BURST_THRESHOLD_HIGH_DB=30.0)
    cfg = proc._make_burst_config()
    assert proc._hs_burst_config(cfg).threshold_high_db == 30.0
    run = proc.high_sensitivity.start(30)
    on = proc._hs_burst_config(cfg)
    assert on.threshold_high_db == 15.0
    assert proc._hs_run_id() == run.id
    proc.high_sensitivity.end_now("expired")
    assert proc._hs_burst_config(on).threshold_high_db == 30.0
    assert proc._hs_run_id() is None


class _FakeDB:
    def __init__(self) -> None:
        self.saved: list[dict] = []
        self.closed: list[tuple[str, float]] = []

    async def hs_run_save(self, run):
        self.saved.append(dict(run))

    async def hs_run_counts(self, run_id):
        return {"detections": 3, "ssm": {"0200574952470001": 2}, "tones": []}

    async def hs_run_close_open(self, reason, ended, started_before=None):
        self.closed.append((reason, ended, started_before))
        return 1


async def test_summary_has_counter_deltas_and_counts(tmp_path, monkeypatch):
    import rfobserver.pipeline.streaming as st

    monkeypatch.setattr(st, "_HS_SUMMARY_DELAY_SEC", 0.0)
    proc = _proc(tmp_path)
    proc._loop = asyncio.get_running_loop()
    db = _FakeDB()
    proc._db = db
    rx = proc._receiver
    rx.overflow_events, rx.overflow_lost_samples = 5, 100
    run = proc.high_sensitivity.start(30, run_id="r1")
    await _settle()
    assert db.saved[0]["id"] == "r1" and db.saved[0]["ended"] is None
    rx.overflow_events, rx.overflow_lost_samples = 7, 160
    proc.high_sensitivity.stop("r1")
    for _ in range(20):
        await asyncio.sleep(0)
    s = db.saved[-1]["summary"]
    assert db.saved[-1]["end_reason"] == "deleted"
    assert s["overflow_events"] == 2 and s["overflow_lost_samples"] == 60
    assert s["detections"] == 3 and s["ssm"] == {"0200574952470001": 2}
    assert s["threshold_db"] == run.threshold_db


async def test_startup_closes_a_run_left_open_by_a_restart(tmp_path):
    proc = _proc(tmp_path)
    db = _FakeDB()
    proc._db = db
    await proc._hs_close_left_open(started_before=123.0)
    assert db.closed == [("restart", 123.0, 123.0)]


class _Book:
    def __init__(self, schedules) -> None:
        self._s = schedules

    def snapshot(self):
        return 1, list(self._s)


def _flagged(start_ms: int, duration_ms: int = 60_000, **kw):
    from rfobserver.schedules.model import Schedule

    return Schedule(
        id="daily",
        start_ms=start_ms,
        duration_ms=duration_ms,
        every_ms=None,
        count=None,
        until_ms=None,
        label="",
        source="api",
        created_ms=0,
        high_sensitivity=True,
        **kw,
    )


def test_flagged_run_starts_hs_lead_seconds_early_and_ends_with_the_capture(tmp_path):
    proc = _proc(tmp_path)
    t0 = 1_795_000_000_000
    proc._schedules = _Book([_flagged(t0, hs_threshold_db=12.0)])
    proc._hs_schedule_tick(t0 / 1000 - 2.5)
    assert proc.high_sensitivity.current() is None
    proc.high_sensitivity._clock = lambda: t0 / 1000 - 1.9
    proc._hs_schedule_tick(t0 / 1000 - 1.9)
    run = proc.high_sensitivity.current()
    assert run is not None and run.id == "daily-0" and run.source == "schedule"
    assert run.threshold_db == 12.0
    assert abs(run.end - (t0 / 1000 + 60.0)) < 1e-6
    proc._hs_schedule_tick(t0 / 1000 - 1.8)  # the same run is not started twice
    assert proc.high_sensitivity.current() is run


def test_flagged_run_while_another_hs_run_is_on_is_skipped(tmp_path):
    proc = _proc(tmp_path)
    t0 = 1_795_000_000_000
    proc.high_sensitivity._clock = lambda: t0 / 1000 - 1.0
    other = proc.high_sensitivity.start(120, run_id="manual")
    proc._schedules = _Book([_flagged(t0)])
    proc._hs_schedule_tick(t0 / 1000 - 1.0)
    assert proc.high_sensitivity.current() is other


def test_scheduled_capture_meta_names_the_run_in_force(tmp_path):
    proc = _proc(tmp_path)
    assert proc._scheduled_capture_meta()["hs_run"] is None
    proc.high_sensitivity.start(30, run_id="daily-0")
    assert proc._scheduled_capture_meta()["hs_run"] == "daily-0"
