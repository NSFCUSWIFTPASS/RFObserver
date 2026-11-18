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
