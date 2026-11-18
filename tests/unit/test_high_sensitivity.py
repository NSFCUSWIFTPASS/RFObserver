"""High-sensitivity controller: a bounded run that overlays the burst threshold
and the isolation budget, and reverts by itself."""

from __future__ import annotations

import pytest

from rfobserver.config import AppSettings
from rfobserver.pipeline.high_sensitivity import HighSensitivity, HSError


class Clock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _settings(**kw):
    return AppSettings(_env_file=None, **kw)


def test_start_overlays_and_expiry_restores():
    clk = Clock(1000.0)
    hs = HighSensitivity(_settings(), clock=clk)
    run = hs.start(120)
    assert hs.burst_threshold_db() == 15.0
    assert hs.isolation_max_per_sec() == 60
    clk.t = 1000.0 + 120
    assert hs.current() is None
    assert hs.burst_threshold_db() == 30.0
    assert hs.isolation_max_per_sec() == 20
    assert run.end_reason == "expired"
    assert run.ended == 1120.0


def test_seconds_and_threshold_are_bounded():
    hs = HighSensitivity(_settings(), clock=Clock(0.0))
    for bad in (0, 301, -1):
        with pytest.raises(HSError) as e:
            hs.start(bad)
        assert e.value.status == 400
    for bad in (9.9, 30.1):
        with pytest.raises(HSError) as e:
            hs.start(60, threshold_db=bad)
        assert e.value.status == 400
    assert hs.current() is None


def test_threshold_per_run_and_from_settings():
    hs = HighSensitivity(_settings(HS_BURST_THRESHOLD_DB=12.0), clock=Clock(0.0))
    hs.start(10)
    assert hs.burst_threshold_db() == 12.0
    hs.end_now("deleted")
    hs.start(10, threshold_db=10.0)
    assert hs.burst_threshold_db() == 10.0


def test_second_run_conflicts_but_same_id_extends_to_300_s():
    clk = Clock(0.0)
    hs = HighSensitivity(_settings(), clock=clk)
    hs.start(100, run_id="a")
    with pytest.raises(HSError) as e:
        hs.start(60, run_id="b")
    assert e.value.status == 409
    clk.t = 50.0
    assert hs.start(300, run_id="a").end == 300.0  # start 0 + 300, not 50 + 300


def test_stop_unknown_run_is_404_and_stop_ends_it():
    hs = HighSensitivity(_settings(), clock=Clock(5.0))
    with pytest.raises(HSError) as e:
        hs.stop("nope")
    assert e.value.status == 404
    hs.start(60, run_id="r")
    run = hs.stop("r")
    assert run.end_reason == "deleted" and run.ended == 5.0
    assert hs.current() is None


def test_listeners_see_start_and_end_once():
    seen = []
    hs = HighSensitivity(_settings(), clock=Clock(0.0))
    hs.listeners.append(lambda ev, run: seen.append((ev, run.id, run.end_reason)))
    hs.start(10, run_id="x")
    hs.end_now("retune")
    hs.end_now("retune")
    assert seen == [("start", "x", None), ("end", "x", "retune")]


def test_a_failing_listener_does_not_break_the_run():
    hs = HighSensitivity(_settings(), clock=Clock(0.0))

    def boom(ev, run):
        raise RuntimeError("listener")

    hs.listeners.append(boom)
    hs.start(10)
    assert hs.current() is not None
