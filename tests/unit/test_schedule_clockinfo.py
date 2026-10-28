"""Scheduled captures: what the host clock is disciplined by, for metadata."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from rfobserver.schedules import clockinfo

TIMESYNC = (
    "0.fr.pool.ntp.org\n"
    "{ Leap=0, Version=4, Mode=4, Stratum=1, Precision=-25, RootDelay=0, "
    "RootDispersion=0, Reference=GPS, Ignored=no PacketCount=4394, Jitter=2.634ms }\n"
)
CHRONY = (
    "C0A80001,192.168.0.1,2,1793212200.1,0.000123456,-0.00002,0.0001,"
    "1.2,0.01,0.1,0.001,0.002,64.0,Normal\n"
)


class FakeRun:
    def __init__(self, outputs: dict[str, Any]) -> None:
        self.outputs = outputs
        self.calls: list[list[str]] = []

    def __call__(self, cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(cmd)
        out = self.outputs.get(cmd[0] + " " + cmd[1])
        if isinstance(out, BaseException):
            raise out
        if out is None:
            raise FileNotFoundError(cmd[0])
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")


@pytest.fixture(autouse=True)
def fresh_cache() -> None:
    clockinfo.clear_cache()


def test_timesyncd(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeRun({"timedatectl show-timesync": TIMESYNC, "timedatectl show": "yes\n"})
    monkeypatch.setattr(clockinfo.subprocess, "run", fake)
    info = clockinfo.clock_info()
    assert info == {
        "clock_source": "ntp",
        "clock_server": "0.fr.pool.ntp.org",
        "clock_offset_ms": None,
        "clock_jitter_ms": 2.634,
        "clock_synced": True,
    }


def test_chrony_preferred_with_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeRun({"chronyc -c": CHRONY, "timedatectl show": "yes\n"})
    monkeypatch.setattr(clockinfo.subprocess, "run", fake)
    info = clockinfo.clock_info()
    assert info["clock_source"] == "ntp" and info["clock_server"] == "192.168.0.1"
    assert info["clock_offset_ms"] == pytest.approx(0.123456)
    assert info["clock_synced"] is True


@pytest.mark.parametrize(
    "error", [FileNotFoundError("x"), subprocess.TimeoutExpired("x", 1), OSError("x")]
)
def test_missing_tools_give_unknown(monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
    fake = FakeRun(
        {"chronyc -c": error, "timedatectl show-timesync": error, "timedatectl show": error}
    )
    monkeypatch.setattr(clockinfo.subprocess, "run", fake)
    assert clockinfo.clock_info() == {
        "clock_source": "unknown",
        "clock_server": None,
        "clock_offset_ms": None,
        "clock_jitter_ms": None,
        "clock_synced": None,
    }


def test_cached_for_a_minute(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeRun({"timedatectl show-timesync": TIMESYNC, "timedatectl show": "no\n"})
    monkeypatch.setattr(clockinfo.subprocess, "run", fake)
    now = [1000.0]
    monkeypatch.setattr(clockinfo.time, "monotonic", lambda: now[0])
    assert clockinfo.clock_info()["clock_synced"] is False
    calls = len(fake.calls)
    now[0] += 59
    clockinfo.clock_info()
    assert len(fake.calls) == calls
    now[0] += 2
    clockinfo.clock_info()
    assert len(fake.calls) > calls
