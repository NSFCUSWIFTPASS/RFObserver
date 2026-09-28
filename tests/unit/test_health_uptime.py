"""/api/health reports process uptime.

A Dashboard load that dies mid-request uses it to tell a restarted server
(watchdog escalation, OOM kill, a deploy) from a network drop.
"""

from __future__ import annotations

import time

from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


def test_health_uptime_starts_near_zero_and_grows() -> None:
    client = TestClient(create_app(AppSettings(_env_file=None)))
    first = client.get("/api/health").json()
    assert first["uptime_sec"] < 5.0, "a fresh app reports a small uptime"
    time.sleep(0.05)
    second = client.get("/api/health").json()
    assert second["uptime_sec"] >= first["uptime_sec"]
