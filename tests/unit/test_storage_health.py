"""Storage state on /api/health, and clearing the sticky flag."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.storage.governor import (
    DEGRADED_CONFIG_KEY,
    GB,
    StorageGovernor,
    StorageSample,
    VolumeSample,
)
from rfobserver.web.app import create_app

T0 = datetime(2026, 9, 23, tzinfo=timezone.utc)


def _tick(gov: StorageGovernor, free_gb: float, evictable: bool = True) -> None:
    gov.tick(
        StorageSample(
            data=VolumeSample(int(free_gb * GB), 1000 * GB),
            db_volume=None,
            db_file_bytes=GB,
            db_reusable_bytes=0,
            auto_bytes=0,
            manual_bytes=0,
            evictable_auto=evictable,
        ),
        min_free_gb=0,
        now=T0,
    )


class _WDB:
    def __init__(self) -> None:
        self.config: dict[str, str] = {}

    async def set_config(self, k: str, v: str) -> None:
        self.config[k] = v


def _client(gov: StorageGovernor | None, wdb: _WDB | None = None) -> TestClient:
    app = create_app(AppSettings(_env_file=None))
    app.state.storage_governor = gov
    app.state.write_database = wdb
    return TestClient(app)


def test_health_without_a_governor_has_no_storage_block():
    body = _client(None).get("/api/health").json()
    assert "storage" not in body and body["status"] == "ok"


def test_steps_1_and_2_are_reported_but_not_degraded():
    gov = StorageGovernor()
    _tick(gov, 40)
    body = _client(gov).get("/api/health").json()
    assert body["storage"]["step"] == 1
    assert body["status"] == "ok"


def test_step_3_is_degraded():
    gov = StorageGovernor()
    _tick(gov, 40, evictable=False)
    _tick(gov, 40, evictable=False)
    assert _client(gov).get("/api/health").json()["status"] == "degraded"


def test_sticky_flag_is_degraded_after_recovery_until_cleared():
    gov = StorageGovernor()
    _tick(gov, 200)
    gov.report_write_error("ENOSPC: No space left on device", now=T0)
    wdb = _WDB()
    c = _client(gov, wdb)
    body = c.get("/api/health").json()
    assert body["status"] == "degraded"
    assert body["storage"]["last_write_error"]["error"].startswith("ENOSPC")
    r = c.post("/api/storage/clear-degraded")
    assert r.status_code == 200 and r.json()["degraded_since"] is None
    assert wdb.config[DEGRADED_CONFIG_KEY] == ""
    assert c.get("/api/health").json()["status"] == "ok"


def test_clear_without_a_governor_is_409():
    assert _client(None).post("/api/storage/clear-degraded").status_code == 409
