"""Storage state on /api/health, and clearing the sticky flag."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.storage.governor import (
    DEGRADED_CONFIG_KEY,
    GB,
    LAST_WRITE_ERROR_CONFIG_KEY,
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
    assert wdb.config[LAST_WRITE_ERROR_CONFIG_KEY] == ""
    assert c.get("/api/health").json()["status"] == "ok"


def test_clear_without_a_governor_is_409():
    assert _client(None).post("/api/storage/clear-degraded").status_code == 409


def test_dashboard_has_the_storage_banner_and_record_notice():
    # dashboard.html (the rec/arm/stop live view with the heartbeat websocket)
    # is served at /live/; "/" is the separate averaged-history landing page.
    html = _client(None).get("/live/").text
    for needle in ('id="storage-banner"', 'id="storage-banner-clear"', 'id="rec-notice"'):
        assert needle in html


def test_config_page_has_the_storage_bar_and_fields():
    html = _client(None).get("/config").text
    for needle in (
        'id="storage-bar"',
        'name="disk_min_free_gb"',
        'name="stats_retention_days"',
        'name="storage_check_sec"',
    ):
        assert needle in html


def test_config_apply_accepts_the_storage_settings(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    app = create_app(AppSettings(_env_file=None))
    c = TestClient(app)
    r = c.post(
        "/config/apply",
        json={"disk_min_free_gb": "12.5", "stats_retention_days": "365", "storage_check_sec": "5"},
    )
    assert r.status_code == 200, r.text
    s = app.state.settings
    assert s.DISK_MIN_FREE_GB == 12.5 and s.STATS_RETENTION_DAYS == 365
    assert s.STORAGE_CHECK_SEC == 5.0


def test_a_write_error_reported_right_after_a_clear_is_persisted():
    """The clear route writes the governor's values, not hard-coded blanks: an
    error a writer thread reports just after the clear stays on disk."""
    gov = StorageGovernor()
    gov.report_write_error("ENOSPC: No space left on device", now=T0)
    real_clear = gov.clear_degraded

    def clear_then_report() -> None:
        real_clear()
        gov.report_write_error("EIO: Input/output error", now=T0)

    gov.clear_degraded = clear_then_report  # type: ignore[method-assign]
    wdb = _WDB()
    assert _client(gov, wdb).post("/api/storage/clear-degraded").status_code == 200
    assert "EIO" in wdb.config[LAST_WRITE_ERROR_CONFIG_KEY]
    assert wdb.config[DEGRADED_CONFIG_KEY] == T0.isoformat()


def test_clear_consumes_the_change_so_the_loop_does_not_rewrite_it():
    gov = StorageGovernor()
    gov.report_write_error("ENOSPC: No space left on device", now=T0)
    gov.take_degraded_change()  # persisted by an earlier tick
    wdb = _WDB()
    _client(gov, wdb).post("/api/storage/clear-degraded")
    assert wdb.config[DEGRADED_CONFIG_KEY] == ""
    assert gov.take_degraded_change() == (False, {})
