"""Scheduled captures on the Captures page: the Schedule card and the
"scheduled" origin in the list."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.storage.local import LocalStorage
from rfobserver.web.app import create_app

STATIC = Path(__file__).resolve().parents[2] / "src/rfobserver/web/static"


def _app(tmp_path: Path, monkeypatch) -> TestClient:
    monkeypatch.chdir(tmp_path)
    app = create_app(AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path)))
    ls = LocalStorage(str(tmp_path), max_gb=1)
    ls.set_storage_path(str(tmp_path))
    app.state.local_storage = ls
    return TestClient(app)


def test_scheduled_capture_is_listed_with_its_origin(tmp_path: Path, monkeypatch) -> None:
    manual = tmp_path / "manual"
    manual.mkdir()
    sc16 = manual / "S-h-20261028T183000250-sched-w1-0.sc16"
    sc16.write_bytes(b"\0" * 400)
    sc16.with_suffix(".json").write_text(
        json.dumps(
            {
                "file": sc16.name,
                "origin": "scheduled",
                "start_time": "2026-10-28T18:30:00.250000+00:00",
                "duration_sec": 0.1,
                "schedule": {"id": "w1", "run": 0, "label": "Pass 1", "status": "recorded"},
            }
        )
    )
    other = manual / "S-h-20261028T183100.sc16"
    other.write_bytes(b"\0" * 40)
    with _app(tmp_path, monkeypatch) as c:
        c.app.state.local_storage.track(sc16)  # type: ignore[attr-defined]
        c.app.state.local_storage.track(other)  # type: ignore[attr-defined]
        rows = {r["filename"]: r for r in c.get("/captures/list").json()}
    assert rows[sc16.name]["origin"] == "scheduled"
    assert rows[sc16.name]["schedule_label"] == "Pass 1"
    assert rows[other.name]["origin"] == "manual"
    assert "schedule_label" not in rows[other.name]


def test_page_has_the_schedule_card(tmp_path: Path, monkeypatch) -> None:
    with _app(tmp_path, monkeypatch) as c:
        html = c.get("/captures/").text
    assert 'id="schedule-card"' in html
    assert "/static/schedules.js" in html
    assert "Start (UTC)" in html
    assert "—" not in html


def test_schedules_script_uses_the_api_and_no_em_dash() -> None:
    js = (STATIC / "schedules.js").read_text()
    assert "/api/schedules" in js
    assert "visibilitychange" in js
    assert "—" not in js
