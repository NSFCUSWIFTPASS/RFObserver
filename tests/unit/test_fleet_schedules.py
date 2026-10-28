"""Fleet schedules: one schedule sent to several sensors, their schedules
merged by id, and a delete that reaches every sensor."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi.testclient import TestClient

from rfobserver.fleet.app import create_fleet_app

if TYPE_CHECKING:
    from pathlib import Path

HOSTS = {"10.0.0.1": "a", "10.0.0.2": "b"}


class Sensors:
    def __init__(self) -> None:
        self.schedules: dict[str, dict[str, dict[str, Any]]] = {h: {} for h in HOSTS}
        self.down: set[str] = set()
        self.posted: list[tuple[str, dict[str, Any]]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        if host in self.down:
            raise httpx.ConnectError("refused", request=request)
        if path == "/api/identity":
            return httpx.Response(200, json={"id": HOSTS[host]})
        mine = self.schedules[host]
        if path == "/api/schedules" and request.method == "POST":
            body = json.loads(request.content)
            self.posted.append((HOSTS[host], body))
            if body.get("duration_ms", 0) <= 0:
                return httpx.Response(400, json={"detail": "duration_ms is required"})
            sched = {**body, "start": body["start"], "runs": 1}
            mine[body["id"]] = sched
            return httpx.Response(
                201,
                json={
                    "schedule": sched,
                    "next_runs": [{"run": 0, "start": body["start"], "start_local": "x"}],
                    "warnings": ["sweeping"] if host == "10.0.0.2" else [],
                },
            )
        if path == "/api/schedules" and request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "now_ms": 1,
                    "timezone": "America/Los_Angeles" if host == "10.0.0.1" else "UTC",
                    "schedules": [
                        {
                            "schedule": s,
                            "next_run": {"run": 0, "start": s["start"], "start_local": "y"},
                            "last_run": None,
                        }
                        for s in mine.values()
                    ],
                    "recent": [
                        {
                            "id": sid,
                            "label": "",
                            "run": 0,
                            "status": "recorded",
                            "reason": None,
                            "start_ms": 1000 if host == "10.0.0.1" else 2000,
                            "start": s["start"],
                            "start_local": "z",
                            "filename": f"{HOSTS[host]}.sc16",
                            "info": None,
                        }
                        for sid, s in mine.items()
                    ],
                },
            )
        if path.startswith("/api/schedules/") and request.method == "DELETE":
            sid = path.rsplit("/", 1)[1]
            if mine.pop(sid, None) is None:
                return httpx.Response(404, json={"detail": f"No schedule {sid}"})
            return httpx.Response(200, json={"deleted": sid})
        return httpx.Response(404, json={"detail": "nope"})


@pytest.fixture
def fleet(tmp_path: Path) -> Any:
    sensors = Sensors()
    app = create_fleet_app(tmp_path, mdns=False, transport=httpx.MockTransport(sensors.handler))
    app.state.registry.add("a", "10.0.0.1", 80, source="manual")
    app.state.registry.add("b", "10.0.0.2", 80, source="manual")
    with TestClient(app) as client:
        yield client, sensors


BODY = {"start": "2026-10-28T18:30:00.250Z", "duration_ms": 2000, "label": "Pass"}


def test_fan_out_sends_one_id_to_each_sensor(fleet: Any) -> None:
    client, sensors = fleet
    r = client.post("/fleet/api/schedules", json={**BODY, "sensors": ["a", "b"]})
    assert r.status_code == 201, r.text
    body = r.json()
    sid = body["id"]
    assert len(sid) == 12
    assert {k: v["ok"] for k, v in body["results"].items()} == {"a": True, "b": True}
    assert sorted(name for name, _ in sensors.posted) == ["a", "b"]
    assert all(b["id"] == sid and b["source"] == "fleet" for _, b in sensors.posted)
    assert all("sensors" not in b for _, b in sensors.posted)
    assert body["warnings"] == ["b: sweeping"]


def test_given_id_is_kept_and_one_sensor_down_is_partial(fleet: Any) -> None:
    client, sensors = fleet
    sensors.down.add("10.0.0.2")
    r = client.post("/fleet/api/schedules", json={**BODY, "id": "w1", "sensors": ["a", "b"]})
    assert r.status_code == 201
    res = r.json()["results"]
    assert res["a"]["ok"] is True and res["b"]["ok"] is False
    assert "ConnectError" in res["b"]["detail"]


def test_all_down_is_502_and_all_refused_is_400(fleet: Any) -> None:
    client, sensors = fleet
    bad = client.post("/fleet/api/schedules", json={**BODY, "duration_ms": 0, "sensors": ["a"]})
    assert bad.status_code == 400
    assert bad.json()["results"]["a"]["detail"] == "duration_ms is required"
    sensors.down.update(HOSTS)
    down = client.post("/fleet/api/schedules", json={**BODY, "sensors": ["a", "b"]})
    assert down.status_code == 502


def test_sensors_are_required_and_known(fleet: Any) -> None:
    client, _ = fleet
    assert client.post("/fleet/api/schedules", json=BODY).status_code == 400
    assert client.post("/fleet/api/schedules", json={**BODY, "sensors": []}).status_code == 400
    r = client.post("/fleet/api/schedules", json={**BODY, "sensors": ["a", "zz"]})
    assert r.status_code == 400 and "zz" in r.json()["detail"]


def test_merged_list_groups_by_id(fleet: Any) -> None:
    client, sensors = fleet
    client.post("/fleet/api/schedules", json={**BODY, "id": "w1", "sensors": ["a", "b"]})
    client.post("/fleet/api/schedules", json={**BODY, "id": "w2", "sensors": ["a"]})
    body = client.get("/fleet/api/schedules").json()
    by_id = {s["id"]: s for s in body["schedules"]}
    assert set(by_id["w1"]["sensors"]) == {"a", "b"} and set(by_id["w2"]["sensors"]) == {"a"}
    assert by_id["w1"]["sensors"]["a"]["next_run"]["run"] == 0
    assert body["sensors"]["a"]["timezone"] == "America/Los_Angeles"
    assert body["sensors"]["b"]["timezone"] == "UTC"
    assert [(r["sensor"], r["id"]) for r in body["recent"]][:1] == [("b", "w1")]
    assert all("sensor_label" in r for r in body["recent"])
    assert isinstance(body["now_ms"], int) and body["errors"] == {}
    sensors.down.add("10.0.0.2")
    body = client.get("/fleet/api/schedules").json()
    assert "b" in body["errors"] and set(body["schedules"][0]["sensors"]) == {"a"}


def test_delete_reaches_every_sensor(fleet: Any) -> None:
    client, sensors = fleet
    client.post("/fleet/api/schedules", json={**BODY, "id": "w1", "sensors": ["a", "b"]})
    r = client.delete("/fleet/api/schedules/w1")
    assert r.status_code == 200
    assert {k: v["ok"] for k, v in r.json()["results"].items()} == {"a": True, "b": True}
    assert sensors.schedules == {"10.0.0.1": {}, "10.0.0.2": {}}
    assert client.delete("/fleet/api/schedules/w1").status_code == 404


def test_fleet_page_has_the_card_with_a_sensor_picker(fleet: Any) -> None:
    client, _ = fleet
    html = client.get("/captures").text
    assert 'id="schedule-card"' in html and 'data-fleet="1"' in html
    assert 'id="sched-sensors"' in html
