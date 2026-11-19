"""Fleet high-sensitivity: every reachable sensor enters, or none stays in."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi.testclient import TestClient

from rfobserver.fleet.app import create_fleet_app

if TYPE_CHECKING:
    from pathlib import Path

HOSTS = {"10.0.0.1": "a", "10.0.0.2": "b", "10.0.0.3": "c"}


class Sensors:
    def __init__(self) -> None:
        self.down: set[str] = set()
        self.refuse: set[str] = set()
        self.timeout_after_enter: set[str] = set()
        self.on: dict[str, str] = {}  # host -> run id
        self.deleted: list[str] = []
        self.posted: list[tuple[str, dict[str, Any]]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        if host in self.down:
            raise httpx.ConnectError("refused", request=request)
        if path == "/api/identity":
            return httpx.Response(200, json={"id": HOSTS[host]})
        if path == "/api/health":
            return httpx.Response(200, json={"status": "ok"})
        if path == "/api/high-sensitivity" and request.method == "POST":
            body = json.loads(request.content)
            self.posted.append((HOSTS[host], body))
            if host in self.refuse:
                return httpx.Response(409, json={"detail": "high-sensitivity run x is on"})
            self.on[host] = body["id"]
            if host in self.timeout_after_enter:
                raise httpx.ReadTimeout("slow", request=request)
            run = {"id": body["id"], "threshold_db": body.get("threshold_db") or 15.0}
            return httpx.Response(200, json={"run": run})
        if path.startswith("/api/high-sensitivity/") and request.method == "DELETE":
            rid = path.rsplit("/", 1)[1]
            self.deleted.append(HOSTS[host])
            if self.on.get(host) != rid:
                return httpx.Response(404, json={"detail": "not on"})
            del self.on[host]
            return httpx.Response(200, json={"run": {"id": rid, "end_reason": "deleted"}})
        if path.startswith("/api/high-sensitivity/") and request.method == "GET":
            return httpx.Response(200, json={"id": path.rsplit("/", 1)[1], "summary": {}})
        return httpx.Response(404, json={"detail": "nope"})


@pytest.fixture
def fleet(tmp_path: Path) -> Any:
    sensors = Sensors()
    app = create_fleet_app(tmp_path, mdns=False, transport=httpx.MockTransport(sensors.handler))
    for host, sid in HOSTS.items():
        app.state.registry.add(sid, host, 80, source="manual")
    with TestClient(app) as client:
        yield client, sensors


def _states(r: Any) -> dict[str, str]:
    return {k: v["state"] for k, v in r.json()["results"].items()}


def test_all_reachable_enter_offline_is_skipped(fleet: Any) -> None:
    client, sensors = fleet
    sensors.down.add("10.0.0.3")
    r = client.post("/fleet/api/high-sensitivity", json={"seconds": 120, "id": "daily"})
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True and r.json()["id"] == "daily"
    assert _states(r) == {"a": "entered", "b": "entered", "c": "offline"}
    assert all(b["source"] == "fleet" and b["seconds"] == 120 for _, b in sensors.posted)


def test_one_refusal_rolls_back_the_others(fleet: Any) -> None:
    client, sensors = fleet
    sensors.refuse.add("10.0.0.2")
    r = client.post("/fleet/api/high-sensitivity", json={"seconds": 60})
    assert r.status_code == 409
    assert _states(r) == {"a": "rolled_back", "b": "failed", "c": "rolled_back"}
    assert sensors.on == {}
    assert "is on" in r.json()["results"]["b"]["detail"]


def test_timeout_on_start_is_rolled_back_too(fleet: Any) -> None:
    client, sensors = fleet
    sensors.timeout_after_enter.add("10.0.0.3")
    r = client.post("/fleet/api/high-sensitivity", json={"seconds": 60})
    assert r.status_code == 409
    assert sensors.on == {}  # c entered before timing out; the rollback reached it
    assert "c" in sensors.deleted
    assert _states(r)["c"] == "rolled_back"


def test_min_sensors_not_met_rolls_back(fleet: Any) -> None:
    client, sensors = fleet
    sensors.down.update({"10.0.0.2", "10.0.0.3"})
    r = client.post("/fleet/api/high-sensitivity", json={"seconds": 60, "min_sensors": 2})
    assert r.status_code == 409
    assert _states(r) == {"a": "rolled_back", "b": "offline", "c": "offline"}


def test_chosen_sensors_only(fleet: Any) -> None:
    client, sensors = fleet
    r = client.post("/fleet/api/high-sensitivity", json={"seconds": 60, "sensors": ["a"]})
    assert r.status_code == 200 and set(r.json()["results"]) == {"a"}


def test_all_offline_is_502_and_bad_bodies_400(fleet: Any) -> None:
    client, sensors = fleet
    assert client.post("/fleet/api/high-sensitivity", json={"seconds": 0}).status_code == 400
    assert client.post("/fleet/api/high-sensitivity", json={"seconds": 9}).status_code != 400
    bad = {"seconds": 9, "sensors": ["zz"]}
    assert client.post("/fleet/api/high-sensitivity", json=bad).status_code == 400
    sensors.on.clear()
    sensors.down.update(HOSTS)
    assert client.post("/fleet/api/high-sensitivity", json={"seconds": 9}).status_code == 502


def test_stop_and_summaries(fleet: Any) -> None:
    client, sensors = fleet
    client.post("/fleet/api/high-sensitivity", json={"seconds": 60, "id": "r"})
    d = client.delete("/fleet/api/high-sensitivity/r")
    assert d.status_code == 200 and sensors.on == {}
    g = client.get("/fleet/api/high-sensitivity/r").json()
    assert set(g["sensors"]) == {"a", "b", "c"} and g["sensors"]["a"]["id"] == "r"


def test_fleet_schedule_passes_the_flag_through(fleet: Any) -> None:
    client, sensors = fleet
    seen: list[dict[str, Any]] = []
    orig = sensors.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/schedules" and request.method == "POST":
            seen.append(json.loads(request.content))
            return httpx.Response(201, json={"schedule": {}, "next_runs": [], "warnings": []})
        return orig(request)

    sensors.handler = handler  # type: ignore[method-assign]
    client.app.state.hub._client._transport = httpx.MockTransport(handler)  # type: ignore[attr-defined]
    body = {
        "start": "2026-11-19T16:00:00Z",
        "duration_ms": 60000,
        "high_sensitivity": True,
        "sensors": ["a"],
    }
    assert client.post("/fleet/api/schedules", json=body).status_code == 201
    assert seen and seen[0]["high_sensitivity"] is True


def test_fleet_refuses_ids_that_are_not_plain(fleet: Any) -> None:
    client, sensors = fleet
    r = client.post("/fleet/api/high-sensitivity", json={"seconds": 10, "id": "daily/0dB"})
    assert r.status_code == 400 and sensors.posted == []
