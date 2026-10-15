"""Fleet Detections: attribution summaries, merging sensors, and the
/fleet/api/detections and /fleet/api/detections/hourly endpoints."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi.testclient import TestClient

from rfobserver.fleet.app import create_fleet_app
from rfobserver.fleet.detections import attribution_summary, matches, merge

if TYPE_CHECKING:
    from pathlib import Path

DECODED = json.dumps(
    {
        "decoded": True,
        "at": "2026-10-15T15:05:28+00:00",
        "frames": [
            {
                "time": "@0.002s",
                "model": "Acurite-Tower",
                "id": 1234,
                "temperature_C": 21.5,
                "battery_ok": True,  # booleans are left out
                "count": 1,
                "num_rows": 1,
                "rows": [{"len": 592, "data": "7a2ab5b3f7cadffeea2450f0aa"}],
            },
            {"model": "Acurite-Tower"},
        ],
    }
)
TRIED = json.dumps({"attempted": True, "decoded": False, "at": "x"})


def det(i: int, t: str, model: str | None = None, attribution: str | None = None) -> dict[str, Any]:
    return {
        "id": i,
        "burst_id": f"b{i}",
        "start_time": t,
        "center_freq_hz": 915e6,
        "bandwidth_hz": 1e5,
        "duration_ms": 2.0,
        "peak_power_db": -40.0,
        "model": model,
        "protocol_id": None,
        "attribution": attribution,
    }


def test_summary_of_a_decoded_burst() -> None:
    s = attribution_summary(det(1, "t", "Acurite-Tower", DECODED))
    assert s["status"] == "decoded"
    assert s["fields"] == [["id", 1234], ["temperature_C", 21.5]]
    assert (s["bits"], s["frames"]) == (592, 2)
    assert s["data"] == "7a2ab5b3f7cadffe" and s["data_full"].startswith("7a2ab5b3f7cadffeea")


def test_summary_states_and_bad_records() -> None:
    assert attribution_summary(det(1, "t", None, TRIED))["status"] == "tried"
    assert attribution_summary(det(1, "t"))["status"] == "not_tried"
    broken = attribution_summary(det(1, "t", "M", "{not json"))
    assert broken["status"] == "decoded" and broken["fields"] == []
    assert attribution_summary(det(1, "t", "M", json.dumps({"frames": "x"})))["frames"] == 0


def test_matches_applies_filters() -> None:
    d, t, n = det(1, "t", "M", DECODED), det(2, "t", None, TRIED), det(3, "t")
    assert [matches(x, True, None, None) for x in (d, t, n)] == [True, False, False]
    assert [matches(x, False, None, None) for x in (d, t, n)] == [False, True, True]
    assert [matches(x, None, None, "tried") for x in (d, t, n)] == [False, True, False]
    assert [matches(x, None, "M", None) for x in (d, t, n)] == [True, False, False]


def test_merge_sorts_newest_first_and_tags_sensors() -> None:
    a = {"id": "a"}
    b = {"id": "b"}
    rows = merge(
        [
            (a, [det(1, "2026-10-15T10:00:03+00:00"), det(2, "2026-10-15T10:00:01+00:00")]),
            (b, [det(3, "2026-10-15T10:00:02.500000+00:00")]),
        ],
        2,
    )
    assert [(r["sensor"]["id"], r["burst_id"]) for r in rows] == [("a", "b1"), ("b", "b3")]
    assert "attribution" in rows[0] and isinstance(rows[0]["attribution"], dict)


# --- endpoints ---


class Sensors:
    """Two sensors: 'new' has the hourly endpoint and filters; 'old' has
    neither (it ignores attributed/model and 404s the hourly endpoint)."""

    def __init__(self) -> None:
        self.rows = {
            "10.0.0.1": [
                det(1, "2026-10-15T10:00:05+00:00", "M", DECODED),
                det(2, "2026-10-15T10:00:03+00:00", None, TRIED),
            ],
            "10.0.0.2": [
                det(3, "2026-10-15T10:00:04+00:00"),
                det(4, "2026-10-15T10:00:01+00:00", "M", DECODED),
            ],
        }
        self.seen: list[httpx.URL] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request.url)
        host = request.url.host
        path = request.url.path
        q = request.url.params
        if path == "/api/identity":
            return httpx.Response(200, json={"id": "new" if host == "10.0.0.1" else "old"})
        if path == "/api/detections/hourly":
            if host == "10.0.0.2":
                return httpx.Response(404, json={"detail": "Not Found"})
            return httpx.Response(
                200,
                json={
                    "covered_since": 1000,
                    "hours": [
                        {
                            "t": 3600,
                            "all": 5,
                            "attributed": 2,
                            "tried": 1,
                            "not_tried": 2,
                            "models": {"M": 2},
                        }
                    ],
                },
            )
        if path == "/api/detections.json":
            rows = self.rows[host]
            if "until" in q:
                rows = [r for r in rows if r["start_time"] < q["until"]]
            if "since" in q:
                rows = [r for r in rows if r["start_time"] >= q["since"]]
            if host == "10.0.0.1":  # the new sensor filters
                if q.get("attributed") == "true":
                    rows = [r for r in rows if r["model"]]
                if q.get("attributed") == "false":
                    rows = [r for r in rows if not r["model"]]
            return httpx.Response(200, json={"detections": rows[: int(q.get("limit", 200))]})
        return httpx.Response(404)


@pytest.fixture
def fleet(tmp_path: Path) -> Any:
    sensors = Sensors()
    app = create_fleet_app(tmp_path, mdns=False, transport=httpx.MockTransport(sensors.handler))
    app.state.registry.add("new", "10.0.0.1", 80, source="manual")
    app.state.registry.add("old", "10.0.0.2", 80, source="manual")
    with TestClient(app) as client:
        yield client, sensors


RANGE = {"since": "2026-10-15T00:00:00+00:00", "until": "2026-10-16T00:00:00+00:00"}


def test_hourly_marks_old_sensors_unsupported(fleet: Any) -> None:
    client, _ = fleet
    body = client.get("/fleet/api/detections/hourly", params=RANGE).json()
    by_id = {s["id"]: s for s in body["sensors"]}
    assert by_id["new"]["supported"] and by_id["new"]["covered_since"] == 1000
    assert by_id["new"]["hours"][0]["attributed"] == 2
    assert by_id["old"]["supported"] is False and by_id["old"]["error"] is None


def test_detections_merge_page_and_filter(fleet: Any) -> None:
    client, sensors = fleet
    body = client.get("/fleet/api/detections", params={**RANGE, "limit": 3}).json()
    assert [r["burst_id"] for r in body["rows"]] == ["b1", "b3", "b2"]
    assert body["rows"][0]["sensor"]["id"] == "new"
    assert body["rows"][0]["attribution"]["status"] == "decoded"
    assert body["next_before"] == "2026-10-15T10:00:03+00:00"
    nxt = client.get(
        "/fleet/api/detections", params={**RANGE, "limit": 3, "until": body["next_before"]}
    ).json()
    assert [r["burst_id"] for r in nxt["rows"]] == ["b4"] and nxt["next_before"] is None

    # The old sensor ignores attributed=true; the fleet filters its rows itself.
    att = client.get("/fleet/api/detections", params={**RANGE, "status": "decoded"}).json()
    assert [r["burst_id"] for r in att["rows"]] == ["b1", "b4"]
    tried = client.get("/fleet/api/detections", params={**RANGE, "status": "tried"}).json()
    assert [r["burst_id"] for r in tried["rows"]] == ["b2"]
    assert any(u.params.get("attributed") == "false" for u in sensors.seen)

    one = client.get("/fleet/api/detections", params={**RANGE, "sensor": "old"}).json()
    assert {r["sensor"]["id"] for r in one["rows"]} == {"old"}


def test_detections_page_renders(fleet: Any) -> None:
    client, _ = fleet
    r = client.get("/detections")
    assert r.status_code == 200
    assert "fleet-detections.js" in r.text and 'href="/detections" class="nav-link"' in r.text
    assert 'data-id="new"' in r.text and 'data-id="old"' in r.text


def test_detection_popup_and_download(fleet: Any) -> None:
    client, _ = fleet
    q = {"sensor": "new", "burst_id": "b1", "start_time": "2026-10-15T10:00:05+00:00"}
    r = client.get("/fleet/api/detection", params=q)
    assert r.status_code == 200 and "content-disposition" not in r.headers
    doc = r.json()
    assert doc["sensor"]["id"] == "new" and doc["detection"]["burst_id"] == "b1"
    assert "attribution" not in doc["detection"]
    assert doc["attribution"]["frames"][0]["rows"][0]["len"] == 592  # the full record

    dl = client.get("/fleet/api/detection", params={**q, "download": "true"})
    assert dl.headers["content-disposition"] == 'attachment; filename="new-b1.json"'
    assert dl.json() == doc


def test_detection_popup_errors(fleet: Any) -> None:
    client, _ = fleet
    q = {"sensor": "new", "burst_id": "b9", "start_time": "2026-10-15T10:00:05+00:00"}
    assert client.get("/fleet/api/detection", params=q).status_code == 404
    assert client.get("/fleet/api/detection", params={**q, "start_time": "x"}).status_code == 400
    assert client.get("/fleet/api/detection", params={**q, "sensor": "nope"}).status_code == 404
