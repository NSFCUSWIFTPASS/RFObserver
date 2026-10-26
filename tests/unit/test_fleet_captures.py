"""Fleet Captures: every sensor's captures merged into one list, and each
capture's detail, PSD, detections and files read through the fleet."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi.testclient import TestClient

from rfobserver.fleet.app import create_fleet_app
from rfobserver.fleet.captures import merge_page, parse_cursor

if TYPE_CHECKING:
    from pathlib import Path


def cap(name: str, t: str | None) -> dict[str, Any]:
    return {
        "filename": f"{name}.sc16",
        "origin": "auto",
        "size_bytes": 4000,
        "has_psd": True,
        "files": [{"name": f"{name}.sc16", "size_bytes": 4000}],
        "sigmf": {"meta": f"{name}.sigmf-meta", "data": f"{name}.sigmf-data"},
        "meta": None if t is None else {"start_time": t},
    }


# --- merging ---


def test_merge_takes_the_newest_heads_and_advances_each_cursor() -> None:
    a, b = {"id": "a"}, {"id": "b"}
    rows, cursor = merge_page(
        [
            (
                a,
                [cap("a1", "2026-10-26T10:00:05+00:00"), cap("a2", "2026-10-26T10:00:01+00:00")],
                0,
            ),
            (
                b,
                [cap("b1", "2026-10-26T10:00:03+00:00"), cap("b2", "2026-10-26T09:00:00+00:00")],
                4,
            ),
        ],
        3,
    )
    assert [(r["sensor"]["id"], r["filename"]) for r in rows] == [
        ("a", "a1.sc16"),
        ("b", "b1.sc16"),
        ("a", "a2.sc16"),
    ]
    assert cursor == {"a": 2, "b": 5}


def test_merge_keeps_each_sensors_own_order() -> None:
    # A sensor's list is newest-first by its index; a capture without a start
    # time (or out of order) must not make the merge skip one of its others.
    a = {"id": "a"}
    rows, cursor = merge_page(
        [
            (
                a,
                [
                    cap("x", "2026-10-26T10:00:00+00:00"),
                    cap("y", None),
                    cap("z", "2026-10-26T11:00:00+00:00"),
                ],
                0,
            )
        ],
        2,
    )
    assert [r["filename"] for r in rows] == ["x.sc16", "y.sc16"]
    assert cursor == {"a": 2}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, {}),
        ("", {}),
        ("a:3,b:0", {"a": 3, "b": 0}),
        ("a:x,b:2,c", {"b": 2}),
        ("a:-1", {}),
    ],
)
def test_parse_cursor(raw: str | None, expected: dict[str, int]) -> None:
    assert parse_cursor(raw) == expected


# --- endpoints ---


class Sensors:
    """Two sensors with captures; 'b' also counts how often it was asked."""

    def __init__(self) -> None:
        self.caps = {
            "10.0.0.1": [
                cap("a1", "2026-10-26T10:00:05+00:00"),
                cap("a2", "2026-10-26T10:00:01+00:00"),
            ],
            "10.0.0.2": [cap("b1", "2026-10-26T10:00:03+00:00")],
        }
        self.data = bytes(range(256)) * 4
        self.seen: list[httpx.Request] = []
        self.down: set[str] = set()

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request)
        host, path, q = request.url.host, request.url.path, request.url.params
        if host in self.down:
            raise httpx.ConnectError("refused", request=request)
        if path == "/api/identity":
            return httpx.Response(200, json={"id": "a" if host == "10.0.0.1" else "b"})
        if path == "/captures/list":
            rows = self.caps[host]
            off, lim = int(q.get("offset", 0)), int(q.get("limit", 1000))
            return httpx.Response(
                200, json=rows[off : off + lim], headers={"X-Total-Count": str(len(rows))}
            )
        if path == "/captures/detail/a1.sc16":
            return httpx.Response(200, json={"filename": "a1.sc16", "meta": {"gain_db": 40}})
        if path == "/captures/psd/a1.sc16":
            return httpx.Response(200, json={"grid": [[1.0]], "start": int(q.get("start", 0))})
        if path == "/captures/detections/a1.sc16":
            return httpx.Response(200, json={"detections": [], "pending": False})
        if path == "/captures/download/a1.sc16":
            rng = request.headers.get("range")
            disp = {
                "content-disposition": 'attachment; filename="a1.sc16"',
                "accept-ranges": "bytes",
            }
            if rng:
                lo = int(rng.split("=")[1].split("-")[0])
                body = self.data[lo:]
                return httpx.Response(
                    206,
                    content=b"" if request.method == "HEAD" else body,
                    headers={
                        **disp,
                        "content-range": f"bytes {lo}-{len(self.data) - 1}/{len(self.data)}",
                        "content-length": str(len(body)),
                        "content-type": "application/octet-stream",
                    },
                )
            return httpx.Response(
                200,
                content=b"" if request.method == "HEAD" else self.data,
                headers={
                    **disp,
                    "content-length": str(len(self.data)),
                    "content-type": "application/octet-stream",
                },
            )
        if path.startswith("/captures/download/"):
            return httpx.Response(409, json={"detail": "Capture is still being recorded"})
        return httpx.Response(404, json={"detail": "nope"})


@pytest.fixture
def fleet(tmp_path: Path) -> Any:
    sensors = Sensors()
    app = create_fleet_app(tmp_path, mdns=False, transport=httpx.MockTransport(sensors.handler))
    app.state.registry.add("a", "10.0.0.1", 80, source="manual")
    app.state.registry.add("b", "10.0.0.2", 80, source="manual")
    with TestClient(app) as client:
        yield client, sensors


def test_list_merges_pages_and_tags_sensors(fleet: Any) -> None:
    client, _ = fleet
    body = client.get("/fleet/api/captures", params={"limit": 2}).json()
    assert [(r["sensor"]["id"], r["filename"]) for r in body["captures"]] == [
        ("a", "a1.sc16"),
        ("b", "b1.sc16"),
    ]
    assert body["total"] == 3 and body["errors"] == {}
    assert body["sensor"] is None
    nxt = client.get("/fleet/api/captures", params={"limit": 2, "cursor": body["cursor"]}).json()
    assert [r["filename"] for r in nxt["captures"]] == ["a2.sc16"]
    assert nxt["more"] is False and body["more"] is True


def test_list_one_sensor_and_a_sensor_down(fleet: Any) -> None:
    client, sensors = fleet
    one = client.get("/fleet/api/captures", params={"sensor": "b"}).json()
    assert [r["filename"] for r in one["captures"]] == ["b1.sc16"] and one["total"] == 1
    sensors.down.add("10.0.0.2")
    body = client.get("/fleet/api/captures").json()
    assert [r["filename"] for r in body["captures"]] == ["a1.sc16", "a2.sc16"]
    assert "b" in body["errors"]


def test_list_is_not_served_stale_after_a_new_capture(fleet: Any) -> None:
    client, sensors = fleet
    client.get("/fleet/api/captures")
    sensors.caps["10.0.0.2"].insert(0, cap("b0", "2026-10-26T12:00:00+00:00"))
    body = client.get("/fleet/api/captures").json()
    assert body["captures"][0]["filename"] == "b0.sc16"


@pytest.mark.parametrize("kind", ["detail", "psd", "detections"])
def test_capture_reads_go_through(fleet: Any, kind: str) -> None:
    client, sensors = fleet
    r = client.get(f"/fleet/s/a/captures/{kind}/a1.sc16", params={"start": 500})
    assert r.status_code == 200, r.text
    seen = sensors.seen[-1]
    assert seen.url.path == f"/captures/{kind}/a1.sc16"
    if kind == "psd":
        assert r.json()["start"] == 500


def test_capture_reads_refuse_writes_and_unknowns(fleet: Any) -> None:
    client, _ = fleet
    assert client.post("/fleet/s/a/captures/redetect/a1.sc16").status_code in (404, 405)
    assert client.get("/fleet/s/a/captures/list").status_code == 404
    assert client.get("/fleet/s/nope/captures/detail/a1.sc16").status_code == 404
    assert client.get("/fleet/s/a/captures/detail/missing.sc16").status_code == 404


def test_download_streams_with_range(fleet: Any) -> None:
    client, sensors = fleet
    r = client.get("/fleet/s/a/captures/download/a1.sc16")
    assert r.status_code == 200 and r.content == sensors.data
    assert r.headers["content-disposition"] == 'attachment; filename="a1.sc16"'
    assert r.headers["accept-ranges"] == "bytes"
    part = client.get("/fleet/s/a/captures/download/a1.sc16", headers={"Range": "bytes=1000-"})
    assert part.status_code == 206 and part.content == sensors.data[1000:]
    assert part.headers["content-range"] == f"bytes 1000-1023/{len(sensors.data)}"
    head = client.head("/fleet/s/a/captures/download/a1.sc16")
    assert head.status_code == 200 and head.headers["content-length"] == str(len(sensors.data))
    busy = client.get("/fleet/s/a/captures/download/rec.sc16")
    assert busy.status_code == 409 and "recorded" in busy.json()["detail"]


def test_download_from_a_sensor_that_is_down_is_502(fleet: Any) -> None:
    client, sensors = fleet
    sensors.down.add("10.0.0.1")
    assert client.get("/fleet/s/a/captures/download/a1.sc16").status_code == 502


def test_captures_page_is_the_fleet_version(fleet: Any) -> None:
    client, _ = fleet
    html = client.get("/captures").text
    assert "Replay a Raw File" not in html and "replay-raw-start-btn" not in html
    assert 'id="redetect-btn"' not in html and "/captures/redetect/" not in html
    assert "/fleet/api/captures" in html
    assert 'href="/captures"' in html  # in the fleet nav
    assert client.get("/captures/", follow_redirects=False).status_code in (200, 307, 308)


def test_sensor_captures_page_keeps_replay_and_redetect(tmp_path: Path, monkeypatch: Any) -> None:
    from rfobserver.config import AppSettings
    from rfobserver.web.app import create_app

    monkeypatch.chdir(tmp_path)
    app = create_app(AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path)))
    with TestClient(app) as c:
        html = c.get("/captures/").text
    assert "Replay a Raw File" in html and 'id="redetect-btn"' in html
    assert json.dumps("/fleet/api/captures") not in html
