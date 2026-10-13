"""Fleet mode: the sensor registry, the sensor hub, the fleet app, and the
sensor's /api/identity."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.fleet import sensors as sensors_mod
from rfobserver.fleet.app import create_fleet_app
from rfobserver.fleet.registry import PALETTE, Registry, Sensor, parse_address
from rfobserver.fleet.sensors import SensorHub, proxy_allowed
from rfobserver.web.app import create_app

if TYPE_CHECKING:
    from pathlib import Path


# --- a fake fleet of sensors on one transport ---


class FakeSensors:
    """Answers like RFObserver sensors at ``host:port`` keys; counts requests."""

    def __init__(self) -> None:
        self.hosts: dict[str, str] = {}  # "host:port" -> sensor id
        self.down: set[str] = set()
        self.calls: list[str] = []
        self.delay = 0.0
        self.in_flight = 0
        self.max_in_flight = 0

    def add(self, sensor_id: str, host: str, port: int = 80) -> None:
        self.hosts[f"{host}:{port}"] = sensor_id

    async def handler(self, request: httpx.Request) -> httpx.Response:
        key = f"{request.url.host}:{request.url.port or 80}"
        self.calls.append(f"{key}{request.url.path}")
        if key in self.down or key not in self.hosts:
            raise httpx.ConnectError("refused", request=request)
        sid = self.hosts[key]
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
        finally:
            self.in_flight -= 1
        path = request.url.path
        if path == "/api/identity":
            return httpx.Response(
                200,
                json={
                    "id": sid,
                    "hostname": sid,
                    "sensor_name": f"name-{sid}",
                    "version": "0.11.0",
                    "center_freq_hz": 915e6,
                    "sample_rate_hz": 20e6,
                    "gain_db": 40.0,
                    "active": True,
                },
            )
        if path == "/api/averaged/stats":
            q = request.url.params
            return httpx.Response(
                200,
                json={
                    "sdr_center": q.get("sdr_center"),
                    "points": [
                        {
                            "start_time": "2026-10-13T00:00:00Z",
                            "pwr_avg": -50.0,
                            "kurtosis": 3.1,
                            "extra": 1,
                        },
                    ],
                },
            )
        if path == "/api/averaged/waterfall":
            return httpx.Response(
                200, content=b"\x00" * 16, headers={"content-type": "application/octet-stream"}
            )
        if path == "/api/health":
            return httpx.Response(200, json={"status": "ok", "sensor": sid})
        return httpx.Response(404, json={"detail": "nope"})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


# --- parse_address ---


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("10.1.42.31", ("10.1.42.31", 80)),
        ("10.1.42.31:8888", ("10.1.42.31", 8888)),
        ("http://nano.local:8888/", ("nano.local", 8888)),
        (" host ", ("host", 80)),
        ("[fe80::1]:8080", ("fe80::1", 8080)),
        ("[fe80::1]", ("fe80::1", 80)),
    ],
)
def test_parse_address(text: str, expected: tuple[str, int]) -> None:
    assert parse_address(text) == expected


@pytest.mark.parametrize("text", ["", "host:0", "host:70000", "host:x", "host/path", "http://"])
def test_parse_address_rejects(text: str) -> None:
    with pytest.raises(ValueError):
        parse_address(text)


# --- Sensor / Registry ---


def test_sensor_base_url_and_colour() -> None:
    assert Sensor("a", "10.0.0.1").base_url == "http://10.0.0.1"
    assert Sensor("a", "10.0.0.1", 8888).base_url == "http://10.0.0.1:8888"
    assert Sensor("a", "fe80::1", 8888).base_url == "http://[fe80::1]:8888"
    assert Sensor("a", "x", slot=len(PALETTE)).colour == PALETTE[0]
    assert Sensor("a", "x").public()["label"] == "a"


def test_registry_slots_are_stable_and_persist(tmp_path: Path) -> None:
    path = tmp_path / "fleet.json"
    reg = Registry(path)
    reg.add("a", "10.0.0.1", 80, source="manual")
    reg.add("b", "10.0.0.2", 80, source="manual")
    reg.add("c", "10.0.0.3", 80, source="manual")
    assert reg.remove("b")
    # The freed slot is reused; a and c keep theirs.
    d = reg.add("d", "10.0.0.4", 80, source="manual")
    assert d.slot == 1
    again = Registry(path)
    assert [(s.id, s.slot) for s in again.all()] == [("a", 0), ("d", 1), ("c", 2)]
    assert not reg.remove("nope")


def test_registry_add_same_id_updates_address(tmp_path: Path) -> None:
    reg = Registry(tmp_path / "fleet.json")
    reg.seen_on_mdns("a", ["10.0.0.1"], 80)
    s = reg.add("a", "10.0.0.9", 8888, source="manual")
    assert (s.address, s.port, s.source, s.slot) == ("10.0.0.9", 8888, "manual", 0)
    assert len(reg.all()) == 1


def test_registry_mdns_does_not_move_a_manual_address(tmp_path: Path) -> None:
    reg = Registry(tmp_path / "fleet.json")
    reg.add("a", "10.9.9.9", 80, source="manual")
    s = reg.seen_on_mdns("a", ["10.0.0.1", "10.0.0.2"], 80)
    assert s.address == "10.9.9.9"
    assert s.alt_addresses == ["10.0.0.1", "10.0.0.2"]


def test_registry_mdns_follows_an_mdns_sensor(tmp_path: Path) -> None:
    reg = Registry(tmp_path / "fleet.json")
    reg.seen_on_mdns("a", ["10.0.0.1"], 80)
    s = reg.seen_on_mdns("a", ["10.0.0.5", "10.0.0.1"], 80)
    assert s.address == "10.0.0.1"  # still listed, kept
    assert s.alt_addresses == ["10.0.0.5"]
    s = reg.seen_on_mdns("a", ["10.0.0.7"], 81)
    assert (s.address, s.port, s.alt_addresses) == ("10.0.0.7", 81, [])


def test_registry_update(tmp_path: Path) -> None:
    reg = Registry(tmp_path / "fleet.json")
    reg.add("a", "10.0.0.1", 80, source="manual")
    s = reg.update("a", label="  NNE 60 ", enabled=False)
    assert s is not None
    assert (s.label, s.enabled) == ("NNE 60", False)
    assert reg.update("nope", label="x") is None


def test_registry_survives_bad_file(tmp_path: Path) -> None:
    path = tmp_path / "fleet.json"
    path.write_text("{not json")
    assert Registry(path).all() == []
    path.write_text(json.dumps({"sensors": [{"id": "a", "address": "x"}, {"bogus": 1}]}))
    assert [s.id for s in Registry(path).all()] == ["a"]


# --- proxy allowlist ---


@pytest.mark.parametrize(
    "path",
    [
        "averaged/stats",
        "averaged/waterfall",
        "averaged",
        "health",
        "identity",
        "iq-captures",
        "detections.json",
        "ui-prefs",
    ],
)
def test_proxy_allows_dashboard_reads(path: str) -> None:
    assert proxy_allowed(path)


@pytest.mark.parametrize(
    "path",
    ["config", "captures/x/download", "sensor/arm", "healthz", "averagedx", "../config"],
)
def test_proxy_refuses_the_rest(path: str) -> None:
    assert not proxy_allowed(path)


# --- SensorHub ---


@pytest.fixture
def fake() -> FakeSensors:
    f = FakeSensors()
    f.add("a", "10.0.0.1")
    f.add("b", "10.0.0.2", 8888)
    return f


async def test_hub_caches_and_marks_status(tmp_path: Path, fake: FakeSensors) -> None:
    reg = Registry(tmp_path / "fleet.json")
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    hub = SensorHub(reg, transport=fake.transport())
    try:
        r1 = await hub.get(a, "health")
        r2 = await hub.get(a, "health")
        assert (r1.cached, r2.cached) == (False, True)
        assert len([c for c in fake.calls if c.endswith("/api/health")]) == 1
        assert hub.status("a").online
        fake.down.add("10.0.0.1:80")
        with pytest.raises(httpx.HTTPError):
            await hub.get(a, "health", cache=False)
        assert not hub.status("a").online
        assert "ConnectError" in (hub.status("a").error or "")
    finally:
        await hub.close()


async def test_hub_cache_ttl_open_vs_closed() -> None:
    now = datetime.now(timezone.utc)
    old = (now - timedelta(hours=2)).isoformat()
    recent = now.isoformat()
    assert sensors_mod._cache_ttl({}) == sensors_mod.CACHE_OPEN_SEC
    assert sensors_mod._cache_ttl({"until": recent}) == sensors_mod.CACHE_OPEN_SEC
    assert sensors_mod._cache_ttl({"until": old}) == sensors_mod.CACHE_CLOSED_SEC
    assert sensors_mod._cache_ttl({"until": "garbage"}) == sensors_mod.CACHE_OPEN_SEC


async def test_hub_does_not_cache_errors(tmp_path: Path, fake: FakeSensors) -> None:
    reg = Registry(tmp_path / "fleet.json")
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    hub = SensorHub(reg, transport=fake.transport())
    try:
        assert (await hub.get(a, "detections.json")).status == 404
        assert not (await hub.get(a, "detections.json")).cached
    finally:
        await hub.close()


async def test_hub_cache_evicts_by_count(
    tmp_path: Path, fake: FakeSensors, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sensors_mod, "CACHE_MAX_ENTRIES", 3)
    reg = Registry(tmp_path / "fleet.json")
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    hub = SensorHub(reg, transport=fake.transport())
    try:
        for i in range(5):
            await hub.get(a, "health", {"n": str(i)})
        assert len(hub._cache) == 3
        assert (await hub.get(a, "health", {"n": "4"})).cached
        assert not (await hub.get(a, "health", {"n": "0"})).cached
    finally:
        await hub.close()


async def test_hub_limits_concurrency_per_sensor(tmp_path: Path, fake: FakeSensors) -> None:
    reg = Registry(tmp_path / "fleet.json")
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    hub = SensorHub(reg, transport=fake.transport())
    fake.delay = 0.02
    try:
        await asyncio.gather(*(hub.get(a, "averaged/waterfall", {"n": str(i)}) for i in range(4)))
        assert fake.max_in_flight == 1
        fake.max_in_flight = 0
        await asyncio.gather(*(hub.get(a, "health", {"n": str(i)}) for i in range(6)))
        assert fake.max_in_flight == 2
    finally:
        await hub.close()


async def test_hub_waiters_share_one_request(tmp_path: Path, fake: FakeSensors) -> None:
    reg = Registry(tmp_path / "fleet.json")
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    hub = SensorHub(reg, transport=fake.transport())
    fake.delay = 0.02
    try:
        replies = await asyncio.gather(*(hub.get(a, "averaged/waterfall") for _ in range(3)))
        assert [r.cached for r in replies].count(False) == 1
        assert len([c for c in fake.calls if c.endswith("waterfall")]) == 1
    finally:
        await hub.close()


async def test_hub_refresh_falls_back_to_alt_address(tmp_path: Path, fake: FakeSensors) -> None:
    fake.add("a", "10.0.0.5")
    fake.down.add("10.0.0.1:80")
    reg = Registry(tmp_path / "fleet.json")
    s = reg.seen_on_mdns("a", ["10.0.0.1", "10.0.0.5"], 80)
    hub = SensorHub(reg, transport=fake.transport())
    try:
        await hub.refresh(s)
        st = hub.status("a")
        assert st.online and st.address_used == "10.0.0.5"
        assert reg.get("a").address == "10.0.0.5"  # type: ignore[union-attr]
        fake.down.add("10.0.0.5:80")
        await hub.refresh(reg.get("a"))  # type: ignore[arg-type]
        assert not hub.status("a").online
    finally:
        await hub.close()


async def test_fetch_identity_rejects_non_sensor(tmp_path: Path) -> None:
    transport = httpx.MockTransport(lambda r: httpx.Response(200, json={"status": "ok"}))
    hub = SensorHub(Registry(tmp_path / "fleet.json"), transport=transport)
    try:
        with pytest.raises(ValueError):
            await hub.fetch_identity("http://10.0.0.1")
    finally:
        await hub.close()


# --- the fleet app ---


@pytest.fixture
def fleet_client(tmp_path: Path, fake: FakeSensors) -> Any:
    app = create_fleet_app(tmp_path, mdns=False, transport=fake.transport())
    with TestClient(app) as client:
        yield client


def test_fleet_first_run_shows_settings(fleet_client: TestClient) -> None:
    r = fleet_client.get("/")
    assert r.status_code == 200
    assert "No sensors yet" in r.text


def test_fleet_add_list_patch_remove(fleet_client: TestClient) -> None:
    r = fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.2:8888"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["id"], body["online"], body["colour"]) == ("b", True, PALETTE[0])
    assert body["sensor_name"] == "name-b"
    assert (body["label"], body["custom_label"]) == ("name-b", "")
    assert body["tuning"]["center_freq_hz"] == 915e6

    r = fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.1"})
    assert r.json()["colour"] == PALETTE[1]

    # Adding the same sensor again by address does not duplicate it.
    fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.2:8888"})
    ids = [s["id"] for s in fleet_client.get("/fleet/api/sensors").json()["sensors"]]
    assert ids == ["b", "a"]

    r = fleet_client.patch("/fleet/api/sensors/b", json={"label": "NNE 60", "enabled": False})
    assert (r.json()["label"], r.json()["custom_label"]) == ("NNE 60", "NNE 60")
    assert r.json()["enabled"] is False
    assert fleet_client.patch("/fleet/api/sensors/zz", json={}).status_code == 404

    assert fleet_client.delete("/fleet/api/sensors/a").status_code == 200
    assert fleet_client.delete("/fleet/api/sensors/a").status_code == 404


def test_fleet_add_rejects_bad_or_silent_address(fleet_client: TestClient) -> None:
    assert fleet_client.post("/fleet/api/sensors", json={"address": "a/b"}).status_code == 400
    r = fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.99"})
    assert r.status_code == 400
    assert "No RFObserver sensor" in r.json()["detail"]


def test_fleet_dashboard_selects_a_sensor(fleet_client: TestClient) -> None:
    fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.1"})
    fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.2:8888"})
    r = fleet_client.get("/")
    assert r.status_code == 200
    assert '"/fleet/s/a"' in r.text  # first online sensor
    r = fleet_client.get("/?sensor=b")
    assert '"/fleet/s/b"' in r.text
    assert 'id="fleet-select"' in r.text
    # A hidden sensor is not selectable.
    fleet_client.patch("/fleet/api/sensors/b", json={"enabled": False})
    assert '"/fleet/s/a"' in fleet_client.get("/?sensor=b").text


def test_fleet_settings_page(fleet_client: TestClient) -> None:
    r = fleet_client.get("/settings")
    assert r.status_code == 200
    assert 'id="fleet-table"' in r.text


def test_fleet_proxy(fleet_client: TestClient, fake: FakeSensors) -> None:
    fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.1"})
    r = fleet_client.get("/fleet/s/a/api/health")
    assert r.status_code == 200 and r.json()["sensor"] == "a"
    assert r.headers["x-fleet-cache"] == "miss"
    assert fleet_client.get("/fleet/s/a/api/health").headers["x-fleet-cache"] == "hit"
    assert fleet_client.get("/fleet/s/a/api/config").status_code == 404
    assert fleet_client.get("/fleet/s/zz/api/health").status_code == 404
    fake.down.add("10.0.0.1:80")
    assert fleet_client.get("/fleet/s/a/api/health?x=1").status_code == 502


def test_fleet_stats_fans_out(fleet_client: TestClient, fake: FakeSensors) -> None:
    fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.1"})
    fleet_client.post("/fleet/api/sensors", json={"address": "10.0.0.2:8888"})
    fake.down.add("10.0.0.2:8888")
    r = fleet_client.get(
        "/fleet/api/stats",
        params={"since": "2026-10-13T00:00:00Z", "until": "2026-10-13T01:00:00Z"},
    )
    assert r.status_code == 200
    by_id = {s["id"]: s for s in r.json()["sensors"]}
    assert by_id["a"]["error"] is None
    assert by_id["a"]["points"] == [
        {"start_time": "2026-10-13T00:00:00Z", "pwr_avg": -50.0, "kurtosis": 3.1}
    ]
    assert by_id["a"]["colour"] == PALETTE[0]
    assert by_id["b"]["points"] == [] and by_id["b"]["error"]
    stats_calls = [c for c in fake.calls if "10.0.0.1:80/api/averaged/stats" in c]
    assert stats_calls  # queried at the sensor's own tuning (see FakeSensors)


def test_fleet_ui_prefs(fleet_client: TestClient, tmp_path: Path) -> None:
    r = fleet_client.put("/api/ui-prefs", json={"theme": "dark"})
    assert r.status_code == 200 and r.json()["theme"] == "dark"
    assert fleet_client.get("/api/ui-prefs").json()["theme"] == "dark"
    assert json.loads((tmp_path / "prefs.json").read_text())["theme"] == "dark"
    assert fleet_client.put("/api/ui-prefs", json={"theme": "neon"}).status_code in (400, 422)
    assert fleet_client.put("/api/ui-prefs", content=b"[1]").status_code == 400


def test_fleet_health(fleet_client: TestClient) -> None:
    assert fleet_client.get("/api/health").json()["mode"] == "fleet"


# --- the sensor side ---


def test_sensor_identity() -> None:
    settings = AppSettings(_env_file=None, HOSTNAME="hcro-jet-009", SENSOR_NAME="NNE 60")
    client = TestClient(create_app(settings))
    body = client.get("/api/identity").json()
    assert body["id"] == body["hostname"] == "hcro-jet-009"
    assert body["sensor_name"] == "NNE 60"
    assert body["center_freq_hz"] == float(settings.FREQUENCY_START)
    assert body["active"] is None  # no pipeline attached


# --- mDNS address choice ---


def test_rank_addresses_prefers_local_subnets() -> None:
    import ipaddress

    from rfobserver.mdns import rank_addresses

    nets = [ipaddress.IPv4Network("192.168.97.0/24")]
    got = rank_addresses(["172.18.0.1", "192.168.97.153", "10.0.0.1", "bad"], nets)
    assert got == ["192.168.97.153", "172.18.0.1", "10.0.0.1", "bad"]
    assert rank_addresses(["a", "b"], []) == ["a", "b"]
    two = [ipaddress.IPv4Network("192.168.97.0/24"), ipaddress.IPv4Network("10.1.0.0/24")]
    assert rank_addresses(["10.1.0.5", "8.8.8.8", "192.168.97.9"], two) == [
        "192.168.97.9",
        "10.1.0.5",
        "8.8.8.8",
    ]


def test_advertised_addresses_honour_bind_host() -> None:
    from rfobserver.mdns import local_ipv4_addresses

    assert local_ipv4_addresses("10.1.2.3") == ["10.1.2.3"]
    for addr in local_ipv4_addresses():
        assert not addr.startswith("127.")
