"""Detections page (formerly History): rename, attribution columns, filters, CSV export."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.storage.database import SensorDatabase
from rfobserver.web.app import create_app

UTC = timezone.utc
T0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def settings():
    return AppSettings(_env_file=None)


def _det(burst_id: str, start: datetime, duration_ms: float = 10.0, **extra: Any) -> dict:
    return dict(
        burst_id=burst_id,
        start_time=start,
        stop_time=start + timedelta(milliseconds=duration_ms),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=duration_ms,
        detection_timestamp=start,
        sdr_center_freq_hz=extra.pop("sdr_center_freq_hz", 915e6),
        sample_rate_hz=56e6,
        gain_db=40.0,
        **extra,
    )


async def _add(db: SensorDatabase, burst_id: str, start: datetime, model: Any = None, **kw):
    """Insert one detection; model=None never tried, '' tried with no decode."""
    await db.insert_detection(**_det(burst_id, start, **kw))
    if model is not None:
        await db.update_detection_attribution(
            burst_id=burst_id,
            model=model or None,
            protocol_id=42 if model else None,
            attribution=json.dumps({"model": model}) if model else "{}",
        )


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "det.db"))
    await database.connect()
    yield database
    await database.close()


@pytest.fixture
async def seeded(db):
    """Five rows one minute apart: two decoded models, one tried-no-decode, two never tried."""
    await _add(db, "a", T0, model="SilverSpring-Mesh", duration_ms=5.0)
    await _add(db, "b", T0 + timedelta(minutes=1), model="Acurite-Tower", duration_ms=15.0)
    await _add(db, "c", T0 + timedelta(minutes=2), model="", duration_ms=25.0)
    await _add(db, "d", T0 + timedelta(minutes=3), duration_ms=35.0)
    await _add(db, "e", T0 + timedelta(minutes=4), duration_ms=45.0, sdr_center_freq_hz=2437e6)
    return db


def _ids(rows: list[dict]) -> set[str]:
    return {r["burst_id"] for r in rows}


async def _client(settings, db):
    app = create_app(settings)
    app.state.database = db
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


# -- Rename ------------------------------------------------------------------


def test_detections_page_renders(settings):
    client = TestClient(create_app(settings))
    for path in ("/detections", "/detections/"):
        r = client.get(path, follow_redirects=False)
        assert r.status_code == 200, path
        assert "<title>Detections - RFObserver</title>" in r.text
        assert "<h1>Detections</h1>" in r.text


def test_nav_says_detections(settings):
    r = TestClient(create_app(settings)).get("/detections")
    assert '<a href="/detections" class="nav-link">Detections</a>' in r.text
    assert ">History<" not in r.text


@pytest.mark.parametrize("path", ["/history", "/history/"])
def test_history_redirects_to_detections(settings, path):
    client = TestClient(create_app(settings))
    r = client.get(path + "?sdr_center=915000000", follow_redirects=False)
    assert r.status_code in (301, 308)
    assert r.headers["location"] == "/detections?sdr_center=915000000"
    r = client.get(path, follow_redirects=False)
    assert r.headers["location"] == "/detections"


async def test_page_lists_distinct_models_in_filter(settings, seeded):
    async with await _client(settings, seeded) as ac:
        r = await ac.get("/detections")
    assert r.status_code == 200
    assert '<option value="SilverSpring-Mesh">SilverSpring-Mesh</option>' in r.text
    assert '<option value="Acurite-Tower">Acurite-Tower</option>' in r.text
    for name in ("attributed", "model", "start", "stop"):
        assert f'name="{name}"' in r.text
    assert "Start (UTC)" in r.text and "Stop (UTC)" in r.text
    assert 'id="export-csv"' in r.text


# -- DB layer ----------------------------------------------------------------


async def test_detection_models_distinct_nonempty_sorted(seeded):
    await _add(seeded, "a2", T0 + timedelta(minutes=9), model="SilverSpring-Mesh")
    assert await seeded.detection_models() == ["Acurite-Tower", "SilverSpring-Mesh"]


async def test_query_attributed_filter(seeded):
    assert _ids(await seeded.query_detections(attributed=True)) == {"a", "b"}
    assert _ids(await seeded.query_detections(attributed=False)) == {"c", "d", "e"}
    assert len(await seeded.query_detections(attributed=None)) == 5


async def test_query_model_filter(seeded):
    assert _ids(await seeded.query_detections(model="Acurite-Tower")) == {"b"}
    assert await seeded.query_detections(model="nope") == []


async def test_query_start_stop_utc_boundaries(seeded):
    # Start inclusive, stop exclusive, on the stored "+00:00" ISO strings.
    rows = await seeded.query_detections(
        since=T0 + timedelta(minutes=1), until=T0 + timedelta(minutes=3)
    )
    assert _ids(rows) == {"b", "c"}


async def test_query_fractional_second_boundaries(db):
    stop = T0 + timedelta(minutes=1)
    await _add(db, "just_before", stop - timedelta(microseconds=1))
    await _add(db, "at_stop", stop)
    await _add(db, "just_after", stop + timedelta(microseconds=1))
    assert _ids(await db.query_detections(until=stop)) == {"just_before"}
    assert _ids(await db.query_detections(since=stop)) == {"at_stop", "just_after"}


async def test_query_filters_combine(seeded):
    rows = await seeded.query_detections(
        attributed=False, sdr_center_freq=915e6, since=T0 + timedelta(minutes=2)
    )
    assert _ids(rows) == {"c", "d"}
    rows = await seeded.query_detections(attributed=True, min_duration_ms=10, max_duration_ms=20)
    assert _ids(rows) == {"b"}


async def test_histogram_honours_new_filters(seeded):
    assert (await seeded.duration_histogram(attributed=True))["count"] == 2
    assert (await seeded.duration_histogram(attributed=False))["count"] == 3
    assert (await seeded.duration_histogram(model="SilverSpring-Mesh"))["count"] == 1
    h = await seeded.duration_histogram(
        since=T0 + timedelta(minutes=1), until=T0 + timedelta(minutes=3)
    )
    assert h["count"] == 2 and h["min"] == 15.0 and h["max"] == 25.0


async def test_query_keyset_paging_is_stable_on_ties(db):
    # Same start_time for every row: the id tiebreak must still page without
    # duplicates or gaps.
    for i in range(7):
        await _add(db, f"t{i}", T0)
    seen: list[str] = []
    key = None
    while True:
        page = await db.query_detections(limit=3, before=key)
        if not page:
            break
        seen += [r["burst_id"] for r in page]
        key = (page[-1]["start_time"], page[-1]["id"])
    assert sorted(seen) == sorted(f"t{i}" for i in range(7))
    assert len(seen) == 7


# -- Fragment ----------------------------------------------------------------


async def test_full_fragment_renders_attribution_columns(settings, seeded):
    async with await _client(settings, seeded) as ac:
        r = await ac.get("/api/detections?view=full")
    rows = [line for line in r.text.splitlines() if line.startswith("<tr>")]
    assert len(rows) == 5
    assert all(row.count("<td") == 8 for row in rows)
    assert "<td>true</td><td>SilverSpring-Mesh</td>" in r.text
    assert r.text.count("<td>false</td><td></td>") == 3


async def test_dashboard_fragment_keeps_six_columns(settings, seeded):
    async with await _client(settings, seeded) as ac:
        r = await ac.get("/api/detections")
    rows = [line for line in r.text.splitlines() if line.startswith("<tr>")]
    assert len(rows) == 5
    assert all(row.count("<td") == 6 for row in rows)
    assert "SilverSpring-Mesh" not in r.text


async def test_full_fragment_escapes_model(settings, db):
    await _add(db, "x", T0, model="<script>alert(1)</script>")
    async with await _client(settings, db) as ac:
        r = await ac.get("/api/detections?view=full")
    assert "<script>" not in r.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in r.text


async def test_full_fragment_placeholder_spans_eight(settings, db):
    async with await _client(settings, db) as ac:
        r = await ac.get("/api/detections?view=full")
    assert 'colspan="8"' in r.text


async def test_fragment_new_filters(settings, seeded):
    async with await _client(settings, seeded) as ac:
        t = (await ac.get("/api/detections?view=full&attributed=true")).text
        assert "SilverSpring-Mesh" in t and "Acurite-Tower" in t and "false" not in t
        f = (await ac.get("/api/detections?view=full&attributed=false")).text
        assert "Mesh" not in f and f.count("<tr>") == 3
        m = (await ac.get("/api/detections?view=full&model=Acurite-Tower")).text
        assert m.count("<tr>") == 1 and "Acurite-Tower" in m
        # Naive datetime-local values are UTC; stop is exclusive.
        w = (
            await ac.get(
                "/api/detections?view=full&start=2026-03-01T12:01&stop=2026-03-01T12:03:00"
            )
        ).text
        assert w.count("<tr>") == 2
        assert "12:01:00" in w and "12:02:00" in w
        both = (
            await ac.get("/api/detections?view=full&attributed=true&start=2026-03-01T12:01")
        ).text
        assert both.count("<tr>") == 1 and "Acurite-Tower" in both
        # Empty 'All' values are unfiltered.
        a = (await ac.get("/api/detections?view=full&attributed=&model=&start=&stop=")).text
        assert a.count("<tr>") == 5


async def test_histogram_endpoint_honours_new_filters(settings, seeded):
    async with await _client(settings, seeded) as ac:
        r = await ac.get("/api/detections/histogram?bin_width=10&attributed=true")
        assert r.text.count('data-count="1"') == 2
        assert 'data-lo="20"' not in r.text  # the 25 ms tried-no-decode row is out
        r = await ac.get("/api/detections/histogram?bin_width=10&model=SilverSpring-Mesh")
        assert r.text.count("hist-bar") == 1 and 'data-lo="0"' in r.text
        r = await ac.get(
            "/api/detections/histogram?bin_width=10&start=2026-03-01T12:03&stop=2026-03-01T12:10"
        )
        assert 'data-lo="30"' in r.text and 'data-lo="40"' in r.text
        assert 'data-lo="10"' not in r.text


# -- CSV export ---------------------------------------------------------------


def _parse_csv(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text)))


async def test_csv_header_and_disposition(settings, seeded):
    async with await _client(settings, seeded) as ac:
        r = await ac.get("/api/detections.csv")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    disp = r.headers["content-disposition"]
    assert disp.startswith('attachment; filename="detections_') and disp.endswith('Z.csv"')
    header = next(csv.reader(io.StringIO(r.text)))
    for col in (
        "id",
        "burst_id",
        "start_time",
        "stop_time",
        "center_freq_hz",
        "duration_ms",
        "sdr_center_freq_hz",
        "model",
        "protocol_id",
        "attribution",
    ):
        assert col in header, col
    assert header[-2:] == ["attributed", "burst_attribution"]


async def test_csv_row_values(settings, seeded):
    async with await _client(settings, seeded) as ac:
        rows = _parse_csv((await ac.get("/api/detections.csv")).text)
    by_id = {r["burst_id"]: r for r in rows}
    assert by_id["a"]["attributed"] == "true"
    assert by_id["a"]["burst_attribution"] == "SilverSpring-Mesh"
    assert by_id["a"]["protocol_id"] == "42"
    assert by_id["a"]["attribution"] == '{"model": "SilverSpring-Mesh"}'
    assert by_id["c"]["attributed"] == "false" and by_id["c"]["burst_attribution"] == ""
    assert by_id["d"]["attributed"] == "false"
    # Newest first.
    assert [r["burst_id"] for r in rows] == ["e", "d", "c", "b", "a"]


async def test_csv_exports_all_rows_beyond_page_limit(settings, db):
    for i in range(120):
        await _add(db, f"r{i:03d}", T0 + timedelta(seconds=i))
    async with await _client(settings, db) as ac:
        rows = _parse_csv((await ac.get("/api/detections.csv")).text)
    assert len(rows) == 120
    assert len({r["burst_id"] for r in rows}) == 120


async def test_csv_applies_filters(settings, seeded):
    async with await _client(settings, seeded) as ac:
        t = _parse_csv((await ac.get("/api/detections.csv?attributed=true")).text)
        assert {r["burst_id"] for r in t} == {"a", "b"}
        m = _parse_csv((await ac.get("/api/detections.csv?model=Acurite-Tower")).text)
        assert [r["burst_id"] for r in m] == ["b"]
        w = _parse_csv(
            (await ac.get("/api/detections.csv?start=2026-03-01T12:01&stop=2026-03-01T12:03")).text
        )
        assert {r["burst_id"] for r in w} == {"b", "c"}
        s = _parse_csv((await ac.get("/api/detections.csv?sdr_center=2437000000")).text)
        assert [r["burst_id"] for r in s] == ["e"]
        d = _parse_csv(
            (
                await ac.get("/api/detections.csv?duration_min=20&duration_max=40&attributed=false")
            ).text
        )
        assert {r["burst_id"] for r in d} == {"c", "d"}


async def test_csv_formula_injection_guard(settings, db):
    for i, model in enumerate(["=cmd|'/c calc'!A1", "+1", "-2+3", "@SUM(A1)", "Plain"]):
        await _add(db, f"f{i}", T0 + timedelta(seconds=i), model=model)
    # A negative number must stay a number.
    await db.insert_detection(**_det("neg", T0 - timedelta(seconds=5)))
    async with await _client(settings, db) as ac:
        rows = _parse_csv((await ac.get("/api/detections.csv")).text)
    by_id = {r["burst_id"]: r for r in rows}
    assert by_id["f0"]["model"] == "'=cmd|'/c calc'!A1"
    assert by_id["f0"]["burst_attribution"] == "'=cmd|'/c calc'!A1"
    assert by_id["f1"]["model"] == "'+1"
    assert by_id["f2"]["model"] == "'-2+3"
    assert by_id["f3"]["model"] == "'@SUM(A1)"
    assert by_id["f4"]["model"] == "Plain"
    assert by_id["neg"]["peak_power_db"] == "-30.0"


async def test_csv_streams_in_pages(settings, db, monkeypatch):
    from rfobserver.web.routes import api

    for i in range(23):
        await _add(db, f"p{i:02d}", T0 + timedelta(seconds=i % 5))  # ties on start_time
    monkeypatch.setattr(api, "CSV_PAGE_SIZE", 5)
    calls: list[int] = []
    real = db.query_detections

    async def spy(*args, **kwargs):
        calls.append(kwargs.get("limit"))
        return await real(*args, **kwargs)

    monkeypatch.setattr(db, "query_detections", spy)
    async with await _client(settings, db) as ac:
        rows = _parse_csv((await ac.get("/api/detections.csv")).text)
    assert len(rows) == 23 and len({r["burst_id"] for r in rows}) == 23
    assert len(calls) >= 5 and all(c == 5 for c in calls)


async def test_csv_without_db_is_503(settings):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings)), base_url="http://test"
    ) as ac:
        r = await ac.get("/api/detections.csv")
    assert r.status_code == 503


# -- Review follow-ups --------------------------------------------------------


async def test_full_view_time_column_is_start_time(settings, db):
    row = _det("s", T0)
    row["detection_timestamp"] = T0 + timedelta(seconds=9)
    await db.insert_detection(**row)
    async with await _client(settings, db) as ac:
        full = (await ac.get("/api/detections?view=full")).text
        dash = (await ac.get("/api/detections")).text
        page = (await ac.get("/detections")).text
    assert "<td>2026-03-01T12:00:00+00:00</td>" in full
    assert "12:00:09" not in full
    # The dashboard view keeps the detection timestamp.
    assert "<td>2026-03-01T12:00:09+00:00</td>" in dash
    assert "<th>Start (UTC)</th>" in page


async def test_model_filter_uses_partial_index(tmp_path):
    import sqlite3

    path = str(tmp_path / "plan.db")
    database = SensorDatabase(path)
    await database.connect()
    await database.close()
    conds, params = SensorDatabase._detection_conditions(model="SilverSpring-Mesh")
    with sqlite3.connect(path) as conn:
        for select in ("SELECT * FROM detections", "SELECT duration_ms FROM detections"):
            plan = conn.execute(
                f"EXPLAIN QUERY PLAN {select} WHERE {' AND '.join(conds)}", params
            ).fetchall()
            assert any("idx_detections_model" in str(step[-1]) for step in plan), plan


async def test_explicit_offset_is_converted_to_utc(settings, seeded):
    # 14:01+02:00 is 12:01 UTC; 14:03+02:00 is 12:03 UTC (exclusive).
    q = "start=2026-03-01T14:01:00%2B02:00&stop=2026-03-01T14:03:00%2B02:00"
    async with await _client(settings, seeded) as ac:
        rows = _parse_csv((await ac.get(f"/api/detections.csv?{q}")).text)
        frag = (await ac.get(f"/api/detections?view=full&{q}")).text
    assert {r["burst_id"] for r in rows} == {"b", "c"}
    assert frag.count("<tr>") == 2


@pytest.mark.parametrize("frac", ["5", "5000000", "50"])
async def test_fractional_seconds_any_length(settings, seeded, frac):
    # 12:00:59.5 excludes a (12:00:00) and keeps b (12:01:00) onwards.
    async with await _client(settings, seeded) as ac:
        r = await ac.get(f"/api/detections.csv?start=2026-03-01T12:00:59.{frac}")
    assert r.status_code == 200
    assert {x["burst_id"] for x in _parse_csv(r.text)} == {"b", "c", "d", "e"}


@pytest.mark.parametrize("param", ["start", "stop"])
async def test_bad_dates_are_rejected(settings, seeded, param):
    async with await _client(settings, seeded) as ac:
        csv_r = await ac.get(f"/api/detections.csv?{param}=not-a-date")
        hist_r = await ac.get(f"/api/detections/histogram?{param}=2026-13-45T99:00")
        frag_r = await ac.get(f"/api/detections?view=full&{param}=garbage")
    assert csv_r.status_code == 400
    assert hist_r.status_code == 400
    assert "Invalid start/stop time" in frag_r.text
    assert 'colspan="8"' in frag_r.text


async def test_csv_marks_truncation_on_mid_stream_error(settings, db, monkeypatch):
    from rfobserver.web.routes import api

    for i in range(6):
        await _add(db, f"q{i}", T0 + timedelta(seconds=i))
    monkeypatch.setattr(api, "CSV_PAGE_SIZE", 2)
    real = db.query_detections
    calls = 0

    async def flaky(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("disk I/O error")
        return await real(*args, **kwargs)

    monkeypatch.setattr(db, "query_detections", flaky)
    async with await _client(settings, db) as ac:
        r = await ac.get("/api/detections.csv")
    lines = r.text.strip().splitlines()
    assert len(lines) == 4  # header, first page of 2, marker
    assert lines[-1] == "# export truncated: error"


async def test_csv_guard_catches_leading_space_formula(settings, db):
    await _add(db, "sp", T0, model=" =1+1")
    await _add(db, "tab", T0 + timedelta(seconds=1), model="\t@x")
    async with await _client(settings, db) as ac:
        rows = {r["burst_id"]: r for r in _parse_csv((await ac.get("/api/detections.csv")).text)}
    assert rows["sp"]["model"] == "' =1+1"
    assert rows["sp"]["burst_attribution"] == "' =1+1"
    assert rows["tab"]["model"] == "'\t@x"
