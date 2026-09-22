"""Downloading captures: raw files, SigMF, resumable ranges, and the guards."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from httpx import ASGITransport, AsyncClient

from rfobserver.capture.sigmf_reader import load_sigmf
from rfobserver.config import AppSettings
from rfobserver.storage.sigmf_export import iq_sigmf_meta
from rfobserver.web.app import create_app

BASE = "MOCK0001-host-20260922T170719"
RATE = 2_000_000

CAPTURE_META = {
    "file": f"{BASE}.sc16",
    "format": "sc16",
    "sample_rate_hz": RATE,
    "bandwidth_hz": RATE,
    "center_freq_hz": 915_000_000,
    "gain_db": 30,
    "serial": "322750B",
    "hostname": "nano-super",
    "start_time": "2026-09-22T17:07:19.784029+00:00",
    "total_samples": 64,
    "gaps": [],
}


@pytest.fixture
def settings(tmp_path):
    return AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path / "storage"))


def _seed(settings, *, meta=None, companions=True) -> Path:
    d = Path(settings.STORAGE_PATH) / "auto"
    d.mkdir(parents=True, exist_ok=True)
    sc16 = d / f"{BASE}.sc16"
    # Distinct bytes everywhere so a wrong slice or wrong file cannot pass.
    sc16.write_bytes(np.arange(64 * 2, dtype=np.int16).tobytes())
    sc16.with_suffix(".json").write_text(json.dumps(meta or CAPTURE_META))
    if companions:
        (d / f"{BASE}.psd").write_bytes(b"\x01\x02\x03\x04" * 8)
        (d / f"{BASE}.psd.json").write_text(json.dumps({"rows": 2, "num_bins": 4}))
        (d / f"{BASE}.detections.json").write_text(json.dumps({"detections": []}))
    return sc16


async def _get(app, path, headers=None):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        return await c.get(path, headers=headers or {})


# --- SigMF metadata -----------------------------------------------------------


def test_sigmf_meta_core_fields():
    meta = iq_sigmf_meta(CAPTURE_META)
    g = meta["global"]
    assert g["core:datatype"] == "ci16_le"  # byte-identical to our SC16
    assert g["core:sample_rate"] == RATE
    assert g["core:version"]
    assert "322750B" in g["core:hw"]
    assert meta["annotations"] == []
    assert meta["captures"] == [
        {
            "core:sample_start": 0,
            "core:frequency": 915_000_000,
            "core:datetime": "2026-09-22T17:07:19.784029Z",
        }
    ]


def test_sigmf_meta_marks_each_overflow_gap_as_a_new_capture_segment():
    """Lost samples are a discontinuity. SigMF expresses that as a new captures
    segment whose datetime accounts for everything lost so far, so timestamps
    after an overflow stay right in any SigMF tool."""
    meta = iq_sigmf_meta({**CAPTURE_META, "gaps": [[1000, 500], [3000, 2000]]})
    segs = meta["captures"]
    assert [s["core:sample_start"] for s in segs] == [0, 1000, 3000]
    t0 = datetime.fromisoformat("2026-09-22T17:07:19.784029+00:00")

    def at(seg):
        return datetime.fromisoformat(seg["core:datetime"].replace("Z", "+00:00"))

    # Segment k starts at file sample `start`, after `lost` samples went missing.
    assert (at(segs[1]) - t0).total_seconds() == pytest.approx((1000 + 500) / RATE)
    assert (at(segs[2]) - t0).total_seconds() == pytest.approx((3000 + 500 + 2000) / RATE)


def test_sigmf_meta_without_a_start_time_omits_datetime():
    meta = iq_sigmf_meta({k: v for k, v in CAPTURE_META.items() if k != "start_time"})
    assert "core:datetime" not in meta["captures"][0]


# --- Download route -----------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_download_is_byte_exact_and_an_attachment(settings):
    sc16 = _seed(settings)
    r = await _get(create_app(settings), f"/captures/download/{BASE}.sc16")
    assert r.status_code == 200
    assert r.content == sc16.read_bytes()
    assert r.headers["content-length"] == str(sc16.stat().st_size)
    assert "attachment" in r.headers["content-disposition"]
    assert f"{BASE}.sc16" in r.headers["content-disposition"]
    assert r.headers.get("accept-ranges") == "bytes"


@pytest.mark.asyncio
async def test_range_request_resumes_mid_file(settings):
    """curl -C - relies on this for archive pulls over a flaky link."""
    sc16 = _seed(settings)
    r = await _get(
        create_app(settings), f"/captures/download/{BASE}.sc16", {"Range": "bytes=10-49"}
    )
    assert r.status_code == 206
    assert r.content == sc16.read_bytes()[10:50]
    assert r.headers["content-range"] == f"bytes 10-49/{sc16.stat().st_size}"


@pytest.mark.asyncio
async def test_sigmf_data_is_the_sc16_bytes_under_the_sigmf_name(settings):
    sc16 = _seed(settings)
    app = create_app(settings)
    r = await _get(app, f"/captures/download/{BASE}.sigmf-data")
    assert r.status_code == 200
    assert r.content == sc16.read_bytes()
    assert f"{BASE}.sigmf-data" in r.headers["content-disposition"]
    ranged = await _get(app, f"/captures/download/{BASE}.sigmf-data", {"Range": "bytes=0-3"})
    assert ranged.status_code == 206


@pytest.mark.asyncio
async def test_downloaded_sigmf_pair_loads_in_our_sigmf_reader(settings, tmp_path):
    """Round trip through a real consumer: the replay path's SigMF reader."""
    _seed(settings)
    app = create_app(settings)
    out = tmp_path / "pulled"
    out.mkdir()
    for suffix in (".sigmf-meta", ".sigmf-data"):
        r = await _get(app, f"/captures/download/{BASE}{suffix}")
        assert r.status_code == 200
        assert "attachment" in r.headers["content-disposition"]
        (out / f"{BASE}{suffix}").write_bytes(r.content)
    cap = load_sigmf(out / f"{BASE}.sigmf-data")
    assert cap.sample_rate_hz == RATE
    assert cap.center_freq_hz == 915_000_000
    assert cap.num_samples == 64
    np.testing.assert_array_equal(cap.raw[:6], np.arange(6, dtype=np.int16))


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", [".json", ".psd", ".psd.json", ".detections.json"])
async def test_companion_files_download(settings, suffix):
    sc16 = _seed(settings)
    r = await _get(create_app(settings), f"/captures/download/{BASE}{suffix}")
    assert r.status_code == 200
    assert r.content == (sc16.parent / f"{BASE}{suffix}").read_bytes()


@pytest.mark.asyncio
async def test_missing_companion_is_404(settings):
    _seed(settings, companions=False)
    r = await _get(create_app(settings), f"/captures/download/{BASE}.detections.json")
    assert r.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    [
        f"{BASE}.txt",  # suffix not on the whitelist
        f"{BASE}.sc16.bak",
        ".sc16",  # no base name
        "nonexistent.sc16",
    ],
)
async def test_unknown_or_missing_names_are_404(settings, name):
    _seed(settings)
    r = await _get(create_app(settings), f"/captures/download/{name}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_traversal_is_rejected(settings):
    _seed(settings)
    r = await _get(create_app(settings), "/captures/download/..sc16")
    assert r.status_code in (400, 404)
    r = await _get(create_app(settings), "/captures/download/a..b.sc16")
    assert r.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["recording", "finalizing"])
async def test_capture_still_being_recorded_is_409(settings, state):
    """A growing file would download silently truncated."""
    _seed(settings)
    app = create_app(settings)
    app.state.processor = SimpleNamespace(
        recording_status=lambda: {"state": state, "file": f"{BASE}.sc16"}
    )
    for suffix in (".sc16", ".sigmf-data", ".sigmf-meta"):
        r = await _get(app, f"/captures/download/{BASE}{suffix}")
        assert r.status_code == 409, suffix


@pytest.mark.asyncio
async def test_other_captures_download_while_one_is_recording(settings):
    _seed(settings)
    app = create_app(settings)
    app.state.processor = SimpleNamespace(
        recording_status=lambda: {"state": "recording", "file": "SOMETHING-ELSE.sc16"}
    )
    r = await _get(app, f"/captures/download/{BASE}.sc16")
    assert r.status_code == 200


# --- Listing for scripts and the UI -------------------------------------------


@pytest.mark.asyncio
async def test_list_and_detail_report_downloadable_files(settings):
    sc16 = _seed(settings)
    app = create_app(settings)
    expected_files = {
        f"{BASE}.sc16": sc16.stat().st_size,
        f"{BASE}.json": sc16.with_suffix(".json").stat().st_size,
        f"{BASE}.psd": 32,
        f"{BASE}.psd.json": (sc16.parent / f"{BASE}.psd.json").stat().st_size,
        f"{BASE}.detections.json": (sc16.parent / f"{BASE}.detections.json").stat().st_size,
    }
    for path in ("/captures/list", f"/captures/detail/{BASE}.sc16"):
        body = (await _get(app, path)).json()
        entry = body[0] if isinstance(body, list) else body
        assert {f["name"]: f["size_bytes"] for f in entry["files"]} == expected_files
        assert entry["sigmf"] == {"meta": f"{BASE}.sigmf-meta", "data": f"{BASE}.sigmf-data"}


@pytest.mark.asyncio
async def test_head_reports_size_without_a_body(settings):
    """curl -I, wget --spider and download managers probe with HEAD first."""
    sc16 = _seed(settings)
    async with AsyncClient(
        transport=ASGITransport(app=create_app(settings)), base_url="http://test"
    ) as c:
        r = await c.head(f"/captures/download/{BASE}.sc16")
    assert r.status_code == 200
    assert r.headers["content-length"] == str(sc16.stat().st_size)
    assert r.headers.get("accept-ranges") == "bytes"
    assert r.content == b""
