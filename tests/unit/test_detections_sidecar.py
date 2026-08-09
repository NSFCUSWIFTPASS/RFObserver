"""Tests for rfobserver.storage.detections_sidecar."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from rfobserver.storage import detections_sidecar as ds
from rfobserver.storage.database import SensorDatabase

CAP_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
DURATION_SEC = 10.0
# Nominal PSD time resolution. Deliberately does NOT satisfy
# ROWS * TRES == DURATION_SEC: rows * tres = 40 * 0.5 = 20s != 10s. The sidecar
# must map detections proportionally (offset / duration * rows), NOT via the
# nominal tres, and must report the effective tres (duration / rows).
TRES = 0.5
ROWS = 40
EFF_TRES = DURATION_SEC / ROWS  # 0.25 s/row
CENTER = 915e6
SAMPLE_RATE = 56e6
GAIN = 40.0


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "test.db"))
    await database.connect()
    yield database
    await database.close()


def _write_capture_meta(sc16_path, **overrides):
    meta = {
        "start_time": CAP_START.isoformat(),
        "duration_sec": DURATION_SEC,
        "center_freq_hz": CENTER,
        "sample_rate_hz": SAMPLE_RATE,
        "gain_db": GAIN,
    }
    meta.update(overrides)
    sc16_path.with_suffix(".json").write_text(json.dumps(meta))


def _write_psd_meta(sc16_path, time_resolution_s=TRES, rows=ROWS):
    from rfobserver.storage import psd_grid

    _raw, meta_path = psd_grid.grid_paths(sc16_path)
    meta_path.write_text(json.dumps({"time_resolution_s": time_resolution_s, "rows": rows}))


async def _insert(db, burst_id, start_time, stop_time, **overrides):
    kwargs = dict(
        burst_id=burst_id,
        start_time=start_time,
        stop_time=stop_time,
        center_freq_hz=CENTER,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=(stop_time - start_time).total_seconds() * 1000.0,
        detection_timestamp=start_time,
        sdr_center_freq_hz=CENTER,
        sample_rate_hz=SAMPLE_RATE,
        gain_db=GAIN,
        peak_freq_hz=CENTER + 1000.0,
    )
    kwargs.update(overrides)
    await db.insert_detection(**kwargs)


def test_sidecar_path_replaces_sc16_with_detections_json(tmp_path):
    sc16 = tmp_path / "cap.sc16"
    assert ds.sidecar_path(sc16) == tmp_path / "cap.detections.json"


async def test_write_sidecar_filters_by_window_and_tuning(tmp_path, db):
    sc16 = tmp_path / "cap.sc16"
    _write_capture_meta(sc16)
    _write_psd_meta(sc16)

    # In-window, matching tuning: kept.
    await _insert(
        db,
        "in-window",
        CAP_START + timedelta(seconds=2.0),
        CAP_START + timedelta(seconds=3.0),
    )
    # Before the capture window: excluded (since is >=).
    await _insert(
        db,
        "before-window",
        CAP_START - timedelta(seconds=5.0),
        CAP_START - timedelta(seconds=4.0),
    )
    # At/after the capture end: excluded (until is half-open, <).
    await _insert(
        db,
        "after-window",
        CAP_START + timedelta(seconds=DURATION_SEC),
        CAP_START + timedelta(seconds=DURATION_SEC + 1.0),
    )
    # In-window but different SDR tuning: excluded.
    await _insert(
        db,
        "wrong-tuning",
        CAP_START + timedelta(seconds=2.0),
        CAP_START + timedelta(seconds=3.0),
        sdr_center_freq_hz=2437e6,
    )

    payload = await ds.write_sidecar(sc16, db)

    assert payload["capture_start_time"] == CAP_START.isoformat()
    # Effective resolution (duration / rows), NOT the nominal PSD tres.
    assert payload["time_resolution_s"] == EFF_TRES
    assert payload["center_freq_hz"] == CENTER
    assert payload["sample_rate_hz"] == SAMPLE_RATE
    assert payload["gain_db"] == GAIN
    assert len(payload["detections"]) == 1
    det = payload["detections"][0]
    # Proportional mapping: offset / duration * rows. Under the old /tres math
    # these would be 4 and 6 (round(2.0/0.5), round(3.0/0.5)).
    assert det["row_start"] == 8  # round(2.0 / 10.0 * 40)
    assert det["row_stop"] == 12  # round(3.0 / 10.0 * 40)
    assert det["center_freq_hz"] == CENTER
    assert det["peak_power_db"] == -30.0

    # The file was actually written and matches the returned payload.
    written = json.loads(ds.sidecar_path(sc16).read_text())
    assert written == payload


async def test_build_sidecar_payload_missing_capture_meta_is_empty(tmp_path, db):
    sc16 = tmp_path / "nometa.sc16"
    # No .json / .psd.json companions written at all.
    payload = await ds.build_sidecar_payload(sc16, db)
    assert payload["detections"] == []
    assert payload["capture_start_time"] is None
    assert payload["time_resolution_s"] is None


async def test_build_sidecar_payload_missing_psd_meta_is_empty(tmp_path, db):
    sc16 = tmp_path / "nopsd.sc16"
    _write_capture_meta(sc16)
    # No .psd.json written, so time_resolution_s is unavailable.
    await _insert(
        db,
        "in-window",
        CAP_START + timedelta(seconds=2.0),
        CAP_START + timedelta(seconds=3.0),
    )
    payload = await ds.build_sidecar_payload(sc16, db)
    assert payload["detections"] == []
    assert payload["time_resolution_s"] is None
