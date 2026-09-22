"""Tests for the raw+sidecar PSD grid on-disk format."""

import json

import numpy as np

from rfobserver.storage import psd_grid


def test_write_and_load_roundtrip(tmp_path) -> None:
    sc16 = tmp_path / "cap.sc16"
    raw, meta = psd_grid.grid_paths(sc16)
    assert raw.name == "cap.psd" and meta.name == "cap.psd.json"

    grid = np.arange(6 * 4, dtype=np.float32).reshape(6, 4)
    raw.write_bytes(grid.tobytes())
    psd_grid.write_meta(
        meta,
        rows=6,
        num_bins=4,
        time_resolution_s=0.0005,
        center_freq_hz=915_000_000,
        bandwidth_hz=26_000_000,
        freq_axis=np.arange(4, dtype=np.float64),
        grid_min=0.0,
        grid_max=23.0,
        cal_offset_db=None,
    )

    loaded = psd_grid.load_grid(sc16)
    assert loaded is not None
    mm, m = loaded
    assert mm.shape == (6, 4) and mm.dtype == np.float32
    np.testing.assert_array_equal(mm[:], grid)
    assert m["num_bins"] == 4 and m["rows"] == 6
    assert m["grid_max"] == 23.0 and m["center_freq_hz"] == 915_000_000
    assert "cal_offset_db" not in m


def test_load_missing_returns_none(tmp_path) -> None:
    assert psd_grid.load_grid(tmp_path / "nope.sc16") is None


def test_cal_offset_included_when_set(tmp_path) -> None:
    sc16 = tmp_path / "c.sc16"
    raw, meta = psd_grid.grid_paths(sc16)
    raw.write_bytes(np.zeros((2, 2), np.float32).tobytes())
    psd_grid.write_meta(
        meta,
        rows=2,
        num_bins=2,
        time_resolution_s=1.0,
        center_freq_hz=1,
        bandwidth_hz=1,
        freq_axis=np.zeros(2),
        grid_min=0.0,
        grid_max=0.0,
        cal_offset_db=-12.5,
    )
    loaded = psd_grid.load_grid(sc16)
    assert loaded is not None
    _, m = loaded
    assert m["cal_offset_db"] == -12.5


def test_write_meta_records_alignment_to_the_iq(tmp_path) -> None:
    """The sidecar must pin row 0 to a position in the .sc16.

    Without these fields a reader can only assume row 0 is the IQ's first
    sample, which is how a ~820 ms pipeline-latency misalignment stayed
    invisible (docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md).
    """
    meta_path = tmp_path / "cap.psd.json"
    psd_grid.write_meta(
        meta_path,
        rows=10,
        num_bins=4,
        time_resolution_s=0.001024,
        center_freq_hz=100_000_000,
        bandwidth_hz=2_000_000,
        freq_axis=np.arange(4, dtype=np.float64),
        grid_min=-160.0,
        grid_max=-40.0,
        cal_offset_db=None,
        start_sample_offset=1234,
        slice_samples=2048,
    )
    meta = json.loads(meta_path.read_text())
    assert meta["start_sample_offset"] == 1234
    assert meta["slice_samples"] == 2048


def test_write_meta_alignment_fields_default_to_zero(tmp_path) -> None:
    meta_path = tmp_path / "cap.psd.json"
    psd_grid.write_meta(
        meta_path,
        rows=0,
        num_bins=4,
        time_resolution_s=0.001024,
        center_freq_hz=100_000_000,
        bandwidth_hz=2_000_000,
        freq_axis=np.arange(4, dtype=np.float64),
        grid_min=0.0,
        grid_max=0.0,
        cal_offset_db=None,
    )
    meta = json.loads(meta_path.read_text())
    assert meta["start_sample_offset"] == 0
    assert meta["slice_samples"] == 0
