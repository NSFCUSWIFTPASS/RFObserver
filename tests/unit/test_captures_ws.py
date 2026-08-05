"""Tests for the captures binary-PSD WebSocket + shared slice helpers."""

import struct
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


@pytest.fixture
def settings():
    return AppSettings(_env_file=None)


@pytest.fixture
def client(settings):
    app = create_app(settings)
    return TestClient(app)


@pytest.fixture
def seeded_filename(settings):
    """Seed a small .npz + .sc16 capture and yield the .sc16 filename."""
    storage = Path(settings.STORAGE_PATH)
    storage.mkdir(parents=True, exist_ok=True)

    grid = np.arange(6 * 8, dtype=np.float32).reshape(6, 8)
    freq_axis = np.linspace(-500000, 500000, 8)
    npz_path = storage / "ws-capture.npz"
    np.savez_compressed(
        npz_path,
        grid=grid,
        freq_axis=freq_axis,
        time_resolution_s=np.float64(0.001),
        center_freq_hz=np.int64(915000000),
        bandwidth_hz=np.int64(1000000),
    )
    sc16_path = storage / "ws-capture.sc16"
    sc16_path.write_bytes(b"\x00" * 100)

    yield "ws-capture.sc16"

    npz_path.unlink(missing_ok=True)
    sc16_path.unlink(missing_ok=True)


def test_slice_psd_contiguous_float32_and_downsample():
    from rfobserver.web.routes.captures import _slice_psd

    grid = np.arange(6 * 8, dtype=np.float32).reshape(6, 8)
    fa = np.arange(8, dtype=np.float64)
    sliced, ds_fa, ds_n = _slice_psd(grid, fa, 8, 1, 3, 4)
    assert sliced.dtype == np.float32 and sliced.flags["C_CONTIGUOUS"]
    assert sliced.shape == (3, 4) and ds_n == 4  # 8 bins -> 4 via factor-2 mean
    # Row 1 of the grid is [8..15]; factor-2 mean -> [8.5, 10.5, 12.5, 14.5].
    assert np.allclose(sliced[0], [8.5, 10.5, 12.5, 14.5])
    assert ds_fa.shape == (4,)


def test_psd_frame_bytes_header_and_len():
    from rfobserver.web.routes.captures import _psd_frame_bytes

    sliced = np.zeros((3, 4), dtype=np.float32)
    buf = _psd_frame_bytes(7, sliced)
    start, count, num_bins = struct.unpack("<iii", buf[:12])
    assert (start, count, num_bins) == (7, 3, 4)
    assert len(buf) - 12 == 3 * 4 * 4


def test_ws_streams_meta_then_binary_window(client, seeded_filename):
    with client.websocket_connect(f"/captures/ws/psd/{seeded_filename}") as ws:
        meta = ws.receive_json()
        assert meta["type"] == "meta"
        assert meta["total_rows"] == 6
        assert meta["num_bins"] == 8
        assert meta["center_freq_hz"] == 915000000

        ws.send_json({"start": 0, "count": 2, "max_bins": 512})

        # Requested window plus the bounded push-ahead neighbours arrive as
        # binary frames (order is not guaranteed, so classify by start).
        frames = {}
        for _ in range(2):
            raw = ws.receive_bytes()
            s, count, num_bins = struct.unpack("<iii", raw[:12])
            rows = np.frombuffer(raw[12:], dtype="<f4").reshape(count, num_bins)
            frames[s] = rows

        assert 0 in frames  # the requested window
        assert 2 in frames  # push-ahead next neighbour (start + count)

        rows = frames[0]
        assert rows.shape[0] == 2
        # Decoded rows equal the HTTP endpoint's grid for the same range.
        http = client.get(f"/captures/psd/{seeded_filename}?start=0&count=2&max_bins=512").json()
        assert np.allclose(rows, np.array(http["grid"], dtype=np.float32))


def test_ws_honours_have_to_skip_cached_neighbours(client, seeded_filename):
    with client.websocket_connect(f"/captures/ws/psd/{seeded_filename}") as ws:
        ws.receive_json()  # meta
        # Request 1: client already has the next-neighbour window at start=2, so
        # only the requested window (start=0) should come back (prev is negative).
        ws.send_json({"start": 0, "count": 2, "max_bins": 512, "have": [2]})
        # Request 2 acts as a fence: it serves start=4 (next=6 is out of range,
        # prev=2 is in `have`). If the server wrongly emitted start=2 for request
        # 1, it would arrive before the fence frame and be caught below.
        ws.send_json({"start": 4, "count": 2, "max_bins": 512, "have": [0, 2]})
        starts = []
        for _ in range(2):
            raw = ws.receive_bytes()
            s, _count, _nb = struct.unpack("<iii", raw[:12])
            starts.append(s)
        assert sorted(starts) == [0, 4]


def test_ws_missing_capture_closes(client):
    with (
        pytest.raises(WebSocketDisconnect),
        client.websocket_connect("/captures/ws/psd/nope.sc16") as ws,
    ):
        ws.receive_json()
