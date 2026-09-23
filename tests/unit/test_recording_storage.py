"""Recording under storage pressure: refusal, stop reasons, write failures."""

from __future__ import annotations

import errno
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from fastapi.testclient import TestClient

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.governor import GB, StorageGovernor, StorageSample, VolumeSample
from rfobserver.storage.local import LocalStorage
from rfobserver.web.app import create_app

T0 = datetime(2026, 9, 23, tzinfo=timezone.utc)


def _governor_at(step: int) -> StorageGovernor:
    gov = StorageGovernor()
    free = {0: 200, 1: 40, 3: 40, 4: 20}[step]
    evictable = step == 1
    for _ in range(2 if step == 3 else 1):
        gov.tick(
            StorageSample(
                data=VolumeSample(int(free * GB), 1000 * GB),
                db_volume=None,
                db_file_bytes=0,
                db_reusable_bytes=0,
                auto_bytes=0,
                manual_bytes=0,
                evictable_auto=evictable,
            ),
            min_free_gb=0,
            now=T0,
        )
    assert gov.state.step == step
    return gov


def _proc(
    tmp_path: Path, governor: StorageGovernor | None = None, **overrides
) -> StreamingProcessor:
    storage = tmp_path / "storage"
    storage.mkdir(exist_ok=True)
    base = dict(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=64,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"),
        ARCHIVE_MAX_GB=1.0,
        TRIGGER_PRE_SEC=0.001,
        _env_file=None,
    )
    base.update(overrides)
    s = AppSettings(**base)
    rx = MockReceiver(
        receiver_config=ReceiverConfig(
            gain_db=s.GAIN, bandwidth_hz=s.BANDWIDTH, duration_sec=s.DURATION_SEC
        )
    )
    rx.initialize()
    return StreamingProcessor(
        receiver=rx,
        database=SensorDatabase(s.DB_PATH),
        local_storage=LocalStorage(s.STORAGE_PATH, max_gb=s.ARCHIVE_MAX_GB),
        settings=s,
        storage_governor=governor,
    )


def _only_json(proc: StreamingProcessor) -> dict:
    (p,) = [
        q
        for d in (proc._storage.auto_dir, proc._storage.manual_dir)
        for q in d.glob("*.json")
        if not q.name.endswith((".psd.json", ".detections.json"))
    ]
    return json.loads(p.read_text())


@pytest.mark.parametrize("ram", [True, False])
def test_manual_start_is_refused_at_step_3(tmp_path, ram):
    proc = _proc(tmp_path, _governor_at(3), RECORDING_RAM_BUFFER=ram, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    st = proc.recording_status()
    assert st["state"] == "idle"
    assert "below" in st["refused"] and "floor" in st["refused"]


def test_arm_is_refused_at_step_3(tmp_path):
    proc = _proc(tmp_path, _governor_at(3))
    proc.arm_trigger()
    assert proc.recording_status()["state"] == "idle"
    assert proc.recording_status()["refused"]


def test_armed_trigger_does_not_fire_once_step_3_is_reached(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, TRIGGER_THRESHOLD_DB=-200.0)
    proc.arm_trigger()
    assert proc.recording_status()["state"] == "armed"
    for _ in range(2):
        gov.tick(
            StorageSample(
                data=VolumeSample(40 * GB, 1000 * GB),
                db_volume=None,
                db_file_bytes=0,
                db_reusable_bytes=0,
                auto_bytes=0,
                manual_bytes=0,
                evictable_auto=False,
            ),
            min_free_gb=0,
            now=T0,
        )
    proc._check_trigger_and_record(np.full(4096, 1 << 20, dtype=np.int32), (), 0)
    assert proc.recording_status()["state"] == "armed"  # still waiting, not recording
    assert proc.recording_status()["refused"]


@pytest.mark.parametrize("step", [0, 1])
def test_steps_below_3_allow_recording(tmp_path, step):
    proc = _proc(tmp_path, _governor_at(step), RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
        assert proc.recording_status()["refused"] is None
    finally:
        proc.stop_recording()


def test_no_governor_never_refuses(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    assert proc.recording_status()["state"] == "recording"
    proc.stop_recording()


def test_manual_stop_is_recorded_as_manual(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc.stop_recording()
    assert _only_json(proc)["stopped_reason"] == "manual"


def test_max_duration_stop_is_recorded(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc._effective_max_sec = 0.0
    proc._check_trigger_and_record(np.zeros(1000, dtype=np.int32), (), None)
    assert _only_json(proc)["stopped_reason"] == "max_duration"


def test_trigger_end_stop_is_recorded(tmp_path):
    proc = _proc(
        tmp_path,
        None,
        RECORDING_RAM_BUFFER=True,
        RECORDING_MAX_SEC=5.0,
        TRIGGER_THRESHOLD_DB=0.0,
        TRIGGER_HYSTERESIS=1,
    )
    proc._trigger_initiated = True
    with proc._rec_lock:
        proc._begin_recording()
    proc._check_trigger_and_record(np.zeros(1000, dtype=np.int32), (), None)
    assert _only_json(proc)["stopped_reason"] == "trigger_end"


def _client_with(proc_status: dict) -> TestClient:
    app = create_app(AppSettings(_env_file=None))
    app.state.processor = SimpleNamespace(
        start_recording=lambda: None,
        arm_trigger=lambda: None,
        manual_trigger=lambda: None,
        recording_status=lambda: proc_status,
    )
    return TestClient(app)


@pytest.mark.parametrize("path", ["/api/recording/start", "/api/recording/arm", "/api/trigger"])
def test_api_answers_409_with_the_reason_when_refused(path):
    c = _client_with({"state": "idle", "file": None, "refused": "Recording refused: x"})
    r = c.post(path)
    assert r.status_code == 409
    assert r.json()["detail"] == "Recording refused: x"


def test_api_start_succeeds_when_not_refused():
    c = _client_with({"state": "recording", "file": "a.sc16", "refused": None})
    assert c.post("/api/recording/start").status_code == 200


# --- write failures ---------------------------------------------------------


class _FailingFile:
    """Writes the first `ok` bytes, then raises ENOSPC (on write or on close)."""

    def __init__(self, real, ok: int, on_close: bool = False) -> None:
        self.real, self.left, self.on_close = real, ok, on_close

    def write(self, data) -> int:
        b = memoryview(data).cast("B")
        if not self.on_close and len(b) > self.left:
            self.real.write(b[: self.left])
            self.left = 0
            raise OSError(errno.ENOSPC, "No space left on device")
        self.left -= len(b)
        return self.real.write(b)

    def close(self) -> None:
        self.real.close()
        if self.on_close:
            raise OSError(errno.ENOSPC, "No space left on device")

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _patch_sc16_open(monkeypatch, ok: int, on_close: bool = False) -> None:
    import builtins

    real_open = builtins.open

    def fake_open(path, mode="r", *a, **k):
        f = real_open(path, mode, *a, **k)
        if str(path).endswith(".sc16") and "w" in mode:
            return _FailingFile(f, ok, on_close)
        return f

    monkeypatch.setattr("rfobserver.pipeline.streaming.open", fake_open, raising=False)


def _run_disk_recording(proc: StreamingProcessor, chunks: int, n: int = 1000) -> None:
    """Drive a disk-mode recording the way the receiver thread does."""
    # No dispatch pipeline runs here, so no PSD grids ever arrive and the tail
    # wait would sit out its full cap (10 s) on every finalize.
    proc._await_tail_grids = lambda: None
    proc.start_recording()
    pos = proc._pre_trigger_buf.total_written
    for _ in range(chunks):
        if proc.recording_status()["state"] != "recording":
            break
        proc._check_trigger_and_record(np.ones(n, dtype=np.int32), (), pos)
        pos += n
        time.sleep(0.01)  # let the writer thread drain
    if proc.recording_status()["state"] == "recording":
        proc.stop_recording()
    else:
        proc._end_done.wait(timeout=15)


def test_writer_enospc_ends_the_recording_promptly_and_is_recorded(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False)
    _patch_sc16_open(monkeypatch, ok=6000)  # 1500 samples reach disk
    _run_disk_recording(proc, chunks=50)
    meta = _only_json(proc)
    assert meta["write_failed"] is True
    assert meta["write_error"].startswith("ENOSPC")
    assert meta["stopped_reason"] == "write_error"
    (sc16,) = proc._storage.manual_dir.glob("*.sc16")
    assert sc16.stat().st_size == meta["total_bytes"] == meta["total_samples"] * 4
    assert meta["total_samples"] == 1500
    assert meta["dropped_chunks"] < 5  # ended promptly, not 50 chunks of gaps
    assert all(g[0] <= meta["total_samples"] for g in meta["gaps"])
    assert gov.state.degraded and "ENOSPC" in gov.state.last_write_error["error"]


def test_error_at_close_after_stop_does_not_hang_and_is_recorded(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False)
    _patch_sc16_open(monkeypatch, ok=10**9, on_close=True)
    t0 = time.monotonic()
    _run_disk_recording(proc, chunks=3)
    assert time.monotonic() - t0 < 5
    meta = _only_json(proc)
    assert meta["write_failed"] is True
    assert meta["stopped_reason"] == "manual"
    assert gov.state.degraded


def test_partial_trailing_sample_is_truncated(tmp_path, monkeypatch):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=False)
    _patch_sc16_open(monkeypatch, ok=6002)  # half a sample past 1500
    _run_disk_recording(proc, chunks=50)
    (sc16,) = proc._storage.manual_dir.glob("*.sc16")
    assert sc16.stat().st_size == 6000
    assert _only_json(proc)["total_samples"] == 1500


def test_clean_recording_reports_no_failure(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=False)
    _run_disk_recording(proc, chunks=3)
    meta = _only_json(proc)
    assert meta["write_failed"] is False and "write_error" not in meta
    (sc16,) = proc._storage.manual_dir.glob("*.sc16")
    assert sc16.stat().st_size == meta["total_bytes"]


def test_ram_tofile_failure_keeps_what_reached_disk(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc._write_recording_chunk(np.ones(4000, dtype=np.int32))

    class _FailingArray(np.ndarray):
        """ndarray.tofile cannot be monkeypatched (immutable type); a view of
        this subclass survives the slice finalize takes."""

        def tofile(self, path, *a, **k):
            with open(path, "wb") as f:
                f.write(np.asarray(self[:1000]).tobytes())
            raise OSError(errno.ENOSPC, "No space left on device")

    proc._recording_buf = proc._recording_buf.view(_FailingArray)
    proc.stop_recording()
    meta = _only_json(proc)  # metadata still written
    assert meta["write_failed"] is True and meta["total_samples"] == 1000
    assert gov.state.degraded


def test_disk_floor_guard_ends_the_recording_before_enospc(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False, DISK_MIN_FREE_GB=10)
    proc._disk_usage = lambda p: SimpleNamespace(total=1000 * GB, used=996 * GB, free=4 * GB)
    _run_disk_recording(proc, chunks=40, n=100_000)  # 4 s of IQ at 1 Msps: >= 1 check
    meta = _only_json(proc)
    assert meta["stopped_reason"] == "disk_floor"
    assert meta["write_failed"] is False


def test_disk_floor_guard_is_quiet_above_half_the_floor(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=False, DISK_MIN_FREE_GB=10)
    proc._disk_usage = lambda p: SimpleNamespace(total=1000 * GB, used=994 * GB, free=6 * GB)
    _run_disk_recording(proc, chunks=15, n=100_000)  # 1.5 s of IQ: one check, above floor/2
    assert _only_json(proc)["stopped_reason"] == "manual"


def test_sidecar_write_failure_reports_to_the_governor(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()

    def boom(self, *a, **k):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(Path, "write_text", boom)
    proc.stop_recording()
    assert proc.recording_status()["state"] == "idle"  # finalize did not wedge
    assert "json" in gov.state.last_write_error["error"]


async def test_step_4_writes_the_stats_row_without_a_blob(tmp_path):
    proc = _proc(tmp_path, _governor_at(4))
    captured = {}

    async def fake_insert(**kw):
        captured.update(kw)

    proc._db.insert_avg_window = fake_insert
    result = SimpleNamespace(
        summary_psd=SimpleNamespace(frequencies=[1.0, 2.0], num_bins=2),
        center_freq_hz=915e6,
        capture_num=1,
    )
    stats = SimpleNamespace(average=0.0, max=0.0, median=0.0, std=0.0, kurtosis=0.0)
    await proc._persist_avg_window([1.0, 2.0], result, stats)
    assert captured["powers"] is None
    assert captured["pwr_avg"] == 0.0


async def test_sqlite_disk_full_reports_to_the_governor(tmp_path):
    import sqlite3

    gov = _governor_at(0)
    proc = _proc(tmp_path, gov)

    async def full(**kw):
        raise sqlite3.OperationalError("database or disk is full")

    proc._db.insert_avg_window = full
    result = SimpleNamespace(
        summary_psd=SimpleNamespace(frequencies=[1.0, 2.0], num_bins=2),
        center_freq_hz=915e6,
        capture_num=1,
    )
    stats = SimpleNamespace(average=0.0, max=0.0, median=0.0, std=0.0, kurtosis=0.0)
    await proc._persist_avg_window([1.0, 2.0], result, stats)
    assert "disk is full" in gov.state.last_write_error["error"]
