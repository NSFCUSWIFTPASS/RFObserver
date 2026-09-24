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


# --- no churn after a disk_floor / write_error stop (validation Finding 4) ----


def _healthy_tick(gov: StorageGovernor) -> None:
    gov.tick(
        StorageSample(
            data=VolumeSample(200 * GB, 1000 * GB),
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


def _disk_floor_stop(tmp_path, gov):
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False, DISK_MIN_FREE_GB=10)
    proc._disk_usage = lambda p: SimpleNamespace(total=1000 * GB, used=996 * GB, free=4 * GB)
    _run_disk_recording(proc, chunks=40, n=100_000)
    assert _only_json(proc)["stopped_reason"] == "disk_floor"
    return proc


def test_disk_floor_stop_holds_starts_until_the_next_governor_tick(tmp_path):
    gov = _governor_at(0)
    proc = _disk_floor_stop(tmp_path, gov)
    proc.start_recording()
    st = proc.recording_status()
    assert st["state"] == "idle"
    assert "held" in st["refused"] and "disk_floor" in st["refused"]
    proc.arm_trigger()
    assert proc.recording_status()["state"] == "idle"
    # The first tick to complete may have started before the stop: still held.
    _healthy_tick(gov)
    proc.start_recording()
    assert proc.recording_status()["state"] == "idle"
    assert "held" in proc.recording_status()["refused"]
    # The second must have started after it: released.
    _healthy_tick(gov)
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
    finally:
        proc.stop_recording()


def test_a_tick_in_flight_at_the_stop_does_not_release_the_hold(tmp_path):
    """A tick whose sample was taken before the stop completes after it; its
    stale sample must not release the hold (validation Finding 4 race)."""
    gov = _governor_at(0)
    pre_stop = StorageSample(
        data=VolumeSample(200 * GB, 1000 * GB),
        db_volume=None,
        db_file_bytes=0,
        db_reusable_bytes=0,
        auto_bytes=0,
        manual_bytes=0,
        evictable_auto=False,
    )  # sampled before the stop, while free space still looked fine
    proc = _disk_floor_stop(tmp_path, gov)
    gov.tick(pre_stop, min_free_gb=0, now=T0)  # the in-flight tick completes
    proc.start_recording()
    assert proc.recording_status()["state"] == "idle"
    assert "held" in proc.recording_status()["refused"]
    _healthy_tick(gov)  # a tick that began after the stop
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
    finally:
        proc.stop_recording()


def test_armed_trigger_does_not_fire_during_the_hold(tmp_path):
    gov = _governor_at(0)
    proc = _disk_floor_stop(tmp_path, gov)
    proc._recording_state = "armed"  # e.g. continuous re-arm on idle
    proc._settings.TRIGGER_THRESHOLD_DB = -200.0
    proc._check_trigger_and_record(np.full(4096, 1 << 20, dtype=np.int32), (), 0)
    assert proc.recording_status()["state"] == "armed"


def test_write_error_stop_holds_starts(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False)
    _patch_sc16_open(monkeypatch, ok=6000)
    _run_disk_recording(proc, chunks=50)
    assert _only_json(proc)["stopped_reason"] == "write_error"
    proc.start_recording()
    assert proc.recording_status()["state"] == "idle"
    assert "write_error" in proc.recording_status()["refused"]


def test_manual_stop_does_not_hold(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc.stop_recording()
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
    finally:
        proc.stop_recording()


def test_no_governor_no_hold_after_disk_floor(tmp_path):
    proc = _disk_floor_stop(tmp_path, None)
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
    finally:
        proc.stop_recording()


def test_two_starts_in_the_same_second_get_distinct_files(tmp_path, monkeypatch):
    from rfobserver.pipeline import streaming

    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 23, 23, 14, 15, tzinfo=timezone.utc)

    monkeypatch.setattr(streaming, "datetime", _Frozen)
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    names = []
    for _ in range(3):
        proc.start_recording()
        proc._write_recording_chunk(np.ones(1000, dtype=np.int32))
        names.append(proc.recording_status()["file"])
        proc.stop_recording()
    files = sorted(p.name for p in proc._storage.manual_dir.glob("*.sc16"))
    assert len(set(names)) == 3 and len(files) == 3
    assert names[1].endswith("20260923T231415-2.sc16")
    assert names[2].endswith("20260923T231415-3.sc16")
    jsons = [p for p in proc._storage.manual_dir.glob("*.json")]
    assert len(jsons) == 3


# --- F5: no 0-byte .json on a full disk --------------------------------------


def _truncate_then_enospc(monkeypatch) -> None:
    """Like a real ENOSPC: the open truncates/creates the file, the write fails."""

    def full(self, *a, **k):
        with open(self, "w"):
            pass
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(Path, "write_text", full)


def test_full_disk_leaves_no_capture_json(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc._write_recording_chunk(np.ones(1000, dtype=np.int32))
    _truncate_then_enospc(monkeypatch)
    proc.stop_recording()
    left = [p.name for p in proc._storage.manual_dir.iterdir() if ".json" in p.name]
    assert left == []
    assert "json" in gov.state.last_write_error["error"]


def test_full_disk_leaves_no_psd_json(tmp_path, monkeypatch):
    from rfobserver.storage import psd_grid

    _truncate_then_enospc(monkeypatch)
    meta = tmp_path / "x.psd.json"
    with pytest.raises(OSError):
        psd_grid.write_meta(
            meta,
            rows=1,
            num_bins=2,
            time_resolution_s=0.1,
            center_freq_hz=915e6,
            bandwidth_hz=1e6,
            freq_axis=np.array([1.0, 2.0]),
            grid_min=0.0,
            grid_max=1.0,
            cal_offset_db=None,
        )
    assert list(tmp_path.iterdir()) == []


# --- F7: no orphaned .detections.json after eviction -------------------------


async def test_sidecar_is_skipped_when_the_capture_was_evicted(tmp_path, monkeypatch):
    from rfobserver.storage import detections_sidecar

    written = []

    async def fake_write_sidecar(sc16_path, db):
        written.append(sc16_path)
        sc16_path.with_suffix(".detections.json").write_text("[]")

    monkeypatch.setattr(detections_sidecar, "write_sidecar", fake_write_sidecar)
    proc = _proc(tmp_path, None)
    sc16 = proc._storage.auto_dir / "a.sc16"
    sc16.write_bytes(b"\0" * 8)
    import asyncio

    task = asyncio.ensure_future(proc._deferred_sidecar(sc16, 0.05))
    sc16.unlink()  # evicted inside the grace window
    await task
    assert written == []
    assert list(proc._storage.auto_dir.iterdir()) == []

    kept = proc._storage.auto_dir / "b.sc16"
    kept.write_bytes(b"\0" * 8)
    await proc._deferred_sidecar(kept, 0.0)
    assert written == [kept]


@pytest.mark.parametrize("replay", [False, True])
async def test_sidecar_is_removed_when_the_capture_is_evicted_during_its_write(
    tmp_path, monkeypatch, replay
):
    """Task 12 item 1: the eviction lands after the grace check but while the
    sidecar is being built (the DB query); the finished sidecar is an orphan."""
    from rfobserver.storage import detections_sidecar

    def build_and_evict(sc16_path):
        sc16_path.unlink()  # evicted while the query / re-detection runs
        sc16_path.with_suffix(".detections.json").write_text("[]")

    async def fake_write_sidecar(sc16_path, db):
        build_and_evict(sc16_path)

    def fake_write_sidecar_from_grid(sc16_path, cfg):
        build_and_evict(sc16_path)

    monkeypatch.setattr(detections_sidecar, "write_sidecar", fake_write_sidecar)
    monkeypatch.setattr(detections_sidecar, "write_sidecar_from_grid", fake_write_sidecar_from_grid)
    proc = _proc(tmp_path, None)
    proc._replay_mode = replay
    sc16 = proc._storage.auto_dir / "a.sc16"
    sc16.write_bytes(b"\0" * 8)
    await proc._deferred_sidecar(sc16, 0.0)
    assert list(proc._storage.auto_dir.iterdir()) == []


# --- F8: recording_status "refused" is current -------------------------------


def test_status_refused_clears_on_recovery_without_a_start(tmp_path):
    gov = _governor_at(3)
    proc = _proc(tmp_path, gov)
    proc.start_recording()
    assert proc.recording_status()["refused"]
    for _ in range(3):
        _healthy_tick(gov)
    assert gov.state.step == 0
    assert proc.recording_status()["refused"] is None


def test_status_refused_shows_a_refusal_in_force_without_a_start(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov)
    assert proc.recording_status()["refused"] is None
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
    assert "floor" in proc.recording_status()["refused"]


# --- final review I2: an abandoned writer cannot touch the next recording -----


class _StuckFile:
    """A .sc16 whose first write blocks (a hung NFS/USB volume) until released,
    then fails."""

    def __init__(self, real, release) -> None:
        self.real, self.release = real, release

    def write(self, data) -> int:
        self.release.wait(timeout=10)
        raise OSError(errno.EIO, "Input/output error")

    def close(self) -> None:
        self.real.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def test_an_abandoned_writer_cannot_fail_or_starve_the_next_recording(tmp_path, monkeypatch):
    import builtins
    import threading

    from rfobserver.pipeline import streaming

    monkeypatch.setattr(streaming, "_WRITER_JOIN_TIMEOUT_SEC", 0.3)
    real_open = builtins.open
    release = threading.Event()
    sc16_opens = []

    def fake_open(path, mode="r", *a, **k):
        f = real_open(path, mode, *a, **k)
        if str(path).endswith(".sc16") and "w" in mode:
            sc16_opens.append(path)
            if len(sc16_opens) == 1:
                return _StuckFile(f, release)
        return f

    monkeypatch.setattr("rfobserver.pipeline.streaming.open", fake_open, raising=False)
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=False)
    proc._await_tail_grids = lambda: None

    # Recording 1: its writer blocks on the first write and is abandoned.
    proc.start_recording()
    pos = proc._pre_trigger_buf.total_written
    proc._check_trigger_and_record(np.ones(1000, dtype=np.int32), (), pos)
    old_writer = proc._writer_thread
    proc.stop_recording()
    assert old_writer is not None and old_writer.is_alive()

    # Recording 2 begins; only then does the old write fail.
    proc.start_recording()
    new_writer = proc._writer_thread
    assert new_writer is not None and new_writer is not old_writer
    release.set()
    old_writer.join(timeout=5)
    assert not old_writer.is_alive()  # superseded: it exits instead of lingering

    assert proc._writer_error is None
    pos = proc._pre_trigger_buf.total_written
    for _ in range(3):
        proc._check_trigger_and_record(np.ones(1000, dtype=np.int32), (), pos)
        pos += 1000
    assert proc.recording_status()["state"] == "recording"  # not ended as write_error
    proc.stop_recording()
    assert not new_writer.is_alive()  # its own sentinel reached it
    second = Path(sc16_opens[1])
    meta = json.loads(second.with_suffix(".json").read_text())
    assert meta["write_failed"] is False
    assert meta["stopped_reason"] == "manual"
    assert second.stat().st_size == 3000 * 4


# --- final review M2: a RAM-mode flush failure holds new starts ---------------


def test_ram_flush_failure_holds_starts_until_two_ticks(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc._write_recording_chunk(np.ones(4000, dtype=np.int32))

    class _FailingArray(np.ndarray):
        def tofile(self, path, *a, **k):
            raise OSError(errno.ENOSPC, "No space left on device")

    proc._recording_buf = proc._recording_buf.view(_FailingArray)
    proc.stop_recording()  # a manual stop, but the flush failed
    proc.start_recording()
    assert proc.recording_status()["state"] == "idle"
    assert "held" in proc.recording_status()["refused"]
    assert "write_error" in proc.recording_status()["refused"]
    _healthy_tick(gov)
    proc.start_recording()
    assert proc.recording_status()["state"] == "idle"
    _healthy_tick(gov)
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
    finally:
        proc.stop_recording()
