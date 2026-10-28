"""Scheduled captures: the writer thread and the files it leaves."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.schedules.cutter import RunSpec
from rfobserver.schedules.writer import FinishedRun, ScheduledWriter
from rfobserver.storage import psd_grid
from rfobserver.storage.sigmf_export import iq_sigmf_meta

if TYPE_CHECKING:
    from pathlib import Path

SPEC = RunSpec(
    schedule_id="sch1",
    run=2,
    start_ms=1793212200250,
    n_samples=30,
    rate_hz=1000.0,
    center_freq_hz=915e6,
    label="test window",
)
INFO: dict[str, Any] = {
    "status": "partial",
    "reason": None,
    "requested_start": "2026-10-28T18:30:00.250Z",
    "first_sample_time": "2026-10-28T18:30:00.250000000Z",
    "first_sample_epoch": 1793212200.25,
    "start_error_ns": 0,
    "late_start_samples": 0,
    "gaps": [[10, 5]],
    "lost_samples": 5,
    "stream_start": 1234,
    "samples": 30,
    "gain_db": 40.0,
    "hostname": "jet",
    "serial": "ABC",
    "clock_source": "ntp",
    "clock_server": "pool",
    "clock_offset_ms": None,
}


class Done:
    def __init__(self) -> None:
        self.runs: list[FinishedRun] = []
        self.event = threading.Event()

    def __call__(self, run: FinishedRun) -> None:
        self.runs.append(run)
        self.event.set()


def run_writer(path: Path, ops: list[Any], *, budget: int = 1 << 20, grid: bool = False):
    done = Done()
    w = ScheduledWriter(on_finished=done, max_bytes_in_flight=budget)
    w.start()
    key = ("sch1", 2)
    w.open(key, path, SPEC)
    refused = w.put_ops(key, ops)
    if grid:
        rows = np.arange(12, dtype=np.float32).reshape(3, 4)
        w.put_grid(
            key,
            rows,
            freq_axis=np.linspace(-1, 1, 4),
            time_res=0.01,
            offset_samples=0,
            slice_samples=10,
            cal_offset_db=None,
        )
    w.close(key, dict(INFO))
    assert done.event.wait(5)
    w.stop(5)
    return done.runs[0], refused


def test_ops_become_the_file_and_json(tmp_path: Path) -> None:
    path = tmp_path / "x-sched.sc16"
    a = np.arange(10, dtype=np.int32)
    b = np.arange(100, 115, dtype=np.int32)
    run, refused = run_writer(path, [("iq", a), ("zeros", 5), ("iq", b)])
    assert refused == 0
    data = np.fromfile(path, dtype=np.int32)
    assert np.array_equal(data, np.concatenate([a, np.zeros(5, np.int32), b]))
    assert run.total_samples == 30 and run.write_error is None and run.path == path
    meta = json.loads(path.with_suffix(".json").read_text())
    assert meta["origin"] == "scheduled"
    assert meta["total_samples"] == 30 and meta["total_bytes"] == 120
    assert meta["gaps"] == [] and meta["lost_samples"] == 0  # contiguous in time
    assert meta["duration_sec"] == 0.03 and meta["time_span_sec"] == 0.03
    assert meta["start_time"] == "2026-10-28T18:30:00.250000+00:00"
    assert meta["center_freq_hz"] == 915e6 and meta["sample_rate_hz"] == 1000.0
    sch = meta["schedule"]
    assert sch["id"] == "sch1" and sch["run"] == 2 and sch["label"] == "test window"
    assert sch["zero_filled"] == [[10, 5]] and sch["zero_filled_samples"] == 5
    assert sch["clock_source"] == "ntp" and sch["status"] == "partial"


def test_budget_refuses_iq_and_fills_zeros(tmp_path: Path) -> None:
    path = tmp_path / "y-sched.sc16"
    big = np.arange(20, dtype=np.int32)  # 80 bytes, over a 40-byte budget
    small = np.arange(5, dtype=np.int32)
    run, refused = run_writer(path, [("iq", small), ("iq", big)], budget=40)
    assert refused == 20
    data = np.fromfile(path, dtype=np.int32)
    assert len(data) == 25 and np.all(data[5:] == 0)
    sch = json.loads(path.with_suffix(".json").read_text())["schedule"]
    assert [5, 20] in sch["zero_filled"] and sch["status"] == "partial"


def test_grid_is_written_and_loadable(tmp_path: Path) -> None:
    path = tmp_path / "g-sched.sc16"
    run_writer(path, [("iq", np.arange(30, dtype=np.int32))], grid=True)
    loaded = psd_grid.load_grid(path)
    assert loaded is not None
    grid, meta = loaded
    assert grid.shape == (3, 4) and meta["slice_samples"] == 10
    assert meta["grid_min"] == 0.0 and meta["grid_max"] == 11.0


def test_unwritable_dir_still_finishes(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "z.sc16"
    run, _ = run_writer(path, [("iq", np.arange(3, dtype=np.int32))])
    assert run.write_error is not None and run.total_samples == 0


def test_sigmf_carries_the_schedule_and_marks_zero_fill(tmp_path: Path) -> None:
    path = tmp_path / "s-sched.sc16"
    run_writer(
        path,
        [
            ("iq", np.arange(10, dtype=np.int32)),
            ("zeros", 5),
            ("iq", np.arange(15, dtype=np.int32)),
        ],
    )
    meta = json.loads(path.with_suffix(".json").read_text())
    doc = iq_sigmf_meta(meta)
    assert doc["global"]["rfobs:schedule"]["id"] == "sch1"
    assert len(doc["captures"]) == 1  # no discontinuity: zeros keep the timing
    (ann,) = doc["annotations"]
    assert ann["core:sample_start"] == 10 and ann["core:sample_count"] == 5
    assert "zero" in ann["core:comment"]


def test_clock_details_come_from_the_writer(tmp_path: Path, monkeypatch) -> None:
    from rfobserver.schedules import writer as writer_mod

    monkeypatch.setattr(
        writer_mod,
        "clock_info",
        lambda: {"clock_source": "ntp", "clock_server": "local.ntp", "clock_offset_ms": 0.2},
    )
    path = tmp_path / "c-sched.sc16"
    info = {k: v for k, v in INFO.items() if not k.startswith("clock_")}
    done = Done()
    w = ScheduledWriter(on_finished=done)
    w.start()
    w.open(("sch1", 2), path, SPEC)
    w.put_ops(("sch1", 2), [("iq", np.arange(30, dtype=np.int32))])
    w.close(("sch1", 2), info)
    assert done.event.wait(5)
    w.stop(5)
    sch = json.loads(path.with_suffix(".json").read_text())["schedule"]
    assert sch["clock_server"] == "local.ntp" and sch["clock_offset_ms"] == 0.2
    assert done.runs[0].info["clock_server"] == "local.ntp"


def test_an_existing_file_gets_a_new_name(tmp_path: Path) -> None:
    path = tmp_path / "dup-sched.sc16"
    path.write_bytes(b"old")
    run, _ = run_writer(path, [("iq", np.arange(30, dtype=np.int32))])
    assert path.read_bytes() == b"old"
    assert run.path == tmp_path / "dup-sched-2.sc16" and run.total_samples == 30
    assert json.loads(run.path.with_suffix(".json").read_text())["file"] == run.path.name
