"""The scheduled captures' writer thread.

Separate from the manual/trigger recorder so a scheduled run can be written
while another capture is. The receiver thread only enqueues (never blocks):
IQ beyond the byte budget is refused and written as zeros instead, so a slow
disk costs samples, never timing. On close the writer leaves the same
companions as a manual capture (``.json``, ``.psd`` + ``.psd.json``) and
hands the result to ``on_finished``.

The ``.json`` describes a file that is contiguous in time: its top-level
``gaps`` is empty and ``lost_samples`` 0, so every reader of manual captures
(SigMF export, detections sidecar, waterfall) times it correctly. The
zero-filled stretches are in ``schedule.zero_filled``.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.storage import psd_grid
from rfobserver.storage.governor import describe_write_error

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from rfobserver.schedules.cutter import IQ, Op, RunSpec

logger = logging.getLogger(__name__)

Key = tuple[str, int]
_ZERO_BLOCK = np.zeros(1 << 18, dtype=np.int32)  # 1 MiB


@dataclass
class FinishedRun:
    key: Key
    path: Path
    spec: RunSpec
    info: dict[str, Any]
    total_samples: int
    write_error: str | None


@dataclass
class _Open:
    path: Path
    spec: RunSpec
    fh: Any = None
    grid_fh: Any = None
    grid_rows: int = 0
    grid_meta: dict[str, Any] | None = None
    grid_min: float = float("inf")
    grid_max: float = float("-inf")
    error: str | None = None


@dataclass
class _Enqueued:
    """Receiver-side view of one run: file position and refused stretches."""

    pos: int = 0
    refused: list[list[int]] = field(default_factory=list)


class ScheduledWriter:
    def __init__(
        self,
        on_finished: Callable[[FinishedRun], None],
        max_bytes_in_flight: int = 256 * 2**20,
    ) -> None:
        self._on_finished = on_finished
        self._budget = max_bytes_in_flight
        self._in_flight = 0
        self._lock = threading.Lock()
        self._q: queue.SimpleQueue[tuple[Any, ...] | None] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self._enq: dict[Key, _Enqueued] = {}

    # --- lifecycle ---

    def start(self) -> None:
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._loop, name="sched-writer", daemon=True)
            self._thread.start()

    def stop(self, timeout: float) -> None:
        if self._thread is not None:
            self._q.put(None)
            self._thread.join(timeout)
            self._thread = None

    # --- producer side (receiver / dispatch threads; never blocks) ---

    def open(self, key: Key, path: Path, spec: RunSpec) -> None:
        self._enq[key] = _Enqueued()
        self._q.put(("open", key, path, spec))

    def put_ops(self, key: Key, ops: list[Op]) -> int:
        """Queue a run's ops; IQ over the byte budget is written as zeros
        instead. Returns the samples refused that way."""
        enq = self._enq.get(key)
        if enq is None:
            return 0
        refused = 0
        for kind, value in ops:
            if kind == "iq":
                data: IQ = value  # type: ignore[assignment]
                nbytes = int(data.nbytes)
                with self._lock:
                    ok = self._in_flight + nbytes <= self._budget
                    if ok:
                        self._in_flight += nbytes
                if ok:
                    self._q.put(("iq", key, data))
                else:
                    refused += len(data)
                    self._note_refused(enq, len(data))
                    self._q.put(("zeros", key, len(data)))
                enq.pos += len(data)
            else:
                count = int(value)
                self._q.put(("zeros", key, count))
                enq.pos += count
        return refused

    def put_grid(
        self,
        key: Key,
        rows: IQ,
        *,
        freq_axis: IQ,
        time_res: float,
        offset_samples: int,
        slice_samples: int,
        cal_offset_db: float | None,
    ) -> None:
        if key not in self._enq or rows.size == 0:
            return
        data = np.ascontiguousarray(rows, dtype=np.float32)
        with self._lock:
            if self._in_flight + data.nbytes > self._budget:
                return  # companion data: drop rather than wait
            self._in_flight += data.nbytes
        grid_meta = {
            "freq_axis": np.asarray(freq_axis),
            "time_res": float(time_res),
            "offset_samples": int(offset_samples),
            "slice_samples": int(slice_samples),
            "cal_offset_db": cal_offset_db,
        }
        self._q.put(("grid", key, data, grid_meta))

    def close(self, key: Key, info: dict[str, Any]) -> None:
        enq = self._enq.pop(key, None)
        if enq is None:
            return
        info = dict(info)
        info["writer_refused"] = [list(r) for r in enq.refused]
        self._q.put(("close", key, info))

    @staticmethod
    def _note_refused(enq: _Enqueued, n: int) -> None:
        if enq.refused and enq.refused[-1][0] + enq.refused[-1][1] == enq.pos:
            enq.refused[-1][1] += n
        else:
            enq.refused.append([enq.pos, n])

    # --- writer thread ---

    def _release(self, nbytes: int) -> None:
        with self._lock:
            self._in_flight -= nbytes

    def _loop(self) -> None:
        files: dict[Key, _Open] = {}
        while True:
            item = self._q.get()
            if item is None:
                for key in list(files):
                    self._finish(key, files.pop(key), {"status": "partial", "reason": "stopped"})
                return
            kind, key = item[0], item[1]
            try:
                if kind == "open":
                    files[key] = self._open_files(item[2], item[3])
                    continue
                st = files.get(key)
                if kind == "iq":
                    data = item[2]
                    try:
                        if st is not None:
                            self._write(st, data)
                    finally:
                        self._release(int(data.nbytes))
                elif kind == "zeros" and st is not None:
                    left = int(item[2])
                    while left > 0:
                        k = min(left, len(_ZERO_BLOCK))
                        self._write(st, _ZERO_BLOCK[:k])
                        left -= k
                elif kind == "grid":
                    data = item[2]
                    try:
                        if st is not None:
                            self._write_grid(st, data, item[3])
                    finally:
                        self._release(int(data.nbytes))
                elif kind == "close" and st is not None:
                    self._finish(key, files.pop(key), item[2])
            except Exception:
                logger.exception("Scheduled writer: %s for %s failed", kind, key)

    @staticmethod
    def _open_files(path: Path, spec: RunSpec) -> _Open:
        st = _Open(path=path, spec=spec)
        try:
            st.fh = open(path, "wb")  # noqa: SIM115 (kept open across items)
        except OSError as exc:
            st.error = describe_write_error(exc)
        return st

    @staticmethod
    def _write(st: _Open, data: IQ) -> None:
        if st.fh is None or st.error is not None:
            return
        try:
            st.fh.write(np.ascontiguousarray(data, dtype=np.int32).tobytes())
        except OSError as exc:
            st.error = describe_write_error(exc)

    @staticmethod
    def _write_grid(st: _Open, rows: IQ, meta: dict[str, Any]) -> None:
        if st.error is not None:
            return
        raw_path, _ = psd_grid.grid_paths(st.path)
        try:
            if st.grid_fh is None:
                st.grid_fh = open(raw_path, "wb")  # noqa: SIM115
                st.grid_meta = meta
            st.grid_fh.write(rows.tobytes())
        except OSError as exc:
            logger.warning("Scheduled capture grid %s: %s", raw_path.name, exc)
            return
        st.grid_rows += rows.shape[0]
        st.grid_min = min(st.grid_min, float(rows.min()))
        st.grid_max = max(st.grid_max, float(rows.max()))

    def _finish(self, key: Key, st: _Open, info: dict[str, Any]) -> None:
        for fh in (st.fh, st.grid_fh):
            if fh is not None:
                try:
                    fh.close()
                except OSError as exc:
                    st.error = st.error or describe_write_error(exc)
        total = 0
        if st.fh is not None:
            try:
                size = st.path.stat().st_size
                total = size // 4
            except OSError:
                total = 0
        if st.error is None:
            self._write_companions(st, info, total)
        else:
            logger.error("Scheduled capture %s: %s", st.path.name, st.error)
        try:
            self._on_finished(
                FinishedRun(
                    key=key,
                    path=st.path,
                    spec=st.spec,
                    info=info,
                    total_samples=total,
                    write_error=st.error,
                )
            )
        except Exception:
            logger.exception("Scheduled capture %s: finishing failed", st.path.name)

    def _write_companions(self, st: _Open, info: dict[str, Any], total: int) -> None:
        spec = st.spec
        rate = spec.rate_hz
        zero = sorted([list(g) for g in info.get("gaps") or []] + info.get("writer_refused", []))
        zero_samples = sum(int(n) for _, n in zero)
        status = info.get("status") or "partial"
        if status == "recorded" and (zero_samples or total != spec.n_samples):
            status = "partial"
        info["status"] = status
        first = info.get("first_sample_epoch")
        start_dt = datetime.fromtimestamp(
            first if first is not None else spec.start_ms / 1000, tz=timezone.utc
        )
        duration = round(total / rate, 6) if rate > 0 else 0.0
        meta: dict[str, Any] = {
            "file": st.path.name,
            "format": "sc16",
            "origin": "scheduled",
            "sample_rate_hz": rate,
            "center_freq_hz": spec.center_freq_hz,
            "bandwidth_hz": rate,
            "gain_db": info.get("gain_db"),
            "start_time": start_dt.isoformat(),
            "duration_sec": duration,
            "total_bytes": total * 4,
            "total_samples": total,
            "dropped_chunks": 0,
            "overflow_events": 0,
            "lost_samples": 0,
            "gaps": [],
            "gaps_truncated": False,
            "time_span_sec": duration,
            "pre_trigger_sec": 0.0,
            "trigger_initiated": False,
            "stopped_reason": info.get("reason") or "complete",
            "write_failed": False,
            "hostname": info.get("hostname"),
            "serial": info.get("serial"),
            "mean_power_db": None,
            "background_power_db": None,
            "priority": None,
            "schedule": {
                "id": spec.schedule_id,
                "run": spec.run,
                "label": spec.label,
                "status": status,
                "reason": info.get("reason"),
                "requested_start": info.get("requested_start"),
                "first_sample_time": info.get("first_sample_time"),
                "start_error_ns": info.get("start_error_ns"),
                "late_start_samples": info.get("late_start_samples", 0),
                "requested_samples": spec.n_samples,
                "zero_filled": zero,
                "zero_filled_samples": zero_samples,
                "clock_source": info.get("clock_source"),
                "clock_server": info.get("clock_server"),
                "clock_offset_ms": info.get("clock_offset_ms"),
                "clock_synced": info.get("clock_synced"),
            },
        }
        json_path = st.path.with_suffix(".json")
        try:
            psd_grid.write_text_atomic(json_path, json.dumps(meta, indent=2))
        except OSError as exc:
            st.error = describe_write_error(exc)
            return
        g = st.grid_meta
        if st.grid_rows and g is not None:
            _, meta_path = psd_grid.grid_paths(st.path)
            try:
                psd_grid.write_meta(
                    meta_path,
                    rows=st.grid_rows,
                    num_bins=int(g["freq_axis"].shape[0]),
                    time_resolution_s=g["time_res"],
                    center_freq_hz=int(spec.center_freq_hz),
                    bandwidth_hz=int(rate),
                    freq_axis=g["freq_axis"],
                    grid_min=st.grid_min,
                    grid_max=st.grid_max,
                    cal_offset_db=g["cal_offset_db"],
                    start_sample_offset=g["offset_samples"],
                    slice_samples=g["slice_samples"],
                )
            except OSError as exc:
                logger.warning("Scheduled capture grid meta %s: %s", meta_path.name, exc)
