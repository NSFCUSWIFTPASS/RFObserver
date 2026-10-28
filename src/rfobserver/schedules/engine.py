"""Runs scheduled captures from inside the receiver loop.

``on_chunk`` is called by the receiver thread for every chunk, right after
the chunk is written to the ring. It starts the runs that are due (each a
RunCutter), feeds the active ones and hands their ops to the writer. It
never blocks: the plan is rebuilt only when the book changes, and the writer
only enqueues. ``on_grid`` (dispatch thread) adds each run's PSD rows. A run
whose cutter is done stays open until its PSD rows have arrived (or
GRID_WAIT_SEC passed), then is closed and the writer finishes the files.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.capture.buffer import trim_grid_rows
from rfobserver.schedules.book import RUNS_KEPT
from rfobserver.schedules.cutter import RunCutter, RunSpec
from rfobserver.schedules.model import first_run_from, run_start_ms, runs_between

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from rfobserver.schedules.book import ScheduleBook
    from rfobserver.schedules.cutter import IQ, RingReader
    from rfobserver.schedules.model import Schedule
    from rfobserver.schedules.writer import ScheduledWriter

logger = logging.getLogger(__name__)

MAX_CONCURRENT_RUNS = 2
GRID_WAIT_SEC = 2.0

Key = tuple[str, int]


@dataclass
class _Active:
    cutter: RunCutter
    path: Path
    closing_since: float | None = None  # monotonic, once the cutter is done
    grid_until: int | None = None  # stream position the PSD rows must reach
    grid_seen: int = 0
    grid_offset: int | None = None  # file index of the first PSD row
    grid_rows: int = 0  # PSD rows handed to the writer (filler included)
    first_chunk: int | None = None  # stream position of the chunk the run began in
    # PSD rows that arrived ahead of earlier ones (workers finish out of
    # order), by row number, and the filler value for rows that never come.
    pending: dict[int, Any] = field(default_factory=dict)
    filler: float | None = None
    grid_meta: dict[str, Any] | None = None
    grid_dropped: int = 0


class ScheduleEngine:
    def __init__(
        self,
        book: ScheduleBook,
        writer: ScheduledWriter | Any,
        *,
        directory: Callable[[], Path],
        refusal: Callable[[], str | None],
        capture_meta: Callable[[], dict[str, Any]],
        grace_ms: int,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._book = book
        self._writer = writer
        self._directory = directory
        self._refusal = refusal
        self._capture_meta = capture_meta
        self._grace_ms = grace_ms
        self._now = now
        self._version = -1
        self._next: dict[str, tuple[Schedule, int | None]] = {}
        self._active: dict[Key, _Active] = {}
        self._lock = threading.Lock()  # _active, shared with the dispatch thread
        self._marked = False
        self._refused: dict[Key, int] = {}
        # PSD rows have arrived at least once: closing waits for a run's rows.
        self._grids_flow = False

    # --- receiver thread ---

    def on_chunk(
        self,
        chunk: IQ,
        chunk_start: int,
        capture_time: float,
        gaps: Sequence[tuple[int, int]],
        center_freq_hz: float,
        rate_hz: float,
        ring: RingReader | None,
    ) -> None:
        if not self._marked:
            self._marked = True
            self.mark_missed(int(capture_time * 1000))
        self._refresh(capture_time)
        n = len(chunk)
        # Samples lost before the chunk's first sample (offset 0) are before
        # capture_time; the ones inside it push its end out.
        chunk_end = capture_time + (n + sum(lost for off, lost in gaps if off > 0)) / rate_hz
        oldest_time = capture_time
        if ring is not None:
            oldest = ring.bounds()[0]
            oldest_time = capture_time - (chunk_start - oldest) / rate_hz
        self._start_due(chunk_end, oldest_time, center_freq_hz, rate_hz)

        with self._lock:
            active = list(self._active.items())
        for key, act in active:
            if act.closing_since is None:
                ops = act.cutter.feed(chunk, chunk_start, capture_time, gaps, center_freq_hz, ring)
                if act.first_chunk is None and act.cutter.started:
                    act.first_chunk = chunk_start
                if ops:
                    self._refused[key] = self._refused.get(key, 0) + self._writer.put_ops(key, ops)
                if act.cutter.done:
                    act.closing_since = time.monotonic()
                    start = act.cutter.stream_start
                    act.grid_until = None if start is None else act.cutter.stream_end()
            if act.closing_since is not None and (
                not self._grids_flow
                or act.grid_until is None
                or (act.grid_seen >= act.grid_until and not act.pending)
                or time.monotonic() - act.closing_since >= GRID_WAIT_SEC
            ):
                self._close(key)

    def _refresh(self, capture_time: float) -> None:
        version, schedules = self._book.snapshot()
        if version == self._version:
            return
        self._version = version
        now_ms = int(capture_time * 1000)
        fresh: dict[str, tuple[Schedule, int | None]] = {}
        for s in schedules:
            if s.id in self._next:
                fresh[s.id] = self._next[s.id]
                continue
            k = first_run_from(s, now_ms - self._grace_ms)
            last = self._book.last_run(s.id)
            if k is not None and last is not None and k <= last:
                k = first_run_from(s, run_start_ms(s, last + 1))
            fresh[s.id] = (s, k)
        self._next = fresh

    def _start_due(
        self, chunk_end: float, oldest_time: float, center_freq_hz: float, rate_hz: float
    ) -> None:
        for sid, (s, k) in list(self._next.items()):
            while k is not None and run_start_ms(s, k) / 1000 < chunk_end:
                self._start_run(s, k, oldest_time, center_freq_hz, rate_hz)
                k = first_run_from(s, run_start_ms(s, k) + 1)
            self._next[sid] = (s, k)

    def _start_run(
        self, s: Schedule, k: int, oldest_time: float, center_freq_hz: float, rate_hz: float
    ) -> None:
        start_ms = run_start_ms(s, k)
        if (start_ms + s.duration_ms) / 1000 <= oldest_time:
            self._book.report_run(s.id, k, "missed", start_ms=start_ms, reason="not_running")
            return
        with self._lock:
            # Runs only waiting for their PSD rows no longer record IQ.
            recording = sum(1 for a in self._active.values() if a.closing_since is None)
            busy = recording >= MAX_CONCURRENT_RUNS
        if busy:
            self._book.report_run(s.id, k, "missed", start_ms=start_ms, reason="busy")
            return
        refused = self._refusal()
        if refused is not None:
            self._book.report_run(
                s.id, k, "missed", start_ms=start_ms, reason="storage", info={"detail": refused}
            )
            return
        spec = RunSpec(
            schedule_id=s.id,
            run=k,
            start_ms=start_ms,
            n_samples=max(1, round(s.duration_ms * rate_hz / 1000)),
            rate_hz=rate_hz,
            center_freq_hz=center_freq_hz,
            label=s.label,
        )
        path = self._path(spec)
        key = (s.id, k)
        self._writer.open(key, path, spec)
        with self._lock:
            self._active[key] = _Active(cutter=RunCutter(spec), path=path)
        self._book.report_run(s.id, k, "recording", start_ms=start_ms, filename=path.name)

    def _path(self, spec: RunSpec) -> Path:
        meta = self._capture_meta()
        dt = datetime.fromtimestamp(spec.start_ms // 1000, tz=timezone.utc)
        stem = (
            f"{meta.get('serial') or 'sensor'}-{meta.get('hostname') or 'host'}-"
            f"{dt:%Y%m%dT%H%M%S}{spec.start_ms % 1000:03d}-sched-{spec.schedule_id}-{spec.run}"
        )
        # No stat here (receiver thread): the writer picks another name if
        # this one exists.
        return self._directory() / f"{stem}.sc16"

    def _close(self, key: Key) -> None:
        with self._lock:
            act = self._active.pop(key, None)
        if act is None:
            return
        self._flush_rows(key, act, final=True)
        cut = act.cutter
        refused = self._refused.pop(key, 0)
        info: dict[str, Any] = {
            **cut.info(),
            **self._capture_meta(),
            "status": "partial" if refused and cut.status == "recorded" else cut.status,
            "reason": cut.reason,
            "writer_refused_samples": refused,
            "grid_rows_dropped": act.grid_dropped,
        }
        self._writer.close(key, info)

    def stop(self, reason: str) -> None:
        """End every active run (pipeline stopping or reconfiguring)."""
        with self._lock:
            keys = list(self._active)
        for key in keys:
            with self._lock:
                act = self._active.get(key)
            if act is not None and act.closing_since is None:
                act.cutter.end(reason)
            self._close(key)

    def mark_missed(self, now_ms: int) -> None:
        """Report runs due while nothing was recording (the sensor was off or
        the pipeline stopped) as missed: the newest RUNS_KEPT per schedule."""
        _, schedules = self._book.snapshot()
        until = now_ms - self._grace_ms
        for s in schedules:
            last = self._book.last_run(s.id)
            t0 = s.start_ms if last is None else run_start_ms(s, last + 1)
            # A schedule joined late has no runs before it existed.
            t0 = max(t0, s.created_ms - self._grace_ms)
            for k in runs_between(s, t0, until, RUNS_KEPT):
                self._book.report_run(
                    s.id, k, "missed", start_ms=run_start_ms(s, k), reason="not_running"
                )

    # --- dispatch thread ---

    def on_grid(
        self, psd_grid: Any, chunk_start: int, slice_samples: int, cal_offset_db: float | None
    ) -> None:
        self._grids_flow = True
        with self._lock:
            active = list(self._active.items())
        if not active:
            return
        grid = psd_grid.grid
        rows_n = int(grid.shape[0]) if grid.ndim == 2 else 0
        time_axis = getattr(psd_grid, "time_axis", None)
        time_res = (
            float(time_axis[1] - time_axis[0])
            if time_axis is not None and len(time_axis) > 1
            else 0.0
        )
        meta = {
            "freq_axis": psd_grid.freq_axis,
            "time_res": time_res,
            "slice_samples": slice_samples,
            "cal_offset_db": cal_offset_db,
        }
        for key, act in active:
            with self._lock:
                act.grid_seen = max(act.grid_seen, chunk_start + rows_n * slice_samples)
                if act.grid_meta is None:
                    act.grid_meta = meta
                self._place_rows(act, grid, chunk_start, slice_samples)
            self._flush_rows(key, act, final=False)

    @staticmethod
    def _place_rows(act: _Active, grid: IQ, chunk_start: int, slice_samples: int) -> None:
        """Queue this chunk's PSD rows for a run by row number in the file:
        the file has zeros where samples were lost and the stream does not,
        so each row goes where its first sample is in the file. Row 0 is the
        run's first row boundary (chunks share row boundaries), whatever
        chunk's rows arrive first. Rows past the file's end are dropped."""
        cut = act.cutter
        start = cut.stream_start
        if start is None or act.first_chunk is None or slice_samples <= 0:
            return
        end = cut.stream_end() if cut.done else start + cut.spec.n_samples
        if end is None:
            return
        if act.grid_offset is None:
            steps = -((act.first_chunk - start) // slice_samples)  # ceil((start - first) / slice)
            act.grid_offset = cut.file_index(act.first_chunk + steps * slice_samples) or 0
        rows, first = trim_grid_rows(grid, chunk_start, slice_samples, start, end)
        if rows.size == 0:
            return
        low = float(rows.min())
        act.filler = low if act.filler is None else min(act.filler, low)
        last_row = (cut.spec.n_samples - act.grid_offset) // slice_samples  # rows that fit
        fill = np.full(rows.shape[1], low, dtype=np.float32)
        for k in range(rows.shape[0]):
            pos = first + k * slice_samples
            index = cut.file_index(pos)
            if index is None:
                continue
            r = round((index - act.grid_offset) / slice_samples)
            if pos - slice_samples >= start:
                # Rows between this one and the row before it in the stream
                # cover zero fill: no chunk will ever bring them.
                prev = cut.file_index(pos - slice_samples)
                if prev is not None:
                    for hole in range(round((prev - act.grid_offset) / slice_samples) + 1, r):
                        if act.grid_rows <= hole < last_row:
                            act.pending.setdefault(hole, fill)
            if act.grid_rows <= r < last_row:
                act.pending[r] = np.array(rows[k], dtype=np.float32)  # a copy: held a while

    def _flush_rows(self, key: Key, act: _Active, *, final: bool) -> None:
        """Hand the writer the rows that are next in order; at the end
        (``final``), fill rows that never arrived up to the last one held."""
        with self._lock:
            out: list[IQ] = []
            if final and act.pending:
                top = max(act.pending)
                fill = np.full(
                    len(next(iter(act.pending.values()))), act.filler or 0.0, dtype=np.float32
                )
                while act.grid_rows <= top:
                    out.append(act.pending.pop(act.grid_rows, fill))
                    act.grid_rows += 1
            while act.grid_rows in act.pending:
                out.append(act.pending.pop(act.grid_rows))
                act.grid_rows += 1
            meta = act.grid_meta
            offset = act.grid_offset or 0
        if not out or meta is None:
            return
        rows = np.vstack(out).astype(np.float32, copy=False)
        ok = self._writer.put_grid(
            key,
            rows,
            freq_axis=meta["freq_axis"],
            time_res=meta["time_res"],
            offset_samples=offset,
            slice_samples=meta["slice_samples"],
            cal_offset_db=meta["cal_offset_db"],
        )
        if ok is False:
            act.grid_dropped += int(rows.shape[0])
