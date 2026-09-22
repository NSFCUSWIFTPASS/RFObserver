"""The dispatch loop hands a finished chunk on as soon as it is done.

It used to collect finished results only at the top of its loop, then block up
to 0.5 s in _chunk_queue.get(), which returns early only when a NEW chunk
arrives. So every chunk waited for the next one before its PSD reached the
waterfall, burst detector and recording: a fixed extra chunk period (~205 ms
at the default chunk size) on every chunk.
See docs/debugging/2026-09-22_psd-pipeline-latency.md.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import numpy as np

from rfobserver.pipeline.streaming import _STOP
from tests.unit.test_recording_gaps import _proc

if TYPE_CHECKING:
    from pathlib import Path


def test_finished_chunk_is_handled_without_waiting_for_the_next(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._running = True
    handled = threading.Event()
    handled_at: list[float] = []

    # Processing must outlast the loop's turnaround back into its blocking get():
    # an instantaneous stub finishes first and hides the defect entirely.
    work_s = 0.05

    def slow_process(sc16_buf, recv_time, chunk_start, *rest):  # type: ignore[no-untyped-def]
        time.sleep(work_s)
        return (sc16_buf, recv_time, chunk_start)

    def record(_result):  # type: ignore[no-untyped-def]
        handled_at.append(time.monotonic())
        handled.set()

    proc._process_one_chunk = slow_process  # type: ignore[method-assign]
    proc._handle_chunk_result = record  # type: ignore[method-assign]

    t = threading.Thread(target=proc._dispatch_loop, daemon=True)
    t.start()
    try:
        # Exactly one chunk and no follow-up: nothing else will wake the loop.
        submitted = time.monotonic()
        proc._chunk_queue.put((np.zeros(16, dtype=np.int32), submitted, 0))
        assert handled.wait(2.0), "chunk was never handled"
        waited = handled_at[0] - submitted
        # Before the fix this was ~0.5 s: the loop sat in get() until its timeout.
        assert waited < work_s + 0.1, f"chunk handled {waited * 1000:.0f} ms after submission"
    finally:
        proc._running = False
        proc._chunk_queue.put(_STOP)
        t.join(5.0)
