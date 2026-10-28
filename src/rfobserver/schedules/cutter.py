"""Cut one scheduled run out of the live stream, to the sample.

The receiver thread hands every chunk (its samples, stream position, the
host-clock time of its first sample and its receive gaps) to each active
cutter. The first chunk that reaches the requested instant places the start:
the sample nearest that instant on the host clock, reading back from the
ring when the instant is older than the chunk. From there on samples follow
by stream position only, so a host clock step mid-run cannot shift or tear
the file. Lost samples are written as zeros at their place and listed in
``gaps``, so sample index always equals time from the requested start.
Pure: no I/O, no threads; ``feed`` returns the write operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, Protocol

import numpy as np

from rfobserver.schedules.model import iso_ms

if TYPE_CHECKING:
    from collections.abc import Sequence

IQ = np.ndarray[Any, np.dtype[Any]]
Op = tuple[Literal["iq"], IQ] | tuple[Literal["zeros"], int]


class RingReader(Protocol):
    def read_range(self, start: int, end: int) -> IQ | None: ...

    def bounds(self) -> tuple[int, int]: ...


@dataclass
class RunSpec:
    schedule_id: str
    run: int
    start_ms: int
    n_samples: int
    rate_hz: float
    center_freq_hz: float
    label: str


def _iso_ns(t: float) -> str:
    whole = int(t // 1)
    ns = round((t - whole) * 1e9)
    if ns >= 1_000_000_000:
        whole, ns = whole + 1, ns - 1_000_000_000
    dt = datetime.fromtimestamp(whole, tz=timezone.utc)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{ns:09d}Z"


def _place(target: int, gaps: Sequence[tuple[int, int]], n: int) -> tuple[int, int]:
    """Where time-sample ``target`` (samples after the chunk's first sample
    in time) is in a chunk with receive ``gaps``: (offset in the chunk, zeros
    to write first when it falls inside a gap)."""
    lost_before = 0
    for off, lost in sorted(gaps):
        if target < off + lost_before:
            break
        if target < off + lost_before + lost:
            return off, off + lost_before + lost - target
        lost_before += lost
    return min(target - lost_before, n), 0


class RunCutter:
    def __init__(self, spec: RunSpec) -> None:
        self.spec = spec
        self.started = False
        self.done = False
        self.written = 0
        self.reason: str | None = None
        self._next_pos = 0
        self._gaps: list[list[int]] = []
        self._lost = 0
        self._late = 0
        self._first_time: float | None = None
        self._error_ns: int | None = None
        self.stream_start: int | None = None

    # --- results ---

    @property
    def status(self) -> str:
        if not self.started:
            return "missed" if self.done else "pending"
        if not self.done:
            return "recording"
        if self.reason is None and self._lost == 0 and self.written == self.spec.n_samples:
            return "recorded"
        return "partial"

    def info(self) -> dict[str, Any]:
        first = self._first_time
        return {
            "requested_start": iso_ms(self.spec.start_ms),
            "first_sample_time": None if first is None else _iso_ns(first),
            "start_error_ns": self._error_ns,
            "late_start_samples": self._late,
            "gaps": [list(g) for g in self._gaps],
            "lost_samples": self._lost,
            "stream_start": self.stream_start,
            "samples": self.written,
        }

    # --- feeding ---

    def end(self, reason: str) -> list[Op]:
        """End the run early (stopped, retuned). Nothing is padded."""
        if not self.done:
            self.done = True
            self.reason = reason
        return []

    def feed(
        self,
        chunk: IQ,
        chunk_start: int,
        capture_time: float,
        gaps: Sequence[tuple[int, int]],
        center_freq_hz: float,
        ring: RingReader | None,
    ) -> list[Op]:
        if self.done:
            return []
        n = len(chunk)
        if self.started:
            if center_freq_hz != self.spec.center_freq_hz:
                return self.end("retuned")
            ops: list[Op] = []
            if chunk_start > self._next_pos:
                # Stream samples that never reached this cutter.
                self._zeros(ops, chunk_start - self._next_pos)
            self._chunk(ops, chunk, 0, gaps)
            self._next_pos = chunk_start + n
            return ops
        return self._begin(chunk, chunk_start, capture_time, gaps, ring)

    def _begin(
        self,
        chunk: IQ,
        chunk_start: int,
        capture_time: float,
        gaps: Sequence[tuple[int, int]],
        ring: RingReader | None,
    ) -> list[Op]:
        rate = self.spec.rate_hz
        n = len(chunk)
        # Epoch seconds as a float resolve to about 0.24 us; keep to the
        # difference so the start error adds no rounding of its own.
        delta = self.spec.start_ms / 1000 - capture_time
        target = round(delta * rate)
        span = n + sum(lost for _, lost in gaps)
        if target >= span:
            return []  # not due yet
        self.started = True
        self._first_time = capture_time + target / rate
        self._error_ns = round((target / rate - delta) * 1e9)
        ops: list[Op] = []
        if target >= 0:
            off, head = _place(target, gaps, n)
            if head:
                self._zeros(ops, head)
            self.stream_start = chunk_start + off
            self._chunk(ops, chunk, off, gaps)
        else:
            # Older than this chunk: from the ring, which already holds it.
            # Gaps logged before this chunk are not known here, so the
            # placement counts stream samples back from the chunk.
            p0 = chunk_start + target
            oldest = ring.bounds()[0] if ring is not None else chunk_start
            begin = max(p0, min(oldest, chunk_start))
            data = None
            if ring is not None and begin < chunk_start:
                data = ring.read_range(begin, chunk_start)
            if data is None:
                begin = chunk_start
            late = begin - p0
            if late:
                self._late = min(late, self.spec.n_samples)
                self._zeros(ops, late)
            self.stream_start = begin
            if data is not None:
                self._iq(ops, data)
            self._chunk(ops, chunk, 0, gaps)
        self._next_pos = chunk_start + n
        return ops

    def _chunk(self, ops: list[Op], chunk: IQ, start: int, gaps: Sequence[tuple[int, int]]) -> None:
        """Samples ``chunk[start:]`` with zeros for the gaps after ``start``."""
        pos = start
        for off, lost in sorted(gaps):
            if off <= start:
                continue
            self._iq(ops, chunk[pos:off])
            self._zeros(ops, lost)
            pos = off
        self._iq(ops, chunk[pos:])

    def _room(self) -> int:
        return self.spec.n_samples - self.written

    def _iq(self, ops: list[Op], data: IQ) -> None:
        k = min(len(data), self._room())
        if k > 0:
            ops.append(("iq", np.array(data[:k], dtype=np.int32, copy=True)))
            self.written += k
        self._check_done()

    def _zeros(self, ops: list[Op], count: int) -> None:
        k = min(count, self._room())
        if k > 0:
            if self._gaps and self._gaps[-1][0] + self._gaps[-1][1] == self.written:
                self._gaps[-1][1] += k  # contiguous with the previous fill
            else:
                self._gaps.append([self.written, k])
            self._lost += k
            ops.append(("zeros", k))
            self.written += k
        self._check_done()

    def _check_done(self) -> None:
        if self.written >= self.spec.n_samples:
            self.done = True
