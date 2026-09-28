"""The burst isolation stage.

Completed bursts arrive in batches (one per detector evaluation) from the
burst thread. On its own worker thread the stage applies the gate (SNR over
the noise floor at the burst's peak bin, strongest first, at most
ISOLATION_MAX_PER_SEC), isolates each picked burst (processing/isolate.py) and
fans it out: SigMF archive, add-on modules, and (when on) the rtl_433
attribution worker. Every picked burst ends in exactly one state.
Design: docs/superpowers/specs/2026-09-28-burst-isolation-streaming-design.md
"""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.pipeline.attribution import (
    AttributionItem,
    AttributionWorker,
    ReplayFileSink,
    StrongestQueue,
    db_sink,
    find_rtl433,
)
from rfobserver.processing.isolate import iq_to_complex, isolate_burst
from rfobserver.storage.burst_archive import BurstArchive

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable

    from rfobserver.capture.buffer import CircularBuffer
    from rfobserver.config import AppSettings
    from rfobserver.models import BurstFingerprint
    from rfobserver.pipeline.attribution import AttributionResult

logger = logging.getLogger(__name__)

STATES = ("isolated", "iq_expired", "too_long", "queue_full", "error")
_STOP = object()
# Sweep batches each pin their whole capture's IQ bytes (0.5 s at 56 Msps is
# 112 MB), so at most this many may be queued or in flight at once; further
# ones are dropped as queue_full.
MAX_WHOLE_CAPTURE_BATCHES = 2


@dataclass
class BurstCandidate:
    burst: BurstFingerprint
    snr_db: float


class RingSource:
    def __init__(self, ring: CircularBuffer) -> None:
        self._ring = ring

    def read_range(self, start: int, end: int) -> np.ndarray[Any, np.dtype[Any]] | None:
        return self._ring.read_range(start, end)

    def read_all(self) -> np.ndarray[Any, np.dtype[Any]] | None:
        return None


class WholeCaptureSource:
    """The sweep pipeline's per-capture IQ; converted on the stage thread."""

    def __init__(self, iq_bytes: bytes) -> None:
        self._bytes = iq_bytes
        self._iq: np.ndarray[Any, np.dtype[Any]] | None = None

    def read_range(self, start: int, end: int) -> np.ndarray[Any, np.dtype[Any]] | None:
        return None

    def read_all(self) -> np.ndarray[Any, np.dtype[Any]] | None:
        if self._iq is None:
            self._iq = iq_to_complex(np.frombuffer(self._bytes, dtype=np.int32))
        return self._iq


@dataclass
class IsolationBatch:
    candidates: list[BurstCandidate]
    center_freq_hz: float
    sample_rate_hz: float
    source: RingSource | WholeCaptureSource


class IsolationStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}

    def count(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + n

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)


class IsolationStage:
    def __init__(
        self,
        settings: AppSettings,
        *,
        archive: BurstArchive | None,
        module_feed: Callable[[np.ndarray[Any, np.dtype[Any]], int, dict[str, Any]], None] | None,
        attribution_handoff: Callable[[AttributionItem], None] | None,
        refuse_saving: Callable[[], bool] = lambda: False,
        archive_subdir: str | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._s = settings
        self._archive = archive
        self._module_feed = module_feed
        self._handoff = attribution_handoff
        self._refuse_saving = refuse_saving
        self._archive_subdir = archive_subdir
        self._clock = clock
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=max(1, settings.ISOLATION_QUEUE_MAX))
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        # Serializes submit()'s stop-event check + put_nowait + counting
        # against stop() setting the event, so a submit that is already
        # mid-flight when stop() is called is guaranteed to either finish
        # its put before the event is set (and so be seen by stop()'s
        # drain) or see the event already set and refuse. Held only very
        # briefly in both places -- no blocking calls under it.
        self._submit_lock = threading.Lock()
        # WholeCaptureSource batches queued or in flight (under _submit_lock).
        self._whole_pending = 0
        self._window_start = -1e18
        self._window_count = 0
        self.stats = IsolationStats()

    # -- producer side (burst thread / sweep loop): never blocks --

    def submit(self, batch: IsolationBatch) -> bool:
        n = len(batch.candidates)
        with self._submit_lock:
            self.stats.count("received", n)
            if self._stop_event.is_set():
                self.stats.count("queue_full", n)
                return False
            whole = isinstance(batch.source, WholeCaptureSource)
            if whole and self._whole_pending >= MAX_WHOLE_CAPTURE_BATCHES:
                self.stats.count("queue_full", n)
                return False
            try:
                self._queue.put_nowait(batch)
            except queue.Full:
                self.stats.count("queue_full", n)
                return False
            if whole:
                self._whole_pending += 1
            return True

    def _release(self, batch: IsolationBatch) -> None:
        """A batch has been processed or drained: free its capture slot."""
        if isinstance(batch.source, WholeCaptureSource):
            with self._submit_lock:
                self._whole_pending -= 1

    # -- worker --

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            logger.warning("Isolation stage start() called while the worker thread is running")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name="isolation", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        # Only the event flip is serialized against submit(): if a submit is
        # currently mid-flight (holding _submit_lock, e.g. blocked inside
        # put_nowait), this waits for it to finish first, so that submit
        # either completes its put before the event is set below (and is
        # therefore in the queue for the drain that follows) or observes the
        # event already set and refuses. The lock is released immediately
        # after, before draining or joining, per the "no blocking calls
        # under the lock" rule.
        with self._submit_lock:
            self._stop_event.set()
        # Drain whatever is already queued before the worker even learns to
        # stop: it will never be processed now, so count it as queue_full
        # rather than silently dropping it.
        self._drain_remaining_as_queue_full()
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(_STOP)
        self._thread.join(timeout=5.0)
        if self._thread.is_alive():
            logger.warning("Isolation worker thread did not stop within the timeout")
            return
        self._thread = None
        # If the worker was busy inside process_batch when the event was
        # set, it exits the loop (and drains behind itself, see _loop())
        # only after that batch finishes -- meanwhile a batch submitted in
        # that window could have slipped past submit()'s stop-event check
        # and landed in the queue after our drain above but is still
        # accounted for by _loop()'s own exit-time drain. This final pass
        # is belt-and-suspenders for anything left after the join; empty is
        # the normal case.
        self._drain_remaining_as_queue_full()

    def wait_idle(self, timeout: float) -> bool:
        """Wait until every accepted batch has been processed (or drained),
        queue.join style. True once idle or once the stage is stopping (its
        drain settles whatever is left); False when ``timeout`` passes first.
        Only lossless replay calls this: live producers never wait."""
        q = self._queue
        deadline = time.monotonic() + timeout
        with q.all_tasks_done:
            while q.unfinished_tasks:
                if self._stop_event.is_set():
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                # Short slices so a stop is noticed without a notify.
                q.all_tasks_done.wait(min(remaining, 0.05))
        return True

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            batch = self._queue.get()
            if batch is _STOP:
                self._queue.task_done()
                break
            try:
                self.process_batch(batch)
            except Exception:
                logger.exception("Isolation batch failed")
            finally:
                self._release(batch)
                self._queue.task_done()
        # Whether we got here via the _STOP sentinel or via the stop event
        # flipping between batches, anything still queued behind us was
        # accepted by submit() but will never be processed now.
        self._drain_remaining_as_queue_full()

    def _drain_remaining_as_queue_full(self) -> None:
        while True:
            try:
                batch = self._queue.get_nowait()
            except queue.Empty:
                return
            self._queue.task_done()
            if batch is _STOP:
                continue
            self._release(batch)
            self.stats.count("queue_full", len(batch.candidates))

    def _gate(self, cands: list[BurstCandidate], sample_rate_hz: float) -> list[BurstCandidate]:
        """SNR gate, then cap to ISOLATION_MAX_PER_SEC, strongest first.

        The per-second budget is a fixed 1 s window measured from the first
        candidate seen after the previous window expired (not aligned to
        second boundaries). When the bursts carry stream positions the clock
        is stream time (the newest stop_sample / sample rate), so an offline
        or sped-up replay picks the same bursts on any host; otherwise (sweep
        pipeline) it is the monotonic clock. A clock that goes backwards (ring
        rebuild, replay loop) starts a new window.
        """
        s = self._s
        passed = sorted(
            (c for c in cands if c.snr_db >= s.ISOLATION_SNR_DB),
            key=lambda c: c.snr_db,
            reverse=True,
        )
        stops = [c.burst.stop_sample for c in cands if c.burst.stop_sample is not None]
        now = max(stops) / float(sample_rate_hz) if stops and sample_rate_hz > 0 else self._clock()
        if now < self._window_start or now - self._window_start >= 1.0:
            self._window_start, self._window_count = now, 0
        room = max(0, int(s.ISOLATION_MAX_PER_SEC) - self._window_count)
        picked = passed[:room]
        self._window_count += len(picked)
        if picked:
            self.stats.count("picked", len(picked))
        gated_out = len(cands) - len(picked)
        if gated_out:
            self.stats.count("gated_out", gated_out)
        return picked

    def process_batch(self, batch: IsolationBatch) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for cand in self._gate(batch.candidates, batch.sample_rate_hz):
            state = self._one(cand, batch)
            self.stats.count(state)
            out.append((cand.burst.burst_id, state))
        return out

    def _one(self, cand: BurstCandidate, batch: IsolationBatch) -> str:
        b = cand.burst
        try:
            iso = isolate_burst(
                b,
                read_range=batch.source.read_range,
                read_all=batch.source.read_all,
                sample_rate_hz=batch.sample_rate_hz,
                center_freq_hz=batch.center_freq_hz,
                max_burst_sec=float(self._s.ISOLATION_MAX_BURST_SEC),
            )
        except Exception:
            logger.exception("Isolation failed for burst %s", b.burst_id)
            return "error"
        if isinstance(iso, str):
            return iso
        meta: dict[str, Any] = {
            "burst_id": b.burst_id,
            "freq_hz": iso.freq_hz,
            "freq_low_hz": b.center_freq_hz - b.bandwidth_hz / 2,
            "freq_high_hz": b.center_freq_hz + b.bandwidth_hz / 2,
            "start_time_ms": b.start_time.timestamp() * 1000.0,
            "stop_time_ms": b.stop_time.timestamp() * 1000.0,
            "snr_db": round(cand.snr_db, 1),
        }
        # Each fan-out consumer gets its own try/except: one consumer's
        # failure must not hide the burst from the others, and (unlike a
        # failure in isolate_burst itself) must not change the burst's
        # state -- the burst *was* isolated, only its delivery to one
        # consumer failed.
        if self._archive is not None and not self._refuse_saving():
            try:
                self._archive.save(
                    iso,
                    {
                        "rfobs:snr_db": meta["snr_db"],
                        "rfobs:peak_power_db": b.peak_power_db,
                        "rfobs:bandwidth_hz": b.bandwidth_hz,
                        "core:datetime": b.start_time.isoformat().replace("+00:00", "Z"),
                    },
                    subdir=self._archive_subdir,
                )
            except Exception:
                logger.exception("Archive save failed for burst %s", b.burst_id)
                self.stats.count("archive_error")
        if self._module_feed is not None:
            try:
                v = np.frombuffer(iso.cs16, dtype="<i2").astype(np.float32) / 32768.0
                self._module_feed((v[0::2] + 1j * v[1::2]).astype(np.complex64), iso.rate_hz, meta)
            except Exception:
                logger.exception("Module feed failed for burst %s", b.burst_id)
                self.stats.count("module_error")
        if self._handoff is not None:
            try:
                self._handoff(
                    AttributionItem(
                        burst_id=b.burst_id,
                        cs16=iso.cs16,
                        target_rate_hz=iso.rate_hz,
                        passes=iso.passes,
                        power_db=b.peak_power_db,
                        meta=meta,
                    )
                )
            except Exception:
                logger.exception("Attribution handoff failed for burst %s", b.burst_id)
                self.stats.count("handoff_error")
        return "too_long" if iso.truncated else "isolated"


def _make_attribution_handoff(
    q: StrongestQueue, loop: asyncio.AbstractEventLoop
) -> Callable[[AttributionItem], None]:
    """StrongestQueue is asyncio-only: hand over on the loop thread."""

    def handoff(item: AttributionItem) -> None:
        loop.call_soon_threadsafe(q.put_nowait, item)

    return handoff


def _make_label_sink(
    label: Callable[[AttributionResult], None],
) -> Callable[[AttributionResult], Any]:
    async def label_sink(r: AttributionResult) -> None:
        label(r)

    return label_sink


def build_isolation(
    settings: AppSettings,
    *,
    database: Any,
    storage_path: str,
    loop: asyncio.AbstractEventLoop,
    module_feed: Callable[[np.ndarray[Any, np.dtype[Any]], int, dict[str, Any]], None] | None,
    refuse_saving: Callable[[], bool],
    replay_source: str | None,
    on_label: Callable[[AttributionResult], None] | None,
) -> tuple[IsolationStage | None, AttributionWorker | None, str | None]:
    """Build the stage (and the rtl_433 worker when attribution is on).

    Attribution forces isolation on. In replay, burst files go to
    ``bursts/replay-<stem>/`` and results to its ``attribution.jsonl``, never
    the DB. Returns (stage, worker, rtl_status); all None when both are off.
    """
    if not (settings.ISOLATION_ENABLED or settings.ATTRIBUTION_ENABLED):
        return None, None, None
    archive = BurstArchive(storage_path)
    subdir = f"replay-{Path(replay_source).stem}" if replay_source else None

    # The queue and its handoff closure are built first (when attribution is
    # on) so the handoff can be passed straight into the IsolationStage
    # constructor -- no reaching into a private attribute afterward.
    rtl_status: str | None = None
    rtl: str | None = None
    q: StrongestQueue | None = None
    handoff: Callable[[AttributionItem], None] | None = None
    sinks: list[Any] = []
    if settings.ATTRIBUTION_ENABLED:
        rtl = find_rtl433(settings.ATTRIBUTION_RTL433_PATH or None)
        if rtl is None:
            rtl_status = "rtl_433 not found; attribution unavailable"
            logger.warning("ATTRIBUTION_ENABLED but %s", rtl_status)
        else:
            rtl_status = rtl
            if subdir is not None:
                sinks.append(ReplayFileSink(archive.root / subdir / "attribution.jsonl"))
            else:
                sinks.append(db_sink(database))
            if on_label is not None:
                sinks.append(_make_label_sink(on_label))
            q = StrongestQueue(maxsize=max(1, settings.ISOLATION_QUEUE_MAX))
            handoff = _make_attribution_handoff(q, loop)

    stage = IsolationStage(
        settings,
        archive=archive,
        module_feed=module_feed,
        attribution_handoff=handoff,
        refuse_saving=refuse_saving,
        archive_subdir=subdir,
    )

    worker: AttributionWorker | None = None
    if q is not None and rtl is not None:
        worker = AttributionWorker(
            database,
            rtl,
            queue=q,
            sinks=sinks,
            on_outcome=lambda outcome: stage.stats.count(f"attr_{outcome}"),
        )
    return stage, worker, rtl_status
