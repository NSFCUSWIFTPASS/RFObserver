"""Streaming capture -> process -> detect -> store -> publish pipeline.

Replaces the batch ``ContinuousProcessor`` when streaming mode is active.
Five threads coordinate via bounded queues:

* **Receiver thread** – calls ``recv_chunk()`` in a tight loop, feeds the
  pre-trigger circular buffer and power trigger, enqueues SC16 buffers.
* **Dispatch thread** – pulls chunks from the queue, submits them to a
  ``ThreadPoolExecutor`` (3 workers), collects results in order, and feeds
  PSD grids to the burst detection thread.
* **Processing workers** (3) – each runs ``_process_one_chunk`` (SC16→complex,
  PSD, IQ stats).  Pure functions with no shared mutable state.
* **Burst thread** – runs ``RollingBurstDetector`` on PSD grids received in
  sequence order from the dispatcher.
* **Event loop** – broadcasts PSD + stats to WebSocket, stores bursts to
  SQLite, periodically submits to ZMS.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
import math
import os
import queue
import shutil
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.capture.buffer import CircularBuffer, GridPreBuffer, trim_grid_rows
from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.iq_utils import (
    IQMoments,
    convert_sc16_to_complex,
    finalize_moments,
    moments_from_iq,
)
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import (
    PSDGridConfig,
    compute_psd_grid,
    compute_summary_psd,
)
from rfobserver.storage.governor import (
    HARD_FLOOR_FRACTION,
    describe_write_error,
    is_disk_full_error,
    resolve_floor,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from pathlib import Path

    from rfobserver.capture.receiver import IReceiver
    from rfobserver.config import AppSettings
    from rfobserver.models import BurstFingerprint, IQStatistics, ProcessedDataEnvelope, PSDData
    from rfobserver.pipeline.beacon import ProgressBeacon
    from rfobserver.processing.spectral import PSDGridResult
    from rfobserver.storage.database import SensorDatabase
    from rfobserver.storage.governor import StorageGovernor
    from rfobserver.storage.local import LocalStorage
    from rfobserver.transport.nats_producer import NatsProducer
    from rfobserver.web.websocket import LiveBroadcast
    from rfobserver.zms.monitor import ZmsMonitor

logger = logging.getLogger(__name__)

# Sentinel used to signal threads to shut down.
# Must not be None (timeout returns None, causing false shutdown).
_STOP = object()

# A recording's .json lists at most this many gaps; lost_samples and
# overflow_events keep counting past it (gaps_truncated says so).
_MAX_RECORDED_GAPS = 1000
# Receive gaps remembered at stream positions, for mapping into a pre-roll.
# More than this many overflow gaps inside one pre-roll would undercount
# lost_samples (the oldest fall out of the log); that is not realistic, and
# more than _MAX_RECORDED_GAPS mapped gaps already sets gaps_truncated.
_STREAM_GAP_LOG_LEN = 4096

# Bounds on how long finalize waits for the in-flight PSD grids covering the
# tail of a recording. The IQ is written synchronously in the receive loop but
# grids emerge several chunks later, so at stop time the last chunks of IQ have
# no rows yet. The wait ends the moment the grids catch up, which is the normal
# case and costs roughly one pipeline latency.
#
# The bound is deliberately NOT derived from RECORDING_MAX_SEC. How long the
# tail takes to arrive is a property of the pipeline, not of how long the
# recording ran, and scaling the wait to the recording length breaks exactly
# the case that needs it most: on nano-super (3 workers, ~920 ms median
# latency) a RECORDING_MAX_SEC of 0.5 s truncated the .psd 205 ms short of the
# IQ. _FLOOR covers a slow box's latency; _CEILING keeps the wait inside the
# 15 s budget _request_end_recording allows a manual stop, and a longer
# RECORDING_MAX_SEC may raise the floor but never past it.
_GRID_TAIL_DRAIN_FLOOR_SEC = 3.0

# How often the dispatch loop re-checks for finished work while chunks are in
# flight. Finished results are collected at the top of the loop, and the loop
# otherwise blocks in _chunk_queue.get(), which returns early only for a NEW
# chunk: with a long timeout every chunk waited a full chunk period (~205 ms)
# for its successor before its PSD was handed on. A new chunk still wakes the
# get() immediately, so this bounds only the added delay on finished work.
_RESULT_POLL_SEC = 0.005
_GRID_TAIL_DRAIN_CEILING_SEC = 10.0


def _preroll_gaps(
    stream_gaps: Iterable[tuple[int, int]], start: int, end: int, written: int
) -> list[list[int]]:
    """Map stream-position gaps into a pre-roll that covers stream [start, end).

    A gap at stream position s means samples were lost right before s. Only
    gaps strictly inside the pre-roll that was actually written to the file
    (the first ``written`` samples) become file gaps ``[s - start, lost]``;
    one at ``start`` precedes the file.
    """
    return [[s - start, lost] for s, lost in stream_gaps if start < s < end and s - start < written]


class _StreamResult:
    """Container for results produced by the processing workers."""

    __slots__ = (
        "summary_psd",
        "iq_stats",
        "iq_moments",
        "bursts",
        "psd_grid",
        "center_freq_hz",
        "capture_num",
        "process_ms",
        "latency_ms",
    )

    def __init__(
        self,
        summary_psd: PSDData,
        iq_stats: IQStatistics,
        bursts: list[BurstFingerprint],
        psd_grid: PSDGridResult,
        center_freq_hz: int,
        capture_num: int,
        process_ms: float,
        latency_ms: float,
        iq_moments: IQMoments,
    ) -> None:
        self.summary_psd = summary_psd
        self.iq_stats = iq_stats
        self.iq_moments = iq_moments
        self.bursts = bursts
        self.psd_grid = psd_grid
        self.center_freq_hz = center_freq_hz
        self.capture_num = capture_num
        self.process_ms = process_ms
        self.latency_ms = latency_ms


class _ChunkResult:
    """Intermediate result from a processing worker (before burst detection)."""

    __slots__ = (
        "psd_grid",
        "iq_stats",
        "iq_moments",
        "summary_psd",
        "center_freq_hz",
        "capture_num",
        "recv_time",
        "chunk_start",
        "process_ms",
        "sc16_buf",
    )

    def __init__(
        self,
        psd_grid: PSDGridResult,
        iq_stats: IQStatistics,
        summary_psd: PSDData,
        center_freq_hz: int,
        capture_num: int,
        recv_time: float,
        chunk_start: int,
        process_ms: float,
        sc16_buf: np.ndarray[Any, np.dtype[Any]],
        iq_moments: IQMoments,
    ) -> None:
        self.psd_grid = psd_grid
        self.iq_stats = iq_stats
        self.iq_moments = iq_moments
        self.summary_psd = summary_psd
        self.center_freq_hz = center_freq_hz
        self.capture_num = capture_num
        self.recv_time = recv_time
        self.chunk_start = chunk_start
        self.process_ms = process_ms
        self.sc16_buf = sc16_buf


def _put_nowait_drop_full(q: asyncio.Queue[Any], item: Any) -> None:
    """Loop-thread callback: put if there's room, drop on overflow."""
    with contextlib.suppress(asyncio.QueueFull):
        q.put_nowait(item)


class _LoopHandoff:
    """Thread-to-loop handoff into an asyncio.Queue that stays bounded.

    call_soon_threadsafe alone queues one loop callback per item; while the
    loop is blocked those callbacks, and the results they hold, pile up without
    limit because the queue's own bound only applies once a callback runs
    (measured: RSS +30 MB/s during a 60 s loop wedge on nano-super). This caps
    callbacks in flight at the queue's maxsize and drops at the producer
    beyond that, which is the same drop-on-overflow outcome as before.
    """

    def __init__(self, q: asyncio.Queue[Any]) -> None:
        if q.maxsize <= 0:
            raise ValueError("_LoopHandoff needs a bounded queue")
        self._q = q
        self._limit = q.maxsize
        self._pending = 0
        self._lock = threading.Lock()
        self.dropped = 0

    def submit(self, loop: asyncio.AbstractEventLoop, item: Any) -> bool:
        with self._lock:
            if self._pending >= self._limit:
                self.dropped += 1
                return False
            self._pending += 1
        try:
            loop.call_soon_threadsafe(self._deliver, item)
        except RuntimeError:  # loop closed during shutdown
            with self._lock:
                self._pending -= 1
            return False
        return True

    def _deliver(self, item: Any) -> None:
        with self._lock:
            self._pending -= 1
        _put_nowait_drop_full(self._q, item)


def _signal_stop(q: queue.Queue[Any]) -> None:
    """Enqueue the ``_STOP`` sentinel without ever blocking.

    On shutdown ``_running`` is already False, so the consumer thread has
    stopped draining ``q``. In lossless mode a producer can leave ``q`` full,
    which would make a plain blocking ``put(_STOP)`` wedge forever. Drop pending
    items to make room, then signal — and freeing a slot also unblocks any
    producer parked in a blocking ``put`` so it can observe ``_running`` and
    exit.
    """
    while True:
        try:
            q.put_nowait(_STOP)
            return
        except queue.Full:
            try:
                q.get_nowait()
            except queue.Empty:
                return


def _mem_available_bytes() -> int | None:
    """Available RAM in bytes from /proc/meminfo, or None if unreadable."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _effective_max_recording_sec(settings: Any, mem_available_bytes: int | None) -> float:
    """Auto-stop duration for a recording.

    Disk mode streams IQ+grids to disk, so RAM is not the limit — only the
    configured RECORDING_MAX_SEC (or unlimited). RAM mode holds IQ+grids in RAM,
    so cap the duration to a fraction of available RAM. Returns math.inf for
    "no limit". Falls back to 30 s (or the configured max) if RAM is unknown.
    """
    configured = float(settings.RECORDING_MAX_SEC) if settings.RECORDING_MAX_SEC > 0 else math.inf
    if not settings.RECORDING_RAM_BUFFER:
        return configured
    if mem_available_bytes is None:
        return min(configured, 30.0)
    grid_bps = (1000.0 / settings.PSD_TIME_RESOLUTION_MS) * settings.NUM_FFT_BINS * 4
    iq_bps = settings.BANDWIDTH * 4
    ram_bps = grid_bps + iq_bps
    ram_max = (mem_available_bytes * settings.RECORDING_MEM_FRACTION) / ram_bps
    return min(configured, float(ram_max))


class StreamingProcessor:
    """Streaming pipeline: continuous recv → parallel PSD → rolling burst detection."""

    def __init__(
        self,
        receiver: IReceiver,
        database: SensorDatabase,
        local_storage: LocalStorage,
        settings: AppSettings,
        broadcast: LiveBroadcast | None = None,
        zms_monitor: ZmsMonitor | None = None,
        nats_producer: NatsProducer | None = None,
        drop_on_overflow: bool = True,
        replay_mode: bool = False,
        beacon: ProgressBeacon | None = None,
        storage_governor: StorageGovernor | None = None,
    ) -> None:
        self._receiver = receiver
        self._db = database
        self._storage = local_storage
        self._settings = settings
        self._broadcast = broadcast
        self._zms_monitor = zms_monitor
        self._nats_producer = nats_producer
        self._beacon = beacon
        self._governor = storage_governor
        self._running = False
        # Live capture must never block the receiver thread, so chunks are
        # dropped when processing falls behind (the default). Offline replay of
        # a bounded buffer (e.g. integration tests, file playback) can opt into
        # lossless mode, where the producer and dispatch loop instead block
        # until there is room — every chunk is processed, deterministically.
        self._drop_on_overflow = drop_on_overflow
        # When True, this processor drives only the live WS overlay from a
        # replayed capture: every persistence/egress side effect (DB insert,
        # ZMS submit, NATS publish, IQ recording) is suppressed so replay
        # never pollutes the DB or emits upstream. The live _broadcast path
        # is untouched by this flag.
        self._replay_mode = replay_mode
        # Opt-in: even under replay_mode, an explicit manual record can be
        # allowed to write a real .sc16 (the replayed capture becomes a real
        # capture on disk). Off by default; set via set_replay_recording().
        self._replay_record = False

        total_cores = os.cpu_count() or 4
        self._num_proc_workers = max(1, total_cores - 3)
        self._fft_workers = 1

        # Guards the stream gap log (and its swap with the pre-trigger ring in
        # _recompute_chunk_params): the receiver thread appends, the recording
        # fire site (receiver thread or a web worker) snapshots it.
        self._stream_gaps_lock = threading.Lock()

        # Compute chunk sizing from current settings
        self._recompute_chunk_params()

        # Inter-thread queues (these survive reconfiguration)
        self._chunk_queue: queue.Queue[Any] = queue.Queue(maxsize=4)
        self._burst_queue: queue.Queue[Any] = queue.Queue(maxsize=16)
        self._dropped_chunks = 0
        self._result_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=8)
        self._result_handoff = _LoopHandoff(self._result_queue)
        self._loop: asyncio.AbstractEventLoop | None = None

        # Recording state machine: "idle" | "armed" | "recording" | "finalizing".
        # "finalizing" bridges an auto-stop and the (off-thread) finalize job so
        # continuous mode cannot re-arm — and a new begin cannot clobber the
        # fields the finalize job is still reading — while it runs.
        self._recording_state: str = "idle"
        # Serializes begin/end decisions (receiver thread vs web workers).
        self._rec_lock = threading.Lock()
        self._recording_file: str | None = None
        self._recording_bytes: int = 0
        self._recording_start: float = 0.0
        self._recording_dropped: int = 0
        # Gaps in the current recording: [file_sample_index, lost_samples] (see
        # _write_recording_metadata), their total, and the UHD overflows among them.
        self._recording_gaps: list[list[int]] = []
        self._recording_lost = 0
        self._recording_overflows = 0
        self._recording_gaps_truncated = False
        # Why the current (or last) recording ended; written to the .json.
        self._stop_reason: str | None = None
        # Set by the writer thread (or the RAM flush) on a failed write; the
        # receiver thread ends the recording on seeing it.
        self._writer_error: str | None = None
        # Set by the writer's own disk check when free space drops below half
        # the floor mid-recording (the governor's 10 s tick is too slow).
        self._disk_floor_hit = False
        self._disk_usage: Callable[[Any], Any] = shutil.disk_usage
        # The last refusal logged, so a refusal that persists is logged once.
        # The API and UI show the current one (_recording_refusal).
        self._last_refusal: str | None = None
        # After a recording ends for disk_floor or write_error: (governor tick
        # count at that moment, reason). New starts are held until the governor
        # completes a tick that began after the stop, so continuous triggering cannot start and
        # abort capture after capture before the next storage check.
        self._storage_hold: tuple[int, str] | None = None
        # Stream-position continuity of the current recording (see
        # _write_recording_chunk): the pre-trigger ring its positions refer to,
        # and the stream position just after the file's last sample (None
        # until the file has a sample to continue from).
        self._recording_ring: CircularBuffer | None = None
        self._recording_next_pos: int | None = None
        # Absolute stream sample bounds of the current recording, and how far
        # the .psd companion has been filled within them. The PSD path lags the
        # IQ by the pipeline latency, so grid rows are placed against these
        # bounds rather than appended in arrival order.
        self._recording_start_sample: int | None = None
        self._recording_end_sample: int | None = None
        self._grid_first_sample: int | None = None
        self._grid_last_sample: int = 0
        self._grid_accepting: bool = False
        # Wall time of the file's first sample, taken at the pre-roll read.
        self._recording_wall_start: float | None = None
        self._trigger_initiated: bool = False  # True if trigger fired (vs manual)
        self._below_threshold_count = 0
        # True when the current "armed" state came from continuous auto-arm (vs a
        # manual arm_trigger). Lets a continuous-off toggle release an auto-armed
        # wait without disarming a deliberately manual arm.
        self._continuous_armed: bool = False

        # Disk-streaming write queue (default mode). Items are tagged:
        # ("iq", bytes-like) for the .sc16 stream, ("grid", bytes) for the
        # streamed .psd rows, None to stop the writer.
        self._recording_queue: queue.Queue[Any] = queue.Queue(maxsize=64)
        self._writer_thread: threading.Thread | None = None

        # Recording-control thread: runs the blocking finalization work
        # (writer drain, file close, metadata, disk-cap eviction) so the
        # receiver and dispatch threads never stall on disk I/O — that stall
        # showed up as black gaps in the PSD history at every capture stop.
        # When the pipeline isn't running (unit tests), _schedule_recctl
        # executes jobs inline on the caller instead.
        self._recctl_queue: queue.Queue[Any] = queue.Queue()
        self._recctl_thread: threading.Thread | None = None
        self._end_done = threading.Event()
        self._end_done.set()

        # RAM-buffered recording (when RECORDING_RAM_BUFFER=True)
        self._recording_buf: np.ndarray[Any, np.dtype[Any]] | None = None
        self._recording_buf_pos: int = 0

        # PSD grid accumulation during recording (for .npz companion file)
        self._recording_grids: list[np.ndarray[Any, np.dtype[Any]]] = []
        self._recording_freq_axis: np.ndarray[Any, np.dtype[Any]] | None = None
        self._recording_time_res: float = 0.0
        # Display calibration snapshotted when recording begins, baked into the
        # .npz so the capture viewer can show the same dBm/Hz the live view did.
        self._recording_cal_offset: float | None = None
        # New raw-grid streaming state. Disk mode hands rows to the writer
        # thread via the tagged queue (bounded RAM); RAM mode still uses
        # _recording_grids but is bounded by the RAM-derived _effective_max_sec
        # auto-stop.
        self._grid_raw_path: Any = None
        self._grid_rows: int = 0
        self._grid_dropped: int = 0  # rows dropped when the writer queue was full
        self._grid_min: float = float("inf")
        self._grid_max: float = float("-inf")
        self._effective_max_sec: float = float("inf")
        # Directory the current recording writes into: auto/ for triggered +
        # continuous captures (FIFO-evicted), manual/ for manual + replay records
        # (never evicted). Chosen per-recording in _begin_recording.
        self._recording_dir = local_storage.manual_dir

        self._capture_count = 0

        # Active bursts for WebSocket overlay (written by burst thread, read by consumer)
        self._active_bursts: list[dict[str, object]] = []
        # Per-bin noise floor that the rolling detector currently sees, in dB.
        # Updated by the burst thread after each feed(); read by the consumer
        # for the broadcast payload so the UI can draw the *actual* threshold
        # the detector uses (a per-bin curve, not a horizontal line).
        self._noise_floor_per_bin: list[float] | None = None

        # Burst results from burst thread -> event loop. Payload is
        # (completed_bursts, sdr_center_freq_hz) so each detection can be
        # stamped with the actual tuned center, correct even mid-sweep.
        self._burst_result_queue: asyncio.Queue[tuple[list[BurstFingerprint], int] | None] = (
            asyncio.Queue(maxsize=32)
        )
        self._burst_handoff = _LoopHandoff(self._burst_result_queue)

        # Module manager — attached externally by pipeline/app.py (optional)
        self._module_manager: Any = None

        # Reconfiguration generation counter — each thread tracks its own
        # last-seen generation and reconfigures when it changes.
        self._config_generation = 0

    def _recompute_chunk_params(self) -> None:
        """(Re)compute chunk sizing, buffer pool, and pre-trigger buffer from settings."""
        s = self._settings

        nperseg = s.NUM_FFT_BINS
        overlap = 0.5
        hop = int(nperseg * (1 - overlap))
        slice_samples = int(s.BANDWIDTH * s.PSD_TIME_RESOLUTION_MS / 1000.0)
        if slice_samples < nperseg:
            slice_samples = nperseg
        ffts_per_slice = max(1, (slice_samples - nperseg) // hop + 1)
        actual_slice_samples = nperseg + (ffts_per_slice - 1) * hop

        chunk_slices = s.STREAMING_CHUNK_SLICES
        self._chunk_samples = chunk_slices * actual_slice_samples
        self._chunk_duration = self._chunk_samples / s.BANDWIDTH
        # Samples per PSD grid row: the unit mapping a grid row index to an
        # absolute stream sample position, so recorded rows can be placed by
        # position rather than by arrival order.
        self._slice_samples = actual_slice_samples

        # Buffer pool: 12 pre-allocated SC16 (int32) buffers.
        # Fill a local pool first, then swap — otherwise concurrent put_nowait
        # from in-flight dispatch results can race the fill and deadlock the
        # receiver thread on its own put().
        new_pool: queue.Queue[np.ndarray[Any, np.dtype[Any]]] = queue.Queue(maxsize=12)
        for _ in range(12):
            new_pool.put_nowait(np.zeros(self._chunk_samples, dtype=np.int32))
        self._buf_pool = new_pool

        # Pre-trigger circular buffer (int32 = SC16). Its total_written is the
        # stream position; a new ring restarts positions at 0, so the gap log
        # logged against them restarts with it. Swapped together under the lock
        # so a recording start never pairs one ring with the other's gaps.
        pre_trigger_samples = int(s.TRIGGER_PRE_SEC * s.BANDWIDTH)
        with self._stream_gaps_lock:
            self._pre_trigger_buf = CircularBuffer(max(1, pre_trigger_samples), dtype=np.int32)
            self._stream_gaps: collections.deque[tuple[int, int]] = collections.deque(
                maxlen=_STREAM_GAP_LOG_LEN
            )

        # Parallel pre-trigger PSD-grid buffer: the same TRIGGER_PRE_SEC window
        # of already-computed grids, so a recording's .psd covers the pre-roll
        # IQ. Recreated here (not cleared) so a reconfigured bin count can never
        # mix grid widths.
        self._grid_prebuf = GridPreBuffer(s.TRIGGER_PRE_SEC)

        logger.info(
            "StreamingProcessor: chunk=%d samples (%.1f ms), %d PSD workers "
            "(fft_workers=%d each), pre-trigger=%.2fs (%d samples)",
            self._chunk_samples,
            self._chunk_duration * 1000,
            self._num_proc_workers,
            self._fft_workers,
            s.TRIGGER_PRE_SEC,
            pre_trigger_samples,
        )

    # -- Public API --

    async def run(self) -> None:
        """Start the streaming pipeline."""
        self._running = True
        self._loop = asyncio.get_running_loop()
        if self._beacon is not None:
            self._beacon.mark()

        recv_thread = threading.Thread(target=self._receiver_loop, name="recv", daemon=True)
        dispatch_thread = threading.Thread(target=self._dispatch_loop, name="dispatch", daemon=True)
        burst_thread = threading.Thread(
            target=self._burst_detection_loop, name="burst", daemon=True
        )
        recctl_thread = threading.Thread(target=self._recctl_loop, name="recctl", daemon=True)
        self._recctl_thread = recctl_thread

        recv_thread.start()
        dispatch_thread.start()
        burst_thread.start()
        recctl_thread.start()

        try:
            await self._result_consumer_loop()
        finally:
            self._running = False
            # Stop any active recording, waiting for the finalize job so the
            # capture files are properly closed before threads exit.
            if self._recording_state == "recording":
                self._request_end_recording(wait=True, reason="shutdown")
            elif self._recording_state == "finalizing":
                self._end_done.wait(timeout=15)
            self._recording_state = "idle"
            # Unblock threads (drain-safe: a full queue in lossless mode must not
            # wedge shutdown now that the consumers have stopped).
            _signal_stop(self._chunk_queue)
            _signal_stop(self._burst_queue)
            recv_thread.join(timeout=5)
            dispatch_thread.join(timeout=5)
            burst_thread.join(timeout=5)
            self._recctl_queue.put(None)
            recctl_thread.join(timeout=5)
            self._recctl_thread = None
            # Final drain: the burst thread may have enqueued completed bursts
            # (via call_soon_threadsafe) after the consumer loop's last drain --
            # i.e. a burst finishing right at shutdown. Let those scheduled
            # enqueues run, then persist them so they aren't silently dropped.
            try:
                await asyncio.sleep(0)
                await self._drain_burst_results()
            except Exception:
                logger.exception("Final burst-result drain failed during shutdown")

    def stop(self) -> None:
        self._running = False

    def reconfigure(self) -> None:
        """Signal all threads to pick up changed settings.

        Called by the config route after updating ``AppSettings`` in place.
        The receiver thread will stop streaming, recompute chunk params,
        rebuild buffers, and resume.  The dispatch and burst threads will
        rebuild their PSD / burst configs on the next iteration.
        """
        self._config_generation += 1
        logger.info("Reconfiguration requested (gen=%d)", self._config_generation)

    def set_replay_recording(self, on: bool) -> None:
        """Opt in/out of recording during replay (manual record only)."""
        self._replay_record = bool(on)

    def _recording_refusal(self) -> str | None:
        """Why a recording may not start now, or None. Storage step >= 3:
        free space is below the floor and nothing RFObserver can delete
        would raise it. Also held after a disk_floor/write_error stop until
        the governor's next tick."""
        g = self._governor
        if g is None:
            return None
        st = g.state
        if st.refuse_recording:
            h = st.to_health()
            return (
                f"Recording refused: free space {h['free_gb']} GB is below the "
                f"{h['floor_gb']} GB floor (storage step {st.step}, {h['step_text']})"
            )
        # Held until the second tick to complete after the stop: ticks run one
        # at a time from one loop, so the first may have sampled before the
        # stop, but the second began after it.
        hold = self._storage_hold
        if hold is not None and g.ticks <= hold[0] + 1:
            return (
                f"Recording held: the last capture stopped for {hold[1]}; "
                "waiting for the next storage check"
            )
        return None

    def _note_refusal(self, reason: str) -> None:
        if reason != self._last_refusal:
            logger.warning(reason)
        self._last_refusal = reason

    def _refused(self) -> bool:
        """True (and noted) when storage refuses a recording right now;
        otherwise clears any stale refusal and returns False."""
        reason = self._recording_refusal()
        if reason is not None:
            self._note_refusal(reason)
            return True
        self._last_refusal = None
        return False

    def start_recording(self) -> None:
        """Start recording IQ data immediately (manual mode)."""
        if self._replay_mode and not self._replay_record:
            return
        if self._refused():
            return
        with self._rec_lock:
            # "finalizing" means a finalize job still reads the recording
            # fields; beginning now would clobber them.
            if self._recording_state in ("recording", "finalizing"):
                return
            self._trigger_initiated = False
            self._begin_recording()

    def arm_trigger(self) -> None:
        """Arm the power trigger — recording starts when threshold is exceeded."""
        if self._replay_mode:
            return
        if self._refused():
            return
        with self._rec_lock:
            if self._recording_state in ("recording", "finalizing"):
                return
            self._recording_state = "armed"
            self._continuous_armed = False  # a deliberate manual arm, not continuous
            logger.info("Trigger armed (threshold=%.1f dB)", self._settings.TRIGGER_THRESHOLD_DB)

    def stop_recording(self) -> None:
        """Stop recording or disarm trigger, return to idle.

        Finalization runs on the recording-control thread; a manual stop waits
        for it so the call keeps its synchronous API semantics (the web route
        already wraps this in asyncio.to_thread).
        """
        if self._recording_state == "recording":
            self._request_end_recording(wait=True, reason="manual")
        elif self._recording_state == "finalizing":
            self._end_done.wait(timeout=15)
        self._recording_state = "idle"
        self._trigger_initiated = False
        logger.info("Recording stopped / trigger disarmed")

    def _schedule_recctl(self, fn: Callable[[], None]) -> None:
        """Run ``fn`` on the recording-control thread; inline when the pipeline
        isn't running (unit tests drive begin/end directly)."""
        t = self._recctl_thread
        if t is not None and t.is_alive():
            self._recctl_queue.put(fn)
        else:
            fn()

    def _recctl_loop(self) -> None:
        """Recording-control thread: serializes finalize jobs off the hot path."""
        while True:
            fn = self._recctl_queue.get()
            if fn is None:
                return
            try:
                fn()
            except Exception:
                logger.exception("Recording-control job failed")

    def _request_end_recording(self, wait: bool, reason: str = "manual") -> None:
        """Flip recording -> finalizing and hand finalization to the control
        thread. ``wait`` (manual stop, shutdown) blocks until the job finishes;
        the receiver thread always passes wait=False and never stalls."""
        with self._rec_lock:
            if self._recording_state != "recording":
                return
            self._stop_reason = reason
            # Stops chunk writes (_check_trigger_and_record), which gate on the
            # exact "recording" state. Grid appends deliberately continue: they
            # gate on _grid_accepting so the rows covering the tail of the IQ,
            # still in the worker pool at this point, can land before the file
            # is closed.
            # Freeze the IQ end position before the flip: grid rows are
            # trimmed against it, and grids still in flight keep arriving
            # after this point (that is the tail _await_tail_grids waits for).
            self._recording_end_sample = self._recording_next_pos
            self._recording_state = "finalizing"
            self._end_done.clear()
        self._schedule_recctl(self._end_recording)
        if wait:
            self._end_done.wait(timeout=15)

    def recording_status(self) -> dict[str, object]:
        """Return current recording state for the API."""
        duration = 0.0
        if self._recording_state == "recording" and self._recording_start > 0:
            duration = time.monotonic() - self._recording_start
        return {
            "state": self._recording_state,
            "file": self._recording_file,
            "bytes": self._recording_bytes,
            "duration_sec": round(duration, 1),
            "dropped_chunks": self._recording_dropped,
            # The refusal in force now (not the last one logged), so the UI
            # notice clears as soon as storage recovers.
            "refused": self._recording_refusal(),
        }

    def receive_loss(self) -> dict[str, int]:
        """Cumulative UHD overflow loss since this receiver was built."""
        return {
            "overflow_events": int(getattr(self._receiver, "overflow_events", 0)),
            "overflow_lost_samples": int(getattr(self._receiver, "overflow_lost_samples", 0)),
        }

    # Backward-compat aliases for existing /api/trigger endpoints
    def manual_trigger(self) -> None:
        self.start_recording()

    def stop_trigger(self) -> None:
        self.stop_recording()

    # -- Receiver thread --

    def _receiver_loop(self) -> None:
        """Runs on a dedicated thread.  Calls recv_chunk() and feeds queues.

        Streaming state is kept across dwell iterations: we only call
        ``start_streaming`` / ``stop_streaming`` when the target frequency
        actually changes (or on reconfig / shutdown). In single-frequency
        mode the radio streams continuously instead of cycling stop→start
        every ``DURATION_SEC``.
        """
        s = self._settings
        recv_count = 0
        my_gen = self._config_generation
        current_streaming_freq: int | None = None

        def _ensure_streaming(target_freq: int) -> None:
            nonlocal current_streaming_freq
            if current_streaming_freq == target_freq:
                return
            if current_streaming_freq is not None:
                self._receiver.stop_streaming()
            self._receiver.start_streaming(target_freq)
            current_streaming_freq = target_freq

        def _ensure_stopped() -> None:
            nonlocal current_streaming_freq
            if current_streaming_freq is not None:
                self._receiver.stop_streaming()
                current_streaming_freq = None

        try:
            while self._running:
                # Reconfig tears down and re-initializes the streamer, so we
                # must stop_streaming() first to issue stop_cont before the
                # streamer object is replaced.
                if self._config_generation != my_gen:
                    _ensure_stopped()
                    my_gen = self._config_generation
                    self._reconfigure_receiver()
                    self._recompute_chunk_params()
                    logger.info("Receiver loop reconfigured")

                freqs = self._build_frequency_list()
                logger.debug(
                    "Receiver loop: sweep %d freqs, running=%s",
                    len(freqs),
                    self._running,
                )

                for center_freq in freqs:
                    if not self._running:
                        logger.info("Receiver loop: stopping (running=False)")
                        return
                    if self._config_generation != my_gen:
                        logger.info("Receiver loop: breaking for reconfig")
                        break

                    # Only retune+restart when the frequency actually changes.
                    # Single-freq deployments stay continuously streaming.
                    _ensure_streaming(center_freq)

                    # Dwell at this frequency for DURATION_SEC
                    chunks_per_dwell = max(1, int(s.DURATION_SEC / self._chunk_duration))

                    for _ in range(chunks_per_dwell):
                        if not self._running or self._config_generation != my_gen:
                            break

                        # Get a buffer — never block the receiver thread
                        try:
                            buf = self._buf_pool.get_nowait()
                        except queue.Empty:
                            buf = np.zeros(self._chunk_samples, dtype=np.int32)

                        recv_time = time.monotonic()
                        n = self._receiver.recv_chunk(buf)
                        t_recv_done = time.monotonic()

                        if n < len(buf):
                            logger.warning("recv_chunk short: %d/%d samples", n, len(buf))

                        # Log this chunk's receive gaps at stream positions
                        # BEFORE the ring write, so a pre-roll read that
                        # includes the chunk also sees its gaps. chunk_start
                        # also lets the recording check its own continuity.
                        chunk_gaps = [g for g in self._receiver.last_gaps if g[0] < n]
                        chunk_start = self._pre_trigger_buf.total_written
                        if chunk_gaps:
                            with self._stream_gaps_lock:
                                for off, lost in chunk_gaps:
                                    self._stream_gaps.append((chunk_start + off, lost))

                        # Store raw SC16 in pre-trigger buffer
                        self._pre_trigger_buf.write(buf[:n])

                        # Feed upstream modules (GPU processing, non-blocking)
                        if self._module_manager is not None:
                            self._module_manager.feed_all(buf[:n], center_freq, s.BANDWIDTH)

                        # Handle recording / trigger
                        self._check_trigger_and_record(buf[:n], chunk_gaps, chunk_start)

                        # Enqueue for processing — best-effort, drop if behind.
                        # In lossless mode block until the dispatch loop drains a
                        # slot so no chunk is ever dropped, but poll ``_running``
                        # on a short timeout so shutdown can't wedge the thread on
                        # a full queue.
                        if self._drop_on_overflow:
                            try:
                                self._chunk_queue.put_nowait((buf, recv_time, chunk_start))
                            except queue.Full:
                                self._dropped_chunks += 1
                                with contextlib.suppress(queue.Full):
                                    self._buf_pool.put_nowait(buf)
                        else:
                            while self._running:
                                try:
                                    self._chunk_queue.put(
                                        (buf, recv_time, chunk_start), timeout=0.1
                                    )
                                    break
                                except queue.Full:
                                    continue
                            else:
                                with contextlib.suppress(queue.Full):
                                    self._buf_pool.put_nowait(buf)

                        recv_count += 1
                        if recv_count % 50 == 0:
                            recv_ms = (t_recv_done - recv_time) * 1000
                            loss = self.receive_loss()
                            logger.info(
                                "TIMING recv#%d: recv=%.1fms dropped=%d (IQ=%.1fms) "
                                "handoff_dropped=%d/%d ovf=%d lost=%d",
                                recv_count,
                                recv_ms,
                                self._dropped_chunks,
                                self._chunk_duration * 1000,
                                self._result_handoff.dropped,
                                self._burst_handoff.dropped,
                                loss["overflow_events"],
                                loss["overflow_lost_samples"],
                            )

        except Exception:
            logger.exception("Receiver loop crashed")
        finally:
            _ensure_stopped()
            logger.info("Receiver loop exiting (running=%s)", self._running)
            _signal_stop(self._chunk_queue)

    def _check_trigger_and_record(
        self,
        sc16_buf: np.ndarray[Any, np.dtype[Any]],
        gaps: Sequence[tuple[int, int]] = (),
        chunk_start: int | None = None,
    ) -> None:
        """Handle recording and trigger logic for each chunk.

        ``gaps`` are the chunk's receive gaps, ``(offset_in_chunk, lost)``;
        ``chunk_start`` is its stream position (see _write_recording_chunk).
        """
        if self._replay_mode and not self._replay_record:
            return
        state = self._recording_state

        if state == "recording":
            if self._writer_error is not None:
                self._request_end_recording(wait=False, reason="write_error")
                return
            if self._disk_floor_hit:
                self._request_end_recording(wait=False, reason="disk_floor")
                return
            self._write_recording_chunk(sc16_buf, gaps, chunk_start)

            # Auto-stop on the effective max duration (RAM-derived cap in RAM mode).
            if (time.monotonic() - self._recording_start) >= self._effective_max_sec:
                self._request_end_recording(wait=False, reason="max_duration")
                return

            # Auto-stop for trigger-initiated recordings when power drops
            if self._trigger_initiated:
                if not self._check_power_above_threshold(sc16_buf):
                    self._below_threshold_count += 1
                    if self._below_threshold_count >= self._settings.TRIGGER_HYSTERESIS:
                        self._request_end_recording(wait=False, reason="trigger_end")
                else:
                    self._below_threshold_count = 0
            return

        continuous = self._settings.TRIGGER_CONTINUOUS
        with self._rec_lock:
            state = self._recording_state
            if state == "recording":
                # A manual start flipped the state while this thread waited for
                # the lock. Its pre-roll read may or may not include this chunk;
                # the continuity check in _write_recording_chunk sorts that out.
                # The auto-stop checks run on the next chunk.
                self._write_recording_chunk(sc16_buf, gaps, chunk_start)
                return
            if state == "idle" and continuous and not self._replay_mode:
                # Continuous trigger auto-arms whenever idle -- on sensor start, when
                # the toggle is switched on, and (since a capture's finalize job
                # ends in the idle state) as the re-arm after each capture. While
                # that job runs the state is "finalizing", so re-arming — and the
                # next capture — waits for finalization to complete.
                self._recording_state = "armed"
                self._continuous_armed = True
                state = "armed"
            elif state == "armed" and self._continuous_armed and not continuous:
                # Toggling continuous off releases an auto-armed waiting state; a
                # manual arm (_continuous_armed False) is left untouched.
                self._recording_state = "idle"
                self._continuous_armed = False
                return

            # If armed, check threshold to start recording
            if state == "armed" and self._check_power_above_threshold(sc16_buf):
                # Stay armed on refusal: once space is back the next crossing fires.
                if self._refused():
                    return
                self._trigger_initiated = True
                self._begin_recording()
                # No explicit write of this chunk: the pre-trigger read inside
                # _begin_recording already includes it (it was ring-buffered
                # before this check), so recording it here too would duplicate it.
                # Its gaps were logged before the ring write, so the pre-roll
                # mapping covers them too.

    def _check_power_above_threshold(self, sc16_buf: np.ndarray[Any, np.dtype[Any]]) -> bool:
        """Fast subsampled power estimate from raw SC16 data.

        Returns power in dB re 50 ohm (|z|^2 / 50) — the same scale as
        ``iq_utils.calculate_iq_statistics`` and the iq2ram reference
        ``compute_mean_power_db``. That makes TRIGGER_THRESHOLD_DB compare
        directly against the power values the dashboard/history UI shows;
        without the /50 the threshold sat ~17 dB above the displayed power
        and the trigger fired while the observed power looked below it.
        """
        raw16 = sc16_buf.view(np.int16).reshape(-1, 2)
        step = max(1, len(raw16) // 4096)
        sub = raw16[::step].astype(np.float32) / 32768.0
        power_sq = sub[:, 0] ** 2 + sub[:, 1] ** 2
        mean_power_db = float(10.0 * np.log10(np.mean(power_sq) / 50.0 + 1e-30))
        return mean_power_db > self._settings.TRIGGER_THRESHOLD_DB

    def _write_recording_chunk(
        self,
        sc16_buf: np.ndarray[Any, np.dtype[Any]],
        gaps: Sequence[tuple[int, int]] = (),
        chunk_start: int | None = None,
    ) -> None:
        """Write a chunk to the active recording (RAM buffer or disk queue).

        ``gaps`` (``(offset_in_chunk, lost)``) land at the chunk's file
        position. A dropped chunk becomes one gap holding its own samples and
        the overflow losses inside it.

        ``chunk_start`` (the pre-trigger ring's ``total_written`` before this
        chunk) is checked against the stream position right after the file's
        last sample, which a manual start on another thread can leave out of
        step with this chunk: samples the file already holds (the start's
        pre-roll read included them) are trimmed, and samples that never
        reached it become one gap.
        """
        n = len(sc16_buf)
        ram_buf = self._recording_buf
        file_pos = self._recording_buf_pos if ram_buf is not None else self._recording_bytes // 4

        if chunk_start is not None:
            next_pos = self._recording_next_pos
            if self._pre_trigger_buf is not self._recording_ring:
                # Reconfigured: the new ring restarts positions at 0, so there
                # is nothing to compare with. Continue from this chunk.
                self._recording_ring = self._pre_trigger_buf
            elif next_pos is not None and chunk_start < next_pos:
                skip = next_pos - chunk_start
                if skip >= n:
                    return  # the whole chunk is already in the file
                sc16_buf = sc16_buf[skip:]
                # Gaps before `skip` are inside the pre-roll and were mapped
                # there; one at `skip` (stream next_pos) was not, so it stays.
                gaps = [(off - skip, lost) for off, lost in gaps if off >= skip]
                chunk_start += skip
                n = len(sc16_buf)
            elif next_pos is not None and chunk_start > next_pos:
                self._add_missing_stretch(file_pos, next_pos, chunk_start)

        if ram_buf is not None:
            # RAM-buffered mode
            end = file_pos + n
            written = end <= len(ram_buf)
            if written:
                ram_buf[file_pos:end] = sc16_buf
                self._recording_buf_pos = end
                self._recording_bytes = end * 4  # int32 = 4 bytes
            else:
                self._drop_recording_chunk(file_pos, n, gaps)
                logger.warning("RAM buffer full: dropped chunk")
        else:
            # Disk-streaming mode: convert to bytes on receiver thread
            # (.tobytes() is a fast C-level copy that plays well with GIL)
            try:
                self._recording_queue.put_nowait(("iq", sc16_buf.tobytes()))
                self._recording_bytes += n * 4
                written = True
            except queue.Full:
                written = False
                self._drop_recording_chunk(file_pos, n, gaps)
                logger.warning("Recording queue full: dropped chunk")
        # A dropped chunk is already a gap: the next chunk continues after it.
        if chunk_start is not None:
            self._recording_next_pos = chunk_start + n
            if self._recording_start_sample is None:
                # No pre-roll was written, so the file starts at this chunk.
                self._recording_start_sample = chunk_start
        if written:
            for off, lost in gaps:
                self._add_recording_gap(file_pos + off, lost, overflow=True)

    def _add_missing_stretch(self, file_pos: int, next_pos: int, chunk_start: int) -> None:
        """Account stream ``[next_pos, chunk_start)``, which never reached the
        file, as one gap at ``file_pos``.

        The overflow gaps logged inside it count as overflow events, but their
        lost samples are inside this single gap, as for a dropped chunk. A gap
        logged at ``chunk_start`` belongs to the next chunk's own gaps.
        """
        with self._stream_gaps_lock:
            logged = [lost for s, lost in self._stream_gaps if next_pos <= s < chunk_start]
        if file_pos > 0:
            self._recording_overflows += len(logged)
        self._add_recording_gap(file_pos, chunk_start - next_pos + sum(logged), overflow=False)

    def _drop_recording_chunk(self, file_pos: int, n: int, gaps: Sequence[tuple[int, int]]) -> None:
        """Account a chunk that never reached the file as one gap at ``file_pos``.

        Its UHD overflow gaps still count as overflow events, but their lost
        samples are inside this single gap, so they are not added twice.
        """
        self._recording_dropped += 1
        if file_pos > 0:
            self._recording_overflows += len(gaps)
        self._add_recording_gap(file_pos, n + sum(lost for _, lost in gaps), overflow=False)

    def _add_recording_gap(self, index: int, lost: int, *, overflow: bool) -> None:
        """Record samples missing from the file right before ``index`` (> 0).

        Gaps at the same index (a missing stretch or dropped chunk and the
        next chunk's own gap, or consecutive drops) merge into one entry.
        """
        if index <= 0 or lost <= 0:
            return
        self._recording_lost += lost
        if overflow:
            self._recording_overflows += 1
        # Indices never decrease, so only the last entry can match. Merging
        # adds no entry, so it never sets gaps_truncated.
        if self._recording_gaps and self._recording_gaps[-1][0] == index:
            self._recording_gaps[-1][1] += lost
        elif len(self._recording_gaps) < _MAX_RECORDED_GAPS:
            self._recording_gaps.append([index, lost])
        else:
            self._recording_gaps_truncated = True

    def _add_preroll_gaps(self, ring: CircularBuffer, start: int, end: int, written: int) -> None:
        """Map the logged receive gaps inside a pre-roll into the recording.

        The pre-roll holds stream ``[start, end)`` of ``ring``; the file got
        its first ``written`` samples. If the ring was replaced since the read
        (reconfiguration), the log belongs to the new ring and none apply.
        """
        with self._stream_gaps_lock:
            logged = list(self._stream_gaps) if self._pre_trigger_buf is ring else []
        for idx, lost in _preroll_gaps(logged, start, end, written):
            self._add_recording_gap(idx, lost, overflow=True)

    def _unique_capture_name(self, stem: str) -> str:
        """``<stem>.sc16``, or ``<stem>-2.sc16``, ``-3`` ... when a capture of
        that name exists in auto/ or manual/: two starts in one second would
        otherwise overwrite the first (and share its iq_captures row)."""
        dirs = (self._storage.auto_dir, self._storage.manual_dir)
        name = f"{stem}.sc16"
        n = 1
        while any((d / name).exists() for d in dirs):
            n += 1
            name = f"{stem}-{n}.sc16"
        return name

    def _begin_recording(self) -> None:
        """Start recording: allocate the RAM buffer or start the disk writer.

        Runs at the fire site (receiver thread for triggers, a web worker for
        manual starts) under ``_rec_lock``. Only cheap, non-blocking work stays
        here: in disk mode the capture files are opened inside the writer
        thread and the pre-trigger samples are queued by reference (no
        ``.tobytes()`` copy), so the receiver thread never stalls on capture
        start. RAM-buffered mode still allocates its (RAM-bounded) buffer
        synchronously.
        """
        # The trigger path (arm_trigger / _check_trigger_and_record) already
        # gates itself before ever calling this, so this gate in practice only
        # guards the manual start_recording() call chain -- and must let it
        # through once the user has opted in via set_replay_recording(True).
        if self._replay_mode and not self._replay_record:
            return
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        # Triggered/continuous captures -> auto/ (FIFO); manual/replay -> manual/.
        self._recording_dir = (
            self._storage.auto_dir if self._trigger_initiated else self._storage.manual_dir
        )
        self._recording_file = self._unique_capture_name(
            f"{self._receiver.serial}-{self._settings.HOSTNAME}-{ts}"
        )
        self._recording_bytes = 0
        self._recording_dropped = 0
        self._recording_gaps = []
        self._recording_lost = 0
        self._recording_overflows = 0
        self._recording_gaps_truncated = False
        self._stop_reason = None
        self._writer_error = None
        self._disk_floor_hit = False
        self._recording_wall_start = None
        self._recording_start = time.monotonic()
        self._below_threshold_count = 0
        self._recording_grids = []
        self._recording_freq_axis = None
        self._recording_time_res = 0.0
        self._recording_cal_offset = self._settings.CAL_OFFSET_DB
        self._grid_rows = 0
        self._grid_dropped = 0
        self._grid_min = float("inf")
        self._grid_max = float("-inf")
        self._effective_max_sec = _effective_max_recording_sec(
            self._settings, _mem_available_bytes()
        )
        if self._effective_max_sec != float("inf"):
            logger.info("Recording auto-stop cap: %.1fs", self._effective_max_sec)
        from rfobserver.storage import psd_grid

        s = self._settings
        ring = self._pre_trigger_buf
        pre_data, pre_end = ring.read_with_position()
        t_read = time.time()
        pre_start = pre_end - len(pre_data)
        # Take only the buffered grid rows that actually cover this recording's
        # IQ. Grids reach the pre-buffer behind the chunk queue and worker pool,
        # so when that latency exceeds TRIGGER_PRE_SEC (the field default of
        # 0.2 s against ~820 ms of latency) every buffered grid predates
        # pre_start and this correctly yields nothing: the rows covering the
        # pre-roll have not been computed yet and arrive shortly after as live
        # grids. Selecting by arrival order instead put the .psd ~820 ms out of
        # step with its .sc16. See
        # docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md.
        pre_roll = self._grid_prebuf.drain(from_sample=pre_start)
        self._grid_first_sample = None
        self._grid_last_sample = 0
        self._recording_end_sample = None
        if pre_roll is not None:
            self._recording_freq_axis = pre_roll.freq_axis
            self._recording_time_res = pre_roll.time_res
            self._grid_min = pre_roll.grid_min
            self._grid_max = pre_roll.grid_max
            self._grid_first_sample = pre_roll.start_sample
            self._grid_last_sample = pre_roll.start_sample + pre_roll.rows * self._slice_samples

        if s.RECORDING_RAM_BUFFER:
            self._grid_raw_path = None
            # Pre-allocate RAM for the (RAM-bounded) max recording duration.
            max_sec = self._effective_max_sec if self._effective_max_sec != float("inf") else 30.0
            total_samples = int((s.TRIGGER_PRE_SEC + max_sec) * s.BANDWIDTH)
            self._recording_buf = np.zeros(total_samples, dtype=np.int32)
            self._recording_buf_pos = 0

            written = 0
            if len(pre_data) > 0:
                written = min(len(pre_data), total_samples)
                self._recording_buf[:written] = pre_data[:written]
                self._recording_buf_pos = written
                self._recording_bytes = written * 4
                self._add_preroll_gaps(ring, pre_start, pre_end, written=written)
            self._anchor_recording(ring, pre_start, written, len(pre_data), t_read)

            # Seed the grid list with the pre-roll grids; live grids append after.
            if pre_roll is not None:
                self._recording_grids = list(pre_roll.grids)

            self._grid_accepting = True
            self._recording_state = "recording"
            logger.info(
                "Recording started (RAM): %s (%.1f MB allocated)",
                self._recording_file,
                total_samples * 4 / 1e6,
            )
        else:
            self._recording_buf = None
            self._recording_buf_pos = 0
            self._grid_raw_path = psd_grid.grid_paths(self._recording_dir / self._recording_file)[0]

            # Drain stale queue data
            while not self._recording_queue.empty():
                try:
                    self._recording_queue.get_nowait()
                except queue.Empty:
                    break

            # Queue the pre-trigger samples by reference — the writer's
            # f.write() accepts any buffer-like object, so the old .tobytes()
            # copy (hundreds of MB on the receiver thread) is unnecessary.
            written = 0
            if len(pre_data) > 0:
                try:
                    self._recording_queue.put_nowait(("iq", pre_data))
                    written = len(pre_data)
                    self._recording_bytes = written * 4
                    self._add_preroll_gaps(ring, pre_start, pre_end, written=written)
                except queue.Full:
                    logger.warning("Recording queue full: pre-trigger dropped")
            self._anchor_recording(ring, pre_start, written, len(pre_data), t_read)

            # Seed the pre-roll grids ahead of any live grid so the .psd starts
            # at the pre-trigger head. Queued before the writer thread starts, so
            # they are the first rows it writes; live grids follow after the
            # state flip below.
            if pre_roll is not None:
                for g in pre_roll.grids:
                    try:
                        self._recording_queue.put_nowait(
                            ("grid", np.ascontiguousarray(g, dtype=np.float32).tobytes())
                        )
                        self._grid_rows += int(g.shape[0])
                    except queue.Full:
                        self._grid_dropped += int(g.shape[0])

            self._writer_thread = threading.Thread(
                target=self._file_writer_loop, name="writer", daemon=True
            )
            self._writer_thread.start()
            # Flip last: chunk writes gate on this state, and they must enter
            # the queue behind the pre-trigger samples seeded above.
            self._grid_accepting = True
            self._recording_state = "recording"
            logger.info("Recording started (disk): %s", self._recording_file)

    def _anchor_recording(
        self, ring: CircularBuffer, pre_start: int, written: int, pre_len: int, t_read: float
    ) -> None:
        """Pin where the file starts, once the pre-roll is placed (before the
        state flips to "recording").

        In the stream: the continuity check in _write_recording_chunk continues
        from ``pre_start + written`` in ``ring``, or from the first chunk when
        nothing was written. In wall time: start_time is the pre-roll read
        time minus the pre-roll's span (its samples plus the loss mapped into
        it), not the metadata write time, which runs late by the finalize
        latency.
        """
        self._recording_ring = ring
        self._recording_next_pos = pre_start + written if written > 0 else None
        self._recording_start_sample = pre_start if written > 0 else None
        rx_config = getattr(self._receiver, "config", None)
        rate = float(getattr(rx_config, "bandwidth_hz", None) or self._settings.BANDWIDTH)
        span = (pre_len + self._recording_lost) / rate if rate > 0 else 0.0
        self._recording_wall_start = t_read - span

    def _end_recording(self) -> None:
        """Finalize the recording (runs on the recording-control thread).

        The requester has already flipped the state to "finalizing", which
        stops chunk and grid writes; the job ends by setting "idle" (and the
        done event) so continuous mode can re-arm. The state flip and event
        live in a finally so a failure can never wedge the state machine.
        """
        try:
            self._finalize_recording()
        finally:
            # Set before "idle", so a continuous re-arm already sees the hold.
            if self._governor is not None and self._stop_reason in ("disk_floor", "write_error"):
                self._storage_hold = (self._governor.ticks, self._stop_reason)
            self._recording_state = "idle"
            self._end_done.set()

    def _await_tail_grids(self) -> None:
        """Wait for the in-flight PSD grids covering the tail of the recording.

        The IQ is written synchronously in the receive loop but grids emerge
        several chunks later, so at stop time the last chunks of IQ have no
        grid rows yet. Without this wait the .psd ends short of the .sc16 by one
        pipeline latency, and for a capture shorter than that latency it never
        reaches the trigger instant at all. Returns as soon as the grids reach
        the recording's last sample, which is the normal case.

        A timeout here is not fatal: the rows that did arrive are still
        correctly placed, the .psd is simply short, and the warning says by how
        much.
        """
        end = self._recording_end_sample
        if end is None or self._recording_start_sample is None:
            time.sleep(0.05)
            return
        cap = float(self._settings.RECORDING_MAX_SEC or 0.0)
        if not math.isfinite(cap) or cap <= 0:
            cap = 0.0
        cap = min(max(cap, _GRID_TAIL_DRAIN_FLOOR_SEC), _GRID_TAIL_DRAIN_CEILING_SEC)
        deadline = time.monotonic() + cap
        while self._grid_last_sample < end and time.monotonic() < deadline:
            time.sleep(0.02)
        if self._grid_last_sample < end:
            rate = float(self._settings.BANDWIDTH) or 1.0
            logger.warning(
                "PSD tail did not drain within %.1fs; .psd is %.3fs short of the IQ",
                cap,
                (end - self._grid_last_sample) / rate,
            )

    def _finalize_recording(self) -> None:
        """Stop recording, flush to disk, write metadata."""
        self._await_tail_grids()
        # Past this point no further grid rows may enter the file: the writer
        # thread is about to be stopped and its handle closed.
        self._grid_accepting = False

        duration = time.monotonic() - self._recording_start
        base_name = self._recording_file or "recording.sc16"

        # Add drop count to filename if any chunks were lost
        if self._recording_dropped > 0:
            base_name = base_name.replace(".sc16", f"_drop{self._recording_dropped}.sc16")
            self._recording_file = base_name

        if self._recording_buf is not None:
            # RAM mode: flush buffer to disk. tofile() streams the array straight
            # to the file — avoids the transient full-size copy that .tobytes()
            # makes, which would double IQ RAM right at the memory-cap boundary.
            filepath = self._recording_dir / base_name
            used = self._recording_buf[: self._recording_buf_pos]
            try:
                used.tofile(str(filepath))
            except OSError as exc:
                # Whatever reached disk is kept; metadata, DB insert and
                # eviction still run, flagged as failed.
                self._writer_error = describe_write_error(exc)
            self._recording_buf = None
            self._recording_buf_pos = 0
        else:
            # Disk mode: stop writer thread. Bounded put: if the writer already
            # died (disk full / EROFS), the queue may be full and no consumer
            # will ever drain it, so a plain blocking put would wedge recctl in
            # "finalizing" forever. Time-bounded, then proceed regardless.
            try:
                self._recording_queue.put(None, timeout=10.0)
            except queue.Full:
                logger.error("Recording writer not draining; abandoning writer thread")
            if self._writer_thread is not None:
                self._writer_thread.join(timeout=10)
                self._writer_thread = None

            # Rename file if drops occurred
            if self._recording_dropped > 0:
                orig_name = base_name.replace(f"_drop{self._recording_dropped}.sc16", ".sc16")
                orig = self._recording_dir / orig_name
                dest = self._recording_dir / base_name
                if orig.exists():
                    orig.rename(dest)

        # Metadata comes from the file on disk, never from the enqueue counter.
        self._recording_bytes = self._bytes_from_file(self._recording_dir / base_name)
        if self._writer_error is not None:
            self._report_write_error(f"{base_name}: {self._writer_error}")

        # Finalize the PSD grid companion (<base>.psd + .psd.json). Disk mode
        # streamed rows via the writer thread (which owns and has closed the
        # file by the join above); RAM mode flushes its list here row-by-row
        # (no np.concatenate). Either way, no whole-grid RAM copy.
        from rfobserver.storage import psd_grid

        raw_path, meta_path = psd_grid.grid_paths(self._recording_dir / base_name)
        if self._grid_raw_path is not None:
            # If the .sc16 was drop-renamed, move the streamed grid to match.
            if self._grid_raw_path != raw_path:
                with contextlib.suppress(OSError):
                    self._grid_raw_path.rename(raw_path)
            self._grid_raw_path = None
        elif self._recording_grids:
            try:
                with open(raw_path, "wb") as fh:
                    for g in self._recording_grids:
                        fh.write(np.ascontiguousarray(g, dtype=np.float32).tobytes())
                        self._grid_rows += g.shape[0]
            except OSError as exc:
                self._report_write_error(f"{raw_path.name}: {describe_write_error(exc)}")
            self._recording_grids = []
        if self._recording_freq_axis is not None:
            # The row count must come from the file, never from the queued-rows
            # counter. A row counted at put time but not written (a late put
            # racing the writer's shutdown) would make the sidecar claim more
            # rows than exist, and load_grid's memmap then fails outright with
            # "mmap length is greater than file size". Truncate any partial
            # trailing row for the same reason.
            num_bins = int(self._recording_freq_axis.shape[0])
            row_bytes = 4 * num_bins
            if num_bins > 0 and raw_path.exists():
                size = raw_path.stat().st_size
                actual_rows = size // row_bytes
                if size % row_bytes:
                    with contextlib.suppress(OSError):
                        os.truncate(raw_path, actual_rows * row_bytes)
                if actual_rows != self._grid_rows:
                    logger.warning(
                        "PSD grid rows on disk (%d) differ from rows queued (%d); "
                        "reporting the file",
                        actual_rows,
                        self._grid_rows,
                    )
                self._grid_rows = actual_rows
        if self._grid_rows > 0 and self._recording_freq_axis is not None:
            try:
                psd_grid.write_meta(
                    meta_path,
                    rows=self._grid_rows,
                    num_bins=int(self._recording_freq_axis.shape[0]),
                    time_resolution_s=self._recording_time_res,
                    center_freq_hz=self._settings.FREQUENCY_START,
                    bandwidth_hz=self._settings.BANDWIDTH,
                    freq_axis=self._recording_freq_axis,
                    grid_min=(0.0 if self._grid_min == float("inf") else self._grid_min),
                    grid_max=(0.0 if self._grid_max == float("-inf") else self._grid_max),
                    cal_offset_db=self._recording_cal_offset,
                    start_sample_offset=(
                        self._grid_first_sample - self._recording_start_sample
                        if self._grid_first_sample is not None
                        and self._recording_start_sample is not None
                        else 0
                    ),
                    slice_samples=self._slice_samples,
                )
                logger.info("PSD data saved: %s (%d rows)", meta_path.name, self._grid_rows)
            except OSError as exc:
                self._report_write_error(f"{meta_path.name}: {describe_write_error(exc)}")

        # Write companion metadata JSON
        self._write_recording_metadata(base_name, duration)

        # Schedule the detections sidecar (<base>.detections.json) after a grace
        # period so late-arriving burst detections inside the capture window are
        # persisted. This method runs off the event loop, so hand the coroutine
        # back to the loop thread-safely; failures never affect recording.
        if self._loop is not None and self._db is not None:
            sc16 = self._recording_dir / base_name
            grace = self._settings.DETECTIONS_SIDECAR_GRACE_SEC
            self._loop.call_soon_threadsafe(
                lambda: asyncio.ensure_future(self._deferred_sidecar(sc16, grace))
            )

        logger.info(
            "Recording saved: %s (%d bytes, %.1fs, %d dropped, %d grid rows dropped, "
            "%d overflow gaps (%d samples lost))",
            base_name,
            self._recording_bytes,
            duration,
            self._recording_dropped,
            self._grid_dropped,
            self._recording_overflows,
            self._recording_lost,
        )

        # Keep the capture archive bounded by ARCHIVE_MAX_GB via FIFO eviction of
        # oldest captures. Runs for every finalized capture (this newest one is
        # never evicted), so continuous triggering cannot fill the disk. Sync the
        # cap from settings first -- ARCHIVE_MAX_GB may have been changed at
        # runtime via /config/apply after LocalStorage snapshotted it at startup.
        self._storage.max_bytes = int(self._settings.ARCHIVE_MAX_GB * 1024**3)
        self._storage.enforce_cap()

    def _report_write_error(self, message: str) -> None:
        """Log a failed write and set the governor's sticky flag."""
        logger.error("Write failed: %s", message)
        if self._governor is not None:
            self._governor.report_write_error(message)

    def _check_disk_floor(self) -> None:
        """Writer thread, about once per second of IQ: end the recording
        cleanly if free space is below half the floor, before ENOSPC."""
        try:
            du = self._disk_usage(self._recording_dir)
        except OSError:
            return
        floor = resolve_floor(self._settings.DISK_MIN_FREE_GB, du.total)
        if du.free < floor * HARD_FLOOR_FRACTION and not self._disk_floor_hit:
            logger.warning(
                "Free space %.2f GB below half the %.2f GB floor: ending the recording",
                du.free / 1024**3,
                floor / 1024**3,
            )
            self._disk_floor_hit = True

    def _bytes_from_file(self, path: Path) -> int:
        """The capture's true size, from the closed file: a partial trailing
        sample (a short write) is truncated away. The enqueue counter can
        claim bytes that never reached the disk."""
        try:
            size = path.stat().st_size
        except OSError:
            return 0
        whole = size - size % 4
        if whole != size:
            with contextlib.suppress(OSError):
                os.truncate(path, whole)
        if whole != self._recording_bytes and self._writer_error is None:
            logger.warning(
                "IQ bytes on disk (%d) differ from bytes queued (%d); reporting the file",
                whole,
                self._recording_bytes,
            )
        return whole

    async def _deferred_sidecar(self, sc16_path: Path, grace: float) -> None:
        """Write the detections sidecar after a grace delay (runs on the loop).

        Replay detections are never inserted into the DB (replay_mode
        suppresses all persistence), so a replay recording's sidecar is built
        by re-running burst detection on the recorded PSD grid instead of
        querying the DB.
        """
        from rfobserver.storage.detections_sidecar import write_sidecar, write_sidecar_from_grid

        try:
            await asyncio.sleep(grace)
            if not sc16_path.exists():
                # Evicted inside the grace window: a sidecar now would be an orphan.
                logger.debug("Capture %s is gone; skipping its detections sidecar", sc16_path.name)
                return
            if self._replay_mode:
                s = self._settings
                cfg = BurstDetectionConfig(
                    threshold_high_db=s.BURST_THRESHOLD_HIGH_DB,
                    threshold_low_ratio=s.BURST_THRESHOLD_LOW_RATIO,
                    noise_floor_percentile=s.BURST_NOISE_FLOOR_PERCENTILE,
                    merge_time_sec=s.BURST_MERGE_TIME_MS / 1000.0,
                    merge_freq_bins=s.BURST_MERGE_FREQ_BINS,
                )
                # detect_bursts on a full grid is multi-second CPU work; keep it
                # off the event loop so the live WS/heartbeat stay responsive.
                await asyncio.to_thread(write_sidecar_from_grid, sc16_path, cfg)
            elif self._db is not None:
                await write_sidecar(sc16_path, self._db)
        except Exception:
            logger.exception("Detections sidecar write failed for %s", sc16_path.name)

    def _write_recording_metadata(self, filename: str, duration: float) -> None:
        """Write companion .json with capture metadata."""
        import json as _json

        s = self._settings
        # Read tuning from the receiver's achieved config (same source and
        # fallback the detection-insert path uses at _process_bursts), so the
        # capture's sample_rate/gain exactly match the sdr_center_freq/
        # sample_rate/gain stored on its detections. Hardware coerces requested
        # settings to achievable values, so reading raw settings here would make
        # the detections sidecar's exact-match filter silently return nothing.
        rx_config = getattr(self._receiver, "config", None)
        sample_rate_hz = float(getattr(rx_config, "bandwidth_hz", None) or s.BANDWIDTH)
        gain_db = float(getattr(rx_config, "gain_db", None) or s.GAIN)
        # Report the duration of the actual recorded signal, derived from the
        # sample count so it matches the .sc16/.psd length. The wall-clock
        # `duration` argument covers only the post-trigger span; the pre-trigger
        # pre-roll prepended at _begin_recording is real recorded signal and
        # belongs in the reported duration (and pushes start_time back to the
        # first pre-roll sample). Falls back to the wall clock if the rate is
        # unknown.
        total_samples = self._recording_bytes // 4
        signal_duration = (total_samples / sample_rate_hz) if sample_rate_hz > 0 else duration
        # Gaps: the .sc16 is contiguous (samples are never zero-filled), so the
        # .json describes where samples are missing.
        # - A gap is [file_sample_index, lost_samples]: lost_samples belong
        #   immediately before file_sample_index.
        # - Sources: UHD overflow gaps (also counted in overflow_events), and
        #   chunks dropped from the recording queue or RAM buffer (already in
        #   dropped_chunks, now also one gap of that chunk's length plus any
        #   overflow loss inside it).
        # - Gaps at index 0 (before the file starts) are not recorded.
        # - lost_samples is the sum over gaps, and
        #   time_span_sec = (total_samples + lost_samples) / sample_rate_hz.
        # - gaps holds at most _MAX_RECORDED_GAPS entries; beyond that
        #   gaps_truncated is true while lost_samples and overflow_events keep
        #   counting.
        # start_time and the DB span use the true time span (samples + lost),
        # the wall time the capture covers; duration_sec stays the file's
        # sample length.
        lost = self._recording_lost
        write_error = self._writer_error
        gaps = self._recording_gaps
        if write_error is not None:
            # The file ends early: gaps queued past its end describe nothing.
            gaps = [g for g in gaps if g[0] <= total_samples]
            if not self._recording_gaps_truncated:
                lost = sum(g[1] for g in gaps)
        time_span = (total_samples + lost) / sample_rate_hz if sample_rate_hz > 0 else duration
        if self._recording_wall_start is not None:
            start_dt = datetime.fromtimestamp(self._recording_wall_start, tz=timezone.utc)
        else:
            start_dt = datetime.fromtimestamp(time.time() - time_span, tz=timezone.utc)
        meta = {
            "file": filename,
            "format": "sc16",
            "sample_rate_hz": sample_rate_hz,
            "center_freq_hz": s.FREQUENCY_START,
            "bandwidth_hz": sample_rate_hz,
            "gain_db": gain_db,
            "start_time": start_dt.isoformat(),
            "duration_sec": round(signal_duration, 3),
            "total_bytes": self._recording_bytes,
            "total_samples": total_samples,
            "dropped_chunks": self._recording_dropped,
            "overflow_events": self._recording_overflows,
            "lost_samples": lost,
            "gaps": gaps,
            "gaps_truncated": self._recording_gaps_truncated,
            "time_span_sec": round(time_span, 3),
            "pre_trigger_sec": s.TRIGGER_PRE_SEC,
            "trigger_initiated": self._trigger_initiated,
            "stopped_reason": self._stop_reason or "manual",
            "write_failed": write_error is not None,
            "ram_buffered": s.RECORDING_RAM_BUFFER,
            "hostname": s.HOSTNAME,
            "serial": self._receiver.serial,
        }
        if write_error is not None:
            meta["write_error"] = write_error
        from rfobserver.storage.psd_grid import write_text_atomic

        json_path = self._recording_dir / filename.replace(".sc16", ".json")
        try:
            # Via a tmp file: on a full disk a direct write leaves a 0-byte .json.
            write_text_atomic(json_path, _json.dumps(meta, indent=2))
        except OSError as exc:
            # Report and carry on: the DB insert below still records the capture.
            self._report_write_error(f"{filename} metadata .json: {describe_write_error(exc)}")

        # Record the capture's span in the DB so the Dashboard can highlight
        # where IQ is available and link straight to it. Uses the settings-
        # derived tuning (the same values avg_windows carry, which populate the
        # Dashboard's tuning selectors) so the highlight matches the tuning
        # filter. Scheduled on the loop because this runs on the recording-
        # control thread; skipped in replay mode like the rest of persistence.
        if not self._replay_mode and self._loop is not None and self._db is not None:
            origin = "auto" if self._trigger_initiated else "manual"
            stop_dt = start_dt + timedelta(seconds=time_span)
            self._loop.call_soon_threadsafe(
                lambda: asyncio.ensure_future(
                    self._db.insert_iq_capture(
                        filename=filename,
                        origin=origin,
                        start_time=start_dt,
                        stop_time=stop_dt,
                        duration_sec=round(signal_duration, 3),
                        sdr_center_freq_hz=float(s.FREQUENCY_START),
                        sample_rate_hz=float(s.BANDWIDTH),
                        gain_db=float(s.GAIN),
                        total_samples=total_samples,
                        trigger_initiated=self._trigger_initiated,
                    )
                )
            )

    def _file_writer_loop(self) -> None:
        """Dedicated thread: drains recording queue and writes to disk.

        Owns both capture files — the .sc16 IQ stream and the streamed .psd
        grid — so no file open/write/close ever touches the receiver or
        dispatch threads. Queue items are ("iq", data) / ("grid", data);
        None stops the loop. Pinned to the last CPU core so PSD workers
        can't starve it.
        """
        # Pin writer to dedicated core (last core) for guaranteed CPU time
        try:
            total_cores = os.cpu_count() or 6
            writer_core = total_cores - 1
            os.sched_setaffinity(0, {writer_core})
            logger.info("Writer thread pinned to core %d", writer_core)
        except OSError:
            logger.debug("Could not pin writer thread to core")

        filepath = self._recording_dir / (self._recording_file or "recording.sc16")
        rate = float(self._settings.BANDWIDTH) or 1.0
        check_every = max(1, int(rate * 4))  # about one second of IQ
        since_check = 0
        stopped = False
        try:
            with (
                open(filepath, "wb", buffering=8 * 1024 * 1024) as f,
                open(self._grid_raw_path, "wb") as gf,
            ):
                while True:
                    item = self._recording_queue.get()
                    if item is None:
                        stopped = True
                        break
                    kind, data = item
                    if kind == "iq":
                        f.write(data)
                        since_check += memoryview(data).nbytes
                        if since_check >= check_every:
                            since_check = 0
                            self._check_disk_floor()
                    else:
                        gf.write(data)
                    # No flush: let the OS buffer writes for throughput. The
                    # close at loop exit flushes, and can itself fail (ENOSPC).
        except Exception as exc:
            self._writer_error = describe_write_error(exc)
            logger.error(
                "Recording write failed (%s); ending the recording",
                self._writer_error,
                # An OS error is fully described; anything else is a bug.
                exc_info=not isinstance(exc, OSError),
            )
            if not stopped:
                # Keep consuming so finalize's stop sentinel is never blocked
                # and the receiver thread never sees a full queue.
                while self._recording_queue.get() is not None:
                    pass

    # -- Dispatch thread --

    def _dispatch_loop(self) -> None:
        """Pull chunks, submit to worker pool, collect results in order."""
        s = self._settings
        grid_config = self._make_grid_config()
        freqs = self._build_frequency_list()
        center_freq = freqs[0] if freqs else s.FREQUENCY_START
        my_gen = self._config_generation

        capture_num = 0
        # Pin PSD workers to cores 0..N-2, reserving the last core for the
        # file writer thread during recording.
        total_cores = os.cpu_count() or 6
        worker_cores = set(range(total_cores - 1))  # exclude last core

        def _pin_worker() -> None:
            with contextlib.suppress(OSError):
                os.sched_setaffinity(0, worker_cores)

        executor = ThreadPoolExecutor(
            max_workers=self._num_proc_workers,
            thread_name_prefix="psd",
            initializer=_pin_worker,
        )

        max_inflight = self._num_proc_workers * 2
        pending_futures: list[Future[_ChunkResult]] = []

        try:
            while self._running:
                # Check for reconfiguration — rebuild PSD grid config
                if self._config_generation != my_gen:
                    my_gen = self._config_generation
                    grid_config = self._make_grid_config()
                    freqs = self._build_frequency_list()
                    center_freq = freqs[0] if freqs else s.FREQUENCY_START
                    logger.info("Dispatch loop reconfigured (bins=%d)", s.NUM_FFT_BINS)

                # Drain completed futures before accepting more work
                while pending_futures and pending_futures[0].done():
                    f = pending_futures.pop(0)
                    try:
                        self._handle_chunk_result(f.result())
                    except Exception:
                        logger.exception("Processing worker failed")

                try:
                    item = self._chunk_queue.get(
                        timeout=_RESULT_POLL_SEC if pending_futures else 0.5
                    )
                except queue.Empty:
                    continue
                if item is _STOP:
                    break

                sc16_buf, recv_time, chunk_start = item
                capture_num += 1

                # Too many in flight: drop (live) or, in lossless mode, block on
                # the oldest future so this chunk still gets processed.
                if len(pending_futures) >= max_inflight:
                    if self._drop_on_overflow:
                        self._dropped_chunks += 1
                        with contextlib.suppress(queue.Full):
                            self._buf_pool.put_nowait(sc16_buf)
                        continue
                    f = pending_futures.pop(0)
                    try:
                        self._handle_chunk_result(f.result())
                    except Exception:
                        logger.exception("Processing worker failed")

                future = executor.submit(
                    self._process_one_chunk,
                    sc16_buf,
                    recv_time,
                    chunk_start,
                    capture_num,
                    center_freq,
                    grid_config,
                )
                pending_futures.append(future)

        except Exception:
            logger.exception("Dispatch loop crashed")
        finally:
            for f in pending_futures:
                try:
                    self._handle_chunk_result(f.result(timeout=5.0))
                except Exception:
                    logger.exception("Processing worker failed during shutdown")
            executor.shutdown(wait=True, cancel_futures=True)
            _signal_stop(self._burst_queue)

    def _make_grid_config(self) -> PSDGridConfig:
        """Build a PSDGridConfig from current settings."""
        s = self._settings
        return PSDGridConfig(
            num_bins=s.NUM_FFT_BINS,
            time_resolution_ms=s.PSD_TIME_RESOLUTION_MS,
            num_workers=self._fft_workers,
        )

    def _process_one_chunk(
        self,
        sc16_buf: np.ndarray[Any, np.dtype[Any]],
        recv_time: float,
        chunk_start: int,
        capture_num: int,
        center_freq: int,
        grid_config: PSDGridConfig,
    ) -> _ChunkResult:
        """Pure processing function run on a worker thread."""
        t0 = time.monotonic()
        complex_chunk = convert_sc16_to_complex(sc16_buf)
        t_convert = time.monotonic()

        psd_grid = compute_psd_grid(complex_chunk, self._settings.BANDWIDTH, config=grid_config)
        t_psd = time.monotonic()

        iq_moments = moments_from_iq(complex_chunk)
        iq_stats = finalize_moments(iq_moments)
        t_stats = time.monotonic()

        summary_psd = compute_summary_psd(psd_grid, center_freq, self._settings.BANDWIDTH)

        process_ms = (time.monotonic() - t0) * 1000.0

        if capture_num % 50 == 0:
            logger.info(
                "WORKER chunk#%d: convert=%.1fms psd=%.1fms stats=%.1fms total=%.1fms",
                capture_num,
                (t_convert - t0) * 1000,
                (t_psd - t_convert) * 1000,
                (t_stats - t_psd) * 1000,
                process_ms,
            )

        return _ChunkResult(
            psd_grid=psd_grid,
            iq_stats=iq_stats,
            summary_psd=summary_psd,
            center_freq_hz=center_freq,
            capture_num=capture_num,
            recv_time=recv_time,
            chunk_start=chunk_start,
            process_ms=process_ms,
            sc16_buf=sc16_buf,
            iq_moments=iq_moments,
        )

    def _handle_chunk_result(self, cr: _ChunkResult) -> None:
        """Called from dispatch thread after a worker finishes."""
        with contextlib.suppress(queue.Full):
            self._buf_pool.put_nowait(cr.sc16_buf)

        with contextlib.suppress(queue.Full):
            self._burst_queue.put_nowait((cr.psd_grid, cr.center_freq_hz, cr.capture_num))

        # Persist PSD grids during recording. Disk mode hands rows to the
        # writer thread via the tagged queue (bounded RAM) — writing them
        # synchronously here would back-pressure the dispatch loop into
        # dropping PSD chunks (black waterfall rows) whenever the disk is
        # saturated by the IQ stream. RAM mode keeps them in the list, which
        # is bounded by the RAM-derived _effective_max_sec auto-stop.
        if self._grid_accepting and self._recording_start_sample is not None:
            # Place rows by the samples they describe, not by when they arrived.
            # While still recording there is no end bound yet, so use a sentinel
            # past this chunk's last row and trim only at the head.
            end = self._recording_end_sample
            if end is None:
                end = cr.chunk_start + int(cr.psd_grid.grid.shape[0]) * self._slice_samples
            grid, first_sample = trim_grid_rows(
                cr.psd_grid.grid,
                cr.chunk_start,
                self._slice_samples,
                self._recording_start_sample,
                end,
            )
            self._recording_freq_axis = cr.psd_grid.freq_axis
            if len(cr.psd_grid.time_axis) > 1:
                self._recording_time_res = float(
                    cr.psd_grid.time_axis[1] - cr.psd_grid.time_axis[0]
                )
            if grid.size:
                stored = True
                if self._grid_raw_path is not None:
                    data = np.ascontiguousarray(grid, dtype=np.float32).tobytes()
                    try:
                        self._recording_queue.put_nowait(("grid", data))
                        self._grid_rows += grid.shape[0]
                    except queue.Full:
                        # Companion data — drop rather than stall the dispatch loop.
                        self._grid_dropped += grid.shape[0]
                        stored = False
                else:
                    self._recording_grids.append(grid.copy())
                # Advance the fill trackers only once the rows are actually
                # stored. _await_tail_grids watches _grid_last_sample, so
                # advancing it before the put would let finalize declare the
                # tail complete for rows that were never queued.
                if stored:
                    if self._grid_first_sample is None:
                        self._grid_first_sample = first_sample
                    self._grid_last_sample = first_sample + int(grid.shape[0]) * self._slice_samples
                    self._grid_min = min(self._grid_min, float(grid.min()))
                    self._grid_max = max(self._grid_max, float(grid.max()))
        else:
            # Not recording: keep a rolling window of recent grids tagged with
            # their stream position, so a recording that fires can take the rows
            # that actually cover its pre-roll IQ.
            pre_time_res = 0.0
            if len(cr.psd_grid.time_axis) > 1:
                pre_time_res = float(cr.psd_grid.time_axis[1] - cr.psd_grid.time_axis[0])
            self._grid_prebuf.write(
                cr.psd_grid.grid,
                cr.psd_grid.freq_axis,
                pre_time_res,
                cr.chunk_start,
                self._slice_samples,
            )

        self._capture_count = cr.capture_num
        latency_ms = (time.monotonic() - cr.recv_time) * 1000.0

        if cr.capture_num % 50 == 0:
            logger.info(
                "PROC chunk#%d: process=%.1fms latency=%.1fms (IQ=%.1fms)",
                cr.capture_num,
                cr.process_ms,
                latency_ms,
                self._chunk_duration * 1000,
            )

        result = _StreamResult(
            summary_psd=cr.summary_psd,
            iq_stats=cr.iq_stats,
            bursts=[],
            psd_grid=cr.psd_grid,
            center_freq_hz=cr.center_freq_hz,
            capture_num=cr.capture_num,
            process_ms=cr.process_ms,
            latency_ms=latency_ms,
            iq_moments=cr.iq_moments,
        )

        if self._loop is not None:
            self._result_handoff.submit(self._loop, result)

    # -- Burst detection thread --

    def _burst_detection_loop(self) -> None:
        """Dedicated thread for rolling burst detection."""
        s = self._settings
        burst_config = self._make_burst_config()
        rolling_detector: RollingBurstDetector | None = None
        my_gen = self._config_generation

        try:
            while self._running:
                # Reconfigure: rebuild burst config, reset detector
                if self._config_generation != my_gen:
                    my_gen = self._config_generation
                    burst_config = self._make_burst_config()
                    rolling_detector = None  # re-init on next grid
                    logger.info("Burst detection loop reconfigured")

                try:
                    item = self._burst_queue.get(timeout=2.0)
                except queue.Empty:
                    continue
                if item is _STOP:
                    break

                psd_grid, freq_hz, capture_num = item

                # Skip grids whose bin count doesn't match current config
                # (stale grids from before a reconfiguration)
                if psd_grid.grid.shape[1] != s.NUM_FFT_BINS:
                    continue

                if rolling_detector is None:
                    freqs = self._build_frequency_list()
                    center_freq = freqs[0] if freqs else s.FREQUENCY_START
                    if len(psd_grid.time_axis) > 1:
                        time_res_s = psd_grid.time_axis[1] - psd_grid.time_axis[0]
                    else:
                        time_res_s = s.PSD_TIME_RESOLUTION_MS / 1000.0
                    rolling_detector = RollingBurstDetector(
                        window_rows=s.BURST_WINDOW_ROWS,
                        eval_interval_rows=s.BURST_EVAL_INTERVAL_ROWS,
                        num_bins=s.NUM_FFT_BINS,
                        burst_config=burst_config,
                        center_freq_hz=float(center_freq),
                        freq_axis=psd_grid.freq_axis,
                        time_resolution_s=time_res_s,
                    )

                # Snapshot the rolling-window per-bin noise floor only on
                # evaluation boundaries. detect_bursts already computes the
                # array; we just read it from _last_detection. Recomputing on
                # every chunk wedged the burst-detection thread (see fix in
                # this commit), so we piggy-back on the detector's existing
                # work and only refresh when it actually evaluates.
                prev_rows_since_eval = rolling_detector._rows_since_eval
                completed_bursts = rolling_detector.feed(psd_grid)
                if rolling_detector._rows_since_eval < prev_rows_since_eval:
                    last_det = rolling_detector._last_detection
                    if last_det is not None and last_det.noise_floor_per_bin is not None:
                        self._noise_floor_per_bin = last_det.noise_floor_per_bin.tolist()

                if completed_bursts and self._loop is not None:
                    self._burst_handoff.submit(self._loop, (completed_bursts, int(freq_hz)))

                # Build active burst overlay data for WebSocket.
                # Each burst carries absolute frequency bounds plus real
                # start/stop UTC timestamps (epoch ms). The frontend
                # correlates these with per-spectrum-line timestamps to
                # place rectangles — no pixel math here.
                #
                # BurstFingerprint stores timestamps as
                # ``capture_time + offset_within_window`` (legacy encoding),
                # so the real wall-clock time is ``stored - window_dur``.
                detection = rolling_detector.last_detection
                if detection and detection.bursts:
                    time_res = rolling_detector._time_resolution_s
                    window_dur = rolling_detector._rows_filled * time_res
                    active: list[dict[str, object]] = []
                    for b in detection.bursts:
                        real_start = b.start_time - timedelta(seconds=window_dur)
                        real_stop = b.stop_time - timedelta(seconds=window_dur)
                        active.append(
                            {
                                "id": b.burst_id,
                                "center_freq_hz": b.center_freq_hz,
                                "bandwidth_hz": b.bandwidth_hz,
                                "freq_low_hz": b.center_freq_hz - b.bandwidth_hz / 2,
                                "freq_high_hz": b.center_freq_hz + b.bandwidth_hz / 2,
                                "peak_power_db": round(b.peak_power_db, 1),
                                "duration_ms": round(b.duration_ms, 2),
                                "start_time_ms": real_start.timestamp() * 1000.0,
                                "stop_time_ms": real_stop.timestamp() * 1000.0,
                            }
                        )
                    self._active_bursts = active
                else:
                    self._active_bursts = []

        except Exception:
            logger.exception("Burst detection loop crashed")

    def _make_burst_config(self) -> BurstDetectionConfig:
        """Build a BurstDetectionConfig from current settings."""
        s = self._settings
        return BurstDetectionConfig(
            threshold_high_db=s.BURST_THRESHOLD_HIGH_DB,
            threshold_low_ratio=s.BURST_THRESHOLD_LOW_RATIO,
            merge_freq_bins=s.BURST_MERGE_FREQ_BINS,
            merge_time_sec=s.BURST_MERGE_TIME_MS / 1000.0,
            noise_floor_percentile=s.BURST_NOISE_FLOOR_PERCENTILE,
        )

    # -- Async result consumer (event loop) --

    async def _result_consumer_loop(self) -> None:
        """Broadcast results to WebSocket, store bursts, submit to ZMS.

        The UI has two modes controlled by a High Res toggle:
        - **Normal** (high_res off): accumulate PSD over DURATION_SEC,
          broadcast one averaged update — same cadence as the old batch mode.
        - **High Res** (high_res on): broadcast each chunk's PSD immediately
          for maximum time resolution (~25 updates/sec).

        ZMS always receives DURATION_SEC-averaged data regardless of toggle.
        """
        # Accumulation for normal-mode UI and ZMS
        import numpy as _np

        accum_powers: list[list[float]] = []
        accum_moments: IQMoments | None = None
        accum_start = time.monotonic()
        last_result: _StreamResult | None = None

        while self._running:
            try:
                result = await asyncio.wait_for(self._result_queue.get(), timeout=0.5)
            except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041
                await self._drain_burst_results()
                # Flush accumulator on timeout if data pending
                if accum_powers and last_result is not None:
                    avg = _np.mean(accum_powers, axis=0).tolist()
                    # accum_moments is folded right after each accum_powers.append,
                    # so it is non-None whenever accum_powers is non-empty.
                    assert accum_moments is not None
                    interval_stats = finalize_moments(accum_moments)
                    await self._broadcast_averaged(avg, last_result, len(accum_powers))
                    await self._publish_processed(avg, last_result, interval_stats)
                    accum_powers.clear()
                    accum_moments = None
                    accum_start = time.monotonic()
                    last_result = None
                continue

            await self._drain_burst_results()

            if result is _STOP:
                break

            if self._beacon is not None:
                self._beacon.mark()

            # --- Accumulate for normal-mode UI + downstream publishing ---
            # On reconfigure, NUM_FFT_BINS may change. In-flight results from
            # the prior config can interleave with new-config results in
            # _result_queue (worker pool finishes futures out of order), so
            # detect shape changes per-row, not just on a generation flip,
            # otherwise np.mean below raises on mismatched-shape rows.
            new_powers = result.summary_psd.powers
            if accum_powers and len(accum_powers[0]) != len(new_powers):
                accum_powers.clear()
                accum_moments = None
                accum_start = time.monotonic()
                last_result = None
            accum_powers.append(new_powers)
            accum_moments = (
                result.iq_moments if accum_moments is None else accum_moments.add(result.iq_moments)
            )
            last_result = result

            elapsed = time.monotonic() - accum_start
            if elapsed >= self._settings.DURATION_SEC:
                avg = _np.mean(accum_powers, axis=0).tolist()
                # accum_moments is folded right after each accum_powers.append,
                # so it is non-None whenever accum_powers is non-empty.
                assert accum_moments is not None
                interval_stats = finalize_moments(accum_moments)

                if self._settings.TONE_CHECK_ENABLED:
                    await self._run_tone_check(avg, result)

                # Normal-mode UI broadcast (only if no high-res subscribers)
                if self._broadcast is not None and not self._broadcast.has_high_res_subscribers():
                    await self._broadcast_averaged(avg, result, len(accum_powers))

                # ZMS + NATS always get DURATION_SEC-averaged data
                await self._publish_processed(avg, result, interval_stats)

                accum_powers.clear()
                accum_moments = None
                accum_start = time.monotonic()
                last_result = None

            # --- High-res UI broadcast (every chunk) ---
            if self._broadcast is not None and self._broadcast.has_high_res_subscribers():
                noise_floor_db = float(np.percentile(result.summary_psd.powers, 10))
                # Per-bin chunk max — peak across the high-resolution PSD rows
                # in this chunk. This is what the detector actually compared
                # against; the UI plots it as a "max-hold" trace so transient
                # bursts that get smoothed out of the time-averaged summary
                # are still visible against the per-bin threshold curve.
                max_powers = result.psd_grid.grid.max(axis=0).astype(float).tolist()
                await self._broadcast.publish(
                    {
                        "type": "psd",
                        "center_freq_hz": result.center_freq_hz,
                        "bandwidth_hz": self._settings.BANDWIDTH,
                        "powers": result.summary_psd.powers,
                        "max_powers": max_powers,
                        "frequencies": result.summary_psd.frequencies,
                        "num_bins": result.summary_psd.num_bins,
                        "avg_power_db": result.iq_stats.average,
                        "max_power_db": result.iq_stats.max,
                        "kurtosis": result.iq_stats.kurtosis,
                        "burst_count": len(self._active_bursts),
                        "bursts": self._active_bursts,
                        "capture_num": result.capture_num,
                        "process_ms": result.process_ms,
                        "excess_ms": result.latency_ms,
                        "trigger_threshold_db": self._settings.TRIGGER_THRESHOLD_DB,
                        "burst_threshold_high_db": self._settings.BURST_THRESHOLD_HIGH_DB,
                        "burst_threshold_low_ratio": self._settings.BURST_THRESHOLD_LOW_RATIO,
                        "noise_floor_db": noise_floor_db,
                        "noise_floor_per_bin": self._noise_floor_per_bin,
                        "cal_offset_db": self._settings.CAL_OFFSET_DB,
                        "scale_min_db": self._settings.PSD_SCALE_MIN_DB,
                        "scale_max_db": self._settings.PSD_SCALE_MAX_DB,
                        "chunk_time_ms": datetime.now(timezone.utc).timestamp() * 1000.0,
                    }
                )

    async def _broadcast_averaged(
        self,
        avg_powers: list[float],
        result: _StreamResult,
        chunk_count: int,
    ) -> None:
        """Broadcast a DURATION_SEC-averaged PSD to the UI.

        Burst rectangles are intentionally omitted from the averaged broadcast:
        bursts are detected at PSD-grid resolution (~0.5 ms rows) but each
        averaged-mode waterfall row covers DURATION_SEC, so any rectangle
        timing would be off by 25-1000x. The high-res broadcast (sent only
        when a subscriber opts in) carries them.
        """
        if self._broadcast is None:
            return
        noise_floor_db = float(np.percentile(avg_powers, 10)) if avg_powers else -200.0
        max_powers = result.psd_grid.grid.max(axis=0).astype(float).tolist()
        await self._broadcast.publish(
            {
                "type": "psd",
                "center_freq_hz": result.center_freq_hz,
                "bandwidth_hz": self._settings.BANDWIDTH,
                "powers": avg_powers,
                "max_powers": max_powers,
                "frequencies": result.summary_psd.frequencies,
                "num_bins": result.summary_psd.num_bins,
                "avg_power_db": result.iq_stats.average,
                "max_power_db": result.iq_stats.max,
                "kurtosis": result.iq_stats.kurtosis,
                "burst_count": len(self._active_bursts),
                "bursts": [],
                "capture_num": result.capture_num,
                "process_ms": result.process_ms,
                "excess_ms": result.latency_ms,
                "chunks_averaged": chunk_count,
                "trigger_threshold_db": self._settings.TRIGGER_THRESHOLD_DB,
                "burst_threshold_high_db": self._settings.BURST_THRESHOLD_HIGH_DB,
                "burst_threshold_low_ratio": self._settings.BURST_THRESHOLD_LOW_RATIO,
                "noise_floor_db": noise_floor_db,
                "noise_floor_per_bin": self._noise_floor_per_bin,
                "cal_offset_db": self._settings.CAL_OFFSET_DB,
                "scale_min_db": self._settings.PSD_SCALE_MIN_DB,
                "scale_max_db": self._settings.PSD_SCALE_MAX_DB,
                "chunk_time_ms": datetime.now(timezone.utc).timestamp() * 1000.0,
            }
        )

    def _build_envelope(
        self, avg_powers: list[float], result: _StreamResult, iq_stats: IQStatistics
    ) -> ProcessedDataEnvelope:
        """Construct a ProcessedDataEnvelope from a DURATION_SEC-averaged result.

        Streaming mode has no on-disk source file (audio/PSD only), so
        ``source_path`` is left empty rather than pointing at a fake path.
        Downstream consumers that key off ``source_path`` should treat
        empty as "sensor-processed; no IQ file available".
        """
        from pathlib import Path

        from rfobserver.models import MetadataRecord, ProcessedDataEnvelope, PSDData

        s = self._settings
        averaged_psd = PSDData(
            powers=avg_powers,
            frequencies=result.summary_psd.frequencies,
            center_freq=result.summary_psd.center_freq,
            sample_rate=result.summary_psd.sample_rate,
            num_bins=result.summary_psd.num_bins,
        )
        meta = MetadataRecord(
            hostname=s.HOSTNAME,
            organization=s.ORGANIZATION,
            serial=self._receiver.serial,
            frequency=result.center_freq_hz,
            timestamp=datetime.now(timezone.utc),
            source_path=Path(""),
            gain=s.GAIN,
            sampling_rate=s.BANDWIDTH,
            length=s.DURATION_SEC,
            interval=s.INTERVAL_SEC,
            bit_depth=16,
        )
        return ProcessedDataEnvelope(
            metadata=meta,
            statistics=iq_stats,
            psd_data=averaged_psd,
        )

    async def _run_tone_check(self, avg_powers: list[float], result: _StreamResult) -> None:
        """Evaluate the tone check on the averaged PSD and persist + log it."""
        if self._replay_mode:
            return
        from rfobserver.processing.tone_check import evaluate_tone_check

        tc = evaluate_tone_check(
            avg_powers,
            result.summary_psd.frequencies,
            tone_freq_hz=self._settings.TONE_CHECK_FREQ_HZ,
            threshold_db=self._settings.TONE_CHECK_THRESHOLD_DB,
        )
        try:
            await self._db.insert_tone_check(
                timestamp=datetime.now(timezone.utc),
                tone_freq_hz=tc["tone_freq_hz"],
                sdr_center_freq_hz=result.center_freq_hz,
                in_band=tc["in_band"],
                tone_power_db=tc["tone_power_db"],
                noise_floor_db=tc["noise_floor_db"],
                snr_db=tc["snr_db"],
                detected=tc["detected"],
            )
        except Exception:
            logger.exception("tone-check insert failed")
        state = "DETECTED" if tc["detected"] else ("out-of-band" if not tc["in_band"] else "absent")
        snr = tc["snr_db"]
        logger.info(
            "TONE CHECK %.4f MHz: %s (snr=%s dB, floor=%s dB)",
            tc["tone_freq_hz"] / 1e6,
            state,
            f"{snr:.1f}" if snr is not None else "n/a",
            f"{tc['noise_floor_db']:.1f}" if tc["noise_floor_db"] is not None else "n/a",
        )

    async def _persist_avg_window(
        self, avg_powers: list[float], result: _StreamResult, iq_stats: IQStatistics
    ) -> None:
        """Store the averaged window locally. Runs for every live window,
        independent of whether ZMS/NATS are attached. Flags (interference /
        violations) are not computed in the streaming path yet, so they are left
        NULL until the PSDProcessor gap is closed."""
        s = self._settings
        freqs = result.summary_psd.frequencies
        freq_start = float(freqs[0]) if freqs else 0.0
        freq_step = float(freqs[1] - freqs[0]) if len(freqs) > 1 else 0.0
        try:
            await self._db.insert_avg_window(
                start_time=datetime.now(timezone.utc),
                duration_sec=s.DURATION_SEC,
                sdr_center_freq_hz=float(result.center_freq_hz),
                sample_rate_hz=float(s.BANDWIDTH),
                gain_db=float(s.GAIN),
                num_bins=result.summary_psd.num_bins,
                freq_start_hz=freq_start,
                freq_step_hz=freq_step,
                pwr_avg=iq_stats.average,
                pwr_max=iq_stats.max,
                pwr_median=iq_stats.median,
                pwr_std=iq_stats.std,
                kurtosis=iq_stats.kurtosis,
                powers=(
                    None
                    if self._governor is not None and self._governor.state.skip_psd_blobs
                    else avg_powers
                ),
            )
        except Exception as exc:
            if is_disk_full_error(exc):
                self._report_write_error(f"database: {exc}")
            logger.exception("avg-window persist failed (chunk #%d)", result.capture_num)

    async def _publish_processed(
        self, avg_powers: list[float], result: _StreamResult, iq_stats: IQStatistics
    ) -> None:
        """Build the per-window envelope once, fan out to ZMS + NATS.

        Both fanouts run as background tasks so the consumer loop returns
        immediately. ZMS POSTs and NATS publishes can take 10-25 ms each;
        awaiting them inline previously blocked the next high-res FFT
        broadcast every DURATION_SEC, which the user saw as a stutter.
        """
        if self._replay_mode:
            return
        await self._persist_avg_window(avg_powers, result, iq_stats)
        if self._zms_monitor is None and self._nats_producer is None:
            return
        try:
            envelope = self._build_envelope(avg_powers, result, iq_stats)
        except Exception:
            logger.exception("Envelope construction failed (chunk #%d)", result.capture_num)
            return

        if self._zms_monitor is not None:
            asyncio.create_task(self._zms_submit_async(envelope, result.capture_num))

        if self._nats_producer is not None:
            asyncio.create_task(self._nats_publish_async(envelope))

    async def _zms_submit_async(self, envelope: ProcessedDataEnvelope, chunk_num: int) -> None:
        timeout = max(15.0, 5.0 * self._settings.DURATION_SEC)
        try:
            ok = await asyncio.wait_for(
                self._zms_monitor.submit_observation(envelope),  # type: ignore[union-attr]
                timeout=timeout,
            )
            if ok:
                logger.debug("ZMS observation submitted (chunk #%d)", chunk_num)
        except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041
            logger.warning("ZMS submit timed out (chunk #%d) after %.1fs", chunk_num, timeout)
        except Exception:
            logger.exception("ZMS observation submission failed")

    async def _nats_publish_async(self, envelope: ProcessedDataEnvelope) -> None:
        try:
            await self._nats_producer.publish_stats(envelope, self._settings.HOSTNAME)  # type: ignore[union-attr]
        except Exception:
            logger.exception("NATS stats publish failed")

    async def _drain_burst_results(self) -> None:
        """Process all pending burst results from the burst detection thread."""
        rows: list[dict[str, Any]] = []
        while True:
            try:
                item = self._burst_result_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is None:
                break

            bursts, sdr_center_freq_hz = item

            # SDR capture context, recorded on every detection so they can be
            # categorized by tuning (center / sample rate / gain). Center comes
            # from the dwell that produced these bursts; the rest only change on
            # reconfigure, so reading them here is accurate. LO offset and
            # analog BW aren't configured in the receiver yet (always 0 / None).
            rx_config = getattr(self._receiver, "config", None)
            sample_rate_hz = float(
                getattr(rx_config, "bandwidth_hz", None) or self._settings.BANDWIDTH
            )
            gain_db = float(getattr(rx_config, "gain_db", None) or self._settings.GAIN)
            device_serial = getattr(self._receiver, "serial", None)

            for burst in bursts:
                if self._replay_mode:
                    continue
                rows.append(
                    {
                        "burst_id": burst.burst_id,
                        "start_time": burst.start_time,
                        "stop_time": burst.stop_time,
                        "center_freq_hz": burst.center_freq_hz,
                        "bandwidth_hz": burst.bandwidth_hz,
                        "peak_power_db": burst.peak_power_db,
                        "duration_ms": burst.duration_ms,
                        "detection_timestamp": burst.detection_timestamp,
                        "peak_freq_hz": burst.peak_freq_hz,
                        "sdr_center_freq_hz": float(sdr_center_freq_hz),
                        "sample_rate_hz": sample_rate_hz,
                        "lo_offset_hz": 0.0,
                        "analog_bw_hz": None,
                        "gain_db": gain_db,
                        "antenna": "RX2",
                        "device_serial": device_serial,
                    }
                )

            if bursts:
                logger.info("Detected %d bursts", len(bursts))

        if rows:
            try:
                await self._db.insert_detections(rows)
            except Exception as exc:
                if is_disk_full_error(exc):
                    self._report_write_error(f"database: {exc}")
                logger.exception("insert_detections failed for %d bursts; skipping", len(rows))

    # -- Helpers --

    def _reconfigure_receiver(self) -> None:
        """Reconfigure receiver hardware from current settings (blocking)."""
        from rfobserver.capture.receiver import ReceiverConfig

        s = self._settings
        new_config = ReceiverConfig(
            gain_db=s.GAIN,
            bandwidth_hz=s.BANDWIDTH,
            duration_sec=s.DURATION_SEC,
        )
        # _reconfigure_blocking acquires the hardware lock and re-inits
        reconfigure_fn = getattr(self._receiver, "_reconfigure_blocking", None)
        if reconfigure_fn is not None:
            reconfigure_fn(new_config)
        logger.info(
            "Receiver hardware reconfigured: BW=%d, gain=%d, dur=%.1fs",
            s.BANDWIDTH,
            s.GAIN,
            s.DURATION_SEC,
        )

    def _build_frequency_list(self) -> list[int]:
        s = self._settings
        if s.FREQUENCY_STEP <= 0 or s.FREQUENCY_END <= s.FREQUENCY_START:
            return [s.FREQUENCY_START]
        freqs: list[int] = []
        f = s.FREQUENCY_START
        while f <= s.FREQUENCY_END:
            freqs.append(f)
            f += s.FREQUENCY_STEP
        return freqs
