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
import contextlib
import logging
import os
import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_sc16_to_complex
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid, compute_summary_psd

if TYPE_CHECKING:
    from rfobserver.capture.receiver import IReceiver
    from rfobserver.config import AppSettings
    from rfobserver.models import BurstFingerprint, IQStatistics, PSDData
    from rfobserver.processing.spectral import PSDGridResult
    from rfobserver.storage.database import SensorDatabase
    from rfobserver.storage.local import LocalStorage
    from rfobserver.web.websocket import LiveBroadcast
    from rfobserver.zms.monitor import ZmsMonitor

logger = logging.getLogger(__name__)

# Sentinel used to signal threads to shut down.
# Must not be None (timeout returns None, causing false shutdown).
_STOP = object()


class _StreamResult:
    """Container for results produced by the processing workers."""

    __slots__ = (
        "summary_psd",
        "iq_stats",
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
    ) -> None:
        self.summary_psd = summary_psd
        self.iq_stats = iq_stats
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
        "summary_psd",
        "center_freq_hz",
        "capture_num",
        "recv_time",
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
        process_ms: float,
        sc16_buf: np.ndarray[Any, np.dtype[Any]],
    ) -> None:
        self.psd_grid = psd_grid
        self.iq_stats = iq_stats
        self.summary_psd = summary_psd
        self.center_freq_hz = center_freq_hz
        self.capture_num = capture_num
        self.recv_time = recv_time
        self.process_ms = process_ms
        self.sc16_buf = sc16_buf


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
    ) -> None:
        self._receiver = receiver
        self._db = database
        self._storage = local_storage
        self._settings = settings
        self._broadcast = broadcast
        self._zms_monitor = zms_monitor
        self._running = False

        total_cores = os.cpu_count() or 4
        self._num_proc_workers = max(1, total_cores - 3)
        self._fft_workers = 1

        # Compute chunk sizing from current settings
        self._recompute_chunk_params()

        # Inter-thread queues (these survive reconfiguration)
        self._chunk_queue: queue.Queue[Any] = queue.Queue(maxsize=4)
        self._burst_queue: queue.Queue[Any] = queue.Queue(maxsize=16)
        self._dropped_chunks = 0
        self._result_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=8)
        self._loop: asyncio.AbstractEventLoop | None = None

        # Recording state machine: "idle" | "armed" | "recording"
        self._recording_state: str = "idle"
        self._recording_file: str | None = None
        self._recording_bytes: int = 0
        self._recording_start: float = 0.0
        self._recording_dropped: int = 0
        self._trigger_initiated: bool = False  # True if trigger fired (vs manual)
        self._below_threshold_count = 0

        # Disk-streaming write queue (default mode)
        self._recording_queue: queue.Queue[Any] = queue.Queue(maxsize=64)
        self._writer_thread: threading.Thread | None = None

        # RAM-buffered recording (when RECORDING_RAM_BUFFER=True)
        self._recording_buf: np.ndarray[Any, np.dtype[Any]] | None = None
        self._recording_buf_pos: int = 0

        # PSD grid accumulation during recording (for .npz companion file)
        self._recording_grids: list[np.ndarray[Any, np.dtype[Any]]] = []
        self._recording_freq_axis: np.ndarray[Any, np.dtype[Any]] | None = None
        self._recording_time_res: float = 0.0

        self._capture_count = 0

        # Active bursts for WebSocket overlay (written by burst thread, read by consumer)
        self._active_bursts: list[dict[str, object]] = []

        # Burst results from burst thread -> event loop
        self._burst_result_queue: asyncio.Queue[list[BurstFingerprint] | None] = asyncio.Queue(
            maxsize=32
        )

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

        # Buffer pool: 12 pre-allocated SC16 (int32) buffers
        self._buf_pool: queue.Queue[np.ndarray[Any, np.dtype[Any]]] = queue.Queue(maxsize=12)
        for _ in range(12):
            self._buf_pool.put(np.zeros(self._chunk_samples, dtype=np.int32))

        # Pre-trigger circular buffer (int32 = SC16)
        pre_trigger_samples = int(s.TRIGGER_PRE_SEC * s.BANDWIDTH)
        self._pre_trigger_buf = CircularBuffer(max(1, pre_trigger_samples), dtype=np.int32)

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

        recv_thread = threading.Thread(target=self._receiver_loop, name="recv", daemon=True)
        dispatch_thread = threading.Thread(target=self._dispatch_loop, name="dispatch", daemon=True)
        burst_thread = threading.Thread(
            target=self._burst_detection_loop, name="burst", daemon=True
        )

        recv_thread.start()
        dispatch_thread.start()
        burst_thread.start()

        try:
            await self._result_consumer_loop()
        finally:
            self._running = False
            # Stop any active recording
            if self._recording_state == "recording":
                self._end_recording()
            self._recording_state = "idle"
            # Unblock threads
            self._chunk_queue.put(_STOP)
            self._burst_queue.put(_STOP)
            recv_thread.join(timeout=5)
            dispatch_thread.join(timeout=5)
            burst_thread.join(timeout=5)

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

    def start_recording(self) -> None:
        """Start recording IQ data immediately (manual mode)."""
        if self._recording_state == "recording":
            return
        self._trigger_initiated = False
        self._begin_recording()

    def arm_trigger(self) -> None:
        """Arm the power trigger — recording starts when threshold is exceeded."""
        if self._recording_state == "recording":
            return
        self._recording_state = "armed"
        logger.info("Trigger armed (threshold=%.1f dB)", self._settings.TRIGGER_THRESHOLD_DB)

    def stop_recording(self) -> None:
        """Stop recording or disarm trigger, return to idle."""
        if self._recording_state == "recording":
            self._end_recording()
        self._recording_state = "idle"
        self._trigger_initiated = False
        logger.info("Recording stopped / trigger disarmed")

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
        }

    # Backward-compat aliases for existing /api/trigger endpoints
    def manual_trigger(self) -> None:
        self.start_recording()

    def stop_trigger(self) -> None:
        self.stop_recording()

    # -- Receiver thread --

    def _receiver_loop(self) -> None:
        """Runs on a dedicated thread.  Calls recv_chunk() and feeds queues."""
        s = self._settings
        recv_count = 0
        my_gen = self._config_generation

        try:
            while self._running:
                # Check for reconfiguration before each frequency sweep.
                # Hardware reconfigure + chunk param rebuild happens here,
                # safely between stream stop/start.
                if self._config_generation != my_gen:
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

                    self._receiver.start_streaming(center_freq)

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

                        # Store raw SC16 in pre-trigger buffer
                        self._pre_trigger_buf.write(buf[:n])

                        # Feed upstream modules (GPU processing, non-blocking)
                        if self._module_manager is not None:
                            self._module_manager.feed_all(buf[:n], center_freq, s.BANDWIDTH)

                        # Handle recording / trigger
                        self._check_trigger_and_record(buf[:n])

                        # Enqueue for processing — best-effort, drop if behind
                        try:
                            self._chunk_queue.put_nowait((buf, recv_time))
                        except queue.Full:
                            self._dropped_chunks += 1
                            with contextlib.suppress(queue.Full):
                                self._buf_pool.put_nowait(buf)

                        recv_count += 1
                        if recv_count % 50 == 0:
                            recv_ms = (t_recv_done - recv_time) * 1000
                            logger.info(
                                "TIMING recv#%d: recv=%.1fms dropped=%d (IQ=%.1fms)",
                                recv_count,
                                recv_ms,
                                self._dropped_chunks,
                                self._chunk_duration * 1000,
                            )

                    self._receiver.stop_streaming()

        except Exception:
            logger.exception("Receiver loop crashed")
        finally:
            logger.info("Receiver loop exiting (running=%s)", self._running)
            self._chunk_queue.put(_STOP)

    def _check_trigger_and_record(self, sc16_buf: np.ndarray[Any, np.dtype[Any]]) -> None:
        """Handle recording and trigger logic for each chunk."""
        state = self._recording_state

        if state == "recording":
            self._write_recording_chunk(sc16_buf)

            # Auto-stop on max duration
            max_sec = self._settings.RECORDING_MAX_SEC
            if max_sec > 0 and (time.monotonic() - self._recording_start) >= max_sec:
                self.stop_recording()
                return

            # Auto-stop for trigger-initiated recordings when power drops
            if self._trigger_initiated:
                if not self._check_power_above_threshold(sc16_buf):
                    self._below_threshold_count += 1
                    if self._below_threshold_count >= self._settings.TRIGGER_HYSTERESIS:
                        self.stop_recording()
                else:
                    self._below_threshold_count = 0
            return

        # If armed, check threshold to start recording
        if state == "armed" and self._check_power_above_threshold(sc16_buf):
            self._trigger_initiated = True
            self._begin_recording()
            self._write_recording_chunk(sc16_buf)

    def _check_power_above_threshold(self, sc16_buf: np.ndarray[Any, np.dtype[Any]]) -> bool:
        """Fast subsampled power estimate from raw SC16 data."""
        raw16 = sc16_buf.view(np.int16).reshape(-1, 2)
        step = max(1, len(raw16) // 4096)
        sub = raw16[::step].astype(np.float32) / 32768.0
        power_sq = sub[:, 0] ** 2 + sub[:, 1] ** 2
        mean_power_db = float(10.0 * np.log10(np.mean(power_sq) + 1e-30))
        return mean_power_db > self._settings.TRIGGER_THRESHOLD_DB

    def _write_recording_chunk(self, sc16_buf: np.ndarray[Any, np.dtype[Any]]) -> None:
        """Write a chunk to the active recording (RAM buffer or disk queue)."""
        n = len(sc16_buf)

        if self._recording_buf is not None:
            # RAM-buffered mode
            end = self._recording_buf_pos + n
            if end <= len(self._recording_buf):
                self._recording_buf[self._recording_buf_pos : end] = sc16_buf
                self._recording_buf_pos = end
                self._recording_bytes = end * 4  # int32 = 4 bytes
            else:
                self._recording_dropped += 1
                logger.warning("RAM buffer full — dropped chunk")
        else:
            # Disk-streaming mode — convert to bytes on receiver thread
            # (.tobytes() is a fast C-level copy that plays well with GIL)
            try:
                self._recording_queue.put_nowait(sc16_buf.tobytes())
                self._recording_bytes += n * 4
            except queue.Full:
                self._recording_dropped += 1
                logger.warning("Recording queue full — dropped chunk")

    def _begin_recording(self) -> None:
        """Start recording: allocate RAM buffer or start disk writer."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self._recording_file = f"{self._receiver.serial}-{self._settings.HOSTNAME}-{ts}.sc16"
        self._recording_bytes = 0
        self._recording_dropped = 0
        self._recording_start = time.monotonic()
        self._below_threshold_count = 0
        self._recording_state = "recording"
        self._recording_grids = []
        self._recording_freq_axis = None
        self._recording_time_res = 0.0

        s = self._settings
        pre_data = self._pre_trigger_buf.read()

        if s.RECORDING_RAM_BUFFER:
            # Pre-allocate RAM for max recording duration
            max_sec = s.RECORDING_MAX_SEC if s.RECORDING_MAX_SEC > 0 else 30.0
            total_samples = int((s.TRIGGER_PRE_SEC + max_sec) * s.BANDWIDTH)
            self._recording_buf = np.zeros(total_samples, dtype=np.int32)
            self._recording_buf_pos = 0

            if len(pre_data) > 0:
                n = min(len(pre_data), total_samples)
                self._recording_buf[:n] = pre_data[:n]
                self._recording_buf_pos = n
                self._recording_bytes = n * 4

            logger.info(
                "Recording started (RAM): %s (%.1f MB allocated)",
                self._recording_file,
                total_samples * 4 / 1e6,
            )
        else:
            self._recording_buf = None
            self._recording_buf_pos = 0

            # Drain stale queue data
            while not self._recording_queue.empty():
                try:
                    self._recording_queue.get_nowait()
                except queue.Empty:
                    break

            if len(pre_data) > 0:
                try:
                    self._recording_queue.put_nowait(pre_data.tobytes())
                    self._recording_bytes = len(pre_data) * 4
                except queue.Full:
                    logger.warning("Recording queue full — pre-trigger dropped")

            self._writer_thread = threading.Thread(
                target=self._file_writer_loop, name="writer", daemon=True
            )
            self._writer_thread.start()
            logger.info("Recording started (disk): %s", self._recording_file)

    def _end_recording(self) -> None:
        """Stop recording, flush to disk, write metadata."""
        # Set state to idle FIRST so dispatch thread stops appending grids
        self._recording_state = "idle"
        # Brief sleep lets any in-flight _handle_chunk_result finish its append
        time.sleep(0.05)

        duration = time.monotonic() - self._recording_start
        base_name = self._recording_file or "recording.sc16"

        # Add drop count to filename if any chunks were lost
        if self._recording_dropped > 0:
            base_name = base_name.replace(".sc16", f"_drop{self._recording_dropped}.sc16")
            self._recording_file = base_name

        if self._recording_buf is not None:
            # RAM mode: flush buffer to disk
            filepath = self._storage.storage_path / base_name
            used = self._recording_buf[: self._recording_buf_pos]
            filepath.write_bytes(used.tobytes())
            self._recording_bytes = self._recording_buf_pos * 4
            self._recording_buf = None
            self._recording_buf_pos = 0
        else:
            # Disk mode: stop writer thread
            self._recording_queue.put(None)
            if self._writer_thread is not None:
                self._writer_thread.join(timeout=10)
                self._writer_thread = None

            # Rename file if drops occurred
            if self._recording_dropped > 0:
                orig_name = base_name.replace(f"_drop{self._recording_dropped}.sc16", ".sc16")
                orig = self._storage.storage_path / orig_name
                dest = self._storage.storage_path / base_name
                if orig.exists():
                    orig.rename(dest)

        # Save PSD grid data as .npz companion
        grids = self._recording_grids
        self._recording_grids = []
        if grids and self._recording_freq_axis is not None:
            full_grid = np.concatenate(grids, axis=0)
            npz_path = self._storage.storage_path / base_name.replace(".sc16", ".npz")
            np.savez_compressed(
                npz_path,
                grid=full_grid,
                freq_axis=self._recording_freq_axis,
                time_resolution_s=np.float64(self._recording_time_res),
                center_freq_hz=np.int64(self._settings.FREQUENCY_START),
                bandwidth_hz=np.int64(self._settings.BANDWIDTH),
            )
            logger.info("PSD data saved: %s (%d rows)", npz_path.name, full_grid.shape[0])

        # Write companion metadata JSON
        self._write_recording_metadata(base_name, duration)

        logger.info(
            "Recording saved: %s (%d bytes, %.1fs, %d dropped)",
            base_name,
            self._recording_bytes,
            duration,
            self._recording_dropped,
        )

    def _write_recording_metadata(self, filename: str, duration: float) -> None:
        """Write companion .json with capture metadata."""
        import json as _json

        s = self._settings
        meta = {
            "file": filename,
            "format": "sc16",
            "sample_rate_hz": s.BANDWIDTH,
            "center_freq_hz": s.FREQUENCY_START,
            "bandwidth_hz": s.BANDWIDTH,
            "gain_db": s.GAIN,
            "start_time": datetime.fromtimestamp(
                time.time() - duration, tz=timezone.utc
            ).isoformat(),
            "duration_sec": round(duration, 3),
            "total_bytes": self._recording_bytes,
            "total_samples": self._recording_bytes // 4,
            "dropped_chunks": self._recording_dropped,
            "pre_trigger_sec": s.TRIGGER_PRE_SEC,
            "trigger_initiated": self._trigger_initiated,
            "ram_buffered": s.RECORDING_RAM_BUFFER,
            "hostname": s.HOSTNAME,
            "serial": self._receiver.serial,
        }
        json_path = self._storage.storage_path / filename.replace(".sc16", ".json")
        json_path.write_text(_json.dumps(meta, indent=2))

    def _file_writer_loop(self) -> None:
        """Dedicated thread: drains recording queue and writes to disk.

        Pinned to the last CPU core so PSD workers can't starve it.
        """
        # Pin writer to dedicated core (last core) for guaranteed CPU time
        try:
            total_cores = os.cpu_count() or 6
            writer_core = total_cores - 1
            os.sched_setaffinity(0, {writer_core})
            logger.info("Writer thread pinned to core %d", writer_core)
        except OSError:
            logger.debug("Could not pin writer thread to core")

        filepath = self._storage.storage_path / (self._recording_file or "recording.sc16")
        try:
            with open(filepath, "wb", buffering=8 * 1024 * 1024) as f:
                while True:
                    data = self._recording_queue.get()
                    if data is None:
                        break
                    f.write(data)
                    # No flush — let OS buffer writes for throughput.
                    # Data is flushed on file close in _end_recording.
        except Exception:
            logger.exception("File writer crashed")

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
                    item = self._chunk_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is _STOP:
                    break

                sc16_buf, recv_time = item
                capture_num += 1

                # Drop chunk if too many in flight (backpressure)
                if len(pending_futures) >= max_inflight:
                    self._dropped_chunks += 1
                    with contextlib.suppress(queue.Full):
                        self._buf_pool.put_nowait(sc16_buf)
                    continue

                future = executor.submit(
                    self._process_one_chunk,
                    sc16_buf,
                    recv_time,
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
            self._burst_queue.put(_STOP)

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

        iq_stats = calculate_iq_statistics(complex_chunk)
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
            process_ms=process_ms,
            sc16_buf=sc16_buf,
        )

    def _handle_chunk_result(self, cr: _ChunkResult) -> None:
        """Called from dispatch thread after a worker finishes."""
        with contextlib.suppress(queue.Full):
            self._buf_pool.put_nowait(cr.sc16_buf)

        with contextlib.suppress(queue.Full):
            self._burst_queue.put_nowait((cr.psd_grid, cr.center_freq_hz, cr.capture_num))

        # Accumulate PSD grids during recording (for .npz companion file)
        if self._recording_state == "recording":
            self._recording_grids.append(cr.psd_grid.grid.copy())
            self._recording_freq_axis = cr.psd_grid.freq_axis
            if len(cr.psd_grid.time_axis) > 1:
                self._recording_time_res = float(
                    cr.psd_grid.time_axis[1] - cr.psd_grid.time_axis[0]
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
        )

        if self._loop is not None:
            with contextlib.suppress(asyncio.QueueFull):
                self._loop.call_soon_threadsafe(self._result_queue.put_nowait, result)

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

                completed_bursts = rolling_detector.feed(psd_grid)

                if completed_bursts and self._loop is not None:
                    self._loop.call_soon_threadsafe(
                        self._burst_result_queue.put_nowait, completed_bursts
                    )

                # Build active burst overlay data for WebSocket.
                # Send freq as fractional position [0,1] and time as
                # seconds so the frontend can map to any canvas size
                # and update rate.
                detection = rolling_detector.last_detection
                if detection and detection.bursts:
                    freq_axis = psd_grid.freq_axis
                    num_bins = len(freq_axis)
                    time_res = rolling_detector._time_resolution_s
                    window_dur = rolling_detector._rows_filled * time_res
                    active: list[dict[str, object]] = []
                    for b in detection.bursts:
                        f_lo = b.center_freq_hz - b.bandwidth_hz / 2 - center_freq
                        f_hi = b.center_freq_hz + b.bandwidth_hz / 2 - center_freq
                        bin_start = int(np.searchsorted(freq_axis, f_lo))
                        bin_end = int(np.searchsorted(freq_axis, f_hi))
                        # Time: how many seconds ago the burst ended
                        t_end = (b.stop_time - b.detection_timestamp).total_seconds()
                        age_sec = max(0.0, window_dur - t_end)
                        active.append(
                            {
                                "center_freq_hz": b.center_freq_hz,
                                "bandwidth_hz": b.bandwidth_hz,
                                "peak_power_db": round(b.peak_power_db, 1),
                                "duration_ms": round(b.duration_ms, 2),
                                "freq_start": bin_start / num_bins,
                                "freq_end": min(1.0, bin_end / num_bins),
                                "age_sec": round(age_sec, 4),
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
        accum_start = time.monotonic()
        last_result: _StreamResult | None = None

        while self._running:
            try:
                result = await asyncio.wait_for(self._result_queue.get(), timeout=0.5)
            except TimeoutError:
                await self._drain_burst_results()
                # Flush accumulator on timeout if data pending
                if accum_powers and last_result is not None:
                    avg = _np.mean(accum_powers, axis=0).tolist()
                    await self._broadcast_averaged(avg, last_result, len(accum_powers))
                    await self._submit_zms(avg, last_result)
                    accum_powers.clear()
                    accum_start = time.monotonic()
                    last_result = None
                continue

            await self._drain_burst_results()

            if result is _STOP:
                break

            # --- Accumulate for normal-mode UI + ZMS ---
            accum_powers.append(result.summary_psd.powers)
            last_result = result

            elapsed = time.monotonic() - accum_start
            if elapsed >= self._settings.DURATION_SEC:
                avg = _np.mean(accum_powers, axis=0).tolist()

                # Normal-mode UI broadcast (only if no high-res subscribers)
                if self._broadcast is not None and not self._broadcast.has_high_res_subscribers():
                    await self._broadcast_averaged(avg, result, len(accum_powers))

                # ZMS always gets DURATION_SEC averaged data
                await self._submit_zms(avg, result)

                accum_powers.clear()
                accum_start = time.monotonic()
                last_result = None

            # --- High-res UI broadcast (every chunk) ---
            if self._broadcast is not None and self._broadcast.has_high_res_subscribers():
                await self._broadcast.publish(
                    {
                        "type": "psd",
                        "center_freq_hz": result.center_freq_hz,
                        "bandwidth_hz": self._settings.BANDWIDTH,
                        "powers": result.summary_psd.powers,
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
                    }
                )

    async def _broadcast_averaged(
        self,
        avg_powers: list[float],
        result: _StreamResult,
        chunk_count: int,
    ) -> None:
        """Broadcast a DURATION_SEC-averaged PSD to the UI."""
        if self._broadcast is None:
            return
        await self._broadcast.publish(
            {
                "type": "psd",
                "center_freq_hz": result.center_freq_hz,
                "bandwidth_hz": self._settings.BANDWIDTH,
                "powers": avg_powers,
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
                "chunks_averaged": chunk_count,
            }
        )

    async def _submit_zms(self, avg_powers: list[float], result: _StreamResult) -> None:
        """Submit DURATION_SEC-averaged observation to ZMS."""
        if self._zms_monitor is None:
            return
        try:
            from pathlib import Path

            from rfobserver.models import MetadataRecord, ProcessedDataEnvelope, PSDData

            averaged_psd = PSDData(
                powers=avg_powers,
                frequencies=result.summary_psd.frequencies,
                center_freq=result.summary_psd.center_freq,
                sample_rate=result.summary_psd.sample_rate,
                num_bins=result.summary_psd.num_bins,
            )
            meta = MetadataRecord(
                hostname=self._settings.HOSTNAME,
                organization=self._settings.ORGANIZATION,
                serial=self._receiver.serial,
                frequency=result.center_freq_hz,
                timestamp=datetime.now(timezone.utc),
                source_path=Path("/tmp/rfobserver/streaming"),
                gain=self._settings.GAIN,
                sampling_rate=self._settings.BANDWIDTH,
            )
            envelope = ProcessedDataEnvelope(
                metadata=meta,
                statistics=result.iq_stats,
                psd_data=averaged_psd,
            )
            ok = await self._zms_monitor.submit_observation(envelope)
            if ok:
                logger.debug("ZMS observation submitted (chunk #%d)", result.capture_num)
        except Exception:
            logger.exception("ZMS observation submission failed")

    async def _drain_burst_results(self) -> None:
        """Process all pending burst results from the burst detection thread."""
        while True:
            try:
                bursts = self._burst_result_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if bursts is None:
                break

            for burst in bursts:
                await self._db.insert_detection(
                    burst_id=burst.burst_id,
                    start_time=burst.start_time,
                    stop_time=burst.stop_time,
                    center_freq_hz=burst.center_freq_hz,
                    bandwidth_hz=burst.bandwidth_hz,
                    peak_power_db=burst.peak_power_db,
                    duration_ms=burst.duration_ms,
                    detection_timestamp=burst.detection_timestamp,
                )

            if bursts:
                logger.info("Detected %d bursts", len(bursts))

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
