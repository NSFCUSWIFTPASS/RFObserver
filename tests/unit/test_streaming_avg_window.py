"""Averaged-window persistence runs for every live window, independent of sinks."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from rfobserver.config import AppSettings
from rfobserver.models import IQStatistics

_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _proc(tmp_path, *, replay_mode=False, with_sinks=True):
    from rfobserver.pipeline.streaming import StreamingProcessor

    settings = AppSettings(
        _env_file=None, STORAGE_PATH=str(tmp_path), DB_PATH=str(tmp_path / "d.db")
    )
    db = MagicMock()
    db.insert_avg_window = AsyncMock()
    storage = MagicMock()
    storage.storage_path = tmp_path
    storage.auto_dir = tmp_path / "auto"
    storage.manual_dir = tmp_path / "manual"
    storage.auto_dir.mkdir(exist_ok=True)
    storage.manual_dir.mkdir(exist_ok=True)
    receiver = MagicMock()
    receiver.serial = "sim0"
    proc = StreamingProcessor(
        receiver=receiver,
        database=db,
        local_storage=storage,
        settings=settings,
        broadcast=None,
        zms_monitor=(MagicMock() if with_sinks else None),
        replay_mode=replay_mode,
    )
    return proc, db


def _result():
    summary = SimpleNamespace(
        powers=[-80.0, -70.0, -60.0, -50.0],
        frequencies=[2.409e9, 2.423e9, 2.437e9, 2.451e9],
        center_freq=2.437e9,
        sample_rate=56_000_000,
        num_bins=4,
    )
    return SimpleNamespace(summary_psd=summary, center_freq_hz=2_437_000_000, capture_num=1)


def _stats():
    return IQStatistics(average=-70.0, max=-50.0, median=-72.0, std=3.0, kurtosis=1.2)


@pytest.mark.asyncio
async def test_persist_avg_window_inserts_expected_fields(tmp_path):
    proc, db = _proc(tmp_path)
    await proc._persist_avg_window(
        [-80.0, -70.0, -60.0, -50.0], _result(), _stats(), start_time=_T0, duration_sec=0.537
    )
    db.insert_avg_window.assert_called_once()
    kwargs = db.insert_avg_window.call_args.kwargs
    assert kwargs["num_bins"] == 4
    assert kwargs["sdr_center_freq_hz"] == 2_437_000_000.0
    assert kwargs["freq_start_hz"] == pytest.approx(2.409e9)
    assert kwargs["freq_step_hz"] == pytest.approx(0.014e9, rel=1e-6)
    assert kwargs["pwr_avg"] == -70.0
    assert kwargs["powers"] == [-80.0, -70.0, -60.0, -50.0]
    # The caller's measured span is stored as-is (no clock read, no DURATION_SEC).
    assert kwargs["start_time"] == _T0
    assert kwargs["duration_sec"] == 0.537


@pytest.mark.asyncio
async def test_publish_persists_even_with_no_sinks(tmp_path):
    proc, db = _proc(tmp_path, with_sinks=False)
    await proc._publish_processed(
        [-80.0, -70.0, -60.0, -50.0], _result(), _stats(), start_time=_T0, duration_sec=0.5
    )
    await _drain(proc)
    db.insert_avg_window.assert_called_once()


@pytest.mark.asyncio
async def test_replay_mode_skips_avg_window_persist(tmp_path):
    proc, db = _proc(tmp_path, replay_mode=True)
    await proc._publish_processed(
        [-80.0, -70.0, -60.0, -50.0], _result(), _stats(), start_time=_T0, duration_sec=0.5
    )
    db.insert_avg_window.assert_not_called()


# --- window timing: real start + measured duration, windows tile (2026-09-28) ---
#
# The consumer loop used to store every window with duration_sec=DURATION_SEC and
# start_time=now() at persist (the window's END), and reset its accumulation clock
# only after the awaited persist. Windows were spaced wider than their stored
# duration, which the raw-mode waterfall painted as dark stripes. See
# docs/debugging/2026-09-28_averaged-waterfall-stripes.md.

_BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _FakeClock:
    def __init__(self) -> None:
        self.t = 0.0


def _install_fake_clock(monkeypatch, clock: _FakeClock) -> None:
    """Patch the streaming module's ``time`` and ``datetime`` names only, so the
    event loop keeps its own real clock."""
    import time as _real_time

    from rfobserver.pipeline import streaming

    class _FakeTime:
        def __getattr__(self, name):
            return getattr(_real_time, name)

        @staticmethod
        def monotonic() -> float:
            return 1000.0 + clock.t

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return _BASE + timedelta(seconds=clock.t)

    monkeypatch.setattr(streaming, "time", _FakeTime())
    monkeypatch.setattr(streaming, "datetime", _FakeDatetime)


class _ScheduledQueue:
    """Stands in for _result_queue: result i arrives at arrivals[i] seconds.
    A get() moves the clock to max(now, arrival), so results that arrived while
    the consumer was busy are consumed back-to-back, as with the real queue."""

    def __init__(self, clock: _FakeClock, arrivals: list[float], item_factory) -> None:
        self._clock = clock
        self._items = [(a, item_factory()) for a in arrivals]

    async def get(self):
        from rfobserver.pipeline.streaming import _STOP

        if not self._items:
            return _STOP
        arrival, item = self._items.pop(0)
        self._clock.t = max(self._clock.t, arrival)
        return item


def _stream_result():
    import numpy as np

    from rfobserver.processing.iq_utils import moments_from_iq

    r = _result()
    r.iq_moments = moments_from_iq(np.ones(64, dtype=np.complex64))
    return r


async def _drain(proc) -> None:
    """Wait for the processor's background DB writes (a no-op before they existed)."""
    writer = getattr(proc, "_db_writer", None)
    if writer is not None:
        await writer.drain(5.0)


async def _run_loop(proc, clock: _FakeClock, arrivals: list[float]) -> None:
    proc._result_queue = _ScheduledQueue(clock, arrivals, _stream_result)
    proc._running = True
    await proc._result_consumer_loop()
    await _drain(proc)


@pytest.mark.asyncio
async def test_stored_windows_have_real_start_and_duration_and_tile(tmp_path, monkeypatch):
    clock = _FakeClock()
    _install_fake_clock(monkeypatch, clock)
    proc, db = _proc(tmp_path, with_sinks=False)
    assert proc._settings.DURATION_SEC == 0.5

    stored: list[dict] = []

    async def insert(**kw):
        # The insert runs on the background writer, concurrently with the loop,
        # so its cost does not move the loop's clock (see
        # test_slow_db_insert_does_not_bend_windows_or_drop_results).
        stored.append(kw)

    db.insert_avg_window = insert
    # One result every 0.1 s, the first at t=0.1.
    await _run_loop(proc, clock, [0.1 * (i + 1) for i in range(40)])

    assert len(stored) >= 5
    # First window starts when its first result arrives and closes on the first
    # result at or past DURATION_SEC (t=0.6).
    assert stored[0]["start_time"] == _BASE + timedelta(seconds=0.1)
    assert stored[0]["duration_sec"] == pytest.approx(0.5)
    for prev, nxt in zip(stored, stored[1:], strict=False):
        prev_end = prev["start_time"] + timedelta(seconds=prev["duration_sec"])
        # Tiling: the next window starts where the previous one ended.
        assert abs((nxt["start_time"] - prev_end).total_seconds()) < 1e-6
        # Duration is measured, not nominal, and never shorter than nominal.
        assert prev["duration_sec"] >= 0.5 - 1e-9


@pytest.mark.asyncio
async def test_flushed_window_ends_at_its_last_result(tmp_path, monkeypatch):
    """The idle-flush path stores the window's real span too: it ends at the
    last result's arrival, not at the flush 0.5 s later."""
    clock = _FakeClock()
    _install_fake_clock(monkeypatch, clock)
    proc, db = _proc(tmp_path, with_sinks=False)
    stored: list[dict] = []

    async def insert(**kw):
        stored.append(kw)

    db.insert_avg_window = insert

    class _IdleThenStop(_ScheduledQueue):
        def __init__(self, *a):
            super().__init__(*a)
            self._idled = False

        async def get(self):
            if not self._items and not self._idled:
                self._idled = True
                clock.t += 0.5
                raise asyncio.TimeoutError
            return await super().get()

    proc._result_queue = _IdleThenStop(clock, [0.1, 0.2, 0.3], _stream_result)
    proc._running = True
    await proc._result_consumer_loop()
    await _drain(proc)

    assert len(stored) == 1
    assert stored[0]["start_time"] == _BASE + timedelta(seconds=0.1)
    assert stored[0]["duration_sec"] == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_detection_joins_the_window_whose_real_span_contains_it(tmp_path, monkeypatch):
    """With real start/duration stored, detections_for_window returns a burst
    for the window it happened in, not the next (or previous) one."""
    from rfobserver.storage.database import SensorDatabase

    clock = _FakeClock()
    _install_fake_clock(monkeypatch, clock)
    proc, _ = _proc(tmp_path, with_sinks=False)
    real_db = SensorDatabase(str(tmp_path / "join.db"))
    await real_db.connect()
    try:
        proc._db = real_db
        await _run_loop(proc, clock, [0.1 * (i + 1) for i in range(20)])

        windows = sorted(await real_db.query_avg_windows(limit=100), key=lambda w: w["start_time"])
        assert len(windows) >= 3
        full = [await real_db.get_avg_window(w["id"]) for w in windows]
        s = proc._settings

        def _det(bid: str, at: datetime) -> dict:
            return dict(
                burst_id=bid,
                start_time=at,
                stop_time=at + timedelta(milliseconds=5),
                center_freq_hz=2.437e9,
                bandwidth_hz=1e6,
                peak_power_db=-30.0,
                duration_ms=5.0,
                detection_timestamp=at,
                sdr_center_freq_hz=2_437_000_000.0,
                sample_rate_hz=float(s.BANDWIDTH),
                gain_db=float(s.GAIN),
            )

        # A burst in the middle of each of the first two windows' real spans.
        for k in (0, 1):
            w0 = datetime.fromisoformat(full[k]["start_time"])
            mid = w0 + timedelta(seconds=float(full[k]["duration_sec"]) / 2)
            await real_db.insert_detection(**_det(f"in-w{k}", mid))

        ids = [{d["burst_id"] for d in await real_db.detections_for_window(w)} for w in full[:3]]
        assert ids[0] == {"in-w0"}
        assert ids[1] == {"in-w1"}
        assert ids[2] == set()
        # And the real spans: window k starts at t=0.1 + 0.5 k.
        for k in (0, 1, 2):
            got = datetime.fromisoformat(full[k]["start_time"])
            assert abs((got - (_BASE + timedelta(seconds=0.1 + 0.5 * k))).total_seconds()) < 1e-6
    finally:
        await real_db.close()


# --- review follow-up: no window spans an outage or a retune ---


class _TimeoutQueue(_ScheduledQueue):
    """Like _ScheduledQueue, but emulates wait_for's 0.5 s timeout while the
    next arrival is more than 0.5 s away."""

    async def get(self):
        if self._items and self._items[0][0] - self._clock.t > 0.5:
            self._clock.t += 0.5
            raise asyncio.TimeoutError
        return await super().get()


def _spans(stored: list[dict]) -> list[tuple[float, float]]:
    return [
        (round((s["start_time"] - _BASE).total_seconds(), 6), round(s["duration_sec"], 6))
        for s in stored
    ]


async def _collect(tmp_path, monkeypatch, queue_cls, arrivals, factory=_stream_result):
    clock = _FakeClock()
    _install_fake_clock(monkeypatch, clock)
    proc, db = _proc(tmp_path, with_sinks=False)
    stored: list[dict] = []

    async def insert(**kw):
        stored.append(kw)

    db.insert_avg_window = insert
    proc._result_queue = queue_cls(clock, arrivals, factory)
    proc._running = True
    await proc._result_consumer_loop()
    await _drain(proc)
    return stored


@pytest.mark.asyncio
async def test_window_after_an_outage_starts_when_results_resume(tmp_path, monkeypatch):
    """Results at 0.1-0.6 s, then nothing until 10.0 s. The window opened by
    the close at 0.6 s must not claim the outage (it used to store (0.6, 9.4))."""
    arrivals = [0.1 * (i + 1) for i in range(6)] + [10.0 + 0.1 * i for i in range(8)]
    stored = await _collect(tmp_path, monkeypatch, _TimeoutQueue, arrivals)
    assert _spans(stored) == [(0.1, 0.5), (10.0, 0.5)]


@pytest.mark.asyncio
async def test_short_stall_after_a_close_does_not_stretch_the_next_window(tmp_path, monkeypatch):
    """A 0.35 s stall right after a close is under the 0.5 s idle timeout but
    over the gap limit (max(0.2 s, 4 chunks)): the next window starts at the
    first result after the stall."""
    arrivals = [0.1 * (i + 1) for i in range(6)] + [0.95 + 0.1 * i for i in range(8)]
    stored = await _collect(tmp_path, monkeypatch, _TimeoutQueue, arrivals)
    assert _spans(stored)[:2] == [(0.1, 0.5), (0.95, 0.5)]


@pytest.mark.asyncio
async def test_retune_ends_the_window_at_the_old_tunings_last_result(tmp_path, monkeypatch):
    """A center-frequency change flushes the pending window (ending at its last
    result) instead of mixing tunings; the new tuning starts its own window."""
    tunings = iter([915_000_000] * 3 + [2_437_000_000] * 8)

    def factory():
        r = _stream_result()
        r.center_freq_hz = next(tunings)
        return r

    arrivals = [0.1 * (i + 1) for i in range(11)]
    stored = await _collect(tmp_path, monkeypatch, _ScheduledQueue, arrivals, factory)
    assert _spans(stored) == [(0.1, 0.2), (0.4, 0.5)]
    assert [s["sdr_center_freq_hz"] for s in stored] == [915e6, 2437e6]


@pytest.mark.asyncio
async def test_tone_check_is_stamped_with_the_window_start(tmp_path, monkeypatch):
    clock = _FakeClock()
    _install_fake_clock(monkeypatch, clock)
    proc, db = _proc(tmp_path, with_sinks=False)
    proc._settings.TONE_CHECK_ENABLED = True
    db.insert_tone_check = AsyncMock()
    stored: list[dict] = []

    async def insert(**kw):
        stored.append(kw)

    db.insert_avg_window = insert
    await _run_loop(proc, clock, [0.1 * (i + 1) for i in range(6)])
    assert db.insert_tone_check.call_args.kwargs["timestamp"] == stored[0]["start_time"]


# --- per-window DB writes run off the averaging loop (2026-09-29) ---
#
# The consumer loop used to await the avg-window insert (and the tone-check
# insert) inline. aiosqlite runs every statement on the writer connection
# through one thread, so a slow insert stalled the loop: results piled up in
# the bounded result queue (8) and were dropped, and each window averaged fewer
# chunks. The writes now go through one ordered background writer.

_CHUNK = 0.0366  # 2,048,000 samples at 56 Msps, the default chunk


class _CappedQueue:
    """The real result queue's shape: bounded (8), a result arriving while it
    is full is dropped. Results arrive every chunk. Fake time advances only
    while the consumer waits in get(), and get() yields once so concurrent
    tasks (the DB writer) run while it waits."""

    def __init__(self, clock: _FakeClock, arrivals: list[float], cap: int = 8) -> None:
        self._clock = clock
        self._pending = list(arrivals)
        self._buf: list[float] = []
        self._cap = cap
        self.taken = 0
        self.dropped = 0

    def _admit(self) -> None:
        while self._pending and self._pending[0] <= self._clock.t:
            a = self._pending.pop(0)
            if len(self._buf) < self._cap:
                self._buf.append(a)
            else:
                self.dropped += 1

    async def get(self):
        from rfobserver.pipeline.streaming import _STOP

        await asyncio.sleep(0)
        self._admit()
        if not self._buf:
            if not self._pending:
                return _STOP
            nxt = self._pending[0]
            if nxt - self._clock.t > 0.5:
                self._clock.t += 0.5
                raise asyncio.TimeoutError
            self._clock.t = nxt
            self._admit()
        self._buf.pop(0)
        self.taken += 1
        return _stream_result()


async def _run_with_insert_cost(tmp_path, monkeypatch, cost: float, *, writer_max=None, out=None):
    """Run 60 s of results at one per chunk with DURATION_SEC=1.0 through the
    consumer loop, where each avg-window insert takes ``cost`` seconds.

    An insert awaited by the consumer task itself blocks the loop, so it moves
    fake time on by ``cost``. An insert on another task (the background writer)
    runs concurrently: it finishes once fake time has passed ``cost``.

    ``out`` (a dict), if given, receives ``closed`` (every window start the
    loop published), ``written`` (window starts whose insert finished) and
    ``proc``."""
    from rfobserver.pipeline.streaming import _OrderedDbWriter

    clock = _FakeClock()
    _install_fake_clock(monkeypatch, clock)
    proc, db = _proc(tmp_path, with_sinks=False)
    proc._settings.DURATION_SEC = 1.0
    if writer_max is not None:
        proc._db_writer = _OrderedDbWriter(maxsize=writer_max)
    stored: list[dict] = []
    written: list[datetime] = []
    closed: list[datetime] = []
    consumer: asyncio.Task | None = None

    async def insert(**kw):
        stored.append(kw)
        if asyncio.current_task() is consumer:
            clock.t += cost
        else:
            deadline = clock.t + cost
            while clock.t < deadline and consumer is not None and not consumer.done():
                await asyncio.sleep(0)
        written.append(kw["start_time"])

    orig_publish = proc._publish_processed

    async def publish(*a, **kw):
        closed.append(kw["start_time"])
        await orig_publish(*a, **kw)

    proc._publish_processed = publish  # type: ignore[method-assign]
    db.insert_avg_window = insert
    q = _CappedQueue(clock, [_CHUNK * (i + 1) for i in range(int(60 / _CHUNK))])
    proc._result_queue = q
    proc._running = True
    consumer = asyncio.create_task(proc._result_consumer_loop())
    await consumer
    if out is not None:
        out.update(closed=closed, written=written, proc=proc)
    await _drain(proc)
    return stored, q


@pytest.mark.asyncio
async def test_slow_db_insert_does_not_bend_windows_or_drop_results(tmp_path, monkeypatch):
    """The reviewer's scenario: 0.85 s per insert at DURATION_SEC=1.0."""
    base, base_q = await _run_with_insert_cost(tmp_path, monkeypatch, 0.0)
    slow, slow_q = await _run_with_insert_cost(tmp_path, monkeypatch, 0.85)

    assert slow_q.dropped == base_q.dropped
    assert abs(len(slow) - len(base)) <= 1
    # Every closed window (the last is a flush) lasts DURATION_SEC, plus at
    # most one chunk: the first result at or past it closes the window.
    for w in slow[:-1]:
        assert 1.0 - 1e-9 <= w["duration_sec"] <= 1.0 + _CHUNK + 1e-9
    # And averages as many chunks as with a free insert.
    assert slow_q.taken / len(slow) == pytest.approx(base_q.taken / len(base), rel=0.05)


@pytest.mark.asyncio
async def test_background_writes_keep_insertion_order(tmp_path):
    """Avg-window and tone-check inserts that take varying time still land in
    the order the windows closed."""
    import random

    proc, db = _proc(tmp_path, with_sinks=False)
    proc._settings.TONE_CHECK_ENABLED = True
    log: list[tuple[str, datetime]] = []
    rng = random.Random(7)

    async def avg(**kw):
        await asyncio.sleep(rng.choice([0.0, 0.001, 0.003]))
        log.append(("avg", kw["start_time"]))

    async def tone(**kw):
        await asyncio.sleep(rng.choice([0.0, 0.002]))
        log.append(("tone", kw["timestamp"]))

    db.insert_avg_window = avg
    db.insert_tone_check = tone
    expected = []
    for i in range(20):
        t = _T0 + timedelta(seconds=i)
        await proc._run_tone_check([-80.0, -70.0, -60.0, -50.0], _result(), t)
        await proc._publish_processed(
            [-80.0, -70.0, -60.0, -50.0], _result(), _stats(), start_time=t, duration_sec=1.0
        )
        expected += [("tone", t), ("avg", t)]
    await _drain(proc)
    assert log == expected


@pytest.mark.asyncio
async def test_full_write_queue_drops_counts_and_rate_limits_the_warning(
    tmp_path, monkeypatch, caplog
):
    import logging

    from rfobserver.pipeline.streaming import _OrderedDbWriter

    clock = _FakeClock()
    _install_fake_clock(monkeypatch, clock)
    proc, db = _proc(tmp_path, with_sinks=False)
    proc._db_writer = _OrderedDbWriter(maxsize=2)
    gate = asyncio.Event()
    written: list[datetime] = []

    async def blocked(**kw):
        await gate.wait()
        written.append(kw["start_time"])

    db.insert_avg_window = blocked
    caplog.set_level(logging.WARNING, logger="rfobserver.pipeline.streaming")

    def warnings() -> int:
        return sum("DB write queue full" in r.getMessage() for r in caplog.records)

    async def publish(i: int) -> None:
        # Never blocks, however far behind the writer is.
        await asyncio.wait_for(
            proc._publish_processed(
                [-80.0, -70.0, -60.0, -50.0],
                _result(),
                _stats(),
                start_time=_T0 + timedelta(seconds=i),
                duration_sec=1.0,
            ),
            timeout=0.5,
        )

    await publish(0)
    for _ in range(3):
        await asyncio.sleep(0)  # the writer takes window 0 and blocks on it
    for i in range(1, 6):  # 1 and 2 queue, 3-5 are dropped
        await publish(i)
    assert proc.db_writes_dropped == 3
    assert warnings() == 1
    clock.t += 30.0
    await publish(6)
    assert proc.db_writes_dropped == 4
    assert warnings() == 1  # rate-limited
    clock.t += 31.0
    await publish(7)
    assert proc.db_writes_dropped == 5
    assert warnings() == 2

    gate.set()
    await _drain(proc)
    assert written == [_T0, _T0 + timedelta(seconds=1), _T0 + timedelta(seconds=2)]


@pytest.mark.asyncio
async def test_drain_writes_everything_queued(tmp_path):
    proc, db = _proc(tmp_path, with_sinks=False)
    written: list[datetime] = []

    async def insert(**kw):
        await asyncio.sleep(0.005)
        written.append(kw["start_time"])

    db.insert_avg_window = insert
    times = [_T0 + timedelta(seconds=i) for i in range(10)]
    for t in times:
        await proc._publish_processed(
            [-80.0, -70.0, -60.0, -50.0], _result(), _stats(), start_time=t, duration_sec=1.0
        )
    assert await proc._db_writer.drain(5.0) == 0
    assert written == times


@pytest.mark.asyncio
async def test_drain_times_out_and_reports_discarded_writes(tmp_path, caplog):
    import logging

    proc, db = _proc(tmp_path, with_sinks=False)

    async def stuck(**kw):
        await asyncio.Event().wait()

    db.insert_avg_window = stuck
    for i in range(3):
        await proc._publish_processed(
            [-80.0, -70.0, -60.0, -50.0],
            _result(),
            _stats(),
            start_time=_T0 + timedelta(seconds=i),
            duration_sec=1.0,
        )
    await asyncio.sleep(0)
    caplog.set_level(logging.WARNING, logger="rfobserver.pipeline.streaming")
    # One in flight plus two queued.
    assert await proc._db_writer.drain(0.1) == 3
    assert any("discarded 3" in r.getMessage() for r in caplog.records)
    assert proc._db_writer.idle


@pytest.mark.asyncio
async def test_processor_stop_drains_pending_writes(tmp_path, monkeypatch):
    """run() drains the writer once the consumer loop exits, so the last
    windows before a stop are stored."""
    import threading as _real_threading

    from rfobserver.pipeline import streaming

    class _NoThread:
        def __init__(self, *a, **kw) -> None:
            pass

        def start(self) -> None:
            pass

        def join(self, timeout=None) -> None:
            pass

    class _Threading:
        Thread = _NoThread

        def __getattr__(self, name):
            return getattr(_real_threading, name)

    monkeypatch.setattr(streaming, "threading", _Threading())
    proc, db = _proc(tmp_path, with_sinks=False)
    written: list[datetime] = []

    async def insert(**kw):
        await asyncio.sleep(0.01)
        written.append(kw["start_time"])

    db.insert_avg_window = insert
    times = [_T0 + timedelta(seconds=i) for i in range(5)]

    async def loop() -> None:
        for t in times:
            await proc._publish_processed(
                [-80.0, -70.0, -60.0, -50.0], _result(), _stats(), start_time=t, duration_sec=1.0
            )

    proc._result_consumer_loop = loop  # type: ignore[method-assign]
    await asyncio.wait_for(proc.run(), timeout=10)
    assert written == times


@pytest.mark.asyncio
async def test_insert_slower_than_a_window_fills_the_writer_not_the_loop(tmp_path, monkeypatch):
    """2.5 s per insert at DURATION_SEC=1.0: the writer can never keep up. The
    loop's windows and result drops are unchanged; the writer's queue fills,
    drops are counted, and the rows that are written keep their order."""
    base, base_q = await _run_with_insert_cost(tmp_path, monkeypatch, 0.0, writer_max=8)
    out: dict = {}
    slow, slow_q = await _run_with_insert_cost(tmp_path, monkeypatch, 2.5, writer_max=8, out=out)
    assert slow_q.dropped == base_q.dropped
    assert slow_q.taken == base_q.taken
    assert len(out["closed"]) == len(base)
    for d in (w["duration_sec"] for w in base[:-1]):
        assert 1.0 - 1e-9 <= d <= 1.0 + _CHUNK + 1e-9
    proc = out["proc"]
    assert proc.db_writes_dropped > 0
    written = out["written"]
    # Every accepted write lands, in window order, and the dropped ones are
    # exactly the difference.
    assert written == sorted(written)
    assert set(written) <= set(out["closed"])
    assert len(out["closed"]) - len(written) == proc.db_writes_dropped


@pytest.mark.asyncio
async def test_oldest_pending_write_start_tracks_the_backlog(tmp_path):
    proc, db = _proc(tmp_path, with_sinks=False)
    gate = asyncio.Event()

    async def blocked(**kw):
        await gate.wait()

    db.insert_avg_window = blocked
    assert proc.oldest_pending_write_start is None
    for i in range(3):
        await proc._publish_processed(
            [-80.0, -70.0, -60.0, -50.0],
            _result(),
            _stats(),
            start_time=_T0 + timedelta(seconds=i),
            duration_sec=1.0,
        )
    await asyncio.sleep(0)
    # In flight counts as pending until its insert finishes.
    assert proc.oldest_pending_write_start == _T0
    gate.set()
    await _drain(proc)
    assert proc.oldest_pending_write_start is None


@pytest.mark.asyncio
async def test_cancel_during_the_drain_does_not_skip_thread_shutdown(tmp_path, monkeypatch):
    """The supervisor cancels run() after its stop timeout. A cancel landing
    while the writer drains must not skip stopping and joining the threads."""
    import threading as _real_threading

    from rfobserver.pipeline import streaming

    joined: list[str] = []
    signalled: list[object] = []

    class _NoThread:
        def __init__(self, *a, name="", **kw) -> None:
            self.name = name

        def start(self) -> None:
            pass

        def join(self, timeout=None) -> None:
            joined.append(self.name)

    class _Threading:
        Thread = _NoThread

        def __getattr__(self, name):
            return getattr(_real_threading, name)

    monkeypatch.setattr(streaming, "threading", _Threading())
    monkeypatch.setattr(streaming, "_signal_stop", lambda q: signalled.append(q))
    proc, db = _proc(tmp_path, with_sinks=False)
    in_insert = asyncio.Event()

    async def stuck(**kw):
        in_insert.set()
        await asyncio.Event().wait()

    db.insert_avg_window = stuck

    async def loop() -> None:
        await proc._publish_processed(
            [-80.0, -70.0, -60.0, -50.0], _result(), _stats(), start_time=_T0, duration_sec=1.0
        )

    proc._result_consumer_loop = loop  # type: ignore[method-assign]
    task = asyncio.create_task(proc.run())
    await asyncio.wait_for(in_insert.wait(), timeout=5)
    await asyncio.sleep(0.05)  # run() is now in the writer drain
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert signalled == [proc._chunk_queue, proc._burst_queue]
    assert joined == ["recv", "dispatch", "burst", "recctl"]
    assert proc._db_writer.idle
