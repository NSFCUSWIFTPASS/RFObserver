"""Tone detector orchestration: averaged windows in, Tone detections out, with
the IQ read from a ring and the burst rows already covering a tone labelled
instead of duplicated. A 1.3 MS/s, 128-bin setup keeps it quick."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from rfobserver.pipeline.tone_detector import ToneDetector, ToneWindow
from rfobserver.storage.database import SensorDatabase

RATE = 1_300_000
BINS = 128
STEP = RATE / BINS  # 10.16 kHz
CENTER = 915e6
FREQ_START = CENTER - RATE / 2
T0 = datetime(2026, 11, 18, 17, 0, 0, tzinfo=timezone.utc)
NOISE_POWER = 1e-6


class FakeRing:
    """Holds the IQ of chosen windows at their stream positions (SC16 packed
    in int32, as the pipeline's ring does); anything else reads as gone."""

    def __init__(self) -> None:
        self.spans: dict[int, np.ndarray] = {}

    def put(self, start: int, iq: np.ndarray) -> None:
        self.spans[start] = iq

    def read_range(self, start: int, end: int) -> np.ndarray | None:
        for s, iq in self.spans.items():
            if s <= start and end <= s + len(iq):
                part = iq[start - s : end - s] * 8000.0
                out = np.empty(2 * len(part), dtype="<i2")
                out[0::2] = np.round(part.real).astype("<i2")
                out[1::2] = np.round(part.imag).astype("<i2")
                return out.view(np.int32)
        return None


def _iq(
    seconds: float,
    tone_hz: float | None = None,
    on: tuple[float, float] = (0.4, 0.5),
    bin_snr_db: float = 5.0,
    seed: int = 1,
    t_offset: float = 0.0,
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    n = int(seconds * RATE)
    x = np.sqrt(NOISE_POWER / 2) * (rng.normal(size=n) + 1j * rng.normal(size=n))
    if tone_hz is not None:
        amp = np.sqrt(NOISE_POWER * STEP / RATE * 10 ** (bin_snr_db / 10))
        t = np.arange(n) / RATE + t_offset
        m = (t >= on[0]) & (t < on[1])
        x[m] += amp * np.exp(2j * np.pi * (tone_hz - CENTER) * t[m])
    return x


def _powers(seed: int, bump: dict[int, float] | None = None) -> list[float]:
    rng = np.random.default_rng(seed)
    p = -100.0 + 0.04 * rng.normal(size=BINS)
    for k, v in (bump or {}).items():
        p[k] += v
    return p.tolist()


def _window(i: int, powers: list[float]) -> ToneWindow:
    return ToneWindow(
        key=(CENTER, BINS, 35.0),
        powers=powers,
        freq_start_hz=FREQ_START,
        freq_step_hz=STEP,
        sdr_center_hz=CENTER,
        start_sample=i * RATE,
        end_sample=(i + 1) * RATE,
        start_time=T0 + timedelta(seconds=i),
    )


def _bin(f: float) -> int:
    return int(round((f - FREQ_START) / STEP))


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "t.db"))
    await database.connect()
    yield database
    await database.close()


def _detector(db, ring) -> ToneDetector:
    return ToneDetector(
        db,
        ring,
        sample_rate_hz=RATE,
        threshold_db=0.25,
        meta=lambda: {"sample_rate_hz": float(RATE), "gain_db": 35.0, "device_serial": "T"},
        hold_sec=0.0,
    )


async def _run(det: ToneDetector, windows: list[ToneWindow]) -> None:
    task = asyncio.create_task(det.run())
    for w in windows:
        while not det.submit(w):
            await asyncio.sleep(0.01)
    await det.stop()
    await task


async def _rows(db) -> list[dict]:
    rows = await db.detections_overlapping(
        T0 - timedelta(seconds=1), T0 + timedelta(seconds=60), 0.0, 2e9
    )
    async with db._db.execute("SELECT burst_id, attribution, duration_ms FROM detections") as c:
        extra = {r[0]: (r[1], r[2]) for r in await c.fetchall()}
    for r in rows:
        r["attribution"], r["duration_ms"] = extra[r["burst_id"]]
    return rows


TONE = 915_123_456.0


async def test_a_weak_tone_becomes_a_tone_detection(db) -> None:
    ring = FakeRing()
    ring.put(12 * RATE, _iq(1.0, TONE, on=(0.4, 0.5)))
    windows = [_window(i, _powers(i)) for i in range(12)]
    windows.append(_window(12, _powers(12, {_bin(TONE): 0.55})))
    det = _detector(db, ring)
    await _run(det, windows)

    rows = await _rows(db)
    assert len(rows) == 1
    r = rows[0]
    assert r["model"] == "Tone"
    assert abs(r["center_freq_hz"] - TONE) < 5.0
    assert abs(r["start_time"] - (T0 + timedelta(seconds=12.4))) < timedelta(milliseconds=3)
    assert abs(r["duration_ms"] - 100.0) < 5.0
    a = json.loads(r["attribution"])
    assert a["source"] == "tone_detector"
    frame = a["frames"][0]
    assert frame["model"] == "Tone" and frame["protocol"] == "CW"
    assert abs(frame["snr_db"] - 5.0) < 1.5
    assert det.stats["inserted"] == 1


async def test_bursts_covering_the_tone_are_labelled_not_duplicated(db) -> None:
    s = T0 + timedelta(seconds=12.401)
    await db.insert_detections(
        [
            dict(
                burst_id="strong",
                start_time=s,
                stop_time=s + timedelta(milliseconds=98),
                center_freq_hz=FREQ_START + _bin(TONE) * STEP,
                bandwidth_hz=120_000.0,
                peak_power_db=-60.0,
                duration_ms=98.0,
                detection_timestamp=s,
            )
        ]
    )
    ring = FakeRing()
    ring.put(12 * RATE, _iq(1.0, TONE, on=(0.4, 0.5), bin_snr_db=30.0))
    windows = [_window(i, _powers(i)) for i in range(12)]
    windows.append(_window(12, _powers(12, {_bin(TONE): 6.0})))
    det = _detector(db, ring)
    await _run(det, windows)

    rows = await _rows(db)
    assert [r["burst_id"] for r in rows] == ["strong"]
    assert rows[0]["model"] == "Tone"
    assert json.loads(rows[0]["attribution"])["source"] == "tone_detector"
    assert det.stats["labelled"] == 1 and det.stats["inserted"] == 0


async def test_a_fragment_does_not_stand_in_for_the_tone(db) -> None:
    s = T0 + timedelta(seconds=12.45)
    await db.insert_detections(
        [
            dict(
                burst_id="frag",
                start_time=s,
                stop_time=s + timedelta(milliseconds=20),
                center_freq_hz=FREQ_START + _bin(TONE) * STEP,
                bandwidth_hz=STEP,
                peak_power_db=-80.0,
                duration_ms=20.0,
                detection_timestamp=s,
            )
        ]
    )
    ring = FakeRing()
    ring.put(12 * RATE, _iq(1.0, TONE, on=(0.4, 0.5)))
    windows = [_window(i, _powers(i)) for i in range(12)]
    windows.append(_window(12, _powers(12, {_bin(TONE): 0.55})))
    await _run(_detector(db, ring), windows)

    rows = {r["burst_id"]: r for r in await _rows(db)}
    assert set(rows) - {"frag"} and len(rows) == 2
    assert rows["frag"]["model"] is None


async def test_a_tone_across_two_windows_is_one_detection(db) -> None:
    ring = FakeRing()
    both = _iq(2.0, TONE, on=(0.7, 1.3))
    ring.put(12 * RATE, both)
    windows = [_window(i, _powers(i)) for i in range(12)]
    windows.append(_window(12, _powers(12, {_bin(TONE): 1.0})))
    windows.append(_window(13, _powers(13, {_bin(TONE): 1.0})))
    await _run(_detector(db, ring), windows)

    rows = await _rows(db)
    assert len(rows) == 1
    assert abs(rows[0]["duration_ms"] - 600.0) < 5.0


async def test_a_window_already_gone_from_the_ring_is_counted(db) -> None:
    windows = [_window(i, _powers(i)) for i in range(12)]
    windows.append(_window(12, _powers(12, {_bin(TONE): 0.55})))
    det = _detector(db, FakeRing())
    await _run(det, windows)
    assert det.stats["too_late"] == 1
    assert await _rows(db) == []


async def test_a_full_queue_drops_windows(db) -> None:
    det = _detector(db, FakeRing())
    accepted = [det.submit(_window(i, _powers(i))) for i in range(50)]
    assert not all(accepted)
    assert det.stats["dropped"] == accepted.count(False)
