"""Tone detector orchestration: each chunk's PSD grid in, the listed
frequencies' bins (and their guard bins) tracked, Tone detections out."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pytest

from rfobserver.pipeline.tone_detector import ToneDetector
from rfobserver.storage.database import SensorDatabase

BINS = 64
RATE = 1_300_000.0
STEP = RATE / BINS
CENTER = 915_000_000
ROW = 0.0002
ROWS_PER_CHUNK = 197
T0 = datetime(2026, 11, 18, 17, 0, 0, tzinfo=timezone.utc)
FLOOR_DB = -100.0
TONE = CENTER + 10 * STEP  # bin-centred


def _chunks(
    seconds: float,
    tone: tuple[float, float] | None = None,
    snr_db: float = 5.0,
    wide: bool = False,
    center: int = CENTER,
    seed: int = 1,
    t_start: float = 0.0,
):
    """Results as the consumer gets them: a dB grid per chunk (noise in every
    bin, plus a tone in TONE's bin while on), capture time, stream position."""
    rng = np.random.default_rng(seed)
    freq_axis = (np.arange(BINS) - BINS // 2) * STEP
    k = int(np.argmin(np.abs(freq_axis + center - TONE)))
    n_chunks = round(seconds / (ROW * ROWS_PER_CHUNK))
    out = []
    for c in range(n_chunks):
        lin = 10 ** (FLOOR_DB / 10) * rng.exponential(size=(ROWS_PER_CHUNK, BINS, 4)).mean(axis=2)
        t = t_start + (c * ROWS_PER_CHUNK + np.arange(ROWS_PER_CHUNK)) * ROW
        if tone is not None:
            on = (t >= tone[0]) & (t < tone[1])
            if wide:  # broadband: every bin rises
                lin[on, :] += 10 ** ((FLOOR_DB + snr_db) / 10)
            else:
                lin[on, k] += 10 ** ((FLOOR_DB + snr_db) / 10)
        out.append(
            SimpleNamespace(
                psd_grid=SimpleNamespace(grid=10 * np.log10(lin), freq_axis=freq_axis),
                center_freq_hz=center,
                capture_time=T0.timestamp() + t[0],
                chunk_start=c * ROWS_PER_CHUNK * 100,
            )
        )
    return out


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "t.db"))
    await database.connect()
    yield database
    await database.close()


def _detector(db, freqs=(TONE,)) -> ToneDetector:
    return ToneDetector(
        db,
        freqs=list(freqs),
        threshold_db=3.0,
        meta=lambda: {"sample_rate_hz": RATE, "gain_db": 35.0, "device_serial": "T"},
    )


async def _run(det: ToneDetector, results) -> None:
    for r in results:
        det.on_result(r, ROW)
    await det.stop()


async def _rows(db) -> list[dict]:
    async with db._db.execute(
        "SELECT burst_id, start_time, center_freq_hz, model, attribution, duration_ms "
        "FROM detections ORDER BY start_time"
    ) as c:
        rows = await c.fetchall()
    return [
        {
            "burst_id": r[0],
            "start_time": datetime.fromisoformat(r[1]),
            "center_freq_hz": r[2],
            "model": r[3],
            "attribution": r[4],
            "duration_ms": r[5],
        }
        for r in rows
    ]


async def test_a_5_db_tone_at_a_listed_frequency_is_recorded(db) -> None:
    det = _detector(db)
    await _run(det, _chunks(4.0, tone=(2.0, 2.1)))
    rows = await _rows(db)
    assert len(rows) == 1
    r = rows[0]
    assert r["model"] == "Tone"
    assert r["center_freq_hz"] == TONE
    assert abs(r["start_time"] - (T0 + timedelta(seconds=2.0))) < timedelta(milliseconds=2)
    assert abs(r["duration_ms"] - 100.0) < 3.0
    a = json.loads(r["attribution"])
    assert a["source"] == "tone_detector"
    frame = a["frames"][0]
    assert frame["model"] == "Tone" and frame["protocol"] == "CW"
    assert frame["tone_hz"] == TONE
    assert abs(frame["snr_db"] - 5.0) < 0.6
    assert abs(frame["level_db"] - (FLOOR_DB + 5.0)) < 0.6
    assert det.stats["inserted"] == 1


async def test_tones_elsewhere_are_ignored(db) -> None:
    """Only listed frequencies are watched."""
    det = _detector(db, freqs=(CENTER - 20 * STEP,))
    await _run(det, _chunks(4.0, tone=(2.0, 2.5), snr_db=20.0))
    assert await _rows(db) == []


async def test_a_tone_of_seconds_is_one_detection(db) -> None:
    await _run(_detector(db), _chunks(8.0, tone=(2.0, 5.0)))
    rows = await _rows(db)
    assert len(rows) == 1
    assert abs(rows[0]["duration_ms"] - 3000.0) < 3.0


async def test_a_frequency_outside_the_tuned_band_is_counted_not_tracked(db) -> None:
    det = _detector(db, freqs=(CENTER + 5 * RATE,))
    await _run(det, _chunks(1.0))
    assert det.stats["out_of_band"] == 1
    assert det.status_freqs() == {CENTER + 5 * RATE: "out of band"}


async def test_a_retune_ends_the_tone_and_starts_the_bin_over(db) -> None:
    """A tone open when the tuning changes is closed; the new tuning learns its
    own floor before anything is found."""
    det = _detector(db)
    first = _chunks(3.0, tone=(2.5, 3.5))
    later = _chunks(0.5, tone=(0.0, 0.5), center=CENTER + 1000, t_start=3.0, seed=2)
    await _run(det, first + later)
    rows = await _rows(db)
    assert len(rows) == 1
    assert rows[0]["duration_ms"] < 520.0


async def test_rows_without_capture_time_are_skipped(db) -> None:
    det = _detector(db)
    results = _chunks(1.0)
    for r in results:
        r.capture_time = None
    await _run(det, results)
    assert det.stats["skipped"] == len(results)


async def test_stop_writes_a_tone_still_on(db) -> None:
    await _run(_detector(db), _chunks(3.0, tone=(2.0, 3.5)))
    assert len(await _rows(db)) == 1


async def test_broadband_energy_at_a_listed_frequency_is_not_recorded(db) -> None:
    await _run(_detector(db), _chunks(4.0, tone=(2.0, 2.04), snr_db=8.0, wide=True))
    assert await _rows(db) == []


async def test_a_retune_that_brings_a_frequency_into_band_starts_watching_it(db) -> None:
    det = _detector(db, freqs=(CENTER + RATE,))
    for r in _chunks(0.5):
        det.on_result(r, ROW)
    assert det.status_freqs() == {CENTER + RATE: "out of band"}
    for r in _chunks(0.5, center=int(CENTER + RATE), t_start=0.5):
        det.on_result(r, ROW)
    assert det.status_freqs() == {CENTER + RATE: "tracking"}
    await det.stop()


async def test_tones_found_in_a_high_sensitivity_run_carry_its_id(db) -> None:
    det = ToneDetector(
        db,
        freqs=[TONE],
        threshold_db=3.0,
        meta=lambda: {"sample_rate_hz": RATE, "gain_db": 35.0, "device_serial": "T"},
        hs_run=lambda: "r1",
    )
    await _run(det, _chunks(4.0, tone=(2.0, 2.1)))
    assert len(await db.query_detections(hs_run="r1")) == 1
