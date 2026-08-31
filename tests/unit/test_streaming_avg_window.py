"""Averaged-window persistence runs for every live window, independent of sinks."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from rfobserver.config import AppSettings
from rfobserver.models import IQStatistics


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
        nats_producer=(MagicMock() if with_sinks else None),
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
    await proc._persist_avg_window([-80.0, -70.0, -60.0, -50.0], _result(), _stats())
    db.insert_avg_window.assert_called_once()
    kwargs = db.insert_avg_window.call_args.kwargs
    assert kwargs["num_bins"] == 4
    assert kwargs["sdr_center_freq_hz"] == 2_437_000_000.0
    assert kwargs["freq_start_hz"] == pytest.approx(2.409e9)
    assert kwargs["freq_step_hz"] == pytest.approx(0.014e9, rel=1e-6)
    assert kwargs["pwr_avg"] == -70.0
    assert kwargs["powers"] == [-80.0, -70.0, -60.0, -50.0]


@pytest.mark.asyncio
async def test_publish_persists_even_with_no_sinks(tmp_path):
    proc, db = _proc(tmp_path, with_sinks=False)
    await proc._publish_processed([-80.0, -70.0, -60.0, -50.0], _result(), _stats())
    db.insert_avg_window.assert_called_once()


@pytest.mark.asyncio
async def test_replay_mode_skips_avg_window_persist(tmp_path):
    proc, db = _proc(tmp_path, replay_mode=True)
    await proc._publish_processed([-80.0, -70.0, -60.0, -50.0], _result(), _stats())
    db.insert_avg_window.assert_not_called()
