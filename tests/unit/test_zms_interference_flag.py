"""An observation whose pwr_avg is above ZMS_INTERFERENCE_THRESHOLD_DB goes to
OpenZMS flagged as interference (2026-11-06)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from rfobserver.config import AppSettings
from rfobserver.processing.detection import interference_flag


def test_interference_flag_is_strictly_above_the_threshold():
    assert interference_flag(-60.0, -70.0) is True
    assert interference_flag(-70.0, -70.0) is False
    assert interference_flag(-80.0, -70.0) is False


def test_interference_flag_is_none_without_a_threshold():
    assert interference_flag(-60.0, None) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("threshold", "expected"), [(-75.0, True), (-65.0, False), (None, False)])
async def test_sweep_pipeline_flags_the_observation(tmp_path, threshold, expected):
    from rfobserver.pipeline.continuous import ContinuousProcessor

    settings = AppSettings(_env_file=None, ZMS_INTERFERENCE_THRESHOLD_DB=threshold)
    zms = MagicMock()
    zms.submit_observation = AsyncMock(return_value=True)
    proc = ContinuousProcessor(
        receiver=MagicMock(),
        database=MagicMock(),
        local_storage=MagicMock(),
        settings=settings,
        zms_monitor=zms,
    )
    envelope = SimpleNamespace(statistics=SimpleNamespace(average=-70.0))
    await proc._fanout_envelope(envelope, 1)
    zms.submit_observation.assert_awaited_once_with(envelope, interference=expected)
