"""Mapping averaged windows to rf-db rows."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from rfobserver.storage.database import WindowStatsRow
from rfobserver.transport.rfdb import MetadataKey, to_output_row

_T0 = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)


def _window(**overrides: object) -> WindowStatsRow:
    fields: dict[str, object] = dict(
        id=7,
        start_time=_T0,
        duration_sec=0.557,
        sdr_center_freq_hz=915_000_000.4,
        sample_rate_hz=26_000_000.0,
        gain_db=35.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.2,
    )
    fields.update(overrides)
    return WindowStatsRow(**fields)  # type: ignore[arg-type]


def test_maps_a_window_to_its_rfdb_row():
    row = to_output_row(_window(), window_sec=0.5)

    assert row is not None
    assert row.metadata == MetadataKey(
        frequency=915_000_000,
        sample_rate=26_000_000,
        bandwidth=26_000_000,
        gain=35,
        length=0.5,
        interval=0.5,
        bit_depth="16",
    )
    assert row.created_at == _T0
    assert (row.average_db, row.max_db, row.median_db, row.std_dev, row.kurtosis) == (
        -70.0,
        -50.0,
        -72.0,
        3.0,
        1.2,
    )


def test_length_is_the_configured_window_not_the_measured_one():
    a = to_output_row(_window(duration_sec=0.502), window_sec=0.5)
    b = to_output_row(_window(duration_sec=0.557), window_sec=0.5)

    assert a is not None and b is not None
    assert a.metadata == b.metadata


@pytest.mark.parametrize(
    "overrides",
    [
        {"gain_db": None},
        {"pwr_avg": None},
        {"kurtosis": None},
        {"pwr_max": float("-inf")},
        {"pwr_median": float("nan")},
    ],
)
def test_rows_rfdb_cannot_store_are_rejected(overrides):
    assert to_output_row(_window(**overrides), window_sec=0.5) is None
