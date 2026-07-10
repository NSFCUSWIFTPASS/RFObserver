"""Tests for the RAM-derived recording duration cap."""

import math

from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import _effective_max_recording_sec


def _settings(**kw) -> AppSettings:
    base = dict(
        BANDWIDTH=26_000_000,
        NUM_FFT_BINS=1024,
        PSD_TIME_RESOLUTION_MS=0.2,
        RECORDING_MAX_SEC=0.0,
        RECORDING_RAM_BUFFER=False,
        RECORDING_MEM_FRACTION=0.5,
        _env_file=None,
    )
    base.update(kw)
    return AppSettings(**base)


def test_disk_mode_is_not_ram_capped() -> None:
    # Disk mode streams grids to disk -> no RAM cap; unlimited when MAX_SEC=0.
    assert _effective_max_recording_sec(_settings(), 4_000_000_000) == math.inf


def test_disk_mode_respects_configured_max() -> None:
    assert _effective_max_recording_sec(_settings(RECORDING_MAX_SEC=30.0), 4_000_000_000) == 30.0


def test_ram_mode_caps_by_available_ram() -> None:
    # RAM mode holds IQ + grids: iq=26e6*4=104MB/s, grid=(1000/0.2)*1024*4=20.48MB/s
    # budget = 0.5 * 2.0GB = 1.0GB -> ~1.0e9 / 124.48e6 ~= 8.03s
    s = _settings(RECORDING_RAM_BUFFER=True)
    cap = _effective_max_recording_sec(s, 2_000_000_000)
    assert 7.0 < cap < 9.0


def test_ram_mode_takes_min_with_configured() -> None:
    s = _settings(RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=3.0)
    assert _effective_max_recording_sec(s, 2_000_000_000) == 3.0


def test_unreadable_mem_falls_back() -> None:
    # RAM mode with no mem info -> conservative: configured max, or 30s if unlimited.
    assert _effective_max_recording_sec(_settings(RECORDING_RAM_BUFFER=True), None) == 30.0
    assert (
        _effective_max_recording_sec(
            _settings(RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=10.0), None
        )
        == 10.0
    )
