"""RTL-SDR receiver against a fake librtlsdr: conversion, gain steps, rates,
drops reported as gaps, and capture times."""

from __future__ import annotations

import ctypes
import threading
import time
from typing import Any

import numpy as np
import pytest

from rfobserver.capture import rtlsdr_receiver as rtl
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.capture.rtlsdr_receiver import RtlSdrReceiver, u8_to_sc16, valid_rate

RATE = 2_400_000


class FakeLib:
    """librtlsdr: async reads deliver the planned blocks, then wait for cancel."""

    def __init__(self, blocks: list[bytes] | None = None, open_result: int = 0) -> None:
        self.blocks = blocks or []
        self.open_result = open_result
        self.calls: list[tuple[str, Any]] = []
        self.gains = [0, 9, 14, 27, 37, 77, 87, 125, 144, 157, 166, 197, 207, 229, 254, 280, 297]
        self.cancel = threading.Event()
        self.freq = 0

    def rtlsdr_get_device_usb_strings(self, idx: int, m: Any, p: Any, s: Any) -> int:
        m.value, p.value, s.value = b"Nooelec", b"NESDR SMArt v5", b"86861207"
        return 0

    def rtlsdr_open(self, dev: Any, idx: int) -> int:
        return self.open_result

    def rtlsdr_get_tuner_type(self, dev: Any) -> int:
        return 5

    def rtlsdr_set_sample_rate(self, dev: Any, rate: int) -> int:
        self.calls.append(("rate", rate))
        return 0

    def rtlsdr_get_sample_rate(self, dev: Any) -> int:
        return next(v for k, v in reversed(self.calls) if k == "rate")

    def rtlsdr_get_tuner_gains(self, dev: Any, out: Any) -> int:
        if out is not None:
            for i, g in enumerate(self.gains):
                out[i] = g
        return len(self.gains)

    def rtlsdr_set_tuner_gain_mode(self, dev: Any, mode: int) -> int:
        return 0

    def rtlsdr_set_tuner_gain(self, dev: Any, tenths: int) -> int:
        self.calls.append(("gain", tenths))
        return 0

    def rtlsdr_set_agc_mode(self, dev: Any, on: int) -> int:
        return 0

    def rtlsdr_set_freq_correction(self, dev: Any, ppm: int) -> int:
        self.calls.append(("ppm", ppm))
        return 0

    def rtlsdr_reset_buffer(self, dev: Any) -> int:
        return 0

    def rtlsdr_set_center_freq(self, dev: Any, f: int) -> int:
        self.freq = f
        return 0

    def rtlsdr_read_async(self, dev: Any, cb: Any, ctx: Any, n: int, length: int) -> int:
        for block in self.blocks:
            buf = (ctypes.c_ubyte * len(block)).from_buffer_copy(block)
            cb(buf, len(block), None)
        self.cancel.wait(5)
        return 0

    def rtlsdr_cancel_async(self, dev: Any) -> int:
        self.cancel.set()
        return 0

    def rtlsdr_close(self, dev: Any) -> int:
        self.calls.append(("close", None))
        return 0


def _rx(lib: FakeLib, gain: int = 30, rate: int = RATE) -> RtlSdrReceiver:
    return RtlSdrReceiver(
        ReceiverConfig(gain_db=gain, bandwidth_hz=rate, duration_sec=0.1), ppm=12, lib=lib
    )


def test_u8_maps_symmetric_around_127_5_without_wrapping() -> None:
    raw = np.array([0, 127, 128, 255], dtype=np.uint8)
    out = np.empty(4, dtype=np.int16)
    u8_to_sc16(raw, out)
    assert out.tolist() == [-32640, -128, 128, 32640]


def test_rates() -> None:
    assert valid_rate(2_400_000) and valid_rate(250_000) and valid_rate(3_200_000)
    assert not valid_rate(56_000_000) and not valid_rate(500_000)


def test_initialize_sets_rate_ppm_and_nearest_gain_step() -> None:
    lib = FakeLib()
    rx = _rx(lib, gain=30)
    rx.initialize()
    assert rx.serial == "RTL86861207"
    assert ("rate", RATE) in lib.calls and ("ppm", 12) in lib.calls
    assert ("gain", 297) in lib.calls and rx.gain_db == pytest.approx(29.7)


def test_bad_rate_and_failed_open_explain_themselves() -> None:
    with pytest.raises(RuntimeError, match="cannot sample at 56.000 MS/s"):
        _rx(FakeLib(), rate=56_000_000).initialize()
    with pytest.raises(RuntimeError, match="plugdev"):
        _rx(FakeLib(open_result=-3)).initialize()


def _blocks(n_blocks: int, block_samples: int) -> list[bytes]:
    """Blocks whose I bytes count up, so the order of samples can be checked."""
    out = []
    for b in range(n_blocks):
        iq = np.empty(2 * block_samples, dtype=np.uint8)
        iq[0::2] = (b * 7 + np.arange(block_samples)) % 256
        iq[1::2] = 128
        out.append(iq.tobytes())
    return out


def test_chunks_are_contiguous_across_blocks() -> None:
    blocks = _blocks(4, 1000)
    lib = FakeLib(blocks)
    rx = _rx(lib)
    rx.initialize()
    t0 = time.time()
    rx.start_streaming(101_000_000)
    try:
        a = np.empty(1500, dtype=np.int32)
        b = np.empty(1500, dtype=np.int32)
        assert rx.recv_chunk(a) == 1500 and rx.recv_chunk(b) == 1500
    finally:
        rx.stop_streaming()
    got = np.concatenate([a, b]).view(np.int16)[0::2]
    want_u8 = np.frombuffer(b"".join(blocks), dtype=np.uint8)[0::2][:3000]
    want = np.empty(3000, dtype=np.int16)
    u8_to_sc16(want_u8, want)
    assert np.array_equal(got, want)
    assert lib.freq == 101_000_000 and rx.last_gaps == [] and rx.overflow_events == 0
    # Host time of the first sample: just before the first block arrived.
    assert rx.last_device_time is not None and t0 - 1 < rx.last_device_time < time.time()


def test_drops_are_not_charged_to_blocks_queued_before_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rtl, "QUEUE_BLOCKS", 2)
    lib = FakeLib(_blocks(5, 1000))  # 3 of 5 blocks overflow the 2-block queue
    rx = _rx(lib)
    rx.initialize()
    rx.start_streaming(101_000_000)
    try:
        for _ in range(100):
            if rx._drop_pending == 3 * 2000:
                break
            time.sleep(0.01)
        buf = np.empty(2000, dtype=np.int32)
        assert rx.recv_chunk(buf) == 2000
        # The two queued blocks came before the drop: no gap in this chunk.
        assert rx.last_gaps == [] and rx.overflow_events == 0
    finally:
        rx.stop_streaming()
    assert rx._drop_pending == 0  # cleared with the stream


def test_drop_is_reported_at_its_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bytes lost before a block show up at that block's place in the chunk."""
    lib = FakeLib()
    rx = _rx(lib)
    rx.initialize()
    rx._streaming = True  # feed the queue by hand, no USB thread
    rx._blocks.put_nowait((bytes(2000), time.time(), 0))

    def later_block() -> None:
        time.sleep(0.05)
        rx._blocks.put_nowait((bytes(2000), time.time(), 600))  # 300 samples lost before it

    threading.Thread(target=later_block).start()
    buf = np.empty(1500, dtype=np.int32)
    assert rx.recv_chunk(buf) == 1500
    assert rx.last_gaps == [(1000, 300)]
    assert (rx.overflow_events, rx.overflow_lost_samples) == (1, 300)
