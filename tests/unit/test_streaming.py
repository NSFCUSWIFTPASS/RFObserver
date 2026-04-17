"""Tests for the streaming pipeline components."""

from __future__ import annotations

import numpy as np
import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.iq_utils import convert_bytes_to_complex, convert_sc16_to_complex
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import PSDGridResult

# ---------------------------------------------------------------------------
# Phase 1: SC16 conversion
# ---------------------------------------------------------------------------


class TestConvertSc16ToComplex:
    def test_roundtrip_matches_bytes_path(self):
        """convert_sc16_to_complex should produce the same result as convert_bytes_to_complex."""
        rng = np.random.default_rng(42)
        # Create random SC16 data as interleaved int16
        raw16 = rng.integers(-30000, 30000, size=200, dtype=np.int16)
        iq_bytes = raw16.tobytes()
        sc16_int32 = np.frombuffer(iq_bytes, dtype=np.int32)

        from_bytes = convert_bytes_to_complex(iq_bytes)
        from_sc16 = convert_sc16_to_complex(sc16_int32)

        np.testing.assert_array_equal(from_bytes, from_sc16)

    def test_normalization_range(self):
        """Output should be in [-1, 1] range."""
        sc16 = np.array([0x7FFF0001, 0x80010000], dtype=np.int32)
        result = convert_sc16_to_complex(sc16)
        assert result.dtype == np.complex64
        assert np.all(np.abs(result.real) <= 1.0)
        assert np.all(np.abs(result.imag) <= 1.0)

    def test_empty_input(self):
        sc16 = np.array([], dtype=np.int32)
        result = convert_sc16_to_complex(sc16)
        assert len(result) == 0
        assert result.dtype == np.complex64


# ---------------------------------------------------------------------------
# Phase 1: MockReceiver streaming
# ---------------------------------------------------------------------------


class TestMockReceiverStreaming:
    def test_start_stop_streaming(self):
        config = ReceiverConfig(gain_db=40, bandwidth_hz=56_000_000, duration_sec=0.5)
        mock = MockReceiver(config, seed=123)
        mock.initialize()

        mock.start_streaming(2_437_000_000)
        assert mock._streaming is True

        mock.stop_streaming()
        assert mock._streaming is False

    def test_recv_chunk_fills_buffer(self):
        config = ReceiverConfig(gain_db=40, bandwidth_hz=1_000_000, duration_sec=0.5)
        mock = MockReceiver(config, seed=123)
        mock.initialize()

        chunk_size = 1024
        buf = np.zeros(chunk_size, dtype=np.int32)

        mock.start_streaming(2_437_000_000)
        n = mock.recv_chunk(buf)
        mock.stop_streaming()

        assert n == chunk_size
        # Buffer should not be all zeros after filling
        assert np.any(buf != 0)

    def test_recv_chunk_produces_valid_sc16(self):
        """The buffer should be convertible to complex samples."""
        config = ReceiverConfig(gain_db=40, bandwidth_hz=1_000_000, duration_sec=0.5)
        mock = MockReceiver(config, seed=42)
        mock.initialize()

        buf = np.zeros(512, dtype=np.int32)
        mock.start_streaming(2_437_000_000)
        mock.recv_chunk(buf)
        mock.stop_streaming()

        # Should convert without error
        complex_data = convert_sc16_to_complex(buf)
        assert complex_data.dtype == np.complex64
        assert len(complex_data) == 512


# ---------------------------------------------------------------------------
# Phase 2: RollingBurstDetector
# ---------------------------------------------------------------------------


class TestRollingBurstDetector:
    @pytest.fixture()
    def _make_detector(self):
        """Factory for creating a detector with a small window."""
        freq_axis = np.fft.fftshift(np.fft.fftfreq(32, 1.0 / 1_000_000))

        def factory(window_rows=100, eval_interval=50):
            return RollingBurstDetector(
                window_rows=window_rows,
                eval_interval_rows=eval_interval,
                num_bins=32,
                burst_config=BurstDetectionConfig(
                    threshold_high_db=10.0,
                    threshold_low_ratio=0.6,
                ),
                center_freq_hz=2_437_000_000.0,
                freq_axis=freq_axis,
                time_resolution_s=0.0002,
            )

        return factory

    def test_no_bursts_in_noise(self, _make_detector):
        """Flat noise floor should produce no bursts."""
        det = _make_detector()
        rng = np.random.default_rng(42)

        # Feed enough rows to trigger evaluation
        for _ in range(3):
            grid = rng.normal(-80.0, 1.0, size=(50, 32)).astype(np.float32)
            time_axis = np.arange(50) * 0.0002
            freq_axis = np.fft.fftshift(np.fft.fftfreq(32, 1.0 / 1_000_000))
            psd = PSDGridResult(
                grid=grid,
                time_axis=time_axis,
                freq_axis=freq_axis,
                ffts_per_slice=1,
                total_ffts=50,
            )
            bursts = det.feed(psd)
            # Noise should not produce bursts (or very few false positives)
            assert len(bursts) <= 2  # allow small false positive margin

    def test_reset_clears_state(self, _make_detector):
        det = _make_detector()
        # Feed some data
        grid = np.full((50, 32), -80.0, dtype=np.float32)
        time_axis = np.arange(50) * 0.0002
        freq_axis = np.fft.fftshift(np.fft.fftfreq(32, 1.0 / 1_000_000))
        psd = PSDGridResult(
            grid=grid,
            time_axis=time_axis,
            freq_axis=freq_axis,
            ffts_per_slice=1,
            total_ffts=50,
        )
        det.feed(psd)

        det.reset()
        assert det._rows_filled == 0
        assert det._write_pos == 0
        assert len(det._pending_bursts) == 0

    def test_detects_injected_burst(self, _make_detector):
        """A strong signal injected into a noise grid should be detected."""
        det = _make_detector(window_rows=100, eval_interval=50)

        # First feed: pure noise to establish floor
        noise = np.full((50, 32), -80.0, dtype=np.float32)
        time_axis = np.arange(50) * 0.0002
        freq_axis = np.fft.fftshift(np.fft.fftfreq(32, 1.0 / 1_000_000))
        psd_noise = PSDGridResult(
            grid=noise.copy(),
            time_axis=time_axis,
            freq_axis=freq_axis,
            ffts_per_slice=1,
            total_ffts=50,
        )
        det.feed(psd_noise)

        # Second feed: inject a burst in the middle rows, middle bins
        signal = noise.copy()
        signal[10:30, 12:18] = -50.0  # 30 dB above noise floor
        psd_burst = PSDGridResult(
            grid=signal,
            time_axis=time_axis,
            freq_axis=freq_axis,
            ffts_per_slice=1,
            total_ffts=50,
        )
        bursts = det.feed(psd_burst)

        # Should detect at least one burst
        assert len(bursts) >= 1
