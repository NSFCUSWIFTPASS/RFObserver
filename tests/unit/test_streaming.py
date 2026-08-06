"""Tests for the streaming pipeline components."""

from __future__ import annotations

import numpy as np
import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.iq_utils import (
    calculate_iq_statistics,
    convert_bytes_to_complex,
    convert_sc16_to_complex,
    finalize_moments,
    moments_from_iq,
)
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import (
    PSDGridConfig,
    PSDGridResult,
    compute_psd_grid,
    compute_summary_psd,
)

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
        assert len(det._tracked) == 0
        assert det._total_rows_written == 0

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


# ---------------------------------------------------------------------------
# Interval-accumulated envelope statistics
# ---------------------------------------------------------------------------


class TestIntervalEnvelopeStatistics:
    def _make_processor(self, tmp_path):
        from rfobserver.config import AppSettings
        from rfobserver.pipeline.streaming import StreamingProcessor
        from rfobserver.storage.database import SensorDatabase
        from rfobserver.storage.local import LocalStorage

        storage_path = tmp_path / "storage"
        storage_path.mkdir()
        settings = AppSettings(
            FREQUENCY_START=915_000_000,
            FREQUENCY_END=915_000_000,
            BANDWIDTH=1_000_000,
            DURATION_SEC=0.5,
            GAIN=35,
            NUM_FFT_BINS=64,
            PSD_TIME_RESOLUTION_MS=0.5,
            STREAMING_CHUNK_SLICES=10,
            MOCK_RECEIVER=True,
            STORAGE_PATH=str(storage_path),
            DB_PATH=str(tmp_path / "test.db"),
            ARCHIVE_MAX_GB=0.01,
            _env_file=None,
        )
        rx_config = ReceiverConfig(
            gain_db=settings.GAIN,
            bandwidth_hz=settings.BANDWIDTH,
            duration_sec=settings.DURATION_SEC,
        )
        rx = MockReceiver(receiver_config=rx_config)
        rx.initialize()
        proc = StreamingProcessor(
            receiver=rx,
            database=SensorDatabase(settings.DB_PATH),
            local_storage=LocalStorage(
                storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB
            ),
            settings=settings,
        )
        return proc, settings

    def _make_result(self, iq, center_freq, bandwidth):
        from rfobserver.pipeline.streaming import _StreamResult

        grid_config = PSDGridConfig(num_bins=64, time_resolution_ms=0.5, num_workers=1)
        psd_grid = compute_psd_grid(iq, bandwidth, config=grid_config)
        summary = compute_summary_psd(psd_grid, center_freq, bandwidth)
        moments = moments_from_iq(iq)
        return _StreamResult(
            summary_psd=summary,
            iq_stats=finalize_moments(moments),
            bursts=[],
            psd_grid=psd_grid,
            center_freq_hz=center_freq,
            capture_num=1,
            process_ms=1.0,
            latency_ms=1.0,
            iq_moments=moments,
        ), moments

    def test_envelope_stats_are_interval_accumulated(self, tmp_path):
        """The envelope uses the folded interval moments, not the last chunk."""
        proc, settings = self._make_processor(tmp_path)
        center_freq = settings.FREQUENCY_START
        bandwidth = settings.BANDWIDTH

        rng = np.random.default_rng(0)
        n = 40_000
        iq_lo = ((rng.standard_normal(n) + 1j * rng.standard_normal(n)) * 0.01).astype(np.complex64)
        iq_hi = ((rng.standard_normal(n) + 1j * rng.standard_normal(n)) * 0.20).astype(np.complex64)

        result_lo, m0 = self._make_result(iq_lo, center_freq, bandwidth)
        result_hi, m1 = self._make_result(iq_hi, center_freq, bandwidth)

        interval_stats = finalize_moments(m0.add(m1))

        # Interval-accumulated average must differ from either chunk's own average.
        assert abs(interval_stats.average - result_lo.iq_stats.average) > 0.5
        assert abs(interval_stats.average - result_hi.iq_stats.average) > 0.5

        # Folded moments equal the whole-array computation over the concatenation.
        whole = calculate_iq_statistics(np.concatenate([iq_lo, iq_hi]))
        assert abs(interval_stats.average - whole.average) < 1e-6
        assert abs(interval_stats.kurtosis - whole.kurtosis) < 1e-6

        avg_powers = result_hi.summary_psd.powers
        envelope = proc._build_envelope(avg_powers, result_hi, interval_stats)

        assert envelope.statistics.average == interval_stats.average
        assert envelope.statistics.kurtosis == interval_stats.kurtosis
        # NOT the last chunk's stats.
        assert envelope.statistics.average != result_hi.iq_stats.average
