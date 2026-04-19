"""FM demodulator module — GPU-accelerated via CuPy.

Receives wideband SC16 IQ, frequency-shifts to an FM channel,
downsamples, FM-demodulates, and outputs 48 kHz mono PCM audio
via a queue for WebSocket streaming to the browser.
"""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
from typing import Any

import numpy as np

from rfobserver.modules.base import ParamDescriptor, UpstreamModule
from rfobserver.modules.manager import register_module

logger = logging.getLogger(__name__)

try:
    import cupy as cp

    HAS_CUPY = True
except ImportError:
    HAS_CUPY = False


@register_module
class FMDemodModule(UpstreamModule):
    """FM broadcast demodulator running on the Jetson GPU."""

    module_type = "fm_demod"
    has_audio_output = True
    audio_sample_rate = 48_000

    # Default parameters
    _DEFAULTS: dict[str, Any] = {
        "channel_freq_hz": 101_900_000,
        "channel_bw_hz": 200_000,
        "audio_rate": 48_000,
        "deemphasis_tau": 75e-6,
        "volume": 0.8,
    }

    @classmethod
    def parameters(cls) -> list[ParamDescriptor]:
        return [
            ParamDescriptor(
                name="channel_freq_hz",
                label="Frequency",
                type="number",
                default=101_900_000,
                unit="Hz",
                step=100_000,
            ),
            ParamDescriptor(
                name="channel_bw_hz",
                label="Channel BW",
                type="number",
                default=200_000,
                unit="Hz",
                min=50_000,
                max=300_000,
                step=10_000,
            ),
            ParamDescriptor(
                name="volume",
                label="Volume",
                type="range",
                default=0.8,
                min=0,
                max=1,
                step=0.05,
            ),
            ParamDescriptor(
                name="deemphasis_tau",
                label="De-emphasis",
                type="select",
                default="75e-6",
                options=["75e-6", "50e-6"],
            ),
        ]

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        super().__init__(params)
        if not HAS_CUPY:
            msg = "CuPy is required for FM demod (pip install cupy-cuda11x)"
            raise RuntimeError(msg)

        # Merge defaults
        for k, v in self._DEFAULTS.items():
            self._params.setdefault(k, v)

        if not self._params.get("channel_freq_hz"):
            self._params["channel_freq_hz"] = 101_900_000

        self._running = False
        self._input_queue: queue.Queue[tuple[np.ndarray, int, int]] = queue.Queue(maxsize=8)
        self._thread: threading.Thread | None = None

        # DSP state (initialized on first chunk)
        self._phase_acc = 0.0  # frequency shift phase accumulator
        self._prev_sample: Any = None  # for FM demod continuity
        self._deemph_state = 0.0  # de-emphasis filter state
        self._lp_taps: Any = None  # GPU lowpass filter taps
        self._audio_lp_taps: Any = None  # GPU audio lowpass taps
        self._intermediate_rate = 0  # after first decimation
        self._decim1 = 0  # first decimation factor
        self._decim2 = 0  # second decimation factor (to audio rate)
        self._chunks_processed = 0
        self._needs_reinit = False

    def configure(self, params: dict[str, Any]) -> None:
        for k, v in params.items():
            if k in self._DEFAULTS or k == "channel_freq_hz":
                self._params[k] = v
        # Flag for reinit on next chunk (don't null taps — race with processing thread)
        self._needs_reinit = True

    def feed(self, sc16_buf: np.ndarray, center_freq_hz: int, sample_rate: int) -> None:
        with contextlib.suppress(queue.Full):
            self._input_queue.put_nowait((sc16_buf.copy(), center_freq_hz, sample_rate))

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._process_loop, name=f"fm-{self.module_id}", daemon=True
        )
        self._thread.start()
        logger.info(
            "FM demod started: %s (%.1f MHz)", self.module_id, self._params["channel_freq_hz"] / 1e6
        )

    def stop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("FM demod stopped: %s", self.module_id)

    def status(self) -> dict[str, Any]:
        return {
            "module_id": self.module_id,
            "module_type": self.module_type,
            "running": self._running,
            "channel_freq_hz": self._params["channel_freq_hz"],
            "channel_bw_hz": self._params["channel_bw_hz"],
            "audio_rate": self._params["audio_rate"],
            "volume": self._params["volume"],
            "chunks_processed": self._chunks_processed,
        }

    def _process_loop(self) -> None:
        """Main processing thread — pulls chunks, runs GPU pipeline."""
        try:
            while self._running:
                try:
                    sc16_buf, center_freq, sample_rate = self._input_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                audio = self._demod_chunk(sc16_buf, center_freq, sample_rate)
                if audio is not None and len(audio) > 0:
                    # Scale and convert to int16 PCM
                    vol = float(self._params.get("volume", 0.8))
                    pcm = (audio * vol * 32000).astype(np.int16)
                    with contextlib.suppress(Exception):
                        self._output_queue.put_nowait(pcm.tobytes())

                self._chunks_processed += 1

        except Exception:
            logger.exception("FM demod processing loop crashed: %s", self.module_id)

    def _demod_chunk(
        self, sc16_buf: np.ndarray, center_freq: int, sample_rate: int
    ) -> np.ndarray | None:
        """GPU FM demodulation pipeline for one chunk."""
        p = self._params
        channel_freq = int(p["channel_freq_hz"])
        channel_bw = int(p["channel_bw_hz"])
        audio_rate = int(p["audio_rate"])
        tau = float(p["deemphasis_tau"])

        # Frequency offset from capture center
        delta_f = channel_freq - center_freq

        # Initialize filter taps on first call or after reconfigure
        if self._lp_taps is None or self._intermediate_rate == 0 or self._needs_reinit:
            self._needs_reinit = False
            self._phase_acc = 0.0
            self._prev_sample = None
            self._deemph_state = 0.0
            self._init_filters(sample_rate, channel_bw, audio_rate)

        # 1. SC16 → complex64 on GPU
        gpu_buf = cp.asarray(sc16_buf)
        raw16 = gpu_buf.view(cp.int16).reshape(-1, 2)
        n = raw16.shape[0]
        iq = cp.empty(n, dtype=cp.complex64)
        iq.real = raw16[:, 0]
        iq.imag = raw16[:, 1]
        iq *= cp.float32(1.0 / 32768.0)

        # 2. Frequency shift
        t = cp.arange(n, dtype=cp.float64) / sample_rate + self._phase_acc
        phase = cp.float64(-2.0 * float(cp.pi) * delta_f) * t
        shift = cp.exp(1j * phase).astype(cp.complex64)
        iq *= shift
        self._phase_acc += n / sample_rate
        # Keep phase accumulator bounded to avoid precision loss
        if self._phase_acc > 1e6:
            self._phase_acc -= int(self._phase_acc)

        # 3. Lowpass filter + decimate to intermediate rate
        #    Decimate FIRST then filter the smaller signal (much faster)
        if self._decim1 > 1:
            iq_dec = iq[:: self._decim1]
            iq_dec = self._fir_filter_gpu(iq_dec, self._lp_taps)
        else:
            iq_dec = iq

        # 4. FM demodulate: instantaneous frequency
        if self._prev_sample is not None:
            prev = cp.concatenate([cp.asarray([self._prev_sample]), iq_dec[:-1]])
        else:
            prev = cp.concatenate([cp.zeros(1, dtype=cp.complex64), iq_dec[:-1]])
        self._prev_sample = complex(iq_dec[-1].get())

        # angle(x[n] * conj(x[n-1]))
        product = iq_dec * cp.conj(prev)
        demod = cp.arctan2(product.imag, product.real)

        # Scale: output = demod * fs / (2π * max_deviation)
        max_dev = 75_000.0  # 75 kHz for broadcast FM
        demod *= cp.float32(self._intermediate_rate / (2.0 * cp.pi * max_dev))

        # 5. De-emphasis filter (IIR on GPU via cumulative sum approximation)
        #    y[n] = alpha * x[n] + (1-alpha) * y[n-1]
        #    Equivalent to exponential moving average — use scipy.signal.lfilter
        #    on CPU (C-optimized, ~100x faster than Python for-loop)
        alpha = float(1.0 / (1.0 + tau * self._intermediate_rate))
        audio_inter = cp.asnumpy(demod).astype(np.float32)
        from scipy.signal import lfilter

        b_coeff = np.array([alpha], dtype=np.float32)
        a_coeff = np.array([1.0, -(1.0 - alpha)], dtype=np.float32)
        audio_inter, zf = lfilter(
            b_coeff,
            a_coeff,
            audio_inter,
            zi=np.array([self._deemph_state * (1.0 - alpha)], dtype=np.float32),
        )
        self._deemph_state = float(zf[0] / (1.0 - alpha)) if (1.0 - alpha) != 0 else 0.0

        # 6. Decimate to audio rate
        return audio_inter[:: self._decim2] if self._decim2 > 1 else audio_inter

    def _init_filters(self, sample_rate: int, channel_bw: int, audio_rate: int) -> None:
        """Pre-compute FIR filter taps on the GPU."""
        from scipy.signal import firwin

        # First decimation: sample_rate → intermediate (~250 kHz)
        self._intermediate_rate = max(channel_bw, 250_000)
        self._decim1 = max(1, sample_rate // self._intermediate_rate)
        self._intermediate_rate = sample_rate // self._decim1

        # Lowpass taps for first decimation
        cutoff = channel_bw / 2
        num_taps = min(255, self._decim1 * 8 + 1)
        if num_taps % 2 == 0:
            num_taps += 1
        taps1 = firwin(num_taps, cutoff, fs=sample_rate).astype(np.float32)
        self._lp_taps = cp.asarray(taps1)

        # Second decimation: intermediate → audio_rate
        self._decim2 = max(1, self._intermediate_rate // audio_rate)
        if self._decim2 > 1:
            cutoff2 = audio_rate / 2 * 0.9
            num_taps2 = min(127, self._decim2 * 8 + 1)
            if num_taps2 % 2 == 0:
                num_taps2 += 1
            taps2 = firwin(num_taps2, cutoff2, fs=self._intermediate_rate).astype(np.float32)
            self._audio_lp_taps = cp.asarray(taps2)
        else:
            self._audio_lp_taps = None

        logger.info(
            "FM filters: %d Hz → %d Hz (÷%d) → %d Hz (÷%d), taps=%d+%d",
            sample_rate,
            self._intermediate_rate,
            self._decim1,
            audio_rate,
            self._decim2,
            len(taps1),
            len(taps2) if self._decim2 > 1 else 0,
        )

    @staticmethod
    def _fir_filter_gpu(signal: Any, taps: Any) -> Any:
        """Apply FIR filter on GPU via overlap-add FFT convolution."""
        n = len(signal)
        m = len(taps)
        # Use FFT convolution for efficiency
        fft_len = 1
        while fft_len < n + m - 1:
            fft_len *= 2

        if signal.dtype == cp.complex64 or signal.dtype == cp.complex128:
            sig_fft = cp.fft.fft(signal, fft_len)
            tap_fft = cp.fft.fft(taps.astype(cp.complex64), fft_len)
            result = cp.fft.ifft(sig_fft * tap_fft)[:n]
            return result.astype(cp.complex64)
        else:
            sig_fft = cp.fft.rfft(signal.astype(cp.float32), fft_len)
            tap_fft = cp.fft.rfft(taps.astype(cp.float32), fft_len)
            result = cp.fft.irfft(sig_fft * tap_fft, fft_len)[:n]
            return result.astype(cp.float32)
