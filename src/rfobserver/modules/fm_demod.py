"""FM demodulator module — GPU-accelerated via CuPy.

Receives wideband SC16 IQ, frequency-shifts to an FM channel,
downsamples, FM-demodulates, and outputs 48 kHz mono PCM audio
via a queue for WebSocket streaming to the browser.
"""

from __future__ import annotations

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

        # DSP state, rebuilt by _init_dsp when the tuning, chunk size or
        # parameters change.
        self._dsp_key: tuple[Any, ...] | None = None
        self._decim1 = 1  # sample rate -> intermediate (box-car average)
        self._intermediate_rate = 0.0  # Hz, exact (sample_rate / decim1)
        self._nco: Any = None  # GPU mixing tone for one chunk, starting at phase 0
        self._nco_step = 0.0  # its phase advance per chunk (radians)
        self._nco_phase = 0.0  # phase at the next chunk's first sample
        self._prev_sample: complex | None = None  # FM discriminator continuity
        # Mixed samples left over when a chunk is not a multiple of the
        # decimation (2,048,000 / 224 leaves 192): they start the next chunk.
        # Dropping them made a phase jump, a click, at every chunk boundary.
        self._tail: Any = None
        self._deemph_zi: np.ndarray = np.zeros(1)
        self._audio_taps: np.ndarray = np.ones(1)
        self._audio_zi: np.ndarray = np.zeros(0)
        self._rs_step = 1.0  # intermediate samples per output sample
        self._rs_pos = 0.0  # next output's position, in the next chunk's samples
        self._rs_last: float | None = None  # last intermediate sample

        # Diagnostics for status().
        self._chunks_processed = 0
        self._chunks_dropped = 0
        self._proc_ms = 0.0
        self._error: str | None = None

    def configure(self, params: dict[str, Any]) -> None:
        for k, v in params.items():
            if k in self._DEFAULTS or k == "channel_freq_hz":
                self._params[k] = v
        # A new frequency, bandwidth or de-emphasis changes the DSP key, so the
        # processing thread rebuilds its state on the next chunk; volume is
        # read per chunk and needs no rebuild (a rebuild would click).

    def feed(self, sc16_buf: np.ndarray, center_freq_hz: int, sample_rate: int) -> None:
        try:
            self._input_queue.put_nowait((sc16_buf.copy(), center_freq_hz, sample_rate))
        except queue.Full:
            self._chunks_dropped += 1  # demod behind: this chunk is a gap in the audio

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
            "chunks_dropped": self._chunks_dropped,
            "audio_dropped": self.output_dropped,
            "proc_ms": round(self._proc_ms, 1),
            "error": self._error,
        }

    def _process_loop(self) -> None:
        """Main processing thread: pulls chunks, runs the GPU pipeline."""
        import time

        logged = False
        while self._running:
            try:
                sc16_buf, center_freq, sample_rate = self._input_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            t0 = time.perf_counter()
            try:
                audio = self._demod_chunk(sc16_buf, center_freq, sample_rate)
            except Exception as exc:
                # Keep running (a retune or new parameters may fix it) but say so.
                self._error = f"Demodulation failed: {type(exc).__name__}: {exc}"
                if not logged:
                    logger.exception("FM demod failed: %s", self.module_id)
                    logged = True
                continue
            if audio is not None and len(audio) > 0:
                vol = float(self._params.get("volume", 0.8))
                # Clip: past full deviation the int16 cast would wrap (crackle).
                pcm = np.clip(audio * (vol * 32000.0), -32767, 32767).astype(np.int16)
                self.emit(pcm.tobytes())
            self._chunks_processed += 1
            ms = (time.perf_counter() - t0) * 1000.0
            self._proc_ms = ms if self._chunks_processed == 1 else 0.9 * self._proc_ms + 0.1 * ms

    def _demod_chunk(
        self, sc16_buf: np.ndarray, center_freq: int, sample_rate: int
    ) -> np.ndarray | None:
        """GPU FM demodulation of one chunk; float32 audio at audio_rate, or
        None when the channel is outside the captured band."""
        p = self._params
        channel_freq = float(p["channel_freq_hz"])
        channel_bw = float(p["channel_bw_hz"])
        delta_f = channel_freq - center_freq
        n = len(sc16_buf)

        if abs(delta_f) + channel_bw / 2 > sample_rate / 2:
            lo, hi = (center_freq - sample_rate / 2) / 1e6, (center_freq + sample_rate / 2) / 1e6
            self._error = (
                f"{channel_freq / 1e6:.1f} MHz is outside the captured band "
                f"({lo:.1f} to {hi:.1f} MHz)"
            )
            return None
        self._error = None

        key = (sample_rate, center_freq, n, channel_freq, channel_bw, p["deemphasis_tau"])
        if key != self._dsp_key:
            self._init_dsp(sample_rate, delta_f, n)
            self._dsp_key = key

        # 1. SC16 -> complex64 on the GPU.
        raw16 = cp.asarray(sc16_buf).view(cp.int16).reshape(-1, 2)
        iq = cp.empty(n, dtype=cp.complex64)
        iq.real = raw16[:, 0]
        iq.imag = raw16[:, 1]

        # 2. Shift the channel to 0 Hz: the precomputed tone times this chunk's
        #    starting phase (the scale to +-1 folds into the same multiply).
        start = np.complex64(np.exp(-1j * self._nco_phase) / 32768.0)
        iq *= self._nco
        iq *= start
        self._nco_phase = (self._nco_phase + self._nco_step) % (2.0 * np.pi)

        # 3. Decimate to the intermediate rate (~250 kHz) by box-car averaging:
        #    a sinc anti-alias filter, fast on the GPU. The remainder carries
        #    over, so the stream stays contiguous.
        d = self._decim1
        if d > 1:
            if self._tail is not None and len(self._tail):
                iq = cp.concatenate([self._tail, iq])
            used = (len(iq) // d) * d
            self._tail = iq[used:].copy()
            iq_dec = iq[:used].reshape(-1, d).mean(axis=1)
        else:
            iq_dec = iq

        # 4. FM discriminator: angle(x[n] * conj(x[n-1])), continuous across chunks.
        first = cp.asarray(
            [self._prev_sample if self._prev_sample is not None else iq_dec[0].item()],
            dtype=cp.complex64,
        )
        prev = cp.concatenate([first, iq_dec[:-1]])
        self._prev_sample = complex(iq_dec[-1].item())
        product = iq_dec * cp.conj(prev)
        demod = cp.arctan2(product.imag, product.real)
        # Scale to +-1 at 75 kHz deviation (broadcast FM).
        demod *= cp.float32(self._intermediate_rate / (2.0 * np.pi * 75_000.0))
        x = cp.asnumpy(demod).astype(np.float64)

        from scipy.signal import lfilter

        # 5. De-emphasis (one-pole low-pass), state carried across chunks.
        tau = float(p["deemphasis_tau"])
        alpha = 1.0 / (1.0 + tau * self._intermediate_rate)
        x, self._deemph_zi = lfilter([alpha], [1.0, alpha - 1.0], x, zi=self._deemph_zi)

        # 6. Audio low-pass (15 kHz) before resampling, state carried across chunks.
        x, self._audio_zi = lfilter(self._audio_taps, [1.0], x, zi=self._audio_zi)

        # 7. Resample to exactly audio_rate by interpolating at a continuous
        #    fractional position (the intermediate rate is rarely a multiple).
        return self._resample(x)

    def _resample(self, x: np.ndarray) -> np.ndarray:
        if self._rs_last is not None:
            x = np.concatenate([[self._rs_last], x])
            pos = self._rs_pos + 1.0  # positions were relative to the next chunk
        else:
            pos = self._rs_pos
        last = len(x) - 1
        if last < pos:
            self._rs_pos = pos - len(x)
            self._rs_last = float(x[-1])
            return np.zeros(0, dtype=np.float32)
        count = int((last - pos) // self._rs_step) + 1
        at = pos + self._rs_step * np.arange(count)
        out = np.interp(at, np.arange(len(x)), x).astype(np.float32)
        # Where the next output falls, relative to the next chunk's first sample.
        self._rs_pos = at[-1] + self._rs_step - len(x)
        self._rs_last = float(x[-1])
        return out

    def _init_dsp(self, sample_rate: int, delta_f: float, n: int) -> None:
        """Rebuild filters, mixing tone and resampler for this tuning."""
        from scipy.signal import firwin, lfilter_zi

        audio_rate = int(self._params["audio_rate"])
        channel_bw = float(self._params["channel_bw_hz"])
        self._decim1 = max(1, int(sample_rate // max(channel_bw, 250_000)))
        self._intermediate_rate = sample_rate / self._decim1

        # Mixing tone for one chunk (float64 phase, then complex64), so a chunk
        # costs two multiplies instead of a full-rate exp().
        w = -2.0 * np.pi * delta_f / sample_rate
        k = cp.arange(n, dtype=cp.float64)
        self._nco = cp.exp(1j * w * k).astype(cp.complex64)
        self._nco_step = (2.0 * np.pi * delta_f * n / sample_rate) % (2.0 * np.pi)
        self._nco_phase = 0.0

        self._prev_sample = None
        self._tail = None
        self._deemph_zi = np.zeros(1)
        cutoff = min(15_000.0, 0.45 * audio_rate)
        self._audio_taps = firwin(97, cutoff, fs=self._intermediate_rate)
        self._audio_zi = lfilter_zi(self._audio_taps, [1.0]) * 0.0
        self._rs_step = self._intermediate_rate / audio_rate
        self._rs_pos = 0.0
        self._rs_last = None
        logger.info(
            "FM demod %s: %d Hz -> %.1f Hz (/%d) -> %d Hz audio, offset %.3f MHz",
            self.module_id,
            sample_rate,
            self._intermediate_rate,
            self._decim1,
            audio_rate,
            delta_f / 1e6,
        )
