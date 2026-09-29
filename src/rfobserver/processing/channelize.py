"""Channelize a detected burst out of wideband IQ into a narrowband .cs16 blob
that rtl_433 can decode, and pack it as interleaved little-endian int16. Pure
DSP - no file or subprocess I/O.

The burst is channelized in the frequency domain, block by block (overlap-save):
each block's FFT keeps only the output band's bins around the burst's offset (a
raised-cosine taper on the outer edges limits ringing) and one short inverse
FFT per block yields the decimated output. That is the same shift-to-DC plus
low-pass plus decimate as the time-domain mixer and resample_poly it replaced
(ported from gr-modules ssn_scan.py) at a fraction of the cost: every input
sample goes through one FFT, where the polyphase filter ran a long FIR over the
upsampled stream. The FFTs run on one thread: on nano-super, workers=-1 was no
faster (the transforms are memory-bound) and would compete with the receiver.
"""

from __future__ import annotations

from math import gcd

import numpy as np
from scipy import fft as sfft

# Bursts at or above this bandwidth take the 1.6 Msps SSN-mesh tier; narrower
# bursts take the 1.0 Msps full-default-decoder tier. Bandwidth-keyed per the
# spec's fixed two-tier policy.
TIER_BANDWIDTH_HZ: float = 200_000.0

_SSN_FLEX = "n=ssnmesh,m=FSK_PCM,s=16,l=16,r=8000"

# Fraction of the kept band, on each side, over which the taper rolls off.
TAPER_FRACTION = 0.05
# Overlap-save geometry: input samples per block FFT (about), output samples
# per block (at least), and output samples discarded at each block edge. The
# taper's impulse response is about 20 output samples long, inside the
# discarded edge. A block holds a whole number of up/down periods so the
# output rate is exact (26 -> 1.6 Msps: 512 x 65 = 33280 in, 2048 out). On
# nano-super these blocks were about 20% faster than 65536-sample ones and
# the output matched the resample_poly path equally well (-58 dB).
BLOCK_IN = 32_768
BLOCK_OUT_MIN = 1_024
EDGE_OUT = 64
# Blocks transformed per FFT call (about 4 MB of spectrum at BLOCK_IN).
BLOCKS_PER_CALL = 16
# Rates whose ratio does not reduce to a block of at most this many input
# samples are channelized with one FFT of the whole burst instead.
BLOCK_IN_MAX = 1 << 20


def resample_ratio(sample_rate_hz: int, target_rate_hz: int) -> tuple[int, int]:
    """Return the gcd-reduced (up, down) that takes sample_rate_hz ->
    target_rate_hz."""
    g = gcd(int(sample_rate_hz), int(target_rate_hz))
    return int(target_rate_hz) // g, int(sample_rate_hz) // g


def _taper(n_out: int) -> np.ndarray:
    """Weights for n_out kept bins in FFT order: 1 in the middle, a raised
    cosine to near 0 over the outer TAPER_FRACTION on each side."""
    m = np.abs(np.fft.fftfreq(n_out, 1.0 / n_out))
    half = n_out / 2.0
    edge = max(1.0, TAPER_FRACTION * n_out)
    x = np.clip((m - (half - edge)) / edge, 0.0, 1.0)
    w: np.ndarray = (0.5 * (1.0 + np.cos(np.pi * x))).astype(np.float32)
    return w


def _select(spec: np.ndarray, k0: int, n_out: int) -> np.ndarray:
    """The n_out bins centred on bin k0 (wrapping), tapered, in FFT order."""
    n_in = spec.shape[-1]
    m = np.fft.fftfreq(n_out, 1.0 / n_out).astype(np.int64)
    w = _taper(n_out)
    if n_out > n_in:  # upsampling: only the input's own bins exist
        w[2 * np.abs(m) >= n_in] = 0.0
    sel: np.ndarray = spec[..., (k0 + m) % n_in] * w
    return sel


def _channelize_blocks(
    iq: np.ndarray, fs: float, offset_hz: float, up: int, down: int, n_keep: int, k: int, j: int
) -> np.ndarray:
    """Overlap-save: blocks of k * down input samples, k * up output samples,
    of which j * up at each edge are discarded."""
    l_in, l_out = k * down, k * up
    o_in, o_out = j * down, j * up
    h_in, h_out = l_in - 2 * o_in, l_out - 2 * o_out
    nb = -(-n_keep // h_out)
    x = np.asarray(iq, dtype=np.complex64)
    n = len(x)
    bin_hz = fs / l_in
    k0 = int(round(offset_hz / bin_hz))
    out = np.empty((nb, l_out), dtype=np.complex64)
    # A few blocks at a time, straight from the input (only the first and
    # last chunks need a zero-padded copy), so the working set stays small and
    # no burst-sized buffer is allocated and faulted in.
    for c in range(0, nb, BLOCKS_PER_CALL):
        c1 = min(nb, c + BLOCKS_PER_CALL)
        lo = c * h_in - o_in  # input position of this chunk's first block
        hi = (c1 - 1) * h_in - o_in + l_in
        if lo >= 0 and hi <= n:
            seg = x[lo:hi]
        else:
            seg = np.zeros(hi - lo, dtype=np.complex64)
            a, b = max(lo, 0), min(hi, n)
            if b > a:
                seg[a - lo : b - lo] = x[a:b]
        blocks = np.lib.stride_tricks.sliding_window_view(seg, l_in)[::h_in]
        spec = sfft.fft(blocks, axis=-1)
        out[c:c1] = sfft.ifft(_select(spec, k0, l_out), axis=-1, overwrite_x=True)
    # Each block was shifted by k0 bins with its phase restarting at its own
    # first sample. Restore the phase of one continuous mix by offset_hz from
    # the burst's first sample, and remove the residual under half a bin.
    rate = fs * up / down
    s_b = np.arange(nb, dtype=np.float64) * h_in - o_in  # block starts, input samples
    cyc_b = np.mod(s_b * (offset_hz / fs), 1.0)
    i = np.arange(l_out, dtype=np.float64)
    phase = -2.0 * np.pi * (cyc_b[:, None] + ((offset_hz - k0 * bin_hz) / rate) * i[None, :])
    out *= np.exp(1j * phase).astype(np.complex64)
    out *= np.float32(l_out / l_in)
    return np.ascontiguousarray(out[:, o_out : l_out - o_out]).reshape(-1)[:n_keep]


def _channelize_whole(
    iq: np.ndarray, fs: float, offset_hz: float, up: int, down: int, n_keep: int
) -> np.ndarray:
    """One FFT of the whole burst: for rates that do not block well.

    n_out is rounded to next_fast_len rather than an exact up/down multiple,
    so the actual output rate is about 12-25 ppm off target_rate_hz depending
    on burst length; harmless for rtl_433's decoders.

    Working-set note: the RAM guard's _ISOLATION_WORKING_SET_FACTOR (3, in
    streaming.py) was measured on the block path above (_channelize_blocks,
    ~1.4 copies); this whole-burst fallback holds the full FFT/IFFT arrays at
    once and measured about 2.3 copies instead.
    """
    n_in = sfft.next_fast_len(len(iq))
    n_out = -(-n_in * up // down)  # >= n_keep
    buf = np.zeros(n_in, dtype=np.complex64)
    buf[: len(iq)] = iq
    spec = sfft.fft(buf, overwrite_x=True)
    del buf
    bin_hz = fs / n_in
    k0 = int(round(offset_hz / bin_hz))
    out: np.ndarray = sfft.ifft(_select(spec, k0, n_out), overwrite_x=True)[:n_keep]
    rate = fs * n_out / n_in
    w = -2.0 * np.pi * (offset_hz - k0 * bin_hz) / rate
    out *= np.exp(1j * w * np.arange(len(out), dtype=np.float64)).astype(np.complex64)
    out *= np.float32(n_out / n_in)
    return out


def channelize(
    iq: np.ndarray, sample_rate_hz: float, offset_hz: float, target_rate_hz: int
) -> np.ndarray:
    """Shift offset_hz to DC and decimate to target_rate_hz, as complex64.

    The output has ceil(len(iq) * up / down) samples, as resample_poly gave.
    """
    fs = float(sample_rate_hz)
    up, down = resample_ratio(int(sample_rate_hz), int(target_rate_hz))
    n_keep = -(-len(iq) * up // down)
    if n_keep == 0:
        return np.zeros(0, dtype=np.complex64)
    j = -(-EDGE_OUT // up)
    k = max(-(-BLOCK_IN // down), -(-BLOCK_OUT_MIN // up), 4 * j)
    k = sfft.next_fast_len(k)
    if k * down > BLOCK_IN_MAX:
        res = _channelize_whole(iq, fs, float(offset_hz), up, down, n_keep)
    else:
        res = _channelize_blocks(iq, fs, float(offset_hz), up, down, n_keep, k, j)
    return np.asarray(res, dtype=np.complex64)


def channelize_to_cs16(
    iq: np.ndarray, sample_rate_hz: float, offset_hz: float, target_rate_hz: int
) -> bytes:
    """Shift offset_hz to DC, decimate to target_rate_hz, pack as <i2 I/Q
    peak-normalized to 30000."""
    res = channelize(iq, sample_rate_hz, offset_hz, target_rate_hz)
    peak = float(np.max(np.abs(res))) if len(res) else 1.0
    scale = np.float32(30000.0 / (peak or 1.0))
    scaled = res * scale
    out = np.empty(len(res) * 2, dtype="<i2")
    out[0::2] = scaled.real.astype("<i2")
    out[1::2] = scaled.imag.astype("<i2")
    return bytes(out.tobytes())


def select_rate_and_protocols(bandwidth_hz: float) -> tuple[int, list[list[str]]]:
    """Bandwidth-keyed two-tier policy. Returns (target_rate_hz, passes) where
    each pass is an rtl_433 argument list (empty list = full default -R set)."""
    if bandwidth_hz >= TIER_BANDWIDTH_HZ:
        return 1_600_000, [["-R", "383"], ["-X", _SSN_FLEX]]
    return 1_000_000, [[]]
