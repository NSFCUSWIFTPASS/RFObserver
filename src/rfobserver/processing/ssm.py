"""Decode the surrogate smart meter (SSM, NSFCUSWIFTPASS/ssm-tx) frame out of a
channelized burst. Pure DSP - no I/O.

The SSM streams one fixed 5184-bit frame per hop as DBPSK (a 1 bit is a 180
degree phase change from the previous symbol) at 62.5 kbaud, RRC alpha 0.35.
The burst is decoded at its own rate (16 samples per symbol at 1 MS/s, 25.6 at
1.6 MS/s; symbols are read by interpolation), which avoids a resample on the
1.6 MS/s tier. The frame is found by correlating with its sync + preamble, then
MAC and text are read from the differential decisions. See
docs/debugging/2026-11-05_ssm-waveform-dbpsk.md for what is on air.
"""

from __future__ import annotations

from functools import lru_cache
from math import gcd
from typing import Any, TypeAlias, cast

import numpy as np
from scipy import fft as sfft
from scipy import signal

IQ: TypeAlias = "np.ndarray[Any, np.dtype[Any]]"

SSM_MAC = 0x0200574952470001
SSM_TEXT = b"Hello from Boulder, CO!" * 4
# A frame is SSM when its 128 preamble bits decode with at most this many
# errors (chance agreement of random bits is about 1e-20).
PREAMBLE_MAX_ERRORS = 16

BAUD = 62_500
RATE_HZ = 1_000_000  # modulate()'s native rate
SPS = 16  # samples per symbol at RATE_HZ
RRC_ALPHA = 0.35
RRC_SPAN = 8  # symbols
# Offsets searched: +-15 kHz (the squared signal's tone sits at twice it).
CFO_SEARCH_HZ = 15_000
# The offset is estimated on this many samples from the middle of the burst
# (32.8 ms: 30 Hz resolution, ample for 62.5 kbaud).
CFO_SEGMENT = 32_768
# The frame start is searched on every DECIM-th sample, then refined.
DECIM = 4

# Frame layout, bit indices (half-open).
PREAMBLE = (32, 160)
MAC = (160, 224)
TEXT = (224, 960)
FIXED = (32, 4960)  # preamble, MAC, text, Gold code: fixed and always sent
FRAME_BITS = 5184


def _bits_of(value: int, n: int) -> IQ:
    return np.array([(value >> (n - 1 - i)) & 1 for i in range(n)], dtype=np.uint8)


def _lfsr(taps: list[int], state: IQ, n: int) -> IQ:
    s = state.copy()
    out = np.empty(n, dtype=np.uint8)
    for i in range(n):
        out[i] = s[-1]
        fb = 0
        for t in taps:
            fb ^= int(s[t])
        s = np.concatenate([[fb], s[:-1]])
    return out


def _gold_code() -> IQ:
    """ssm-tx's generate_gold_code(n=7, transmitter_id=1)."""
    pn1 = _lfsr([0, 6], np.ones(7, dtype=np.uint8), 127)
    pn2 = _lfsr([0, 1], _bits_of(1, 7), 127)
    return cast("IQ", pn1 ^ pn2)


def _crc64(data: bytes) -> int:
    """CRC-64/ECMA-182 as ssm-tx's generator computes it (the CRC on air
    differs; it is not checked)."""
    crc, poly = 0xFFFFFFFFFFFFFFFF, 0x42F0E1EBA9EA3693
    for byte in data:
        crc ^= byte << 56
        for _ in range(8):
            crc = ((crc << 1) ^ poly) if crc & (1 << 63) else (crc << 1)
            crc &= 0xFFFFFFFFFFFFFFFF
    return crc ^ 0xFFFFFFFFFFFFFFFF


def frame_bits(mac: int = SSM_MAC, text: bytes = SSM_TEXT) -> IQ:
    """The 5184-bit SSM frame for ``mac`` and a 92-byte ``text``."""
    if len(text) != (TEXT[1] - TEXT[0]) // 8:
        raise ValueError(f"SSM text must be 92 bytes, got {len(text)}")
    barker = np.array([1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1], dtype=np.uint8)
    gold = _gold_code()
    text_bits = np.unpackbits(np.frombuffer(text, dtype=np.uint8))
    gold_bits = np.concatenate([np.tile(gold, 31), gold[:63]])
    crc = _crc64(np.packbits(np.concatenate([text_bits, gold_bits])).tobytes())
    frame = np.concatenate(
        [
            _bits_of(0xAAAAAAAA, 32),
            np.tile(barker, 10)[:128],
            _bits_of(mac, 64),
            text_bits,
            gold_bits,
            _bits_of(crc, 64),
            np.tile(1 - barker, 10)[:128],
            _bits_of(0x55555555, 32),
        ]
    )
    return cast("IQ", frame.astype(np.uint8))


@lru_cache(maxsize=4)
def _rrc(sps: float = SPS) -> IQ:
    n = int(round(RRC_SPAN * sps))
    t = (np.arange(n) - n / 2) / sps
    a = RRC_ALPHA
    h = np.empty(n)
    for i, x in enumerate(t):
        if x == 0:
            h[i] = 1 + a * (4 / np.pi - 1)
        elif abs(abs(x) - 1 / (4 * a)) < 1e-10:
            h[i] = (a / np.sqrt(2)) * (
                (1 + 2 / np.pi) * np.sin(np.pi / (4 * a))
                + (1 - 2 / np.pi) * np.cos(np.pi / (4 * a))
            )
        else:
            num = np.sin(np.pi * x * (1 - a)) + 4 * a * x * np.cos(np.pi * x * (1 + a))
            h[i] = num / (np.pi * x * (1 - (4 * a * x) ** 2))
    return cast("IQ", (h / np.sqrt(np.sum(h**2))).astype(np.float32))


def _resample(iq: IQ, from_hz: int, to_hz: int) -> IQ:
    """Polyphase resample with a short low-pass: the SSM occupies about
    +-42 kHz, so a cutoff at 0.3 x the lower rate passes it untouched and the
    filter can be a fraction of resample_poly's default (41 taps for 1.6 to
    1 MS/s, a third of the time, -49 dB from the default's output)."""
    if from_hz == to_hz:
        return iq
    g = gcd(int(from_hz), int(to_hz))
    up, down = to_hz // g, from_hz // g
    taps = signal.firwin(5 * max(up, down) + 1, 0.3 * min(from_hz, to_hz), fs=up * from_hz)
    return cast("IQ", signal.resample_poly(iq, up, down, window=taps).astype(np.complex64))


def modulate(bits: IQ, rate_hz: int = RATE_HZ) -> IQ:
    """DBPSK + RRC baseband of ``bits`` at 1 MS/s, resampled to ``rate_hz``.
    Symbol k sits at sample 16 k (at 1 MS/s)."""
    symbols = np.where(np.cumsum(bits) % 2 == 1, -1.0, 1.0).astype(np.float32)
    up = np.zeros(len(symbols) * SPS, dtype=np.float32)
    up[::SPS] = symbols
    bb = np.convolve(up, _rrc(), mode="same").astype(np.complex64)
    return _resample(bb, RATE_HZ, rate_hz)


@lru_cache(maxsize=4)
def _reference(rate_hz: int) -> tuple[IQ, IQ]:
    """(matched-filtered sync + preamble waveform at ``rate_hz``, the known
    frame bits)."""
    bits = frame_bits()
    wave = modulate(bits[: PREAMBLE[1]], rate_hz)
    ref = signal.oaconvolve(wave, _rrc(rate_hz / BAUD), mode="same")
    return cast("IQ", ref.astype(np.complex64)), bits


def _payload(raw: bytes) -> str:
    for p in range(1, len(raw) + 1):
        if len(raw) % p == 0 and raw == raw[:p] * (len(raw) // p):
            raw = raw[:p]
            break
    return "".join(chr(b) if 0x20 <= b <= 0x7E else "?" for b in raw)


def _as_int(bits: IQ) -> int:
    return int("".join(str(int(b)) for b in bits), 2)


def decode_ssm(cs16: bytes, rate_hz: int) -> dict[str, Any] | None:
    """The SSM frame in ``cs16`` (<i2 I/Q at ``rate_hz``) as an rtl_433-shaped
    frame dict, or None when there is no SSM frame in it."""
    raw = np.frombuffer(cs16[: len(cs16) // 4 * 4], dtype="<i2").astype(np.float32)
    iq = (raw[0::2] + 1j * raw[1::2]).astype(np.complex64)
    rate = int(rate_hz)
    sps = rate / BAUD
    if len(iq) < TEXT[1] * sps:
        return None

    # Frequency offset: BPSK squared is a tone at twice the offset.
    mid = max(0, (len(iq) - CFO_SEGMENT) // 2)
    seg = iq[mid : mid + CFO_SEGMENT]
    n_fft = sfft.next_fast_len(len(seg))
    spec = np.abs(sfft.fft(seg * seg, n_fft))
    freqs = sfft.fftfreq(n_fft, 1.0 / rate)
    near = np.abs(freqs) <= 2 * CFO_SEARCH_HZ
    cfo = float(freqs[near][np.argmax(spec[near])]) / 2
    step = np.float32(-2 * np.pi * cfo / rate)
    phase = np.arange(len(iq), dtype=np.float32) * step
    iq = iq * (np.cos(phase) + 1j * np.sin(phase)).astype(np.complex64)

    y = signal.oaconvolve(iq, _rrc(sps), mode="same")
    ref, known = _reference(rate)
    if len(y) < len(ref):
        return None
    coarse = signal.oaconvolve(y[::DECIM], np.conj(ref[::DECIM][::-1]), mode="valid")
    guess = int(np.argmax(np.abs(coarse))) * DECIM
    cands = [s for s in range(guess - DECIM, guess + DECIM + 1) if 0 <= s <= len(y) - len(ref)]
    rc = np.conj(ref)
    start = max(cands, key=lambda s: abs(np.dot(y[s : s + len(ref)], rc)))

    n_sym = min(FRAME_BITS, int((len(y) - 1 - start) / sps) + 1)
    if n_sym < TEXT[1]:
        return None
    pos = start + np.arange(n_sym) * sps
    grid = np.arange(len(y))
    sym = np.interp(pos, grid, y.real) + 1j * np.interp(pos, grid, y.imag)
    bits = np.zeros(n_sym, dtype=np.uint8)
    bits[1:] = (np.real(sym[1:] * np.conj(sym[:-1])) < 0).astype(np.uint8)

    lo, hi = PREAMBLE
    errs = int(np.sum(bits[lo:hi] != known[lo:hi]))
    if hi - lo - errs < errs:  # inverted decisions: a 180 degree convention flip
        bits[1:] = 1 - bits[1:]
        errs = hi - lo - errs
    if errs > PREAMBLE_MAX_ERRORS:
        return None

    src_id = f"{_as_int(bits[MAC[0] : MAC[1]]):016x}"
    text = np.packbits(bits[TEXT[0] : TEXT[1]]).tobytes()
    bit_errors = None
    if src_id == f"{SSM_MAC:016x}":
        end = min(FIXED[1], n_sym)
        bit_errors = int(np.sum(bits[FIXED[0] : end] != known[FIXED[0] : end]))
    return {
        "model": "SSM",
        "src_id": src_id,
        "payload": _payload(text),
        "preamble_errors": errs,
        "bit_errors": bit_errors,
    }
