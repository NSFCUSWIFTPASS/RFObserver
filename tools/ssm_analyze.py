"""Find SSM hops in a 26 MS/s ci16 capture, decode each as DBPSK against the
ssm-tx frame, and report CFO, correlation, bit errors, text, MAC and CRC.

REF_DAT is ssm-tx's src/ssm_tx/data/ssm_bpsk_1msps_16sps_norm.dat, the file
the transmitter streams. See docs/debugging/2026-11-05_ssm-waveform-dbpsk.md.

usage: ssm_analyze.py CAPTURE REF_DAT [--seconds S]
"""

import argparse
import sys

import numpy as np
from scipy import signal

FS = 26_000_000
FC = 915_000_000
FS_BB = 1_000_000
SPS = 16
NFFT = 1024
REF_SYM0 = 1313


def frame_bits() -> np.ndarray:
    """The frame bpsk_generator.py builds (5184 bits)."""

    def lfsr(taps, state, n):
        state = state.copy()
        out = []
        for _ in range(n):
            out.append(state[-1])
            fb = 0
            for t in taps:
                fb ^= state[t]
            state = np.concatenate([[fb], state[:-1]])
        return np.array(out, dtype=int)

    pn1 = lfsr([0, 6], np.ones(7, dtype=int), 127)
    pn2 = lfsr([0, 1], np.array([int(b) for b in format(1, "07b")]), 127)
    gold = pn1 ^ pn2

    def bits_of(v, n):
        return np.array([int(b) for b in format(v, f"0{n}b")], dtype=int)

    barker = np.array([1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1])
    pre = np.concatenate([np.tile(barker, 9), barker[:11]])
    text = np.unpackbits(np.frombuffer(("Hello from Boulder, CO!" * 4).encode(), dtype=np.uint8))
    goldb = np.concatenate([np.tile(gold, 31), gold[:63]])
    payload = np.packbits(np.concatenate([text, goldb]))
    crc = crc64(payload.tobytes())
    msg = np.concatenate([text, goldb, bits_of(crc, 64)])
    post = np.concatenate([np.tile(1 - barker, 9), (1 - barker)[:11]])
    return np.concatenate(
        [
            bits_of(0xAAAAAAAA, 32),
            pre,
            bits_of(0x0200574952470001, 64),
            msg,
            post,
            bits_of(0x55555555, 32),
        ]
    ).astype(np.uint8)


def crc64(data: bytes) -> int:
    crc, poly = 0xFFFFFFFFFFFFFFFF, 0x42F0E1EBA9EA3693
    for byte in data:
        crc ^= byte << 56
        for _ in range(8):
            crc = ((crc << 1) ^ poly) if crc & (1 << 63) else (crc << 1)
            crc &= 0xFFFFFFFFFFFFFFFF
    return crc ^ 0xFFFFFFFFFFFFFFFF


def rrc(alpha=0.35, span=8):
    n = span * SPS
    t = (np.arange(n) - n / 2) / SPS
    h = np.zeros(n)
    for i, x in enumerate(t):
        if x == 0:
            h[i] = 1 + alpha * (4 / np.pi - 1)
        elif abs(abs(x) - 1 / (4 * alpha)) < 1e-10:
            h[i] = (alpha / np.sqrt(2)) * (
                (1 + 2 / np.pi) * np.sin(np.pi / (4 * alpha))
                + (1 - 2 / np.pi) * np.cos(np.pi / (4 * alpha))
            )
        else:
            h[i] = (
                np.sin(np.pi * x * (1 - alpha)) + 4 * alpha * x * np.cos(np.pi * x * (1 + alpha))
            ) / (np.pi * x * (1 - (4 * alpha * x) ** 2))
    return h / np.sqrt(np.sum(h**2))


def load(path, start, count):
    raw = np.memmap(path, dtype=np.int16, mode="r")
    seg = np.asarray(raw[2 * start : 2 * (start + count)], dtype=np.float32)
    return (seg[0::2] + 1j * seg[1::2]).astype(np.complex64)


def find_hops(path, seconds):
    """Per 1 ms: averaged 1024-pt spectrum; a hop is a run of ms whose peak
    bin sits 20 dB over the median floor at a steady frequency."""
    blk = FS // 1000
    win = np.hanning(NFFT).astype(np.float32)
    rows = []
    for ms in range(int(seconds * 1000)):
        x = load(path, ms * blk, 8 * NFFT).reshape(8, NFFT) * win
        p = np.mean(np.abs(np.fft.fftshift(np.fft.fft(x, axis=1), axes=1)) ** 2, axis=0)
        p[NFFT // 2 - 2 : NFFT // 2 + 3] = np.median(p)  # DC spike
        k = int(np.argmax(p))
        rows.append((k, 10 * np.log10(p[k] / np.median(p))))
    hops, cur = [], None
    for ms, (k, snr) in enumerate(rows):
        on = snr > 20
        if on and cur and abs(k - cur["k"]) <= 4 and ms - cur["end"] <= 2:
            cur["end"] = ms
            cur["ks"].append(k)
        else:
            if cur and cur["end"] - cur["start"] >= 40:
                hops.append(cur)
            cur = {"start": ms, "end": ms, "k": k, "ks": [k]} if on else None
    if cur and cur["end"] - cur["start"] >= 40:
        hops.append(cur)
    for h in hops:
        h["f_off"] = (np.median(h["ks"]) - NFFT // 2) * FS / NFFT
    return hops


def demod(path, hop, ref, bits, h):
    t0 = max(0, (hop["start"] - 5) * (FS // 1000))
    n = (hop["end"] - hop["start"] + 15) * (FS // 1000)
    x = load(path, t0, n)
    x = x * np.exp(-2j * np.pi * hop["f_off"] * np.arange(len(x)) / FS).astype(np.complex64)
    y = signal.resample_poly(x, 1, FS // FS_BB).astype(np.complex64)
    # CFO: BPSK squared is a tone at 2*cfo.
    sq = y**2
    nz = 1 << 22
    sp = np.abs(np.fft.fft(sq, nz))
    fr = np.fft.fftfreq(nz, 1 / FS_BB)
    m = np.abs(fr) < 30_000
    cfo = fr[m][np.argmax(sp[m])] / 2
    y = y * np.exp(-2j * np.pi * cfo * np.arange(len(y)) / FS_BB)
    c = signal.fftconvolve(y, np.conj(ref[::-1]), mode="valid")
    i = int(np.argmax(np.abs(c)))
    seg = y[i : i + len(ref)]
    rho = np.abs(c[i]) / (np.linalg.norm(ref) * np.linalg.norm(seg) + 1e-12)
    mf = np.convolve(y, h, mode="same")
    # On air the frame is differentially encoded (DBPSK): a bit is the sign
    # change between consecutive symbols. REF_SYM0 is where the reference's
    # first symbol sits.
    idx = i + REF_SYM0 + np.arange(len(bits) + 1) * SPS
    sym = mf[idx[idx < len(mf)]]  # a hop cut off at the end of the window is shorter
    diff = sym[1:] * np.conj(sym[:-1])
    rx = (diff.real < 0).astype(np.uint8)
    c2 = signal.correlate(rx * 2.0 - 1, bits * 2.0 - 1, mode="full")
    k = int(np.argmax(np.abs(c2)))
    lag = k - (len(bits) - 1)
    flip = c2[k] < 0
    if flip:
        rx = 1 - rx
    al = np.full(len(bits), 2, dtype=np.uint8)
    t = np.arange(len(bits))  # frame bit t was received as rx[t + lag]
    ok = (t + lag >= 0) & (t + lag < len(rx))
    al[t[ok]] = rx[t[ok] + lag]
    errs = int(np.sum(al != bits))
    core = int(np.sum(al[32:4960] != bits[32:4960]))  # preamble..gold, all sent
    dn = diff / np.mean(np.abs(diff))
    evm = np.sqrt(np.mean((np.abs(dn.real) - 1) ** 2 + dn.imag**2))
    text = np.packbits(al[224:960] & 1).tobytes()
    mac = int("".join(map(str, al[160:224] & 1)), 2)
    crc_rx = int("".join(map(str, al[4960:5024] & 1)), 2)
    crc_ok = crc64(np.packbits(al[224:4960] & 1).tobytes()) == crc_rx
    return {
        "f_mhz": (FC + hop["f_off"] + cfo) / 1e6,
        "dur_ms": hop["end"] - hop["start"] + 1,
        "cfo_hz": cfo,
        "rho": rho,
        "bit_errors": errs,
        "core_errors": core,
        "evm_pct": 100 * evm,
        "flipped": flip,
        "lag": lag,
        "mac": f"{mac:016x}",
        "text": text[:23],
        "crc_ok": crc_ok,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("capture")
    ap.add_argument("ref")
    ap.add_argument("--seconds", type=float, default=2.0)
    a = ap.parse_args()
    bits = frame_bits()
    ref = np.fromfile(a.ref, dtype=np.complex64)
    h = rrc()
    print(f"reference: {len(ref)} samples, frame {len(bits)} bits")
    hops = find_hops(a.capture, a.seconds)
    print(f"hops found in {a.seconds} s: {len(hops)}")
    for hop in hops:
        r = demod(a.capture, hop, ref, bits, h)
        print(
            f"t={hop['start']:5d} ms  {r['f_mhz']:9.4f} MHz  {r['dur_ms']:3d} ms  "
            f"cfo {r['cfo_hz']:8.1f} Hz  rho {r['rho']:.3f}  "
            f"core err {r['core_errors']:4d}/4928  evm {r['evm_pct']:5.1f}%  "
            f"flip {r['flipped']}  lag {r['lag']}  mac {r['mac']}  "
            f"crc {r['crc_ok']}  text {r['text']!r}"
        )
        sys.stdout.flush()


if __name__ == "__main__":
    main()
