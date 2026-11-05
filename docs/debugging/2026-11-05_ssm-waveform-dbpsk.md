# SSM transmissions: what is on air, and can they be attributed by content

## The question

2026-11-05. Can rtl_433 decode the surrogate smart meter (SSM) BPSK transmissions and attribute
them by matching their text, so RFObserver can label SSM bursts as "SSM"?

Data: `iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z_915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.dat`
(HCRO, rpi-002, B205mini, ci16_le, 26 MS/s at 915 MHz, 20 s, RX gain 35 dB, overflow flagged;
Google Drive folder `iq_data`, 2,080,000,000 bytes). Transmitter source: `NSFCUSWIFTPASS/ssm-tx`
at main, 2026-11-04 (v0.4.1), waveform file `src/ssm_tx/data/ssm_bpsk_1msps_16sps_norm.dat`.

## The answer

rtl_433 cannot: it has no phase demodulator, so a pattern match never gets bits to match. A small
DBPSK decoder can. The SSM streams a differentially encoded BPSK frame (62.5 kbaud, RRC 0.35),
and decoding it that way recovers MAC `0200574952470001` and the text "Hello from Boulder, CO!"
from 82 of the 83 hops in the capture, with 0 errors in the 4928 fixed bits that are sent.

## Procedure

1. **Read the transmitter source** (`bpsk_generator.py`, `ssm_uhd.py`, `ssm_config.yaml`). Gives
   the intended frame: 5184 bits at 62.5 kbaud = 82.944 ms; sync `0xAAAAAAAA` (32), Barker-13
   preamble (128), MAC (64), text x4 (736), Gold code n=7 id 1 (4000), CRC-64/ECMA (64),
   inverted Barker postamble (128), `0x55555555` (32). `ssm_uhd.py` streams the `.dat` above as
   cf32. Controls for: guessing parameters from the air.
2. **Rebuild the frame bits** from the generator code (`frame_bits()` in `tools/ssm_analyze.py`)
   and compare with the reference waveform file sampled at its symbol centres. Isolates: whether
   the file on the transmitter is what the generator describes.
3. **Measure the reference file directly**: clock line of `|diff(x)|^2`, -20 dB width, envelope
   per ms. Isolates: symbol rate and where the frame starts, independent of the generator.
4. **Decode the reference file's own bits**, plain and differential, and search for ASCII and
   for the rebuilt frame by bit correlation. Isolates: the encoding actually used.
5. **Decode the capture.** Per 1 ms, an averaged 1024-point spectrum; a hop is a run of ms with
   the peak bin 20 dB over the median floor at a steady frequency (40 ms minimum). Each hop is
   mixed to baseband, resampled 26 -> 1 MS/s, its frequency offset estimated from the BPSK-squared
   tone (2x offset), located by correlation with the reference file, matched-filtered (RRC),
   and decoded differentially. Bits are aligned to the frame by correlation and scored on the
   preamble, MAC, text and Gold code (frame bits 32 to 4959), which are both fixed and sent.

Script: `tools/ssm_analyze.py CAPTURE REF_DAT --seconds 20`.

## Evidence

Reference file (step 3): 82,944 cf32 samples, imaginary part all zero, clock line at
62,500.75 Hz, -20 dB extent +-39.6 kHz, first `|x| > 0.05` at sample 1308. RMS per ms:

```
[0.   0.92 0.5  0.5  0.47 0.48 0.49 0.48 ... 0.49 0.48]
```

Reference file, plain BPSK bits from the signal start (step 4), eye min/mean 1.812 / 1.941:

```
1011011100110011001100110011001100110010101110110010101000100110...
```

The same bits differentially decoded:

```
1101100101010101010101010101010101010111110011010111111001101011...
pol 0 sh 5 byte 28 b'Hello from Boulder, CO!Hello from Boulder, CO!Hello from Boulder, CO!H'
frame corr 5027 lag 5
```

Per-section disagreement, reference (differential) vs rebuilt frame:

```
{'sync': 1, 'pre': 0, 'mac': 0, 'text': 0, 'gold': 0, 'crc': 33, 'post': 57, 'end': 32}
ref symbols available 5101 needed 5185
```

Capture, first 2 s (step 5):

```
hops found in 2.0 s: 8
t=   52 ms   917.2993 MHz   83 ms  cfo -11249.7 Hz  rho 0.936  core err    0/4928  evm  17.2%  mac 0200574952470001  text b'Hello from Boulder, CO!'
t=  294 ms   923.8993 MHz   82 ms  cfo  12574.0 Hz  rho 0.947  core err    0/4928  evm  17.8%  mac 0200574952470001  text b'Hello from Boulder, CO!'
t=  534 ms   906.1993 MHz   83 ms  cfo   9853.4 Hz  rho 0.936  core err    0/4928  evm  16.9%  mac 0200574952470001  text b'Hello from Boulder, CO!'
t=  778 ms   922.0993 MHz   82 ms  cfo -10080.5 Hz  rho 0.944  core err    0/4928  evm  17.2%  mac 0200574952470001  text b'Hello from Boulder, CO!'
t= 1018 ms   913.3993 MHz   83 ms  cfo  -1089.5 Hz  rho 0.941  core err    0/4928  evm  18.1%  mac 0200574952470001  text b'Hello from Boulder, CO!'
t= 1259 ms   909.4993 MHz   83 ms  cfo   9070.2 Hz  rho 0.935  core err    0/4928  evm  16.6%  mac 0200574952470001  text b'Hello from Boulder, CO!'
t= 1500 ms   919.6993 MHz   83 ms  cfo   2031.3 Hz  rho 0.944  core err    0/4928  evm  17.1%  mac 0200574952470001  text b'Hello from Boulder, CO!'
t= 1741 ms   926.2993 MHz   84 ms  cfo    463.8 Hz  rho 0.950  core err    0/4928  evm  18.1%  mac 0200574952470001  text b'Hello from Boulder, CO!'
```

Full 20 s: 83 hops; 82 with `core err 0/4928` and the text; hop spacing 240 to 244 ms. The one
failure:

```
t=13557 ms   924.4993 MHz   79 ms  cfo 3198.1 Hz  rho 0.623  core err 1050/4928  lag 513  mac 137bab63232b9161
```

The CRC check fails on every hop, including the clean reference file (see below).

## Findings about the transmitter

1. **The streamed waveform is differentially encoded.** `bpsk_generator.py` maps bits straight to
   +-1 symbols; the `.dat` the SSM transmits is the same frame differentially encoded. Read as
   plain BPSK it gives coin-flip bits.
2. **The last ~84 bits are never sent.** The file starts with about 1.3 ms (1308 samples) of
   near silence, then 5101 symbols fit in the remaining samples: the end pattern and part of the
   postamble are cut off.
3. **The CRC on air is not the generator's CRC.** 33 of the 64 CRC bits differ from the CRC-64
   the current generator computes over text + Gold code, while preamble, MAC, text and Gold code
   match exactly. The file was likely made by an earlier generator version. A receiver cannot
   validate it as written.
4. **SSM1 and SSM2 send identical frames.** The generator hard-codes Gold code `transmitter_id=1`
   and the MAC, and both units stream the one file, so content identifies "an SSM", not which one.
   (From the source; the files on the TX nodes were not checked.)

## Measured and REJECTED (do not retry)

- **rtl_433 for SSM.** Its demodulators (OOK and FSK variants, Manchester, DMC, PIWM) read the
  envelope or an FM discriminator. BPSK has a flat envelope and constant frequency, so there are
  no bits for a flex `match=` to test. Not run on the capture; ruled out from rtl_433's
  demodulator set.
- **Plain (coherent) BPSK decode of the capture.** 2552 to 2590 bit errors of 5184 on every hop
  (50%), although correlation with the reference waveform was 0.94: the timing and carrier were
  right, the encoding assumption was wrong.
- **Matching the rebuilt frame against the reference file as plain BPSK**, at every sample offset
  and bit lag: best bit correlation 131 of 5184, which is noise.

## Measurement traps hit

- **Sampling the reference from sample 0.** The frame starts at about sample 1313, not 0; sign
  agreement at `k * 16` from 0 was 0.501 and suggested a different frame entirely.
- **"cfo" is mostly hop-detection quantization.** The coarse frequency comes from 25.4 kHz
  bins, so the reported offset (up to +-12.6 kHz) is the residual from that, not the
  transmitter's offset. The true hop centre is `f_mhz`.
- **Constant ~78 "errors" against the rebuilt frame** came from the transmitter (truncated end,
  different CRC), not from noise: the clean reference file shows the same count.
- **The venv imports the main checkout.** From a worktree, run with `PYTHONPATH=src` or the code
  under test is the other branch's.

## Open, not yet answered

- Why the CRC differs: which generator version produced the `.dat` (the shallow clone's history
  of `src/ssm_tx/data/` shows only the v0.4.1 merge).
- The low-SNR limit: at what SNR the MAC + text match starts failing. Every hop here was strong.
- Why EVM is about 17% on strong hops (differential decision on RRC-filtered samples; not
  investigated, did not affect decoding).
- `..._ssm_freq_sweep_OVF.dat` (same folder) was not analysed.
- Whether the t = 13.557 s hop was cut by a USB overflow gap (79 ms, 513-bit lag): likely, not
  verified.

## CORRECTION 2026-11-05 (later the same day): "83 hops" counts only the strongest signal

The hop finder in `tools/ssm_analyze.py` follows the single strongest bin each millisecond, so
the "83 hops in 20 s" above counts one signal per instant, not every SSM burst in the capture.
The earlier statement that the capture contains 83 SSM bursts is withdrawn; 83 is the number of
main hops, and 82 of them decoding with 0 errors still stands.

Replaying the first 2 s through RFObserver's own detector (`run_replay`, thresholds 20, 30 and
40 dB) gave 9, 32 and 29 detections. Besides the main hops (83.1 ms, peak within 3.6 kHz of the
decoded centre) there are many 81.7 ms bursts at other frequencies. Decoding the ones at
t = 53 ms (main hop 917.30 MHz):

```
 908.1064 t=53 ms dur 82 snr~33 dB  rho 0.956 core err 0 mac 0200574952470001 text b'Hello from Boulder, CO!'
 926.5020 t=53 ms dur 82 snr~32 dB  rho 0.956 core err 0 mac 0200574952470001 text b'Hello from Boulder, CO!'
```

So each hop also appears 9.2 MHz either side at the same time, and the detector also reports its
image across the 915 MHz RX centre (912.70 MHz for 917.30 MHz). Whether the +-9.2 MHz copies are
made by the transmitter or the receiver is open.

Detector bandwidths for the main hops were 1 to 26 MHz at every threshold, so RFObserver hands
them to attribution on the 1.6 MS/s tier, not the 1.0 MS/s tier the SSM's own rate suggests.

Trap: a strongest-bin hop finder hides simultaneous copies; count bursts with the detector the
product uses.
