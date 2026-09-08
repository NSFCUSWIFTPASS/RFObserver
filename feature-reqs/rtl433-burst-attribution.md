# Feature request: per-burst protocol attribution via rtl_433

Status: proposal / recon
Author: Oren Collaco
Date: 2026-09-07

## Summary

Run each detected burst through `rtl_433` and label it with a protocol (and,
where the decoder supports it, decoded fields). Today RFObserver detects bursts
and stores five-parameter fingerprints (time, frequency, bandwidth, power) but
says nothing about what a burst *is*. rtl_433 carries ~250 device decoders for
the sub-GHz ISM bands and, on master, a Silver Spring Networks / Itron mesh FSK
decoder (protocol 383) that has already been validated against the HCRO
915 MHz captures. Attaching that identification to each burst turns the
detection log from "energy was here" into "this endpoint transmitted here."

This is recon plus a concrete integration proposal, not an implemented feature.

## What already exists in RFObserver (no new work needed)

1. Burst detection is done and structured. `processing/burst.py::detect_bursts`
   emits `BurstFingerprint` objects (`models.py`) carrying exactly what a
   channelizer needs per burst:
   - `center_freq_hz` (band midpoint), `peak_freq_hz` (absolute Hz of peak bin)
   - `bandwidth_hz`, `peak_power_db`, `duration_ms`, `start_time` / `stop_time`

2. The capture format is byte-identical to what rtl_433 reads. Captures are
   stored as `.sc16` = interleaved signed int16 I/Q. That is exactly rtl_433's
   `cs16`. No conversion is required, only slice + channelize + resample, then
   `rtl_433 -s <rate> -r <file>.cs16`.

3. The companion-file layout is the natural place to store attribution:

   ```
   <base>.sc16            raw IQ (interleaved int16 I/Q)
   <base>.json            center_freq_hz, sample_rate_hz, duration, gain
   <base>.psd / .psd.json PSD grid
   <base>.detections.json per-burst records + row_start/row_stop  <- attribution lands here
   ```

   `storage/detections_sidecar.py` already builds and writes that sidecar
   (`write_sidecar_from_grid`), and the captures UI overlay already reads it.
   Adding a `model` / `protocol` field per detection makes it render for free.

4. There is no existing rtl_433 / classify / attribution code in the repo
   (grep is clean). The `modules/` system (`UpstreamModule`) is for live,
   GPU-accelerated streaming demod (FM audio out a queue). rtl_433 is a
   subprocess over a file, so attribution belongs on the offline / replay side,
   not the live module path.

## The mismatch to bridge

rtl_433 assumes a narrowband channel at a modest sample rate. Its OOK/ASK
decoders express pulse widths in microseconds; FSK protocols such as SSN mesh
383 need roughly 1.6 Msps. RFObserver captures are wideband (B205mini at 56 MHz
`BANDWIDTH`). You cannot point rtl_433 at the raw `.sc16`; it would see the
whole band as noise. Every burst must be channelized first:

1. Frequency-shift `peak_freq_hz` to DC: `iq *= exp(-2j*pi*(f/fs)*n)`
2. Low-pass to the burst bandwidth
3. Decimate / resample to a target rate (`scipy.signal.resample_poly`)
4. Write interleaved int16 to a temp `.cs16`

This is exactly the pipeline already prototyped in the sibling gr-modules
project (`iq-processing/ssn_scan.py`), so the DSP is settled and portable.

The second half is rate and protocol selection. rtl_433 can run its full
default protocol set in one pass, but the useful input rate depends on the
modulation. A practical two-tier policy driven by `bandwidth_hz`:

| Burst bandwidth                       | Target rate | rtl_433 pass                                   |
|---------------------------------------|-------------|------------------------------------------------|
| narrow, ~100-200 kHz (2-FSK, ISM OOK) | 1.0 Msps    | full default `-R` set (OOK / ASK 433 / 915)    |
| SSN-mesh-class 2-FSK 100 kbit/s       | 1.6 Msps    | `-R 383` (plus flex `-X` for weaker frames)    |

Run both passes per burst, keep whichever decodes.

SNR is the gate, not protocol match. This was established directly against the
HCRO data: clean 393 ms hops at 50-60 dB did not decode, and 6 of 20 Feb 4-6
captures decoded only where a burst reached high SNR (best at 75 dB above
noise). Skip bursts whose `peak_power_db` is under roughly 12-15 dB over the
sidecar's noise floor; they will not decode and only cost subprocess time.

## Proposed integration (smallest viable)

1. New offline module `processing/attribution.py`:

   ```python
   def attribute_capture(
       sc16_path: Path,
       detections: list[dict],
       rtl_path: str,
   ) -> list[dict]:
       ...
   ```

   For each detection: memmap-slice IQ around `[start, stop]` with a small
   guard via `capture/sigmf_reader.py`, channelize to the target rate, write a
   temp `.cs16`, run `rtl_433 -s <rate> -F json -r <file>`, parse stdout.
   Returns `{burst_id, model, protocol_id, fields, rate, decoded: bool}` per
   burst.

2. New route `POST /captures/attribute/{filename}` mirroring the existing
   `POST /captures/redetect/{filename}` in `web/routes/captures.py`. It runs
   attribution off the event loop (`asyncio.to_thread`), merges `model` /
   `protocol` into `<base>.detections.json`, and returns the payload. The UI
   overlay picks it up with no further change.

3. rtl_433 discovery identical to `ssn_scan.find_rtl()`: check `$RTL433`, then
   a known build path, then `PATH`. rtl_433 is a build dependency, not a pip
   package. Protocol 383 requires a build from master (`silver_spring_mesh.c`);
   configure file-only with `-DENABLE_RTLSDR=OFF -DENABLE_SOAPYSDR=OFF`.

## Deployment reality

- Attribution is CPU / subprocess bound and per-burst. It fits the workstation
  replaying pulled captures (how the sibling SSN work already runs), not the
  live Jetson hot path. Running it on-sensor means cross-building rtl_433 for
  aarch64 and rate-limiting how many bursts per capture get attributed.
- One 56 MHz capture can hold hundreds of bursts. Cap attribution to the
  strongest N by `peak_power_db` (the sidecar already carries it), the same way
  the sibling batch script caps at 40.

## Test data and reproduction

All validation used HCRO 915 MHz captures from the `hcro-rpi-002` node, pulled
from Google Drive (account `collaco.oren@gmail.com`, folder id
`1c9JjIo9pRX7nAgpN_YYSmAFuVa5J7N1I`, link-sharing on, `gdown <file-id>`). Each
capture is raw `complex64` (interleaved float32 I/Q = SigMF `cf32_le`), 26 Msps,
915 MHz center, ~1.25 GB, no SigMF sidecar (parameters live in the filename).
Note the datatype: these are `cf32_le`, not the `ci16_le` RFObserver records
itself. `capture/sigmf_reader.py::load_raw` already supports `cf32_le`, so
RFObserver can read them directly by passing `datatype`, `sample_rate_hz`, and
`center_freq_hz`; the channelize-and-decode step is identical regardless of the
input datatype.

### Batch result: 6 of 19 Feb 4-6 captures decoded SSN mesh

Full log in the sibling project at `/mnt/storage/ssn-batch/results.log`. The
captures that produced protocol-383 matches, with the gdown file id and the
channel/frequency and SNR-over-noise of each decoding burst:

| Capture (Feb, UTC)  | gdown file id                      | Decoding burst(s)                          |
|---------------------|------------------------------------|--------------------------------------------|
| feb4_19-39-48       | `1dCnRoLKx3AZs9x4qakzzDgoJIj_reraq` | 919.399 MHz @ 75 dB (strongest overall)    |
| feb6_06-58-17       | `118Q3ZKA3G8Zno4jeCZ7zwYvNyP9FLWN5`| 917.907 MHz @ 50 dB                         |
| feb5_18-29-48       | `1Y0JkybK-8PIDvQcojtCt2kUH5E39j2Lh`| 910.404 MHz @ 61 dB (flex `-X` only)        |
| feb5_05-29-58       | `1p9Ivfn--vPV2jC0omjtZE9p1boyJU0Da`| 917.000 @ 55, 904.688 @ 46, 913.4 @ 46 dB   |
| feb4_18-47-27       | `1LVd69ecyNs-lZLr8VML81FzENcL0wis3`| 911.299 @ 37, 905.866 @ 32 dB (flex only)   |
| feb4_16-31-37       | `12MX-DnMnLtj9kiR7zcpd6BOiI45Ielvr`| 923.912 @ 32, 906.202 @ 31 dB (flex only)   |

The 13 captures that did NOT decode either had no bursts above threshold or
their strongest burst was too weak (the Feb 5 22:52 capture, 19 bursts at only
~30 dB, is the one the old `feb5_demod_summary.md` note wrongly called
"not decodable" before the decoder existed). This is the SNR gate in action:
matches concentrate at high SNR, and the packed `-R 383` decoder needs more SNR
than the flex `-X "n=ssnmesh,m=FSK_PCM,s=16,l=16,r=8000"` form, which catches
weaker/shorter frames.

Decoded frames carry the Silver Spring OUI `001350` in their EUI-64 source /
egress ids, RF channel, message type (route advert / poll / data), TLVs, and a
valid CRC-32, which is what makes them safe to attribute rather than guess.

### Three standalone burst files for a self-contained regression test

The sibling project ships three extracted, channelized `.cs16` bursts (already
at 1.6 Msps, no download or DSP needed) that decode directly. In
`gr-modules/iq-processing/ssn_bursts/`:

| File                            | rtl_433 -R 383 result                          |
|---------------------------------|------------------------------------------------|
| `burst_feb4_919MHz_75dB.cs16`   | SilverSpring-Mesh, RF channel 57, CRC valid    |
| `burst_feb5_917MHz_56dB.cs16`   | SilverSpring-Mesh, RF channel 49, CRC valid (2 frames: poll + data) |
| `burst_feb5_913MHz_47dB.cs16`   | SilverSpring-Mesh, RF channel 37, CRC valid    |

Each decodes with:

```bash
rtl_433 -s 1600k -R 383 -r burst_feb4_919MHz_75dB.cs16
```

These are the smallest reproducer and the right fixture for a unit test of the
final decode step, independent of the channelization stage.

### End-to-end reproduction through the proposed pipeline

1. Pull a decoding capture, e.g. the 75 dB one:
   `gdown 1dCnRoLKx3AZs9x4qakzzDgoJIj_reraq -O feb4_1939.dat`
2. Read it with `load_raw(datatype="cf32_le", sample_rate_hz=26e6,
   center_freq_hz=915e6)`.
3. Run `detect_bursts` to get the `BurstFingerprint` at ~919.4 MHz.
4. Channelize that burst to 1.6 Msps and decode with `-R 383`.

The reference implementation of steps 2-4 is `iq-processing/ssn_scan.py`
(shift to baseband, `resample_poly(4, 65)` for 26 Msps -> 1.6 Msps exactly,
write `.cs16`, run rtl_433). rtl_433 must be built from master for protocol 383
(`src/devices/silver_spring_mesh.c`); configure file-only with
`-DENABLE_RTLSDR=OFF -DENABLE_SOAPYSDR=OFF`.

Note the sibling captures are 26 Msps and RFObserver records at 56 Msps; the
only change for RFObserver data is the resample ratio (compute it as
`gcd`-reduced `target/source`, as `ssn_scan.py` already does), not the pipeline.

## What this would give the RFI / enforcement workflow

- A named emitter behind a detection ("Silver Spring / Itron mesh endpoint,
  OUI 001350") rather than an anonymous energy blob, which is directly useful
  for HCRO interference attribution and enforcement.
- Decoded fields for the protocols rtl_433 fully parses (device id, channel,
  message type), attached to the capture record and browsable in the UI.
- A reusable channelize-and-decode primitive that any future decoder (rtlamr,
  a GNU Radio flowgraph, a custom demod) can plug into behind the same route.

## Open questions to pin down before implementing

- Rate / protocol policy: fixed two-tier table above, or a per-band lookup
  keyed off center frequency (433.92, 868, 915) and bandwidth?
- Where attribution runs by default: an explicit UI action per capture, a batch
  pass over stored captures, or opportunistically during replay.
- How to represent multiple decodes in one burst window (a poll plus a data
  frame in the same channel occurred in the HCRO data).

## References

- Sibling prototype: `gr-modules/iq-processing/ssn_scan.py`,
  `batch_ssn.py`, `dump_ssn.py` (channelize + resample + rtl_433 383).
- rtl_433 SSN decoder: `src/devices/silver_spring_mesh.c` on upstream master
  (protocol 383).
- Validated against HCRO NIC514 915 MHz meter captures: 2-FSK, 100 kbit/s,
  FHSS on the 300 kHz channel grid, OUI 001350.
