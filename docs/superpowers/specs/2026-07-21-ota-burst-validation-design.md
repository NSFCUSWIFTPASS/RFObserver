# Hardware-in-the-loop OTA burst validation

Date: 2026-07-21

## Context and goal

The simulated burst matrix (`tests/integration/test_burst_waveform_matrix.py`)
validates RFObserver's detection against synthetic IQ fed through the pipeline.
This work closes the loop in the real world: a USRP transmits the same burst
waveforms over the air, the **actual field sensor** (a Jetson running RFObserver
against a USRP) receives and detects them, and we validate the detections
against ground truth. It exercises the real RF front end, the real receiver
path, and the deployed config end-to-end -- not just the DSP.

## Hardware topology

- **TX:** workstation + Ettus **B200mini** (serial 321D126), UHD 4.9. 70 MHz-6 GHz,
  up to 56 MHz BW, TX-capable.
- **RX:** Jetson `nano-super` (`ocollaco@192.168.97.153`) + Ettus **B200mini**,
  running RFObserver.
- **Link:** over-the-air, antennas on both ends.
- **Band:** **915 MHz ISM (902-928 MHz)**. The widest burst (20 MHz occupied)
  centered at 915 fits (905-925); narrower bursts get a small offset. We only
  *transmit* within ISM; the sensor *receives* a wider span (receiving
  out-of-band is fine).

## Locked decisions (from brainstorming)

- I set up the Jetson RX (Phase A): install UHD, bring up RFObserver in real mode.
- Subset first, then the full 5x5, at the **28 MHz** field sample rate.
- Read detections via a **JSON web endpoint** (see the readout note -- the
  existing `/api/detections` returns HTML, so a small JSON endpoint is added).
- **Frequency barcode** for ground truth (timing-independent): each combo is
  transmitted at a distinct assigned center offset, so a detection's
  `(center, bandwidth)` pair identifies exactly which combo it was.

## Sensor config for the test (all env-driven)

Real mode on the Jetson via env (config already supports every knob):

```
RFOBS_MOCK_RECEIVER=false        # real receiver (default is already false)
RFOBS_SENSOR_ACTIVE=true
RFOBS_FREQUENCY_START=915000000
RFOBS_FREQUENCY_END=915000000    # == START -> fixed single-frequency dwell
RFOBS_BANDWIDTH=28000000         # 28 MHz sample rate (field)
RFOBS_NUM_FFT_BINS=2048          # field default
RFOBS_GAIN=<calibrated in Phase C>
RFOBS_WEB_PORT=8888
```

## Phase A -- Jetson real-SDR bring-up

1. Install UHD on the Jetson (JetPack 6.2 / Ubuntu 22.04 aarch64): `libuhd-dev`,
   `uhd-host`, `python3-uhd` via apt (fall back to source build if the apt UHD
   is too old for the B200mini firmware). Download the B2xx FPGA/firmware images
   (`uhd_images_downloader`).
2. Confirm the RX B200mini enumerates: `uhd_find_devices` shows it; a short
   `rx_samples_to_file` at 915 MHz / 28 MHz captures without overflow.
3. RFObserver's real receiver imports `uhd`; run it in real mode (env above) and
   confirm it tunes and streams (web UI shows a live spectrum at 915 MHz, noise
   floor sane). Deliverable: sensor receiving live, no bursts yet.

## Phase B -- TX harness (workstation)

New standalone transmitter script (e.g. `tools/ota_tx.py`, UHD `MultiUSRP`),
NOT part of the installed package:

- Reuses the exact comb generator `make_iq_with_wideband_burst` (import from the
  test synth, or factor a tiny shared copy) so TX waveforms are identical to the
  simulated ones.
- For each burst in the (subset -> full) matrix: build the IQ at the TX sample
  rate (28 MHz), transmit at 915 MHz center with the burst's **assigned barcode
  offset**, then idle for a fixed **inter-burst gap (~3 s)** so detections don't
  overlap (gap is for cleanliness, NOT identity).
- Writes a **ground-truth schedule** JSON: per burst `{index, tx_wallclock,
  duration_ms, occupied_bw_hz, offset_hz, tx_gain}`.

**Frequency-barcode offset assignment.** Usable band ~903-927 MHz (±12 MHz of
915). For each occupied BW `b`, the max offset is `±(12 MHz - b/2)`; the 5
durations of that BW are placed at 5 distinct offsets evenly spread across
`[-max, +max]`. Examples: 50 kHz -> ~{-11, -5.5, 0, +5.5, +11} MHz; 20 MHz ->
{-2, -1, 0, +1, +2} MHz (20 MHz still fits since bursts are sent one at a time).
Result: within a BW the 5 durations have distinct centers; across BWs the
occupied bandwidth differs. So the `(center, bandwidth)` pair is unique for every
combo -- the barcode. A helper `barcode_offset(bw, duration)` computes it; both
TX and the validator import it so they agree by construction.

## Phase C -- Link calibration

OTA power/range are unknown until measured, so before the matrix run:

- Transmit a repeating mid burst (e.g. 500 kHz / 10.24 ms) while watching the
  sensor's live noise floor + detections.
- Sweep TX gain (and RX `GAIN`) until the burst lands ~20-40 dB above the RX
  noise floor without ADC clipping (RFObserver's IQ stats / a clip indicator).
- Record the working `(tx_gain, rx_gain)` for the matrix run.

## Phase D -- Validation

**Readout:** add a small JSON detections endpoint to RFObserver
(`GET /api/detections.json`, or extend the API) returning the recent detections
as JSON (`center_freq_hz, bandwidth_hz, duration_ms, peak_power_db,
start_time/stop_time, sdr_center_freq_hz, sample_rate_hz`). The existing
`/api/detections` is an HTML fragment for the History page; a JSON sibling is
reusable for any external tooling and keeps validation clean. Small, testable
addition (unit test mirrors the existing detections-fragment tests).

**Validation script** (workstation): after a TX run, pull detections from
`http://192.168.97.153:8888/api/detections.json` for the test window. For each
scheduled combo, its identity is its barcode `(center = 915e6 + barcode_offset,
bandwidth)`. Match a detection by **nearest center (within a few FFT bins) AND
compatible bandwidth**, taking the strongest such candidate; this needs no
timing. Report per combo: matched? measured duration / center / bandwidth vs
transmitted, pass/fail against OTA-realistic tolerances. Summarize a matrix
(detected / duration err / bw err per combo). The TX wall-clock in the schedule
is kept only as a secondary sanity signal, not for identity.

## Phase E -- Run

1. Subset (a few durations x BWs, e.g. {2.7, 83.2 ms} x {150 kHz, 2 MHz, 20 MHz})
   to close the loop; iterate on gain/placement.
2. Full 5x5 once the loop is solid. Save the schedule + detections + report.

## Ground truth via frequency barcode (no shared clock)

TX (workstation) and RX (Jetson) clocks are independent, and **915 ISM is
crowded** (LoRa, cordless phones, telemetry) -- the sensor logs ambient bursts
too. Rather than correlate by time, each combo is transmitted at a **distinct
assigned center frequency** (the barcode, see Phase B), so identity is purely
spectral: a detection is matched to the combo whose `(center, bandwidth)` it
fits, independent of when it arrived or clock drift. Transmitting each burst
well above ambient (Phase C) keeps the intended burst the strongest at its
assigned center. Ambient RFI at *other* frequencies is simply ignored; ambient
energy that happens to land on an assigned center is rejected by the bandwidth
check and (secondarily) by not matching the expected duration.

## Tolerances (OTA-realistic, looser than the sim matrix)

Derived from the field grid resolution (bin = 28 MHz / 2048 ~= 13.7 kHz;
`PSD_TIME_RESOLUTION_MS`), then loosened for real-world effects:

- **Detected:** a matching detection exists in the window.
- **Duration:** within `max(k * time_res, ~20%)` -- OTA ramp/AGC smears edges.
- **Center:** within a few bins of `915e6 + offset`.
- **Bandwidth:** within the sim tolerance widened for multipath/skirt; the
  narrowest (50 kHz) is resolution-limited and may only get a loose sanity bound.

Exact constants calibrated during Phase E against observed spread.

## Risks

- **Ambient 915 ISM RFI:** the barcode ignores energy at non-assigned centers;
  a quiet moment / assigning barcodes to lightly-used offsets further helps.
- **OTA transmit legality:** 915 ISM is license-free at low power (US Part 15);
  keep TX gain low and range short. Wideband (20 MHz) emissions must stay within
  902-928.
- **No time sync / clock drift:** irrelevant to identity now -- the barcode
  matches by frequency, not time.
- **Barcode collision with a same-BW ambient signal:** two combos of the same BW
  sit at distinct assigned centers; if ambient traffic squats exactly on one
  assigned center with a similar bandwidth, that combo's match is ambiguous ->
  reassign that offset or run in a quieter window.
- **Gain/SNR:** Phase C is explicitly a calibration loop; the widest bursts have
  the lowest per-bin SNR and are the hardest OTA.
- **Jetson UHD/firmware:** apt UHD may lag the B200mini firmware; source build is
  the fallback.

## Verification

- Phase A: `uhd_find_devices` on the Jetson shows the B200mini; RFObserver web UI
  shows a live 915 MHz spectrum with a sane noise floor.
- Phase D: the new JSON endpoint has a unit test; full RFObserver check suite
  (ruff/mypy/pytest) stays green.
- Phase E: the validation report shows the transmitted subset (then full matrix)
  detected with duration/center/bandwidth within the OTA tolerances; artifacts
  (schedule JSON, detections JSON, report) saved under the scratchpad/repo.
