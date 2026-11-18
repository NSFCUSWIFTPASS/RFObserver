# Low-level tone detector

## Why

We want to know when a CW tone is on, down to about 5 dB above the noise floor of a
12.7 kHz PSD bin, at 26 MS/s like production. The burst detector cannot: a cell must be
30 dB above its bin's floor (12.7 kHz x 0.2 ms) to start a detection. A tone that weak is
still plain once integrated: on 2026-11-18, twenty 100 ms pulses about 5 dB over the cell
floor produced zero detections, but raised their bin in the 1 s averaged PSD by 0.55 dB
against a window-to-window noise of about 0.04 dB (13 sigma), and the bin fell back the
second they stopped.

The burst-path Tone classifier (feat/tone-attribution) stays as it is: it labels tones
strong enough to be bursts. This detector finds the ones below that.

## Goals

- Detect tones of 100 ms and longer at about 5 dB over the 12.7 kHz-bin floor.
- Report tone frequency (to tens of Hz), start time and duration (to a few ms), SNR.
- Write each one to the detections table as model `Tone`, so it shows on the Detections
  page, the fleet view and anywhere detections go.
- Its own enable on the Config page, independent of burst detection, isolation and
  attribution.

## Non-goals

- Modulated signals. Only unmodulated carriers.
- Tones shorter than about 50 ms at the lowest levels (integration is what buys the
  sensitivity).
- Fixing the B200 receive step (docs/debugging/2026-11-18_b200-rx-gain-step.md).

## Anti-goals

- Spamming detections from slow floor changes, passband ripple, or every RFID reader hop.
- Costing the pipeline enough to drop more chunks on a 15 W Jetson.

## How it works

Two stages, both off the burst path.

**Stage A: flag bins in the averaged PSD.** Each closed averaging window (DURATION_SEC,
1 s at HCRO) already yields a 2048-bin averaged PSD. Keep a per-bin baseline for the
current tuning: the median of that bin over the last N windows that were not flagged
(N about 60). A bin is flagged when it exceeds its baseline by at least a threshold
(default about 0.25 dB, roughly 6 sigma; a setting). Adjacent flagged bins are one
candidate. The per-bin baseline cancels the passband ripple and the fixed spurs. The
baseline is per tuning and restarts after a retune or gain change.

**Stage B: confirm and measure in the IQ ring.** For each candidate (a few per window at
most, capped), read that window's IQ from the ring (it is 1 to 2 s old, the ring holds
2 s), shift the candidate bin to DC and decimate to a narrow channel (e.g. 50 kHz), then:

- Long FFT over the window: a single line holding most of the channel's in-band power
  (the classifier's line test), giving the tone frequency.
- A narrow filter around the line (a few hundred Hz) and its envelope at about 1 ms:
  where it is on gives start and duration, and its level against the channel noise gives
  SNR. A brief level step (the B200's 3.4 ms, 5 dB) must not split a tone.
- No dips and no AM on the narrow envelope (the classifier's checks, scaled for low SNR).

A confirmed tone becomes a detection: model `Tone`, protocol `CW`, centre and peak at the
tone frequency, its start, duration and power, and attribution JSON with `source:
"tone_detector"`, tone_hz, snr_db and duration_ms. A tone still on at the window's end
continues into the next window's detection instead of starting a new row.

## Duplicates with the burst path (decided 2026-11-18)

One row per tone, whether or not attribution is on. Stage B measures the tone while its
IQ is in the ring, then holds the result about 3 s, past the burst detector's worst-case
delay (it writes a burst only once it stops growing, 0.5 to 1.3 s after it starts), and
looks for burst detections overlapping the tone's frequency (within a bin or two) and time:

- If they cover at least 80 % of the tone's duration, the detector labels those rows Tone
  (source `tone_detector`, with its tone_hz, SNR and duration) instead of writing a row.
- Otherwise (too weak for the burst detector, or only fragments caught), it writes its
  own Tone row.

The burst-path tone classifier stays; when both label the same burst they agree.

## Settings

- `TONE_DETECTOR_ENABLED` (off by default), on the Config page in its own group, with the
  threshold. Applies at next start, like isolation.
- It needs the IQ ring: turning it on keeps the 2 s ring even with isolation off (the
  same RAM guard as isolation decides; if the guard refuses, the detector stays off and
  says why in the health JSON, like isolation does).

## Verification

- Unit tests: synthetic tones at 3 to 30 dB over the bin floor in noise, through both
  stages; slow floor drift and ripple do not flag; a reader-like carrier with dips is not
  confirmed; a tone across a window boundary is one detection.
- Over the air on nano-super, 26 MS/s: 20 pulses each at the 2026-11-18 gain 15 level
  (about 5 dB) and a few dB lower to find where it stops; report hits, misses and false
  detections over a quiet hour.
- Cost: CPU per window on nano-super at 15 W, and chunk drops with it on and off.

## Left to the builder

The exact threshold and N, the channel width, how candidates are capped per window, and
where stage B runs (a worker thread like isolation's).
