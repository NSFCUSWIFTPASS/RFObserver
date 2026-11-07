# The unattributed long hops at HCRO are a UHF RFID reader

## The question

2026-11-06. "Can you figure out what the 231130 captures are? There was some activity and I
scheduled a capture on all." Then: "These were not attributed", "They are pretty long too", and
"Why are all sensors detecting it? RFID shouldn't be this powerful right?"

Schedule eb0b19ac0ea8: one 5 s IQ capture at 23:11:30 UTC on all six HCRO sensors (0.14.0b0,
B205mini, 915 MHz centre, 26 MS/s, gain 35). In the capture window every sensor had 12 to 76
bursts, all unattributed except 2 SilverSpring-Mesh on .35.

## The answer

A UHF RFID reader (EPC Gen2) hopping over the US 50-channel plan, 902.75 + n x 0.5 MHz,
with a continuous carrier on each channel for about 390 ms (the FCC 400 ms dwell limit) and a
new channel about every 430 ms. Neither rtl_433 nor the SSM decoder recognizes it, so nothing
attributed it. A reader may radiate 4 W EIRP (FCC 15.247) and keeps its carrier on for the
whole dwell, so every sensor on the site hears it; the tag, not the reader, limits RFID to
metres. Loudest on .32 (-55 dB peak), quietest on .35 (-87 dB).

## The procedure

1. **Detections in the capture window** (`/api/detections.json?since=...&until=...` on each
   sensor): sizes, durations, frequencies, model. Separates the long narrow hops (63 to 127 kHz,
   80 to 390 ms, many 388 ms) from short wide bursts (2 to 5 ms, 200 kHz to 1 MHz).
2. **The channel raster** of the long hops on .32: 904.25, 904.75, 907.25, 909.75, 912.25,
   912.75, 914.75, 917.25, 919.75, 920.25, 922.25, 924.75 MHz.
3. **Spectrogram** of .32's capture (`/captures/download/...sc16`): one emitter, one channel at a
   time, hop every ~0.43 s, steady for each dwell.
4. **One hop at 1 MS/s** (914.75 MHz, capture offset 2.165 to 2.555 s): spectrum, envelope,
   phase against the carrier.
5. **The SSM decoder on the hops**, as is and with the carrier removed: no match, which rules
   out the SSM transmitter (DBPSK, no carrier).
6. **Envelope dips**: widths and pulse-to-pulse spacing, against Gen2 PIE.

## Evidence

```
hop 914.75 MHz, 1 MS/s:
  carrier at +0.77 kHz holds 62% of the in-band power (same +0.71 to +0.77 kHz on every hop)
  low pulses: 5747 in 386 ms; width p10/50/90 0.5/4.0/9.0 us
  pulse-to-pulse spacing, most common: ~20 us and ~40 us (data-0 / data-1 at Tari ~20 us)
  carrier-only pauses over 0.5 ms: 245 per hop, median 0.58 ms (tag reply windows)
decode_ssm: None on 4 hops, also after removing the carrier
```

Per-sensor loudness in the window (peak dB, uncalibrated): .32 -55, .34 -61, .31 -66,
.36 -75, .33 -77, .35 -87.

## Measured and REJECTED (do not retry)

- **The SSM transmitter in another mode.** 15.6 kbaud-like dip rate, a carrier with 60 to 70%
  of the power, and no SSM preamble even with the carrier removed. The SSM is 62.5 kbaud
  DBPSK with no carrier.
- **A first PIE-only classifier.** Noise also gives about 80% of its dip spacings in the PIE
  range; the carrier share (0.68 to 0.71 on hops, 0.07 on an SSM hop, 0.01 on noise) is what
  separates them, so both are required.
- **A Tari estimate.** The lower quartile of the spacings gave 8 to 20 us across hops of the
  same reader (split dips); not reported.

## Measurement traps hit

- My first envelope threshold (20x the median power) found 0.6 ms of signal: the window was
  mostly signal, so the median was the signal. Use a spectrogram first.
- The database's detection times are about 0.59 s later than the same hop in the capture
  (database 23:11:32.754 for the hop at capture offset 2.165 s). Not investigated.

## The change

`processing/rfid.py`, `classify_rfid_reader(cs16, rate_hz, freq_hz)`: a carrier line holding at
least 30% of the in-band power, at least 2000 dips a second of 1.5 to 15 us, and at least 60%
of their spacings (under 100 us) in 6 to 60 us. It reports the carrier frequency and the US
channel when the carrier is on the raster. It runs after the SSM decoder and before rtl_433, as
decoder `rfid` (on by default, a checkbox on the Config page). A replay of .32's capture
labels all 23 reader hops RFID-Reader on the right channel; the 5 short wide bursts stay
not_decoded.

## Open, not yet answered

- Where the reader is (nearest .32 by level) and whose it is.
- Sensors whose saved `ATTRIBUTION_DECODERS` list predates `rfid` (for example .34,
  `ssm,silverspring`) do not run it until it is ticked on the Config page.
- The 0.59 s offset between database detection times and capture sample times.
