# SSM attribution

Status: draft, 2026-11-05. Findings this builds on:
`docs/debugging/2026-11-05_ssm-waveform-dbpsk.md`.

## Intent

Our surrogate smart meters (SSM1, SSM2; `NSFCUSWIFTPASS/ssm-tx`) hop across 902-928 MHz with an
83 ms BPSK frame. RFObserver detects their hops as bursts, but attribution runs rtl_433, which
has no PSK demodulator, so every SSM hop ends up `not_decoded`, indistinguishable from unknown
traffic. This adds an SSM decoder to attribution so SSM hops are labelled `SSM`, with the MAC and
text decoded from the frame, through the same path as every other attribution.

## Goals

- An SSM hop that reaches attribution is labelled `model: "SSM"`, with its MAC as `src_id` and
  its text as `payload`.
- The result travels the existing attribution path unchanged: same queue, same detections row
  (`model`, `protocol_id`, `attribution` JSON), same live labels, Detections page, fleet
  Detections, and (later) OpenZMS annotation. Nothing downstream gets an SSM-specific branch.
- A burst that is not an SSM frame is attributed exactly as today.

## Non-goals

- Telling SSM1 from SSM2. Both send the identical frame today (same MAC, same Gold code).
- Fixing ssm-tx (its truncated frame end and its CRC mismatch). That is the ssm-tx repo's job.
- A CRC or integrity claim. The CRC on air does not match the generator's, so the result
  carries no `mic`.
- Decoding other PSK signals in general.

## Anti-goals

- SSM decoding must not cost rtl_433 its attributions: a non-SSM burst must still reach rtl_433.
- No SSM special cases outside the decoder: if a page or the annotation sender has to know
  about SSM, the result shape is wrong.

## What is on air (from the findings)

| Item | Value |
|---|---|
| Modulation | BPSK, differentially encoded (DBPSK): bit = phase change between consecutive symbols |
| Symbol rate | 62,500 baud; RRC pulse shaping, alpha 0.35; about 84 kHz occupied |
| Frame | 5184 bits, 82.944 ms: sync `0xAAAAAAAA` (32), Barker-13 preamble (128), MAC (64), text (736 = 92 bytes), Gold code n=7 id 1 (4000), CRC-64 (64), inverted-Barker postamble (128), `0x55555555` (32) |
| Sent | Everything up to about bit 5100; the last ~84 bits are cut off by the transmitter |
| Current content | MAC `0200574952470001`; text "Hello from Boulder, CO!" x4 |
| Hops | about 83 ms each, about every 241 ms, centres 902.9 to 927.4 MHz |

## Decisions

| Decision | Choice |
|---|---|
| Path | Same attribution pipeline as rtl_433 results (queue, sinks, storage, UI, annotations) |
| Order | SSM check first on every burst; a match is final and rtl_433 is not run. No match: rtl_433 as today |
| What counts as SSM | A frame whose preamble is found (see Matching); any MAC. The MAC is reported, not required |
| Identity field | `src_id`: the 64-bit MAC as 16 lowercase hex characters, the format rtl_433's SilverSpring-Mesh decoder uses (e.g. `"00135003004b9712"`) |
| Text field | `payload`: the text section as ASCII |
| Expected content | Built in (today's frame), not configurable |
| On/off | No new setting: SSM decoding is part of attribution and follows `ATTRIBUTION_ENABLED` |

## Result shape

On a match the worker builds the same `AttributionResult` an rtl_433 decode does, with the frame
dict in the place of an rtl_433 frame:

```json
{"decoded": true, "at": "2026-11-05T17:02:11.204871+00:00",
 "frames": [{"model": "SSM", "src_id": "0200574952470001",
             "payload": "Hello from Boulder, CO!", "preamble_errors": 0,
             "bit_errors": 0}]}
```

- `model` (row and frame): `"SSM"`. `protocol_id`: `null` (not an rtl_433 protocol).
- `outcome`: `"decoded"`, so the existing `attr_decoded` counter counts it.
- `payload`: the 92-byte text section decoded as ASCII. When it is the same string repeated (four
  identical 23-byte copies today), report one copy; otherwise the whole section. Non-printable
  bytes are kept out by replacing them, so `payload` is always a printable string. (Our call.)
- `src_id` and `payload` are reported exactly as decoded, as rtl_433 reports its fields: a weak
  hop can carry a damaged MAC or text. (User decision.)
- `preamble_errors`: always present, 0 to 16: errors in the 128 preamble bits. It is the quality
  indicator that shows a damaged decode even when `bit_errors` is null. (User decision.)
- `bit_errors`: errors against the built-in frame over frame bits 32 to 4959 (preamble, MAC,
  text, Gold code: fixed and always sent), only when `src_id` is the known MAC
  `0200574952470001`; otherwise `null`, since another unit's Gold code is unknown. (Our call.)
- The frame dict has no `rows`, `data`, `mic` or `protocol` keys. Existing summary code
  (`attribution_summary`) then shows `src_id`, `payload`, `preamble_errors`, `bit_errors` as
  fields with no change (it drops `null` values and caps at 4 fields).

## Matching

Input: the burst as the attribution item carries it, `cs16` at `target_rate_hz`, on either tier.
The decoder resamples its input to 1.0 MS/s itself (5/8 from 1.6 MS/s), so it always works at 16
samples per symbol, and every burst is checked whatever its tier.

Measured 2026-11-05 (replay of the 2025-12-12 capture through RFObserver's detector, thresholds
20, 30 and 40 dB, first 2 s): every true SSM hop was recorded 1 to 26 MHz wide, so all of them
take the 1.6 MS/s tier; checking only the 1.0 MS/s tier would never see one. The recorded
`peak_freq_hz` was within 3.6 kHz of the decoded hop centre on all 8 hops.

Steps (as validated in `tools/ssm_analyze.py`, which works at the same rate):

1. Frequency offset: the BPSK-squared signal is a tone at twice the offset; take the strongest
   bin within +-30 kHz and correct. Isolation centres the burst on its peak bin; measured
   residuals were under 4 kHz, and the search covers +-15 kHz.
2. Matched filter (RRC, alpha 0.35, 16 samples per symbol).
3. Locate the frame by correlating with the built-in preamble waveform (sync + Barker block,
   differentially encoded and RRC-shaped). Symbol timing and frame start come from the peak.
4. Decide symbols, differentially decode, read MAC (bits 160 to 223) and text (224 to 959).
5. Declare SSM when the 128 preamble bits decode with at most 16 errors (12.5%) and enough of the
   burst follows the preamble to hold MAC and text. Otherwise no match. (Threshold our call; the
   strong HCRO hops have 0 errors, so this leaves margin for weak hops without accepting noise:
   chance agreement on 128 random bits within 16 errors is about 1e-20.)

A hop cut short (overflow gap, burst split by the detector) is still SSM if the preamble, MAC and
text are inside it; `bit_errors` then counts over the fixed bits that are present. A burst too
short to hold the MAC and text is no match.

## Where it lives

- A pure-DSP module beside `processing/channelize.py` (for example `processing/ssm.py`): the
  built-in frame, the reference preamble waveform, and `decode_ssm(cs16, rate_hz) -> dict | None`.
  No I/O.
- `AttributionWorker._drain` calls it before `decode_cs16`, in the same worker thread call
  (`asyncio.to_thread`). A match builds the result above and skips rtl_433.
- `tools/ssm_analyze.py` stays a standalone analysis script.

## Constraints

- Python 3.10 compatible, numpy and scipy only (both already dependencies).
- CPU: one 83 ms hop is about 83k samples at 1 MS/s (133k at 1.6 MS/s before resampling); the
  check must stay in the low milliseconds on nano-super so it does not slow the queue (it runs on
  every burst, SSM or not).
- No change to storage schema, APIs, settings, or pages.
- Attribution still requires rtl_433 to be found to start (unchanged): a host without rtl_433 has
  no attribution at all, SSM included. (Our call; changing startup is out of scope.)

## Testing

- Unit, synthetic: build frames from the built-in definition (differential encoding, RRC), at
  1 MS/s:
  - clean frame: `model "SSM"`, `src_id`, `payload`, `preamble_errors 0`, `bit_errors 0`;
  - the same frame at 1.6 MS/s (resampled up from 1 MS/s): same result;
  - frequency offsets up to +-15 kHz and random phase: still matched;
  - added noise at a few SNRs: matched with `preamble_errors` and `bit_errors` counted, and a
    level where it stops matching and falls through;
  - a different MAC and text: matched, `src_id` and `payload` reflect them, `bit_errors null`;
  - noise only, a tone, an FSK burst: no match;
  - a frame cut after the text: matched; cut before the MAC: no match.
- Unit, worker: an SSM burst produces the result shape above and rtl_433 is not invoked; a
  non-SSM burst still goes to rtl_433.
- Real data: one HCRO hop from the 2025-12-12 capture, channelized to 1.0 MS/s with RFObserver's
  own `channelize_to_cs16`, stored as a test fixture (about 330 KB as cs16; our call). Expect MAC
  `0200574952470001`, the text, and 0 errors.
- Summary: `attribution_summary` of a stored SSM result shows `src_id`, `payload`, `bit_errors`.

## Left to the builder

- Function and module names, the exact correlation and timing method, and how the preamble
  reference is built, within the steps above.
- How to keep the real-hop fixture small (cs16 as-is, or compressed).

## Open

- Whether ssm-tx will fix the frame (full length, valid CRC, per-unit Gold code / MAC). If it
  does, `bit_errors` against the built-in frame changes meaning; the match itself (preamble) does
  not.
- Low-SNR behaviour on real weak hops (only strong HCRO hops have been decoded so far).
- Duplicate detections: in the 2025-12-12 capture each SSM hop also appears as its image across
  the RX centre and as copies 9.2 MHz either side, at the same time. The copies decode as SSM
  (0 errors at 908.1 and 926.5 MHz for the 917.3 MHz hop), so they will be labelled SSM, which is
  true of their content; the detection count overstates SSM transmissions about 4x. Whether the
  copies come from the TX or the RX is not known. Out of scope here.
