# SSM Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Label surrogate smart meter (SSM) bursts `model: "SSM"` in attribution by decoding their DBPSK frame, before rtl_433, through the existing attribution path.

**Architecture:** A pure-DSP module `processing/ssm.py` builds the known SSM frame and decodes a channelized burst (cs16 at its tier rate) into an rtl_433-shaped frame dict or `None`. `AttributionWorker._drain` calls it first; a match becomes a normal `decoded` result and rtl_433 is skipped.

**Tech Stack:** Python 3.10+, numpy, scipy.signal (already dependencies), pytest.

**Spec:** `docs/superpowers/specs/2026-11-05-ssm-attribution-design.md`. Findings: `docs/debugging/2026-11-05_ssm-waveform-dbpsk.md`. Validated reference implementation of the DSP: `tools/ssm_analyze.py` (`frame_bits`, `crc64`, `rrc`, `demod`).

## Global Constraints

- Python 3.10 compatible; numpy and scipy only.
- No change to storage schema, APIs, settings, or pages.
- Result shape exactly: frame `{"model": "SSM", "src_id": <16 lowercase hex>, "payload": <str>, "preamble_errors": <int 0..16>, "bit_errors": <int or None>}`; row `model "SSM"`, `protocol_id None`, outcome `"decoded"`; attribution JSON `{"decoded": true, "at": <iso>, "frames": [frame]}`. No `rows`, `data`, `mic`, `protocol` keys.
- Frame layout (bit indices): sync 0-31 (`0xAAAAAAAA`), Barker-13 preamble 32-159, MAC 160-223, text 224-959 (92 bytes), Gold code 960-4959 (n=7, transmitter id 1), CRC 4960-5023, inverted-Barker postamble 5024-5151, end 5152-5183 (`0x55555555`).
- On air: DBPSK, bit 1 = a 180 degree phase change from the previous symbol; 62,500 baud; RRC alpha 0.35; 16 samples per symbol at 1.0 MS/s.
- Known MAC `0x0200574952470001`; known text `b"Hello from Boulder, CO!" * 4`.
- Match: preamble (bits 32-159) with at most 16 errors, and the burst long enough to hold MAC and text (through bit 959).
- `bit_errors`: over bits 32-4959 that are present in the burst, only when `src_id == "0200574952470001"`; else `None`.
- CPU: low milliseconds per burst on nano-super.
- No em-dashes in code or docs; no emojis.
- Worktree runs: prefix `PYTHONPATH=src` (the shared venv `../RFObserver/.venv310` otherwise imports the main checkout).
- Before every commit: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH=src ../RFObserver/.venv310/bin/mypy src/rfobserver/`, the no-numpy lint venv mypy (`/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/lintvenv/bin/mypy src/rfobserver/` with `PYTHONPATH=`), unit and integration tests. Array aliases as `TypeAlias = "np.ndarray[Any, np.dtype[Any]]"`, `cast()` not `# type: ignore`.
- Commits: `git -c user.name="Oren Collaco" -c user.email="orencollaco97@gmail.com" commit`, explicit paths only, no co-author line.

## Review Focus

1. Empty or tiny cs16 (0 bytes, a few hundred samples): `decode_ssm` returns `None`, never raises. Test in Task 1.
2. Frame not at the start of the burst (30 ms of noise before it, as when isolation adds lead): still matched. Test in Task 1.
3. `decode_ssm` raising inside the worker: the burst still goes to rtl_433 and is counted from rtl_433's result, not lost. Test in Task 3.
4. A strong non-SSM burst on the 1.6 MS/s tier (a SilverSpring-like FSK frame): no match, so SSM never steals rtl_433's attributions. Test in Task 1.
5. Peak-normalized cs16 (peak scaled to 30000, as `channelize_to_cs16` writes it): decoded the same as unscaled. Covered by Task 2's real fixture, which is made with `channelize_to_cs16`.

---

### Task 1: SSM frame and decoder (`processing/ssm.py`)

**Files:**
- Create: `src/rfobserver/processing/ssm.py`
- Test: `tests/unit/test_ssm_decode.py`

**Interfaces:**
- Produces:
  - `SSM_MAC: int = 0x0200574952470001`, `SSM_TEXT: bytes = b"Hello from Boulder, CO!" * 4`, `PREAMBLE_MAX_ERRORS: int = 16`
  - `frame_bits(mac: int = SSM_MAC, text: bytes = SSM_TEXT) -> np.ndarray` (uint8, 5184; `text` must be 92 bytes, else `ValueError`)
  - `modulate(bits: np.ndarray, rate_hz: int = 1_000_000) -> np.ndarray` (complex64; DBPSK, RRC, 1 MS/s then resampled to `rate_hz`; for tests and for building the preamble reference)
  - `decode_ssm(cs16: bytes, rate_hz: int) -> dict[str, Any] | None` (the frame dict in Global Constraints, or `None`)

- [ ] **Step 1: Write the failing tests** in `tests/unit/test_ssm_decode.py`. Helper `_cs16(iq)` packs complex to `<i2` interleaved peak-scaled to 30000 (as `channelize_to_cs16`). Helper `_noise(n, rng)` complex Gaussian. Tests (seeded `np.random.default_rng`):

```python
def test_clean_frame_decodes():
    f = decode_ssm(_cs16(modulate(frame_bits())), 1_000_000)
    assert f == {"model": "SSM", "src_id": "0200574952470001",
                 "payload": "Hello from Boulder, CO!", "preamble_errors": 0, "bit_errors": 0}

def test_frame_at_1600k_decodes():
    f = decode_ssm(_cs16(modulate(frame_bits(), 1_600_000)), 1_600_000)
    assert f["src_id"] == "0200574952470001" and f["bit_errors"] == 0

@pytest.mark.parametrize("cfo", [-15_000, -4_000, 0, 3_600, 15_000])
def test_offset_and_phase(cfo): ...  # rotate by exp(j(2*pi*cfo*t + 1.1)); bit_errors == 0

def test_noise_counts_errors_then_falls_through():
    # SNR (per sample, 1 MS/s) 6 dB: matched, preamble_errors <= 16, bit_errors >= 0
    # SNR -12 dB: decode_ssm(...) is None

def test_other_mac_and_text():
    f = decode_ssm(_cs16(modulate(frame_bits(0x0200574952470002, b"SSM2 test frame 0002 ok" * 4))), 1_000_000)
    assert f["src_id"] == "0200574952470002" and f["bit_errors"] is None
    assert f["payload"] == "SSM2 test frame 0002 ok"

def test_payload_not_repeated_is_reported_whole():  # 92 distinct printable bytes -> payload == all 92 chars

def test_non_printable_payload_bytes_are_replaced():  # a text byte 0x07 -> "?" in payload

@pytest.mark.parametrize("make", ["noise", "tone", "fsk"])
def test_non_ssm_is_none(make): ...  # 83 ms of noise; a CW tone; a 2-FSK burst at 100 kbaud, +-50 kHz, 1.6 MS/s

def test_cut_after_text_matches_and_before_mac_does_not():
    # keep symbols through bit 1000: matched, bit_errors == 0; through bit 200: None

def test_frame_after_30ms_of_noise():  # 30 ms noise prefix at 20 dB below the frame: matched

@pytest.mark.parametrize("n", [0, 1, 400])
def test_tiny_input_is_none(n): ...  # b"\0\0\0\0" * n -> None
```

- [ ] **Step 2: Run, verify they fail**
Run: `PYTHONPATH=src ../RFObserver/.venv310/bin/pytest tests/unit/test_ssm_decode.py -q`
Expected: collection error, `No module named 'rfobserver.processing.ssm'`.

- [ ] **Step 3: Implement `processing/ssm.py`.** Port `frame_bits`, `crc64` (generator's CRC-64/ECMA-182 over text + Gold) and `rrc` from `tools/ssm_analyze.py`. `modulate`: differential encode (state starts at +1; bit 1 flips), upsample by 16, convolve with RRC `mode="same"`, then `scipy.signal.resample_poly` to `rate_hz` (gcd-reduced up/down). `decode_ssm`, in order:
  1. Unpack `<i2` I/Q to complex64; resample to 1 MS/s if `rate_hz != 1_000_000`; `None` if shorter than 960 symbols x 16 samples.
  2. Frequency offset from the squared signal: FFT (zero-pad to a power of two at least 4x the length), strongest bin within +-30 kHz, offset = bin / 2; de-rotate.
  3. Matched filter with the RRC taps.
  4. Frame start: correlate (FFT) the matched-filtered signal with the matched-filtered `modulate(frame_bits()[:160])` reference (sync + preamble; built once at import or cached); take `argmax(|c|)`. Symbol k of the frame is at `start + k * 16` in the reference's alignment.
  5. Differential decisions `bit_k = (Re(y_k * conj(y_{k-1})) < 0)` for the symbols present; bit 0 has no predecessor (score from bit 1).
  6. `preamble_errors` = mismatches with `frame_bits()` over 32-159 (also try the inverted decision vector, keep the better: guards a 180 degree reference ambiguity); `None` if > `PREAMBLE_MAX_ERRORS` or if fewer than 960 bits are present.
  7. MAC from 160-223 as `f"{v:016x}"`; text bytes from 224-959; payload: the shortest prefix `p` with `len(text) % len(p) == 0` and `text == p * (len(text)//len(p))`, decoded with non-printable bytes (outside 0x20-0x7e) replaced by `"?"`.
  8. `bit_errors` when `src_id == "0200574952470001"`: mismatches over the present bits in 32-4959; else `None`.

- [ ] **Step 4: Run, verify they pass** (same command). Expected: all pass. Then time it: `PYTHONPATH=src ../RFObserver/.venv310/bin/python -c "import timeit, ..."` on one clean 1.6 MS/s frame; record the per-call ms in the commit message.

- [ ] **Step 5: Run every check in Global Constraints, then commit**

```bash
git add src/rfobserver/processing/ssm.py tests/unit/test_ssm_decode.py
git commit -m "ssm: DBPSK frame decoder for surrogate smart meter bursts"
```

### Task 2: Real HCRO hop fixture

**Files:**
- Create: `tests/fixtures/ssm_hop_917MHz_1600k.cs16.gz`
- Modify: `tests/unit/test_ssm_decode.py`

**Interfaces:**
- Consumes: `decode_ssm(cs16, rate_hz)` from Task 1; `channelize_to_cs16(iq, sample_rate_hz, offset_hz, target_rate_hz)` from `processing/channelize.py`.

- [ ] **Step 1: Make the fixture** from the capture `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/ssm/hcro_ssm_fhss_26M.dat` (ci16_le, 26 MS/s, centre 915 MHz): samples from 47 ms to 140 ms (the hop at t = 52 ms), `channelize_to_cs16(iq, 26e6, 917_297_900 - 915e6, 1_600_000)` (917.2979 MHz is the detector's recorded `peak_freq_hz` for this hop), gzip, write. One-off; not committed as a script. Note the source file, window and offset in the test's docstring.

- [ ] **Step 2: Write the test** `test_real_hcro_hop`: gunzip the fixture, `decode_ssm(data, 1_600_000)`; assert `src_id == "0200574952470001"`, `payload == "Hello from Boulder, CO!"`, `bit_errors == 0`, `preamble_errors == 0`.

- [ ] **Step 3: Run it.** Expected: PASS. If `bit_errors` is not 0, the Task 1 bit/symbol alignment disagrees with the transmitter (the findings saw a 5-bit lag against the reference file's own start): fix the decoder, not the test.

- [ ] **Step 4: Checks, then commit**

```bash
git add tests/fixtures/ssm_hop_917MHz_1600k.cs16.gz tests/unit/test_ssm_decode.py
git commit -m "ssm: real HCRO hop fixture for the decoder"
```

### Task 3: SSM first in the attribution worker

**Files:**
- Modify: `src/rfobserver/pipeline/attribution.py` (`AttributionWorker._drain`, about lines 236-280)
- Test: `tests/unit/test_attribution_sinks.py` (reuse `_item`, `_DB`, the worker harness there)
- Test: `tests/unit/test_fleet_detections.py`

**Interfaces:**
- Consumes: `decode_ssm` (Task 1), imported in `attribution.py` as a module attribute so tests can monkeypatch `attr.decode_ssm`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_ssm_match_skips_rtl433_and_is_a_normal_decoded_result(monkeypatch):
    frame = {"model": "SSM", "src_id": "0200574952470001", "payload": "Hello from Boulder, CO!",
             "preamble_errors": 0, "bit_errors": 0}
    monkeypatch.setattr(attr, "decode_ssm", lambda cs16, rate: frame)
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: pytest.fail("rtl_433 must not run"))
    # run worker on one _item(); assert result.model == "SSM", protocol_id is None,
    # outcome == "decoded", json.loads(result.attribution)["frames"] == [frame],
    # json.loads(...)["decoded"] is True and "at" in it; on_outcome got ["decoded"]

async def test_no_ssm_match_goes_to_rtl433(monkeypatch):
    monkeypatch.setattr(attr, "decode_ssm", lambda cs16, rate: None)
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: [{"model": "SilverSpring-Mesh"}])
    # result.model == "SilverSpring-Mesh", protocol_id == 383

async def test_ssm_decoder_crash_falls_through_to_rtl433(monkeypatch):
    def boom(*a): raise RuntimeError("x")
    monkeypatch.setattr(attr, "decode_ssm", boom)
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: [])
    # outcome == "not_decoded" (rtl_433's), not "failed"
```

In `tests/unit/test_fleet_detections.py`:

```python
def test_summary_of_an_ssm_burst():
    rec = json.dumps({"decoded": True, "at": "x", "frames": [{"model": "SSM",
        "src_id": "0200574952470001", "payload": "Hello from Boulder, CO!",
        "preamble_errors": 0, "bit_errors": 0}]})
    s = attribution_summary(det(1, "t", "SSM", rec))
    assert s["status"] == "decoded"
    assert s["fields"] == [["src_id", "0200574952470001"], ["payload", "Hello from Boulder, CO!"],
                           ["preamble_errors", 0], ["bit_errors", 0]]
```

- [ ] **Step 2: Run, verify the worker tests fail** (`attr.decode_ssm` does not exist) and the summary test passes already (it pins that no summary change is needed).
Run: `PYTHONPATH=src ../RFObserver/.venv310/bin/pytest tests/unit/test_attribution_sinks.py tests/unit/test_fleet_detections.py -q`

- [ ] **Step 3: Implement.** In `_drain`, before the rtl_433 call: `frame = await asyncio.to_thread(decode_ssm, item.cs16, item.target_rate_hz)` inside `try/except Exception` (log with `logger.exception("ssm decode failed for burst %s", ...)`, treat as no match). On a match build `AttributionResult(item, "SSM", None, json.dumps({"decoded": True, "at": now_iso, "frames": [frame]}), "decoded")`, count and send to sinks exactly as an rtl_433 decode, and `continue`. Keep `_protocol_id_for` unchanged.

- [ ] **Step 4: Run, verify pass** (same command), then the whole unit suite.

- [ ] **Step 5: Checks, then commit**

```bash
git add src/rfobserver/pipeline/attribution.py tests/unit/test_attribution_sinks.py tests/unit/test_fleet_detections.py
git commit -m "attribution: try the SSM decoder before rtl_433"
```

### Task 4: End-to-end replay and nano-super timing

**Files:**
- Modify: `docs/superpowers/specs/2026-11-05-ssm-attribution-design.md` (append a dated "Verified" note)

- [ ] **Step 1: Replay** the first 2 s of the capture with isolation and attribution on, writing the replay attribution file:
`run_replay(<capture>, sample_rate_hz=26_000_000, center_freq_hz=915_000_000, datatype="ci16_le", threshold_db=40.0, max_seconds=2.0, overrides={"ISOLATION_ENABLED": True, "ATTRIBUTION_ENABLED": True})` from a scratch cwd (check `run_replay`'s signature for the overrides keyword and the attribution.jsonl location).
Expected: every attributed burst whose `freq_hz` is within 5 kHz of a main hop (917.2993, 923.8993, 906.1993, 922.0993, 913.3993, 909.4993, 919.6993, 926.2993 MHz) has `model "SSM"` and `src_id "0200574952470001"`. Report how many other bursts were labelled SSM (expected: the +-9.2 MHz copies and RX images).

- [ ] **Step 2: Time on nano-super.** Copy `src/rfobserver/processing/ssm.py` and the Task 2 fixture to a scratch dir on `ocollaco@192.168.97.153` (not into `~/GitHub/RFObserver`, do not touch `rfobs-replay-data` or `rfobs-stall`), run `decode_ssm` 50 times with its `.venv` python, report median ms. Expected: low milliseconds (under 20 ms). If higher, stop and report before going on.

- [ ] **Step 3: Append** a "Verified 2026-11-05" section to the spec with the replay counts and the nano-super median, then commit:

```bash
git add docs/superpowers/specs/2026-11-05-ssm-attribution-design.md
git commit -m "docs: SSM attribution verified on the HCRO replay and nano-super"
```
