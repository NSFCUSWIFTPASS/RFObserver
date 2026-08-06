# Interval Statistics Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`).

**Goal:** Make the per-interval IQ statistics sent to ZMS/NATS reproduce `rf-processor`'s whole-capture computation, by accumulating additive moments over the DURATION interval instead of emitting the last chunk's subsampled stats.

**Architecture:** New moment primitives in `processing/iq_utils.py` (`IQMoments`, `moments_from_iq`, `finalize_moments`), folded across chunks in `pipeline/streaming.py` and finalized at each DURATION flush into the envelope's `statistics`.

**Tech Stack:** numpy (float64 moment accumulation), Python dataclasses. Env: prefix Python with `PYTHONPATH=`, use `.venv/bin`. ruff global.

## Global Constraints

- No emojis, no em-dashes anywhere. No "Co-Authored-By" in commits.
- Every Python command prefixed with `PYTHONPATH=`; `.venv/bin/...`. All CLAUDE.md checks stay green (ruff check + format, mypy, unit, integration@NATS:4222).
- Accumulate moments in **float64** (match rf-processor / numpy defaults; float32 sums over ~28M samples lose kurtosis precision).
- Reference formulas (`p=|z|^2`): `average=10log10(mean(p)/50)`, `max=10log10(max(p)/50)`, `median=10log10(median(p)/50)`, `std=std(|z|)`, `kurtosis=(M*S2/S1^2-1)*(M+1)/(M-1)` with `S1=sum(p)`, `S2=sum(p^2)`, `M=len`.
- `IQStatistics` fields, DB schema, and the live per-chunk UI cadence do NOT change. The live per-chunk broadcast keeps last-chunk stats; only the ZMS/NATS envelope becomes interval-accurate.

---

### Task 1: Moment primitives + corrected stats (`processing/iq_utils.py`)

**Files:** Modify `src/rfobserver/processing/iq_utils.py`. Test: `tests/unit/test_iq_stats_parity.py` (new).

**Interfaces produced:**
- `IQMoments` dataclass: `n:int, s_abs:float, s_pow:float, s_pow2:float, max_pow:float, hist:np.ndarray`; method `add(other)->IQMoments` (pure, returns a new folded IQMoments).
- `moments_from_iq(data:np.ndarray)->IQMoments` (full-resolution, one pass).
- `finalize_moments(m:IQMoments)->IQStatistics`.
- `calculate_iq_statistics(data)` now == `finalize_moments(moments_from_iq(data))`.

- [ ] **Step 1: Write the failing parity test** in `tests/unit/test_iq_stats_parity.py`:

```python
import numpy as np
import pytest
from rfobserver.processing.iq_utils import (
    IQMoments, moments_from_iq, finalize_moments, calculate_iq_statistics,
)

def _refproc(data):
    # verbatim copy of reference_software/rf-processor/src/rf_processor/iq_utils.py
    mean_db = 10 * np.log10(np.mean(np.abs(data) ** 2 / 50))
    max_db = 10 * np.log10(np.max(np.abs(data) ** 2 / 50))
    median_db = 10 * np.log10(np.median(np.abs(data) ** 2 / 50))
    std = np.std(np.abs(data))
    p = np.abs(data) ** 2
    m = len(p); s1 = np.sum(p); s2 = np.sum(p ** 2)
    k = m * s2 / s1 ** 2 - 1.0
    kurt = k * (m + 1.0) / (m - 1.0)
    return dict(average=float(mean_db), max=float(max_db), median=float(median_db),
               std=float(std), kurtosis=float(kurt))

def _signals():
    rng = np.random.default_rng(0)
    n = 1_000_000
    yield (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64) * 0.05
    t = np.arange(n)
    yield ((rng.standard_normal(n) + 1j * rng.standard_normal(n)) * 0.03
           + 0.02 * np.exp(2j * np.pi * 0.11 * t)).astype(np.complex64)

@pytest.mark.parametrize("nchunks", [1, 7, 64])
def test_folded_moments_match_refproc(nchunks):
    for iq in _signals():
        ref = _refproc(iq)
        parts = np.array_split(iq, nchunks)
        acc = moments_from_iq(parts[0])
        for pc in parts[1:]:
            acc = acc.add(moments_from_iq(pc))
        got = finalize_moments(acc)
        assert abs(got.average - ref["average"]) < 1e-4
        assert abs(got.std - ref["std"]) < 1e-4
        assert abs(got.kurtosis - ref["kurtosis"]) / (abs(ref["kurtosis"]) + 1e-9) < 1e-3
        assert abs(got.max - ref["max"]) < 1e-6            # full-res max is exact
        assert abs(got.median - ref["median"]) < 0.06     # histogram bin (~0.035 dB)

def test_calculate_iq_statistics_delegates():
    iq = (np.random.default_rng(1).standard_normal(50000)
          + 1j * np.random.default_rng(2).standard_normal(50000)).astype(np.complex64) * 0.1
    a = calculate_iq_statistics(iq)
    b = finalize_moments(moments_from_iq(iq))
    assert (a.average, a.std, a.max, a.kurtosis) == (b.average, b.std, b.max, b.kurtosis)

def test_add_is_order_independent():
    rng = np.random.default_rng(3)
    a = moments_from_iq((rng.standard_normal(1000)+1j*rng.standard_normal(1000)).astype(np.complex64))
    b = moments_from_iq((rng.standard_normal(2000)+1j*rng.standard_normal(2000)).astype(np.complex64))
    ab = finalize_moments(a.add(b)); ba = finalize_moments(b.add(a))
    assert abs(ab.average - ba.average) < 1e-9 and ab.max == ba.max
```

- [ ] **Step 2: Run, expect fail** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_iq_stats_parity.py -x -q` (import errors: `IQMoments`/`moments_from_iq`/`finalize_moments` absent).

- [ ] **Step 3: Implement** in `iq_utils.py` (add above `calculate_iq_statistics`):

```python
from dataclasses import dataclass

_DB_OFFSET = -16.989700043360187  # 10*log10(50); power ref = |z|^2 / 50

# Log-spaced |z|^2 bin edges (uniform in dB) for the streaming median. |z| is
# normalized to [-1,1] so p=|z|^2 <= ~2; span well below the noise floor to +20 dB.
HIST_EDGES = np.logspace(-12.0, 2.0, 4001)  # 4000 bins, ~0.035 dB each

@dataclass
class IQMoments:
    n: int
    s_abs: float
    s_pow: float
    s_pow2: float
    max_pow: float
    hist: np.ndarray  # int64 counts, len == len(HIST_EDGES) - 1

    def add(self, other: "IQMoments") -> "IQMoments":
        return IQMoments(
            n=self.n + other.n,
            s_abs=self.s_abs + other.s_abs,
            s_pow=self.s_pow + other.s_pow,
            s_pow2=self.s_pow2 + other.s_pow2,
            max_pow=max(self.max_pow, other.max_pow),
            hist=self.hist + other.hist,
        )

def moments_from_iq(data: np.ndarray) -> IQMoments:
    mag = np.abs(data).astype(np.float64)      # float64: exact sums over 10s of M samples
    p = mag * mag
    hist, _ = np.histogram(p, bins=HIST_EDGES)
    return IQMoments(
        n=int(mag.size),
        s_abs=float(mag.sum()),
        s_pow=float(p.sum()),
        s_pow2=float(np.dot(p, p)),
        max_pow=float(p.max()) if mag.size else 0.0,
        hist=hist.astype(np.int64),
    )

def _hist_median_pow(m: IQMoments) -> float:
    total = int(m.hist.sum())
    if total == 0:
        return 0.0
    cum = np.cumsum(m.hist)
    idx = int(np.searchsorted(cum, (total + 1) / 2.0))
    idx = min(idx, len(HIST_EDGES) - 2)
    lo, hi = HIST_EDGES[idx], HIST_EDGES[idx + 1]
    return float(np.sqrt(lo * hi))            # geometric center (bin midpoint in dB)

def finalize_moments(m: IQMoments) -> IQStatistics:
    if m.n == 0:
        return IQStatistics(average=0.0, max=0.0, median=0.0, std=0.0, kurtosis=0.0)
    mean_pow = m.s_pow / m.n
    mean_abs = m.s_abs / m.n
    variance = max(0.0, mean_pow - mean_abs * mean_abs)   # Var(|z|) = E[|z|^2]-E[|z|]^2
    average = 10.0 * np.log10(mean_pow) + _DB_OFFSET
    max_db = 10.0 * np.log10(m.max_pow) + _DB_OFFSET
    median_db = 10.0 * np.log10(_hist_median_pow(m)) + _DB_OFFSET
    k = m.n * m.s_pow2 / (m.s_pow ** 2) - 1.0
    kurtosis = k * (m.n + 1.0) / (m.n - 1.0) if m.n > 1 else k
    return IQStatistics(
        average=float(average), max=float(max_db), median=float(median_db),
        std=float(np.sqrt(variance)), kurtosis=float(kurtosis),
    )
```
Then replace the body of `calculate_iq_statistics(data)` with:
```python
def calculate_iq_statistics(data: np.ndarray) -> IQStatistics:
    """Power statistics matching rf-processor (full resolution, no subsampling)."""
    return finalize_moments(moments_from_iq(data))
```
Remove the now-dead subsampling / old-std / old-max code from the old body.

- [ ] **Step 4: Run tests -> PASS.** Also `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q` (nothing else regressed; any test asserting the OLD std/max values must be updated to the corrected definition -- that is the fix, not a regression).

- [ ] **Step 5: Commit** — `git commit -am "iq stats: moment-based stats matching rf-processor (fix std, max, add IQMoments)"`.

---

### Task 2: Accumulate moments over the interval + envelope (`pipeline/streaming.py`)

**Files:** Modify `src/rfobserver/pipeline/streaming.py`. Test: extend `tests/unit/test_streaming.py` (or nearest streaming/envelope test).

**Interfaces:** consumes `IQMoments`, `moments_from_iq`, `finalize_moments` (Task 1). `_ChunkResult` and `_StreamResult` gain an `iq_moments` field; `_build_envelope` gains an `iq_stats` parameter.

- [ ] **Step 1: Write the failing pipeline test** in `tests/unit/test_streaming.py`: construct a `StreamingProcessor` (mock receiver, as existing streaming tests do), build two `_StreamResult`s whose `iq_moments` come from two IQ chunks of **different power**, and assert `_build_envelope(avg_powers, last_result, finalize_moments(m0.add(m1)))` sets `envelope.statistics.average` equal to the combined-moment value and NOT `last_result.iq_stats.average`. (If direct `_build_envelope` calling is awkward, assert at the `finalize_moments(m0.add(m1))` level that the combined average differs from each chunk's and equals the whole-array `calculate_iq_statistics`.)

- [ ] **Step 2: Run, expect fail** (envelope still uses last-chunk stats / signature mismatch).

- [ ] **Step 3: Implement.**
  - Import `IQMoments, moments_from_iq, finalize_moments` in `streaming.py` (line 37 import group).
  - `_ChunkResult` (`:98-129`): add `"iq_moments"` to `__slots__`, constructor param `iq_moments: IQMoments`, and `self.iq_moments = iq_moments`.
  - `_StreamResult` (`:63-95`): same — add `"iq_moments"` slot, param, assignment.
  - Worker `_process_chunk` (`:963`): replace
    ```python
    iq_stats = calculate_iq_statistics(complex_chunk)
    ```
    with
    ```python
    iq_moments = moments_from_iq(complex_chunk)
    iq_stats = finalize_moments(iq_moments)
    ```
    and pass `iq_moments=iq_moments` into the `_ChunkResult(...)` at `:980`.
  - `_StreamResult` construction (`:1030`): pass `iq_moments=cr.iq_moments`.
  - Consumer loop (`:1177-1231`): add `accum_moments: IQMoments | None = None` beside `accum_powers`. Wherever `accum_powers.clear()` + `accum_start = time.monotonic()` reset the interval (the shape-change branch `:1208-1211`, the timeout-flush `:1191-1193`, and the DURATION-flush `:1229-1230`), also set `accum_moments = None`. After `accum_powers.append(new_powers)` (`:1212`), fold:
    ```python
    accum_moments = result.iq_moments if accum_moments is None else accum_moments.add(result.iq_moments)
    ```
    At BOTH flush sites, compute `interval_stats = finalize_moments(accum_moments)` (guard: `accum_moments` is non-None whenever `accum_powers` is non-empty) and pass it through: `await self._publish_processed(avg, <result-or-last_result>, interval_stats)`.
  - `_publish_processed(self, avg_powers, result, iq_stats)` (`:1393`): add the `iq_stats: IQStatistics` param; pass to `_build_envelope(avg_powers, result, iq_stats)`.
  - `_build_envelope(self, avg_powers, result, iq_stats)` (`:1330`): add the param; change `statistics=result.iq_stats` (`:1356`) to `statistics=iq_stats`.
  - Leave the live per-chunk broadcast fields (`result.iq_stats.average`/`kurtosis` at `:1251-1253`, `:1298-1300`) UNCHANGED -- UI liveness, intentionally last-chunk.

- [ ] **Step 4: Run** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming.py -x -q` -> PASS.

- [ ] **Step 5: Commit** — `git commit -am "streaming: accumulate IQ moments over the interval; envelope stats now interval-accurate"`.

---

### Task 3: Full verification + finish

- [ ] **Step 1:** `ruff check src/ tests/ && ruff format --check src/ tests/`
- [ ] **Step 2:** `PYTHONPATH= .venv/bin/mypy src/rfobserver/`
- [ ] **Step 3:** `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
- [ ] **Step 4:** `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`
- [ ] **Step 5:** If green, use superpowers:finishing-a-development-branch.

## Self-Review (author)

- Coverage: Task 1 = corrected moment-based math + parity tests; Task 2 = interval accumulation + envelope wiring + test; Task 3 = CI/finish. Matches spec.
- Placeholders: none; Task 1 ships complete code, Task 2 gives exact line-anchored edits.
- Type consistency: `IQMoments` and `finalize_moments`/`moments_from_iq` used identically across iq_utils, both result containers, worker, consumer, and tests; `_build_envelope`/`_publish_processed` gain a matching `iq_stats: IQStatistics` param.
