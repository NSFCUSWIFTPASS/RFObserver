# Subsample-Moments Perf Fix Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Restore ~5-10 ms/chunk stats on the sensor by subsampling the sums+histogram in `moments_from_iq` while keeping `max` full-resolution, so the Jetson runs realtime again and live data returns.

**Tech:** numpy. Env: prefix Python with `PYTHONPATH=`, use `.venv/bin`. ruff global. No emojis, no em-dashes, no Co-Authored-By.

## Global Constraints
- Only touch `src/rfobserver/processing/iq_utils.py::moments_from_iq` and `tests/unit/test_iq_stats_parity.py`.
- `finalize_moments`, `IQMoments`, `IQMoments.add`, `calculate_iq_statistics`, HIST_EDGES, and all pipeline wiring stay unchanged.
- `max_pow` MUST be computed over the FULL array (exact peak); everything else subsampled to ~262K.
- All CLAUDE.md checks stay green.

---

### Task 1: Subsample sums+histogram, full-res max (`moments_from_iq`) + tests

**Files:** Modify `src/rfobserver/processing/iq_utils.py` (`moments_from_iq` only). Test: `tests/unit/test_iq_stats_parity.py`.

- [ ] **Step 1: Update the parity tests first (TDD).**
  - Add an EXACT formula test that bypasses subsampling by building `IQMoments` from the full array directly, and asserts `finalize_moments` matches a verbatim rf-processor copy to < 1e-6 for average/std/kurtosis (and max exact, median within one bin):
    ```python
    def _full_moments(data):
        import numpy as np
        from rfobserver.processing.iq_utils import IQMoments, HIST_EDGES
        mag = np.abs(data).astype(np.float64); p = mag*mag
        hist,_ = np.histogram(p, bins=HIST_EDGES)
        return IQMoments(n=int(mag.size), s_abs=float(mag.sum()), s_pow=float(p.sum()),
                         s_pow2=float(np.dot(p,p)), max_pow=float(p.max()), hist=hist.astype("int64"))

    def test_finalize_exact_on_full_moments():
        for iq in _signals():
            ref = _refproc(iq)
            got = finalize_moments(_full_moments(iq))
            assert abs(got.average - ref["average"]) < 1e-6
            assert abs(got.std - ref["std"]) < 1e-6
            assert abs(got.kurtosis - ref["kurtosis"]) / (abs(ref["kurtosis"])+1e-9) < 1e-6
            assert abs(got.max - ref["max"]) < 1e-9
    ```
  - Relax the existing folded `moments_from_iq` test to sampling-noise tolerances and assert max EXACT:
    ```python
    @pytest.mark.parametrize("nchunks", [1, 7, 64])
    def test_folded_moments_match_refproc(nchunks):
        for iq in _signals():
            ref = _refproc(iq)
            parts = np.array_split(iq, nchunks)
            acc = moments_from_iq(parts[0])
            for pc in parts[1:]:
                acc = acc.add(moments_from_iq(pc))
            got = finalize_moments(acc)
            assert abs(got.average - ref["average"]) < 0.2          # dB, subsample noise
            assert abs(got.std - ref["std"]) / (ref["std"]+1e-12) < 0.05
            assert abs(got.kurtosis - ref["kurtosis"]) / (abs(ref["kurtosis"])+1e-9) < 0.05
            assert abs(got.median - ref["median"]) < 0.1            # dB
            assert abs(got.max - ref["max"]) < 1e-9                 # full-res -> exact
    ```
    (Ensure `_signals()` uses enough samples that a 262K subsample is representative; the existing 1,000,000-sample signals are fine. Keep the delegation + order-independence tests.)
  - Add a loose perf guard (skippable):
    ```python
    def test_moments_from_iq_is_cheap():
        import time
        rng = np.random.default_rng(0)
        iq = (rng.standard_normal(2_000_000) + 1j*rng.standard_normal(2_000_000)).astype(np.complex64)
        moments_from_iq(iq)  # warm
        t = time.perf_counter()
        for _ in range(5):
            moments_from_iq(iq)
        assert time.perf_counter() - t < 1.0   # 5 chunks < 1s (guards against full-array histogram)
    ```

- [ ] **Step 2: Run, expect the folded/perf tests to fail** appropriately against the current full-res code where relevant, then the perf test passes only after the fix. `PYTHONPATH= .venv/bin/pytest tests/unit/test_iq_stats_parity.py -x -q`.

- [ ] **Step 3: Implement** the new `moments_from_iq`:
    ```python
    def moments_from_iq(data: np.ndarray) -> IQMoments:
        """Additive power moments. max is full-resolution (exact peak); the sums and
        median histogram use a ~262K subsample so per-chunk cost stays realtime on
        constrained sensors (folded over an interval this is a very large sample)."""
        n_full = data.shape[0]
        if n_full == 0:
            return IQMoments(0, 0.0, 0.0, 0.0, 0.0, np.zeros(len(HIST_EDGES) - 1, dtype=np.int64))
        # Full-resolution max power (no sqrt): max(re^2 + im^2)
        re = data.real.astype(np.float64)
        im = data.imag.astype(np.float64)
        max_pow = float(np.max(re * re + im * im))
        # Subsample everything else
        step = max(1, n_full // (1 << 18))
        sub = data[::step]
        mag = np.abs(sub).astype(np.float64)
        p = mag * mag
        hist, _ = np.histogram(p, bins=HIST_EDGES)
        return IQMoments(
            n=int(mag.size),
            s_abs=float(mag.sum()),
            s_pow=float(p.sum()),
            s_pow2=float(np.dot(p, p)),
            max_pow=max_pow,
            hist=hist.astype(np.int64),
        )
    ```
  (Keep the field ORDER matching the `IQMoments` dataclass. If constructing positionally is unclear, use keyword args.)

- [ ] **Step 4: Run** `PYTHONPATH= .venv/bin/pytest tests/unit/test_iq_stats_parity.py -x -q` -> PASS, then the full unit suite `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`.

- [ ] **Step 5:** `ruff check src/ tests/ && ruff format --check src/ tests/` + `PYTHONPATH= .venv/bin/mypy src/rfobserver/`.

- [ ] **Step 6: Commit** — `git commit -am "iq stats: subsample per-chunk moments (keep max full-res) to restore realtime on the sensor"`.

## Self-Review
- Only `moments_from_iq` + its tests change. max full-res/exact; sums+median subsampled. `re*re+im*im` avoids sqrt. Empty-array guarded.
