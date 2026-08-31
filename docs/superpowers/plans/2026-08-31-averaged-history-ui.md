# Averaged-History UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `/averaged/` page showing historical averaged windows over a user-selected datetime range (presets: last day / last 2 days / last week) as a stats-over-time chart, a time-bucketed PSD waterfall with a captures-style selector line, the selected bucket's PSD spectrum, per-bucket stats, and the range's detections. Week-scale ranges are compressed by server-side time-bucket aggregation delivered as one binary HTTP response.

**Architecture:** `SensorDatabase.query_avg_waterfall` pages through the range's PSD blobs, aggregates them into `max_rows` time buckets (per-bin mean, 2048 -> `max_bins` bin downsampling via the `_slice_psd` reshape-mean pattern), and returns header/meta/PSD/stats arrays; `query_avg_stats` does the same over the light columns only (blob-independent, works after retention prunes PSD); `avg_window_configs` feeds the tuning filter. `api.py` packs the waterfall into a binary body (16-byte header + 48-byte meta + float32 rows + float64 stats, little-endian) with an in-memory LRU cache, adds `since`/`until` to `/api/detections.json`, and the page is a new `averaged.py` route + `averaged.html` + `averaged.js` reusing `shared-charts.js`.

**Tech Stack:** Python 3.11, aiosqlite, numpy (float32 PSD aggregation), FastAPI, pytest + pytest-asyncio, httpx ASGITransport.

**Spec:** `docs/superpowers/specs/2026-08-31-averaged-history-ui-design.md`

## Global Constraints

- **Run all commands with `PYTHONPATH=` prefix** and the venv: e.g. `PYTHONPATH= .venv/bin/pytest ...`. The host PYTHONPATH leaks system Python 3.10 packages otherwise.
- **No emojis, no em-dashes** anywhere in code, comments, or docs.
- **mypy runs without numpy stubs** (numpy is treated as `Any`). Keep return values concrete: aggregate with explicit numpy calls and return plain lists/arrays annotated with concrete types, never `return np.frombuffer(...).tolist()` at an `-> list[float]` boundary without a `float()` comprehension.
- **Binary packing uses explicit `struct`/`bytes`**, annotated concretely (see `_psd_frame_bytes` in `captures.py` for the established pattern).
- **Commit staging is explicit paths only.** Never `git add -A` or `git add .`.
- **No `Co-Authored-By` / Claude trailer** in commits.
- **Full check suite before each commit** where practical: `~/.local/bin/ruff check src/ tests/`, `~/.local/bin/ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q` (the final task runs the full suite incl. integration with NATS on `:4222`).
- Branch: `feat/averaged-window-store` (continue the current feature branch).

---

### Task 1: DB aggregation methods + unit tests

**Files:**
- Modify: `src/rfobserver/storage/database.py` (add `query_avg_waterfall`, `query_avg_stats`, `avg_window_configs`)
- Test: `tests/unit/test_database.py`

**Interfaces:**
- `SensorDatabase.query_avg_waterfall(*, since: datetime, until: datetime, sdr_center_freq: float | None = None, sample_rate: float | None = None, gain: float | None = None, max_rows: int = 600, max_bins: int = 512) -> dict[str, Any]`
  - Returns: `{"bucket_sec", "num_bins", "min_db", "max_db", "total_windows", "freq_start_hz", "freq_step_hz", "buckets": [{"start_epoch", "count", "pwr_avg", "pwr_max", "pwr_median", "pwr_std", "kurtosis"}], "psd_rows": list[list[float]]}` where a bucket with no PSD (empty or pruned) has a row of `math.nan`.
- `SensorDatabase.query_avg_stats(*, since, until, sdr_center_freq=None, sample_rate=None, gain=None, max_points: int = 600) -> dict[str, Any]`
  - Returns: `{"bucket_sec", "min_pwr", "max_pwr", "points": [{"start_time", "count", "pwr_avg", "pwr_max", "pwr_median", "pwr_std", "kurtosis"}]}`
- `SensorDatabase.avg_window_configs() -> dict[str, Any]`
  - Returns: `{"configs": [...], "latest": {...} | None}`

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_database.py`:

```python
def _avg_common(**overrides):
    c = dict(
        duration_sec=0.5, sdr_center_freq_hz=100e6, sample_rate_hz=4.0, gain_db=40.0,
        num_bins=4, freq_start_hz=98.0, freq_step_hz=1.0, pwr_avg=-70.0, pwr_max=-50.0,
        pwr_median=-72.0, pwr_std=3.0, kurtosis=1.0, powers=[-80.0, -70.0, -60.0, -50.0],
    )
    c.update(overrides)
    return c


async def test_query_avg_waterfall_buckets_by_time(db):
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    # 8 windows, 1s apart, 2s buckets -> 4 buckets of 2 windows.
    for i in range(8):
        await db.insert_avg_window(
            start_time=base + timedelta(seconds=i),
            powers=[-80.0, -70.0, -60.0, -50.0],
            **_avg_common(),
        )
    result = await db.query_avg_waterfall(
        since=base, until=base + timedelta(seconds=8), max_rows=4
    )
    assert result["bucket_sec"] == pytest.approx(2.0)
    assert len(result["buckets"]) == 4
    assert [b["count"] for b in result["buckets"]] == [2, 2, 2, 2]
    row0 = result["psd_rows"][0]
    assert row0 == pytest.approx([-80.0, -70.0, -60.0, -50.0], abs=1e-3)
    assert result["min_db"] <= -80.0
    assert result["max_db"] >= -50.0


async def test_query_avg_waterfall_downsamples_bins(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    # num_bins=4, max_bins=2 -> factor 2 mean per group.
    await db.insert_avg_window(
        start_time=base, num_bins=4, freq_start_hz=98.0, freq_step_hz=1.0,
        powers=[-80.0, -70.0, -60.0, -50.0],
        **_avg_common(num_bins=4, powers=[-80.0, -70.0, -60.0, -50.0]),
    )
    result = await db.query_avg_waterfall(
        since=base, until=base + timedelta(seconds=1), max_rows=1, max_bins=2
    )
    assert result["num_bins"] == 2
    assert result["psd_rows"][0] == pytest.approx([-75.0, -55.0], abs=1e-3)
    # Downsampled axis is still uniform: start' = 98 + (2-1)*1/2 = 98.5, step' = 2.
    assert result["freq_start_hz"] == pytest.approx(98.5)
    assert result["freq_step_hz"] == pytest.approx(2.0)


async def test_query_avg_waterfall_pruned_blob_stats_only(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(start_time=base, **_avg_common(powers=[-70.0, -60.0, -50.0, -40.0]))
    # Prune the blob (retention) -> the window still counts toward stats.
    await db._db.execute("UPDATE avg_windows SET psd_powers = NULL WHERE id = 1")
    await db._db.commit()
    result = await db.query_avg_waterfall(
        since=base, until=base + timedelta(seconds=1), max_rows=1
    )
    assert result["total_windows"] == 0  # no PSD windows
    assert result["buckets"][0]["count"] == 1  # stats still counted
    assert all(math.isnan(v) for v in result["psd_rows"][0])
    assert result["buckets"][0]["pwr_avg"] == pytest.approx(-70.0)


async def test_query_avg_stats_works_without_blobs(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(start_time=base, **_avg_common(pwr_max=-50.0))
    await db.insert_avg_window(start_time=base + timedelta(seconds=1), **_avg_common(pwr_max=-45.0))
    result = await db.query_avg_stats(since=base, until=base + timedelta(seconds=2), max_points=1)
    assert len(result["points"]) == 1
    p = result["points"][0]
    assert p["count"] == 2
    assert p["pwr_avg"] == pytest.approx(-70.0)
    assert p["pwr_max"] == pytest.approx(-45.0)  # max of maxes


async def test_avg_window_configs_distinct_and_latest(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(start_time=base, sdr_center_freq_hz=100e6, **_avg_common())
    await db.insert_avg_window(start_time=base, sdr_center_freq_hz=200e6, **_avg_common())
    await db.insert_avg_window(
        start_time=base + timedelta(seconds=1), sdr_center_freq_hz=200e6, **_avg_common()
    )
    result = await db.avg_window_configs()
    assert {c["sdr_center_freq_hz"] for c in result["configs"]} == {100e6, 200e6}
    assert result["latest"]["sdr_center_freq_hz"] == 200e6
```

`math` is already imported in `test_database.py`? No - add `import math` at the top if missing (check; `datetime, timedelta, timezone` are imported, `pytest` is imported).

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k "waterfall or avg_window_configs or query_avg_stats" -x -q`
Expected: FAIL with `AttributeError: ... 'query_avg_waterfall'`.

- [ ] **Step 3: Implement `query_avg_waterfall`**

Add to `SensorDatabase` (uses existing `_sdr_conditions`; keep numpy usage concrete):

```python
async def query_avg_waterfall(
    self,
    *,
    since: datetime,
    until: datetime,
    sdr_center_freq: float | None = None,
    sample_rate: float | None = None,
    gain: float | None = None,
    max_rows: int = 600,
    max_bins: int = 512,
) -> dict[str, Any]:
    """Aggregate the range's averaged windows into max_rows time buckets.

    Each bucket's PSD is the per-bin mean of its windows' PSDs (2048 bins
    downsampled to max_bins by group-mean); scalar stats are mean-of-means
    except pwr_max which is max-of-maxes. Windows whose blob was pruned by
    retention count toward the stats but contribute no PSD (their bucket row
    becomes all-NaN). Returns arrays ready for the binary packer.
    """
    assert self._db is not None
    span = (until - since).total_seconds()
    if span <= 0:
        return {
            "bucket_sec": 0.0, "num_bins": 0, "min_db": 0.0, "max_db": 0.0,
            "total_windows": 0, "freq_start_hz": 0.0, "freq_step_hz": 0.0,
            "buckets": [], "psd_rows": [],
        }
    bucket_sec = span / max_rows
    conditions, params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
    where = f"WHERE start_time >= ? AND start_time < ?"
    if conditions:
        where += " AND " + " AND ".join(conditions)
    query = (
        "SELECT start_time, num_bins, freq_start_hz, freq_step_hz, psd_powers, "
        "pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis "
        f"FROM avg_windows {where} ORDER BY start_time"
    )
    # ... (fetchmany paging, numpy accumulation; see plan body for the full code)
```

Full implementation (the paging + accumulation core):

```python
        since_epoch = since.timestamp()
        # Accumulators: float64 sums (PSD), per-bucket window lists for stats.
        psd_sum = np.zeros((max_rows, max_bins), dtype=np.float64)
        psd_n = np.zeros(max_rows, dtype=np.int64)
        stat_avg = [0.0] * max_rows
        stat_max = [-float("inf")] * max_rows
        stat_med = [0.0] * max_rows
        stat_std = [0.0] * max_rows
        stat_kurt = [0.0] * max_rows
        stat_n = [0] * max_rows
        total_windows = 0
        gmin, gmax = float("inf"), float("-inf")
        freq_start_hz = 0.0
        freq_step_hz = 0.0
        rows_scanned = 0
        async with self._db.execute(query, params) as cursor:
            while True:
                rows = await cursor.fetchmany(5000)
                if not rows:
                    break
                for r in rows:
                    rows_scanned += 1
                    t = datetime.fromisoformat(r[0]).timestamp()
                    idx = int((t - since_epoch) / bucket_sec)
                    idx = max(0, min(idx, max_rows - 1))
                    num_bins = int(r[1])
                    if rows_scanned == 1:
                        freq_start_hz = float(r[2])
                        freq_step_hz = float(r[3])
                    stat_n[idx] += 1
                    stat_avg[idx] += float(r[5])
                    stat_max[idx] = max(stat_max[idx], float(r[6]))
                    stat_med[idx] += float(r[7])
                    stat_std[idx] += float(r[8])
                    stat_kurt[idx] += float(r[9])
                    blob = r[4]
                    if blob is None:
                        continue
                    total_windows += 1
                    powers = np.frombuffer(blob, dtype="<f4")
                    if len(powers) != num_bins:
                        num_bins = len(powers)
                    if num_bins > max_bins:
                        factor = num_bins // max_bins
                        trim = factor * max_bins
                        powers = powers[:trim].reshape(max_bins, factor).mean(axis=1)
                    else:
                        pad = max_bins - num_bins
                        if pad > 0:
                            powers = np.concatenate(
                                [powers, np.full(pad, np.nan, dtype=np.float32)]
                            )
                    gmin = min(gmin, float(np.nanmin(powers)))
                    gmax = max(gmax, float(np.nanmax(powers)))
                    psd_sum[idx] += powers
                    psd_n[idx] += 1
        # Normalize + build rows. NaN = no PSD in that bucket.
        psd_rows: list[list[float]] = []
        for i in range(max_rows):
            if psd_n[i] > 0:
                psd_rows.append([float(x) for x in (psd_sum[i] / psd_n[i])])
            else:
                psd_rows.append([float("nan")] * max_bins)
        buckets = [
            {
                "start_epoch": since_epoch + i * bucket_sec,
                "count": stat_n[i],
                "pwr_avg": stat_avg[i] / stat_n[i] if stat_n[i] else 0.0,
                "pwr_max": stat_max[i] if stat_n[i] else 0.0,
                "pwr_median": stat_med[i] / stat_n[i] if stat_n[i] else 0.0,
                "pwr_std": stat_std[i] / stat_n[i] if stat_n[i] else 0.0,
                "kurtosis": stat_kurt[i] / stat_n[i] if stat_n[i] else 0.0,
            }
            for i in range(max_rows)
        ]
        return {
            "bucket_sec": bucket_sec,
            "num_bins": max_bins,
            "min_db": gmin if gmin != float("inf") else 0.0,
            "max_db": gmax if gmax != float("-inf") else 0.0,
            "total_windows": total_windows,
            "freq_start_hz": freq_start_hz,
            "freq_step_hz": freq_step_hz,
            "buckets": buckets,
            "psd_rows": psd_rows,
        }
```

Notes:
- `num_bins` is capped at `max_bins`; pad with NaN when a window has fewer bins (config change mid-range). The downsampled axis is uniform: `start' = freq_start + (factor-1)*step/2`, `step' = factor*step` (derive from the FIRST window's axis in the API packer or here; the plan packs it here so the API stays dumb: add `freq_start_hz`/`freq_step_hz` already-downsampled when a downsample happened).
- mypy: keep `psd_rows` as `list[list[float]]` built with explicit `float()`; `np.frombuffer` results feed sums only, never returned raw.

- [ ] **Step 4: Implement `query_avg_stats` + `avg_window_configs`**

```python
async def query_avg_stats(
    self,
    *,
    since: datetime,
    until: datetime,
    sdr_center_freq: float | None = None,
    sample_rate: float | None = None,
    gain: float | None = None,
    max_points: int = 600,
) -> dict[str, Any]:
    """Scalar stats timeline for a range. Light columns only (no PSD blob
    reads), so it works after retention prunes blobs and over any range."""
    assert self._db is not None
    span = (until - since).total_seconds()
    if span <= 0:
        return {"bucket_sec": 0.0, "min_pwr": 0.0, "max_pwr": 0.0, "points": []}
    bucket_sec = span / max_points
    conditions, params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
    where = "WHERE start_time >= ? AND start_time < ?"
    if conditions:
        where += " AND " + " AND ".join(conditions)
    query = (
        "SELECT start_time, pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis "
        f"FROM avg_windows {where} ORDER BY start_time"
    )
    since_epoch = since.timestamp()
    n = [0] * max_points
    avg = [0.0] * max_points
    mx = [-float("inf")] * max_points
    med = [0.0] * max_points
    std = [0.0] * max_points
    kurt = [0.0] * max_points
    gmin, gmax = float("inf"), float("-inf")
    async with self._db.execute(query, params) as cursor:
        while True:
            rows = await cursor.fetchmany(10000)
            if not rows:
                break
            for r in rows:
                idx = int((datetime.fromisoformat(r[0]).timestamp() - since_epoch) / bucket_sec)
                idx = max(0, min(idx, max_points - 1))
                n[idx] += 1
                avg[idx] += float(r[1]); med[idx] += float(r[3])
                std[idx] += float(r[4]); kurt[idx] += float(r[5])
                if r[2] is not None:
                    mx[idx] = max(mx[idx], float(r[2]))
                    gmin = min(gmin, float(r[1])); gmax = max(gmax, float(r[2]))
    points = [
        {
            "start_time": (since + timedelta(seconds=i * bucket_sec)).isoformat(),
            "count": n[i],
            "pwr_avg": avg[i] / n[i] if n[i] else None,
            "pwr_max": mx[i] if n[i] and mx[i] != -float("inf") else None,
            "pwr_median": med[i] / n[i] if n[i] else None,
            "pwr_std": std[i] / n[i] if n[i] else None,
            "kurtosis": kurt[i] / n[i] if n[i] else None,
        }
        for i in range(max_points)
    ]
    return {
        "bucket_sec": bucket_sec,
        "min_pwr": gmin if gmin != float("inf") else 0.0,
        "max_pwr": gmax if gmax != float("-inf") else 0.0,
        "points": points,
    }


async def avg_window_configs(self) -> dict[str, Any]:
    """Distinct SDR tuning configs present in avg_windows + the most recent one."""
    assert self._db is not None
    self._db.row_factory = aiosqlite.Row
    async with self._db.execute(
        """SELECT DISTINCT sdr_center_freq_hz, sample_rate_hz, gain_db
           FROM avg_windows
           WHERE sdr_center_freq_hz IS NOT NULL
           ORDER BY sdr_center_freq_hz, sample_rate_hz, gain_db"""
    ) as cursor:
        configs = [dict(row) for row in await cursor.fetchall()]
    async with self._db.execute(
        """SELECT sdr_center_freq_hz, sample_rate_hz, gain_db
           FROM avg_windows
           WHERE sdr_center_freq_hz IS NOT NULL
           ORDER BY start_time DESC LIMIT 1"""
    ) as cursor:
        row = await cursor.fetchone()
    return {"configs": configs, "latest": dict(row) if row else None}
```

- [ ] **Step 5: Run the DB tests + lint + types**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -x -q && ~/.local/bin/ruff check src/ tests/ && ~/.local/bin/ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass. (If `ruff format` complains, run `~/.local/bin/ruff format src/ tests/` and re-check.)

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/storage/database.py tests/unit/test_database.py
git commit -m "avg-windows: waterfall + stats aggregation and tuning configs"
```

---

### Task 2: API endpoints (binary waterfall + stats + configs + detections range)

**Files:**
- Modify: `src/rfobserver/web/routes/api.py` (waterfall/stats/configs routes, `since`/`until` on `detections.json`, LRU cache)
- Test: `tests/integration/test_web_integration.py`

**Interfaces:**
- `GET /api/averaged/waterfall?since&until&sdr_center&sample_rate&gain&max_rows&max_bins` -> `application/octet-stream`
  - Binary layout (all little-endian): `struct "<4i"` magic `0x52464F42`, version 1, bucket_count, num_bins; `struct "<6d"` bucket_sec, min_db, max_db, total_windows, freq_start_hz, freq_step_hz; `bucket_count * num_bins` float32 (row-major, NaN = no PSD); `bucket_count * struct "<7d"` start_epoch, pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis, count.
- `GET /api/averaged/stats` -> JSON (pass-through of `query_avg_stats`, `max_points` param)
- `GET /api/averaged/configs` -> JSON (pass-through of `avg_window_configs`)
- `GET /api/detections.json` gains optional `since`/`until`

- [ ] **Step 1: Write the failing integration tests**

Add to `tests/integration/test_web_integration.py`:

```python
@pytest.mark.asyncio
async def test_api_averaged_waterfall_binary(app_with_db):
    import struct
    from datetime import datetime, timedelta, timezone

    from httpx import ASGITransport, AsyncClient

    app, db = app_with_db
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(4):
        await db.insert_avg_window(
            start_time=start + timedelta(seconds=i), duration_sec=0.5,
            sdr_center_freq_hz=915e6, sample_rate_hz=56e6, gain_db=40.0,
            num_bins=4, freq_start_hz=0.0, freq_step_hz=1.0, pwr_avg=-70.0,
            pwr_max=-50.0, pwr_median=-72.0, pwr_std=3.0, kurtosis=1.0,
            powers=[-80.0, -70.0, -60.0, -50.0],
        )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get(
            "/api/averaged/waterfall",
            params={"since": start.isoformat(), "until": (start + timedelta(seconds=4)).isoformat(),
                    "max_rows": 2, "max_bins": 2},
        )
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("application/octet-stream")
        body = r.content
        magic, version, bucket_count, num_bins = struct.unpack_from("<4i", body, 0)
        assert magic == 0x52464F42 and version == 1
        assert bucket_count == 2 and num_bins == 2
        meta = struct.unpack_from("<6d", body, 16)
        bucket_sec, min_db, max_db, total_windows, f_start, f_step = meta
        assert bucket_sec == pytest.approx(2.0)
        assert total_windows == 4
        off = 16 + 48
        psd = struct.unpack_from(f"<{bucket_count * num_bins}f", body, off)
        assert psd[0] == pytest.approx(-75.0, abs=1e-3)  # mean of [-80,-70]
        off += bucket_count * num_bins * 4
        stats = struct.unpack_from(f"<{bucket_count * 7}d", body, off)
        assert stats[6] == 2  # count in first bucket


@pytest.mark.asyncio
async def test_api_averaged_waterfall_rejects_bad_range(app_with_db):
    from datetime import datetime, timedelta, timezone

    from httpx import ASGITransport, AsyncClient

    app, _ = app_with_db
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get(
            "/api/averaged/waterfall",
            params={"since": start.isoformat(), "until": (start - timedelta(hours=1)).isoformat()},
        )
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_api_averaged_stats_and_configs(app_with_db):
    from datetime import datetime, timezone

    from httpx import ASGITransport, AsyncClient

    app, db = app_with_db
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(
        start_time=start, duration_sec=0.5, sdr_center_freq_hz=915e6,
        sample_rate_hz=56e6, gain_db=40.0, num_bins=2, freq_start_hz=0.0,
        freq_step_hz=1.0, pwr_avg=-70.0, pwr_max=-50.0, pwr_median=-72.0,
        pwr_std=3.0, kurtosis=1.0, powers=[-70.0, -60.0],
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        s = await c.get(
            "/api/averaged/stats",
            params={"since": start.isoformat(), "until": "2026-01-02T00:00:00+00:00"},
        )
        assert s.status_code == 200
        body = s.json()
        assert body["points"][0]["count"] == 1
        cfg = await c.get("/api/averaged/configs")
        assert cfg.status_code == 200
        assert cfg.json()["latest"]["sdr_center_freq_hz"] == 915e6


@pytest.mark.asyncio
async def test_api_detections_json_since_until(app_with_db):
    from datetime import datetime, timedelta, timezone

    from httpx import ASGITransport, AsyncClient

    app, db = app_with_db
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    for bid, dt in (("early", start), ("late", start + timedelta(hours=2))):
        await db.insert_detection(
            burst_id=bid, start_time=dt, stop_time=dt + timedelta(seconds=1),
            center_freq_hz=915e6, bandwidth_hz=1e6, peak_power_db=-30.0,
            duration_ms=100.0, detection_timestamp=dt,
        )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get(
            "/api/detections.json",
            params={"since": (start + timedelta(hours=1)).isoformat(),
                    "until": (start + timedelta(hours=3)).isoformat()},
        )
        assert [d["burst_id"] for d in r.json()["detections"]] == ["late"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_web_integration.py -k "waterfall or stats_and_configs or since_until" -x -q`
Expected: FAIL (404 / missing params).

- [ ] **Step 3: Implement the packer + routes + cache**

In `api.py`:

```python
import struct
from collections import OrderedDict
from datetime import datetime  # already imported

_WATERFALL_MAGIC = 0x52464F42
_WATERFALL_VERSION = 1
_WATERFALL_CACHE: "OrderedDict[tuple[Any, ...], bytes]" = OrderedDict()
_WATERFALL_CACHE_MAX = 8


def _pack_waterfall(result: dict[str, Any]) -> bytes:
    """Pack query_avg_waterfall output into the binary body format."""
    buckets = result["buckets"]
    rows = result["psd_rows"]
    n = len(buckets)
    nb = result["num_bins"]
    header = struct.pack("<4i", _WATERFALL_MAGIC, _WATERFALL_VERSION, n, nb)
    meta = struct.pack(
        "<6d",
        result["bucket_sec"], result["min_db"], result["max_db"],
        result["total_windows"], result["freq_start_hz"], result["freq_step_hz"],
    )
    psd = b"".join(struct.pack(f"<{nb}f", *row) for row in rows)
    stats = b"".join(
        struct.pack(
            "<7d",
            b["start_epoch"], b["pwr_avg"], b["pwr_max"], b["pwr_median"],
            b["pwr_std"], b["kurtosis"], float(b["count"]),
        )
        for b in buckets
    )
    return header + meta + psd + stats


def _waterfall_cached(key: tuple[Any, ...], result: dict[str, Any]) -> bytes:
    payload = _pack_waterfall(result)
    _WATERFALL_CACHE[key] = payload
    _WATERFALL_CACHE.move_to_end(key)
    while len(_WATERFALL_CACHE) > _WATERFALL_CACHE_MAX:
        _WATERFALL_CACHE.popitem(last=False)
    return payload


@router.get("/averaged/waterfall")
async def averaged_waterfall(
    request: Request,
    since: str,
    until: str,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
    max_rows: str | None = None,
    max_bins: str | None = None,
) -> Response:
    """Averaged-window waterfall over a range, as one binary body."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    since_dt = _opt_dt(since)
    until_dt = _opt_dt(until)
    if since_dt is None or until_dt is None or since_dt >= until_dt:
        raise HTTPException(status_code=400, detail="since/until must be valid ISO times with since < until")
    mr = max(10, min(2000, int(max_rows) if max_rows else 600))
    mb = max(64, min(2048, int(max_bins) if max_bins else 512))
    key = (since, until, sdr_center, sample_rate, gain, mr, mb)
    cached = _WATERFALL_CACHE.get(key)
    if cached is not None:
        return Response(content=cached, media_type="application/octet-stream")
    result = await db.query_avg_waterfall(
        since=since_dt, until=until_dt,
        sdr_center_freq=_opt_float(sdr_center), sample_rate=_opt_float(sample_rate),
        gain=_opt_float(gain), max_rows=mr, max_bins=mb,
    )
    return Response(content=_waterfall_cached(key, result), media_type="application/octet-stream")


@router.get("/averaged/stats")
async def averaged_stats(
    request: Request,
    since: str,
    until: str,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
    max_points: str | None = None,
) -> dict[str, Any]:
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    since_dt = _opt_dt(since)
    until_dt = _opt_dt(until)
    if since_dt is None or until_dt is None or since_dt >= until_dt:
        raise HTTPException(status_code=400, detail="since/until must be valid ISO times with since < until")
    return await db.query_avg_stats(
        since=since_dt, until=until_dt,
        sdr_center_freq=_opt_float(sdr_center), sample_rate=_opt_float(sample_rate),
        gain=_opt_float(gain), max_points=int(max_points) if max_points else 600,
    )


@router.get("/averaged/configs")
async def averaged_configs(request: Request) -> dict[str, Any]:
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    return await db.avg_window_configs()
```

- [ ] **Step 4: Add `since`/`until` to `/api/detections.json`**

```python
@router.get("/detections.json")
async def detections_json(
    request: Request,
    limit: int = 200,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    ...
    rows = await db.query_detections(
        limit=limit,
        sdr_center_freq=_opt_float(sdr_center),
        sample_rate=_opt_float(sample_rate),
        since=_opt_dt(since),
        until=_opt_dt(until),
    )
```

- [ ] **Step 5: Run tests + lint + types**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_web_integration.py -k "waterfall or stats_and_configs or since_until" -x -q && ~/.local/bin/ruff check src/ tests/ && ~/.local/bin/ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/web/routes/api.py tests/integration/test_web_integration.py
git commit -m "api: binary averaged-waterfall + stats/configs endpoints, detections since/until"
```

---

### Task 3: `/averaged/` page route + template skeleton + nav

**Files:**
- Add: `src/rfobserver/web/routes/averaged.py`
- Add: `src/rfobserver/web/templates/averaged.html`
- Modify: `src/rfobserver/web/app.py` (include router), `src/rfobserver/web/templates/base.html` (nav link)

- [ ] **Step 1: Page route**

`averaged.py`:

```python
"""Averaged-history page route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/averaged/", response_class=HTMLResponse)
async def averaged_page(request: Request) -> Any:
    return HTMLResponse(_page_html())


def _page_html() -> str:
    # Served as a standalone string (no Jinja dependency in this app) that
    # includes averaged.js and the toolbar/waterfall/psd/stats/detections cards.
    ...
```

Note: check how `history.py` serves its template (`TemplateResponse` or inline HTML) and mirror it. If the app uses `fastapi.templating.Jinja2Templates`, use the same mechanism so `{% extends "base.html" %}` works.

- [ ] **Step 2: Template skeleton**

`averaged.html` extends `base.html`, contains: toolbar card (datetime-local inputs, presets, tuning selects, Apply, retention hint), stats-timeline card, waterfall card (canvas + legend + slider), PSD card, selected-bucket stats card, detections card, and loads `shared-charts.js` + `averaged.js`.

- [ ] **Step 3: Wire app + nav**

In `app.py` import and include `averaged.router`; in `base.html` add `<a href="/averaged" class="nav-link">Averaged</a>` after History.

- [ ] **Step 4: Verify page renders + lint/types**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_web_integration.py -k "averaged" -x -q` (existing averaged API tests still pass) and add a smoke test `GET /averaged/` returns 200. Then `~/.local/bin/ruff check src/ tests/ && ~/.local/bin/ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/web/routes/averaged.py src/rfobserver/web/templates/averaged.html src/rfobserver/web/app.py src/rfobserver/web/templates/base.html tests/integration/test_web_integration.py
git commit -m "web: /averaged/ history page shell + nav link"
```

---

### Task 4: Frontend JS (range selector, presets, waterfall, selector line, PSD, stats, detections)

**Files:**
- Add: `src/rfobserver/web/static/averaged.js`
- Modify: `src/rfobserver/web/templates/averaged.html` (final markup + canvas wiring)

- [ ] **Step 1: Write the JS (no test harness; verified on Jetson + by inspection)**

`averaged.js` responsibilities:
- On load: fetch `/api/averaged/configs`, populate the tuning selects (default = `latest`), set range inputs to last 24 h (until = now, since = now - 1 day), call `loadAll()`.
- `loadAll()`: fetch stats + waterfall (binary via `fetch(...).then(r => r.arrayBuffer())`) + detections for the current range/tuning; render stats chart, waterfall, reset slider to the newest bucket, render PSD + bucket stats, render detections table + overlay.
- Binary parse (`parseWaterfall(buf)`): `DataView`; read `<4i` header, `<6d` meta, float32 PSD rows, `<7d` stats; rebuild `frequencies` from `freq_start_hz + i*freq_step_hz`; return `{meta, rows, stats, freqs}`.
- Waterfall render: `ImageData(canvas.width, canvas.height)`; per row call `renderWaterfallRow(img, w, y, powers, min, max)` with NaN-safe powers (empty row -> dark placeholder like captures' not-loaded rows); `putImageData`; draw the selector line (captures `drawHighlight` pattern) on the overlay canvas; draw detection overlay boxes (`time -> row = floor((t - since)/bucket_sec)`, `freq -> x` via rebuilt axis, captures `drawDetectionOverlay` math).
- Slider (`min=0, max=bucket_count-1`) + click-to-select on the waterfall: update highlight, PSD chart (`drawPSD`), and the selected-bucket stats card. When the bucket row is all-NaN show "No PSD (empty or pruned)" in the PSD card.
- Stats chart: two lines (pwr_avg, pwr_max) over the range from `/api/averaged/stats` points; simple min/max axes with labels (dashboard timeseries pattern).
- Presets: Last Day / Last 2 Days / Last Week buttons set the inputs and call `loadAll()`.
- Dates: `datetime-local` values convert via `new Date(inputValue).toISOString()` for the API; render times in local time.

- [ ] **Step 2: Verify by inspection + unit suite still green**

Run: `~/.local/bin/ruff check src/ tests/ && ~/.local/bin/ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/ && PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q` (JS is not linted by ruff; Python must stay green).

- [ ] **Step 3: Commit**

```bash
git add src/rfobserver/web/static/averaged.js src/rfobserver/web/templates/averaged.html
git commit -m "web: averaged-history UI (range presets, waterfall selector line, PSD/stats/detections)"
```

---

### Task 5: Full-suite verification + Jetson deploy

- [ ] **Step 1: Run the FULL check suite exactly as CI does**

```bash
docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js
~/.local/bin/ruff check src/ tests/
~/.local/bin/ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
docker stop rfobs-test-nats
```

Expected: all green (integration ~4-5 min).

- [ ] **Step 2: Push the branch**

```bash
git push origin feat/averaged-window-store
```

- [ ] **Step 3: Deploy to the mock Jetson (`nano-super`, no SDR)**

```bash
ssh ocollaco@192.168.97.153 'cd ~/GitHub/RFObserver && git fetch --quiet origin && git checkout feat/averaged-window-store && git pull --quiet --ff-only origin feat/averaged-window-store && git log --oneline -1'
```

- [ ] **Step 4: Run the mock pipeline + exercise the presets**

```bash
ssh ocollaco@192.168.97.153 'cd ~/GitHub/RFObserver && fuser -k 8888/tcp 2>/dev/null; sleep 1; PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_SENSOR_ACTIVE=true RFOBS_WEB_PORT=8888 nohup .venv/bin/rfobserver run >/tmp/rfobs-run.log 2>&1 & sleep 12; echo "--- stats (last day) ---"; curl -s "http://127.0.0.1:8888/api/averaged/stats?since=$(python3 -c "from datetime import datetime,timedelta,timezone;print((datetime.now(timezone.utc)-timedelta(days=1)).isoformat())")&until=$(python3 -c "from datetime import datetime,timezone;print(datetime.now(timezone.utc).isoformat())")&max_points=10" | head -c 300; echo; echo "--- waterfall bytes ---"; curl -s "http://127.0.0.1:8888/api/averaged/waterfall?since=...&until=...&max_rows=10&max_bins=64" -o /tmp/wf.bin && ls -la /tmp/wf.bin && python3 -c "import struct;d=open(\"/tmp/wf.bin\",\"rb\").read();print(\"header\",struct.unpack_from(\"<4i\",d,0));print(\"meta\",struct.unpack_from(\"<6d\",d,16))"'
```

- [ ] **Step 5: Open the page and confirm interaction**

Via the GUI or a headless check: `curl -s http://127.0.0.1:8888/averaged/ | head -c 200`. Manual: load the page in a browser, switch presets, drag the selector line, confirm the PSD chart and stats card track it. (Report to the user what was verified programmatically and what needs a browser.)

- [ ] **Step 6: Stop the Jetson pipeline and clean up**

```bash
ssh ocollaco@192.168.97.153 'fuser -k 8888/tcp 2>/dev/null; echo stopped'
```

- [ ] **Step 7: Report** that the averaged-history UI is implemented, CI-green, and verified on `nano-super`; HCRO untouched.

---

## Self-Review

**Spec coverage:**
- Datetime start/stop selector + last-day / last-2-days / last-week presets -> Task 3 toolbar + Task 4 JS. Yes.
- Same dashboard look -> new `/averaged/` page reusing shared-charts + dashboard layout. Yes.
- Selector line on the waterfall moving the PSD (captures-style, reused) -> Task 4 (drawHighlight + drawPSD patterns). Yes.
- Compression at week scale -> time-bucket aggregation (query_avg_waterfall) + bin downsampling + binary payload. Yes.
- Stats work after PSD retention -> query_avg_stats is blob-independent. Yes.
- Detections in range -> since/until on detections.json + overlay/table. Yes.

**Placeholder scan:** no TBD/TODO; every step carries real code or exact commands. The binary format constants match between the packer (`_pack_waterfall`) and the integration test's `struct.unpack_from`.

**Type consistency:** `query_avg_waterfall`/`query_avg_stats`/`avg_window_configs` signatures match between Task 1 and Task 2 call sites. `_pack_waterfall` consumes exactly the keys the DB method returns. `max_rows`/`max_bins`/`max_points` are clamped server-side and default consistently (600/512/600).
