# Recording memory-safety — design

## Problem

A manual/triggered recording accumulates one PSD grid (`float32`, `(n_slices,
num_fft_bins)`) per processed chunk into an unbounded in-RAM list
(`StreamingProcessor._recording_grids`), **regardless of `RECORDING_RAM_BUFFER`**.
It grows ~20 MB/s at default settings (26 MHz, 1024 bins, 0.2 ms slices) and, at
stop, `np.concatenate(grids)` allocates a second full copy. On a memory-limited
board (Jetson/RPi, 7–8 GB) a long-enough recording exhausts RAM → swap thrash →
whole-OS starvation (SSH dead, PID 1 can't pet the watchdog → reset). This was
reproduced: `MemAvailable` fell linearly 5.3 GB → 0.67 GB in ~78 s and recovered
fully the instant the process was killed, confirming the grid list as the cause.
It is storage-independent (reproduced writing to a fast NVMe SSD).

Two secondary problems compound it:
- `_end_recording` runs **synchronously in the asyncio event loop**, so stop
  blocks the WebUI/websocket/heartbeat (measured 7.5–8.6 s for a 5 s recording).
- The captures viewer (`captures.py`) does `np.load(npz)["grid"]`, loading the
  **entire** grid into RAM on every window request → the same OOM at view time.

## Decisions (from brainstorming)

- Grid persistence **follows the existing `RECORDING_RAM_BUFFER` flag**, because
  streaming grids to disk on a slow-SD RPi would recreate the I/O bottleneck.
- **RAM mode:** keep grids (and IQ) in RAM but **cap the recording duration from
  available RAM** so it can never OOM.
- **Disk mode:** stream grid rows to disk during recording (no RAM
  accumulation, no concatenate).
- Unify the on-disk grid format to a memmap-friendly raw file + JSON sidecar for
  **all new recordings**; the reader falls back to the legacy `.npz` for existing
  captures.

## Architecture

### On-disk grid format (new, both modes)

Companion files next to `<base>.sc16`:
- `<base>.psd` — raw `float32`, row-major `(rows, num_fft_bins)`, C-contiguous.
- `<base>.psd.json` — metadata:
  `{"rows", "num_bins", "time_resolution_s", "center_freq_hz", "bandwidth_hz",
    "freq_axis": [...], "grid_min", "grid_max", "cal_offset_db"?}`.

`grid_min`/`grid_max` are computed incrementally while recording so the reader
never has to scan the whole grid. `cal_offset_db` is omitted when uncalibrated.

### Grid collection — gated by `RECORDING_RAM_BUFFER` (`streaming.py`)

The append site (`_handle_chunk_result`, currently
`self._recording_grids.append(grid.copy())`) and `_end_recording` change to:

- **Disk mode (`False`):** on `_begin_recording` open `<base>.psd` for binary
  append and start a running `min`/`max` and `rows` counter and cache
  `freq_axis`/`time_res`. Per chunk, write `grid.astype(float32, copy=False).tobytes()`
  to the file (bounded RAM: one chunk). At stop, close the file and write
  `<base>.psd.json`. No list, no concatenate.
- **RAM mode (`True`):** keep appending grids to the in-RAM list as today, but the
  recording is bounded by a RAM-derived max duration (below). At stop, write
  `<base>.psd` by streaming the list rows to the file (no `np.concatenate`; write
  each array's bytes sequentially) and write `<base>.psd.json`.

Writing raw bytes sequentially in both modes avoids the `concatenate`/`savez`
doubling entirely. (`savez_compressed` was measured to spike RAM ~2× even from a
memmap, so the compressed `.npz` writer is dropped for new recordings.)

### RAM-derived duration cap (`streaming.py`, `_begin_recording`)

At record start, compute the effective auto-stop duration:

```
avail = MemAvailable_bytes()                       # from /proc/meminfo
budget = avail * RECORDING_MEM_FRACTION            # new setting, default 0.5
grid_bps = (1000.0 / PSD_TIME_RESOLUTION_MS) * NUM_FFT_BINS * 4
iq_bps   = BANDWIDTH * 4                            # sc16 = 4 bytes/sample
ram_bps  = (grid_bps + iq_bps) if RECORDING_RAM_BUFFER else 0   # disk mode holds ~nothing
ram_max_sec = inf if ram_bps == 0 else budget / ram_bps
configured  = RECORDING_MAX_SEC if RECORDING_MAX_SEC > 0 else inf
effective_max_sec = min(configured, ram_max_sec)
```

The existing auto-stop check in `_check_trigger_and_record` uses
`effective_max_sec` instead of raw `RECORDING_MAX_SEC`. In disk mode `ram_bps==0`
→ no RAM cap (bounded by disk space / configured max). In RAM mode the cap
guarantees IQ+grids stay within `budget`. If `effective_max_sec` is very small
(tight RAM), log a warning at record start. The RAM-mode pre-allocation
(`_begin_recording`) uses `effective_max_sec` for its buffer size too.

### Off-loop stop (`streaming.py` / `api.py`)

`_end_recording` does blocking file I/O; run it off the event loop. The recording
stop path (`stop_recording`) invoked from the async API handler
(`/api/recording/stop`) wraps the blocking finalize in `asyncio.to_thread(...)`
(or `run_in_executor`) so the event loop keeps serving the WebSocket/heartbeat.
Auto-stop paths already run on the receiver thread — keep those synchronous.

### Reader (`captures.py`)

`get_psd_grid`:
- Resolve `<base>`. If `<base>.psd` + `<base>.psd.json` exist → new path:
  `np.memmap(<base>.psd, dtype=float32, mode="r", shape=(rows, num_bins))`, slice
  `[start:end]`, downsample bins as today, use `grid_min`/`grid_max` from the
  sidecar (no full scan). Only the requested window is materialized.
- Else if `<base>.npz` exists → legacy path unchanged.
- `has_psd` (list/detail) is true if either companion exists.

### Deploy — memory backstop (`deploy/rfobserver.service`)

Add `MemoryHigh`/`MemoryMax` so any regression is OOM-killed + auto-restarted
rather than freezing the board:

```
MemoryHigh=3G
MemoryMax=4G
```

(Not app code — an OS-level guardrail.) Values are sized for a ~7–8 GB sensor;
document that they should be tuned per box.

### Settings (`config.py`)

- `RECORDING_MEM_FRACTION: float = 0.5` — fraction of `MemAvailable` a RAM-mode
  recording may use for its buffers before auto-stop.

## Error handling

- `<base>.psd` open/write failure in disk mode: log, stop the recording, keep the
  `.sc16` (IQ) intact; no PSD companion for that capture.
- Missing/corrupt `.psd.json`: reader treats it as "no PSD" (returns the existing
  not-found response), does not crash the viewer.
- `MemAvailable` unreadable: fall back to a conservative fixed cap
  (`RECORDING_MAX_SEC` or 30 s) and log.

## Testing

- **Grid streaming (disk mode):** a recording writes `<base>.psd` + `.psd.json`
  with correct `rows`/`num_bins`; RSS stays bounded across many chunks (no
  linear growth); no `.npz` produced.
- **RAM-derived cap:** with a tiny `RECORDING_MEM_FRACTION`/large rate, the
  computed `effective_max_sec` is small and the recording auto-stops at it; the
  helper that computes the cap is unit-tested against known inputs.
- **Off-loop stop:** stopping a recording does not block the event loop — an
  `/api/*` request served concurrently with a stop returns promptly (or a unit
  test asserting `stop_recording` schedules the finalize via `to_thread`).
- **Reader back-compat:** `get_psd_grid` returns a window from a new `.psd`
  capture (memmap path) and from a legacy `.npz` capture; window slicing +
  bin-downsampling match; `grid_min/max` come from the sidecar.
- **Round-trip:** grid written in disk mode reads back bit-identical via the
  reader's memmap slice.

## Out of scope

- No change to the IQ `.sc16` recording path beyond the existing RAM/disk flag.
- No re-compression of PSD grids (raw `.psd` is uncompressed; disk-space tradeoff
  accepted — it's memmap-scalable and avoids the RAM spike).
- Migrating/rewriting existing `.npz` captures (reader keeps reading them).
