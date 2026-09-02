# Dashboard zoom "does not recompute": stale poll response overwrites the zoom

Date: 2026-09-02
Component: web UI, `src/rfobserver/web/static/averaged.js` (the averaged/Dashboard page)
Symptom: after drag-zooming the Averaged PSD Waterfall, the time axis shows the
zoomed (narrow) range but the waterfall still shows the pre-zoom coarse
resolution. Screenshot: axis `08:41:17 → 08:42:32` (~75 s) with header
`10101 windows, 601 buckets (18.0 s/row)` (a full ~3 h aggregation). Each visible
band is one 18 s bucket, so it looks like the zoom "did not recompute".

## The answer

`loadAll()` had **no guard against a stale async response overwriting newer
state**. In "Now" (live) mode a poll fires `loadAll` every 2 s for the current
wide range. When the user drag-zooms while such a poll is in flight, the poll's
wide response (larger, slower) resolves **after** the drag's narrow response and
overwrites `state.wf` with the coarse 18 s/601-bucket data, then re-renders it
under the already-zoomed axis. `setLive(false)` on the drag only cancels the
*next* scheduled poll; it does not abort the one already in flight.

Fix: a monotonic load-sequence token plus an `AbortController`. Each `loadAll`
takes a token and aborts the previous load's fetches; after every `await` it
bails out if a newer load has started, so only the latest load may commit state
or render. The `finally` is likewise gated so a superseded load cannot clear the
`loading`/`stale` flags out from under the newer one.

## The procedure that produced it (probes, in order)

1. **Server-side isolation** — seeded a throwaway DB with 3 h of `avg_windows`
   (0.5 s duration, 5 s cadence → 2160 windows) and called `query_avg_waterfall`
   directly for a wide range vs. two zoomed ranges. Controls for "is the bug in
   the SQL/aggregation?". Result: server recomputes perfectly (below) → bug is
   client-side.
2. **Client zoom in isolation** — served the seeded DB, drove the page over the
   LAN, set an **absolute** 3 h range (not live), drag-zoomed. Controls for "does
   the zoom path itself work without live polling?". Result: recomputes fine
   (18 s → 5.2 s → raw). So the bug needs the live/poll path.
3. **Code read of `loadAll`** — confirmed no request-sequencing / stale-response
   guard; concurrent `loadAll` calls are last-write-wins.
4. **Deterministic race reproduction** — monkeypatched `window.fetch` in the page
   to delay any waterfall fetch whose span > 1 h by 6–8 s (simulates the slow
   wide poll landing last), then fired a wide load followed immediately by a
   narrow load. Reproduced the exact symptom.
5. **Fix verification** — same delayed-wide instrument, real **synthetic** drag
   (dispatching `mousedown`/`mousemove`×6/`mouseup`, because the automation's
   `left_click_drag` often omits the intermediate `mousemove` the handler needs):
   the zoom recomputes AND the later wide response is discarded.

## Evidence (raw)

Server recompute (probe 1):
```
[WIDE 3h]  span 10800 s  mode aggregated  bucket_sec 18.000  buckets 600
[ZOOM 20m] span 1200 s   mode raw         bucket_sec 2.000   buckets 240
[ZOOM 2m]  span 120 s    mode raw         bucket_sec 0.200   buckets 24
```

Race reproduced, pre-fix (probe 4), narrow apply then delayed wide apply:
```
afterNarrow:  "48 windows, no averaging (raw rows)"        range 08:45 → 08:47
afterWide:    "3995 windows, 600 buckets (18.0 s/row)"     range 08:45 → 08:47   <-- WIDE overwrote
```

Fix verified, post-fix, real synthetic drag with a wide poll in flight (probe 5):
```
base:      "3875 windows, 601 buckets (18.0 s/row)"   (3 h view)
afterDrag: "1080 windows, 601 buckets (4.5 s/row)"    range 08:22 → 09:07   (zoom recomputed)
afterWide: "1080 windows, 601 buckets (4.5 s/row)"    range 08:22 → 09:07   (stale wide discarded)
```

## Measured and REJECTED (do not retry)

- **"The server does not recompute the average for the zoomed window."** Refuted
  by probe 1: `bucket_sec = span / max_rows`, so the server always recomputes.
  The original feature framing ("recalculate the average on zoom") was already
  implemented server-side; the real defect is the client discarding that result.
- **"The overlay canvas swallows the drag."** `.avg-chart-wf-overlay` is
  `pointer-events: none` (style.css); the drag reaches `avg-wf`. Not the cause.
- **"loadAll fails on the zoom fetch and keeps old data."** The failure path does
  not `renderAll`, so the axis would stay wide too; the screenshot has a narrow
  axis, so a render with the new axis did happen. Not the cause.

## Measurement traps hit

- **Browser cache served the OLD `averaged.js`.** After editing the file, a normal
  reload reused the cached script (`performance.getEntriesByType('resource')`
  showed the script entry `transferSize: 0`, 50715 bytes = pre-fix), so the first
  post-fix test still "failed". A hard reload (`cmd+shift+r`) loaded the 52728-byte
  fixed file and the guard worked. Always confirm `transferSize > 0` for the JS
  before trusting a client-side fix test.
- **`document.hidden` in the automation tab suppresses "Now" polling.** `pollTick`
  returns early when `document.hidden`, so the automated tab never auto-loaded;
  had to drive loads via absolute Apply / synthetic events instead of relying on
  live polling.
- **`left_click_drag` intermittently omits `mousemove`**, so the drag handler saw
  `x1 == x0` (< 8 px) and treated it as a click (no zoom). Use synthetic
  `mousemove` events for a reliable drag.
- **Remote Chrome cannot reach the workstation's `127.0.0.1`.** The Claude-in-Chrome
  target is the user's Mac; bind the test server to `0.0.0.0` and use the LAN IP.

## Open, not yet answered

- Raw mode renders each `avg_window` at its stored `duration_sec` (0.5 s) with the
  real inter-window gap, so a zoomed raw view can look sparse/striped. Not part of
  this bug, but worth a separate look at how raw rows tile the time axis.
