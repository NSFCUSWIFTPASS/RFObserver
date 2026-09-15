# Dashboard spinner clears before the picked range appears

Date: 2026-09-15.

## The question

"When I select a range, for example last 24 hours, the spinning stops before
the load is complete and the dashboard blur goes away, but it updates a few
seconds after that." The spinner should stop only once the selected range has
rendered.

Reproduced locally: the mock pipeline with 74,000 windows seeded over 24 h
(`seed_windows.py`), so a 24 h waterfall takes 3.8 s. The Dashboard was open in
Now mode.

## The answer

- In Now mode, a preset click calls `pollTick()`.
- `pollTick()` only scheduled the next 2 s tick when a load was already in
  flight (`state.loading`).
- That in-flight poll was for the previous range and was still the latest load
  (`loadSeq`). So when it finished it rendered the old range and cleared the
  spinner and blur.
- The picked range loaded on the next tick, seconds later.
- On long ranges a poll is in flight most of the time, so this happened on most
  clicks.

Fix:
- User actions call `pollTick(true)` and start a new load at once. `loadAll`
  already aborts the older fetches and bumps `loadSeq`, so the old range can
  neither render nor clear the spinner.
- Timer ticks, and the visibility refresh, keep deferring.

A separate finding: `/static` had no `Cache-Control`, so browsers kept running
a stale `averaged.js` after an update until a hard refresh. Static files now
carry `Cache-Control: no-cache`, and the ETag turns revalidation into a 304.

## Procedure

1. **Read `averaged.js`.** Found where the spinner is cleared: only in
   `loadAll`'s `finally`, and only by the latest load. Traced the preset click
   into `pollTick`, which returned early on `state.loading`.
2. **Browser instrumentation in the user's Chrome**, against the seeded mock
   instance. A `fetch` wrapper logged waterfall start and end with the requested
   span. MutationObservers logged the spinner class and `#avg-status`. The
   sequence: pick 12 h, wait until a poll is in flight, then pick 24 h.
3. **A Puppeteer regression** in `tests/ui/puppeteer_avg_history.js`:
   - waterfall responses are slowed by 1.5 s, so a poll is reliably in flight;
   - it picks "Last 24 hours" during a 7-day poll;
   - it records `#avg-status` at the moment the spinner turns off.

   It failed before the fix and passes after.
4. **Repeated step 2 after the fix.** It still failed in Chrome until a hard
   reload, which led to the cache finding.

## Evidence

Before the fix (step 2):

```
17.78s fetch START waterfall span=12.00h
17.84s CLICK 24h (waterfall fetches in flight: 1)
17.84s spinner ON
19.43s fetch END waterfall span=12.00h status=200
19.50s spinner OFF
19.50s status: 37110 windows in 601 buckets (1.2 min/row) - Live     <- 12 h data
21.50s fetch START waterfall span=24.00h                              <- next 2 s tick
25.48s fetch END waterfall span=24.00h status=200
25.56s status: 74114 windows in 601 buckets (2.4 min/row) - Live     <- 24 h, 6 s after spinner off
```

After the fix, with a hard reload:

```
6.00s CLICK 24h (waterfall fetches in flight: 1)
6.00s fetch START waterfall span=24.00h
6.01s spinner ON
6.01s fetch ABORTED waterfall span=12.00h
9.61s fetch END waterfall span=24.00h
9.84s spinner OFF
9.84s status: 74363 windows in 601 buckets (2.4 min/row) - Live
```

Puppeteer regression:

```
before: spinner turned off with status: 74488 windows in 601 buckets (16.8 min/row) - Live   (FAIL)
after:  spinner turned off with status: 74465 windows in 601 buckets (2.4 min/row) - Live    (PASS)
```

Static headers after the fix:
`HTTP/1.1 200`, `cache-control: no-cache`, `etag: "9537..."`. A conditional
GET with `If-None-Match` returns `304`.

## Measured and REJECTED (do not retry)

- **"The server-side one-at-a-time waterfall cap (branch B) makes the load
  finish late."** Rejected. The instrumented timeline shows the picked range's
  fetch was not even started until the next poll tick, 2 s after the old poll
  finished. The spinner was cleared by the old poll, not by a slow new one.

## Measurement traps

- **The browser ran the old script.** After editing `averaged.js`, a normal
  navigation in Chrome still showed the bug: `fetch("/static/averaged.js",
  {cache: "no-store"})` returned the new text while the page ran the cached
  one. Without `Cache-Control`, Chrome's heuristic freshness can last hours or
  days for a file with an old Last-Modified. Hard reload, or check the served
  headers, before concluding a UI fix did not work.
- **The Chrome extension's browser is not on this workstation.** `localhost:8888`
  refused the connection; use the workstation LAN address (192.168.97.169).
- **The existing quick-range UI check** called `waitStatusContains()` after
  `waitSpinnerCycle()`. That masked this bug: the status caught up on its own a
  few seconds after the spinner had already stopped.

## Open, not yet answered

- Drag-zoom and absolute Apply leave Now mode and call `loadAll(false)`
  directly, which already supersedes. They were not re-measured.
- Other stale-asset paths, such as a reverse proxy or service worker, do not
  exist today and were not considered.
