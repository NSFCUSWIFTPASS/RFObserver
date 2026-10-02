# Web UI lag over the VPN

## 1. The question

Date: 2026-10-01. Reported by the user: "the UI seems laggy (could be the VPN but check),
ideally we want as less data sent frequently as possible", then "moving between pages
seems slower, like between the Dashboard to config, technically moving to config
shouldn't take much time". The user also noted that a page may be served with no internet
connection.

Hosts:
- The HCRO field sensor at `http://10.1.42.31:8080/`, reached over the VPN.
  It runs v0.10.0b0 at 26 Msps, DURATION_SEC 1.0, 2048 bins, CPU PSD backend, with
  isolation and attribution on.
- nano-super (Orin Nano, 15 W, mock receiver at 56 Msps, DURATION_SEC 0.25) for the A/B.

Symptoms on the HCRO sensor, measured from the workstation (5 requests each, gzip accepted):

```
/api/health        connect 224  ttfb avg 1867 min  475  total avg 1921 max 3888 ms      813 B
/                  connect 227  ttfb avg  944 min  441  total avg 1146 max 2554 ms    13403 B
/live/             connect 195  ttfb avg  534 min  402  total avg 1444 max 2339 ms    75719 B
/config            connect 217  ttfb avg 1317 min  482  total avg 1317 max 3084 ms        0 B
/detections        connect 201  ttfb avg 2423 min  802  total avg 2511 max 4307 ms    10340 B
/captures          connect 300  ttfb avg 1256 min  644  total avg 1256 max 2158 ms        0 B
/static/style.css  connect 200  ttfb avg 1541 min  309  total avg 1851 max 3688 ms    37725 B
ping: rtt min/avg/max/mdev = 89.713/115.739/211.135/47.753 ms
```

## 2. The answer

The UI sent far more data than it displays, and over a VPN that congests the link, so
every request queues behind it.

- **Dashboard in "Now" mode:** re-fetched the whole range every 2 s, about 1.2 MB of
  float32 waterfall. That is 5.15 Mbit/s for one tab, measured on nano-super.
- **Live page:** streamed 147 KB JSON frames at 10 to 16 per second, 3.7 to 5.8 Mbit/s
  after deflate.
- **Detections, Config and Captures:** received that same PSD stream over `/ws/live`,
  then discarded it, because they only use the heartbeat.

Page changes paid for two more things on top:
- The Config and Captures nav links pointed at URLs that 307-redirect, costing one extra
  round trip per click.
- Every page had a render-blocking htmx `<script>` from unpkg.com. Config also had a
  blocking Leaflet stylesheet from unpkg. With no internet, both would stall the page.

The Jetson's server is not the bottleneck. On nano-super, the old code served every page
in 3 to 30 ms with both kinds of client attached.

## 3. Procedure

1. Measured HCRO page loads with curl, splitting time into TCP connect (network only) and
   TTFB (network plus server). Connect was 200 to 300 ms against a 115 ms ping, which
   means the link was queueing. Even a static file had a TTFB anywhere from 0.3 to 3.7 s.
2. Followed redirects per nav link (`curl -w %{http_code}`): `/config` and `/captures`
   answer 307.
3. Measured what each page sends, on the workstation's mock pipeline:
   - the Live WebSocket: frame rate, raw size, deflated size per field;
   - Dashboard polls: Resource Timing transfer sizes.
4. A/B on nano-super: old code (main at 3cbb5a7) against new code, run from scratch
   copies with a fresh DB each, warmed up to more than 700 windows in the 15-minute range
   so the Dashboard is in aggregated mode. Per variant:
   - server CPU over 45 s from `/proc/<pid>/stat`, plus the WORKER per-chunk timings
     logged in that window;
   - the same with one high-res Live client and one Dashboard tab attached; the clients'
     cost is the difference;
   - page TTFB with the clients attached;
   - Dashboard to Config navigation timing in a real browser.
5. Correctness of the incremental poll: in a browser, paused live mode and compared the
   merged waterfall and stats with a fresh full fetch of the identical range, in raw mode
   and in aggregated mode.

## 4. Evidence

Live WebSocket, workstation mock at 56 Msps, per high-res frame:

```
before: psd 15.9 msg/s, 147.1 KB/msg; field sizes powers 42.6 KB, noise_floor_per_bin 42.5 KB,
        max_powers 42.5 KB, frequencies 29.7 KB; deflated 44 KB/frame = 5.8 Mbit/s
after:  psd 16.7 msg/s, raw 30.6 KB/msg, deflated 3.4 KB/msg, 0.46 Mbit/s
        frames with frequencies: 1/201, with noise floor: 12/201
?psd=0 (heartbeat-only pages): no psd frames
```

nano-super A/B (CPU in % of one core; worker = per-chunk process time):

| | old idle | old + clients | new idle | new + clients |
|---|---|---|---|---|
| server CPU | 244.7% | 259.0% | 242.0% | 245.3% |
| worker total | 119.5 ms | 133.7 ms | 120.3 ms | 121.4 ms |
| Live feed (wire) | | 3.67 Mbit/s, 10.4 fps | | 0.30 Mbit/s, 11.0 fps |
| Dashboard tab | | 5.15 Mbit/s (waterfall 1240 KB/poll) | | 0.08 Mbit/s (waterfall 6.1 KB/poll) |

Dashboard to Config in a browser on the LAN:

```
old: redirects 1, ttfb 46 ms, load 197 ms; leaflet.css from unpkg 114 ms (render-blocking)
new: redirects 0, ttfb 32 ms, load 72 ms; style.css, htmx, theme.js all 304 (300 B)
```

Pages with gzip, new code: `/` 13.5 KB to 2.9 KB, `/live/` 75.7 KB to 18.5 KB,
`style.css` 38 KB to 8.4 KB, `averaged.js` 78 KB to 21 KB. A full waterfall reload went
from 1.23 MB (float32) to 654 KB (int16), and to 52 KB gzipped on mock data.

Incremental poll against a fresh full fetch of the same range:

```
raw:        heldRows 199, fullRows 200, cntDiff 0, maxPsdDiff 0, min/max identical, statsDiffs 0
            (the 1 missing row is a window written after the last poll)
aggregated: heldRows 601, fullRows 601, grid identical, maxPsdDiff 0 on all equal-count rows,
            only difference: the newest bucket gained windows after the last poll
```

## 5. Changes

- **Live frames** (`web/websocket.py`, `dashboard.html`):
  - dB arrays are rounded to 0.1 dB;
  - `frequencies` is sent only when the axis changes;
  - `noise_floor_per_bin` is sent at most once a second;
  - the client keeps the last copies and applies the calibration offset to a copy;
  - `?psd=0` lets the heartbeat-only pages opt out of PSD frames.

  The frame rate is unchanged, as high-res needs every frame.
- **Gzip** (`web/gzip.py`): pages, static files and API bodies are compressed. Downloads,
  Range requests, open-ended streams (the CSV export), errors and small bodies are not.
  Bodies over 64 KB are compressed on a worker thread.
- **Dashboard incremental poll** (`averaged.js`, `database.py`, `api.py`):
  - a live tick fetches only the rows from 10 s before the newest held row, with the
    range's `bucket_sec` forced so the server answers on the same epoch-anchored grid;
  - the client merges that tail into what it holds;
  - it reloads in full on any range, tuning or Refresh action, on an axis change, on the
    raw-to-aggregated switch, and every 5 minutes;
  - `bucket_sec` is bounded to at most max_rows + 1 buckets.

  Waterfall format v3: int16 rows in centi-dB.
- **Navigation**:
  - the nav links point at `/config/` and `/captures/` (no redirect);
  - htmx is vendored at `static/vendor/htmx-2.0.4.min.js` and deferred (it is identical
    to the npm package's `dist/htmx.min.js`, whose integrity hash was verified);
  - Leaflet's stylesheet now loads with its script on first map open, and a failed
    offline load can be retried.

## 6. Measured and REJECTED (do not retry)

- **"The Jetson's web server is slow."** Rejected for this code: on nano-super the old code
  served pages in 3 to 30 ms with a Live client and a Dashboard tab attached.
- **"Cap the Live frame rate."** Rejected by the user: high-res exists to show every frame.

## 7. Measurement traps

- The mock receiver cannot overflow, so on nano-super pipeline load shows up as worker
  time, not as `overflow_events`.
- curl to `/config` reports 0 B because it does not follow the 307. The page-time script
  still uses the old paths, so its `/config` and `/captures` rows time the redirect only.
- Resource Timing `transferSize` is about 300 B for a 304, so a revalidated file looks
  "free" in byte counts, but it still costs one round trip.

## 8. Open, not yet answered

- **The HCRO sensor is losing IQ right now.** Its `/api/health` showed 1,239 overflow
  events and 2,370,466,363 lost samples over 5,900 s of uptime. A 61 s sample showed
  13 events and 1.02 s of IQ lost (1.7%). The cause has not been determined; we may not run
  commands on that box. Hypotheses to check there (`tegrastats`, `top`, the TIMING/PROC
  logs):
  - CPU contention from isolation and attribution (rtl_433), or from the old UI's
    per-poll aggregation and JSON serialization while tabs are open;
  - USB.

  Compare the overflow rate with every browser tab closed against with tabs open, before
  and after deploying this change.
- Each 2 s Dashboard poll still refetches the range's detections list, up to 200 rows
  (12 KB gzipped). That is now the largest part of a poll.
- The connect time at HCRO (200 to 300 ms against a 115 ms ping) was measured while
  someone's browser tabs on the old code may have been loading the link. It should be
  re-measured after deploying.
