# Fleet mode: one UI for every sensor

Date: 2026-10-13. Status: design, not started.

## 1. Goal

One place to watch, and later control, every RFObserver sensor (six at HCRO).

- **Fleet Dashboard:** power and kurtosis over time for all sensors on shared
  charts, one colour per sensor, lines plus points. A sensor selector in the
  top bar picks which sensor the waterfall and spectrum show, and its name is
  drawn on both.
- **Fleet Live:** connects to every sensor. One spectrum and waterfall per
  sensor, and one shared power plot (all sensors, same colours as the
  Dashboard). No kurtosis for now.
- **Settings:** the sensor list (found by mDNS, plus IPs added by hand), with
  each sensor's label and colour, and later its configuration.

## 2. Decisions

1. **Same repository, a new process mode.** `rfobserver fleet` starts a web-only
   FastAPI app with its own routes and templates. It reuses the existing chart
   code (waterfall, PSD, crosshair and hover, range picker, binary decoders,
   theme). It never loads the capture pipeline, and sensors never load fleet
   code. One wheel, a new entry point and a `rfobserver.fleet` package.
   - Why: the fleet's only data source is the sensor API. Keeping both in one
     repository means an API change and its fleet consumer ship in one commit,
     under one test suite. Sensors already run mixed versions (.31 on 0.10.0b0).
2. **No rf-db dependency.** rf-db and its schema belong to the HCRO Grafana
   setup. Everything the fleet shows comes from the sensors' own APIs, so fleet
   mode works at any site with nothing but sensors on a network.
3. **The fleet server talks to the sensors; browsers talk only to the fleet
   server.**
   - One origin: no CORS on the sensors.
   - One place for credentials once sensor writes need a token (section 8).
   - Caching and a per-sensor request limit: field sensors are sensitive to
     load (the GIL starvation that caused the HCRO overflows,
     docs/debugging/2026-10-01_hcro-overflows-storage-scans.md).
   - Browsers do not need to reach the sensor subnet.
4. **Discovery:** mDNS on the shared subnet, plus a manual IP list.

## 3. Architecture

```
browser ──HTTP/WS──> fleet server ──HTTP──> sensor /api/...
                     (rfobserver fleet)  └─WS──> sensor /ws/live (one per sensor)
                       ├─ registry   (mDNS + manual list, fleet.json)
                       ├─ sensor clients (httpx, per-sensor limits, cache)
                       └─ live hub   (fan-out to browsers)
```

### 3.1 Registry

A sensor entry: `id` (the hostname; a replaced SDR keeps the same entry), `address`,
`port`, `label`, `colour`, `source` (`mdns` or `manual`), `enabled`, plus
what the last identity poll returned and when.

- **mDNS:** the fleet browses `_rfobserver._tcp.local.`. Entries found there are
  updated as they come and go. A sensor that drops off mDNS is kept, marked
  offline, not deleted.
- **Manual:** `host[:port]` entries added on the Settings page, for sensors
  mDNS cannot see (another subnet, multicast blocked).
- **Merging:** entries are matched by `id` from `/api/identity`, so a sensor
  that is both discovered and added by hand appears once.
- **Stored in** `fleet.json` in the fleet's state directory (same pattern as
  the sensor `.env`: written by the Settings page, read at start).
- **Order and colour:** sensors keep the order they were added, and each keeps
  its colour for good, from a fixed categorical palette in that order. A colour
  follows the sensor, never its position in a filtered list. Six sensors fit
  in one palette. The palette is checked with the colour-vision validator
  before shipping.

### 3.2 Sensor clients

One `httpx.AsyncClient` per sensor.

- **Limits:** at most one waterfall and two other requests in flight per
  sensor. More wait in a queue.
- **Timeouts:** 5 s for identity and stats, 30 s for a waterfall. A sensor
  that does not answer is shown offline with its last-seen time, and the
  other sensors are unaffected.
- **Identity poll** every 30 s: `/api/identity` (new, section 4) and
  `/api/health`.
- **Cache:** stats and waterfall responses are cached per (sensor, query) for
  a short time (10 s for ranges that touch now, 10 min for closed ranges), so
  many browsers, or one page refreshing, cost a sensor one query.

### 3.3 Live hub

The fleet server opens `/ws/live` once per enabled sensor, in the default
(averaged) mode: one PSD frame per `DURATION_SEC`, about 20 KB of JSON at
2048 bins. That is about 120 KB/s for six sensors.

- Each frame is tagged with the sensor id and fanned out to every browser on
  the fleet Live page. A sensor serves one stream however many people watch.
- **Browsers that cannot keep up:** a slow browser gets the newest frame
  per sensor, not a growing queue.
- **Reconnects:** a lost sensor socket is reconnected with backoff, and shown
  as reconnecting in that sensor's tile.
- **Full resolution:** high-resolution mode is not used on the fleet Live page.
  Opening a sensor full-size links to that sensor's own Live page.

### 3.4 Fleet API (browser to fleet server)

| Route | Returns |
|---|---|
| `GET /fleet/api/sensors` | Registry with status (online, version, tuning, last seen) |
| `POST /fleet/api/sensors` | Add a manual sensor |
| `PATCH /fleet/api/sensors/{id}` | Label, colour, enabled |
| `DELETE /fleet/api/sensors/{id}` | Remove a manual entry |
| `GET /fleet/api/stats?since&until&max_points` | Per-sensor stats timelines in one response |
| `GET /fleet/api/sensors/{id}/waterfall?...` | That sensor's `/api/averaged/waterfall`, passed through |
| `WS /fleet/ws/live` | Fan-out of every sensor's live frames, tagged by id |

## 4. Sensor-side changes

1. **`GET /api/identity`:** hostname, sensor name, SDR serial, version,
   latitude and longitude, current tuning (centre frequency, sample rate,
   gain), `DURATION_SEC` and uptime. Cheap, no database access.
2. **mDNS advertisement** of `_rfobserver._tcp.local.` with the web port, and
   TXT records `id`, `hostname`, `version`. Done in-process with
   `python-zeroconf` (pure Python, works in a container and without system
   files). Off with `RFOBS_MDNS_ENABLED=false`.
   - Alternative: a static Avahi service file installed by `deploy/install.sh`.
     No new dependency, but the TXT data cannot follow version changes.
3. Nothing else. Stats, waterfall and live already exist and are used as is.

## 5. Fleet Dashboard

- **Power and kurtosis charts:** one series per sensor, in its colour, drawn as
  a 2 px line plus point markers. Legend always shown and clickable to hide a
  sensor; up to four series also labelled at their line ends. The hover
  tooltip lists every sensor's value at that time.
  - Each series comes from that sensor's `/api/averaged/stats` for the range,
    with the same `max_points` for all, so their buckets line up.
  - **Different tunings:** sensors may be tuned differently. By default each
    series shows the sensor's current tuning, and the tooltip names it.
- **Sensor selector (top bar):** chooses the sensor for the waterfall and the
  spectrum at the selected time. The sensor's label and colour are drawn in
  the corner of both. The choice is kept in the URL, so a link opens the same
  view.
- **Time range, presets, drag-to-zoom and Now** behave as on the sensor
  Dashboard and apply to every chart.

## 6. Fleet Live

- A grid of tiles, one per enabled sensor: spectrum over a short waterfall,
  with the label, colour and status. Two or three columns depending on width.
- One shared power plot across the top: each sensor's `avg_power_db` from its
  live frames, in its colour, over the last few minutes.
- A tile shows its own state: live, reconnecting, offline (with last seen).
- **Load in the browser:** six waterfalls at one row per second are light.
  Rendering reuses the sensor Live page's canvas code.

## 7. Settings

- **Sensor table:** label, colour, address, source (mDNS or manual), version,
  online state, enabled switch, remove (manual entries only).
- **Add sensor:** `host[:port]`. The fleet fetches `/api/identity` before
  saving, and refuses an address that is not an RFObserver sensor.
- **Fleet settings:** default time range, refresh interval.
- Configuring the sensors themselves is phase 3 (section 8).

## 8. Control (phase 3, decide before building)

- **Two sources of truth.** The AWX deploy job rewrites its deploy-managed
  `.env` keys (frequency, gain, rf-db settings and others) on every run. If the
  fleet also edits them, the two fight. Options: the fleet edits only the keys
  AWX does not manage (trigger, priority, display), or AWX stops managing
  survey settings.
- **Authentication.** Sensor write endpoints (`/config/apply`, recording,
  sensor on and off) have none. A fleet control plane makes retuning six
  sensors one click. Add a shared token on sensor write endpoints
  (`RFOBS_API_TOKEN`), which the fleet server holds; reads stay open.

## 9. Deployment

- `rfobserver fleet --port 8080`, as a systemd unit or a container on any host
  on the sensor subnet. Its state directory holds `fleet.json`.
- **mDNS in a container** needs host networking (multicast does not cross the
  default Docker bridge).
- At HCRO, node1 is on the sensor subnet. To confirm: multicast is allowed on
  10.1.42.0/24. If not, the manual list covers it.

## 10. Phases

1. **Fleet Dashboard, read-only:** sensor side (`/api/identity`, mDNS), the
   fleet process, registry with mDNS and manual entries, Settings sensor
   table, multi-sensor power and kurtosis, sensor selector with waterfall and
   spectrum.
2. **Fleet Live:** live hub, tile grid, shared power plot.
3. **Control:** sensor API token, then per-sensor configuration from Settings,
   after the AWX question is settled.

## 11. Testing

- Fleet against several mock sensors (`RFOBS_MOCK_RECEIVER=true` on different
  ports) in one test run: registry merge, an offline sensor, a slow sensor not
  holding up the others, cache hits, live fan-out and reconnect.
- mDNS: advertisement and browsing on loopback; a unit test of the merge by
  `id` without the network.
- Load: the per-sensor request limit holds when several browsers load a long
  range at once.
- UI: screenshots of both pages with six mock sensors, in light and dark.

## 12. Open questions

- The colour palette and whether the per-sensor colour is user-editable or
  fixed by order.
- Whether the fleet should also show detections across sensors (the earlier
  four-sensor analysis matched frames between sensors).
- Where the fleet runs at HCRO, and whether multicast reaches it.
