# "Load failed" on a custom 24 h Dashboard range (field sensor)

Date: 2026-09-17. **Open: the field cause is not proven.** This records what was
ruled out, and the diagnostics added so the next occurrence names its own cause.

## The question

On a deployed sensor, selecting a custom (absolute From/To) 24 h range on the
Dashboard shows "Load failed". Reported timing: the spinner ran **under about
30 s** before the message. The box was running **bfbac90** at the time (from
its `git pull` output, `Updating bfbac90..a788dbf`), so it had branches A to D
but not the later Dashboard fixes.

Host: `rf-nano-002@rfnano`, `~/github/RFObserver`. It is not reachable from this
workstation, and the user asked that nothing be run on it.

## What "Load failed" means

`averaged.js` `loadAll()` sets it only when one of the four range requests
**rejects**, that is a network-level failure, not an HTTP error status:

- `/api/averaged/waterfall`, `/api/averaged/stats`, `/api/detections.json`,
  `/api/iq-captures` are awaited together with `Promise.all`, so any one of
  them failing produces the same message.
- A non-2xx waterfall response says "Waterfall load failed (<status>)" instead.
- A load superseded by a newer one returns silently (the `loadSeq` guard), so
  the message means the *current* load's request died.

Before this change the message carried nothing else: no endpoint, no reason, no
timing, and no indication whether the server was still there.

## Measured and REJECTED (do not retry)

- **"It is the `.177` LAN sensor."** Rejected: `.177` runs an Aug 25 build
  (a35a079, 93 commits behind) that has no Dashboard page at all, and no
  "Load failed" string anywhere. It had also been up 58 days with
  `NRestarts=0`, no OOM kills and no watchdog lines.
- **"The browser gives up on a long request."** Rejected: a request left
  pending for 400 s was still waiting (Chrome did not time it out). This also
  rules out the "queued behind another heavy query" theory for a failure that
  appears in under 30 s, since the heavy-query cap (branch B) makes waits
  longer, not shorter.
- **"A field-size 24 h custom range fails by itself."** Not reproduced. With
  75,767 seeded windows over 24 h (the field scale from the 2026-09-14
  validation), on bfbac90 with the pipeline active and the watchdog enabled,
  the four requests returned 200 every time: waterfall 3.3 to 3.9 s, stats
  2.1 s, the other two 0.1 s. Pinning the server to one core did not change
  that.

## Still standing (not yet distinguished)

All fit "fails in under 30 s with no HTTP status":

1. **The process died under the request** and systemd restarted it: a watchdog
   escalation (exit 90), the supervisor give-up exit (91), or an OOM kill.
   Note 30 s is also `WATCHDOG_TIMEOUT_SEC`.
2. **A network drop** between browser and sensor (for example WiFi).
3. **Something else closing the connection** in front of the app.

The sensor's own journal would separate 1 from 2 and 3 immediately, but that
box is off limits for this investigation.

## What was added (diagnostics only, no retry)

- `/api/health` now reports `uptime_sec`.
- A failed load now reports which request died, why, after how long, and what
  the server looks like immediately afterwards, from a single health probe:

```
Load failed: waterfall (Failed to fetch) after 27s - server restarted during the load (up 2s)
Load failed: waterfall (Failed to fetch) after 3s  - server unreachable
Load failed: stats (Failed to fetch) after 12s     - server up 6.4 h
```

- "restarted during the load" is only claimed when the server's uptime is
  shorter than the load that just failed, which is the signature of cause 1.
- In Now mode the same detail is appended to "Update failed ... - retrying".
- No automatic retry of the failed load was added, as the user asked.

## Measurement traps

- **The stalling-request test measured CORS, not a timeout.** The stall
  endpoint ran on a second port, so the response was cross-origin and rejected
  as "TypeError: Failed to fetch" exactly when it arrived at 400 s. The useful
  half of the result still holds: the browser waited the full 400 s.
- **A fresh server looks like a restarted one.** An early version reported
  "server restarted 33s ago" for any failure within 120 s of boot. Comparing
  uptime against the failed load's duration is the accurate test.
- **`ubuntu@192.168.97.177` is the hostname, not the login.** The login is
  `ocollaco`.
- **Local hardware is far faster than the sensor.** Two cores of this
  workstation still finished the 24 h aggregate in under 4 s; `CPUQuota` on a
  user unit did not bite either. Do not read "it did not reproduce locally" as
  "it cannot happen on the box".

## Open, not yet answered

- The actual field cause, pending either the box's journal around a failure or
  the new message from a recurrence.
- Whether that sensor has `WATCHDOG_ENABLED=true`, how much RAM it has, and
  what its 24 h window count is. All three bear on cause 1.
- Whether the failure also happens on the "Last 24 hours" preset, or only on an
  absolute custom range.
