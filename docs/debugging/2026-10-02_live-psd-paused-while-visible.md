# Live page: PSD stream stops while the page is in view

## 1. The question

Date: 2026-10-02. HCRO sensor (rfnano, v0.10.0b0, 8179f33 deployed at about 05:29), Live
page in High Res over the VPN. Reported: the Live page "still hangs occasionally"; then,
from the sensor journal, "There's a point where the [PSD] stream resumes". The user also
confirmed that pausing PSD while the tab is not focused is intended.

## 2. The answer

Partly answered. The freezes match the page telling the server `wants_psd=False` while
the user was watching. With that flag the server sends heartbeats only, so the socket
watchdog stays satisfied and the charts stop. The stream resumes when the page next sends
`True`. Why the page's view state was wrong is not determined. The page now repairs it,
and the server logs enough to explain the next occurrence.

## 3. Procedure

1. Read the journal's WebSocket lines in order. Each `Client set ...` line is one control
   message from a page, so the sequence shows what the page believed.
2. Compared that sequence with the page's rules (`dashboard.html`):
   - `setPsdVisible` only sends on a change;
   - `onopen` sends the current view;
   - `wants_psd` is true when the tab is visible and an IntersectionObserver reports the
     charts on screen.
3. Reproduced a lost "resume" message on the mock pipeline. In headless Chrome:
   - forced `visibilityState` to hidden, then visible;
   - wrapped `WebSocket.prototype.send` to drop the first `set_view` carrying `true`;
   - counted PSD frames before and after.

## 4. Evidence

Journal (pid 579437, after the deploy):

```
05:29:56 WebSocket /ws/live [accepted]          (page reconnects after the restart)
05:29:56 Client set high_res=True
05:29:56 Client set wants_psd=False
05:30:02 Client set wants_psd=True              (stream resumes)
05:31:56 RECV STALL: recv=117.5ms ... recording=armed   -> UHD overflow
05:31:56 WebSocket /ws/live [accepted]          (another new socket, no page load)
05:31:56 Client set high_res=True
05:31:56 Client set wants_psd=False
05:32:01 Client set wants_psd=False             (a second False with no True between)
```

The page sends only on a change, so the second `False` means a `True` was either never
sent or sent on a socket that the server never logged. uvicorn logged no close for any of
these sockets, so it is unknown why the 05:31:56 socket was opened.

Mock reproduction, frames counted in the page:

```
visible 2 s: 34 psd   hidden 2 s: 0   visible, True dropped: 23 in 2 s, 65 in the next 4 s
server: wants_psd=False (hidden=True in_view=True why=visibility)
        wants_psd=True  (hidden=False in_view=True why=no-psd)    3.1 s later
```

## 5. Changes

- **Page:**
  - If the charts should stream but no PSD frame arrives for 3 s, the page resends its
    view. Repeats back off to once a minute.
  - On becoming visible, the page measures whether the charts are on screen instead of
    trusting the observer's last answer.
  - Each new socket opens with `hello {reason: load | watchdog | closed, silent_ms}`.
  - `set_view` carries `hidden`, `in_view` and `why`.
- **Server:** logs
  - the hello;
  - each view change, with its inputs;
  - each close, with seconds open, frames sent and frames dropped;
  - a rate-limited warning when a client's queue overflows or one send blocks 1 s or more.

## 6. Measured and REJECTED (do not retry)

- **"The sensor stops sending."** Two raw sockets and a headless Live page ran for 8 and
  4 minutes from the workstation with no gap over 0.44 s (see
  2026-10-01_ui-lag-over-vpn.md section 9).
- **"Pausing while unfocused is the bug."** No: the user wants it.

## 7. Measurement traps

- The socket watchdog cannot see this failure: heartbeats keep flowing while PSD is off.
- `Client set wants_psd` lines did not name the client, so two tabs from one address
  could not be told apart. They do now.

## 8. Open, not yet answered

- Why the page held `wants_psd=False` while in view; candidates are a missed
  visibilitychange or IntersectionObserver callback, or a second Live tab.
- Why the 05:31:56 socket was opened (the watchdog or a close), and the 117 ms GIL stall
  at the same instant. The next occurrence's `Live client ... connected: reason=` and
  `closed after` lines answer the first.

## 9. Freeze at 06:46 to 06:48 with the new logging (2026-10-02)

Journal (pid 580085, 9897c77), the Live lines:

```
06:47:00 Live client :50563 closed after 60 s: sent=60 dropped=0     (heartbeats only)
06:47:01 Live client :50614 connected: reason=watchdog silent_ms=16777
06:47:01   set wants_psd=True (hidden=False in_view=True why=open)
06:47:02   set wants_psd=False (hidden=True ...)  closed after 2 s        (the user reloads)
06:47:02 GET /live/
06:47:03 Live client :50625 connected: reason=load
06:48:00 Live client :50668 connected: reason=watchdog silent_ms=5998
06:48:00   set wants_psd=False (hidden=True in_view=True why=open)
06:48:01 Client :50625 set wants_psd=True (why=no-psd); closed after 58 s: sent=1530 dropped=0
```

What it shows:
- **The server never stopped.** :50625 was fed 1530 frames in 58 s (26/s) with nothing
  dropped, and its replacement was opened while it still worked.
- **The page stopped running.** The watchdog checks every 1 s and fires after 5 s of
  silence, so `silent_ms=16777` means its own timer also did not run for about 11 s.
  Neither messages nor timers were processed: a long task in the page, or the browser
  pausing the tab. The tab was visible at the reconnect (hidden=False).
- **The queued work replayed out of order.** At 06:48:01 the old socket's `no-psd` resend
  arrived after the new socket's hello. Both timer ticks ran back to back once the page
  resumed.

**Rejected (do not retry):** unbounded client state. The burst and label maps are pruned
against the oldest waterfall row on the same epoch-ms clock (isolation.py and
streaming.py both use `timestamp() * 1000`). Attributions are a deque of 50, and
`_active_bursts` is replaced on each detection pass.

**Changes:**
- The page's 1 s timer measures its own lateness. Over 2 s late, it sends
  `freeze {late_ms, hidden, longest_task_ms}`, which the server logs as a warning, and
  resets the watchdog instead of reconnecting.
- Verified on the mock: a forced 7 s busy loop gave
  `page frozen 6260 ms (hidden=False, longest task 7000 ms)` with no reconnect.

**Open:** what freezes the page on the reporting browser. The next freeze report answers
whether it is the page's own code (`longest task` about equal to the freeze) or the
browser pausing the tab (a large freeze with no long task).

## 10. Stall at 08:09 with the freeze report deployed (8dcde72)

```
08:09:05 Live client :53288 connected: reason=watchdog silent_ms=6000
08:09:05   set wants_psd=False (hidden=True in_view=True why=open)
08:09:12 Client :53202 set wants_psd=True (hidden=False in_view=True why=no-psd)
08:09:12 Client :53202 set wants_psd=False (hidden=True in_view=True why=visibility)
08:09:12 Live client :53202 closed after 67 s: sent=1009 dropped=0
(no "page frozen" line)
```

What it shows:
- **Not a page freeze.** The page's 1 s timer ran on time, or it would have sent a freeze
  report.
- **Not the server.** It handed the old socket every frame (`dropped=0`).
- **The old TCP connection stalled in both directions.** The page saw 6 s with no
  message, and messages the page sent on that socket earlier reached the server 7 s
  later, after the replacement socket (opened at once) had already logged in. That is a
  single connection stalled on the network path, typically TCP retransmission backoff
  after packet loss, not the sensor.
- **The watchdog recovered it** in 6 s with a new connection.

Control: 300 pings from the workstation to the sensor over the same VPN in 60 s gave 0%
loss, rtt 82/125/299 ms. The reporting browser's own path (10.1.0.130) was not measured
and may differ.

**Superseded:** section 9's reading that freezes are the page not running holds for the
06:47 event (`silent_ms=16777`, timer late). The 08:09 event is a different failure: the
network connection, with the page running.
