# NNE60 (.31): "writer timeout" capture whose file kept growing after its metadata

## The question

2026-11-06. NNE60, hcro-jet-001, 10.1.42.31, RFObserver 0.14.0b0 (restarted at about 21:12 UTC;
the error is from the 0.13 run before it). `/api/health` showed `status: degraded` with:

```
last_write_error: 3227508-hcro-jet-001-20261106T030046.sc16: writer timeout: the file writer did
not finish within 10 s; the .sc16 may be incomplete or keep growing after this metadata was written
degraded_since: 2026-10-06T21:40:54Z
```

"What could have caused this error ... it needs to be handled better in case the writer can
continuously keep writing."

## The answer

The capture's disk writer needed more than 10 s to write about 14 MB, so finalize abandoned it at
the fixed 10 s limit and wrote the metadata from the file as it was then (10,400,000 bytes). The
abandoned writer then finished its queue: the file is 14,496,000 bytes, and nothing ever corrected
the `.json`, the database row or the archive index, which kept saying "failed, 10.4 MB". The
growth is bounded (the writer only drains what was already queued for that capture, at most 64
items), but it was invisible. Why the disk stalled for over 10 s is NOT determined (no log access
on .31).

Fixed: finalize waits while the writer makes progress and abandons it only when one write or close
has been blocked 10 s, or after 60 s in all; a writer that is abandoned and later exits cleanly
gets its capture rewritten from disk (marked `finished_late`, no longer failed); any write or close
blocked for 1 s or more is logged with its size and duration.

## The procedure

1. **Read-only `GET /captures/detail/<file>` on .31.** Compares the metadata with the file on
   disk; isolates "the writer kept going" from "the file is short".
2. **Read the finalize code** (`pipeline/streaming.py`, `_finalize_recording`,
   `_file_writer_loop`, `_note_abandoned_writer_exit`). Establishes what bounds the growth (the
   recording's own queue, maxsize 64; a superseded writer exits when that queue is empty) and what
   happens at the writer's exit (a log line only).
3. **`/api/collection/hourly` for 23:00-07:00 UTC.** Puts the 03:00 hour in context of the disk's
   load (controls for "an unusual burst at 03:00").
4. **Newest 1,500 captures from `/captures/list`.** Frequency check for `write_failed`.
5. **Governor code** (`storage/governor.py`). Explains why "degraded" survives a restart.
6. SSH to .31 for syslog/kernel lines around 03:00:46 UTC: no working user (tried rf-nano-aa,
   rf-nano-bb, rf-nano, rf-nano-cc; all refused). Stopped guessing.

## Evidence

The capture (`GET /captures/detail/3227508-hcro-jet-001-20261106T030046.sc16`):

```
size 14496000
start_time 2026-11-06T03:00:46.552946Z  duration_sec 0.1  total_bytes 10400000  total_samples 2600000
sample_rate_hz 26e6  trigger_initiated True  stopped_reason trigger_end  priority event
write_failed True  dropped_chunks 0  overflow_events 0
write_error: writer timeout: the file writer did not finish within 10 s; ...
```

Disk load on .31 (`/api/collection/hourly`, UTC hours):

```
11-06T02:00 auto 490  7.57 GB  evicted at cap 488  7.55 GB
11-06T03:00 auto 486  7.66 GB  evicted at cap 452  7.67 GB
11-06T04:00 auto 731 12.23 GB  evicted at cap 764 12.22 GB
11-06T05:00 auto 856 14.01 GB  evicted at cap 867 14.01 GB
```

Health: `free_gb 213.2, auto_gb 600.0, priority_gb 300.3, db_gb 25.1`; the other sensors have
about 820 GB free. Newest 1,500 captures (19:30-22:00 UTC, of 27,875): 0 with `write_failed`.

## Measured and REJECTED (do not retry)

- **"The abandoned writer can keep writing forever."** It cannot: it writes only what is in that
  recording's queue (maxsize 64), and once a newer recording starts it exits when the queue is
  empty. Here it added 4,096,000 bytes (about 0.04 s of IQ).
- **"Something unusual happened at 03:00."** That hour's writes and evictions are ordinary for .31
  (450-870 captures and 7-14 GB an hour, around the clock).
- **"Health is degraded now."** `degraded` is a sticky acknowledgement flag: it stays until "Clear
  Warning" on the Live page (`POST /api/storage/clear-degraded`). Set since 2026-10-06 on .31; later
  errors only replaced the message. Storage step was "healthy".

## Measurement traps hit

- The 10 s in the message was the whole wait, not a stall: a writer that was slowly but steadily
  draining its queue got the same error as a hung volume.
- Paging all 27,875 captures to count failures would read every `.json` on the busiest disk in the
  fleet; I stopped at the newest 1,500.

## Open, not yet answered

- What blocked the writer on .31 at about 03:00:46-03:00:56 UTC. Candidates: ext4 journal
  commits while the cap evicts captures continuously, or the 25 GB SQLite database's checkpoints on
  the same volume. The new "Recording writer: ... blocked" log line names the step next time;
  syslog/kernel lines from .31 would also help.
- The collection statistics keep the bytes counted at finalize for a late-finished capture.
