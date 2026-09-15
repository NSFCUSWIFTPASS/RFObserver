"""Measure UHD overflow loss via per-packet time_spec on a B200mini.

Streams continuously at RATE into CHUNK-sample buffers like recv_chunk, and
stalls the consumer STALL_MS every STALL_EVERY chunks to force overflows.
For every recv() that returns samples, compares the packet's time_spec with
where the previous samples ended; a positive difference is lost samples.
Checks: received + gap_samples == device-time span * RATE.
"""
import sys, time
import numpy as np
import uhd

RATE = float(sys.argv[1]) if len(sys.argv) > 1 else 56e6
SECS = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
STALL_MS = float(sys.argv[3]) if len(sys.argv) > 3 else 60.0
STALL_EVERY = int(sys.argv[4]) if len(sys.argv) > 4 else 10
CHUNK = 2_048_000

usrp = uhd.usrp.MultiUSRP("num_recv_frames=1024")
usrp.set_rx_rate(RATE, 0); usrp.set_rx_gain(30, 0); usrp.set_rx_antenna("RX2", 0)
usrp.set_rx_freq(uhd.libpyuhd.types.tune_request(915e6), 0)
usrp.set_time_now(uhd.types.TimeSpec(time.time()))  # production: host epoch
st = uhd.usrp.StreamArgs("sc16", "sc16"); st.channels = [0]
rx = usrp.get_rx_stream(st)
md = uhd.types.RXMetadata()
buf = np.zeros(CHUNK, dtype=np.int32)
cmd = uhd.types.StreamCMD(uhd.types.StreamMode.start_cont); cmd.stream_now = True
rx.issue_stream_cmd(cmd)

float_err = 0; received = 0; gaps = 0; gap_events = 0; overflows = 0; no_ts = 0
first_t = None; next_t = None; chunks = 0
w0 = time.monotonic()
while time.monotonic() - w0 < SECS:
    total = 0
    while total < CHUNK:
        n = rx.recv(buf[total:], md, timeout=1.0)
        if md.error_code == uhd.types.RXMetadataErrorCode.overflow:
            overflows += 1
        elif md.error_code != uhd.types.RXMetadataErrorCode.none:
            print("error", md.strerror()); break
        if n > 0:
            if not md.has_time_spec:
                no_ts += 1
            else:
                t = md.time_spec.to_ticks(RATE); tr = md.time_spec.get_real_secs()
                if first_t is None:
                    first_t = t
                elif next_t is not None:
                    d = t - next_t; dr = round((tr - next_tr) * RATE); float_err = max(float_err, abs(dr - d))
                    if d != 0:
                        gaps += d; gap_events += 1
                next_t = t + n; next_tr = tr + n / RATE
            total += n; received += n
    chunks += 1
    if STALL_EVERY and chunks % STALL_EVERY == 0:
        time.sleep(STALL_MS / 1000)
wall = time.monotonic() - w0
cmd = uhd.types.StreamCMD(uhd.types.StreamMode.stop_cont); cmd.stream_now = True
rx.issue_stream_cmd(cmd)
span = next_t - first_t
print(f"rate={RATE/1e6:.1f}MS/s wall={wall:.2f}s chunks={chunks} overflows={overflows} "
      f"gap_events={gap_events} gap_samples={gaps} received={received} no_ts={no_ts} max_float_err_samples={float_err}")
print(f"device_span_samples={span} received+gaps={received+gaps} mismatch={span-(received+gaps)} "
      f"loss={gaps/span*100:.2f}% wall_expected={wall*RATE:.0f}")
