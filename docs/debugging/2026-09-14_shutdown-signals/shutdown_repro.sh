#!/bin/bash
# Reproduce RFObserver shutdown behaviour with the mock receiver (no SDR).
# Usage: shutdown_repro.sh <SIGNAL> <WEB_PORT> [python]
#   WEB_PORT=0 runs headless. python defaults to the repo .venv (3.11); pass a
#   3.10 interpreter with the repo installed to reproduce the Jetson behaviour.
# Starts `rfobserver run` (mock, sensor active) through probe_stop.py, waits
# 15 s, sends SIGNAL, and measures time to exit (cap 40 s). If still alive after
# 8 s it asks faulthandler for all thread stacks (SIGUSR1), then SIGKILLs.
# Output goes to $OUT/shutdown-<SIG>-<PORT>-<venv>/log.txt.
SIG=$1; PORT=$2; PY=${3:-$(git rev-parse --show-toplevel)/.venv/bin/python}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-/tmp/rfobs-shutdown}
S=$OUT/shutdown-$SIG-$PORT-$(basename "$(dirname "$(dirname "$PY")")")
rm -rf "$S"; mkdir -p "$S"; cd "$S" || exit 1
PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_SENSOR_ACTIVE=true RFOBS_WEB_PORT=$PORT \
  RFOBS_STORAGE_PATH="$S/data" RFOBS_DB_PATH="$S/data/db.sqlite" \
  "$PY" "$HERE/probe_stop.py" > "$S/log.txt" 2>&1 &
PID=$!
sleep 15
echo "sending $SIG to $PID"
T0=$(date +%s.%N)
kill -s "$SIG" $PID
for i in $(seq 1 80); do
  if ! kill -0 $PID 2>/dev/null; then break; fi
  if [ "$i" -eq 16 ]; then
    echo "--- still alive after 8 s: thread dump ---"
    kill -USR1 $PID; sleep 1
  fi
  sleep 0.5
done
if kill -0 $PID 2>/dev/null; then echo "still alive at 40 s: SIGKILL"; kill -9 $PID; fi
wait $PID 2>/dev/null; RC=$?
echo "exit rc=$RC after $(echo "$(date +%s.%N) - $T0" | bc) s"
grep -E -i 'deactivated|finished server|Traceback|KeyboardInterrupt|PROBE' "$S/log.txt" | tail -15
