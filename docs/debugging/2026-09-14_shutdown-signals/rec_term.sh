#!/bin/bash
# rec_term.sh <SIGNAL> <WEB_PORT> [python]: start the mock pipeline, start a
# manual recording, send SIGNAL 4 s later, then list the capture files and the
# shutdown log lines. Shows what a stop does to an in-progress recording.
SIG=$1; PORT=$2; PY=${3:-$(git rev-parse --show-toplevel)/.venv/bin/python}
HERE=$(cd "$(dirname "$0")" && pwd)
D=${OUT:-/tmp/rfobs-shutdown}/rec-$SIG-$PORT
rm -rf "$D"; mkdir -p "$D"
PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_SENSOR_ACTIVE=true RFOBS_WEB_PORT=$PORT \
  RFOBS_STORAGE_PATH="$D/data" RFOBS_DB_PATH="$D/data/db.sqlite" \
  "$PY" "$HERE/probe_stop.py" > "$D/log.txt" 2>&1 &
PID=$!
for i in $(seq 1 40); do curl -s -o /dev/null "localhost:$PORT/api/health" && break; sleep 0.5; done
sleep 3
curl -s -X POST "localhost:$PORT/api/recording/start"; echo
sleep 4
kill -s "$SIG" $PID
for i in $(seq 1 30); do kill -0 $PID 2>/dev/null || break; sleep 0.5; done
kill -0 $PID 2>/dev/null && { echo "alive at 15 s, SIGKILL"; kill -9 $PID; }
wait $PID; echo "rc=$?"
find "$D/data" -type f -not -name 'db.sqlite*' -printf '%P %s\n' | sort
grep -E 'Recording|PROBE|deactivated' "$D/log.txt" | cut -c1-160
