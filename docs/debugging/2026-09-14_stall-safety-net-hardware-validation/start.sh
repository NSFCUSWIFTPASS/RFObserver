#!/bin/bash
# Launch the stall-test pipeline as a transient systemd unit mirroring the
# production unit (Type=simple, Restart=on-failure, RestartSec=5).
# Usage: start.sh [extra --setenv=K=V ...]
set -euo pipefail
T=/home/ocollaco/rfobs-stalltest
mkdir -p "$T/data" "$T/ctl"
sudo systemctl reset-failed rfobs-stalltest 2>/dev/null || true
sudo systemd-run --unit=rfobs-stalltest --uid=ocollaco --gid=ocollaco \
  -p Restart=on-failure -p RestartSec=5 \
  -p WorkingDirectory=/home/ocollaco/rfobs-stall \
  --setenv=HOME=/home/ocollaco \
  --setenv=PYTHONPATH=/home/ocollaco/rfobs-stall/src \
  --setenv=PYTHONUNBUFFERED=1 \
  --setenv=STALLTEST_CTL=$T/ctl \
  --setenv=RFOBS_SENSOR_ACTIVE=true \
  --setenv=RFOBS_WATCHDOG_ENABLED=true \
  --setenv=RFOBS_WEB_PORT=8888 \
  --setenv=RFOBS_FREQUENCY_START=915000000 \
  --setenv=RFOBS_FREQUENCY_END=915000000 \
  --setenv=RFOBS_STORAGE_PATH=$T/data \
  --setenv=RFOBS_DB_PATH=$T/data/rfobserver.db \
  "$@" \
  /home/ocollaco/GitHub/RFObserver/.venv/bin/python $T/harness.py
