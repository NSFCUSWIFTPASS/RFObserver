#!/bin/bash
# A/B the PSD worker count: for each W, run the live pipeline, warm up 20 s,
# then measure 90 s of processing drops (TIMING dropped= delta over recv#
# delta) and UHD overflows. Prints one summary row per W.
T=/home/ocollaco/rfobs-stalltest
for W in "$@"; do
  sudo systemctl stop rfobs-stalltest 2>/dev/null
  "$T/start.sh" --setenv=STALLTEST_PROC_WORKERS="$W" >/dev/null
  sleep 20
  since=$(date '+%Y-%m-%d %H:%M:%S')
  sleep 90
  until_=$(date '+%Y-%m-%d %H:%M:%S')
  j=$(journalctl -u rfobs-stalltest --no-pager -o cat --since "$since" --until "$until_")
  first=$(echo "$j" | grep -m1 'TIMING recv#')
  last=$(echo "$j" | grep 'TIMING recv#' | tail -1)
  r0=$(echo "$first" | sed -E 's/.*recv#([0-9]+).*/\1/'); d0=$(echo "$first" | sed -E 's/.*dropped=([0-9]+).*/\1/')
  r1=$(echo "$last" | sed -E 's/.*recv#([0-9]+).*/\1/'); d1=$(echo "$last" | sed -E 's/.*dropped=([0-9]+).*/\1/')
  ovf=$(echo "$j" | grep -c 'UHD overflow')
  lat=$(echo "$j" | grep 'PROC chunk#' | sed -E 's/.*latency=([0-9.]+)ms.*/\1/' | sort -n | awk '{a[NR]=$1} END{print a[int(NR/2)+1]}')
  pid=$(systemctl show rfobs-stalltest -p MainPID --value)
  cpu=$(top -b -n1 -p "$pid" | tail -1 | awk '{print $9}')
  echo "workers=$W chunks=$((r1-r0)) dropped=$((d1-d0)) drop_pct=$(awk -v a=$((d1-d0)) -v b=$((r1-r0)) 'BEGIN{printf "%.1f", 100*a/b}') uhd_overflows_90s=$ovf median_latency_ms=$lat cpu_pct=$cpu restarts=$(systemctl show rfobs-stalltest -p NRestarts --value)"
done
sudo systemctl stop rfobs-stalltest
