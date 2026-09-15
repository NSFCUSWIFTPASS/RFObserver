#!/bin/bash
# Every 30 s: auto/ capture count and size (eviction), NRestarts, main PID,
# CPU%, RSS. Appends CSV to $T/sampler.csv until killed.
T=/home/ocollaco/rfobs-stalltest
echo "time,auto_files,auto_mb,manual_files,nrestarts,pid,cpu_pct,rss_mb,disk_free_gb" >> "$T/sampler.csv"
while true; do
  pid=$(systemctl show rfobs-stalltest -p MainPID --value)
  nr=$(systemctl show rfobs-stalltest -p NRestarts --value)
  af=$(ls "$T/data/auto"/*.sc16 2>/dev/null | wc -l)
  amb=$(du -sm "$T/data/auto" 2>/dev/null | cut -f1)
  mf=$(ls "$T/data/manual"/*.sc16 2>/dev/null | wc -l)
  if [ "$pid" != "0" ]; then
    read -r cpu rss <<<"$(ps -o %cpu=,rss= -p "$pid" 2>/dev/null)"
  else
    cpu=0; rss=0
  fi
  free=$(df -BG --output=avail "$T" | tail -1 | tr -dc 0-9)
  echo "$(date +%H:%M:%S),$af,$amb,$mf,$nr,$pid,$cpu,$((${rss:-0}/1024)),$free" >> "$T/sampler.csv"
  sleep 30
done
