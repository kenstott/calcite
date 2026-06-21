#!/usr/bin/env bash
# Monitor the live DQ run for Iceberg writes and downloads.
# Samples the per-worker launch.logs every INTERVAL seconds and prints, per schema,
# new Iceberg chunk-writes and download events since the previous sample, so we can
# spot reprocessing (writes to already-materialized partitions) and re-downloads of
# cached data. Writes a rolling summary to SUMMARY; append-only event log to EVENTS.
set -uo pipefail

RUNS_DIR="/mnt/c/Users/Admin/calcite/govdata/scripts/parallel/runs"
SUMMARY="/home/adminwsl/dq-monitor-summary.txt"
EVENTS="/home/adminwsl/dq-monitor-events.txt"
INTERVAL="${1:-60}"
STATE="$(mktemp -d)"
trap 'rm -rf "$STATE"' EXIT

: > "$EVENTS"

while true; do
  ts="$(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "================ $ts ================"
    printf "%-18s %8s %8s %8s %8s %8s %8s\n" "schema" "icebWr" "rows" "dnload" "cacheHit" "catGate" "404/unav"
  } > "$SUMMARY.tmp"

  for d in "$RUNS_DIR"/worker-*-dq-etl-resume; do
    [ -d "$d" ] || continue
    log="$d/launch.log"
    [ -f "$log" ] || continue
    schema="$(basename "$d" | sed -E 's/^worker-(.*)-dq-etl-resume$/\1/')"

    iceb=$(grep -c "Writing Iceberg chunk" "$log" 2>/dev/null | head -1)
    rows=$(grep -oE "Writing Iceberg chunk[^:]*: [0-9]+ rows" "$log" 2>/dev/null \
            | grep -oE "[0-9]+ rows" | grep -oE "[0-9]+" | awk '{s+=$1} END{print s+0}')
    dnl=$(grep -cE "Downloading |Download attempt 1/" "$log" 2>/dev/null | head -1)
    hit=$(grep -cE "ache hit|cache fresh|reusing without refetch|already converted|already materialized" "$log" 2>/dev/null | head -1)
    gate=$(grep -c "catalog gate" "$log" 2>/dev/null | head -1)
    unav=$(grep -cE "404|unavailable|not published|not available" "$log" 2>/dev/null | head -1)

    printf "%-18s %8s %8s %8s %8s %8s %8s\n" \
      "$schema" "$iceb" "$rows" "$dnl" "$hit" "$gate" "$unav" >> "$SUMMARY.tmp"

    # delta detection: report newly-appeared Iceberg writes and downloads since last sample
    prev_iceb=$(cat "$STATE/$schema.iceb" 2>/dev/null || echo 0)
    prev_dnl=$(cat "$STATE/$schema.dnl" 2>/dev/null || echo 0)
    if [ "$iceb" -gt "$prev_iceb" ] 2>/dev/null; then
      grep "Writing Iceberg chunk" "$log" 2>/dev/null | tail -n $((iceb - prev_iceb)) \
        | sed "s/^/[$ts][$schema][WRITE] /" >> "$EVENTS"
    fi
    if [ "$dnl" -gt "$prev_dnl" ] 2>/dev/null; then
      grep -E "Downloading " "$log" 2>/dev/null | tail -n 5 \
        | sed "s/^/[$ts][$schema][DNLOAD] /" >> "$EVENTS"
    fi
    echo "$iceb" > "$STATE/$schema.iceb"
    echo "$dnl"  > "$STATE/$schema.dnl"
  done

  status=$(grep -E "Running:|Done:" /home/adminwsl/dq-etl-resume.log 2>/dev/null | tail -1)
  echo "$status" >> "$SUMMARY.tmp"
  mv "$SUMMARY.tmp" "$SUMMARY"

  # stop when the pool is gone
  if ! pgrep -f "run-pool.sh dq-etl-resume" >/dev/null 2>&1; then
    echo "[$ts] pool no longer running — monitor exiting" >> "$EVENTS"
    break
  fi
  sleep "$INTERVAL"
done
