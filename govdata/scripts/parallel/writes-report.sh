#!/usr/bin/env bash
# Report Iceberg writes (and a row total) per schema for the current --etl-resume run.
set -uo pipefail
RUNS=/mnt/c/Users/Admin/calcite/govdata/scripts/parallel/runs
printf "%-18s %8s %12s\n" "schema" "icebChunks" "rowsWritten"
for d in "$RUNS"/worker-*-dq-etl-resume; do
  [ -d "$d" ] || continue
  log="$d/launch.log"
  [ -f "$log" ] || continue
  schema=$(basename "$d" | sed -E 's/^worker-(.*)-dq-etl-resume$/\1/')
  chunks=$(grep -c "Writing Iceberg chunk" "$log" 2>/dev/null || echo 0)
  rows=$(grep -oE "Writing Iceberg chunk[^:]*: [0-9]+ rows" "$log" 2>/dev/null \
          | grep -oE "[0-9]+ rows" | grep -oE "[0-9]+" | awk '{s+=$1} END{print s+0}')
  if [ "$chunks" -gt 0 ]; then
    printf "%-18s %8s %12s\n" "$schema" "$chunks" "$rows"
  fi
done | sort -k2 -rn
