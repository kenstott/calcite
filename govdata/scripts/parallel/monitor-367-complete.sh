#!/bin/bash
# Monitor issue #367 (sec 2023-2025 filing_metadata) to completion
set -e

LOG_DIR="/home/kstott/calcite/govdata/scripts/parallel/runs"
ISSUE_URL="https://github.com/kenstott/govdata-ops/issues/367"

echo "[$(date -u +%H:%M:%S)] Starting monitor for issue #367..."
echo ""

# Monitor until the run completes
while true; do
  # Find the latest worker log
  if [ -d "$LOG_DIR/worker-sec-2023-2023" ]; then
    LATEST_LOG=$(ls -t "$LOG_DIR"/worker-sec-2023-2023/launch_*.log 2>/dev/null | head -1)
    if [ -f "$LATEST_LOG" ]; then
      LAST_LINE=$(tail -1 "$LATEST_LOG" 2>/dev/null)
      LAST_TIME=$(tail -1 "$LATEST_LOG" 2>/dev/null | grep -oE '\[.*\]' | head -1)
      
      # Check for completion markers
      if grep -q "Done: [1-9]" "$LATEST_LOG" 2>/dev/null || \
         grep -q "Prune metadata" "$LATEST_LOG" 2>/dev/null || \
         grep -q "Processing complete" "$LATEST_LOG" 2>/dev/null || \
         grep -q "ERROR\|FAILED" "$LATEST_LOG" 2>/dev/null; then
        echo "[$(date -u +%H:%M:%S)] 2023 phase changed, checking status..."
        tail -20 "$LATEST_LOG"
        echo ""
      fi
    fi
  fi
  
  # Check if all three years have completed
  YEARS_DONE=0
  for Y in 2023 2024 2025; do
    if [ -d "$LOG_DIR/worker-sec-${Y}-${Y}" ]; then
      WORKER_LOG=$(ls -t "$LOG_DIR"/worker-sec-${Y}-${Y}/launch_*.log 2>/dev/null | head -1)
      if [ -f "$WORKER_LOG" ]; then
        # Count "Done:" lines to see if any processing occurred
        if grep -q "Done: [1-9]" "$WORKER_LOG" 2>/dev/null; then
          ((YEARS_DONE++))
        fi
      fi
    fi
  done
  
  # Check if process is still running
  if ! pgrep -f "sih-govdata-355-prune.jar" > /dev/null 2>&1; then
    echo "[$(date -u +%H:%M:%S)] ETL process ended, checking final status..."
    
    # Check all three year logs
    for Y in 2023 2024 2025; do
      if [ -d "$LOG_DIR/worker-sec-${Y}-${Y}" ]; then
        WORKER_LOG=$(ls -t "$LOG_DIR"/worker-sec-${Y}-${Y}/launch_*.log 2>/dev/null | head -1)
        if [ -f "$WORKER_LOG" ]; then
          echo ""
          echo "=== Year $Y ==="
          # Show summary
          tail -5 "$WORKER_LOG"
          
          # Check for errors
          if grep -q "ERROR\|Exception\|FAILED" "$WORKER_LOG" 2>/dev/null; then
            echo "⚠ ERRORS DETECTED IN YEAR $Y"
            grep "ERROR\|Exception\|FAILED" "$WORKER_LOG" | tail -5
          fi
        fi
      fi
    done
    
    echo ""
    echo "[$(date -u +%H:%M:%S)] Monitor complete. Issue: $ISSUE_URL"
    break
  fi
  
  sleep 30
done
