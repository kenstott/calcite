#!/bin/bash
# Monitor for #349 completion, then resume #353

REPO="/home/kstott/calcite"
cd "$REPO"

check_housing_workers() {
  ps aux | grep -E "worker-housing" | grep -v grep | wc -l
}

check_pool_running() {
  ps aux | grep "run-pool.sh" | grep housing | grep -v grep | wc -l
}

echo "[$(date)] Starting #353 completion monitor"
echo "Watching for #349 (housing pool) to complete..."

# Check every 5 minutes initially, then every 30 seconds in final phase
while true; do
  workers=$(check_housing_workers)
  pools=$(check_pool_running)
  
  if [ "$workers" -eq 0 ] && [ "$pools" -eq 0 ]; then
    echo "[$(date)] ✓ Housing pool cleared — #349 appears complete"
    sleep 10  # Brief grace period
    
    # Double-check
    workers=$(check_housing_workers)
    pools=$(check_pool_running)
    if [ "$workers" -eq 0 ] && [ "$pools" -eq 0 ]; then
      echo "[$(date)] ✓ Confirmed: Housing conflict cleared, ready to resume #353"
      echo "Resume command: cd $REPO && ./govdata/scripts/data_purge.sh --schema housing --tables hmda_loans,hmda_applicant_demographics --env prod && GOVDATA_JAR=$REPO/govdata/build/libs/sih-govdata-hmda352.jar ./govdata/scripts/parallel/force-reprocess.sh --schema housing --tables hmda_loans,hmda_applicant_demographics --start 2019 --end 2026"
      exit 0
    fi
  else
    echo "[$(date)] Housing workers: $workers, Pool: $pools — still running"
  fi
  
  sleep 300  # Check every 5 minutes
done
