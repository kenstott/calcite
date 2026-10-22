#!/usr/bin/env bash
#
# reload-comtrade-capped.sh — reload every comtrade_flows year that still holds a partition cut
# off at the API's per-call record limit, one year at a time, until none is left.
#
# The Comtrade API silently truncates a response at the subscription tier's record limit (see
# ComtradeTransformer). Partitions loaded before the transformer split by partner hold exactly that
# many rows. The free tier allows 500 calls a day, so a full reload is spread over days: this script
# runs one year, checks that year's partitions on MinIO, and retries a year whose run failed (quota
# exhausted, upstream error) after an hour. A year is done when no partition of it holds exactly
# the limit; the script is idempotent, so it can be stopped and restarted at any point.
#
# Usage: GOVDATA_JAR=<private jar with the split> reload-comtrade-capped.sh \
#            [--from-year DATA_YEAR] [--to-year DATA_YEAR] [--limit N] [--attempts N]
#   Data years, descending from --from-year (default 2025) to --to-year (default 2008).
#   --limit    the maxRecords in econ-schema.yaml (default 100000)
#   --attempts runs per year before giving up (default 6)
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FROM=2025; TO=2008; LIMIT=100000; ATTEMPTS=6
while [ $# -gt 0 ]; do
  case "$1" in
    --from-year) FROM="$2"; shift 2 ;;
    --to-year)   TO="$2"; shift 2 ;;
    --limit)     LIMIT="$2"; shift 2 ;;
    --attempts)  ATTEMPTS="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
: "${GOVDATA_JAR:?set GOVDATA_JAR to a private jar that contains the partner split}"
[ -f "$GOVDATA_JAR" ] || { echo "no such jar: $GOVDATA_JAR" >&2; exit 2; }

set -a; . "$HERE/../../.env.prod"; set +a
LOG="$HERE/runs/reload-comtrade-capped-$(date +%Y%m%d-%H%M).log"
log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }

# Partitions of one data year holding exactly the limit.
capped_in_year() {
  local year="$1" endpoint="${AWS_ENDPOINT_OVERRIDE#http://}"
  duckdb -noheader -list -c "
    INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
    SET s3_access_key_id='${AWS_ACCESS_KEY_ID}'; SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
    SET s3_region='us-east-1'; SET s3_endpoint='${endpoint}'; SET s3_url_style='path'; SET s3_use_ssl=false;
    SELECT count(*) FROM (SELECT reporter, flow FROM iceberg_scan('s3://govdata-parquet-v1/econ/comtrade_flows', allow_moved_paths=true)
      WHERE year='${year}' GROUP BY reporter, flow HAVING count(*) = ${LIMIT});" 2>/dev/null | tail -1
}

# Only one comtrade_flows reload at a time: they share the day's API quota.
wait_for_idle() {
  while pgrep -f 'force-reprocess.sh .*--tables comtrade_flows' > /dev/null; do
    log "another comtrade_flows reprocess is running; waiting"
    sleep 600
  done
}

for ((year = FROM; year >= TO; year--)); do
  attempt=0
  while true; do
    left="$(capped_in_year "$year")"
    [[ "$left" =~ ^[0-9]+$ ]] || { log "could not count capped partitions for $year (got '$left'); stopping"; exit 1; }
    if [ "$left" -eq 0 ]; then
      log "data year $year: no partition at the $LIMIT-row limit"
      break
    fi
    if [ "$attempt" -ge "$ATTEMPTS" ]; then
      log "data year $year: $left partition(s) still at the limit after $ATTEMPTS runs; stopping"
      exit 1
    fi
    attempt=$((attempt + 1))
    wait_for_idle
    log "data year $year: $left partition(s) at the limit; run $attempt/$ATTEMPTS (pipeline year $((year + 1)))"
    (cd "$HERE" && ./force-reprocess.sh --schema econ --tables comtrade_flows \
        --start $((year + 1)) --end $((year + 1)) --skip-daily >> "$LOG" 2>&1)
    log "data year $year: run $attempt exited $?"
    left="$(capped_in_year "$year")"
    if [[ "$left" =~ ^[0-9]+$ ]] && [ "$left" -gt 0 ]; then
      log "data year $year: $left partition(s) still at the limit; waiting an hour (quota or upstream error)"
      sleep 3600
    fi
  done
done
log "done: no data year from $FROM down to $TO has a partition at the limit"
