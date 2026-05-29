#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env
_env_dq="$SCRIPT_DIR/../../.env.dq"
if [ -f "$_env_dq" ]; then set -a; source "$_env_dq"; set +a; fi

# ── argument parsing ──────────────────────────────────────────────────────────
SCHEMA="${1:-}"
if [ -z "$SCHEMA" ]; then
  echo "Usage: $(basename "$0") <schema> [--mode daily|historical] [--dry-run]" >&2
  exit 1
fi

MODE="daily"
DRY_RUN=false
REBUILD=false
INCLUDE_DAILY=false
REBUILD_START_YEAR=""
shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      shift
      MODE="${1:?--mode requires an argument: daily or historical}"
      ;;
    --dry-run)
      DRY_RUN=true
      ;;
    --rebuild)
      REBUILD=true
      ;;
    --include-daily)
      INCLUDE_DAILY=true
      ;;
    --start-year)
      shift
      REBUILD_START_YEAR="${1:?--start-year requires a 4-digit year}"
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

if [[ "$MODE" != "daily" && "$MODE" != "historical" ]]; then
  echo "ERROR: --mode must be 'daily' or 'historical', got: $MODE" >&2
  exit 1
fi

# ── DQ bucket config ──────────────────────────────────────────────────────────
# Defaults to production. Set GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq (and
# GOVDATA_DQ_TRACKER_BUCKET=govdata-tracker-v1-dq) plus CF_ACCOUNT_ID /
# CF_API_TOKEN to use an isolated test bucket instead of purging production.
export GOVDATA_DQ_BUCKET="${GOVDATA_DQ_BUCKET:-govdata-parquet-v1}"
export GOVDATA_DQ_TRACKER_BUCKET="${GOVDATA_DQ_TRACKER_BUCKET:-govdata-tracker-v1-dq}"

WORKER_ID="worker-dq-${SCHEMA}-${MODE}"
DQ_SQL="$GOVDATA_ROOT/scripts/${SCHEMA}_dq.sql"
RUN_DIR="$SCRIPT_DIR/runs/$WORKER_ID"
mkdir -p "$RUN_DIR"

if [ ! -f "$DQ_SQL" ]; then
  echo "ERROR: DQ script not found: $DQ_SQL" >&2
  exit 1
fi

RUN_DATE=$(date +%Y-%m-%d)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RUN_DIR/dq_${TIMESTAMP}.log"
ETL_LOG_DIR=""  # set after rebuild ETL so error handler finds the right log

S3_RESULT_PATH="s3://${GOVDATA_DQ_TRACKER_BUCKET}/dq-results/schema=${SCHEMA}/run_date=${RUN_DATE}/type=${MODE}/results.parquet"

# ── error handler + EXIT trap (must be defined before rebuild block) ──────────
_SCRIPT_COMPLETE=false

_collect_log_tail() {
  local log="$1" lines="${2:-30}"
  if [ -f "$log" ] && [ -s "$log" ]; then
    echo "<details><summary>\`$(basename "$log")\` — last ${lines} lines</summary>"
    echo ""
    echo '```'
    tail -n "$lines" "$log"
    echo '```'
    echo "</details>"
  else
    echo "_\`$(basename "$log")\` not found or empty_"
  fi
}

# Extract a one-line root-cause summary from a log file.
# Greps for the first Exception/Error/OOM line and returns it.
_extract_root_cause() {
  local log="$1"
  [ -f "$log" ] || { echo "(log not found)"; return; }
  local line
  line=$(grep -m1 -E "OutOfMemoryError|Exception|ERROR |FATAL |BUILD FAILURE|HTTP Error|HTTP [45][0-9][0-9]" "$log" 2>/dev/null || true)
  if [ -n "$line" ]; then
    echo "${line:0:200}"
  else
    echo "(no error pattern found in log)"
  fi
}

_file_script_error_issue() {
  _SCRIPT_COMPLETE=true
  local detail="$1"
  if ! command -v gh >/dev/null 2>&1; then
    log_info "$WORKER_ID: gh not found in PATH ($PATH) — skipping issue filing"
    return
  fi
  if [ -z "${GH_TOKEN:-}${GITHUB_TOKEN:-}" ]; then
    if ! gh auth status --hostname github.com >/dev/null 2>&1; then
      log_info "$WORKER_ID: gh not authenticated (no GH_TOKEN, no stored credential) — skipping issue filing"
      return
    fi
  fi
  gh label create "dq"      --color "#0075ca" --description "Data quality"    --repo kenstott/calcite 2>/dev/null || true
  gh label create "dq-fail" --color "#d93f0b" --description "DQ hard failure" --repo kenstott/calcite 2>/dev/null || true

  # Locate the ETL log — it lands in runs/worker-<schema>-initial/, not in RUN_DIR
  local latest_etl=""
  if [ -n "${ETL_LOG_DIR:-}" ]; then
    latest_etl=$(ls -t "${ETL_LOG_DIR}"/etl_*.log 2>/dev/null | head -1 || true)
  fi
  # Fallback: also check RUN_DIR in case a future code path writes there
  if [ -z "$latest_etl" ]; then
    latest_etl=$(ls -t "$RUN_DIR"/etl_*.log 2>/dev/null | head -1 || true)
  fi

  # Extract root cause: prefer DQ log when DuckDB ran (it has the actual crash),
  # fall back to ETL log when DQ log is empty/missing (ETL crashed before DuckDB started).
  local root_cause_log
  if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
    root_cause_log="$LOG_FILE"
  else
    root_cause_log="${latest_etl:-$LOG_FILE}"
  fi
  local root_cause
  root_cause=$(_extract_root_cause "$root_cause_log")

  # Build log sections (collapsible so the issue is readable without scrolling)
  local log_sections=""
  if [ -n "$latest_etl" ]; then
    log_sections="$(_collect_log_tail "$latest_etl" 40)"$'\n\n'
  fi
  if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
    log_sections="${log_sections}$(_collect_log_tail "$LOG_FILE" 30)"
  fi

  local issue_title="[DQ] ${SCHEMA}: ERROR — ${root_cause:0:80}"

  local open_issue
  open_issue=$(gh issue list \
    --repo kenstott/calcite \
    --state open \
    --label dq \
    --limit 200 \
    --json number,title \
    --jq ".[] | select(.title | startswith(\"[DQ] ${SCHEMA}:\")) | .number" \
    2>&1 | head -1)
  if [ -n "$open_issue" ] && [[ "$open_issue" =~ ^[0-9]+$ ]]; then
    gh issue comment "$open_issue" \
      --repo kenstott/calcite \
      --body "**Script error ${RUN_DATE}** — ${detail}

**Root cause:** \`${root_cause}\`

${log_sections}" \
      && log_info "$WORKER_ID: commented on DQ issue #${open_issue}" \
      || log_info "$WORKER_ID: WARNING: failed to comment on issue #${open_issue}"
  else
    gh issue create \
      --repo kenstott/calcite \
      --title "${issue_title}" \
      --label "dq" \
      --label "dq-fail" \
      --body "## DQ Script Error: \`${SCHEMA}\`

**Date:** ${RUN_DATE}
**Mode:** ${MODE}
**Worker:** \`${WORKER_ID}\`

## Root Cause

\`\`\`
${root_cause}
\`\`\`

## Detail

${detail}

## Logs

${log_sections}" \
      && log_info "$WORKER_ID: created DQ error issue" \
      || log_info "$WORKER_ID: WARNING: gh issue create failed"
  fi
}

_on_exit() {
  local code=$?
  [ -n "${TMP_DIR:-}" ] && rm -rf "$TMP_DIR"
  if [ "$code" -ne 0 ] && ! $_SCRIPT_COMPLETE; then
    _file_script_error_issue "Script exited unexpectedly with code ${code} — see log: \`${LOG_FILE}\`"
  fi
}
trap '_on_exit' EXIT

# ── helpers ───────────────────────────────────────────────────────────────────

# Reset an R2 bucket via the Cloudflare API (delete + recreate).
# One HTTP call per operation — no per-object Class A charges.
# Requires CF_ACCOUNT_ID and CF_API_TOKEN with r2:write permission.
_cf_reset_bucket() {
  local bucket="$1"
  local cf_api="https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/r2/buckets"
  local auth=(-H "Authorization: Bearer ${CF_API_TOKEN}")
  log_info "$WORKER_ID: resetting R2 bucket '$bucket' via Cloudflare API"
  # DELETE — ignore errors (bucket may not exist yet on first run)
  curl -s -X DELETE "${cf_api}/${bucket}" "${auth[@]}" 2>/dev/null || true
  sleep 3
  # CREATE — error code 10004 means "already exists and you own it", which is
  # acceptable (DELETE may have raced or bucket was pre-created manually)
  local resp
  resp=$(curl -s -X POST "${cf_api}" "${auth[@]}" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${bucket}\"}" 2>/dev/null || echo '{}')
  if echo "$resp" | grep -q '"success":true'; then
    log_info "$WORKER_ID: bucket '$bucket' created"
  elif echo "$resp" | grep -q '"code":10004'; then
    log_info "$WORKER_ID: bucket '$bucket' already exists — continuing"
  else
    log_info "$WORKER_ID: ERROR: failed to create R2 bucket '${bucket}': ${resp}"
    exit 1
  fi
  log_info "$WORKER_ID: bucket '$bucket' reset complete"
}

# ── rebuild: teardown + ETL before DQ ────────────────────────────────────────
if $REBUILD; then
  log_info "$WORKER_ID: --rebuild: starting teardown for schema=$SCHEMA (bucket=$GOVDATA_DQ_BUCKET)"

  # Delete only Iceberg metadata directories per table — avoids Class A per-object
  # delete charges on parquet data files. Tables become invisible to Iceberg so the
  # ETL re-creates them. Orphaned data files are cleaned up by Iceberg maintenance.
  _DQ_REMOTE="${GOVDATA_RCLONE_REMOTE:-r2}"
  # Ensure DQ buckets exist using the same S3 endpoint/credentials as the Java ETL.
  # Falls back through aws CLI → python3 SigV4 → rclone, in order of reliability.
  _ensure_bucket() {
    local bucket=$1
    local ep="${AWS_ENDPOINT_OVERRIDE:-}"
    if command -v aws >/dev/null 2>&1; then
      local flag=""; [ -n "$ep" ] && flag="--endpoint-url $ep"
      aws $flag s3 mb "s3://$bucket" 2>/dev/null || true
    elif command -v python3 >/dev/null 2>&1; then
      python3 - "$bucket" "$ep" "${AWS_ACCESS_KEY_ID:-}" "${AWS_SECRET_ACCESS_KEY:-}" <<'PYEOF'
import sys, urllib.request, hmac, hashlib, datetime
bucket, ep, kid, secret = sys.argv[1:]
if not ep: sys.exit(0)
host = ep.replace('http://','').replace('https://','')
now = datetime.datetime.now(datetime.timezone.utc)
d, ts = now.strftime('%Y%m%d'), now.strftime('%Y%m%dT%H%M%SZ')
ph = hashlib.sha256(b'').hexdigest()
hdr = f'host:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{ts}\n'
sh = 'host;x-amz-content-sha256;x-amz-date'
cr = f'PUT\n/{bucket}\n\n{hdr}\n{sh}\n{ph}'
sts = f'AWS4-HMAC-SHA256\n{ts}\n{d}/us-east-1/s3/aws4_request\n{hashlib.sha256(cr.encode()).hexdigest()}'
def sign(k,m): return hmac.new(k,m.encode(),hashlib.sha256).digest()
k = sign(sign(sign(sign(('AWS4'+secret).encode(),d),'us-east-1'),'s3'),'aws4_request')
sig = hmac.new(k,sts.encode(),hashlib.sha256).hexdigest()
auth = f'AWS4-HMAC-SHA256 Credential={kid}/{d}/us-east-1/s3/aws4_request,SignedHeaders={sh},Signature={sig}'
req = urllib.request.Request(f'{ep}/{bucket}', method='PUT')
for h,v in [('Host',host),('x-amz-date',ts),('x-amz-content-sha256',ph),('Authorization',auth)]: req.add_header(h,v)
try: urllib.request.urlopen(req)
except urllib.error.HTTPError as e:
  body = e.read().decode()
  if 'AlreadyOwned' not in body and 'AlreadyExists' not in body: print(f'bucket create error: {e.code} {body[:100]}', file=sys.stderr)
PYEOF
    else
      rclone mkdir "${_DQ_REMOTE}:$bucket" 2>/dev/null || true
    fi
  }
  _ensure_bucket "$GOVDATA_DQ_BUCKET"
  _ensure_bucket "$GOVDATA_DQ_TRACKER_BUCKET"
  log_info "$WORKER_ID: --rebuild: DQ buckets verified: ${GOVDATA_DQ_BUCKET}, ${GOVDATA_DQ_TRACKER_BUCKET}"
  log_info "$WORKER_ID: --rebuild: removing Iceberg metadata for schema=${SCHEMA} in ${GOVDATA_DQ_BUCKET}"
  if [ -n "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
    # MinIO: no Class A charges, so purge all data files for the schema.
    # This prevents the ETL fast-path from reusing stale data written by
    # an older engine (e.g. null expression: columns from pre-DuckDB evaluator).
    log_info "$WORKER_ID: --rebuild: full data purge for schema=${SCHEMA} in ${GOVDATA_DQ_BUCKET} (MinIO)"
    python3 - "${AWS_ENDPOINT_OVERRIDE}" "${AWS_ACCESS_KEY_ID:-}" "${AWS_SECRET_ACCESS_KEY:-}" \
        "${GOVDATA_DQ_BUCKET}" "${SCHEMA}" <<'PYEOF'
import sys, urllib.request, urllib.parse, hmac, hashlib, datetime, xml.etree.ElementTree as ET
ep, kid, secret, bucket, schema = sys.argv[1:]
if not ep: sys.exit(0)
host = ep.replace('http://','').replace('https://','')
ns = {'s':'http://s3.amazonaws.com/doc/2006-03-01/'}
def sign(k,m): return hmac.new(k,m.encode(),hashlib.sha256).digest()
def auth_headers(method, path, qs_dict, body=b''):
    now = datetime.datetime.now(datetime.timezone.utc)
    d, ts = now.strftime('%Y%m%d'), now.strftime('%Y%m%dT%H%M%SZ')
    ph = hashlib.sha256(body).hexdigest()
    qs = '&'.join(f'{urllib.parse.quote(k,safe="")}={urllib.parse.quote(str(v),safe="")}' for k,v in sorted(qs_dict.items()))
    hdr = f'host:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{ts}\n'
    sh = 'host;x-amz-content-sha256;x-amz-date'
    cr = f'{method}\n/{bucket}{path}\n{qs}\n{hdr}\n{sh}\n{ph}'
    sts = f'AWS4-HMAC-SHA256\n{ts}\n{d}/us-east-1/s3/aws4_request\n{hashlib.sha256(cr.encode()).hexdigest()}'
    k = sign(sign(sign(sign(('AWS4'+secret).encode(),d),'us-east-1'),'s3'),'aws4_request')
    sig = hmac.new(k,sts.encode(),hashlib.sha256).hexdigest()
    auth = f'AWS4-HMAC-SHA256 Credential={kid}/{d}/us-east-1/s3/aws4_request,SignedHeaders={sh},Signature={sig}'
    return ts, ph, auth, qs
total, token = 0, None
while True:
    params = {'list-type':'2','prefix':f'{schema}/','max-keys':'1000'}
    if token: params['continuation-token'] = token
    ts, ph, auth, qs_str = auth_headers('GET','/',params)
    req = urllib.request.Request(f'{ep}/{bucket}/?{qs_str}')
    for h,v in [('Host',host),('x-amz-date',ts),('x-amz-content-sha256',ph),('Authorization',auth)]: req.add_header(h,v)
    resp = urllib.request.urlopen(req).read().decode()
    root = ET.fromstring(resp)
    keys = [c.find('s:Key',ns).text for c in root.findall('.//s:Contents',ns)]
    for key in keys:
        enc = '/'.join(urllib.parse.quote(p,safe='') for p in key.split('/'))
        ts2, ph2, auth2, _ = auth_headers('DELETE',f'/{enc}',{})
        req2 = urllib.request.Request(f'{ep}/{bucket}/{enc}', method='DELETE')
        for h,v in [('Host',host),('x-amz-date',ts2),('x-amz-content-sha256',ph2),('Authorization',auth2)]: req2.add_header(h,v)
        urllib.request.urlopen(req2); total += 1
    nxt = root.find('s:NextContinuationToken',ns)
    if nxt is None: break
    token = nxt.text
print(f'Purged {total} objects for schema={schema} from {bucket}')
PYEOF
  else
    # R2: metadata-only teardown avoids Class A charges on data files.
    for table in $(rclone lsd "${_DQ_REMOTE}:${GOVDATA_DQ_BUCKET}/${SCHEMA}" 2>/dev/null | awk '{print $NF}' | grep -v "^$" || true); do
      log_info "$WORKER_ID: --rebuild: clearing metadata for table ${table}"
      rclone purge "${_DQ_REMOTE}:${GOVDATA_DQ_BUCKET}/${SCHEMA}/${table}/metadata" 2>/dev/null || true
    done
  fi
  export FORCE=true
  export FORCE_FRESH=true
  # Redirect ETL writes to the DQ bucket and its companion tracker.
  export GOVDATA_PARQUET_DIR="s3://${GOVDATA_DQ_BUCKET}"
  export CALCITE_TRACKER_S3_BUCKET="s3://${GOVDATA_DQ_TRACKER_BUCKET}"

  # 3. Delete existing DQ results so the post-ETL run starts clean.
  log_info "$WORKER_ID: --rebuild: purging dq-results for schema=$SCHEMA"
  rclone purge "${_DQ_REMOTE:-r2}:${GOVDATA_DQ_TRACKER_BUCKET}/dq-results/schema=$SCHEMA" 2>/dev/null || true

  # 4. Run historical ETL pass.
  log_info "$WORKER_ID: --rebuild: running historical ETL for schema=$SCHEMA"
  REBUILD_MODEL="$RUN_DIR/models/rebuild_${SCHEMA}_$(date +%Y%m%d_%H%M%S).json"
  mkdir -p "$(dirname "$REBUILD_MODEL")"
  export GOVDATA_RUN_MODE="historical"
  export GOVDATA_INCREMENTAL_START_YEAR="$(date +%Y)"
  if [ -n "$REBUILD_START_YEAR" ]; then
    export GOVDATA_START_YEAR="$REBUILD_START_YEAR"
  else
    export GOVDATA_START_YEAR="$(get_dq_start_year "$SCHEMA")"
  fi
  log_info "$WORKER_ID: --rebuild: year range ${GOVDATA_START_YEAR}–$((GOVDATA_INCREMENTAL_START_YEAR - 1)) for schema=$SCHEMA"
  if ! generate_single_schema_model "$SCHEMA" "$REBUILD_MODEL" 2>/dev/null; then
    log_info "$WORKER_ID: --rebuild: ERROR — no single-schema model generator for schema=$SCHEMA"
    exit 1
  fi
  run_etl "$REBUILD_MODEL" "worker-${SCHEMA}-initial"
  ETL_LOG_DIR="$SCRIPT_DIR/runs/worker-${SCHEMA}-initial"
  log_info "$WORKER_ID: --rebuild: historical ETL complete"

  # 5. Optionally run daily (current-year) ETL pass before DQ.
  if $INCLUDE_DAILY; then
    log_info "$WORKER_ID: --include-daily: running daily ETL pass for schema=$SCHEMA"
    DAILY_MODEL="$RUN_DIR/models/daily_${SCHEMA}_$(date +%Y%m%d_%H%M%S).json"
    export GOVDATA_RUN_MODE="daily"
    unset GOVDATA_START_YEAR
    if generate_single_schema_model "$SCHEMA" "$DAILY_MODEL" 2>/dev/null; then
      run_etl "$DAILY_MODEL" "worker-${SCHEMA}-daily"
      ETL_LOG_DIR="$SCRIPT_DIR/runs/worker-${SCHEMA}-daily"
      log_info "$WORKER_ID: --include-daily: daily ETL complete"
    else
      log_info "$WORKER_ID: --include-daily: no daily model for schema=$SCHEMA — skipping"
    fi
  fi

  log_info "$WORKER_ID: --rebuild: ETL complete — proceeding to DQ"
fi

# ── pre-flight anti-pattern checks (warn only — do not abort) ─────────────────
log_info "$WORKER_ID: pre-flight checks"

# Check for local directory named "s3" or "s3:" — signals broken storage wiring
if [ -n "${GOVDATA_PARQUET_DIR:-}" ]; then
  bad_dirs=$(find "$GOVDATA_PARQUET_DIR" -maxdepth 5 -type d \( -name "s3" -o -name "s3:" \) 2>/dev/null || true)
  if [ -n "$bad_dirs" ]; then
    log_info "WARNING: local 's3' or 's3:' directory found — storageProvider may be misconfigured:"
    echo "$bad_dirs" >&2
  fi
fi

# Check for deprecated source= partition on S3
deprecated_path="${GOVDATA_RCLONE_REMOTE:-r2}:${GOVDATA_DQ_BUCKET}/source=${SCHEMA}/"
deprecated_check=$(rclone ls "$deprecated_path" 2>/dev/null | head -1 || true)
if [ -n "$deprecated_check" ]; then
  log_info "WARNING: deprecated path exists: $deprecated_path — this should be removed"
fi

# ── run DQ ────────────────────────────────────────────────────────────────────
TMP_DIR=$(mktemp -d)
RESULT_LOCAL="$TMP_DIR/results.parquet"

log_info "$WORKER_ID: running DQ for schema=$SCHEMA mode=$MODE"
log_info "$WORKER_ID: DQ script: $DQ_SQL"
log_info "$WORKER_ID: log: $LOG_FILE"

DQ_EXIT=0
{
  # DuckDB 1.5.2+ disables Iceberg version guessing by default; enable it for our internal tables.
  # DuckDB 1.5.2+ ignores SET s3_* for iceberg and auto-loads ~/.aws/credentials instead.
  # CREATE SECRET overrides that and routes all S3/iceberg traffic to R2.
  _raw_ep="${AWS_ENDPOINT_OVERRIDE:-https://21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com}"
  # DuckDB ENDPOINT needs host:port only (no scheme). Detect SSL from prefix.
  if [[ "$_raw_ep" == http://* ]]; then
    _r2_endpoint="${_raw_ep#http://}"
    _use_ssl=false
    _region="us-east-1"
  else
    _r2_endpoint="${_raw_ep#https://}"
    _use_ssl=true
    _region="auto"
  fi
  cat <<_DUCKDB_PREAMBLE_
SET unsafe_enable_version_guessing = true;
SET http_timeout = 60000;
SET http_retries = 1;
CREATE OR REPLACE SECRET r2 (
    TYPE s3,
    KEY_ID '${AWS_ACCESS_KEY_ID}',
    SECRET '${AWS_SECRET_ACCESS_KEY}',
    ENDPOINT '${_r2_endpoint}',
    REGION '${_region}',
    URL_STYLE 'path',
    USE_SSL ${_use_ssl}
);
_DUCKDB_PREAMBLE_

  # Substitute env vars referenced in the DQ SQL (credentials, endpoints)
  envsubst < "$DQ_SQL"

  # Append COPY to write structured results to a local Parquet file before the session ends.
  # dq_results is a TEMP TABLE created inside the DQ SQL — it is still in scope here.
  cat <<SQL
-- Write structured DQ results to local Parquet (appended by worker-dq-run.sh)
COPY (
  SELECT
    schema,
    tbl            AS table_name,
    test,
    status,
    value,
    threshold,
    detail,
    DATE '${RUN_DATE}'  AS run_date,
    '${MODE}'           AS run_type,
    '${SCHEMA}'         AS schema_name
  FROM dq_results
) TO '${RESULT_LOCAL}' (FORMAT PARQUET);
SQL
} | duckdb 2>&1 | tee "$LOG_FILE" || DQ_EXIT=$?

if [ $DQ_EXIT -ne 0 ]; then
  if [ ! -f "$RESULT_LOCAL" ]; then
    # DuckDB crashed before COPY completed — no results written, real error
    log_info "$WORKER_ID: DuckDB exited with code $DQ_EXIT and no results written — table likely missing or schema misconfigured"
    log_info "$WORKER_ID: SCHEMA RESULT: FAIL (script error)"
    _file_script_error_issue "DuckDB exited with code ${DQ_EXIT} — table likely missing or schema misconfigured. Check log: \`${LOG_FILE}\`"
    exit 1
  fi
  # DuckDB exited non-zero but results were written (e.g. T3 sample SELECT hit HTTP 403
  # mid-script but COPY completed). Continue to verdict — the actual DQ checks are in
  # the result file.
  log_info "$WORKER_ID: DuckDB exited with code $DQ_EXIT but results parquet was written — checking verdict (non-fatal statement error, see log)"
fi

# ── verdict ───────────────────────────────────────────────────────────────────
if [ ! -f "$RESULT_LOCAL" ]; then
  log_info "$WORKER_ID: result Parquet not written — DQ script may have failed silently"
  _file_script_error_issue "Result Parquet not written — DQ script may have failed silently. Check log: \`${LOG_FILE}\`"
  exit 1
fi

VERDICT=$(duckdb -csv -c "
  SELECT CASE
    WHEN SUM(CASE WHEN status='fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status='warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END
  FROM read_parquet('${RESULT_LOCAL}');" 2>/dev/null | tail -1 | tr -d '"')

FAIL_COUNT=$(duckdb -csv -c "SELECT COUNT(*) FROM read_parquet('${RESULT_LOCAL}') WHERE status='fail';" 2>/dev/null | tail -1)
WARN_COUNT=$(duckdb -csv -c "SELECT COUNT(*) FROM read_parquet('${RESULT_LOCAL}') WHERE status='warn';" 2>/dev/null | tail -1)

log_info "$WORKER_ID: SCHEMA RESULT: $VERDICT (fails=$FAIL_COUNT warns=$WARN_COUNT)"

# Print failing tests for immediate visibility
if [ "$FAIL_COUNT" -gt 0 ]; then
  log_info "$WORKER_ID: failing tests:"
  duckdb -c "SELECT tbl AS table_name, test, value, threshold, detail FROM read_parquet('${RESULT_LOCAL}') WHERE status='fail' ORDER BY tbl, test;" 2>/dev/null || true
fi

# ── upload results ────────────────────────────────────────────────────────────
if $DRY_RUN; then
  log_info "$WORKER_ID: --dry-run — skipping S3 upload (local results at $RESULT_LOCAL)"
else
  log_info "$WORKER_ID: uploading results to $S3_RESULT_PATH"
  _s3_key="dq-results/schema=${SCHEMA}/run_date=${RUN_DATE}/type=${MODE}/results.parquet"
  _ep="${AWS_ENDPOINT_OVERRIDE:-}"
  if command -v aws >/dev/null 2>&1; then
    _flag=""; [ -n "$_ep" ] && _flag="--endpoint-url $_ep"
    aws $_flag s3 cp "$RESULT_LOCAL" "s3://${GOVDATA_DQ_TRACKER_BUCKET}/${_s3_key}"
  elif command -v python3 >/dev/null 2>&1 && [ -n "$_ep" ]; then
    python3 - "$RESULT_LOCAL" "$_ep" "$_s3_key" "${GOVDATA_DQ_TRACKER_BUCKET}" \
      "${AWS_ACCESS_KEY_ID:-}" "${AWS_SECRET_ACCESS_KEY:-}" <<'PYEOF'
import sys, urllib.request, urllib.parse, hmac, hashlib, datetime
local_file, ep, key, bucket, kid, secret = sys.argv[1:]
with open(local_file,'rb') as f: body = f.read()
host = ep.replace('http://','').replace('https://','')
now = datetime.datetime.now(datetime.timezone.utc)
d, ts = now.strftime('%Y%m%d'), now.strftime('%Y%m%dT%H%M%SZ')
ph = hashlib.sha256(body).hexdigest()
# URI-encode each path component (= must be %3D in canonical request)
canon_key = '/'.join(urllib.parse.quote(p, safe='') for p in key.split('/'))
hdr = f'host:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{ts}\n'
sh = 'host;x-amz-content-sha256;x-amz-date'
cr = f'PUT\n/{bucket}/{canon_key}\n\n{hdr}\n{sh}\n{ph}'
sts = f'AWS4-HMAC-SHA256\n{ts}\n{d}/us-east-1/s3/aws4_request\n{hashlib.sha256(cr.encode()).hexdigest()}'
def sign(k,m): return hmac.new(k,m.encode(),hashlib.sha256).digest()
k = sign(sign(sign(sign(('AWS4'+secret).encode(),d),'us-east-1'),'s3'),'aws4_request')
sig = hmac.new(k,sts.encode(),hashlib.sha256).hexdigest()
auth = f'AWS4-HMAC-SHA256 Credential={kid}/{d}/us-east-1/s3/aws4_request,SignedHeaders={sh},Signature={sig}'
req = urllib.request.Request(f'{ep}/{bucket}/{key}', data=body, method='PUT')
for h,v in [('Host',host),('x-amz-date',ts),('x-amz-content-sha256',ph),('Authorization',auth),('Content-Length',str(len(body)))]: req.add_header(h,v)
try:
    urllib.request.urlopen(req)
    print(f'Uploaded {len(body)} bytes to {bucket}/{key}')
except urllib.error.HTTPError as e:
    print(f'upload error: {e.code} {e.read().decode()[:200]}', file=sys.stderr); sys.exit(1)
PYEOF
  else
    rclone copyto "$RESULT_LOCAL" "${GOVDATA_RCLONE_REMOTE:-r2}:${GOVDATA_DQ_TRACKER_BUCKET}/${_s3_key}"
  fi
  log_info "$WORKER_ID: results written to $S3_RESULT_PATH"
fi

# ── GitHub issue filing ───────────────────────────────────────────────────────
_gh_available() {
  command -v gh >/dev/null 2>&1 || return 1
  [ -n "${GH_TOKEN:-}${GITHUB_TOKEN:-}" ] && return 0
  gh auth status --hostname github.com >/dev/null 2>&1
}
if _gh_available; then
  # Ensure labels exist (no-op if already present)
  gh label create "dq"      --color "#0075ca" --description "Data quality"    --repo kenstott/calcite 2>/dev/null || true
  gh label create "dq-warn" --color "#e4e669" --description "DQ warning"      --repo kenstott/calcite 2>/dev/null || true
  gh label create "dq-fail" --color "#d93f0b" --description "DQ hard failure" --repo kenstott/calcite 2>/dev/null || true

  # Find existing open DQ issue for this schema
  OPEN_ISSUE=$(gh issue list \
    --repo kenstott/calcite \
    --state open \
    --label dq \
    --limit 200 \
    --json number,title \
    --jq ".[] | select(.title | startswith(\"[DQ] ${SCHEMA}:\")) | .number" \
    2>/dev/null | head -1)

  if [ "$VERDICT" = "PASS" ]; then
    if [ -n "$OPEN_ISSUE" ]; then
      gh issue close "$OPEN_ISSUE" \
        --repo kenstott/calcite \
        --comment "DQ passed on ${RUN_DATE} (${MODE} mode). Closing." \
        2>/dev/null && log_info "$WORKER_ID: closed DQ issue #${OPEN_ISSUE}" || \
        log_info "$WORKER_ID: WARNING: failed to close DQ issue #${OPEN_ISSUE}"
    fi
  else
    case "$VERDICT" in
      FAIL) VERDICT_LABEL="dq-fail" ;;
      *)    VERDICT_LABEL="dq-warn" ;;
    esac

    FINDINGS_TABLE=$(duckdb -noheader -list -c "
      SELECT '| ' || tbl || ' | ' || test || ' | ' || status || ' | ' ||
             COALESCE(CAST(value AS VARCHAR), '—') || ' | ' ||
             COALESCE(CAST(threshold AS VARCHAR), '—') || ' | ' ||
             COALESCE(REPLACE(detail, '|', '/'), '') || ' |'
      FROM read_parquet('${RESULT_LOCAL}')
      WHERE status != 'pass'
      ORDER BY status DESC, tbl, test
      LIMIT 50;" 2>/dev/null || echo "| (could not read findings) | | | | | |")

    FINDINGS_MD="| Table | Test | Status | Value | Threshold | Detail |
|-------|------|--------|-------|-----------|--------|
${FINDINGS_TABLE}"

    if [ -n "$OPEN_ISSUE" ]; then
      gh issue comment "$OPEN_ISSUE" \
        --repo kenstott/calcite \
        --body "**Re-run ${RUN_DATE}** — ${VERDICT}: ${FAIL_COUNT} fails, ${WARN_COUNT} warns

${FINDINGS_MD}

Results: \`${S3_RESULT_PATH}\`" \
        2>/dev/null && log_info "$WORKER_ID: commented on DQ issue #${OPEN_ISSUE}" || \
        log_info "$WORKER_ID: WARNING: failed to comment on DQ issue #${OPEN_ISSUE}"
    else
      gh issue create \
        --repo kenstott/calcite \
        --title "[DQ] ${SCHEMA}: ${VERDICT} — ${FAIL_COUNT} fails, ${WARN_COUNT} warns" \
        --label "dq" \
        --label "$VERDICT_LABEL" \
        --body "## DQ ${VERDICT}: \`${SCHEMA}\`

**Date:** ${RUN_DATE}
**Mode:** ${MODE}
**Fails:** ${FAIL_COUNT} | **Warns:** ${WARN_COUNT}

## Findings

${FINDINGS_MD}

## Results

\`${S3_RESULT_PATH}\`" \
        2>/dev/null && log_info "$WORKER_ID: created DQ issue for ${VERDICT}" || \
        log_info "$WORKER_ID: WARNING: failed to create DQ issue (gh not authenticated?)"
    fi
  fi
fi

_SCRIPT_COMPLETE=true
log_info "$WORKER_ID complete"

[ "$VERDICT" = "FAIL" ] && exit 1
exit 0
