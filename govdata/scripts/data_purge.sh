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
# data_purge.sh — Purge tracker entries and Iceberg tables for a set of tables.
#
# Unlike data_fix.sh (which writes a "cleared" tracker marker and triggers ETL),
# data_purge.sh fully deletes:
#   1. The Iceberg table directory at <parquet bucket>/<schema>/<table>
#   2. All tracker entries matching year=*/source_key=<table>/* (both per-year
#      incremental markers and the _table_complete completion marker for that table)
#   2b. For document-based schemas (sec / sec_secondary): the per-ACCESSION document
#      completions (source_key=<accession>, table_name in metadata/facts/contexts/
#      relationships/mda/chunks/13f/13dg/_no_xbrl/_filing_meta/_error_count). These are keyed
#      by accession + output-type suffix, not the schema table name, so step 2 never matches
#      them; without clearing them SecFilingCache.filterUnprocessed treats accessions as
#      complete and skips re-ingest, leaving the purged tables empty. Schema-level; forces
#      a full document re-ingest on the next run.
#   2c. The ConversionMetadata row-count cache for the table (<operating-base>/.aperio/
#      <schema>/.conversions.json). DuckDBIcebergCountStarRule answers COUNT(*) from this
#      cached value and only self-heals when it is null/0 — left alone after Step 1 purges
#      the Iceberg data, it keeps reporting the OLD nonzero count for a now-empty table.
#   3. With --raw: the raw HTTP cache for the table — the object store at
#      <raw bucket>/<schema>/<table> (govdata-raw-v1, what prod/DQ runs actually
#      read/write) plus the local mirror at $ETL_RAW_CACHE_DIR/<table> if present.
#      Use this when a prior run cached a corrupt/truncated/stale upstream response
#      and HttpSource keeps re-using it instead of refetching. Note the raw cache is
#      keyed by partition dims, not the URL, so a URL/transformer change does not
#      invalidate it — purge raw explicitly after such a fix.
#
# It does NOT trigger ETL. The next run-pool invocation will re-ingest from
# scratch.
#
# Usage:
#   data_purge.sh --schema <schema> --tables <t1,t2,...> \
#                 [--env prod|dq] [--remote <rclone-remote>] [--raw] [--dry-run]
#
# --env selects the BUCKET-NAME set; --remote (optional) overrides the object-store ENDPOINT
# (rclone remote) independently, so the two can be mixed:
# --env prod (default): prod bucket names (govdata-parquet-v1 / -tracker-v1); remote defaults to r2.
# --env dq:             DQ bucket names (GOVDATA_DQ_*); remote defaults to $GOVDATA_DQ_RCLONE_REMOTE.
# --remote <name>:      force the rclone remote, e.g. `--env prod --remote minio` purges the prod
#                       bucket names on a local MinIO endpoint (the store used when .env.prod points
#                       AWS_ENDPOINT_OVERRIDE at localhost).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

# ── Argument parsing ──────────────────────────────────────────────────────────
SCHEMA=""
TABLES_CSV=""
DRY_RUN=false
ENV_NAME="prod"
REMOTE_OVERRIDE=""
PURGE_RAW=false
RAW_CACHE_DIR="${ETL_RAW_CACHE_DIR:-/tmp/etl-raw-cache}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)  SCHEMA="$2";     shift 2 ;;
    --tables)  TABLES_CSV="$2"; shift 2 ;;
    --table)   TABLES_CSV="$2"; shift 2 ;;  # alias for single table
    --env)     ENV_NAME="$2";   shift 2 ;;
    --remote)  REMOTE_OVERRIDE="$2"; shift 2 ;;  # override the rclone remote independent of --env
    --raw)     PURGE_RAW=true;  shift   ;;
    --dry-run) DRY_RUN=true;    shift   ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$SCHEMA" || -z "$TABLES_CSV" ]]; then
  cat <<EOF
Usage: data_purge.sh --schema <schema> --tables <t1,t2,...> [--env prod|dq] [--remote <rclone-remote>] [--raw] [--dry-run]

Examples:
  data_purge.sh --schema patents --tables patent_claims,patent_summaries --env dq
  data_purge.sh --schema lands   --tables national_forests --env dq --raw
  data_purge.sh --schema fec     --tables committee_contributions --dry-run
  data_purge.sh --schema edu     --tables ccd_schools --env prod --remote minio

Flags:
  --remote  Override the rclone remote (the object-store endpoint) independent of the bucket-name
            set chosen by --env. Defaults: --env prod -> r2, --env dq -> \$GOVDATA_DQ_RCLONE_REMOTE.
            Use e.g. `--env prod --remote minio` to purge the PROD bucket names on a LOCAL MinIO
            endpoint (the store a deployment reads/writes when .env.prod points AWS_ENDPOINT_OVERRIDE
            at localhost). The DuckDB tracker read follows AWS_ENDPOINT_OVERRIDE from the sourced env.
  --raw     Also purge the raw HTTP cache for each table: the object store at
            <raw bucket>/<schema>/<table> (GOVDATA_RAW_DIR, default
            s3://govdata-raw-v1, via the env's rclone remote) plus the local
            mirror at \$ETL_RAW_CACHE_DIR/<table> if present. Use when a cached
            response is corrupt or stale and is being reused instead of refetching.
EOF
  exit 1
fi

if [[ "$ENV_NAME" != "prod" && "$ENV_NAME" != "dq" ]]; then
  echo "Error: --env must be 'prod' or 'dq' (got '$ENV_NAME')"
  exit 1
fi

# Split tables CSV into array
IFS=',' read -ra TABLES <<< "$TABLES_CSV"

# ── Load environment ──────────────────────────────────────────────────────────
if [[ -f "$GOVDATA_HOME/.env.prod" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$GOVDATA_HOME/.env.prod"
  set +a
elif [[ -f "$GOVDATA_HOME/.env.test" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$GOVDATA_HOME/.env.test"
  set +a
fi

if [[ "$ENV_NAME" == "dq" ]]; then
  if [[ ! -f "$GOVDATA_HOME/.env.dq" ]]; then
    echo "Error: --env dq requested but $GOVDATA_HOME/.env.dq not found"
    exit 1
  fi
  set -a
  # shellcheck source=/dev/null
  source "$GOVDATA_HOME/.env.dq"
  set +a
fi

if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]]; then
  echo "Error: AWS credentials not set. Source .env.prod or .env.dq first."
  exit 1
fi

# ── Derived variables ─────────────────────────────────────────────────────────
if [[ "$ENV_NAME" == "dq" ]]; then
  : "${GOVDATA_DQ_BUCKET:?GOVDATA_DQ_BUCKET not set (check .env.dq)}"
  : "${GOVDATA_DQ_RCLONE_REMOTE:?GOVDATA_DQ_RCLONE_REMOTE not set (check .env.dq)}"
  PARQUET_BUCKET="s3://${GOVDATA_DQ_BUCKET}"
  RCLONE_REMOTE="${GOVDATA_DQ_RCLONE_REMOTE}"
else
  PARQUET_BUCKET="${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}"
  RCLONE_REMOTE="r2"
fi

# ── Tracker ───────────────────────────────────────────────────────────────────
# Postgres is the canonical record of ETL state; there is no second copy to keep in step.
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/tracker_pg.sh"
PG_NS="$(pg_ns_from_bucket "$PARQUET_BUCKET")" || exit 2
echo "Tracker schema: ${PG_NS}"

# --remote decouples the object-store endpoint from the bucket-name set chosen by --env. The
# rclone deletes target RCLONE_REMOTE while the DuckDB tracker read targets AWS_ENDPOINT_OVERRIDE
# (from the sourced env). e.g. `--env prod --remote minio` purges the prod bucket NAMES
# (govdata-parquet-v1 / govdata-tracker-v1) on a LOCAL MinIO endpoint — the store this deployment
# actually reads/writes when .env.prod points AWS_ENDPOINT_OVERRIDE at localhost.
if [[ -n "$REMOTE_OVERRIDE" ]]; then
  RCLONE_REMOTE="$REMOTE_OVERRIDE"
fi
if ! rclone listremotes 2>/dev/null | grep -qx "${RCLONE_REMOTE}:"; then
  echo "Error: rclone remote '${RCLONE_REMOTE}:' is not configured (rclone listremotes)."
  exit 1
fi

s3_to_rclone() {
  local path="${1#s3://}"
  path="${path%/}/"
  echo "${RCLONE_REMOTE}:${path}"
}
RCLONE_PARQUET="$(s3_to_rclone "$PARQUET_BUCKET")"
# Raw HTTP cache lives in the configured cache repository — the object store
# (govdata-raw-v1, same bucket for prod and dq, reached via the env's remote) is
# what prod/DQ runs read and write. GOVDATA_RAW_DIR overrides the bucket.
RAW_BUCKET="${GOVDATA_RAW_DIR:-s3://govdata-raw-v1}"
RCLONE_RAW="$(s3_to_rclone "$RAW_BUCKET")"

# ── Banner ────────────────────────────────────────────────────────────────────
echo "=================================================="
echo "data_purge.sh"
echo "=================================================="
echo "  Env:       $ENV_NAME (rclone remote: ${RCLONE_REMOTE})"
echo "  Schema:    $SCHEMA"
echo "  Tables:    ${TABLES[*]}"
echo "  Iceberg:   ${PARQUET_BUCKET}/${SCHEMA}/<table>"
echo "  Tracker:   ${PG_NS} (postgres) — pipeline_tracker + table_completion"
$PURGE_RAW && echo "  Raw cache: ${RAW_BUCKET}/${SCHEMA}/<table>/  (+ local ${RAW_CACHE_DIR}/<table>/)"
case "$SCHEMA" in sec|sec_secondary)
  echo "  Documents: clearing per-accession completions (metadata/facts/contexts/relationships/mda/chunks/_no_xbrl/…) so the next run re-ingests filings" ;;
esac
$DRY_RUN && echo "  *** DRY RUN — no changes will be made ***"
echo ""

ERRORS=0

# ── Per-table purge ───────────────────────────────────────────────────────────
for TABLE in "${TABLES[@]}"; do
  TABLE="$(echo -n "$TABLE" | xargs)"  # trim whitespace
  [[ -z "$TABLE" ]] && continue

  echo "── Purging ${SCHEMA}/${TABLE} ──"
  ICEBERG_PATH="${RCLONE_PARQUET}${SCHEMA}/${TABLE}"

  # Step 1: Drop Iceberg table directory.
  # Removing the Iceberg data is the AUTHORITATIVE re-ingest trigger: with no committed
  # data the pipeline logs "no Iceberg data — force-reprocessing all combinations" and
  # rebuilds every period regardless of (stale) tracker markers. So this step must be
  # reliable — distinguish a real `rclone ls` failure from a genuinely empty table rather
  # than swallowing errors (a swallowed transient error silently skips the purge, leaving
  # data in place so --etl-resume wrongly skips the table).
  echo "  [1/2] Iceberg: ${PARQUET_BUCKET}/${SCHEMA}/${TABLE}"
  if $DRY_RUN; then
    echo "        [DRY RUN] Would purge: $ICEBERG_PATH"
  else
    rclone_ls_out=""; ls_ok=false
    for attempt in 1 2 3; do
      if rclone_ls_out=$(rclone ls "$ICEBERG_PATH" 2>/dev/null); then ls_ok=true; break; fi
      sleep 2
    done
    if ! $ls_ok; then
      echo "        ERROR: 'rclone ls $ICEBERG_PATH' failed after 3 attempts — NOT skipping (would leave data in place)"
      ERRORS=$((ERRORS+1))
    else
      FILE_COUNT=$(echo -n "$rclone_ls_out" | grep -c . || true)
      if [[ "$FILE_COUNT" -eq 0 ]]; then
        echo "        No files found — already empty"
      elif rclone purge "$ICEBERG_PATH" 2>/dev/null; then
        # verify the purge actually emptied the directory
        remain=$(rclone ls "$ICEBERG_PATH" 2>/dev/null | grep -c . || true)
        if [[ "$remain" -eq 0 ]]; then
          echo "        Purged $FILE_COUNT files"
        else
          echo "        ERROR: $remain files remain after purge of $ICEBERG_PATH"
          ERRORS=$((ERRORS+1))
        fi
      else
        echo "        ERROR: failed to purge $ICEBERG_PATH"
        ERRORS=$((ERRORS+1))
      fi
    fi
  fi

  # Step 2: Delete tracker markers for this table.
  # The tracker keys markers by the fetch-unit's dimension VALUES, not the table name —
  # IncrementalTracker.flattenKeyValues writes source_key=<single dim value> or, for
  # multi-dim tables, a sorted "k1=v1__k2=v2__..." composite (e.g.
  # source_key=effective_year=2024__state_fips=01__type=cdo_annual__year=2025). The table
  # appears only as the type= dimension VALUE, which is not even the table name
  # (type=cdo_annual ≠ table cdo_annual_summaries). So a path glob "source_key=<table>"
  # matches nothing. The marker PARQUET content, however, carries the real table_name
  # column — so we resolve the per-source_key marker dirs via DuckDB and delete those.
  # (Compacted markers under year=*/_compacted/ are left in place: they are superseded by
  # the newer markers written when the table re-ingests, and Step 1's Iceberg removal is
  # what actually forces that re-ingest.)
  echo "  [2/2] Tracker: marker rows where table_name=${TABLE}"
  pg_tracker_purge_table "$PG_NS" "$TABLE" "$($DRY_RUN && echo true || echo false)" || exit 2

  # Step 2b: Clear the ConversionMetadata row-count cache for this table.
  # DuckDBIcebergCountStarRule answers COUNT(*) from the cached rowCount in
  # <operating-base>/.aperio/<schema>/.conversions.json and only self-heals when that cache is
  # null/0. Purging the Iceberg data (Step 1) leaves the old NONZERO count behind, so COUNT(*)
  # keeps reporting phantom rows for the now-empty table until it is re-populated. Null the cached
  # count so the rule self-heals to the real (0) count on the next query. Operating base is
  # GOVDATA_DATA_DIR or ~/.govdata (same resolution GovDataDriver uses).
  CONV_FILE="${GOVDATA_DATA_DIR:-$HOME/.govdata}/.aperio/${SCHEMA}/.conversions.json"
  echo "  [2c/2] Count cache: ${CONV_FILE}"
  if $DRY_RUN; then
    echo "        [DRY RUN] Would null cached rowCount for '${TABLE}'"
  elif [[ -f "$CONV_FILE" ]] && command -v python3 >/dev/null 2>&1; then
    _cc=$(python3 - "$CONV_FILE" "$TABLE" <<'PY' || echo 0
import json, sys
f, table = sys.argv[1], sys.argv[2]
try:
    with open(f) as fh: data = json.load(fh)
except Exception:
    print(0); sys.exit(0)
def clr(o):
    n = 0
    if isinstance(o, dict):
        if isinstance(o.get(table), dict) and "rowCount" in o[table]:
            o[table]["rowCount"] = None; n += 1
        for k, v in o.items():
            if k != table and isinstance(v, dict) and isinstance(v.get(table), dict) and "rowCount" in v[table]:
                v[table]["rowCount"] = None; n += 1
    return n
n = clr(data)
if n:
    with open(f, "w") as fh: json.dump(data, fh, indent=2)
print(n)
PY
)
    if [[ "${_cc:-0}" -gt 0 ]]; then
      echo "        Nulled cached rowCount for '${TABLE}'"
    else
      echo "        No cached rowCount entry for '${TABLE}'"
    fi
  else
    echo "        No local .conversions.json (or python3 missing) — nothing to clear"
  fi

  # Step 3 (optional): Delete the raw HTTP cache — the object store (what prod/DQ
  # runs actually read/write) and the local mirror if one is in use.
  if $PURGE_RAW; then
    RAW_OBJ_PATH="${RCLONE_RAW}${SCHEMA}/${TABLE}"
    RAW_LOCAL_PATH="${RAW_CACHE_DIR}/${TABLE}"
    echo "  [3/3] Raw cache: ${RAW_BUCKET}/${SCHEMA}/${TABLE}  (+ local ${RAW_LOCAL_PATH})"
    if $DRY_RUN; then
      echo "        [DRY RUN] Would purge $RAW_OBJ_PATH and remove $RAW_LOCAL_PATH"
    else
      # Object store (govdata-raw-v1) — the cache prod/DQ runs key by partition and reuse
      raw_obj_ls=$(rclone ls "$RAW_OBJ_PATH" 2>/dev/null || true)
      if [[ -z "$raw_obj_ls" ]]; then
        echo "        Object store: no files found — skipping"
      elif rclone purge "$RAW_OBJ_PATH" 2>/dev/null; then
        echo "        Object store: purged"
      else
        echo "        ERROR: failed to purge $RAW_OBJ_PATH"
        ERRORS=$((ERRORS+1))
      fi
      # Local mirror, only if a local cache dir is in use
      if [[ -d "$RAW_LOCAL_PATH" ]]; then
        raw_size=$(du -sh "$RAW_LOCAL_PATH" 2>/dev/null | cut -f1)
        if rm -rf "$RAW_LOCAL_PATH"; then
          echo "        Local: removed ${raw_size:-?}"
        else
          echo "        ERROR: failed to remove $RAW_LOCAL_PATH"
          ERRORS=$((ERRORS+1))
        fi
      else
        echo "        Local: no cache directory — skipping"
      fi
    fi
  fi
  echo ""
done

# ── Per-accession document completions (document-based schemas: sec / sec_secondary) ──
# SEC tables are populated per-DOCUMENT by DocumentETLProcessor; per-accession completion is
# tracked as ROWS inside the marker parquets — source_key=accession_number=...__year=Y,
# table_name=<output suffix> (metadata/facts/contexts/relationships/mda/insider/earnings/chunks)
# — and the historical ones are COMPACTED into year=*/_compacted/*.parquet, one file mixing many
# accessions and outputs. SecFilingCache.filterUnprocessed reads these and SKIPS any accession
# whose outputs look complete, so a plain Iceberg/per-table purge leaves the derived tables empty
# ("rebuild is a no-op"). Map each purged table to its completion suffix and REWRITE the marker
# parquets to DROP only those rows — surgical, so completions for tables you did NOT purge
# (e.g. stock_prices, filing_metadata) survive and don't needlessly re-ingest. Dropping any of an
# accession's output rows makes filterUnprocessed re-process that whole accession next run.
# institutional_holdings (13F-HR) and beneficial_ownership (SC 13D/G) ARE tracked here too —
# SecFilingCache.recordInventory writes their completion the same way as every other output,
# just under the shorter suffixes "13f"/"13dg" (see FileInventory.hasInstitutionalHoldings/
# hasBeneficialOwnership and the _13f.parquet/_13dg.parquet output filenames that set them).
case "$SCHEMA" in
  sec|sec_secondary)
    # Suffix map lives in tracker_pg.sh (sec_accession_suffixes_for_tables) — shared with
    # force-reprocess.sh, which needs the identical mapping to purge accessions before a forced
    # reprocess actually re-fetches anything.
    _suffixes=()
    for _t in "${TABLES[@]}"; do
      _t="$(echo -n "$_t" | xargs)"
      while IFS= read -r _s; do
        [[ -n "$_s" ]] && _suffixes+=("$_s")
      done < <(sec_accession_suffixes_for_tables "$_t")
    done
    if [[ ${#_suffixes[@]} -eq 0 ]]; then
      echo "── ${SCHEMA}: no document-completion suffix maps to the purged tables — skipping ──"
    else
      # Both sec and sec_secondary are produced by one EDGAR document pass whose per-accession
      # completions are tracked under the "sec" schema tracker — so clear there for either.
      _doc_schema=sec
      _inlist=""; for _s in "${_suffixes[@]}"; do _inlist="${_inlist:+$_inlist,}'$_s'"; done
      echo "── Clearing accession completions for ${SCHEMA}: table_name IN (${_inlist}) ──"
      pg_tracker_purge_accessions "$PG_NS" "$_inlist" "$($DRY_RUN && echo true || echo false)" || exit 2
    fi
    echo ""
    ;;
esac

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=================================================="
if $DRY_RUN; then
  echo "Dry run complete — no changes made"
elif [[ $ERRORS -gt 0 ]]; then
  echo "Purge completed with $ERRORS error(s)"
  exit 1
else
  echo "Purge complete"
fi
echo "=================================================="
