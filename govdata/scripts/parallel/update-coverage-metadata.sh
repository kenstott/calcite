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
# update-coverage-metadata.sh — queries real production data (MinIO, never R2/-dq) for
# each target table's actual year coverage and writes a machine-generated
# `observedCoverage:` block into that table's entry in its schema YAML, immediately after
# its `pattern:` line. This is metadata ONLY: it never touches raw data, Iceberg tables,
# or the tracker — it reads production and writes source YAML.
#
# Deliberately does NOT rewrite a table's hand-authored `comment:` prose — that requires
# editorial judgment (why a gap exists, what it means for callers) that this script can't
# supply. observedCoverage is a compact, always-accurate supplement a human or the
# defect-register-runner can read alongside the prose, not a replacement for it.
#
# Scope: base tables only (the `tables`/`partitionedTables` keys). Views are skipped —
# their coverage is a function of their SQL, not a single partition scan.
#
# Usage:
#   update-coverage-metadata.sh --schema <schema> [--tables <t1,t2,...>] [--dry-run] [--no-commit]
#   update-coverage-metadata.sh --all-schemas [--dry-run] [--no-commit]
#
# With --schema and no --tables, every base table in that schema's YAML that declares a
# `pattern:` containing a `year=` partition segment is checked. --all-schemas does this
# for every schema YAML under src/main/resources.
#
# Commits automatically per schema touched (one commit per schema file actually changed)
# unless --no-commit is given — this mirrors an unattended run-scheduled.sh invocation,
# which has no human present to review a diff before the next cycle.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESOURCES_DIR="$GOVDATA_ROOT/src/main/resources"
PY_EDITOR="$SCRIPT_DIR/update-coverage-metadata.py"

# shellcheck source=/dev/null
source "$SCRIPT_DIR/common.sh"
load_env

SCHEMA=""
TABLES=""
ALL_SCHEMAS=false
DRY_RUN=false
NO_COMMIT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)       SCHEMA="$2";  shift 2 ;;
    --tables)       TABLES="$2";  shift 2 ;;
    --all-schemas)  ALL_SCHEMAS=true; shift ;;
    --dry-run)      DRY_RUN=true; shift ;;
    --no-commit)    NO_COMMIT=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$ALL_SCHEMAS" != true && -z "$SCHEMA" ]]; then
  cat <<EOF >&2
Usage: $0 --schema <schema> [--tables <t1,t2,...>] [--dry-run] [--no-commit]
       $0 --all-schemas [--dry-run] [--no-commit]
EOF
  exit 1
fi

# ── DuckDB / MinIO setup (production bucket, never R2 or -dq — see the
#    govdata-minio-vs-r2-endpoint / verify-govdata-defects-against-minio-not-r2 memories) ──
ENDPOINT="${AWS_ENDPOINT_OVERRIDE#http://}"
ENDPOINT="${ENDPOINT#https://}"
if [[ -z "$ENDPOINT" || -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "ERROR: AWS_ENDPOINT_OVERRIDE/AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY not set after load_env — refusing to silently fall back to R2." >&2
  exit 1
fi
BUCKET="govdata-parquet-v1"

duckdb_setup() {
  cat <<SQL
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE SECRET minio (
  TYPE S3, KEY_ID '${AWS_ACCESS_KEY_ID}', SECRET '${AWS_SECRET_ACCESS_KEY}',
  ENDPOINT '${ENDPOINT}', REGION 'auto', USE_SSL false, URL_STYLE 'path'
);
SET unsafe_enable_version_guessing = true;
SQL
}

# ── Resolve a schema name to its YAML file via the schemaName: key (directory names don't
#    always match the logical schema name, e.g. cyber_vuln lives under resources/cyber/).
#    schemaName is almost always "${SOME_VAR:default}" (a VariableResolver default), so
#    match on the default value, not a literal — plain "edu" is the one exception, and
#    the same regex matches that too. ──
resolve_yaml() {
  local schema="$1"
  local yaml f val
  for f in "$RESOURCES_DIR"/*/*-schema.yaml; do
    val="$(grep -m1 -E '^schemaName:' "$f" | sed -E 's/^schemaName: *"?(\$\{[A-Z_]+:)?([a-zA-Z_]+)\}?"? *$/\2/')"
    if [[ "$val" == "$schema" ]]; then
      echo "$f"
      return 0
    fi
  done
  return 1
}

# ── List base tables in a YAML that declare a year partition ────────────────────────────
tables_with_year_partition() {
  local yaml="$1"
  awk '
    /^  - name: / { name=$3 }
    /^    pattern:.*year=/ { print name }
  ' "$yaml"
}

process_schema() {
  local schema="$1"
  local requested_tables="$2"
  local yaml
  yaml="$(resolve_yaml "$schema")"
  if [[ -z "$yaml" ]]; then
    echo "ERROR: no schema YAML found with schemaName: $schema" >&2
    return 1
  fi

  local -a table_list=()
  if [[ -n "$requested_tables" ]]; then
    IFS=',' read -ra table_list <<< "$requested_tables"
  else
    while IFS= read -r t; do table_list+=("$t"); done < <(tables_with_year_partition "$yaml")
  fi

  echo "=== $schema ($yaml) — ${#table_list[@]} table(s) ==="
  local changed=false

  for tbl in "${table_list[@]}"; do
    tbl="$(echo -n "$tbl" | xargs)"
    [[ -z "$tbl" ]] && continue

    local result duck_err
    duck_err="$(mktemp)"
    # A missing table, or one whose year dimension isn't literally a column named
    # `year` (e.g. keyed by `congress`, or year-as-column-not-partition with a
    # different name), makes duckdb exit non-zero; with `set -o pipefail` that would
    # otherwise abort this whole script via `set -e` on a plain (non-local)
    # assignment — that's an expected, per-table SKIP case, not a fatal error, so
    # explicitly don't let it propagate here. Capture stderr so the SKIP reason is
    # honest about which of those it actually was, instead of always claiming
    # "not materialized" for what might really be a column-name mismatch.
    result="$(duckdb -json -c "
$(duckdb_setup)
SELECT MIN(year) mn, MAX(year) mx, COUNT(DISTINCT year) dc, COUNT(*) n,
       array_agg(DISTINCT year ORDER BY year) yrs
FROM iceberg_scan('s3://${BUCKET}/${schema}/${tbl}', allow_moved_paths=true);
" 2>"$duck_err" | tail -1)" || result=""

    if [[ -z "$result" || "$result" == "[]" ]]; then
      local first_err
      first_err="$(grep -m1 -iE 'error' "$duck_err" || true)"
      rm -f "$duck_err"
      if [[ -n "$first_err" ]]; then
        echo "  SKIP $tbl — query failed against s3://${BUCKET}/${schema}/${tbl}: ${first_err}"
        echo "         (likely no 'year' column — this table may key on something else, e.g. congress/event_year/date; not auto-handled)"
      else
        echo "  SKIP $tbl — not materialized in production (no data at s3://${BUCKET}/${schema}/${tbl})"
      fi
      continue
    fi
    rm -f "$duck_err"

    local mn mx dc n yrs_csv
    mn="$(echo "$result" | python3 -c 'import json,sys; d=json.load(sys.stdin)[0]; print(d["mn"])' 2>/dev/null || true)"
    if [[ -z "$mn" || "$mn" == "None" ]]; then
      echo "  SKIP $tbl — query returned no rows"
      continue
    fi
    if ! [[ "$mn" =~ ^-?[0-9]+$ ]]; then
      echo "  SKIP $tbl — 'year' column holds non-integer values (e.g. \"$mn\"); this table's"
      echo "         year partition is a composite label (e.g. YYYY:QN), not observedCoverage's plain year"
      continue
    fi
    mx="$(echo "$result" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["mx"])')"
    dc="$(echo "$result" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["dc"])')"
    n="$(echo "$result" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["n"])')"
    yrs_csv="$(echo "$result" | python3 -c 'import json,sys; print(",".join(str(y) for y in json.load(sys.stdin)[0]["yrs"]))')"

    local span=$(( mx - mn + 1 ))
    local contiguous="true"
    [[ "$span" -ne "$dc" ]] && contiguous="false"

    local checked_at
    checked_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    local py_args=(--file "$yaml" --table "$tbl" --min-year "$mn" --max-year "$mx"
                    --distinct-years "$dc" --contiguous "$contiguous" --row-count "$n"
                    --checked-at "$checked_at")
    [[ "$contiguous" == "false" ]] && py_args+=(--years "$yrs_csv")
    "$DRY_RUN" && py_args+=(--dry-run)

    set +e
    out="$(python3 "$PY_EDITOR" "${py_args[@]}" 2>&1)"
    rc=$?
    set -e
    case "$rc" in
      0) echo "  $tbl: $out"; [[ "$out" == UPDATED:* ]] && changed=true ;;
      3) echo "  $tbl: no 'pattern:' line — no year partition, skipping" ;;
      *) echo "  ERROR $tbl: $out" >&2 ;;
    esac
  done

  if $changed && ! $DRY_RUN && ! $NO_COMMIT; then
    (cd "$GOVDATA_ROOT/.." && git add "$yaml" && git commit -q -m "chore(govdata): refresh observed-coverage metadata for $schema

Auto-generated by update-coverage-metadata.sh against production (MinIO).")
    echo "  committed: $yaml"
  fi
}

if $ALL_SCHEMAS; then
  for yaml in "$RESOURCES_DIR"/*/*-schema.yaml; do
    s="$(grep -m1 -E '^schemaName:' "$yaml" | sed -E 's/^schemaName: *"?(\$\{[A-Z_]+:)?([a-zA-Z_]+)\}?"? *$/\2/')"
    [[ -z "$s" ]] && continue
    process_schema "$s" ""
  done
else
  process_schema "$SCHEMA" "$TABLES"
fi
