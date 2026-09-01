#!/usr/bin/env bash
#
# vc_remove.sh — remove one scope's contribution from the ref.vectorized_chunks PG compute
# layer (vc_staging), for ETL development retries: "I want to reingest this table / this SEC
# accession / this year, drop what's already staged for it first."
#
# This is NOT a replacement for data_purge.sh. data_purge.sh deletes a whole Iceberg table plus
# its tracker rows. vc_remove.sh operates one layer up, on the PG staging table that FEEDS
# ref.vectorized_chunks — it never touches Iceberg/MinIO directly. Removed rows move into
# vc_tombstones; the (separate, not-yet-built) sync step is what turns a tombstone into an
# Iceberg equality-delete on the next run. One mechanism for "a chunk leaves Iceberg" — whether
# triggered by a content-hash-changed re-chunk or by this explicit call — never a partition
# overwrite. See sql/vc_schema.sql for the full design rationale.
#
# Why deletion from vc_staging is enough to force a re-chunk, with no separate "--force" delete
# path needed: the compute step's only way to know "skip this parent, nothing changed" is by
# finding an existing vc_staging row whose parent_hash matches. Remove the row and there is
# nothing left to compare against, so the next compute pass always re-chunks that parent from
# scratch — regardless of whether the SOURCE text actually changed. --force below is therefore
# not about bypassing a hash check (there is none to bypass); it is a guardrail against an
# accidentally-too-broad scope (see "Scope" below).
#
# Usage:
#   vc_remove.sh --source-schema <schema> [--source-table <table>] \
#                [--accession <cik>:<accession_number>] [--year <N>] \
#                [--env prod|dq] [--dry-run] [--force]
#
# Scope:
#   --source-schema only           Every staged chunk from that schema, any table. Broadest
#                                   scope; requires --force (a typo here is a full-schema wipe).
#   + --source-table                Narrow to one source table (e.g. cyber_threat.nist_controls)
#                                   without touching any other table sharing that schema.
#   + --accession <cik>:<accno>     SEC only. Narrow to one filing's stringified_fk. Also clears
#                                   that accession's 'chunks' completion marker in pipeline_tracker
#                                   (reusing tracker_pg.sh's pg_tracker_clear_accession) so
#                                   SecFilingCache actually reprocesses it instead of skipping it
#                                   as already-done.
#   + --year <N>                    Narrow to one partition year. Combinable with --source-table
#                                   and/or --accession.
#
# At least one of --source-table / --accession / --year must narrow the scope, unless --force is
# given (explicit acknowledgement of a full-schema removal).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

SOURCE_SCHEMA=""
SOURCE_TABLE=""
ACCESSION=""
YEAR=""
ENV_NAME="prod"
DRY_RUN=false
FORCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-schema) SOURCE_SCHEMA="$2"; shift 2 ;;
    --source-table)  SOURCE_TABLE="$2";  shift 2 ;;
    --accession)     ACCESSION="$2";     shift 2 ;;
    --year)          YEAR="$2";          shift 2 ;;
    --env)           ENV_NAME="$2";      shift 2 ;;
    --dry-run)       DRY_RUN=true;       shift   ;;
    --force)         FORCE=true;         shift   ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$SOURCE_SCHEMA" ]]; then
  echo "Usage: vc_remove.sh --source-schema <schema> [--source-table <table>] [--accession <cik>:<accno>] [--year <N>] [--env prod|dq] [--dry-run] [--force]" >&2
  exit 1
fi

if [[ -z "$SOURCE_TABLE" && -z "$ACCESSION" && -z "$YEAR" && "$FORCE" != "true" ]]; then
  echo "Error: scope is --source-schema only (every table) — pass --force to acknowledge a full-schema removal," >&2
  echo "       or narrow with --source-table / --accession / --year." >&2
  exit 1
fi

if [[ -n "$ACCESSION" && "$ACCESSION" != *:* ]]; then
  echo "Error: --accession must be '<cik>:<accession_number>' (matches stringified_fk exactly)." >&2
  exit 1
fi

if [[ "$ENV_NAME" != "prod" && "$ENV_NAME" != "dq" ]]; then
  echo "Error: --env must be 'prod' or 'dq' (got '$ENV_NAME')" >&2
  exit 1
fi

# ── Load environment (same convention as data_purge.sh) ──────────────────────
if [[ -f "$GOVDATA_HOME/.env.prod" ]]; then
  set -a; source "$GOVDATA_HOME/.env.prod"; set +a
fi
if [[ "$ENV_NAME" == "dq" ]]; then
  if [[ ! -f "$GOVDATA_HOME/.env.dq" ]]; then
    echo "Error: --env dq requested but $GOVDATA_HOME/.env.dq not found" >&2
    exit 1
  fi
  set -a; source "$GOVDATA_HOME/.env.dq"; set +a
fi

if [[ "$ENV_NAME" == "dq" ]]; then
  : "${GOVDATA_DQ_BUCKET:?GOVDATA_DQ_BUCKET not set (check .env.dq)}"
  PARQUET_BUCKET="s3://${GOVDATA_DQ_BUCKET}"
else
  PARQUET_BUCKET="${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}"
fi

# shellcheck source=./vc_pg.sh
source "$SCRIPT_DIR/vc_pg.sh"
NS="$(pg_ns_from_bucket "$PARQUET_BUCKET")" || exit 2
vc_init_schema "$NS" > /dev/null

# ── Build the WHERE clause for this scope ─────────────────────────────────────
WHERE="source_schema = '${SOURCE_SCHEMA}'"
[[ -n "$SOURCE_TABLE" ]] && WHERE="${WHERE} AND source_table = '${SOURCE_TABLE}'"
[[ -n "$ACCESSION" ]]    && WHERE="${WHERE} AND stringified_fk = '${ACCESSION}'"
[[ -n "$YEAR" ]]         && WHERE="${WHERE} AND year = ${YEAR}"

echo "=================================================="
echo "vc_remove.sh"
echo "=================================================="
echo "  Env:        $ENV_NAME"
echo "  PG schema:  $NS"
echo "  Scope:      $WHERE"
$DRY_RUN && echo "  Mode:       DRY RUN"

N=$(vc_tombstone_and_remove "$NS" "$WHERE" "$DRY_RUN")
if [[ "$DRY_RUN" == "true" ]]; then
  echo "  [DRY RUN] Would move ${N} vc_staging row(s) to vc_tombstones"
else
  echo "  Moved ${N} vc_staging row(s) to vc_tombstones (pending Iceberg sync)"
fi

# SEC accession scope also clears the 'chunks' per-accession completion marker so
# SecFilingCache/DocumentETLProcessor actually reprocesses the filing instead of skipping it.
if [[ -n "$ACCESSION" && "$SOURCE_SCHEMA" == "sec" ]]; then
  ACCNO="${ACCESSION#*:}"
  echo ""
  echo "── Clearing SEC 'chunks' completion marker for accession ${ACCNO} ──"
  pg_tracker_clear_accession "$NS" "'chunks'" "$ACCNO" "$DRY_RUN" "$YEAR"
fi

echo ""
echo "=================================================="
echo "vc_remove.sh complete"
echo "=================================================="
