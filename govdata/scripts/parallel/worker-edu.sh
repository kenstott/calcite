#!/usr/bin/env bash
# Education ETL worker — parameterized by MODE.
#
# Usage:
#   worker-edu.sh <mode> [--force]
#
# Modes:
#   initial   One-time setup: all 10 edu tables, full historical year range.
#             Run once before any recurring cadence workers.
#             K-12 history back to 1987 (CCD), higher-ed to 1986 (IPEDS).
#             Release-window checks are skipped — initial always runs in full.
#
#   annual    Annual cadence: CCD districts/schools, IPEDS (institutions,
#             completions, financials, tuition), and College Scorecard.
#             Each sub-run is individually gated to its known release window:
#               CCD districts/schools        — months 7–8   (July, August)
#               College Scorecard            — month  10    (October)
#               IPEDS institutions/compl/tui — month  11    (November)
#               IPEDS financials             — month   1    (January)
#             Sub-runs outside their window are skipped instantly; others proceed.
#             Pass --force to bypass all window checks (useful for backfill/testing).
#             Optional: EDU_CCD_SINCE_YEAR, EDU_IPEDS_SINCE_YEAR,
#                       EDU_IPEDS_FINANCE_SINCE_YEAR, EDU_SCORECARD_SINCE_YEAR
#
#   biennial  Biennial cadence: NAEP assessments and CRDC civil rights data.
#             Each sub-run is gated to its release window:
#               NAEP assessments             — months 1–3   (Jan–Mar; odd years only)
#               CRDC civil rights data       — months 10–12 (Oct–Dec; even years only)
#             Safe to run every year — sub-runs outside their window skip instantly.
#             Pass --force to bypass all window checks.
#             Optional: EDU_NAEP_SINCE_YEAR, EDU_CRDC_SINCE_YEAR
#
# Required env vars (set in .env.prod or equivalent):
#   EDU_PARQUET_DIR         Local/S3 path for Parquet output
#                           (falls back to ${GOVDATA_PARQUET_DIR}/edu)
#   EDU_CACHE_DIR           Local/S3 path for raw download cache
#                           (falls back to ${GOVDATA_CACHE_DIR}/edu)
#
# Optional env vars:
#   EDU_CCD_SINCE_YEAR           4-digit year — load CCD districts/schools from this year
#   EDU_NAEP_SINCE_YEAR          4-digit year — load NAEP scores from this year
#   EDU_CRDC_SINCE_YEAR          4-digit year — load CRDC civil rights data from this year
#   EDU_IPEDS_SINCE_YEAR         4-digit year — load IPEDS directory/completions/tuition from this year
#   EDU_IPEDS_FINANCE_SINCE_YEAR 4-digit year — load IPEDS financials from this year
#   EDU_SCORECARD_SINCE_YEAR     4-digit year — load College Scorecard tables from this year
#   API_DATA_GOV                 api.data.gov key (required for College Scorecard tables)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <initial|annual|biennial> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-edu-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

EDU_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/edu/edu-schema.yaml"

# ── helpers ──────────────────────────────────────────────────────────────────
# table_in_window reads releaseWindow: from edu-schema.yaml via check-release-window.py

run_edu_model() {
  local model_name=$1 enabled_tables=$2 start_year=$3 end_year=${4:-}

  local model_file="$MODEL_DIR/${model_name}.json"
  local parquet_dir="${EDU_PARQUET_DIR:-${GOVDATA_PARQUET_DIR}}"
  local cache_dir="${EDU_CACHE_DIR:-${GOVDATA_CACHE_DIR}/edu}"
  local end_year_json=""
  [ -n "$end_year" ] && end_year_json=",
      \"endYear\": ${end_year}"

  cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "edu",
  "schemas": [{
    "name": "edu",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "edu",
      "directory": "${parquet_dir}",
      "cacheDirectory": "${cache_dir}",
      "autoDownload": true,
      "startYear": ${start_year}${end_year_json},
      "enabledTables": [${enabled_tables}],
      "s3Config": {
        "accessKeyId": "${AWS_ACCESS_KEY_ID:-}",
        "secretAccessKey": "${AWS_SECRET_ACCESS_KEY:-}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE:-}",
        "region": "${AWS_REGION:-us-east-1}"
      }
    }
  }]
}
ENDJSON

  log_info "$WORKER_ID: running $model_name"
  run_etl "$model_file" "$WORKER_ID"
}

# ── modes ─────────────────────────────────────────────────────────────────────

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-2026}

case "$MODE" in

  initial)
    START=${GOVDATA_START_YEAR:-2010}
    END=$((INCREMENTAL_YEAR - 1))
    # No release-window checks — initial always runs the full historical load.

    run_edu_model "edu-initial-k12" \
      '"ccd_districts", "ccd_schools"' "$START" "$END"

    run_edu_model "edu-initial-assessments" \
      '"naep_scores", "naep_achievement_levels", "crdc_schools"' "$START" "$END"

    run_edu_model "edu-initial-ipeds" \
      '"ipeds_institutions", "ipeds_completions", "ipeds_financials", "ipeds_tuition"' "$START" "$END"

    if [ -n "${API_DATA_GOV:-}" ]; then
      run_edu_model "edu-initial-scorecard" \
        '"college_scorecard", "college_scorecard_programs"' "$START" "$END"
    else
      log_info "$WORKER_ID: API_DATA_GOV not set — skipping college_scorecard tables"
    fi
    ;;

  annual)
    START=$INCREMENTAL_YEAR
    export GOVDATA_SINCE_YEAR="${INCREMENTAL_YEAR}"
    # Each sub-run is gated independently — unrelated sources don't block each other.

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "ccd_districts"; then
      run_edu_model "edu-annual-k12" \
        '"ccd_districts", "ccd_schools"' "$START"
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "college_scorecard"; then
      if [ -n "${API_DATA_GOV:-}" ]; then
        run_edu_model "edu-annual-scorecard" \
          '"college_scorecard", "college_scorecard_programs"' "$START"
      else
        log_info "$WORKER_ID: API_DATA_GOV not set — skipping college_scorecard tables"
      fi
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "ipeds_institutions"; then
      run_edu_model "edu-annual-ipeds-core" \
        '"ipeds_institutions", "ipeds_completions", "ipeds_tuition"' "$START"
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "ipeds_financials"; then
      run_edu_model "edu-annual-ipeds-finance" \
        '"ipeds_financials"' "$START"
    fi
    ;;

  biennial)
    START=$INCREMENTAL_YEAR
    export GOVDATA_SINCE_YEAR="${INCREMENTAL_YEAR}"
    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "naep_scores"; then
      run_edu_model "edu-biennial-naep" \
        '"naep_scores", "naep_achievement_levels"' "$START"
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "crdc_schools"; then
      run_edu_model "edu-biennial-crdc" \
        '"crdc_schools"' "$START"
    fi
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: initial, annual, biennial" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
