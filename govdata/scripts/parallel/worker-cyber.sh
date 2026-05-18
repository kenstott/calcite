#!/usr/bin/env bash
# Cyber ETL worker — parameterized by MODE.
#
# Usage:
#   worker-cyber.sh <mode>
#
# Modes:
#   initial   One-time setup: full NVD catalog + CWE + KEV + static standards + OTX full backfill.
#             Run once before any recurring cadence workers.
#
#   daily     Delta NVD (last 1 day) + KEV refresh. Schedule: every 24 hours.
#             Requires: CYBER_NVD_API_KEY (recommended for higher rate limits)
#
#   weekly    Refresh weekly-cadence sources: CWE catalog, OSV, MITRE ATT&CK techniques,
#             GitHub security advisories, and ATT&CK→NIST mappings.
#             Schedule: weekly (e.g., Sunday 02:00).
#
#   hourly    Live IOC feeds (URLhaus, MalwareBazaar, Feodo, ThreatFox) + OTX delta (1 day).
#             Schedule: every 1–4 hours.
#             Requires: CYBER_THREATFOX_API_KEY (for ioc_mixed), CYBER_OTX_API_KEY (for threat_pulses)
#
#   static    Re-run static standards only (NIST 800-53, NIST CSF 2.0, CIS Controls,
#             OWASP Top 10, ATT&CK→NIST mappings). Use after a schema update.
#
# Required env vars (set in .env.prod or equivalent):
#   GOVDATA_PARQUET_DIR     Local/S3 path for Parquet output
#   GOVDATA_CACHE_DIR       Local/S3 path for raw download cache
#
# Optional env vars:
#   CYBER_NVD_API_KEY       NVD API key (higher rate limit; 5 req/30s vs 1 req/6s without)
#   CYBER_OTX_API_KEY       AlienVault OTX API key (enables threat_pulses table)
#   CYBER_THREATFOX_API_KEY ThreatFox API key (enables ioc_mixed table)
#   CYBER_GITHUB_TOKEN      GitHub PAT (enables osv_vulnerabilities, github_security_advisories)
#   CYBER_OSV_ECOSYSTEMS    Comma-separated OSV ecosystems (enables osv_vulnerabilities)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <initial|daily|weekly|hourly|static> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-cyber-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

CYBER_VULN_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/cyber/cyber-vuln-schema.yaml"
CYBER_THREAT_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/cyber/cyber-threat-schema.yaml"


# ── helpers ──────────────────────────────────────────────────────────────────

run_cyber_model() {
  local schema=$1 model_name=$2 enabled_tables=$3
  shift 3
  local extra_operands="${1:-}"

  local model_file="$MODEL_DIR/${model_name}.json"
  local extra_json=""
  [ -n "$extra_operands" ] && extra_json=",
      ${extra_operands}"
  local fresh_start_json=""
  [ "${FORCE:-false}" = "true" ] && fresh_start_json=',
      "freshStart": true'

  cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "${schema}",
  "schemas": [{
    "name": "${schema}",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "${schema}",
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "autoDownload": true,
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
      },
      "s3Config": {
        "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
        "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
        "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
      },
      "enabledTables": [${enabled_tables}]${extra_json}${fresh_start_json}
    }
  }]
}
ENDJSON

  log_info "$WORKER_ID: running $model_name"
  run_etl "$model_file" "$WORKER_ID"
}

# ── modes ─────────────────────────────────────────────────────────────────────

case "$MODE" in

  initial)
    # Full NVD catalog + KEV + CWE + junction tables
    run_cyber_model "cyber_vuln" "vuln-full" \
      '"vulnerabilities", "vulnerability_cwes", "kev_catalog", "kev_cwes", "cwe_catalog"'

    # Static standards: fetch once; change only when NIST/CIS/OWASP publish new versions
    run_cyber_model "cyber_threat" "threat-static" \
      '"nist_controls", "nist_csf_functions", "cis_controls", "owasp_top10", "attack_to_nist_mappings"'

    # OTX full backfill: no CYBER_OTX_DELTA_DAYS → retrieves all available pulses
    if [ -n "${CYBER_OTX_API_KEY:-}" ]; then
      unset CYBER_OTX_DELTA_DAYS 2>/dev/null || true
      run_cyber_model "cyber_threat" "threat-otx-initial" '"threat_pulses"'
    else
      log_info "$WORKER_ID: CYBER_OTX_API_KEY not set — skipping threat_pulses initial load"
    fi
    ;;

  daily)
    # NVD delta: last 1 day of modified/published CVEs; also refreshes KEV
    run_cyber_model "cyber_vuln" "vuln-daily" \
      '"vulnerabilities", "kev_catalog"' \
      '"nvdDeltaDays": 1'
    ;;

  weekly)
    # Windows read from each table's releaseWindow in the schema YAML (dow: Sunday).
    # cwe_catalog is representative for the vuln group; attack_techniques for the threat group.
    if table_in_window "$CYBER_VULN_SCHEMA_YAML" "cwe_catalog"; then
      run_cyber_model "cyber_vuln" "vuln-weekly" \
        '"cwe_catalog", "osv_vulnerabilities", "vuln_cross_refs", "advisories"'
    fi

    if table_in_window "$CYBER_THREAT_SCHEMA_YAML" "attack_techniques"; then
      run_cyber_model "cyber_threat" "threat-weekly" \
        '"attack_techniques", "attack_to_nist_mappings"'
    fi
    ;;

  hourly)
    # Live IOC feeds
    run_cyber_model "cyber_threat" "threat-hourly" \
      '"ioc_urls", "ioc_hashes", "ioc_ips", "ioc_mixed", "threat_pulses"'
    ;;

  static)
    # Re-run static standards only (safe to re-run at any time; idempotent)
    run_cyber_model "cyber_threat" "threat-static" \
      '"nist_controls", "nist_csf_functions", "cis_controls", "owasp_top10", "attack_to_nist_mappings"'
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: initial, daily, weekly, hourly, static" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
