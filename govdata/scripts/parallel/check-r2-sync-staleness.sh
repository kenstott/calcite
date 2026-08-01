#!/usr/bin/env bash
#
# check-r2-sync-staleness.sh — alert when a schema's R2 sync sentinel has drifted too
# far behind, instead of silently drifting (the failure that let sec sit 4 days stale
# with nothing surfacing it — see catchup-sync-r2.sh's header for the full incident).
#
# Reads every ~/.r2-sync-state/<schema> sentinel (an epoch — see sync-to-r2.sh) and
# flags any schema whose sentinel is older than the threshold. A schema legitimately
# held for a long single ETL run (backfill years, a reprocess chain) is expected to
# lag by hours; the default threshold (48h) is set above normal run lengths so it
# only fires on the actual starvation pattern, not routine held-while-active state.
#
# Usage:
#   govdata/scripts/parallel/check-r2-sync-staleness.sh [--threshold-hours N] [--quiet]
#
# Exit code: 0 if every schema is within threshold, 1 if any schema is stale or has
# no sentinel at all (a schema with data but no sentinel has never synced — also worth
# flagging, not just silently skipping). Intended for cron / dq-report / monitoring,
# not a blocking gate on any ETL path.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

THRESHOLD_HOURS="${GOVDATA_R2_STALENESS_ALERT_HOURS:-48}"
QUIET=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --threshold-hours) shift; THRESHOLD_HOURS="${1:?--threshold-hours requires a number}" ;;
    --quiet) QUIET=true ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
  shift
done

STATE_DIR="${HOME}/.r2-sync-state"
MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
BUCKET="govdata-parquet-v1"
_threshold_secs=$(( THRESHOLD_HOURS * 3600 ))
_now=$(date +%s)

# Cross-check against the schemas that actually exist in MinIO, not just the sentinels
# on disk — a schema with real data but NO sentinel file has never been synced at all,
# which is strictly worse than "stale" and must not be silently skipped.
mapfile -t _schemas < <(rclone lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}" 2>/dev/null | sed 's#/*$##')
if [ "${#_schemas[@]}" -eq 0 ]; then
  log_error "check-r2-sync-staleness: FATAL — no schemas (top-level prefixes) found under ${BUCKET}"
  exit 1
fi

_stale=0
for s in "${_schemas[@]}"; do
  _sf="$STATE_DIR/$s"
  if [ ! -f "$_sf" ]; then
    log_error "check-r2-sync-staleness: [$s] NEVER SYNCED — no sentinel at $_sf"
    _stale=$((_stale + 1))
    continue
  fi
  _v=$(cat "$_sf" 2>/dev/null)
  if ! [[ "$_v" =~ ^[0-9]+$ ]]; then
    log_error "check-r2-sync-staleness: [$s] CORRUPT sentinel ($_sf = '$_v')"
    _stale=$((_stale + 1))
    continue
  fi
  _age=$(( _now - _v ))
  if [ "$_age" -gt "$_threshold_secs" ]; then
    _age_h=$(( _age / 3600 ))
    log_error "check-r2-sync-staleness: [$s] STALE — synced through $(date -u -d "@$_v" +%Y-%m-%dT%H:%MZ), ${_age_h}h ago (threshold ${THRESHOLD_HOURS}h)"
    _stale=$((_stale + 1))
  elif ! $QUIET; then
    log_info "check-r2-sync-staleness: [$s] OK — synced through $(date -u -d "@$_v" +%Y-%m-%dT%H:%MZ)"
  fi
done

if [ "$_stale" -gt 0 ]; then
  log_error "check-r2-sync-staleness: ${_stale}/${#_schemas[@]} schema(s) stale or unsynced — run catchup-sync-r2.sh to recover"
  exit 1
fi

$QUIET || log_info "check-r2-sync-staleness: all ${#_schemas[@]} schema(s) within ${THRESHOLD_HOURS}h"
