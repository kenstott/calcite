#!/usr/bin/env bash
#
# r2-sync-pause.sh — reserve one table against the R2 sync so a remediation can delete on R2
# without stopping the sync service.
#
#   r2-sync-pause.sh reserve <schema> <table> [--ttl SECONDS] [--owner TEXT] [--wait SECONDS]
#   r2-sync-pause.sh release <schema> <table>
#   r2-sync-pause.sh status  <schema> <table>
#
# reserve writes <state>/pause.d/<schema>__<table> (created_at, expires_at; default ttl 8h,
# GOVDATA_R2_PAUSE_TTL) and then waits until no sync pass still has a unit on that table that
# started before the marker (r2_pause_wait_idle, common.sh). It exits 0 only when the table is
# idle: from then on every pass skips it (no copy, no pointer publish) until release or expiry.
# --wait caps the wait (default 4200s, a little over one bulk slice). On a timeout it removes the
# marker again and exits 1: the caller must not delete.
# The marker expires on its own; re-run reserve to extend it. This script never touches R2.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

cmd="${1:?usage: r2-sync-pause.sh reserve|release|status <schema> <table> [--ttl S] [--owner T] [--wait S]}"
schema="${2:?schema required}"
table="${3:?table required}"
shift 3
ttl="${GOVDATA_R2_PAUSE_TTL:-28800}"; owner="${USER:-unknown}"; wait_s=4200
while [ $# -gt 0 ]; do
  case "$1" in
    --ttl) ttl="${2:?}"; shift 2 ;;
    --owner) owner="${2:?}"; shift 2 ;;
    --wait) wait_s="${2:?}"; shift 2 ;;
    *) echo "r2-sync-pause: unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$cmd" in
  reserve)
    r2_pause_create "$schema" "$table" "$ttl" "$owner"
    created=$(r2_pause_created_at "$schema" "$table" 2>/dev/null)
    if r2_pause_wait_idle "$schema" "$table" "$created" "$wait_s"; then
      log_info "r2-pause: [$schema/$table] reserved: no sync pass is working on the table"
    else
      r2_pause_remove "$schema" "$table" >/dev/null
      log_error "r2-pause: [$schema/$table] a sync unit on the table did not end within ${wait_s}s — marker removed, do NOT delete"
      exit 1
    fi ;;
  release) r2_pause_remove "$schema" "$table" ;;
  status)
    if created=$(r2_pause_created_at "$schema" "$table" 2>/dev/null); then echo "paused since $created"; else echo "not paused"; fi ;;
  *) echo "r2-sync-pause: unknown command: $cmd" >&2; exit 2 ;;
esac
