#!/bin/bash
# vc_pg.sh — PostgreSQL compute-layer helpers for ref.vectorized_chunks.
#
# Sits on top of tracker_pg.sh's pg_tracker_exec/pg_ns_from_bucket (same connection, same
# namespace derivation — one PG database, tracker tables and vc_* tables side by side in the
# schema derived from the parquet bucket name) rather than duplicating connection handling.
#
# Schema: see sql/vc_schema.sql for vc_staging / vc_tombstones / vc_sync_state definitions and
# the design rationale (content-addressed keys, tombstone-mediated deletes, no partition
# overwrites).

SCRIPT_DIR_VC_PG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./tracker_pg.sh
source "${SCRIPT_DIR_VC_PG}/tracker_pg.sh"

# Applies vc_schema.sql against namespace $1 (idempotent — CREATE ... IF NOT EXISTS throughout).
# Call this before any other vc_* function touches a namespace for the first time.
vc_init_schema() {
  local ns="$1"
  local url="${GOVDATA_TRACKER_PG_URL:-${CALCITE_TRACKER_PG_URL:-}}"
  if [[ -z "$url" ]]; then
    echo "ERROR: tracker backend is pg but GOVDATA_TRACKER_PG_URL is unset" >&2
    return 1
  fi
  local hostport="${url#*://}"
  local db="${hostport#*/}"
  hostport="${hostport%%/*}"
  local host="${hostport%%:*}"
  local port="${hostport##*:}"
  [[ "$port" == "$host" ]] && port=5432
  PGPASSWORD="${GOVDATA_TRACKER_PG_PASSWORD:-${CALCITE_TRACKER_PG_PASSWORD:-}}" \
    psql -h "$host" -p "$port" \
      -U "${GOVDATA_TRACKER_PG_USER:-${CALCITE_TRACKER_PG_USER:-govdata}}" \
      -d "$db" -v ON_ERROR_STOP=1 -v ns="$ns" -f "${SCRIPT_DIR_VC_PG}/sql/vc_schema.sql"
}

# Counts vc_staging rows matching a scope. $2 is a pre-built SQL WHERE clause (caller's
# responsibility to quote values safely — these scripts are operator-invoked with trusted input,
# same trust model as data_purge.sh's own table/schema arguments).
vc_staging_count() {
  local ns="$1" where="$2"
  pg_tracker_exec "SELECT count(*) FROM \"${ns}\".vc_staging WHERE ${where}"
}

# Moves rows matching $2 (a WHERE clause) from vc_staging into vc_tombstones, then deletes them
# from vc_staging — one transaction, so a crash mid-move never leaves a row in both tables or
# neither. Returns the count moved via stdout (for the caller to report).
vc_tombstone_and_remove() {
  local ns="$1" where="$2" dry_run="$3"
  local n
  n=$(vc_staging_count "$ns" "$where") || return 1
  if [[ "$dry_run" == "true" ]]; then
    echo "$n"
    return 0
  fi
  local now_ms=$(( $(date +%s) * 1000 ))
  pg_tracker_exec "
    BEGIN;
    INSERT INTO \"${ns}\".vc_tombstones
      (source_schema, source_table, stringified_fk, sequence, chunk_id, tombstoned_at)
    SELECT source_schema, source_table, stringified_fk, sequence, chunk_id, ${now_ms}
      FROM \"${ns}\".vc_staging
     WHERE ${where};
    DELETE FROM \"${ns}\".vc_staging WHERE ${where};
    COMMIT;" > /dev/null || return 1
  echo "$n"
}
