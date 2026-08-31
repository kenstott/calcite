#!/bin/bash
# Backs up the Postgres ETL tracker.
#
# Postgres is the canonical record of what the ETL has processed. Losing it does not lose data,
# but it loses the knowledge of which filings are already done — and rebuilding that by inspecting
# object storage costs a full recursive listing of every year partition (hundreds of thousands of
# objects). This backup is what makes Postgres safe to depend on, so it replaces the
# rebuild-from-storage path rather than merely complementing it.
#
# Dumps each tracker schema separately in custom format (-Fc): compressed, and restorable
# individually with pg_restore, so one schema can be recovered without touching the others.
#
# Usage:
#   tracker-backup.sh [--dir <path>] [--keep <n>] [--verify]
#     --dir     destination directory (default: /var/tmp/govdata-tracker-backups)
#     --keep    how many dated backups to retain per schema (default: 7)
#     --verify  after dumping, list the archive's contents to prove it is readable
#
# Restore a schema:
#   pg_restore -h localhost -U govdata -d govdata --clean --if-exists \
#     -n govdata_parquet_v1 <backup file>

set -euo pipefail

# cron invokes this with a minimal PATH (/usr/bin:/bin) that omits ~/.local/bin, where rclone is
# actually installed here -- confirmed live 2026-08-31: every night's upload step was silently
# failing with "rclone: command not found", leaving every backup local-disk-only despite the
# "verified" log line (verification only checks pg_restore --list, not the upload). Prepending
# common user-install locations makes the script correct regardless of invoker (interactive shell
# or cron), rather than depending on a crontab PATH= line that isn't version-controlled.
export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"

BACKUP_DIR="/var/tmp/govdata-tracker-backups"
KEEP=7
VERIFY=false
# Push to MinIO under its own top-level prefix of the parquet bucket. sync-to-r2.sh replicates
# every top-level prefix it finds there, so landing the dump in one means the existing R2 job
# carries it offsite — no second upload path to configure or keep working.
UPLOAD_DEST="${GOVDATA_TRACKER_BACKUP_DEST:-minio:govdata-parquet-v1/_tracker_backups}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)      BACKUP_DIR="$2"; shift 2 ;;
    --keep)     KEEP="$2"; shift 2 ;;
    --verify)   VERIFY=true; shift ;;
    --upload)   UPLOAD_DEST="$2"; shift 2 ;;
    --no-upload) UPLOAD_DEST=""; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/../.env.prod" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/../.env.prod"
  set +a
fi

URL="${GOVDATA_TRACKER_PG_URL:-${CALCITE_TRACKER_PG_URL:-}}"
if [[ -z "$URL" ]]; then
  echo "ERROR: GOVDATA_TRACKER_PG_URL is unset — cannot locate the tracker database" >&2
  exit 2
fi

# jdbc:postgresql://host:port/db -> host, port, db
HOSTPORT="${URL#*://}"
DB="${HOSTPORT#*/}"
HOSTPORT="${HOSTPORT%%/*}"
HOST="${HOSTPORT%%:*}"
PORT="${HOSTPORT##*:}"
[[ "$PORT" == "$HOST" ]] && PORT=5432
USER="${GOVDATA_TRACKER_PG_USER:-${CALCITE_TRACKER_PG_USER:-govdata}}"
export PGPASSWORD="${GOVDATA_TRACKER_PG_PASSWORD:-${CALCITE_TRACKER_PG_PASSWORD:-}}"

mkdir -p "$BACKUP_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"

# Back up every tracker schema present, so a schema added later is not silently missed.
mapfile -t SCHEMAS < <(psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -At \
  -c "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'govdata%' ORDER BY nspname")

if [[ ${#SCHEMAS[@]} -eq 0 ]]; then
  echo "ERROR: no govdata* schemas found in ${DB} — refusing to report success on an empty backup" >&2
  exit 1
fi

echo "Backing up ${#SCHEMAS[@]} tracker schema(s) from ${HOST}:${PORT}/${DB} to ${BACKUP_DIR}"
for schema in "${SCHEMAS[@]}"; do
  out="${BACKUP_DIR}/${schema}_${STAMP}.dump"
  pg_dump -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -n "$schema" -Fc -f "$out"

  # A dump that exists but is empty or truncated is worse than none, because it reads as safety.
  size=$(stat -c%s "$out")
  if [[ "$size" -lt 1024 ]]; then
    echo "ERROR: ${out} is only ${size} bytes — treating as a failed backup" >&2
    exit 1
  fi
  echo "  ${schema}: $(numfmt --to=iec "$size")  ->  $(basename "$out")"

  if $VERIFY; then
    tables=$(pg_restore --list "$out" | grep -c "TABLE DATA" || true)
    if [[ "$tables" -eq 0 ]]; then
      echo "ERROR: ${out} contains no table data — refusing to call this a backup" >&2
      exit 1
    fi
    echo "    verified: ${tables} table(s) of data readable by pg_restore"
  fi

  # Retention is per schema so one schema's churn cannot age out another's backups.
  mapfile -t old < <(ls -1t "${BACKUP_DIR}/${schema}"_*.dump 2>/dev/null | tail -n +$((KEEP + 1)))
  for f in "${old[@]:-}"; do
    [[ -n "$f" ]] && rm -f "$f" && echo "    pruned $(basename "$f")"
  done
done

if [[ -n "$UPLOAD_DEST" ]]; then
  # A backup on the same disk as the database survives a bad DROP but not a lost host, so the
  # copy is the point rather than an extra. rclone remotes come from the existing config:
  # "minio:" is this host (protects the data, not the machine), "r2:" is offsite.
  echo "Uploading to ${UPLOAD_DEST}"
  for schema in "${SCHEMAS[@]}"; do
    src="${BACKUP_DIR}/${schema}_${STAMP}.dump"
    if ! rclone copyto "$src" "${UPLOAD_DEST%/}/${schema}_${STAMP}.dump" --no-traverse; then
      echo "ERROR: upload of $(basename "$src") to ${UPLOAD_DEST} failed — the local copy is" >&2
      echo "       still at ${src}, but this run is NOT a durable backup." >&2
      exit 1
    fi
    # Prove it landed: an upload that silently wrote nothing reads as safety it does not provide.
    remote_size=$(rclone size --json "${UPLOAD_DEST%/}/${schema}_${STAMP}.dump" 2>/dev/null \
      | grep -oE '"bytes":[0-9]+' | cut -d: -f2)
    local_size=$(stat -c%s "$src")
    if [[ "${remote_size:-0}" != "$local_size" ]]; then
      echo "ERROR: ${schema} uploaded as ${remote_size:-0} bytes, expected ${local_size}" >&2
      exit 1
    fi
    echo "  ${schema}: $(numfmt --to=iec "$local_size") confirmed at ${UPLOAD_DEST%/}/${schema}_${STAMP}.dump"
  done
fi

echo "Backup complete: ${BACKUP_DIR}"
if [[ -z "$UPLOAD_DEST" ]]; then
  echo "NOTE: local disk only (--no-upload). Nothing will carry this off-box."
fi
