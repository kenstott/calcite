#!/usr/bin/env bash
#
# restore-tracker-db.sh — rebuild the Postgres ETL tracker cluster on the NVMe-backed
# WSL root filesystem (not the 16TB MinIO drive) and restore its schemas from the
# latest tracker-backup.sh dumps in R2.
#
# Context: cluster "18 main"'s data_directory was pointed at
# /var/lib/postgresql-new/postgresql-data, a mountpoint for a partition on the Seagate
# 16TB disk that died (the same disk MinIO was on). postgresql@18-main.service has
# been failing to start on every boot since 2026-09-17 because that path no longer
# exists — confirmed via journalctl. That data is gone; tracker-backup.sh's nightly
# dumps to R2 (govdata-parquet-v1/_tracker_backups/) are the only remaining copy, and
# the newest ones there are from 2026-09-17 (the morning it died — no dump has
# succeeded since, since tracker-backup.sh needs a live DB to dump FROM).
#
# This script:
#   1. Comments out the dead fstab line (backs up fstab first).
#   2. Points the cluster's data_directory at /var/lib/postgresql/18/main — the
#      standard Debian location, which lives on the NVMe-backed WSL root disk, not
#      the 16TB HDD. That's the whole point: MinIO's heavy I/O and the tracker DB no
#      longer compete for the same physical disk.
#   3. initdb's that directory if it isn't already a valid cluster.
#   4. Starts postgresql@18-main and (idempotently) creates the govdata role/database.
#   5. Downloads the newest dump per schema from R2 and pg_restores each with
#      -Fc --clean --if-exists -n <schema>, mirroring tracker-backup.sh's own
#      documented restore command.
#
# Never touches pg_hba.conf — the existing one already authenticates the govdata
# role correctly (workers have connected against this cluster before); only the data
# location changes here.
#
# Usage (needs sudo):
#   ./restore-tracker-db.sh
#
set -euo pipefail

# sudo's secure_path resets PATH on the root transition, and $HOME is not reliable
# across that boundary either (observed: became /root, so $HOME/.local/bin missed
# the real rclone install at /home/kstott/.local/bin) -- hardcode the real user's
# path rather than derive it.
for _cand in /home/kstott/.local/bin "$HOME/.local/bin"; do
  [ -d "$_cand" ] && [[ ":$PATH:" != *":$_cand:"* ]] && PATH="$_cand:$PATH"
done
export PATH

# Same problem, different symptom: rclone's config (the 'minio'/'r2' remotes) lives at
# /home/kstott/.config/rclone/rclone.conf, but root's $HOME is /root, so rclone silently
# can't see either remote unless pointed at it explicitly.
[ -f /home/kstott/.config/rclone/rclone.conf ] && export RCLONE_CONFIG=/home/kstott/.config/rclone/rclone.conf

PG_VERSION="18"
PG_CLUSTER="main"
PG_BIN="/usr/lib/postgresql/${PG_VERSION}/bin"
PG_CONF_DIR="/etc/postgresql/${PG_VERSION}/${PG_CLUSTER}"
PG_CONF_FILE="${PG_CONF_DIR}/postgresql.conf"
NEW_DATA_DIR="/var/lib/postgresql/${PG_VERSION}/${PG_CLUSTER}"
OLD_DATA_DIR="/var/lib/postgresql-new/postgresql-data"
ENV_PROD="/home/kstott/calcite/govdata/.env.prod"
R2_PREFIX="r2:govdata-parquet-v1/_tracker_backups"
# /var/tmp, not /tmp: /tmp is RAM-backed tmpfs here and dump downloads across
# 7 schemas repeated over several retries filled it to 91% (confirmed live,
# 2026-09-22) -- /var/tmp is disk-backed (and, once setup-wsl-tmp-drive.sh's
# bind-mount is in place, lands on the isolated 8TB-ish drive, not the tmpfs
# or the WSL root disk).
DUMP_TMPDIR="$(mktemp -d /var/tmp/tracker-restore.XXXXXX)"
trap 'rm -rf "$DUMP_TMPDIR"' EXIT

log() { printf '[restore-tracker-db] %s\n' "$*"; }
die() { printf '[restore-tracker-db] ERROR: %s\n' "$*" >&2; exit 1; }

# ---- must be root -----------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
  log "re-exec under sudo…"
  exec sudo -E bash "$0" "$@"
fi

[ -d "$PG_CONF_DIR" ] || die "cluster config $PG_CONF_DIR not found — expected an existing 'postgresql $PG_VERSION $PG_CLUSTER' cluster"
[ -x "$PG_BIN/initdb" ] || die "$PG_BIN/initdb not found — is postgresql-$PG_VERSION installed?"

# ---- fstab: retire the dead entry -------------------------------------------
if grep -q '^/dev/sdd2\s\+/var/lib/postgresql-new' /etc/fstab; then
  FSTAB_BAK="/etc/fstab.bak.$(date +%Y%m%d%H%M%S)"
  cp /etc/fstab "$FSTAB_BAK"
  log "backed up /etc/fstab to $FSTAB_BAK"
  sed -i 's|^/dev/sdd2\s\+/var/lib/postgresql-new.*|# retired by restore-tracker-db.sh: partition was on the dead Seagate disk|' /etc/fstab
  log "commented out the dead /var/lib/postgresql-new fstab line"
else
  log "no dead postgresql-new fstab line found — skipping"
fi

# ---- initialize the cluster on its new (NVMe) location if needed ------------
if [ -f "${NEW_DATA_DIR}/PG_VERSION" ]; then
  log "${NEW_DATA_DIR} already has a valid cluster — skipping initdb"
else
  log "initializing a fresh cluster at ${NEW_DATA_DIR}"
  mkdir -p "$NEW_DATA_DIR"
  chown postgres:postgres "$NEW_DATA_DIR"
  chmod 700 "$NEW_DATA_DIR"
  sudo -u postgres "$PG_BIN/initdb" -D "$NEW_DATA_DIR" --encoding=UTF8
fi
chown -R postgres:postgres "$NEW_DATA_DIR"
chmod 700 "$NEW_DATA_DIR"

# ---- point the cluster at it --------------------------------------------------
if grep -q "^data_directory = '${OLD_DATA_DIR}'" "$PG_CONF_FILE"; then
  sed -i "s|^data_directory = '${OLD_DATA_DIR}'.*|data_directory = '${NEW_DATA_DIR}'\t\t# moved off the dead Seagate disk onto the NVMe (WSL root) disk|" "$PG_CONF_FILE"
  log "updated data_directory in $PG_CONF_FILE"
elif grep -q "^data_directory = '${NEW_DATA_DIR}'" "$PG_CONF_FILE"; then
  log "data_directory already points at ${NEW_DATA_DIR}"
else
  die "unexpected data_directory value in $PG_CONF_FILE — inspect and fix by hand before re-running"
fi

# ---- start it -------------------------------------------------------------------
systemctl daemon-reload
systemctl restart "postgresql@${PG_VERSION}-${PG_CLUSTER}"
sleep 2
systemctl is-active --quiet "postgresql@${PG_VERSION}-${PG_CLUSTER}" \
  || die "postgresql@${PG_VERSION}-${PG_CLUSTER} did not come up — check: journalctl -u postgresql@${PG_VERSION}-${PG_CLUSTER} -n 40"
log "cluster is up"
pg_lsclusters

# ---- role + database (idempotent) ------------------------------------------------
[ -f "$ENV_PROD" ] || die "$ENV_PROD not found — can't read tracker DB credentials"
set -a
# shellcheck disable=SC1090
source <(tr -d '\r' < "$ENV_PROD")
set +a
URL="${GOVDATA_TRACKER_PG_URL:-${CALCITE_TRACKER_PG_URL:-}}"
[ -n "$URL" ] || die "GOVDATA_TRACKER_PG_URL not set in .env.prod"
HOSTPORT="${URL#*://}"
DB="${HOSTPORT#*/}"
HOSTPORT="${HOSTPORT%%/*}"
HOST="${HOSTPORT%%:*}"
PORT="${HOSTPORT##*:}"
[ "$PORT" = "$HOST" ] && PORT=5432
PGUSER_TRACKER="${GOVDATA_TRACKER_PG_USER:-${CALCITE_TRACKER_PG_USER:-govdata}}"
PGPASSWORD_TRACKER="${GOVDATA_TRACKER_PG_PASSWORD:-${CALCITE_TRACKER_PG_PASSWORD:-}}"
[ -n "$PGPASSWORD_TRACKER" ] || die "GOVDATA_TRACKER_PG_PASSWORD not set in .env.prod"

log "ensuring role '${PGUSER_TRACKER}' and database '${DB}' exist"
sudo -u postgres psql -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${PGUSER_TRACKER}') THEN
    CREATE ROLE ${PGUSER_TRACKER} LOGIN PASSWORD '${PGPASSWORD_TRACKER}';
  ELSE
    ALTER ROLE ${PGUSER_TRACKER} WITH LOGIN PASSWORD '${PGPASSWORD_TRACKER}';
  END IF;
END
\$\$;
SQL
sudo -u postgres psql -v ON_ERROR_STOP=1 -tAc "SELECT 1 FROM pg_database WHERE datname = '${DB}'" | grep -q 1 \
  || sudo -u postgres createdb -O "$PGUSER_TRACKER" "$DB"

# ---- download the newest dump per schema from R2 -----------------------------------
command -v rclone >/dev/null || die "rclone not found on PATH"
log "listing dumps under ${R2_PREFIX}"
mapfile -t DUMP_FILES < <(rclone lsf "$R2_PREFIX")
[ "${#DUMP_FILES[@]}" -gt 0 ] || die "no dumps found under ${R2_PREFIX}"

declare -A LATEST_FILE LATEST_STAMP
for f in "${DUMP_FILES[@]}"; do
  [[ "$f" =~ ^(.+)_([0-9]{8}_[0-9]{6})\.dump$ ]] || { log "skipping unrecognized file: $f"; continue; }
  schema="${BASH_REMATCH[1]}"
  stamp="${BASH_REMATCH[2]}"
  if [ -z "${LATEST_STAMP[$schema]:-}" ] || [[ "$stamp" > "${LATEST_STAMP[$schema]}" ]]; then
    LATEST_STAMP[$schema]="$stamp"
    LATEST_FILE[$schema]="$f"
  fi
done
[ "${#LATEST_FILE[@]}" -gt 0 ] || die "no dump files matched the expected <schema>_<stamp>.dump pattern"

log "restoring ${#LATEST_FILE[@]} schema(s): ${!LATEST_FILE[*]}"
export PGPASSWORD="$PGPASSWORD_TRACKER"
for schema in "${!LATEST_FILE[@]}"; do
  file="${LATEST_FILE[$schema]}"
  local_path="${DUMP_TMPDIR}/${file}"
  log "downloading ${file}"
  rclone copyto "${R2_PREFIX}/${file}" "$local_path" --no-traverse \
    || die "download of ${file} failed"

  # pg_restore -n <schema> filters to that schema's objects but does not itself
  # create the schema when restoring into a database where it doesn't already
  # exist (confirmed live: every CREATE TABLE failed with "schema does not exist"
  # and no CREATE SCHEMA statement ran) -- create it explicitly first.
  #
  # DROP + CREATE, not CREATE IF NOT EXISTS: govdata-scheduled.service kept running
  # historical jobs through the whole outage, and the moment this cluster came back
  # up one of them auto-bootstrapped govdata_parquet_v1's schema/tables itself,
  # which then collided with pg_restore --clean (confirmed live: CREATE TABLE
  # succeeded via --clean's own drop/recreate, but CREATE INDEX still hit "already
  # exists" -- --clean did not fully clean house). An explicit DROP CASCADE first
  # guarantees a clean slate no matter what state a stray worker left behind.
  psql -h "$HOST" -p "$PORT" -U "$PGUSER_TRACKER" -d "$DB" -v ON_ERROR_STOP=1 -q \
    -c "DROP SCHEMA IF EXISTS \"${schema}\" CASCADE; CREATE SCHEMA \"${schema}\" AUTHORIZATION ${PGUSER_TRACKER};" \
    || die "could not (re)create schema ${schema}"

  log "restoring schema '${schema}' from ${file}"
  pg_restore -h "$HOST" -p "$PORT" -U "$PGUSER_TRACKER" -d "$DB" \
    --clean --if-exists -n "$schema" "$local_path" \
    || die "pg_restore failed for schema ${schema} — the pg_restore output above is the diagnostic; the dump itself is re-downloadable from R2"
done
unset PGPASSWORD

# ---- verify ------------------------------------------------------------------------
log "restored schemas:"
sudo -u postgres psql -d "$DB" -tAc "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'govdata%' ORDER BY nspname"

cat <<MSG

Done. The tracker DB is back, now on ${NEW_DATA_DIR} (NVMe-backed WSL root disk, not
the 16TB MinIO drive) and restored from R2's 2026-09-17 dumps — the last successful
backup before the Seagate disk died.

IMPORTANT: postgresql@${PG_VERSION}-${PG_CLUSTER} has been down since 2026-09-17. Any
ETL worker started since then (trackerBackend: pg) has been running with no working
tracker connection this whole time. Check for currently-running workers and restart
them now that the DB is back, so today's progress is actually recorded:

  ps aux | grep EtlRunner

MSG
