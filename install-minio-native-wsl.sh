#!/usr/bin/env bash
#
# install-minio-native-wsl.sh — install MinIO as a NATIVE systemd service in WSL,
# replacing the Docker-Desktop container so MinIO auto-starts on WSL boot without
# the "Docker starts the container before the WSL bind mount exists → empty
# buckets on a placeholder FS" trap that minio-w.sh documents.
#
# Why native + systemd:
#   * systemd is enabled in this WSL (/etc/wsl.conf → systemd=true).
#   * A native server reads /home/adminwsl/minio-data directly (same VM, no
#     cross-VM bind race), so the empty-store reboot trap is impossible.
#   * `Restart=always` + `systemctl enable` = comes back on every WSL boot.
#
# This does NOT auto-boot WSL itself at Windows startup — WSL is dormant until
# Windows invokes it. Run register-wsl-boot-task.ps1 on Windows once for that.
#
# Usage (run inside WSL; needs sudo):
#   ./install-minio-native-wsl.sh            # prep only: install binary + unit,
#                                            #   enable service, DO NOT touch the
#                                            #   running Docker container.
#   ./install-minio-native-wsl.sh --cutover  # + stop the Docker minio container
#                                            #   and start the native service now.
#   sudo ./install-minio-native-wsl.sh --cutover
#
set -euo pipefail

# ---- Config (override via environment) --------------------------------------
MINIO_RELEASE="${MINIO_RELEASE:-RELEASE.2025-09-07T16-13-09Z}"  # matches the current container
DATA_DIR="${MINIO_DATA_DIR:-/home/adminwsl/minio-data}"
API_PORT="${MINIO_API_PORT:-9002}"
CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9003}"
GUARD_BUCKET="${MINIO_GUARD_BUCKET:-govdata-parquet-v1}"   # must exist in DATA_DIR or refuse to start
ENV_PROD="${MINIO_ENV_PROD:-/home/adminwsl/calcite/govdata/.env.prod}"  # source of the AWS_* creds
RUN_USER="${MINIO_RUN_USER:-root}"     # data dir is root-owned; run as root to avoid chowning 151 GB
ENV_FILE="/etc/default/minio"
UNIT_FILE="/etc/systemd/system/minio.service"
BIN=/usr/local/bin/minio
MC=/usr/local/bin/mc
CUTOVER=0
[ "${1:-}" = "--cutover" ] && CUTOVER=1

log() { printf '[install-minio] %s\n' "$*"; }
die() { printf '[install-minio] ERROR: %s\n' "$*" >&2; exit 1; }

# ---- must be root ------------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
  log "re-exec under sudo…"
  exec sudo -E MINIO_RELEASE="$MINIO_RELEASE" MINIO_ENV_PROD="$ENV_PROD" bash "$0" "$@"
fi

command -v systemctl >/dev/null || die "systemd not available (need systemd=true in /etc/wsl.conf)"

# ---- creds: adopt EXACTLY what the ETL clients use (never invent) ------------
[ -f "$ENV_PROD" ] || die "creds source not found: $ENV_PROD (set MINIO_ENV_PROD)"
getval() { grep -E "^\s*(export\s+)?$1\s*=" "$ENV_PROD" | head -1 | sed -E "s/.*$1\s*=\s*//; s/^[\"']//; s/[\"'].*$//" | tr -d '\r'; }
ROOT_USER="$(getval AWS_ACCESS_KEY_ID)"
ROOT_PASSWORD="$(getval AWS_SECRET_ACCESS_KEY)"
[ -n "$ROOT_USER" ] && [ -n "$ROOT_PASSWORD" ] || die "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not found in $ENV_PROD — native MinIO must use the same creds clients present, or every request 401s"

# ---- 1. install the minio + mc binaries (pinned, idempotent) ----------------
need_download=1
if [ -x "$BIN" ] && "$BIN" --version 2>/dev/null | grep -q "$MINIO_RELEASE"; then
  need_download=0; log "minio $MINIO_RELEASE already installed"
fi
if [ "$need_download" = 1 ]; then
  url="https://dl.min.io/server/minio/release/linux-amd64/archive/minio.${MINIO_RELEASE}"
  log "downloading $url"
  curl -fsSL -o "$BIN.tmp" "$url" || die "minio download failed"
  chmod +x "$BIN.tmp"; mv -f "$BIN.tmp" "$BIN"
  "$BIN" --version | head -1
fi
if [ ! -x "$MC" ]; then
  log "downloading mc client"
  curl -fsSL -o "$MC.tmp" "https://dl.min.io/client/mc/release/linux-amd64/mc" || die "mc download failed"
  chmod +x "$MC.tmp"; mv -f "$MC.tmp" "$MC"
fi

# ---- 2. EnvironmentFile (0600, root-only) — creds + compression to match old --
if [ -f "$ENV_FILE" ] && [ "${MINIO_FORCE_ENV:-0}" != 1 ]; then
  log "$ENV_FILE exists — keeping it (set MINIO_FORCE_ENV=1 to overwrite)"
else
  log "writing $ENV_FILE"
  umask 077
  cat > "$ENV_FILE" <<ENV
# Managed by install-minio-native-wsl.sh — creds mirror ${ENV_PROD} AWS_* so every
# existing client (ETL AWS creds) keeps working. 0600, root-only.
MINIO_ROOT_USER=${ROOT_USER}
MINIO_ROOT_PASSWORD=${ROOT_PASSWORD}
# Transparent compression for text/columnar payloads (matches the retired container).
MINIO_COMPRESSION_ENABLE=on
MINIO_COMPRESSION_EXTENSIONS=.txt,.csv,.tsv,.json,.xml,.xbrl,.xsd,.html,.log,.parquet
MINIO_COMPRESSION_MIME_TYPES=text/*,application/xml,application/json,application/xbrl+xml,application/octet-stream
# Consumed by minio.service ExecStart / guard (not MinIO's own vars).
MINIO_DATA_DIR=${DATA_DIR}
MINIO_API_PORT=${API_PORT}
MINIO_CONSOLE_PORT=${CONSOLE_PORT}
MINIO_GUARD_BUCKET=${GUARD_BUCKET}
ENV
  chmod 600 "$ENV_FILE"
fi

# ---- 3. systemd unit ---------------------------------------------------------
log "writing $UNIT_FILE"
cat > "$UNIT_FILE" <<UNIT
[Unit]
Description=MinIO (native, WSL) — govdata object store
Documentation=https://min.io/docs
# local-fs.target guarantees /home (the real 151 GB store) is mounted first; the
# ExecStartPre guard is belt-and-suspenders against serving an empty store.
After=local-fs.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
EnvironmentFile=${ENV_FILE}
# Refuse to start if the real data isn't visible (mirrors minio-w.sh guard).
ExecStartPre=/usr/bin/test -d \${MINIO_DATA_DIR}/\${MINIO_GUARD_BUCKET}
ExecStart=${BIN} server \${MINIO_DATA_DIR} --address :\${MINIO_API_PORT} --console-address :\${MINIO_CONSOLE_PORT}
Restart=always
RestartSec=3
# Give MinIO time to flush on stop.
TimeoutStopSec=30
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable minio >/dev/null 2>&1 || true
log "service installed + enabled (starts on every WSL boot)"

# ---- 4. cutover (only with --cutover) ---------------------------------------
if [ "$CUTOVER" != 1 ]; then
  cat <<MSG

PREP COMPLETE — the running Docker container was NOT touched.
To cut over to native (brief MinIO outage while ports/data hand off):

  sudo $0 --cutover

MSG
  exit 0
fi

log "CUTOVER: verifying real data is present before stopping anything"
[ -d "$DATA_DIR/$GUARD_BUCKET" ] || die "guard bucket $DATA_DIR/$GUARD_BUCKET missing — refusing cutover (would serve an empty store)"

if command -v docker >/dev/null && docker ps --format '{{.Names}}' | grep -qx minio; then
  log "stopping Docker 'minio' container and clearing its restart policy (kept, not removed → reversible)"
  docker update --restart=no minio >/dev/null 2>&1 || true
  docker stop minio >/dev/null || die "could not stop docker minio container"
fi

log "starting native minio.service"
systemctl restart minio
sleep 3
if curl -fsS "http://localhost:${API_PORT}/minio/health/live" >/dev/null 2>&1; then
  log "HEALTHY: native MinIO answering on :${API_PORT}"
  systemctl --no-pager --lines=0 status minio || true
  log "verify buckets:  mc alias set local http://localhost:${API_PORT} <user> <pass> && mc ls local"
else
  systemctl --no-pager --lines=20 status minio || true
  die "native MinIO did not pass health check on :${API_PORT} — inspect 'journalctl -u minio'"
fi
