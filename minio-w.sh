#!/usr/bin/env bash
#
# minio-w.sh — bring up the local MinIO on this Windows + WSL2 + Docker Desktop box,
# bound to the REAL data directory. Windows/WSL counterpart to minio-simple.sh (macOS).
#
# Run this inside WSL (the bind source is a WSL path):
#   ./minio-w.sh            # up   (default)
#   ./minio-w.sh down       # stop + remove the container (data is kept on disk)
#   ./minio-w.sh status     # show container state + ports
# From Windows you can also call:  wsl ./minio-w.sh
#
# WHY THIS EXISTS — the reboot trap we hit:
#   On reboot Docker Desktop can auto-start the minio container BEFORE the Ubuntu
#   WSL distro is mounted. The bind source (${DATA_DIR}) isn't visible yet, so
#   Docker SILENTLY CREATES AN EMPTY DIR inside its own VM and MinIO initialises
#   it as a fresh root — you get empty buckets on a ~14 GB placeholder FS while the
#   real 151 GB sits untouched on disk. Re-run this after a reboot to recreate the
#   container bound to the real folder. It REFUSES to start unless the real data is
#   actually visible at the bind source, so it can never serve empty buckets.
#
set -euo pipefail

NAME="${MINIO_NAME:-minio}"
# The real store (≈151 GB) lives here on the WSL root disk (/dev/sdf), NOT in a
# docker-managed named volume. Override with MINIO_DATA_DIR if it ever moves.
DATA_DIR="${MINIO_DATA_DIR:-/home/adminwsl/minio-data}"
API_PORT="${MINIO_API_PORT:-9002}"
CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9003}"
ROOT_USER="${MINIO_ROOT_USER:-admin}"
ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-password123}"
IMAGE="${MINIO_IMAGE:-minio/minio}"
# A bucket that must exist in DATA_DIR for the data to be considered "really there".
# Uses the parquet data bucket (the tracker buckets were retired after the Postgres cutover).
GUARD_BUCKET="${MINIO_GUARD_BUCKET:-govdata-parquet-v1}"

COMPRESS_EXTENSIONS="${MINIO_COMPRESSION_EXTENSIONS:-.txt,.csv,.tsv,.json,.xml,.xbrl,.xsd,.html,.log,.parquet}"
COMPRESS_MIME_TYPES="${MINIO_COMPRESSION_MIME_TYPES:-text/*,application/xml,application/json,application/xbrl+xml,application/octet-stream}"

require_docker() {
  command -v docker >/dev/null 2>&1 || { echo "error: docker not found (enable Docker Desktop WSL integration)." >&2; exit 1; }
  docker info >/dev/null 2>&1 || { echo "error: cannot reach the Docker daemon (is Docker Desktop running?)." >&2; exit 1; }
}

up() {
  require_docker
  # GUARD 1 (host): the bind source must actually hold the data, or starting would
  # create/serve empty buckets. This is the check that prevents the reboot trap.
  if [ ! -d "${DATA_DIR}/${GUARD_BUCKET}" ]; then
    echo "REFUSING to start: '${DATA_DIR}/${GUARD_BUCKET}' not found." >&2
    echo "  The data dir is not mounted/visible — starting now would create EMPTY buckets." >&2
    echo "  Confirm 'ls ${DATA_DIR}' shows your buckets, then re-run." >&2
    exit 1
  fi

  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" \
    -p "${API_PORT}:9000" -p "${CONSOLE_PORT}:9001" \
    -v "${DATA_DIR}:/data" \
    -e "MINIO_ROOT_USER=${ROOT_USER}" \
    -e "MINIO_ROOT_PASSWORD=${ROOT_PASSWORD}" \
    -e "MINIO_COMPRESSION_ENABLE=on" \
    -e "MINIO_COMPRESSION_EXTENSIONS=${COMPRESS_EXTENSIONS}" \
    -e "MINIO_COMPRESSION_MIME_TYPES=${COMPRESS_MIME_TYPES}" \
    --restart unless-stopped \
    "$IMAGE" server /data --console-address ":9001" >/dev/null
  sleep 3

  # GUARD 2 (container): prove the container actually bound the real data, not the
  # 14 GB placeholder. If the bucket isn't visible inside, fail loudly.
  if ! docker exec "$NAME" ls "/data/${GUARD_BUCKET}" >/dev/null 2>&1; then
    echo "ERROR: '${GUARD_BUCKET}' not visible inside the container — wrong bind, MinIO is on the placeholder FS." >&2
    echo "       Check that ${DATA_DIR} is mounted, then re-run." >&2
    exit 1
  fi
  # MinIO is already up and serving at this point (guards passed above).
  echo "minio up:"
  echo "  API      http://localhost:${API_PORT}"
  echo "  Console  http://localhost:${CONSOLE_PORT}   (user: ${ROOT_USER})"
  echo "  bind     ${DATA_DIR}"
  # The store is large (~150 GB), so du walks the whole tree and can take SEVERAL MINUTES.
  # This is only an informational total — MinIO is already usable above, so it is safe to
  # Ctrl-C here. `|| true` keeps `set -e` from treating an interrupted du as a fatal error.
  echo "  computing /data size (large store — may take several minutes; Ctrl-C to skip)…"
  local size
  size=$(docker exec "$NAME" du -sh /data 2>/dev/null | awk '{print $1}') || true
  echo "  /data    ${size:-?}"
}

down() {
  require_docker
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  echo "minio stopped and removed (data kept in ${DATA_DIR})"
}

status() {
  require_docker
  docker ps -a --filter "name=^/${NAME}$" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
}

case "${1:-up}" in
  up)      up ;;
  down)    down ;;
  status)  status ;;
  *)       echo "usage: $0 {up|down|status}" >&2; exit 2 ;;
esac
