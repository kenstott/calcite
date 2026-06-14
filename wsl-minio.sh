#!/usr/bin/env bash
#
# wsl-minio.sh — manage a local MinIO container (Docker Desktop / WSL2)
#
# Single-node MinIO with a persistent named Docker volume and transparent
# compression tuned for parquet/xml/xbrl/text/csv workloads.
#
# Usage:
#   ./wsl-minio.sh up        # create + start (recreates if config changed)
#   ./wsl-minio.sh down      # stop and remove the container (data is kept)
#   ./wsl-minio.sh restart   # down + up
#   ./wsl-minio.sh status    # show container state + ports
#   ./wsl-minio.sh logs      # tail container logs
#   ./wsl-minio.sh shell     # open a shell inside the container
#
set -euo pipefail

# ---- Config (override via environment) --------------------------------------
NAME="${MINIO_NAME:-minio}"
# Persistent storage. On Docker Desktop + WSL2 a *named volume* lives on the
# large docker disk (the full ~1 TB ext4 the engine uses) and MinIO sees its
# real size. A bind mount to a WSL-home path (e.g. ~/minio-data) gets proxied
# onto a tiny ~12 GB filesystem, so MinIO fills up and returns HTTP 507
# "XMinioStorageFull" under load — do NOT use one for real workloads.
VOLUME="${MINIO_VOLUME:-minio-data}"
# Sanity floor: warn if MinIO's /data ends up smaller than this (GB).
MIN_DATA_GB="${MINIO_MIN_DATA_GB:-800}"
API_PORT="${MINIO_API_PORT:-9002}"
CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9003}"
ROOT_USER="${MINIO_ROOT_USER:-admin}"
ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-password123}"
IMAGE="${MINIO_IMAGE:-minio/minio}"

# Compression — text-heavy types compress well; parquet is already compressed
# internally, included for coverage (drop it if you see CPU cost with no gain).
COMPRESS_EXTENSIONS="${MINIO_COMPRESSION_EXTENSIONS:-.txt,.csv,.tsv,.json,.xml,.xbrl,.xsd,.html,.log,.parquet}"
COMPRESS_MIME_TYPES="${MINIO_COMPRESSION_MIME_TYPES:-text/*,application/xml,application/json,application/xbrl+xml,application/octet-stream}"

# -----------------------------------------------------------------------------
require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker not found. Enable Docker Desktop's WSL integration for this distro." >&2
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "error: cannot reach the Docker daemon (permission or daemon down)." >&2
    echo "       try: sudo usermod -aG docker \$USER  (then reopen the shell)" >&2
    exit 1
  fi
}

up() {
  require_docker
  # Recreate to pick up any config changes. The named volume persists across it.
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker volume create "$VOLUME" >/dev/null
  docker run -d --name "$NAME" \
    -p "${API_PORT}:9000" -p "${CONSOLE_PORT}:9001" \
    -v "${VOLUME}:/data" \
    -e "MINIO_ROOT_USER=${ROOT_USER}" \
    -e "MINIO_ROOT_PASSWORD=${ROOT_PASSWORD}" \
    -e "MINIO_COMPRESSION_ENABLE=on" \
    -e "MINIO_COMPRESSION_EXTENSIONS=${COMPRESS_EXTENSIONS}" \
    -e "MINIO_COMPRESSION_MIME_TYPES=${COMPRESS_MIME_TYPES}" \
    --restart unless-stopped \
    "$IMAGE" server /data --console-address ":9001" >/dev/null

  # Report the capacity MinIO actually sees, and warn if it's below the floor.
  sleep 2
  local avail_gb
  avail_gb=$(docker exec "$NAME" df -BG /data 2>/dev/null | awk 'NR==2{gsub("G","",$4); print $4}')
  echo "minio up:"
  echo "  API      http://localhost:${API_PORT}"
  echo "  Console  http://localhost:${CONSOLE_PORT}   (user: ${ROOT_USER})"
  echo "  Volume   ${VOLUME}  (docker-managed, on the large docker disk)"
  echo "  /data    ${avail_gb:-?} GB available"
  if [ -n "${avail_gb:-}" ] && [ "$avail_gb" -lt "$MIN_DATA_GB" ]; then
    echo "  WARNING: /data shows ${avail_gb}GB (< ${MIN_DATA_GB}GB floor) — MinIO will 507 under load." >&2
    echo "           This means a small backing FS (e.g. a WSL-home bind mount); use the named volume." >&2
  fi
}

down() {
  require_docker
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  echo "minio stopped and removed (data kept in volume ${VOLUME})"
}

status() {
  require_docker
  docker ps -a --filter "name=^/${NAME}$" \
    --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
}

logs()  { require_docker; docker logs -f "$NAME"; }
shell() { require_docker; docker exec -it "$NAME" sh; }

case "${1:-up}" in
  up)       up ;;
  down)     down ;;
  restart)  down; up ;;
  status)   status ;;
  logs)     logs ;;
  shell)    shell ;;
  *)
    echo "usage: $0 {up|down|restart|status|logs|shell}" >&2
    exit 2
    ;;
esac
