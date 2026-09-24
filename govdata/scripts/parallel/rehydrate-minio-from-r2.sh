#!/usr/bin/env bash
#
# rehydrate-minio-from-r2.sh — full restore of the govdata-parquet-v1 bucket from R2
# into a freshly (re)built MinIO instance, via the S3 API on both ends.
#
# Never a raw filesystem copy onto the MinIO volume: even in single-drive mode MinIO
# writes an xl.meta sidecar per object, so files dropped directly onto the disk are
# invisible to the S3 API. Going through PutObject on both sides is what makes this
# correct.
#
# Prereq: minio.service must already be up and healthy against the new (empty) disk —
# see replace-minio-disk.sh --start-empty. That step pre-creates the guard-bucket
# directory, which MinIO recognizes as the govdata-parquet-v1 bucket on startup; this
# script's rclone copy then populates it with real objects.
#
# Uses the static 'minio' / 'r2' remotes already in ~/.config/rclone/rclone.conf — no
# ad-hoc credential wiring needed (unlike sync-to-r2.sh's promote-direction path, which
# builds an ad-hoc R2 remote from .env.prod).
#
# Safe to interrupt and re-run: rclone copy skips files that already match on the
# destination by size+modtime, so a killed run resumes instead of re-copying everything.
#
# Usage:
#   ./rehydrate-minio-from-r2.sh                        # full bucket, foreground
#   ./rehydrate-minio-from-r2.sh --schemas sec           # one schema only (validate first)
#   ./rehydrate-minio-from-r2.sh --schemas sec,census    # a few schemas
#   ./rehydrate-minio-from-r2.sh --dry-run
#   ./rehydrate-minio-from-r2.sh --size-only             # re-run to confirm/finish a copy: compares
#                                                         # object size from the listing only, skipping
#                                                         # the per-object HEAD for modtime. Much faster
#                                                         # when nearly everything is already copied, but
#                                                         # misses a same-size content change.
#   nohup ./rehydrate-minio-from-r2.sh > ~/rehydrate-minio.log 2>&1 &
#                                                         # full run in background — can
#                                                         # take hours to days depending
#                                                         # on lake size
#
set -euo pipefail

# rclone lives in ~/.local/bin, which a non-login/background shell's PATH may not include.
[ -d "$HOME/.local/bin" ] && [[ ":$PATH:" != *":$HOME/.local/bin:"* ]] && PATH="$HOME/.local/bin:$PATH"
export PATH

BUCKET="${GOVDATA_BUCKET:-govdata-parquet-v1}"
MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"
MINIO_HEALTH_URL="${GOVDATA_MINIO_HEALTH_URL:-http://localhost:9002/minio/health/live}"
TRANSFERS="${GOVDATA_REHYDRATE_TRANSFERS:-16}"
CHECKERS="${GOVDATA_REHYDRATE_CHECKERS:-32}"
RETRIES="${GOVDATA_REHYDRATE_RETRIES:-6}"
# Objects above the cutoff go up as multipart, where a truncated source stream (S3 400
# IncompleteBody) fails and retries one part instead of the whole object. Worst-case
# buffer is TRANSFERS x UPLOAD_CONCURRENCY x CHUNK_SIZE.
MULTIPART_CUTOFF="${GOVDATA_REHYDRATE_MULTIPART_CUTOFF:-32M}"
CHUNK_SIZE="${GOVDATA_REHYDRATE_CHUNK_SIZE:-16M}"
UPLOAD_CONCURRENCY="${GOVDATA_REHYDRATE_UPLOAD_CONCURRENCY:-2}"

log() { printf '[rehydrate-minio] %s\n' "$*"; }
die() { printf '[rehydrate-minio] ERROR: %s\n' "$*" >&2; exit 1; }

DRY_RUN=false
SIZE_ONLY=false
SCHEMAS_FILTER=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --size-only) SIZE_ONLY=true; shift ;;
    --schemas) SCHEMAS_FILTER="$2"; shift 2 ;;
    *) die "unknown argument: $1 (usage: $0 [--dry-run] [--size-only] [--schemas s1,s2])" ;;
  esac
done

command -v rclone >/dev/null || die "rclone not found on PATH"

log "checking MinIO is up at ${MINIO_HEALTH_URL}..."
curl -fsS "$MINIO_HEALTH_URL" >/dev/null \
  || die "MinIO health check failed — start minio.service first (see replace-minio-disk.sh --start-empty)"

log "checking rclone remotes..."
rclone lsd "${MINIO_REMOTE}:" >/dev/null 2>&1 || die "rclone remote '${MINIO_REMOTE}:' not reachable"
rclone lsd "${R2_REMOTE}:${BUCKET}" >/dev/null 2>&1 || die "rclone remote '${R2_REMOTE}:${BUCKET}' not reachable"

RCLONE_FLAGS=(--transfers "$TRANSFERS" --checkers "$CHECKERS" --stats 30s --stats-one-line -P
  --retries "$RETRIES" --retries-sleep 15s --low-level-retries 20
  --s3-upload-cutoff "$MULTIPART_CUTOFF" --s3-chunk-size "$CHUNK_SIZE"
  --s3-upload-concurrency "$UPLOAD_CONCURRENCY")
[ "$DRY_RUN" = true ] && RCLONE_FLAGS+=(--dry-run)
[ "$SIZE_ONLY" = true ] && RCLONE_FLAGS+=(--size-only)

if [ -n "$SCHEMAS_FILTER" ]; then
  IFS=',' read -ra SCHEMAS <<< "$(echo "$SCHEMAS_FILTER" | tr ' ' ',')"
  for schema in "${SCHEMAS[@]}"; do
    schema="$(echo "$schema" | xargs)"
    [ -n "$schema" ] || continue
    log "rehydrating schema: $schema"
    rclone copy "${R2_REMOTE}:${BUCKET}/${schema}" "${MINIO_REMOTE}:${BUCKET}/${schema}" "${RCLONE_FLAGS[@]}" \
      || die "copy failed for schema $schema — safe to re-run, already-copied files are skipped"
  done
else
  log "rehydrating FULL bucket: ${BUCKET} (this can take hours to days depending on lake size)"
  rclone copy "${R2_REMOTE}:${BUCKET}" "${MINIO_REMOTE}:${BUCKET}" "${RCLONE_FLAGS[@]}" \
    || die "copy failed — safe to re-run, already-copied files are skipped"
fi

log "done."
