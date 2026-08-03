#!/usr/bin/env bash
#
# setup-wsl-tmp-drive.sh - carve an isolated ext4 partition out of the free space
# on the govdata 16TB drive (/dev/sde, already hosting /mnt/minio on sde1) and
# bind-mount /var/tmp onto it.
#
# Why: a runaway DuckDB spill under /var/tmp/govdata (612GB, see
# entity-bridge-duckdb incident) filled the WSL root vhdx, which lives on C:.
# Isolating /var/tmp onto its own partition means a repeat can only fill ITS
# own space and fail cleanly - it can never threaten C: again, and (being yet
# another separate partition from sde1) never threaten MinIO's store either.
#
# Partition boundaries are hardcoded from a real `parted print free` reading
# (see conversation) rather than computed here, specifically to avoid ever
# computing a boundary that could clip the existing minio partition.
#
# Usage (needs sudo):
#   ./setup-wsl-tmp-drive.sh
#
set -euo pipefail

DISK="${TMP_DISK:-/dev/sde}"
PART_NUM="${TMP_PART_NUM:-2}"
PART_DEV="${DISK}${PART_NUM}"
START="${TMP_PART_START:-9536743MiB}"   # exact end of the existing minio partition
LABEL="${TMP_PART_LABEL:-wsltmp}"
MOUNT_POINT="${TMP_MOUNT_POINT:-/mnt/wsltmp}"
VARTMP_SUBDIR="${TMP_VARTMP_SUBDIR:-var-tmp}"

log() { printf '[setup-wsl-tmp] %s\n' "$*"; }
die() { printf '[setup-wsl-tmp] ERROR: %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run with sudo"
command -v parted >/dev/null || die "parted not installed (sudo apt-get install -y parted)"

# ---- 1. partition (idempotent: skip if it already exists) -------------------
if lsblk -no NAME "$PART_DEV" >/dev/null 2>&1; then
  log "$PART_DEV already exists - skipping partition creation"
else
  log "verifying free space at $START on $DISK before touching the partition table"
  parted -s "$DISK" unit MiB print free | grep -q "$START.*Free Space" \
    || die "expected free space starting at $START not found - refusing to guess boundaries. Re-run: parted -s $DISK unit MiB print free"
  log "creating partition $PART_NUM on $DISK from $START to 100%"
  parted -s "$DISK" mkpart primary ext4 "$START" 100%
  parted -s "$DISK" name "$PART_NUM" "$LABEL"
  udevadm settle 2>/dev/null || true
  sleep 1
  lsblk -no NAME "$PART_DEV" >/dev/null 2>&1 || die "partition $PART_DEV did not appear after creation"
fi

# ---- 2. filesystem (idempotent: skip if already formatted) ------------------
FSTYPE="$(lsblk -no FSTYPE "$PART_DEV" 2>/dev/null || true)"
if [ -n "$FSTYPE" ]; then
  log "$PART_DEV already has a filesystem ($FSTYPE) - skipping mkfs"
else
  log "formatting $PART_DEV as ext4 (label=$LABEL)"
  mkfs.ext4 -L "$LABEL" "$PART_DEV"
fi

UUID="$(blkid -s UUID -o value "$PART_DEV")"
[ -n "$UUID" ] || die "could not read UUID of $PART_DEV"

# ---- 3. fstab + mount (same UUID/nofail pattern as the minio entry) ---------
mkdir -p "$MOUNT_POINT"
if grep -q "UUID=$UUID" /etc/fstab; then
  log "fstab entry for $UUID already present"
else
  log "adding fstab entry for $MOUNT_POINT"
  printf 'UUID=%s  %s  ext4  defaults,nofail  0  2\n' "$UUID" "$MOUNT_POINT" >> /etc/fstab
fi

mountpoint -q "$MOUNT_POINT" || mount "$MOUNT_POINT"
mountpoint -q "$MOUNT_POINT" || die "$MOUNT_POINT did not mount - check dmesg/journalctl before proceeding"
log "$MOUNT_POINT is mounted ($(df -h "$MOUNT_POINT" | tail -1))"

# ---- 4. migrate existing /var/tmp contents, then bind-mount -----------------
VARTMP_TARGET="$MOUNT_POINT/$VARTMP_SUBDIR"
mkdir -p "$VARTMP_TARGET"

if mountpoint -q /var/tmp; then
  log "/var/tmp is already a mount - assuming this script already ran; leaving as-is"
else
  log "migrating existing /var/tmp contents to $VARTMP_TARGET"
  rsync -a /var/tmp/ "$VARTMP_TARGET/"
  if grep -q "$VARTMP_TARGET" /etc/fstab; then
    log "fstab bind-mount entry already present"
  else
    log "adding /var/tmp bind-mount to fstab"
    printf '%s  /var/tmp  none  bind,nofail  0  0\n' "$VARTMP_TARGET" >> /etc/fstab
  fi
  mount /var/tmp
  mountpoint -q /var/tmp || die "/var/tmp bind-mount failed - check dmesg/journalctl"
fi

log "done. df -h summary:"
df -h "$MOUNT_POINT" /var/tmp
