#!/usr/bin/env bash
#
# replace-minio-disk.sh — one-time migration: wipe and format a freshly
# wsl --mounted bare disk as ext4 and wire it up as MinIO's /mnt/minio data
# volume, replacing a dead disk's fstab entry.
#
# Context: the old MinIO disk (Seagate Exos ST16000NM001G, UUID
# b23935aa-2a91-4b48-bd14-4a023cd69be1) failed and was physically replaced.
# Before running this, the Windows side must already have taken the new disk
# offline and reattached it to WSL as a bare disk:
#
#   (elevated PowerShell)
#   $disk = Get-Disk | Where-Object { $_.FriendlyName -like '*MG08ACA16TE*' }
#   Set-Disk -Number $disk.Number -IsOffline $true
#   wsl --mount "\\.\PHYSICALDRIVE$($disk.Number)" --bare
#
# This only creates the MinIO partition (sde1-equivalent) — it deliberately
# stops at MINIO_PART_END instead of 100% of the disk, leaving the rest free
# for setup-wsl-tmp-drive.sh (already in this repo) to carve out the isolated
# /var/tmp partition, exactly like the old disk's layout. Run that script
# next, with TMP_DISK set to the same device:
#   sudo TMP_DISK=/dev/sdX ./setup-wsl-tmp-drive.sh
#
# Usage (run inside WSL; needs sudo):
#   ./replace-minio-disk.sh /dev/sdX            # partition + format + mount only
#   ./replace-minio-disk.sh /dev/sdX --start-empty
#                                                # + create the guard-bucket dir
#                                                #   and restart minio.service
#
# --start-empty is separate on purpose: the new disk has no data on it, so
# minio.service's ExecStartPre guard (test -d $MINIO_VOLUMES/$MINIO_GUARD_BUCKET)
# will correctly refuse to start against it. That guard exists specifically to
# stop MinIO from silently serving an empty store — don't pass --start-empty
# until you've decided you're OK starting fresh (vs. rehydrating from R2 first).
#
set -euo pipefail

OLD_UUID="b23935aa-2a91-4b48-bd14-4a023cd69be1"
MOUNTPOINT="/mnt/minio"
ENV_FILE="/etc/default/minio"
# Must match setup-wsl-tmp-drive.sh's TMP_PART_START default exactly — that
# script verifies free space starts here before touching the partition table.
MINIO_PART_END="9536743MiB"

log() { printf '[replace-minio-disk] %s\n' "$*"; }
die() { printf '[replace-minio-disk] ERROR: %s\n' "$*" >&2; exit 1; }

DEV="${1:-}"
START_EMPTY=0
[ "${2:-}" = "--start-empty" ] && START_EMPTY=1
[ -n "$DEV" ] || die "usage: $0 /dev/sdX [--start-empty]  (run 'lsblk' first to find the new bare disk)"

# ---- must be root -------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
  log "re-exec under sudo…"
  exec sudo -E bash "$0" "$@"
fi

# ---- safety checks -------------------------------------------------------
[ -b "$DEV" ] || die "$DEV is not a block device"

if [[ "$DEV" =~ [0-9]$ ]]; then
  die "$DEV looks like a partition, not a whole disk — pass e.g. /dev/sde, not /dev/sde1"
fi

# Refuse known-important devices outright so a typo can't nuke the WSL root
# disk, swap, or the postgres data disk.
ROOT_DEV="/dev/$(lsblk -no PKNAME "$(findmnt -no SOURCE /)" 2>/dev/null || true)"
if [ -n "$ROOT_DEV" ] && [ "$ROOT_DEV" = "$DEV" ]; then
  die "$DEV backs the WSL root filesystem ($ROOT_DEV) — refusing"
fi

PART="${DEV}1"

# Idempotent re-entry: a prior run of this script already wiped/formatted/mounted
# $PART at $MOUNTPOINT, and this invocation is just here for --start-empty (or was
# re-run by habit). Detect that and skip straight past the destructive section
# instead of tripping the "disk in use" guard below on our own mount.
ALREADY_SET_UP=0
if mountpoint -q "$MOUNTPOINT" 2>/dev/null && [ "$(findmnt -no SOURCE "$MOUNTPOINT" 2>/dev/null)" = "$PART" ]; then
  ALREADY_SET_UP=1
  log "$MOUNTPOINT is already mounted from $PART — skipping wipe/format/mount"
fi

if [ "$ALREADY_SET_UP" != 1 ]; then
  if mount | grep -q "^${DEV}[0-9]* "; then
    die "$DEV has a mounted partition right now — refusing to wipe a disk in use"
  fi

  SIZE_BYTES=$(lsblk -bdn -o SIZE "$DEV")
  SIZE_TB=$(( SIZE_BYTES / 1000000000000 ))
  if [ "$SIZE_TB" -lt 14 ] || [ "$SIZE_TB" -gt 18 ]; then
    die "$DEV is ${SIZE_TB}TB — expected ~16TB (TOSHIBA MG08ACA16TE). Refusing; pass the right device."
  fi

  log "target: $DEV (${SIZE_TB}TB) — about to WIPE ALL DATA and create the minio ext4 partition"
  log "(partition 1 only, 0% - ${MINIO_PART_END}; the rest is left free for setup-wsl-tmp-drive.sh)"
  lsblk "$DEV"
  read -r -p "Type YES to confirm wiping $DEV: " CONFIRM
  [ "$CONFIRM" = "YES" ] || die "aborted"

  # ---- partition + format ---------------------------------------------------
  log "wiping existing filesystem signatures"
  wipefs -a "$DEV"

  log "creating GPT label + ext4 partition 1 (0% - ${MINIO_PART_END})"
  parted -s "$DEV" mklabel gpt
  parted -s "$DEV" mkpart primary ext4 0% "$MINIO_PART_END"
  parted -s "$DEV" name 1 minio-data
  partprobe "$DEV"
  sleep 1

  [ -b "$PART" ] || die "expected partition $PART did not appear"

  log "formatting $PART as ext4 (label: minio-data)"
  mkfs.ext4 -L minio-data "$PART"

  NEW_UUID="$(blkid -s UUID -o value "$PART")"
  [ -n "$NEW_UUID" ] || die "could not read UUID of $PART"
  log "new UUID: $NEW_UUID"

  # ---- fstab -----------------------------------------------------------------
  FSTAB_BAK="/etc/fstab.bak.$(date +%Y%m%d%H%M%S)"
  cp /etc/fstab "$FSTAB_BAK"
  log "backed up /etc/fstab to $FSTAB_BAK"

  if grep -q "^UUID=${OLD_UUID}" /etc/fstab; then
    sed -i "s|^UUID=${OLD_UUID}.*|UUID=${NEW_UUID}  ${MOUNTPOINT}  ext4  defaults,nofail  0  2|" /etc/fstab
    log "replaced dead disk's fstab line with the new UUID"
  else
    echo "UUID=${NEW_UUID}  ${MOUNTPOINT}  ext4  defaults,nofail  0  2" >> /etc/fstab
    log "old UUID line not found — appended a fresh fstab entry instead"
  fi

  # ---- mount ------------------------------------------------------------------
  mkdir -p "$MOUNTPOINT"
  systemctl daemon-reload
  mount "$MOUNTPOINT"
  mountpoint -q "$MOUNTPOINT" || die "mount of $MOUNTPOINT did not take"
  log "mounted OK:"
  df -h "$MOUNTPOINT"
fi

# ---- guard bucket / service start -------------------------------------------
if [ "$START_EMPTY" != 1 ]; then
  cat <<MSG

Disk is partitioned, formatted, and mounted at ${MOUNTPOINT}.

Next: carve out the isolated /var/tmp partition in the space left free
(this repo's setup-wsl-tmp-drive.sh already knows the boundary):

  sudo TMP_DISK=$DEV ./setup-wsl-tmp-drive.sh

Then, minio's store is EMPTY — minio.service will still refuse to start (by
design) until the guard-bucket directory exists. Re-run with --start-empty
once you've decided you're accepting a fresh empty store (vs. rehydrating
from R2 first):

  sudo $0 $DEV --start-empty

MSG
  exit 0
fi

[ -f "$ENV_FILE" ] || die "$ENV_FILE not found — can't read MINIO_VOLUMES/MINIO_GUARD_BUCKET"
# shellcheck disable=SC1090
. "$ENV_FILE"
VOL="${MINIO_VOLUMES:-$MOUNTPOINT}"
GUARD="${MINIO_GUARD_BUCKET:-govdata-parquet-v1}"

log "creating guard-bucket dir ${VOL}/${GUARD} (accepting empty store)"
mkdir -p "${VOL}/${GUARD}"

log "restarting minio.service"
systemctl restart minio
sleep 2
systemctl status minio --no-pager --lines=10 || true

cat <<MSG

minio.service is up against the empty store. Next: rehydrate from R2
(govdata/scripts/parallel/rehydrate-minio-from-r2.sh) — try one small schema
first, then the full bucket in the background:

  govdata/scripts/parallel/rehydrate-minio-from-r2.sh --schemas sec
  nohup govdata/scripts/parallel/rehydrate-minio-from-r2.sh > ~/rehydrate-minio.log 2>&1 &

MSG
