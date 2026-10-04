#!/bin/bash
# cutover-tmp-disk.sh -- put the ETL temp dir (/var/tmp/govdata) on its own small SSD-backed disk.
#
# Run as root, after create-tmp-vhd.ps1 has attached the VHDX:
#     sudo bash cutover-tmp-disk.sh --device /dev/sdX [--dry-run]
#
# Order: verify the device -> stop everything gracefully -> prove nothing still uses the directory ->
# format -> fstab -> swap the mount in -> migrate the non-temp content -> guard the services so they
# refuse to start without the mount -> restart. The path does not change, so no worker config, lock
# file or rate-limit slot moves. Anything unexpected aborts BEFORE a service is stopped or a file moves.
set -euo pipefail

MNT=/var/tmp/govdata
OLD=/var/tmp/govdata.old
LABEL=govdata-tmp
EXPECT_GB=120
DEVICE=""
DRY=0
OWNER=kstott
HYGIENE=/home/kstott/calcite/govdata/scripts/tmp-hygiene.py
STOP_WAIT_POOL=120
STOP_WAIT_JVM=240

die() { echo "ERROR: $*" >&2; exit 1; }
say() { echo "[cutover] $*"; }
run() { if [ "$DRY" = 1 ]; then echo "[dry-run] $*"; else "$@"; fi; }

while [ $# -gt 0 ]; do
  case "$1" in
    --device) DEVICE="$2"; shift 2 ;;
    --size-gb) EXPECT_GB="$2"; shift 2 ;;
    --dry-run) DRY=1; shift ;;
    *) die "unknown argument: $1" ;;
  esac
done
[ -n "$DEVICE" ] || die "--device is required"
[ "$DRY" = 1 ] || [ "$(id -u)" = 0 ] || die "run as root (sudo), or pass --dry-run"

# ---- 1. verify the device is the blank disk we mean, before touching anything -----------------------
[ -b "$DEVICE" ] || die "$DEVICE is not a block device"
case "$(lsblk -dno TYPE "$DEVICE")" in disk) ;; *) die "$DEVICE is not a whole disk" ;; esac
size_gb=$(( $(lsblk -bdno SIZE "$DEVICE") / 1073741824 ))
if [ "$size_gb" -lt $(( EXPECT_GB * 95 / 100 )) ] || [ "$size_gb" -gt $(( EXPECT_GB * 105 / 100 )) ]; then
  die "$DEVICE is ${size_gb} GB, expected about ${EXPECT_GB} GB -- wrong disk?"
fi
[ -z "$(lsblk -no MOUNTPOINT "$DEVICE" | tr -d '[:space:]')" ] || die "$DEVICE (or a partition) is mounted"
if [ "$(id -u)" = 0 ]; then   # -p probes the device itself rather than trusting the blkid cache
  existing_label=$(blkid -p -s LABEL -o value "$DEVICE" 2>/dev/null || true)
  existing_type=$(blkid -p -s TYPE -o value "$DEVICE" 2>/dev/null || true)
else                          # dry-run without root: udev's view is all that is readable
  existing_label=$(lsblk -dno LABEL "$DEVICE" | tr -d '[:space:]')
  existing_type=$(lsblk -dno FSTYPE "$DEVICE" | tr -d '[:space:]')
fi
if [ -n "$existing_type" ] && [ "$existing_label" != "$LABEL" ]; then
  die "$DEVICE already holds a '$existing_type' filesystem labelled '${existing_label}' -- refusing to format it"
fi
if findmnt -no SOURCE "$MNT" >/dev/null 2>&1 && [ "$(findmnt -no SOURCE "$MNT")" = "$DEVICE" ]; then
  say "$MNT is already on $DEVICE -- nothing to do"; exit 0
fi
[ -d "$MNT" ] || die "$MNT does not exist"
[ ! -e "$OLD" ] || die "$OLD already exists -- a previous cutover left it; remove it first"
command -v rsync >/dev/null || die "rsync is required"
say "device $DEVICE: ${size_gb} GB, ${existing_type:-blank}; target $MNT (owner $OWNER)"

# ---- 2. graceful stop --------------------------------------------------------------------------------
alive() { pgrep -u "$OWNER" -f "$1" >/dev/null 2>&1; }
wait_gone() { # pattern seconds
  local n=0; while alive "$1" && [ $n -lt "$2" ]; do sleep 2; n=$((n + 2)); done; ! alive "$1"
}
say "stopping the agent daemon, then the scheduler (KillMode=process leaves pools running)"
run systemctl stop govdata-runner.service
run systemctl stop govdata-scheduled.service
POOLS='run-pool\.sh|run-scheduled\.sh|sync-to-r2\.sh|catchup-sync-r2|x-schema\.sh|vss-local'
if [ "$DRY" = 0 ]; then
  say "SIGTERM to pools and syncs (they forward it to their workers)"
  pkill -TERM -u "$OWNER" -f "$POOLS" || true
  wait_gone "$POOLS" "$STOP_WAIT_POOL" || say "some pool scripts still alive after ${STOP_WAIT_POOL}s; continuing to the JVMs"
  JVMS='org\.apache\.calcite\.adapter\.govdata|sih-govdata.*\.jar'
  say "SIGTERM to ETL JVMs; waiting up to ${STOP_WAIT_JVM}s for them to exit on their own"
  pkill -TERM -u "$OWNER" -f "$JVMS" || true
  wait_gone "$JVMS" "$STOP_WAIT_JVM" || die "ETL JVMs still running after ${STOP_WAIT_JVM}s: $(pgrep -u "$OWNER" -f "$JVMS" | tr '\n' ' ') -- stop them yourself, then re-run"
fi

# ---- 3. prove nothing still uses the directory (never kill an unknown process) -----------------------
holders=$(python3 - "$MNT" <<'PY'
import os, sys
root = sys.argv[1].rstrip("/")
seen = set()
for pid in filter(str.isdigit, os.listdir("/proc")):
    if int(pid) == os.getpid():
        continue
    base = "/proc/%s" % pid
    targets = []
    try:
        targets.append(os.readlink(base + "/cwd"))
        for fd in os.listdir(base + "/fd"):
            try:
                targets.append(os.readlink("%s/fd/%s" % (base, fd)))
            except OSError:
                pass
    except OSError:
        continue
    if any(t == root or t.startswith(root + "/") for t in targets):
        try:
            cmd = open(base + "/cmdline").read().replace("\0", " ")[:100]
        except OSError:
            cmd = "?"
        seen.add("%s %s" % (pid, cmd))
print("\n".join(sorted(seen)))
PY
)
if [ -n "$holders" ]; then
  [ "$DRY" = 1 ] && say "(dry-run) these processes would still hold $MNT and block the swap:" || true
  echo "$holders" >&2
  [ "$DRY" = 1 ] || die "processes still use $MNT -- stop them and re-run"
fi

# ---- 4. format, fstab, swap the mount in ------------------------------------------------------------
[ -n "$existing_type" ] || run mkfs.ext4 -F -L "$LABEL" -E lazy_itable_init=1,lazy_journal_init=1 "$DEVICE"
if [ "$DRY" = 1 ]; then uuid="<new-uuid>"; else uuid=$(blkid -s UUID -o value "$DEVICE"); fi
if ! grep -q "[[:space:]]$MNT[[:space:]]" /etc/fstab; then
  run cp /etc/fstab "/etc/fstab.bak-tmpdisk-$(date +%Y%m%d-%H%M%S)"
  if [ "$DRY" = 1 ]; then echo "[dry-run] append to /etc/fstab: UUID=$uuid $MNT ext4 defaults,nofail 0 2"
  else echo "UUID=$uuid $MNT ext4 defaults,nofail 0 2" >> /etc/fstab; fi
fi
run mv "$MNT" "$OLD"
run mkdir "$MNT"
run systemctl daemon-reload
run mount "$MNT"
if [ "$DRY" = 0 ]; then
  mountpoint -q "$MNT" || die "$MNT did not mount; the old directory is at $OLD -- mv it back"
  [ "$(findmnt -no SOURCE "$MNT")" = "$DEVICE" ] || die "$MNT mounted from the wrong device"
fi
run chown "$OWNER:$OWNER" "$MNT"
run chmod 755 "$MNT"

# ---- 5. migrate what is not scratch ------------------------------------------------------------------
say "copying runs/, caches, locks and slots (not the large temp files) from $OLD"
run rsync -a \
  --exclude 'http-source-*' --exclude 'http-raw-cache-*' --exclude 'storage-openstream-*' \
  --exclude 'provider-raw-cache-*' --exclude 'hmda-*' --exclude 'libduckdb*' --exclude 'snappy-*' \
  --exclude 'cache-str-*' --exclude 'pre-daily-build.*' --exclude 'chunk-organizer-duckdb' \
  --exclude '.tmp-hygiene*' "$OLD/" "$MNT/"
[ "$DRY" = 1 ] || sudo -u "$OWNER" python3 "$HYGIENE" init

# ---- 6. services refuse to start without the mount --------------------------------------------------
for unit in govdata-scheduled govdata-runner; do
  d=/etc/systemd/system/$unit.service.d
  run mkdir -p "$d"
  if [ "$DRY" = 1 ]; then echo "[dry-run] write $d/tmp-disk.conf"
  else
    cat > "$d/tmp-disk.conf" <<UNIT
[Unit]
RequiresMountsFor=$MNT

[Service]
ExecStartPre=/usr/bin/mountpoint -q $MNT
UNIT
  fi
done
run systemctl daemon-reload

# ---- 7. start again -----------------------------------------------------------------------------------
run systemctl start govdata-scheduled.service
run systemctl start govdata-runner.service
say "done. $MNT is on $DEVICE. After you have confirmed workers run, free the HDD:  rm -rf $OLD"
[ "$DRY" = 1 ] || { df -h "$MNT" | tail -1; findmnt "$MNT"; }
