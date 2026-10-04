#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Temp-dir hygiene for the ETL workers' java.io.tmpdir.

Workers create large temp files (HTTP downloads, storage streams, raw-cache staging, HMDA CSVs).
They are removed by deleteOnExit()/delete(), which never run when a JVM is killed, runs out of
memory, or the VM dies, so leftovers accumulate (247 GB measured 2026-10-04).

  init    create the marker file that authorises this script to delete under a directory
  reap    delete leftover temp files that no process has open
  sample  append one usage line to a CSV, to size the temp disk from real peak data
  report  summarise the sampled CSV

What reap may delete is an ALLOWLIST of the file names the ETL creates, never a denylist: the same
directory also holds pid files, rate-limit slots, the DataWeb API lock and the usitc slice plan,
which must survive. It only unlinks regular files directly under the root (never recurses, never
follows symlinks), skips any file a process has open, and refuses to run unless the root carries
the marker written by `init`.
"""

import argparse
import csv
import errno
import fcntl
import fnmatch
import os
import stat
import sys
import time
from datetime import datetime

DEFAULT_ROOT = "/var/tmp/govdata"
DEFAULT_STATE_DIR = os.path.expanduser("~/.govdata-tmp-usage")
MARKER = ".tmp-hygiene-ok"
LOCK = ".tmp-hygiene.lock"
DEFAULT_MIN_AGE_HOURS = 6.0
# Native libraries the JVM extracts on every start are mapped, not held open as a file descriptor,
# so the open-file check cannot see them. Deleting a mapped file only unlinks the name, but a day's
# margin keeps this from ever racing a starting JVM.
NATIVE_LIB_MIN_AGE_HOURS = 24.0

# (fnmatch pattern, minimum age in hours or None for the caller's). Every name the ETL writes with
# File.createTempFile / Files.createTempFile. tiger-*-cached-* directories are deliberately absent:
# they are caches, not scratch.
ALLOWLIST = [
    ("http-source-part-*.tmp", None),
    ("http-source-zip-*.zip", None),
    ("http-source-*.tmp", None),
    ("http-raw-cache-*.json", None),
    ("storage-openstream-*.tmp", None),
    ("provider-raw-cache-*.bin", None),
    ("hmda-*.csv", None),
    ("libduckdb_java*.so", NATIVE_LIB_MIN_AGE_HOURS),
    ("snappy-*-libsnappyjava.so", NATIVE_LIB_MIN_AGE_HOURS),
]
GB = 1024.0 ** 3


def log(msg):
    sys.stdout.write("%s %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))
    sys.stdout.flush()


def allow_min_age(name):
    """The allowlist entry's minimum age (None = caller's) if name is allowlisted, else False."""
    for pattern, min_age in ALLOWLIST:
        if fnmatch.fnmatch(name, pattern):
            return min_age
    return False


def open_inodes(root):
    """(st_dev, st_ino) of every file under root that any process currently has open.

    Matching by inode rather than path stays correct when the same directory is reachable through
    more than one mount point (/var/tmp is a bind of /mnt/wsltmp here). A process we may not
    inspect (another user's) cannot hold one of our temp files, so permission errors are skipped.
    """
    real_root = os.path.realpath(root)
    prefixes = (real_root + "/", root.rstrip("/") + "/", "/var/tmp/", "/mnt/wsltmp/")
    found = set()
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        fd_dir = "/proc/%s/fd" % pid
        try:
            fds = os.listdir(fd_dir)
        except OSError:
            continue
        for fd in fds:
            link = "%s/%s" % (fd_dir, fd)
            try:
                target = os.readlink(link)
            except OSError:
                continue
            if not target.startswith(prefixes):
                continue
            try:
                st = os.stat(link)
            except OSError:
                continue
            found.add((st.st_dev, st.st_ino))
    return found


def boot_epoch():
    with open("/proc/uptime") as f:
        return time.time() - float(f.read().split()[0])


def top_level_files(root):
    """Regular files directly under root as (name, path, lstat); symlinks and dirs are skipped."""
    with os.scandir(root) as it:
        for entry in it:
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    continue
                raise
            if stat.S_ISREG(st.st_mode):
                yield entry.name, entry.path, st


def cmd_init(args):
    root = args.root
    if not os.path.isdir(root):
        sys.exit("tmp-hygiene: %s is not a directory" % root)
    marker = os.path.join(root, MARKER)
    with open(marker, "w") as f:
        f.write("tmp-hygiene.py may delete allowlisted temp files directly under this directory\n")
    log("init: authorised %s (%s)" % (root, marker))


def cmd_reap(args):
    root = args.root
    if not os.path.isdir(root):
        sys.exit("tmp-hygiene: %s is not a directory" % root)
    if not os.path.isfile(os.path.join(root, MARKER)):
        sys.exit("tmp-hygiene: refusing to delete under %s: no %s marker (run `init` on the "
                 "directory you mean)" % (root, MARKER))
    lock_fd = os.open(os.path.join(root, LOCK), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("reap: another reaper holds the lock, nothing to do")
        return
    try:
        _reap_locked(args, root)
    finally:
        os.close(lock_fd)


def _reap_locked(args, root):
    now = time.time()
    boot = boot_epoch() if args.boot else None
    in_use = open_inodes(root)
    deleted = skipped_open = skipped_young = 0
    freed = 0
    errors = 0
    biggest = []
    for name, path, st in top_level_files(root):
        age_floor = allow_min_age(name)
        if age_floor is False:
            continue
        if (st.st_dev, st.st_ino) in in_use:
            skipped_open += 1
            continue
        if boot is not None:
            # After a restart nothing from before the boot can still be in use, so a file older
            # than the boot is an orphan whatever its age; one newer belongs to this boot.
            if st.st_mtime >= boot:
                skipped_young += 1
                continue
        else:
            min_hours = args.min_age_hours if age_floor is None else max(age_floor,
                                                                          args.min_age_hours)
            if now - st.st_mtime < min_hours * 3600:
                skipped_young += 1
                continue
        biggest.append((st.st_size, (now - st.st_mtime) / 3600.0, name))
        if args.dry_run:
            deleted += 1
            freed += st.st_size
            continue
        try:
            os.unlink(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                continue
            errors += 1
            log("reap: could not delete %s: %s" % (path, e))
            continue
        deleted += 1
        freed += st.st_size
    mode = "DRY RUN (nothing deleted): would delete" if args.dry_run else "deleted"
    log("reap%s: %s %d file(s), %.1f GB; skipped %d open, %d too young; %d error(s)"
        % (" --boot" if args.boot else "", mode, deleted, freed / GB, skipped_open,
           skipped_young, errors))
    if args.dry_run:
        biggest.sort(reverse=True)
        for size, hours, name in biggest[:10]:
            log("  %6.1f GB  %6.0fh old  %s" % (size / GB, hours, name))
    if errors:
        sys.exit(1)


def usage_snapshot(root, min_age_hours):
    now = time.time()
    in_use = open_inodes(root)
    open_b = recent_b = orphan_b = 0
    open_n = 0
    for _name, _path, st in top_level_files(root):
        if (st.st_dev, st.st_ino) in in_use:
            open_b += st.st_size
            open_n += 1
        elif now - st.st_mtime < min_age_hours * 3600:
            recent_b += st.st_size
        else:
            orphan_b += st.st_size
    vfs = os.statvfs(root)
    return {
        "open_gb": open_b / GB, "recent_gb": recent_b / GB, "orphan_gb": orphan_b / GB,
        "open_files": open_n,
        "fs_used_gb": (vfs.f_blocks - vfs.f_bfree) * vfs.f_frsize / GB,
        "fs_avail_gb": vfs.f_bavail * vfs.f_frsize / GB,
    }


CSV_FIELDS = ["iso", "epoch", "open_gb", "recent_gb", "orphan_gb", "open_files",
              "fs_used_gb", "fs_avail_gb"]


def cmd_sample(args):
    if not os.path.isdir(args.root):
        sys.exit("tmp-hygiene: %s is not a directory" % args.root)
    snap = usage_snapshot(args.root, args.min_age_hours)
    os.makedirs(args.state_dir, exist_ok=True)
    path = os.path.join(args.state_dir, "usage.csv")
    new = not os.path.exists(path)
    now = time.time()
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(CSV_FIELDS)
        w.writerow([datetime.fromtimestamp(now).strftime("%Y-%m-%dT%H:%M:%S"), int(now),
                    "%.2f" % snap["open_gb"], "%.2f" % snap["recent_gb"],
                    "%.2f" % snap["orphan_gb"], snap["open_files"],
                    "%.1f" % snap["fs_used_gb"], "%.1f" % snap["fs_avail_gb"]])


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(round(p * (len(sorted_vals) - 1))))]


def cmd_report(args):
    path = os.path.join(args.state_dir, "usage.csv")
    if not os.path.isfile(path):
        sys.exit("tmp-hygiene: no samples yet at %s" % path)
    days = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            working = float(row["open_gb"]) + float(row["recent_gb"])
            days.setdefault(row["iso"][:10], []).append(
                (working, float(row["open_gb"]), float(row["orphan_gb"]), row["iso"]))
    print("working set = files open + files younger than the reap age (what a temp disk must hold)")
    print("%-10s %7s %9s %9s %9s %10s  peak at" % ("day", "samples", "p50 GB", "p95 GB", "max GB",
                                                  "orphan GB"))
    overall = (0.0, "")
    for day in sorted(days):
        rows = days[day]
        ws = sorted(r[0] for r in rows)
        peak = max(rows)
        print("%-10s %7d %9.1f %9.1f %9.1f %10.1f  %s" % (
            day, len(rows), percentile(ws, 0.5), percentile(ws, 0.95), ws[-1],
            max(r[2] for r in rows), peak[3][11:]))
        if ws[-1] > overall[0]:
            overall = (ws[-1], peak[3])
    print("overall peak working set: %.1f GB at %s" % overall)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--root", default=os.environ.get("GOVDATA_TMPDIR", DEFAULT_ROOT))
    p.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    p.add_argument("--min-age-hours", type=float, default=DEFAULT_MIN_AGE_HOURS)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    r = sub.add_parser("reap")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--boot", action="store_true",
                   help="delete every unopened allowlisted file older than the last boot")
    sub.add_parser("sample")
    sub.add_parser("report")
    args = p.parse_args()
    {"init": cmd_init, "reap": cmd_reap, "sample": cmd_sample, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
