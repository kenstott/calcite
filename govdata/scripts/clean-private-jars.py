#!/usr/bin/env python3
"""Delete private govdata jars that nothing uses any more.

Runners build private jars (sih-govdata-<topic>.jar, in the home directory or in govdata/build/libs)
to test a fix and run a remediation job from. Nothing removed them, so 118 of them (51 GB)
had piled up by 2026-10-15. A jar is deleted when ALL of these hold:

  * it is not one of the shared jars (sih-govdata.jar, sih-govdata-<version>[-SNAPSHOT].jar);
  * it is older than --min-age-hours (a jar just built may not be named anywhere yet);
  * no live process names it in its command line or environment, or has it open;
  * no OPEN issue of the ops repository names it (a remediation job that has not started yet
    still points at its jar).

If the open-issue lookup fails nothing is deleted and the exit status is 2: a jar that a job
still needs is worse to lose than a few gigabytes kept for a day.

Usage: clean-private-jars.py [--delete] [--min-age-hours N] [--repo OWNER/NAME] [DIR ...]
Without --delete it only reports. Default DIRs: the home directory and <checkout>/govdata/build/libs.
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
import time

# The jars every pool and release uses; a private jar is anything else named sih-govdata*.jar.
SHARED = re.compile(r"^sih-govdata(-\d+\.\d+\.\d+(-SNAPSHOT)?)?\.jar$")
NAME = re.compile(r"sih-govdata[A-Za-z0-9._-]*\.jar")


def is_private(basename):
    return basename.startswith("sih-govdata") and basename.endswith(".jar") and not SHARED.match(basename)


def jar_names_in(text):
    """Every sih-govdata*.jar file name mentioned in a block of text (path or bare name)."""
    return {os.path.basename(m) for m in NAME.findall(text or "")}


def classify(files, now, min_age_hours, live_names, issue_names):
    """files: {path: mtime}. Returns (keep, delete): keep maps path -> reason, delete is a list."""
    keep, delete = {}, []
    for path, mtime in sorted(files.items()):
        base = os.path.basename(path)
        if not is_private(base):
            keep[path] = "shared jar"
        elif now - mtime < min_age_hours * 3600:
            keep[path] = "younger than %g h" % min_age_hours
        elif base in live_names:
            keep[path] = "used by a live process"
        elif base in issue_names:
            keep[path] = "named in open issue #%s" % issue_names[base]
        else:
            delete.append(path)
    return keep, delete


def live_jar_names():
    """Names of sih-govdata jars that a running process references (argv, environment, open file)."""
    names = {}
    me = os.getpid()
    for pid in filter(str.isdigit, os.listdir("/proc")):
        if int(pid) == me:
            continue
        for part in ("cmdline", "environ"):
            try:
                with open("/proc/%s/%s" % (pid, part), "rb") as f:
                    text = f.read().replace(b"\0", b" ").decode("utf-8", "replace")
            except OSError:
                continue
            for n in jar_names_in(text):
                names.setdefault(n, pid)
        try:
            for fd in os.listdir("/proc/%s/fd" % pid):
                try:
                    target = os.readlink("/proc/%s/fd/%s" % (pid, fd))
                except OSError:
                    continue
                if target.endswith(".jar"):
                    for n in jar_names_in(target):
                        names.setdefault(n, pid)
        except OSError:
            pass
    return names


def open_issue_jar_names(repo):
    """{jar name: issue number} over the bodies and comments of every open issue; raises on failure."""
    out = subprocess.run(
        ["gh", "issue", "list", "-R", repo, "--state", "open", "--limit", "1000",
         "--json", "number,body,comments"], capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise RuntimeError("gh issue list failed: " + out.stderr.strip()[:300])
    names = {}
    for issue in json.loads(out.stdout):
        text = (issue.get("body") or "") + " " + " ".join(c.get("body", "") for c in issue.get("comments", []))
        for n in jar_names_in(text):
            names.setdefault(n, issue["number"])
    return names


def default_dirs():
    here = os.path.dirname(os.path.abspath(__file__))
    return [os.path.expanduser("~"), os.path.join(here, "..", "build", "libs")]


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("dirs", nargs="*", default=default_dirs())
    ap.add_argument("--delete", action="store_true", help="delete; without it only report")
    ap.add_argument("--min-age-hours", type=float, default=24)
    ap.add_argument("--repo", default="kenstott/govdata-ops")
    args = ap.parse_args(argv)

    files = {}
    for d in args.dirs:
        for path in glob.glob(os.path.join(os.path.realpath(d), "sih-govdata*.jar")):
            if os.path.isfile(path) and not os.path.islink(path):
                files[path] = os.path.getmtime(path)
    try:
        issue_names = open_issue_jar_names(args.repo)
    except Exception as e:  # reported and fatal: nothing is deleted without the issue list
        print("clean-private-jars: cannot read open issues of %s (%s); deleting nothing" % (args.repo, e))
        return 2
    keep, delete = classify(files, time.time(), args.min_age_hours, live_jar_names(), issue_names)

    gb = lambda ps: sum(os.path.getsize(p) for p in ps) / 1073741824
    private_kept = {p: r for p, r in keep.items() if r != "shared jar"}
    print("clean-private-jars: %d private jars, %.1f GB; keeping %d (%.1f GB), %s %d (%.1f GB)" % (
        len(files) - sum(1 for r in keep.values() if r == "shared jar"),
        gb([p for p in files if is_private(os.path.basename(p))]), len(private_kept), gb(private_kept),
        "deleting" if args.delete else "would delete", len(delete), gb(delete)))
    for p, r in private_kept.items():
        print("  keep   %-50s %s" % (os.path.basename(p), r))
    for p in delete:
        if args.delete:
            os.remove(p)
        print("  %s %s" % ("deleted" if args.delete else "delete ", p))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
