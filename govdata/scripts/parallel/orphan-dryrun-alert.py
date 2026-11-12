#!/usr/bin/env python3
"""Reads an `orphan-file-cleanup.sh --metadata-only` dry-run log, records each table's ORPHAN_DELETABLE
count and raises one ops issue per table per breach.

A breach starts when a table's would-delete count exceeds the threshold and ends when it falls back to
or under it; the issue is filed on the transition into breach only, so a table that stays over the
threshold is not re-reported daily. Nothing here deletes anything.

Usage: orphan-dryrun-alert.py --log FILE --state-dir DIR [--threshold N] [--no-issue]
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

# six audited tables carried 68..6,842 deletable files, one burst table 77,910; 1,000 separates the
# noise floor from an accumulation worth a maintainer decision
DEFAULT_THRESHOLD = 1000

LINE = re.compile(
    r"(?P<table>\S+) metadata: on disk (?P<on_disk>\d+), reachable (?P<reachable>\d+), "
    r"orphans (?P<orphans>\d+), inside safety window (?P<young>\d+), "
    r"would delete (?P<deletable>\d+) \((?P<bytes>\d+) bytes\)")
SKIPPED = re.compile(r"(?P<table>\S+) metadata: skipped, commit lock held")


def parse(text):
    """Returns {table: {...counts}} for every dry-run result line; skipped tables are not results."""
    out = {}
    for m in LINE.finditer(text):
        out[m.group("table")] = {k: int(m.group(k)) for k in
                                 ("on_disk", "reachable", "orphans", "young", "deletable", "bytes")}
    return out


def evaluate(results, breached, threshold):
    """Returns (new_breaches, still_breached). A table absent from results keeps its prior state, so a
    skipped or unreachable table neither raises nor clears a breach."""
    still = set(breached)
    new = []
    for table, r in sorted(results.items()):
        if r["deletable"] > threshold:
            if table not in still:
                new.append(table)
            still.add(table)
        else:
            still.discard(table)
    return new, sorted(still)


def issue_title(table):
    return "iceberg: %s has orphan metadata files over the dry-run threshold" % table


def issue_body(table, r, threshold, day):
    return (
        "Daily dry run of the metadata/ orphan pass (%s), `orphan-file-cleanup.sh --metadata-only`, "
        "no `--delete`.\n\n"
        "- table: `%s`\n- on disk: %d, reachable: %d, orphans: %d, inside the safety window: %d\n"
        "- would delete: %d files (%d bytes), threshold %d\n\n"
        "Deleting is a maintainer decision (needs `approved`); nothing was changed. Raised once per breach: "
        "no further issue is filed for this table until its count has fallen to the threshold and exceeded "
        "it again." % (day, table, r["on_disk"], r["reachable"], r["orphans"], r["young"],
                       r["deletable"], r["bytes"], threshold))


def file_issue(table, r, threshold, day, repo):
    """Files one issue unless an open one with the same title exists. Returns the issue ref or None."""
    title = issue_title(table)
    found = subprocess.run(
        ["gh", "issue", "list", "-R", repo, "--state", "open", "--search", '"%s" in:title' % title,
         "--json", "number,title"], capture_output=True, text=True, check=True)
    for i in json.loads(found.stdout):
        if i["title"] == title:
            return "#%d (already open)" % i["number"]
    made = subprocess.run(
        ["gh", "issue", "create", "-R", repo, "--title", title,
         "--body", issue_body(table, r, threshold, day),
         "--label", "type:defect", "--label", "kind:core", "--label", "status:open"],
        capture_output=True, text=True, check=True)
    return made.stdout.strip()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD)
    ap.add_argument("--no-issue", action="store_true")
    ap.add_argument("--repo", default=os.environ.get("GH_OPS_REPO", "kenstott/govdata-ops"))
    a = ap.parse_args(argv)

    with open(a.log, encoding="utf-8", errors="replace") as f:
        results = parse(f.read())
    if not results:
        print("no dry-run result lines in %s" % a.log, file=sys.stderr)
        return 1

    os.makedirs(a.state_dir, exist_ok=True)
    state_path = os.path.join(a.state_dir, "breached.json")
    breached = []
    if os.path.exists(state_path):
        with open(state_path, encoding="utf-8") as f:
            breached = json.load(f)

    day = time.strftime("%Y-%m-%d", time.gmtime())
    with open(os.path.join(a.state_dir, "latest.json"), "w", encoding="utf-8") as f:
        json.dump({"date": day, "threshold": a.threshold, "tables": results}, f, indent=1, sort_keys=True)
    with open(os.path.join(a.state_dir, "history.tsv"), "a", encoding="utf-8") as f:
        for t, r in sorted(results.items()):
            f.write("%s\t%s\t%d\t%d\t%d\n" % (day, t, r["orphans"], r["deletable"], r["bytes"]))

    new, still = evaluate(results, breached, a.threshold)
    failed = False
    for t in new:
        if a.no_issue:
            print("BREACH %s deletable=%d" % (t, results[t]["deletable"]))
            continue
        try:
            print("BREACH %s -> %s" % (t, file_issue(t, results[t], a.threshold, day, a.repo)))
        except (subprocess.CalledProcessError, OSError, ValueError) as e:
            # not recorded as breached, so the next run reports it again
            print("could not file the issue for %s: %s" % (t, e), file=sys.stderr)
            still = [x for x in still if x != t]
            failed = True
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(still, f)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
