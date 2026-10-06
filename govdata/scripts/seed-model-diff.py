#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Schema YAMLs whose model differs between two commits.

Prints one path per line. A file counts as changed only when its parsed content differs once the
generated `observedCoverage` blocks are removed: update-coverage-metadata.sh rewrites those (row
counts, timestamps) in every schema YAML each night, and comments and formatting carry no model, so
none of them should force a seed rebuild. A file added or deleted between the commits counts as
changed. A file that cannot be parsed is an error, never silently treated as unchanged.

Usage: seed-model-diff.py <base> [<head>]      (head defaults to HEAD; run inside the repository)
"""
import json
import subprocess
import sys

import yaml

PATHSPECS = ["govdata/src/main/resources/*-schema.yaml",
             "govdata/src/main/resources/**/*-schema.yaml"]
IGNORED_KEY = "observedCoverage"


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True)


def strip(node):
    if isinstance(node, dict):
        return {k: strip(v) for k, v in node.items() if k != IGNORED_KEY}
    if isinstance(node, list):
        return [strip(v) for v in node]
    return node


def model_at(rev, path):
    """Normalised model of `path` at `rev`, or None when the file does not exist there."""
    shown = git("show", "%s:%s" % (rev, path))
    if shown.returncode != 0:
        return None
    try:
        return json.dumps(strip(yaml.safe_load(shown.stdout)), sort_keys=True, default=str)
    except yaml.YAMLError as e:
        raise SystemExit("seed-model-diff: %s at %s does not parse: %s" % (path, rev, e))


def main():
    if len(sys.argv) not in (2, 3):
        raise SystemExit(__doc__)
    base, head = sys.argv[1], (sys.argv[2] if len(sys.argv) == 3 else "HEAD")
    listed = git("diff", "--name-only", base, head, "--", *PATHSPECS)
    if listed.returncode != 0:
        raise SystemExit("seed-model-diff: git diff failed: %s" % listed.stderr.strip())
    for path in sorted(set(listed.stdout.split())):
        if model_at(base, path) != model_at(head, path):
            print(path)


if __name__ == "__main__":
    main()
