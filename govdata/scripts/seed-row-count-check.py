#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Iceberg tables in a staged seed whose conversion tracker holds no row count.

Reads every <aperio>/<schema>/.conversions.json, prints one `schema.table` per line for each
ICEBERG_PARQUET record with no rowCount, and exits 1 if there is any. A tracker is either a flat
map of records or a map of warehouse root to such a map. A directory with no tracker at all is an
error: there is then nothing to check.

Usage: seed-row-count-check.py <aperio-dir>
"""
import glob
import json
import os
import sys


def is_record(node):
    return isinstance(node, dict) and "conversionType" in node


def records(tracker):
    """Yields (name, record) for a flat tracker or one namespaced by warehouse root."""
    for key, value in tracker.items():
        if is_record(value):
            yield key, value
        elif isinstance(value, dict):
            for name, record in value.items():
                if is_record(record):
                    yield name, record


def main(argv):
    if len(argv) != 2:
        sys.exit("usage: seed-row-count-check.py <aperio-dir>")
    trackers = sorted(glob.glob(os.path.join(argv[1], "*", ".conversions.json")))
    if not trackers:
        sys.exit("seed-row-count-check: no .conversions.json under %s" % argv[1])
    iceberg = 0
    missing = []
    for path in trackers:
        schema = os.path.basename(os.path.dirname(path))
        with open(path) as f:
            tracker = json.load(f)
        for name, record in records(tracker):
            if record.get("conversionType") != "ICEBERG_PARQUET":
                continue
            iceberg += 1
            if record.get("rowCount") is None:
                missing.append("%s.%s" % (schema, name))
    if missing:
        print("\n".join(sorted(missing)))
        sys.exit("seed-row-count-check: %d of %d Iceberg tables have no recorded row count"
                 % (len(missing), iceberg))
    print("seed-row-count-check: all %d Iceberg tables have a recorded row count" % iceberg)


if __name__ == "__main__":
    main(sys.argv)
