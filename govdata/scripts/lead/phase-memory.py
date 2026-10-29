#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Build the per-pipeline memory table admission uses, from .claude/etl-memory-log.csv.

Writes .claude/lead/phase-memory.json: {pipeline: {peak_mb, xmx_mb, samples}} using the largest
observed peak and the largest recommended Xmx per pipeline.
"""
import collections
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import leadlib  # noqa: E402


def build(csv_path):
    peak = collections.defaultdict(int)
    xmx = collections.defaultdict(int)
    n = collections.defaultdict(int)
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            p = r["pipeline"]
            peak[p] = max(peak[p], int(r["peakMB"]))
            xmx[p] = max(xmx[p], int(r["recommended_xmx_mb"]))
            n[p] += 1
    return {p: {"peak_mb": peak[p], "xmx_mb": xmx[p], "samples": n[p]} for p in sorted(peak)}


def main():
    src = os.path.join(leadlib.REPO, ".claude", "etl-memory-log.csv")
    table = build(src)
    os.makedirs(leadlib.LEAD_DIR, exist_ok=True)
    with open(leadlib.MEMORY_TABLE, "w") as f:
        json.dump(table, f, indent=1, sort_keys=True)
        f.write("\n")
    print(f"{len(table)} pipelines -> {leadlib.MEMORY_TABLE}")


if __name__ == "__main__":
    main()
