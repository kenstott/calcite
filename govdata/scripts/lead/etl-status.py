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
"""Read-only snapshot of ETL state for the lead: phase, memory, pool budget, live jobs.

Derived from processes, the pool budget file and the run logs; it changes nothing.
Run-level detail (per-worker rollups) stays in govdata/scripts/parallel/pool_status.py.

Usage:  etl-status.py [--json]
"""
import argparse
import glob
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import leadlib  # noqa: E402

RECENT_S = 600


def newest(pattern):
    files = glob.glob(os.path.join(leadlib.RUNS_DIR, pattern))
    return max(files, key=os.path.getmtime) if files else None


def snapshot(procs=None):
    procs = procs if procs is not None else leadlib.process_table()
    jvms = [(p, e, c) for p, e, c in procs if "EtlRunner" in c and "java" in c.split()[0]]
    jobs = []
    for pid, et, cmd in procs:
        words = cmd.split()
        if len(words) < 2 or os.path.basename(words[0]) not in ("bash", "sh"):
            continue
        script = os.path.basename(words[1])
        if script in ("run-pool.sh", "force-reprocess.sh"):
            jobs.append({"pid": pid, "elapsed_s": et,
                         "kind": "pool" if script == "run-pool.sh" else "force-reprocess",
                         "args": " ".join(words[2:])[:120]})
    phase = "idle"
    sched = os.path.join(leadlib.RUNS_DIR, "pids", "scheduled.pid")
    try:
        with open(sched) as f:
            spid = int(f.read().strip())
    except (OSError, ValueError):
        spid = None
    if spid and leadlib.pid_alive(spid):
        log = newest("scheduled-*.log")
        phase = "scheduled-" + ("historical" if log and "historical" in os.path.basename(log) else "daily")
    elif jobs:
        phase = "remediation"
    budget = leadlib.read_conf(leadlib.BUDGET_FILE)
    locks = []
    for f in sorted(glob.glob(os.path.join(leadlib.LOCK_DIR, "*"))):
        try:
            with open(f) as fh:
                pid = int(fh.read().strip())
        except (OSError, ValueError):
            pid = None
        locks.append({"lock": os.path.basename(f), "pid": pid, "alive": bool(pid and leadlib.pid_alive(pid))})
    return {
        "time": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "mode": leadlib.mode(),
        "phase": phase,
        "mem_available_mb": leadlib.mem_available_mb(),
        "etl_jvms": len(jvms),
        "jobs": jobs,
        "pool_budget": {"reserve_mb": budget.get("RESERVE_MB"), "max_workers": budget.get("MAX_WORKERS")},
        "locks": locks,
    }


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    s = snapshot()
    if a.json:
        print(json.dumps(s, indent=2))
    else:
        print(f"mode={s['mode']} phase={s['phase']} free={s['mem_available_mb']}MB etl_jvms={s['etl_jvms']} "
              f"budget=reserve {s['pool_budget']['reserve_mb']}MB / workers {s['pool_budget']['max_workers']}")
        for j in s["jobs"]:
            print(f"  job {j['kind']} pid={j['pid']} {j['elapsed_s']}s {j['args']}")
        for l in s["locks"]:
            print(f"  lock {l['lock']} pid={l['pid']} alive={l['alive']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
