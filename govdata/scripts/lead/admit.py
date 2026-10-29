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
"""Admission: may this job start now? ADMIT, WAIT (try later) or REFUSE (do not run).

Checks, in order:
  1. memory      free memory minus the job's estimate must leave HEADROOM_MB
  2. lock        one owner per schema (and per schema+year when --year is given); a lock whose pid is
                 dead is stale and ignored
  3. live work   no running pool/force-reprocess/worker already on the same schema (+year)
  4. heavy slot  jobs that apply data (--apply) take the single heavy lock
Exit codes: 0 ADMIT, 3 WAIT, 4 REFUSE.  With --take the lock is created, owned by --owner-pid.

Usage: admit.py --type defect|sourcing|remediation|infra [--schema S] [--year Y] [--pipeline P]
                [--est-mb N] [--apply] [--take --owner-pid PID] [--json]
"""
import argparse
import fcntl
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import leadlib  # noqa: E402

HEADROOM_MB = int(os.environ.get("ADMIT_HEADROOM_MB", "6000"))
DEFAULT_EST_MB = int(os.environ.get("ADMIT_DEFAULT_EST_MB", "4096"))
EXIT = {"ADMIT": 0, "WAIT": 3, "REFUSE": 4}


def estimate_mb(pipeline, est_mb):
    if est_mb:
        return est_mb
    if pipeline:
        try:
            with open(leadlib.MEMORY_TABLE) as f:
                table = json.load(f)
        except (OSError, ValueError):
            return DEFAULT_EST_MB
        row = table.get(pipeline)
        if row:
            return max(row["xmx_mb"], row["peak_mb"])
    return DEFAULT_EST_MB


def lock_name(schema, year):
    return f"{schema}-{year}" if year else schema


def lock_state(schema, year):
    """(owner_pid, alive) for the schema lock or the schema+year lock that conflicts, else None."""
    names = {lock_name(schema, None)}
    if year:
        names.add(lock_name(schema, year))
    else:
        # a schema-wide request conflicts with any year lock of that schema
        try:
            names.update(n for n in os.listdir(leadlib.LOCK_DIR) if n.startswith(schema + "-"))
        except OSError:
            pass
    for n in sorted(names):
        path = os.path.join(leadlib.LOCK_DIR, n)
        try:
            with open(path) as f:
                pid = int(f.read().strip())
        except (OSError, ValueError):
            continue
        if leadlib.pid_alive(pid):
            return n, pid
    return None


def live_work(schema, year, procs):
    """A running worker.sh for this schema (and year), or a running force-reprocess.sh on it.

    A pool's own command line is deliberately not matched: it keeps listing every spec it was started
    with after those workers have finished, so matching it reports work that is no longer running.
    Specs still queued inside a pool are not seen here; the schema lock covers daemon-started work."""
    wpat = re.compile(r"worker\.sh\s+%s\s+(?:daily|%s)\b" % (re.escape(schema), year or r"\d{4}"))
    for pid, _et, cmd in procs:
        words = cmd.split()
        if len(words) < 2 or os.path.basename(words[0]) not in ("bash", "sh"):
            continue
        script = os.path.basename(words[1])
        if script == "worker.sh" and wpat.search(cmd):
            return pid, script
        if script == "force-reprocess.sh" and ("--schema %s " % schema) in cmd + " ":
            return pid, script
    return None


def heavy_free():
    fd = os.open(leadlib.HEAVY_LOCK, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


def decide(a, procs=None, free_mb=None):
    procs = procs if procs is not None else leadlib.process_table()
    free = free_mb if free_mb is not None else leadlib.mem_available_mb()
    est = estimate_mb(a.pipeline, a.est_mb)
    if free - est < HEADROOM_MB:
        return "WAIT", f"memory: {free}MB free, job needs {est}MB, headroom {HEADROOM_MB}MB"
    if a.schema:
        held = lock_state(a.schema, a.year)
        if held:
            return "WAIT", f"lock {held[0]} held by pid {held[1]}"
        busy = live_work(a.schema, a.year, procs)
        if busy:
            return "WAIT", f"{a.schema}{'/' + a.year if a.year else ''} already running ({busy[1]} pid {busy[0]})"
    if a.apply and not heavy_free():
        return "WAIT", "heavy slot taken"
    return "ADMIT", f"{free}MB free, estimate {est}MB"


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--type", required=True, choices=["defect", "sourcing", "remediation", "infra"])
    ap.add_argument("--schema")
    ap.add_argument("--year")
    ap.add_argument("--pipeline")
    ap.add_argument("--est-mb", type=int)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--take", action="store_true")
    ap.add_argument("--owner-pid", type=int)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    if a.take and not (a.schema and a.owner_pid):
        ap.error("--take needs --schema and --owner-pid")
    verdict, reason = decide(a)
    if verdict == "ADMIT" and a.take:
        os.makedirs(leadlib.LOCK_DIR, exist_ok=True)
        with open(os.path.join(leadlib.LOCK_DIR, lock_name(a.schema, a.year)), "w") as f:
            f.write(str(a.owner_pid))
    if a.json:
        print(json.dumps({"verdict": verdict, "reason": reason, "mode": leadlib.mode()}))
    else:
        print(f"{verdict}: {reason}")
    return EXIT[verdict]


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
