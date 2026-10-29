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
"""Shared helpers for the lead tools: paths, the rollout mode, memory and process inspection.

The mode file (.claude/lead/mode) is the single rollback switch for the whole operating model:
  shadow   - log what would be refused, refuse nothing
  guard    - hooks and gates refuse role violations; admission is advisory
  enforce  - admission is binding and workers are confined to their worktree
Remove the file or write 'shadow' to turn enforcement off.
"""
import os
import re
import subprocess

REPO = os.environ.get("CLAUDE_PROJECT_DIR") or os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
LEAD_DIR = os.path.join(REPO, ".claude", "lead")
MODE_FILE = os.path.join(LEAD_DIR, "mode")
MEMORY_TABLE = os.path.join(LEAD_DIR, "phase-memory.json")
LOCK_DIR = os.path.join(LEAD_DIR, "locks")
RUNS_DIR = os.path.join(REPO, "govdata", "scripts", "parallel", "runs")
BUDGET_FILE = os.path.join(RUNS_DIR, "pool-budget.conf")
HEAVY_LOCK = os.environ.get("GOVDATA_HEAVY_LOCK", "/tmp/govdata-heavy.lock")
MODES = ("shadow", "guard", "enforce")


def mode():
    """The rollout mode; a missing or unreadable file means shadow (enforce nothing)."""
    try:
        with open(MODE_FILE) as f:
            m = f.read().strip()
    except OSError:
        return "shadow"
    return m if m in MODES else "shadow"


def mem_available_mb():
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    raise RuntimeError("MemAvailable not found in /proc/meminfo")


def read_conf(path):
    out = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip().strip('"')
    except OSError:
        pass
    return out


def process_table():
    """[(pid, etime_seconds, cmdline)] for every process, from one ps call."""
    res = subprocess.run(["ps", "-eo", "pid=,etimes=,args="], capture_output=True, text=True, check=True)
    rows = []
    for line in res.stdout.splitlines():
        m = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)", line)
        if m:
            rows.append((int(m.group(1)), int(m.group(2)), m.group(3)))
    return rows


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
