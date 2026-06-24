#!/usr/bin/env python3
"""Show the status of the current (or a given) run-pool job.

Parses the newest ``pool-*.log`` written by ``run-pool.sh`` under the runs
directory and prints a per-worker rollup: the latest overall counts, which
workers are running (with elapsed time + last activity), which finished OK,
and which failed (with the failure reason).

Usage:
    pool_status.py                       # newest pool log under ./runs
    pool_status.py --runs-dir PATH       # explicit runs dir
    pool_status.py --pool FILE           # a specific pool-*.log
    pool_status.py --watch 10            # refresh every 10s until done
    pool_status.py --no-color

Runs dir resolution order: --runs-dir, $RUNPOOL_RUNS_DIR, ./runs next to this
script, then ./runs in the cwd.
"""

import argparse
import glob
import os
import re
import sys
import time

# ── line patterns (see pool-*.log) ────────────────────────────────────────────
# [18:40:02] Running: 5  Done: 10  Failed: 2  Queued: 3  Restarts: 0  Heap: 22528/26563MB  Free: 10558MB  | worker-a worker-b
STATUS_RE = re.compile(
    r"^\[(?P<t>\d{2}:\d{2}:\d{2})\]\s+Running:\s+(?P<running>\d+)\s+Done:\s+(?P<done>\d+)\s+"
    r"Failed:\s+(?P<failed>\d+)\s+Queued:\s+(?P<queued>\d+)\s+Restarts:\s+(?P<restarts>\d+)\s+"
    r"Heap:\s+(?P<heap_used>\d+)/(?P<heap_total>\d+)MB\s+Free:\s+(?P<free>\d+)MB"
    # The final completion line has Running 0, no "| workers" suffix, and trailing spaces —
    # so the workers group is lazy/optional and trailing whitespace is absorbed; otherwise that
    # line fails to match and the monitor is stuck on the prior "Running 1" line forever.
    r"(?:\s+\|\s+(?P<workers>.*?))?\s*$"
)
# Sentinel printed by run-pool.sh after the monitor loop exits (all workers reaped).
POOL_COMPLETE_RE = re.compile(r"^=== Pool Complete ===\s*$")
# Final summary line, e.g. "Total: 20  Done: 19  Failed: 1  Restarts: 0"
TOTAL_RE = re.compile(
    r"^Total:\s+(?P<total>\d+)\s+Done:\s+(?P<done>\d+)\s+Failed:\s+(?P<failed>\d+)")
# [2026-06-17 15:48:00] worker-sec-dq-rebuild FAILED (40m): check launch.log for details
# [2026-06-17 15:27:35] worker-weather-dq-rebuild finished OK (22m)
EVENT_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+(?P<worker>worker-\S+)\s+"
    r"(?P<state>finished OK|FAILED)\s+\((?P<dur>[^)]+)\)(?::\s*(?P<reason>.*))?$"
)
#   worker-health-dq-rebuild     [7m5s] Processing batch 9 ...
ACTIVITY_RE = re.compile(r"^\s+(?P<worker>worker-\S+)\s+\[(?P<elapsed>[^\]]+)\]\s+(?P<msg>.*)$")


class C:
    """ANSI colors, blanked when disabled."""
    RESET = BOLD = DIM = RED = GREEN = YELLOW = CYAN = GREY = ""

    @classmethod
    def enable(cls):
        cls.RESET, cls.BOLD, cls.DIM = "\033[0m", "\033[1m", "\033[2m"
        cls.RED, cls.GREEN, cls.YELLOW = "\033[31m", "\033[32m", "\033[33m"
        cls.CYAN, cls.GREY = "\033[36m", "\033[90m"


def resolve_runs_dir(arg):
    if arg:
        return arg
    env = os.environ.get("RUNPOOL_RUNS_DIR")
    if env:
        return env
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runs")
    if os.path.isdir(here):
        return here
    return os.path.join(os.getcwd(), "runs")


def newest_pool_log(runs_dir):
    logs = glob.glob(os.path.join(runs_dir, "pool-*.log"))
    if not logs:
        return None
    return max(logs, key=os.path.getmtime)


def parse(path):
    """Return (last_status, events{worker:(state,dur,reason,ts)}, activity{worker:(elapsed,msg)}, final).

    ``final`` is None until the pool prints its ``=== Pool Complete ===`` sentinel, then a dict
    with the run totals — used to render a definitive COMPLETE banner and stop the watch loop.
    """
    last_status = None
    events = {}
    activity = {}
    final = None
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        complete_seen = False
        for line in fh:
            line = line.rstrip("\n")
            if POOL_COMPLETE_RE.match(line):
                complete_seen = True
                final = final or {}
                continue
            if complete_seen and final is not None and not final:
                m = TOTAL_RE.match(line)
                if m:
                    final = m.groupdict()
                continue
            m = STATUS_RE.match(line)
            if m:
                last_status = m.groupdict()
                continue
            m = EVENT_RE.match(line)
            if m:
                g = m.groupdict()
                # last event for a worker wins (it may be relaunched)
                events[g["worker"]] = (g["state"], g["dur"], (g.get("reason") or "").strip(), g["ts"])
                continue
            m = ACTIVITY_RE.match(line)
            if m:
                g = m.groupdict()
                activity[g["worker"]] = (g["elapsed"], g["msg"].strip())
    return last_status, events, activity, final


def schema_of(worker):
    """worker-<schema>-<mode>[-year] -> <schema> (best effort, for grouping)."""
    name = worker[len("worker-"):] if worker.startswith("worker-") else worker
    for mode in ("-dq-rebuild", "-dq-etl-resume", "-dq", "-historical", "-daily"):
        i = name.find(mode)
        if i != -1:
            return name[:i]
    return name


def _elapsed_str(pool_name, end_epoch):
    """Total wall-clock from the pool's start (encoded in its filename) to end_epoch."""
    m = re.search(r"pool-(\d{8})-(\d{6})", pool_name)
    if not m:
        return None
    try:
        start = time.mktime(time.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S"))
    except ValueError:
        return None
    secs = max(0, int(end_epoch - start))
    h, rem = divmod(secs, 3600)
    mnt, s = divmod(rem, 60)
    if h:
        return f"{h}h{mnt:02d}m"
    if mnt:
        return f"{mnt}m{s:02d}s"
    return f"{s}s"


def render(path, color):
    last_status, events, activity, final = parse(path)
    out = []
    pool_name = os.path.basename(path)
    mtime = os.path.getmtime(path)
    age = time.time() - mtime
    # Completed: span start→last write. Running: start→now (the live wall-clock).
    elapsed = _elapsed_str(pool_name, mtime if final is not None else time.time())
    if final is not None:
        # Pool exited cleanly — no ambiguous "stalled" guess.
        if final:
            tail = (f"  Total {final['total']} · {C.GREEN}Done {final['done']}{C.RESET}"
                    f" · {(C.RED if int(final['failed']) else C.GREY)}Failed {final['failed']}{C.RESET}")
        else:
            tail = ""
        out.append(f"{C.BOLD}run-pool: {pool_name}{C.RESET}  "
                   f"{C.BOLD}{C.GREEN}✓ POOL COMPLETE{C.RESET}{tail}")
    else:
        stale = " (no update >120s — may be finished/stalled)" if age > 120 else ""
        out.append(f"{C.BOLD}run-pool: {pool_name}{C.RESET}  "
                   f"{C.GREY}updated {int(age)}s ago{stale}{C.RESET}")
    if elapsed:
        out.append(f"  {C.BOLD}Elapsed {elapsed}{C.RESET}")

    if not last_status:
        out.append(f"{C.YELLOW}No status line parsed yet — pool may be starting up.{C.RESET}")
        return "\n".join(out)

    s = last_status
    out.append(
        f"  [{s['t']}]  {C.CYAN}Running {s['running']}{C.RESET}  "
        f"{C.GREEN}Done {s['done']}{C.RESET}  "
        f"{(C.RED if int(s['failed']) else C.GREY)}Failed {s['failed']}{C.RESET}  "
        f"Queued {s['queued']}  Restarts {s['restarts']}  "
        f"{C.GREY}Heap {s['heap_used']}/{s['heap_total']}MB · Free {s['free']}MB{C.RESET}"
    )

    running = [w for w in (s["workers"] or "").split() if w]
    terminal = set(events)
    running = [w for w in running if w not in terminal]  # reconcile late finishers
    failed = [(w, d) for w, d in events.items() if d[0] == "FAILED"]
    done = [(w, d) for w, d in events.items() if d[0] == "finished OK"]

    if running:
        out.append(f"\n{C.BOLD}RUNNING ({len(running)}){C.RESET}")
        for w in running:
            el, msg = activity.get(w, ("?", "(no activity line yet)"))
            if len(msg) > 110:
                msg = msg[:107] + "..."
            out.append(f"  {C.CYAN}●{C.RESET} {w:<34} {C.GREY}[{el}]{C.RESET} {msg}")

    if failed:
        out.append(f"\n{C.BOLD}{C.RED}FAILED ({len(failed)}){C.RESET}")
        for w, (_, dur, reason, ts) in sorted(failed):
            out.append(f"  {C.RED}✗{C.RESET} {w:<34} {C.GREY}({dur}, {ts}){C.RESET} {reason}")

    if done:
        out.append(f"\n{C.BOLD}{C.GREEN}DONE ({len(done)}){C.RESET}")
        for w, (_, dur, _, ts) in sorted(done):
            out.append(f"  {C.GREEN}✓{C.RESET} {w:<34} {C.GREY}({dur}){C.RESET}")

    nq = int(s["queued"])
    if nq:
        out.append(f"\n{C.GREY}QUEUED: {nq} schema(s) not yet started{C.RESET}")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description="Show run-pool job status.")
    ap.add_argument("--runs-dir", help="runs directory (default: ./runs next to this script)")
    ap.add_argument("--pool", help="explicit pool-*.log file to read")
    ap.add_argument("--watch", nargs="?", type=float, const=5.0, metavar="SECS",
                    help="refresh in place every SECS (default 5) until nothing is running")
    ap.add_argument("--no-color", action="store_true", help="disable ANSI colors")
    args = ap.parse_args()

    if not args.no_color and sys.stdout.isatty():
        C.enable()

    def pick():
        if args.pool:
            return args.pool if os.path.isfile(args.pool) else None
        return newest_pool_log(resolve_runs_dir(args.runs_dir))

    def show_once():
        path = pick()
        if not path:
            where = args.pool or resolve_runs_dir(args.runs_dir)
            print(f"No pool-*.log found ({where}).", file=sys.stderr)
            return None
        body = render(path, not args.no_color)
        return path, body

    if not args.watch:
        r = show_once()
        if r is None:
            sys.exit(1)
        print(r[1])
        return

    # In-place watch: clear once, hide cursor, then each frame homes the cursor and
    # clears per line (no full-screen wipe → no flicker, no scrolling). Output is
    # clamped to the terminal height so it never pushes the screen up.
    hide_cursor, show_cursor = "\033[?25l", "\033[?25h"
    sys.stdout.write("\033[2J\033[H" + hide_cursor)
    sys.stdout.flush()
    try:
        while True:
            r = show_once()
            if r is None:
                sys.exit(1)
            path, body = r
            lines = body.split("\n")
            try:
                rows = os.get_terminal_size().lines
            except OSError:
                rows = 0
            if rows and len(lines) > rows - 1:
                keep = max(1, rows - 2)
                hidden = len(lines) - keep
                lines = lines[:keep] + [
                    f"{C.GREY}… ({hidden} more lines — enlarge the terminal to see all){C.RESET}"]
            frame = "\033[H" + "".join(ln + "\033[K\n" for ln in lines) + "\033[J"
            sys.stdout.write(frame)
            sys.stdout.flush()
            # stop once the pool printed its completion sentinel, or nothing is running
            last_status, events, _, final = parse(path)
            running = [w for w in ((last_status or {}).get("workers") or "").split()
                       if w and w not in events]
            if final is not None:
                break
            if last_status and int(last_status["running"]) == 0 and not running:
                break
            time.sleep(args.watch)
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write(show_cursor + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
