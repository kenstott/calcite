#!/usr/bin/env python3
"""Text dashboard for the govdata runners: what they are doing, what they fixed, what is stuck.

    runners-dashboard.py              print once
    runners-dashboard.py --watch 30   redraw every 30 seconds (Ctrl-C to stop)

Sources: the runner daemon (systemd + journal), the live `claude -p` agents, the
kenstott/govdata-ops issues (gh), the fix branches and merged commits (git), the ETL pool
and scheduled-window logs, and /proc/meminfo. A source that cannot be read is shown as an
error in its own section, never left out.
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

OPS_REPO = "kenstott/govdata-ops"
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RUNS = os.path.join(REPO, "govdata", "scripts", "parallel", "runs")
JOURNAL_WINDOW = "-6h"
GH_TTL = 60

USE_COLOR = sys.stdout.isatty()


def color(text, code):
    return "\033[%sm%s\033[0m" % (code, text) if USE_COLOR else text


def bold(t): return color(t, "1")
def dim(t): return color(t, "2")
def red(t): return color(t, "31")
def yellow(t): return color(t, "33")
def green(t): return color(t, "32")
def cyan(t): return color(t, "36")


def run(cmd, timeout=20, cwd=None):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    if p.returncode != 0:
        raise RuntimeError("%s: %s" % (" ".join(cmd[:3]), (p.stderr or p.stdout).strip()[:160]))
    return p.stdout


def age(seconds):
    seconds = int(seconds)
    if seconds < 90:
        return "%ds" % seconds
    if seconds < 5400:
        return "%dm" % (seconds // 60)
    if seconds < 172800:
        return "%dh%02dm" % (seconds // 3600, seconds % 3600 // 60)
    return "%dd" % (seconds // 86400)


def iso_age(ts):
    t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - t).total_seconds()


def clip(text, width):
    text = text.replace("\n", " ")
    return text if len(text) <= width else text[: max(width - 1, 0)] + "…"


# ── collectors: each returns data or raises; render() shows the error under its section ──

_gh_cache = {"at": 0, "data": None}
# On disk so an external loop (`watch ...`) that starts a fresh process each time still shares it.
_GH_CACHE_FILE = os.path.join(tempfile.gettempdir(), "runners-dashboard-issues-%d.json" % os.getuid())


def issues(ttl):
    if _gh_cache["data"] is None and os.path.exists(_GH_CACHE_FILE):
        with open(_GH_CACHE_FILE) as f:
            _gh_cache["data"] = json.load(f)
        _gh_cache["at"] = os.path.getmtime(_GH_CACHE_FILE)
    if _gh_cache["data"] is None or time.time() - _gh_cache["at"] > ttl:
        out = run(["gh", "issue", "list", "-R", OPS_REPO, "--state", "all", "--limit", "400",
                   "--json", "number,title,state,labels,updatedAt,closedAt"], timeout=60)
        _gh_cache["data"] = json.loads(out)
        _gh_cache["at"] = time.time()
        tmp = _GH_CACHE_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(out)
        os.replace(tmp, _GH_CACHE_FILE)
    for i in _gh_cache["data"]:
        i["labelset"] = {l["name"] for l in i["labels"]}
    return _gh_cache["data"]


def daemon():
    show = run(["systemctl", "show", "govdata-runner", "-p", "ActiveState,MainPID"])
    kv = dict(line.split("=", 1) for line in show.splitlines() if "=" in line)
    up = None
    if kv.get("MainPID", "0") != "0":
        up = int(run(["ps", "-o", "etimes=", "-p", kv["MainPID"]]).strip())
    return kv["ActiveState"], kv["MainPID"], up


def agents():
    found = []
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            with open("/proc/%s/cmdline" % pid, "rb") as f:
                argv = f.read().decode(errors="replace").split("\0")
        except OSError:
            continue                      # the process exited while we scanned
        if len(argv) < 3 or not argv[0].endswith("/claude") or argv[1] != "-p":
            continue
        m = re.search(r"You are the (\S+) skill.*?issue #(\d+)", argv[2], re.S)
        if m:
            found.append({"pid": pid, "age": int(run(["ps", "-o", "etimes=", "-p", pid]).strip()),
                          "skill": m.group(1), "issue": int(m.group(2))})
    return found


def branches():
    rows = []
    git_dir = run(["git", "rev-parse", "--git-common-dir"], cwd=REPO).strip()
    if not os.path.isabs(git_dir):
        git_dir = os.path.join(REPO, git_dir)
    for br in run(["git", "branch", "--list", "fix/*", "--format=%(refname:short)"], cwd=REPO).split():
        slug = br[len("fix/"):]
        ahead = int(run(["git", "rev-list", "--count", "main.." + br], cwd=REPO).strip())
        rows.append({"slug": slug, "ahead": ahead,
                     "dq": os.path.exists(os.path.join(git_dir, "fix-branch-dq", slug))})
    return rows


def merged_commits(hours=24):
    out = run(["git", "log", "--since=%d hours ago" % hours, "main",
               "--format=%h%x1f%ct%x1f%s%x1f%b%x1e"], cwd=REPO)
    rows = []
    for rec in out.split("\x1e"):
        rec = rec.strip("\n")
        if not rec:
            continue
        h, ct, subj, body = rec.split("\x1f")
        refs = sorted({int(n) for n in re.findall(r"govdata-ops#(\d+)", subj + body)})
        rows.append({"hash": h, "age": time.time() - int(ct), "subject": subj, "refs": refs})
    return rows


def release_state():
    tag = run(["git", "tag", "-l", "engine-v*", "--sort=-v:refname"], cwd=REPO).split()[0]
    unreleased = int(run(["git", "rev-list", "--count", tag + "..main"], cwd=REPO).strip())
    behind, ahead = run(["git", "rev-list", "--left-right", "--count", "origin/main...main"],
                        cwd=REPO).split()
    head = run(["git", "rev-parse", "HEAD"], cwd=REPO).strip()
    stamp = os.path.join(REPO, "govdata", "build", "libs", "sih-govdata.jar.commit")
    staged = open(stamp).read().strip() if os.path.exists(stamp) else None
    return tag, unreleased, int(ahead), int(behind), staged, head


def pool():
    out = run(["ps", "-eo", "pid,etimes,args"])
    managers = []
    for line in out.splitlines():
        m = re.match(r"\s*(\d+)\s+(\d+)\s+bash \S*run-pool\.sh\s+(.*)$", line)
        if m:
            managers.append({"pid": m.group(1), "age": int(m.group(2)), "args": m.group(3)})
    info = []
    for mg in managers:
        log, last = None, None
        fd_dir = "/proc/%s/fd" % mg["pid"]
        try:
            for fd in os.listdir(fd_dir):
                target = os.readlink(os.path.join(fd_dir, fd))
                if "/runs/pool-" in target and target.endswith(".log"):
                    log = target
        except OSError as e:
            raise RuntimeError("cannot read %s: %s" % (fd_dir, e))
        if log:
            with open(log, errors="replace") as f:
                lines = [l for l in f.read().splitlines()[-400:] if "Running:" in l]
            last = lines[-1] if lines else None
        mg["log"], mg["last"] = log, last
        info.append(mg)
    return info


def window():
    logs = sorted((os.path.join(RUNS, n) for n in os.listdir(RUNS)
                   if n.startswith("scheduled-") and n.endswith(".log")), key=os.path.getmtime)
    if not logs:
        return None, [], []
    log = logs[-1]
    with open(log, errors="replace") as f:
        text = f.read().splitlines()
    head = next((l for l in text if "Starting" in l and "window" in l), "")
    pre = [l for l in text if "pre-daily-release:" in l]
    errs = [l for l in text if "ERROR" in l][-4:]
    return head, pre[-6:], errs


def resources():
    mem = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":")
            mem[k] = int(v.split()[0]) // 1024
    swap_used = mem["SwapTotal"] - mem["SwapFree"]
    return mem["MemAvailable"], mem["MemTotal"], swap_used, mem["SwapTotal"]


def journal():
    out = run(["journalctl", "-u", "govdata-runner", "--since", JOURNAL_WINDOW, "--no-pager",
               "-o", "short-iso"], timeout=30)
    return out.splitlines()


# ── rendering ──

def section(title, width):
    return "\n" + bold(cyan("── %s %s" % (title, "─" * max(width - len(title) - 4, 0))))


def guarded(fn, *args):
    try:
        return fn(*args), None
    except Exception as e:  # shown in the section, not hidden
        return None, "%s: %s" % (type(e).__name__, e)


def status_of(issue):
    for s in ("status:investigating", "status:needs-dq", "status:ready-to-run", "status:running",
              "status:blocked", "status:waiting-resources", "status:open", "status:partial"):
        if s in issue["labelset"]:
            return s[len("status:"):]
    return "none"


def type_of(issue):
    for t in ("defect", "remediation", "sourcing"):
        if "type:" + t in issue["labelset"]:
            return t
    return "other"


def render(ttl):
    width = min(shutil.get_terminal_size((120, 40)).columns, 140)
    out = []
    now = datetime.now().astimezone()

    dm, dm_err = guarded(daemon)
    ag, ag_err = guarded(agents)
    iss, iss_err = guarded(issues, ttl)
    by_num = {i["number"]: i for i in iss} if iss else {}

    # header
    if dm:
        state, pid, up = dm
        d = (green("active") if state == "active" else red(state)) + \
            (" pid %s up %s" % (pid, age(up)) if up is not None else "")
    else:
        d = red("daemon unreadable (%s)" % dm_err)
    out.append(bold("GOVDATA RUNNERS") + "  " + now.strftime("%Y-%m-%d %H:%M:%S %Z") +
               "   daemon: " + d + "   agents: %s" % (len(ag) if ag is not None else "?"))

    # resources + release
    res, res_err = guarded(resources)
    rel, rel_err = guarded(release_state)
    line = []
    if res:
        avail, total, sw_used, sw_total = res
        line.append("mem %.0f/%.0f GB free  swap %.0f/%.0f GB" %
                    (avail / 1024, total / 1024, sw_used / 1024, sw_total / 1024))
    else:
        line.append(red("resources: " + res_err))
    if rel:
        tag, unreleased, ahead, behind, staged, head = rel
        jar = green("jar=HEAD") if staged == head else yellow("jar=%s HEAD=%s" % ((staged or "none")[:7], head[:7]))
        line.append("release %s (+%d unreleased)  origin: %d ahead %d behind  %s" %
                    (tag, unreleased, ahead, behind, jar))
    else:
        line.append(red("release: " + rel_err))
    out.append("  ".join(line))

    # pool
    out.append(section("ETL POOL", width))
    pl, pl_err = guarded(pool)
    if pl_err:
        out.append(red("  " + pl_err))
    elif not pl:
        out.append(yellow("  no pool running"))
    else:
        for mg in pl:
            out.append("  %-8s %-6s %s" % ("pid " + mg["pid"], age(mg["age"]), clip(mg["args"], 40)))
            if mg["last"]:
                out.append(dim("    " + clip(re.sub(r"^\[[^]]*\]\s*", "", mg["last"]), width - 4)))
    wn, wn_err = guarded(window)
    if wn:
        head, pre, errs = wn
        if head:
            out.append(dim("  " + clip(head, width - 2)))
        for l in pre[-3:]:
            out.append(dim("  " + clip(l, width - 2)))
    elif wn_err:
        out.append(red("  window log: " + wn_err))

    # agents
    out.append(section("AGENTS (current work)", width))
    if ag_err:
        out.append(red("  " + ag_err))
    elif not ag:
        out.append(yellow("  none running"))
    else:
        for a in sorted(ag, key=lambda x: x["age"]):
            title = by_num.get(a["issue"], {}).get("title", "?")
            skill = a["skill"].replace("-runner", "").replace("-register", "").replace("remediation-ledger", "remediation")
            out.append("  %-12s #%-4d %-6s %s" % (skill, a["issue"], age(a["age"]),
                                                   clip(title, width - 32)))
    br, br_err = guarded(branches)
    if br_err:
        out.append(red("  branches: " + br_err))
    for b in br or []:
        if not (b["ahead"] or b["dq"] or re.match(r"(defect|sourcing)-", b["slug"])):
            continue
        state = green("DQ passed") if b["dq"] else ("commits, no DQ yet" if b["ahead"] else "no commits yet")
        out.append(dim("  branch fix/%-22s %d commit(s) ahead  %s" % (b["slug"], b["ahead"], state)))

    # queue
    out.append(section("QUEUE (open issues)", width))
    if iss_err:
        out.append(red("  " + iss_err))
    else:
        for t in ("defect", "sourcing", "remediation"):
            counts = {}
            for i in iss:
                if i["state"] == "OPEN" and type_of(i) == t:
                    counts[status_of(i)] = counts.get(status_of(i), 0) + 1
            cells = ["%s %d" % (k, counts[k]) for k in sorted(counts, key=lambda k: -counts[k])]
            out.append("  %-12s %s" % (t, " · ".join(cells) if cells else dim("none")))

    # recent fixes
    out.append(section("RECENT FIXES (24h)", width))
    mc, mc_err = guarded(merged_commits)
    if mc_err:
        out.append(red("  commits: " + mc_err))
    else:
        shown = [c for c in mc if re.match(r"(fix|feat|docs|revert)", c["subject"])][:10]
        for c in shown:
            refs = " ".join("#%d" % r for r in c["refs"])
            out.append("  %-6s %s %-16s %s" % (age(c["age"]), c["hash"], refs,
                                                 clip(c["subject"], width - 36)))
        if not shown:
            out.append(dim("  none"))
    if iss:
        closed = [i for i in iss if i["state"] == "CLOSED" and i.get("closedAt")
                  and iso_age(i["closedAt"]) < 86400]
        closed.sort(key=lambda i: iso_age(i["closedAt"]))
        if closed:
            out.append(dim("  closed in 24h:"))
        for i in closed[:8]:
            res_l = next((l[len("resolution:"):] for l in i["labelset"] if l.startswith("resolution:")), "?")
            out.append("    %-6s #%-4d %-10s %s" % (age(iso_age(i["closedAt"])), i["number"], res_l,
                                                     clip(i["title"], width - 30)))

    # attention: blockers, stale running, failures, repeated retries
    out.append(section("BLOCKERS & ATTENTION (age = time since last update)", width))
    live_issues = {a["issue"] for a in ag or []}
    if iss:
        blocked = [i for i in iss if i["state"] == "OPEN" and
                   ({"status:blocked", "status:waiting-resources"} & i["labelset"])]
        blocked.sort(key=lambda i: iso_age(i["updatedAt"]))
        for i in blocked[:8]:
            out.append("  %-21s #%-4d %-6s %s" % (status_of(i), i["number"],
                                                  age(iso_age(i["updatedAt"])), clip(i["title"], width - 42)))
        stale = [i for i in iss if i["state"] == "OPEN" and "status:running" in i["labelset"]
                 and type_of(i) == "remediation" and i["number"] not in live_issues]
        for i in sorted(stale, key=lambda i: i["number"]):
            out.append("  %s #%-4d %-6s %s" % (red("STALE running   "), i["number"],
                                               age(iso_age(i["updatedAt"])),
                                               clip(i["title"] + "  (no live agent)", width - 34)))
        if not blocked and not stale:
            out.append(green("  none"))
    if pl:
        for mg in pl:
            m = re.search(r"Failed: (\d+)", mg["last"] or "")
            if m and int(m.group(1)) > 0:
                out.append(red("  pool pid %s: %s worker(s) failed" % (mg["pid"], m.group(1))))
    if wn:
        for l in wn[2]:
            out.append(red("  " + clip(l, width - 2)))

    jl, jl_err = guarded(journal)
    if jl_err:
        out.append(red("  daemon journal: " + jl_err))
    else:
        recent = time.time() - 600
        resumes = {}
        for l in jl:
            m = re.match(r"(\S+)\s+\S+\s+\S+\[\d+\]:\s+\[[!+]\] Resuming (?:stale status:running |waiting-resources: )?(\w+#\d+)", l)
            if m:
                try:
                    ts = datetime.fromisoformat(m.group(1)).timestamp()
                except ValueError:
                    continue
                if ts > recent:
                    resumes[m.group(2)] = resumes.get(m.group(2), 0) + 1
        repeated = {k: v for k, v in resumes.items() if v >= 5}
        if repeated:
            out.append(yellow("  daemon retried %s in the last 10 min without an agent to show for it" %
                              ", ".join("%s x%d" % kv for kv in sorted(repeated.items()))))

        # daemon activity
        out.append(section("DAEMON ACTIVITY (spawns, last 6h)", width))
        spawns = [l for l in jl if "Spawning agent" in l or "Orphan found" in l
                  or "Orphan reaped" in l or "Unreaped orphan" in l][-8:]
        for l in spawns:
            m = re.match(r"(\S+)\s+\S+\s+\S+\[\d+\]:\s+(.*)$", l)
            if m:
                ts = datetime.fromisoformat(m.group(1)).astimezone().strftime("%H:%M")
                out.append("  %s  %s" % (ts, clip(re.sub(r"^\[[^]]\]\s*", "", m.group(2)), width - 9)))
        if not spawns:
            out.append(dim("  none"))

    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watch", type=int, nargs="?", const=15, metavar="SECONDS",
                    help="redraw every SECONDS (15 when given without a value; minimum 5)")
    args = ap.parse_args()
    if args.watch is not None and args.watch < 5:
        ap.error("--watch must be at least 5 seconds: each redraw forks git, ps, systemctl and journalctl")
    if not args.watch:
        print(render(ttl=GH_TTL))
        return 0
    try:
        while True:
            frame = render(ttl=max(args.watch, GH_TTL))
            sys.stdout.write("\033[H\033[J" + frame + "\n" + dim("\nrefresh every %ds — Ctrl-C to exit" % args.watch) + "\n")
            sys.stdout.flush()
            time.sleep(args.watch)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
