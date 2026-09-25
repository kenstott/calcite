#!/usr/bin/env bash
#
# Stop all govdata ETL and runner activity, killing the entire process tree:
#   govdata-scheduled / govdata-runner systemd units
#   launcher (run-scheduled / run-pool / run-pool-persist / run-all-dq / worker-dq-run /
#             runner-daemon) -> worker shells -> EtlRunner JVMs, runner `claude -p` agents,
#             sync-to-r2 / rclone -> DuckDB CLIs, plus the heap sampler.
#
# Designed around three failure modes:
#   (1) RESPAWN: the scheduler relaunches the pool when it dies, so the systemd units are
#       stopped first. Both units run with KillMode=process (a stop signals only the main
#       script), so stopping them does NOT stop their children — the tree-kill below does.
#   (2) ORPHANS: when a parent shell dies its EtlRunner / DuckDB / agent children reparent to
#       init (pid 1), so a tree-walk from the launcher alone misses them. We therefore ALSO
#       sweep by JVM main-class / script name to catch reparented stragglers.
#   (3) SELF-KILL: a broad pattern can match the CALLER's own command line (a nohup launcher, a
#       monitoring cron, or a runner agent that invoked this script) and kill the very task that
#       invoked teardown. We exclude this script's pid, every ancestor, and its entire process
#       group from every kill, so it can never cut its own branch.
#
# Exits non-zero if a systemd unit could not be stopped or any process survived.
# Safe to run anytime; idempotent. Preserves /tmp/heap-peaks.tsv first.
set +e

SELF=$$
SELF_PG=$(ps -o pgid= -p "$SELF" 2>/dev/null | tr -d ' ')
RUNS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/runs"
UNITS="govdata-scheduled.service govdata-runner.service"

# Launcher roots: everything below each is torn down bottom-up.
LAUNCHERS='run-scheduled\.sh|run-pool-persist\.sh|run-pool-resume\.sh|run-pool\.sh|run-all-dq\.sh|worker-dq-run\.sh|runner-daemon-openrouter\.py'
# Stragglers reparented to init when their parent died.
ORPHANS='EtlRunner|GovDataSchemaFactory|worker\.sh|worker-[a-z_]+\.sh|worker-dq-run\.sh|claude -p You are the|sync-to-r2\.sh'

# Ancestors of this script (parent, grandparent, ... up to init) are never killed.
ANCESTORS=" "
_p=$SELF
while [ -n "$_p" ] && [ "$_p" -gt 1 ] 2>/dev/null; do
  ANCESTORS="$ANCESTORS$_p "
  _p=$(ps -o ppid= -p "$_p" 2>/dev/null | tr -d ' ')
done

# 0. Preserve the heap-peak sampler output before anything is torn down.
if [ -f /tmp/heap-peaks.tsv ]; then
  cp /tmp/heap-peaks.tsv "$RUNS/heap-peaks.tsv" 2>/dev/null && echo "preserved heap-peaks.tsv"
fi

# skip(pid): true (skip it) if pid is us, an ancestor, in our process group, or pid<=1.
skip() {
  local pid=$1 pg
  [ -z "$pid" ] && return 0
  { [ "$pid" -le 1 ]; } 2>/dev/null && return 0
  case "$ANCESTORS" in *" $pid "*) return 0 ;; esac
  pg=$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')
  [ -n "$pg" ] && [ "$pg" = "$SELF_PG" ] && return 0
  return 1
}

# tree(pid): emit pid and all descendants, children first (bottom-up) for clean teardown.
tree() {
  local p=$1 k
  for k in $(pgrep -P "$p" 2>/dev/null); do tree "$k"; done
  echo "$p"
}

sig_pid() { skip "$1" || kill "-$2" "$1" 2>/dev/null; }

RC=0

# 1. Stop the systemd units so nothing relaunches the pool or the agents. Needs privilege:
#    try unattended first, then passwordless sudo, and say so loudly if neither works.
for unit in $UNITS; do
  systemctl is-active --quiet "$unit" 2>/dev/null || continue
  systemctl stop --no-ask-password "$unit" 2>/dev/null \
    || sudo -n systemctl stop "$unit" 2>/dev/null
  if systemctl is-active --quiet "$unit" 2>/dev/null; then
    echo "WARNING: $unit is still active and may relaunch work — run: sudo systemctl stop $unit" >&2
    RC=1
  else
    echo "stopped $unit"
  fi
done

# 2. Stop the heap sampler (exact script name; cannot match this script).
for pid in $(pgrep -f '_heapsample\.sh' 2>/dev/null); do sig_pid "$pid" KILL; done

# 3. Tree-kill each launcher root, bottom-up: TERM (let JVMs flush) then KILL.
launchers=$(pgrep -f "$LAUNCHERS" 2>/dev/null)
for sig in TERM KILL; do
  for root in $launchers; do
    skip "$root" && continue
    for pid in $(tree "$root"); do sig_pid "$pid" "$sig"; done
  done
  [ "$sig" = TERM ] && sleep 3
done

# 4. Sweep orphaned ETL and agent processes by name (reparented to init when their parent died).
for sig in TERM KILL; do
  for pid in $(pgrep -f "$ORPHANS" 2>/dev/null); do
    skip "$pid" && continue
    for p in $(tree "$pid"); do sig_pid "$p" "$sig"; done
  done
  [ "$sig" = TERM ] && sleep 2
done

# 5. rclone and DuckDB CLIs (exact binary names).
for pid in $(pgrep -x rclone 2>/dev/null); do sig_pid "$pid" KILL; done
for pid in $(pgrep -x duckdb 2>/dev/null); do sig_pid "$pid" KILL; done

sleep 2

# count(pattern): live matches, excluding this script and its ancestors.
count() {
  local n=0 pid
  for pid in $(pgrep -f "$1" 2>/dev/null); do skip "$pid" || n=$((n + 1)); done
  echo "$n"
}
launcher_n=$(count "$LAUNCHERS")
orphan_n=$(count "$ORPHANS")
rclone_n=$(pgrep -xc rclone 2>/dev/null); rclone_n=${rclone_n:-0}
duckdb_n=$(pgrep -xc duckdb 2>/dev/null); duckdb_n=${duckdb_n:-0}
sampler_n=$(count '_heapsample\.sh')
echo "survivors: launcher=$launcher_n etl_and_agents=$orphan_n rclone=$rclone_n duckdb=$duckdb_n sampler=$sampler_n"
if [ $((launcher_n + orphan_n + rclone_n + duckdb_n + sampler_n)) -gt 0 ]; then
  RC=1
fi
exit $RC
