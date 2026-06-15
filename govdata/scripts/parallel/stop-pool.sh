#!/usr/bin/env bash
#
# Cleanly stop the govdata DQ pool and every ETL descendant:
#   launcher (run-all-dq / run-pool-persist / run-pool) -> worker shells ->
#   EtlRunner JVMs -> DuckDB CLIs, plus the heap sampler.
#
# Designed around the two failure modes seen before:
#   (1) ORPHANS: when a parent shell dies its EtlRunner / DuckDB children reparent to
#       init (pid 1), so a tree-walk from the launcher alone misses them. We therefore
#       ALSO sweep by JVM main-class / process name to catch reparented stragglers.
#   (2) SELF-KILL: a broad pattern like 'run-all-dq' can match the CALLER's own command
#       line (the nohup launcher, or a monitoring cron) and kill the very task that
#       invoked teardown. We exclude this script's pid, its parent, and its entire
#       process group from every kill, so it can never cut its own branch.
#
# Safe to run anytime; idempotent. Preserves /tmp/heap-peaks.tsv first.
set +e

SELF=$$
PARENT=$PPID
SELF_PG=$(ps -o pgid= -p "$SELF" 2>/dev/null | tr -d ' ')
RUNS=/mnt/c/Users/Admin/calcite/govdata/scripts/parallel/runs

# 0. Preserve the heap-peak sampler output before anything is torn down.
if [ -f /tmp/heap-peaks.tsv ]; then
  cp /tmp/heap-peaks.tsv "$RUNS/heap-peaks.tsv" 2>/dev/null && echo "preserved heap-peaks.tsv"
fi

# skip(pid): true (skip it) if pid is us, our parent, in our process group, or pid<=1.
skip() {
  local pid=$1 pg
  [ -z "$pid" ] && return 0
  { [ "$pid" -le 1 ]; } 2>/dev/null && return 0
  [ "$pid" = "$SELF" ] && return 0
  [ "$pid" = "$PARENT" ] && return 0
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

# 1. Stop the heap sampler first (exact script name; cannot match this script).
for pid in $(pgrep -f '_heapsample\.sh' 2>/dev/null); do sig_pid "$pid" KILL; done

# 2. Tree-kill each pool launcher root, bottom-up: TERM (let JVMs flush) then KILL.
launchers=$(pgrep -f 'run-pool-persist\.sh|run-pool\.sh|run-all-dq\.sh' 2>/dev/null)
for sig in TERM KILL; do
  for root in $launchers; do
    skip "$root" && continue
    for pid in $(tree "$root"); do sig_pid "$pid" "$sig"; done
  done
  [ "$sig" = TERM ] && sleep 3
done

# 3. Sweep orphaned ETL processes by name (reparented to init when their shell died).
for sig in TERM KILL; do
  for pat in 'EtlRunner' 'GovDataSchemaFactory' 'worker-[a-z_]*-dq'; do
    for pid in $(pgrep -f "$pat" 2>/dev/null); do sig_pid "$pid" "$sig"; done
  done
  [ "$sig" = TERM ] && sleep 2
done

# 4. DuckDB CLIs (exact binary name).
for pid in $(pgrep -x duckdb 2>/dev/null); do sig_pid "$pid" KILL; done

sleep 2
echo "survivors:" \
  "launcher=$(pgrep -fc 'run-pool-persist\.sh|run-pool\.sh|run-all-dq\.sh' 2>/dev/null || echo 0)" \
  "etl=$(pgrep -fc EtlRunner 2>/dev/null || echo 0)" \
  "java=$(pgrep -fc GovDataSchemaFactory 2>/dev/null || echo 0)" \
  "duckdb=$(pgrep -xc duckdb 2>/dev/null || echo 0)" \
  "sampler=$(pgrep -fc _heapsample\.sh 2>/dev/null || echo 0)"
