#!/usr/bin/env bash
#
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
# TEMPORARY, ISOLATED completion tracker for the `historical` pool alias's
# per-year (and :once) slots — NOT the IncrementalTracker/PGPipelineTracker
# used by the ETL pipeline itself, and NOT the old ~/.run-pool-completed.state
# checkpoint removed in c6f46f523 (which silently skipped whole schemas on
# re-run because --force was easy to forget). This one has no force flag:
# to redo a slot, delete its marker file (or the one line in it) — see below.
#
# State: one flat file per schema under $(hcy_state_dir), each line a completed
# slot token ("2020", "once", "historical"). Delete the whole directory to wipe
# every marker; delete one schema's file, or one line in it, to redo just that
# schema/year. This file is sourced only from run-pool.sh's `historical` alias
# branch — daily, dq, and explicit `schema:mode` invocations never consult it.

hcy_state_dir() {
  local dir="${GOVDATA_HISTORICAL_COMPLETE_DIR:-$SCRIPT_DIR/runs/historical-complete}"
  mkdir -p "$dir"
  echo "$dir"
}

hcy_is_complete() {   # <schema> <token>
  local schema="$1" token="$2" dir
  dir=$(hcy_state_dir)
  [ -f "$dir/${schema}.done" ] && grep -qxF "$token" "$dir/${schema}.done" 2>/dev/null
}

hcy_mark_complete() {   # <schema> <token> — flock-guarded so concurrent workers never lose an append
  local schema="$1" token="$2" dir
  dir=$(hcy_state_dir)
  (
    flock 9
    grep -qxF "$token" "$dir/${schema}.done" 2>/dev/null || echo "$token" >> "$dir/${schema}.done"
  ) 9>>"$dir/.lock"
}

# Append "<schema>:<token>" to the caller's $queue array unless already marked
# complete, and remember it in $HCY_TRACKED_SLOTS so the pool's completion
# handler knows to write a marker for it (and only it) on a clean exit.
hcy_enqueue() {   # <schema> <token>
  local schema="$1" token="$2" slot="$1:$2"
  if hcy_is_complete "$schema" "$token"; then
    log_info "[historical-complete] SKIP ${slot} — already marked complete in $(hcy_state_dir)/${schema}.done (delete that file, or the '${token}' line in it, to redo this slot)"
    return
  fi
  queue+=("$slot")
  HCY_TRACKED_SLOTS="${HCY_TRACKED_SLOTS}${slot}
"
}

hcy_is_tracked() {   # <slot> — was this slot enqueued via hcy_enqueue this run?
  printf '%s' "$HCY_TRACKED_SLOTS" | grep -qxF "$1"
}

# Guards sec_primary/sec_secondary/sec_13f only. DocumentETLProcessor.processAccession catches
# every per-accession failure (including retry-exhausted transient network errors) internally and
# always returns a normal result — it never throws — so worker.sh exits 0 even when a sustained
# outage caused EVERY accession in the year to fail. Without this check that year would still get
# marked complete below and silently never be retried by a later `historical` run. This does NOT
# catch a PARTIAL failure (some accessions failed, others succeeded) — that is a real gap but a
# different, harder problem (see DocumentETLProcessor's unconditional markProcessed-on-empty-result);
# this only refuses the total-wipeout case, which a transient/sustained outage for the whole run
# produces.
hcy_sec_all_failed() {   # <schema> <log_file>
  local schema="$1" log_file="$2"
  case "$schema" in
    sec_primary|sec_secondary|sec_13f) ;;
    *) return 1 ;;
  esac
  [ -f "$log_file" ] || return 1
  # Sum across every "Document ETL completed: N processed, N skipped, N failed" line in the log —
  # not just the last one — in case a single worker run makes more than one such pass.
  local processed=0 skipped=0 failed=0 found=0 p s f
  while read -r p s f; do
    found=1
    processed=$((processed + p))
    skipped=$((skipped + s))
    failed=$((failed + f))
  done < <(grep -oE "Document ETL completed: [0-9]+ processed, [0-9]+ skipped, [0-9]+ failed" "$log_file" \
            | sed -E 's/.*completed: ([0-9]+) processed, ([0-9]+) skipped, ([0-9]+) failed/\1 \2 \3/')
  [ "$found" -eq 1 ] || return 1
  [ "$processed" -eq 0 ] && [ "$skipped" -eq 0 ] && [ "$failed" -gt 0 ]
}

HCY_TRACKED_SLOTS=""
