#!/usr/bin/env bash
# schema-claim.sh -- one remediation run per (target bucket, schema, years), refused at LAUNCH.
#
# Sourced by force-reprocess.sh. check_schema_year_conflict (common.sh) already stops two WORKERS writing
# the same schema+years, but it only sees running workers: a run that was launched and is merely waiting
# for the schema (the daily pool, another remediation) has no worker yet, so a second launch of the same
# remediation sails through, sits in the queue for hours, and then redoes the same work. Agents that are
# respawned while their earlier job is still alive did exactly that (2026-10-04: ag nass_land_values x3,
# housing hmda_lar x2, sec_primary financial_line_items 2015 x2, ...).
#
# A claim is a small file under the claims dir naming the owning force-reprocess.sh. It conflicts with a
# new claim iff ALL of: same target bucket (GOVDATA_PARQUET_DIR) -- so a DQ run against the -dq bucket
# never conflicts with a production run --, the two schemas can write the same tables (the same rule as
# the worker guard, so sec_primary/sec_secondary/sec_13f stay concurrent), and the year ranges overlap.
# A daily run spans every year (0-9999), like the worker guard's keyword modes.
#
# A claim only counts while its owner is alive AND is still a force-reprocess.sh (a recycled pid does not
# hold one); dead claims are removed as they are found. The check and the write happen under one flock.

# claim_schema_years <claim_dir> <owner_pid> <bucket> <schema> <start> <end> <tables> [<owner_cmd>]
# <owner_cmd> is what the owner's command line must contain for the claim to count (default
# force-reprocess.sh; worker-dq-run.sh claims its own). Returns 0 and records the claim, or 1 with the
# owner described on stderr.
claim_schema_years() {
  local dir=$1 owner=$2 bucket=$3 schema=$4 start=$5 end=$6 tables=$7 pattern=${8:-force-reprocess.sh}
  mkdir -p "$dir"
  local f c_pid c_bucket c_schema c_start c_end c_tables c_since c_pattern rc=0
  (
    flock 9
    for f in "$dir"/*.claim; do
      [ -e "$f" ] || continue
      c_pid=""; c_bucket=""; c_schema=""; c_start=""; c_end=""; c_tables=""; c_since=""; c_pattern=""
      # shellcheck disable=SC1090
      source "$f" 2>/dev/null
      if [ -z "$c_pid" ] || ! kill -0 "$c_pid" 2>/dev/null \
         || ! tr '\0' ' ' < "/proc/$c_pid/cmdline" 2>/dev/null | grep -qF "${c_pattern:-force-reprocess.sh}"; then
        rm -f "$f"; continue
      fi
      [ "$c_bucket" = "$bucket" ] || continue
      if declare -F _schemas_write_same_tables >/dev/null; then
        _schemas_write_same_tables "$c_schema" "$schema" || continue
      else
        [ "$c_schema" = "$schema" ] || continue
      fi
      if [ "$c_start" -le "$end" ] && [ "$start" -le "$c_end" ]; then
        echo "REFUSING: schema '$schema' years $start-$end (bucket $bucket) overlaps a remediation run that is already" \
             "in progress: ${c_pattern:-force-reprocess.sh} pid $c_pid, schema '$c_schema' years $c_start-$c_end," \
             "tables '$c_tables', running for $(( $(date +%s) - c_since ))s." \
             "One run per schema+years: wait for it to finish (or stop it) instead of launching another." >&2
        exit 1
      fi
    done
    {
      echo "c_pid=$owner"
      echo "c_bucket='$bucket'"
      echo "c_schema='$schema'"
      echo "c_start=$start"
      echo "c_end=$end"
      echo "c_tables='$tables'"
      echo "c_since=$(date +%s)"
      echo "c_pattern='$pattern'"
    } > "$dir/$owner.claim"
  ) 9>"$dir/.lock" || rc=1
  return $rc
}

# release_schema_years_claim <claim_dir> <owner_pid>
release_schema_years_claim() {
  rm -f "$1/$2.claim"
}
