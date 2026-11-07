#!/usr/bin/env bash
# Pair accounting for fix-sec.sh. A pair is "acc|table"; every file holds one pair per line.

# fix_sec_split_attempted <gap_pairs> <attempted_pairs>
# Prints the number of gap pairs that were attempted by an earlier pass and are still absent
# from their table. The gap list is built from the tables themselves, so every pair in it is
# absent; the attempted-list never removes one.
fix_sec_attempted_absent() {
  comm -12 <(sort -u "$1") <(sort -u "$2") | wc -l | tr -d ' '
}

# fix_sec_outcome <offered_pairs> <gap_pairs_after_pass>
# Prints "<materialized> <still_absent>": offered pairs whose table now holds a row, and offered
# pairs the table still lacks. Both are read from the table enumeration, never from the tracker.
fix_sec_outcome() {
  _still=$(comm -12 <(sort -u "$1") <(sort -u "$2") | wc -l | tr -d ' ')
  _offered=$(sort -u "$1" | wc -l | tr -d ' ')
  echo "$((_offered - _still)) $_still"
}

# fix_sec_cooldown_ended <worker_exit_code> <worker_log>
# Returns 0 when a worker round ended on the sec.gov host cool-down: the worker deferred (exit 75,
# #1084), or its log carries the cool-down text ("Host cool-down", "no sec.gov requests from this
# host until"). The cool-down message reads "HTTP 403/429" and never the bare "HTTP 429", so the
# rate-limit grep alone does not see it (#1113).
fix_sec_cooldown_ended() {
  [ "${1:-0}" -eq 75 ] && return 0
  [ -n "${2:-}" ] && [ -f "$2" ] \
    && grep -aqE "Host cool-down|no sec\.gov requests from this host until" "$2"
}
