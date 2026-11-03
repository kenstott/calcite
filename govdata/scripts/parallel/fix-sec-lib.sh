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
