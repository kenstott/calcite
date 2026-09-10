#!/usr/bin/env bash
# Create (or update) the govdata defect-tracking label vocabulary in the ops repo.
# Idempotent: --force makes `gh label create` overwrite an existing label's colour and
# description, so re-running after editing this file reconciles the repo to it.
#
# Usage:
#   GH_OPS_REPO=kenstott/govdata-ops ./gh-labels.sh
#   ./gh-labels.sh --dry-run          # print what would be created
#
# See docs/ops/defect-tracking.md for what each label means.

set -euo pipefail

REPO="${GH_OPS_REPO:-kenstott/govdata-ops}"
DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

# name|colour|description
LABELS=(
  # kind — what class of problem
  "kind:wrong|A3342C|Bad data or no data delivered — transformer, ETL config, view, or raw-data fault"
  "kind:gap|8A6A16|Missing columns, tables, or schemas"
  "kind:core|5B54D6|Cross-cutting — planner, UDF, read path, MCP/tool layer"

  # type — which issue shape this is
  "type:defect|B60205|A reported data defect (was: Defect Register entry)"
  "type:remediation|0E8A16|A production reprocess job (was: Remediation Ledger row)"
  "type:infra|1D76DB|A cross-cutting code fix with no year axis"
  "type:sourcing|C5DEF5|Dataset availability research — can this data be onboarded at all"

  # status — exactly one at a time on an open issue
  "status:open|D93F0B|Never looked at"
  "status:investigating|FBCA04|Root-cause in progress"
  "status:partial|F5E7CE|Some improvements committed, more to do"
  "status:needs-dq|E99695|Fix committed, NOT yet DQ-validated — blocks the ledger-runner"
  "status:ready-to-run|0E8A16|DQ-validated, command written, safe against production"
  "status:running|1D76DB|A production job is in flight"
  "status:pending-etl|C2E0C6|Expected to resolve on a future scheduled ETL run"
  "status:blocked|5B6472|Viable but stuck on something outside this role — say what would unblock it"
  "status:watch|EDE6F7|Possible issue, no fix made, kept visible in case it recurs"

  # resolution — applied when closing
  "resolution:fixed|2E7D4A|Fixed and verified; original request fully addressed"
  "resolution:capped|7A4C14|Fixed to the achievable limit; completing the original request is not feasible"
  "resolution:not-available|444C58|The data does not exist upstream"
  "resolution:withdrawn|CFD3D7|Filed in error"
  "resolution:not-reproducible|2F5D7C|Assume filed in error, or already closed by an overlapping fix"
)

SCHEMAS=(
  ag banking census cftc crime cyber_threat cyber_vuln disasters econ econ_reference
  edu energy environment fec fedregister fiscal geo health housing lands officials
  patents ref research sec transport weather
)
for s in "${SCHEMAS[@]}"; do
  LABELS+=("schema:${s}|EFF3F6|govdata ${s} schema")
done

echo "Repo: $REPO"
echo "Labels: ${#LABELS[@]}"
echo

for entry in "${LABELS[@]}"; do
  IFS='|' read -r name colour desc <<< "$entry"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '  %-32s #%s  %s\n' "$name" "$colour" "$desc"
    continue
  fi
  gh label create "$name" \
    --repo "$REPO" \
    --color "$colour" \
    --description "$desc" \
    --force >/dev/null
  printf '  ok  %s\n' "$name"
done

[ "$DRY_RUN" -eq 1 ] && echo && echo "Dry run — nothing created."
exit 0
