#!/usr/bin/env bash
#
# Set up GitHub-issue-based govdata defect tracking, replacing the Defect Register and
# Remediation Ledger artifacts. See docs/ops/defect-tracking.md for what it builds and why.
#
#   ./setup-issue-tracking.sh            # rehearse everything, change nothing (default)
#   ./setup-issue-tracking.sh --apply    # actually create the repo, labels and issues
#
# Safe to re-run. The repo is created only if missing, labels reconcile idempotently, and the
# migration refuses to run twice unless you pass --force-migrate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REPO="${GH_OPS_REPO:-kenstott/govdata-ops}"
REGISTER="$SCRIPT_DIR/artifacts/defect-register.html"
LEDGER="$SCRIPT_DIR/artifacts/remediation-ledger.html"
OUT="${TMPDIR:-/tmp}/govdata-issue-migration"
APPLY=0
ASSUME_YES=0
FORCE_MIGRATE=0
SKIP_REPO=0
SKIP_LABELS=0
SKIP_MIGRATE=0

usage() {
  awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "${BASH_SOURCE[0]}"
  cat <<'EOF'

Options:
  --apply               Perform the changes. Without it, nothing is created.
  --repo OWNER/NAME     Ops repo (default: $GH_OPS_REPO, else kenstott/govdata-ops)
  --register PATH       Defect Register HTML   (default: ops/artifacts/defect-register.html)
  --ledger PATH         Remediation Ledger HTML (default: ops/artifacts/remediation-ledger.html)
  --out DIR             Where the reviewable plan is written
  --yes                 Don't prompt for confirmation (for unattended runs)
  --force-migrate       File issues even though the repo already has some
  --skip-repo           Assume the repo exists
  --skip-labels         Don't touch labels
  --skip-migrate        Set up repo and labels only
  -h, --help            This message
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --apply) APPLY=1 ;;
    --repo) REPO="$2"; shift ;;
    --register) REGISTER="$2"; shift ;;
    --ledger) LEDGER="$2"; shift ;;
    --out) OUT="$2"; shift ;;
    --yes|-y) ASSUME_YES=1 ;;
    --force-migrate) FORCE_MIGRATE=1 ;;
    --skip-repo) SKIP_REPO=1 ;;
    --skip-labels) SKIP_LABELS=1 ;;
    --skip-migrate) SKIP_MIGRATE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf '\033[33m    ! %s\033[0m\n' "$*"; }
die()  { printf '\033[31m    x %s\033[0m\n' "$*" >&2; exit 1; }

# A usable terminal to prompt on. Absent when the script is run from a non-interactive
# context (a Claude Code `!` command, CI, a pipe), where the prompts can't be answered.
have_tty() { [ -r /dev/tty ] && { : </dev/tty; } 2>/dev/null; }

confirm() {
  [ "$ASSUME_YES" -eq 1 ] && return 0
  printf '\n    %s [y/N] ' "$1"
  read -r reply </dev/tty
  case "$reply" in [yY]*) return 0 ;; *) die "stopped at your request" ;; esac
}

# --------------------------------------------------------------------------- preflight

step "Preflight"

command -v gh >/dev/null || die "gh CLI not found"
gh auth status >/dev/null 2>&1 || die "gh is not authenticated — run: gh auth login"
info "gh $(gh --version | head -1 | awk '{print $3}'), authenticated as $(gh api user --jq .login)"

command -v python3 >/dev/null || die "python3 not found"
python3 -c 'import bs4' 2>/dev/null || die "python3 bs4 missing — pip install beautifulsoup4"
info "python3 $(python3 --version | awk '{print $2}') with bs4"

[ -f "$REGISTER" ] || die "register HTML not found: $REGISTER"
[ -f "$LEDGER" ]   || die "ledger HTML not found: $LEDGER"
info "register: $REGISTER ($(du -h "$REGISTER" | cut -f1), modified $(date -r "$REGISTER" '+%Y-%m-%d %H:%M'))"
info "ledger:   $LEDGER ($(du -h "$LEDGER" | cut -f1), modified $(date -r "$LEDGER" '+%Y-%m-%d %H:%M'))"

warn "These are point-in-time snapshots. If either artifact has been edited since, re-save it"
warn "(Artifact action:\"read\") and pass --register/--ledger, or the migration will miss those edits."

[ -x "$SCRIPT_DIR/gh-labels.sh" ] || die "missing $SCRIPT_DIR/gh-labels.sh"
[ -x "$SCRIPT_DIR/migrate-artifacts-to-issues.py" ] || die "missing $SCRIPT_DIR/migrate-artifacts-to-issues.py"

if [ "$APPLY" -eq 0 ]; then
  warn "REHEARSAL — nothing will be created. Re-run with --apply to perform the setup."
elif [ "$ASSUME_YES" -eq 0 ] && ! have_tty; then
  # Fail here, before anything is created, rather than at the first unanswerable prompt.
  die "no terminal to prompt on (non-interactive shell), and --yes was not given.

      This run would create a repo and file issues without being able to ask you first.
      Review the plan, then re-run with --yes to proceed unattended:

          govdata/scripts/ops/setup-issue-tracking.sh          # writes the plan
          govdata/scripts/ops/setup-issue-tracking.sh --apply --yes"
fi

# --------------------------------------------------------------------------- repo

REPO_EXISTS=0
if gh repo view "$REPO" >/dev/null 2>&1; then
  REPO_EXISTS=1
fi

if [ "$SKIP_REPO" -eq 1 ]; then
  step "Repo — skipped"
  [ "$REPO_EXISTS" -eq 1 ] || die "$REPO does not exist and --skip-repo was given"
elif [ "$REPO_EXISTS" -eq 1 ]; then
  step "Repo — $REPO already exists"
  vis=$(gh repo view "$REPO" --json visibility --jq .visibility)
  info "visibility: $vis"
  [ "$vis" = "PRIVATE" ] || warn "NOT private. Defect entries carry production paths, bucket names
      and reprocess commands. Make it private before migrating: gh repo edit $REPO --visibility private"
else
  step "Repo — $REPO does not exist"
  info "will create it as a PRIVATE repo"
  if [ "$APPLY" -eq 1 ]; then
    confirm "Create private repo $REPO?"
    gh repo create "$REPO" --private \
      --description "Govdata data-defect and remediation tracking" >/dev/null
    info "created $REPO"
    REPO_EXISTS=1
  fi
fi

# --------------------------------------------------------------------------- labels

if [ "$SKIP_LABELS" -eq 1 ]; then
  step "Labels — skipped"
elif [ "$APPLY" -eq 1 ]; then
  step "Labels"
  GH_OPS_REPO="$REPO" "$SCRIPT_DIR/gh-labels.sh" | tail -3
  info "$(gh label list --repo "$REPO" --limit 200 | wc -l) labels now on $REPO"
else
  step "Labels — would create"
  GH_OPS_REPO="$REPO" "$SCRIPT_DIR/gh-labels.sh" --dry-run | sed -n '2p;4,8p'
  info "... and the rest; run with --apply to create them"
fi

# --------------------------------------------------------------------------- migration

if [ "$SKIP_MIGRATE" -eq 1 ]; then
  step "Migration — skipped"
  echo
  if [ "$APPLY" -eq 1 ]; then
    info "Done. Repo and labels are ready; nothing was migrated."
  else
    info "Rehearsal complete. Nothing was created; migration was skipped."
  fi
  exit 0
fi

step "Migration plan"
python3 "$SCRIPT_DIR/migrate-artifacts-to-issues.py" \
  --register "$REGISTER" --ledger "$LEDGER" --repo "$REPO" --out "$OUT"

echo
info "Per-issue markdown and plan.tsv are in $OUT"

if [ "$APPLY" -eq 0 ]; then
  echo
  info "Rehearsal complete. Review $OUT/plan.tsv, then re-run with --apply."
  exit 0
fi

# Filing issues is not idempotent — a second run would duplicate every one of them.
if [ "$REPO_EXISTS" -eq 1 ]; then
  existing=$(gh issue list --repo "$REPO" --state all --limit 200 \
               --label type:defect --json number --jq 'length' 2>/dev/null || echo 0)
  if [ "$existing" -gt 0 ] && [ "$FORCE_MIGRATE" -eq 0 ]; then
    warn "$REPO already has $existing type:defect issues — the migration has already run."
    warn "Re-running would duplicate every issue. Pass --force-migrate if you really mean to."
    exit 1
  fi
fi

echo
info "About to file the issues listed above in $REPO."
confirm "File them now?"

python3 "$SCRIPT_DIR/migrate-artifacts-to-issues.py" \
  --register "$REGISTER" --ledger "$LEDGER" --repo "$REPO" --out "$OUT" --create

# --------------------------------------------------------------------------- verify

step "Verifying the queues the runner roles depend on"

ready=$(gh issue list --repo "$REPO" --state open \
          --label type:remediation --label status:ready-to-run --json number --jq 'length')
defects=$(gh issue list --repo "$REPO" --state open --label type:defect --json number --jq 'length')
sourcing=$(gh issue list --repo "$REPO" --state open --label type:sourcing --json number --jq 'length')
info "remediation jobs ready to run : $ready"
info "open defects                  : $defects"
info "open sourcing items           : $sourcing"

step "Orphan check — defects marked ready with no job filed"
orphans=0
for n in $(gh issue list --repo "$REPO" --state open --label type:defect \
             --label status:ready-to-run --json number --jq '.[].number'); do
  c=$(gh api "repos/$REPO/issues/$n/sub_issues" --jq 'length' 2>/dev/null || echo 0)
  if [ "$c" -eq 0 ]; then
    info "ORPHAN: #$n has no remediation job"
    orphans=$((orphans + 1))
  fi
done
[ "$orphans" -eq 0 ] && info "none"

# --------------------------------------------------------------------------- done

step "Done"
cat <<EOF

    Tracking now lives in https://github.com/$REPO

    Add this to your shell profile so the runner skills find it:
        export GH_OPS_REPO=$REPO

    Remaining, by hand:
      1. Edit the two artifacts once to add a banner pointing at the ops repo,
         then stop writing to them. They stay as linked archives.
      2. Triage the migrated sourcing items; they all arrive as status:open.

    Reference: docs/ops/defect-tracking.md
EOF
