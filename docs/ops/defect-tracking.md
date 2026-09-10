# Govdata defect tracking — GitHub issues

The Defect Register and Remediation Ledger are GitHub issues in a private ops repo. This file is
the contract: label vocabulary, the issue shapes, and the queries each runner role works from.

The two Claude artifacts are retired as stores of record. They remain as historical archives and
are linked from the issues that came out of them. Nothing writes to them any more.

## Repo

```bash
export GH_OPS_REPO="${GH_OPS_REPO:-kenstott/govdata-ops}"
```

A **private** repo, separate from `kenstott/calcite`, which is public. Defect entries carry
production bucket names, internal paths, reprocess commands, and detailed accounts of which
published datasets are wrong — none of which belongs in a public tracker.

Code bugs with no data dimension still belong in `kenstott/calcite` issues, as today. A defect
that is both (a read-path bug that makes a table unqueryable, say) is filed in the ops repo and
cross-linked; the ops repo is the one the runner roles work from.

## Issue shapes

There are three, distinguished by `type:`.

### `type:defect` — one reported data defect

Replaces a Defect Register entry. Title is `D-NNN <schema>.<table> — <one-line effect>`, keeping
the `D-NNN` id so older references still resolve by search. New defects continue the sequence.

Body sections, in order — these mirror the Register's four columns:

```markdown
## Effect as reported
## Analysis
## Suggested solution
```

Evidence goes in the body when it is the root-cause account, and in comments when it accumulates
during investigation. Comments are append-only: two sessions writing at once cannot clobber
each other, which is the failure mode the artifact workflow spent most of its rules avoiding.

### `type:remediation` — one production job

Replaces a Remediation Ledger row. **Filed as a sub-issue of the defect it serves**, so the
Register→Ledger relationship is a real link rather than a name match across two documents.

Title is `<slot>:<year-range> <tables> — <what it fixes>`. Body:

```markdown
## Run command
```bash
<exact, complete, executable command>
```
## Prerequisites
## Verification after run
```

A job with no year axis says so in the title (`n/a (snapshot tables)`), as the Ledger did.

### `type:sourcing` — one dataset gap, where no table exists at all

Replaces the register's "Dataset Availability Research" table ("What we looked for and could not
find"). Worked by the sourcing-runner. Title is `sourcing: <dataset sought>`. Body:

```markdown
**Needed for** — <the question or analysis it blocks>
**Confirmed via** — <what was already searched>
## Finding
```

Three outcomes: **built** (close completed, `resolution:fixed`), **not viable** (close not planned,
`resolution:not-available`), **blocked** (stays open on `status:blocked`). A closed
`resolution:not-available` is a finding, not settled fact — reopen it if later research says
otherwise.

### `type:infra` — a cross-cutting fix with no year axis

Replaces the Ledger's "Infrastructure & cross-cutting fixes" table. Not a sub-issue of anything;
touches every schema sharing the affected code path.

## Labels

### `kind:` — what class of problem (defects only)

| Label | Meaning |
|---|---|
| `kind:wrong` | Bad data, or no data, delivered — transformer, ETL config, view, or raw-data fault |
| `kind:gap` | Missing columns, tables, or schemas |
| `kind:core` | Cross-cutting — planner, UDF, read path, MCP/tool layer; not scoped to one schema's data |

### `status:` — exactly one at a time, on open issues

| Label | Meaning | Who acts |
|---|---|---|
| `status:open` | Never looked at | defect-register-runner |
| `status:investigating` | Root-cause in progress | defect-register-runner |
| `status:partial` | Some improvements committed, more to do | defect-register-runner |
| `status:needs-dq` | Fix committed, **not yet DQ-validated** | defect-register-runner |
| `status:ready-to-run` | DQ-validated, command written, safe against production | remediation-ledger-runner |
| `status:running` | A production job is in flight | remediation-ledger-runner |
| `status:pending-etl` | Expected to resolve on a future scheduled run; no explicit job | nobody — recheck later |
| `status:blocked` | Viable but stuck on something outside the role — usually a credential with no self-service registration | whoever can unblock it |
| `status:watch` | Possible issue, no fix made, kept visible in case it recurs | nobody — recheck later |

`status:blocked` is not the same as closing something as not viable: the work is known to be
possible, and the issue stays open naming what would unblock it. Blocked items are the cheapest
thing in the backlog to recheck, since the blocker is usually external and can change on its own.

`status:needs-dq` is the handoff boundary between the two roles, and it is the reason this
vocabulary exists. In the artifacts that prerequisite lived in prose — a Ledger row whose evidence
paragraph said "PREREQUISITE — do not run this against production yet." A label makes it a
condition the ledger-runner's own query filters on, so a job that is not ready cannot be picked up
by accident.

### `resolution:` — applied when closing

| Label | Close as | Meaning |
|---|---|---|
| `resolution:fixed` | completed | Fixed and verified; original request fully addressed |
| `resolution:capped` | completed | Fixed to the achievable limit; completing the original request is not feasible |
| `resolution:not-available` | not planned | The data does not exist upstream |
| `resolution:withdrawn` | not planned | Filed in error — incorrect, a bad idea, doesn't hold up |
| `resolution:not-reproducible` | not planned | Assume filed in error, or overlapped with a fix that already closed it |

Closed state carries "is it done"; the label carries "in what sense." Both are needed — `capped`
and `fixed` are both closed-completed but mean different things to someone deciding whether to
refile.

### `schema:` — one per govdata schema

`ag banking census cftc crime cyber_threat cyber_vuln disasters econ econ_reference edu energy
environment fec fedregister fiscal geo health housing lands officials patents ref research sec
transport weather`

A `kind:core` defect that spans schemas takes no `schema:` label, or several if the blast radius
is known.

## The queries that replace reading a document

Each of these is one cheap call. None of them requires holding a 90KB–800KB page in context.

```bash
R="$GH_OPS_REPO"

# ledger-runner's work queue — jobs that are actually safe to run
gh issue list -R "$R" --state open \
  --label type:remediation --label status:ready-to-run \
  --json number,title,labels

# defect-register-runner's work queue
gh issue list -R "$R" --state open --label type:defect \
  --search 'label:status:open OR label:status:investigating OR label:status:needs-dq'

# what is in flight right now, across every session
gh issue list -R "$R" --state open --label status:running --json number,title,assignees

# one item, in full
gh issue view <N> -R "$R" --comments
```

### The orphan check, as a query

A defect that is DQ-validated but has no job filed is invisible work — the Ledger looks caught up
while ready work sits stranded one artifact over. This happened in practice and cost hours of
idle scans. As sub-issues it is a mechanical check rather than a prose-matching heuristic:

```bash
# defects marked ready to run that have no remediation sub-issue
for n in $(gh issue list -R "$R" --state open --label type:defect \
             --label status:ready-to-run --json number --jq '.[].number'); do
  c=$(gh api "repos/$R/issues/$n/sub_issues" --jq 'length')
  [ "$c" -eq 0 ] && echo "ORPHAN: #$n has no remediation job"
done
```

Not "does any Ledger row mention this schema+table," which is what the artifact workflow had to
approximate. A defect either has a linked job or it does not.

## Filing a sub-issue

The sub-issue API takes the child's **database id**, not its issue number:

```bash
child_id=$(gh api "repos/$R/issues/<child_number>" --jq '.id')
gh api "repos/$R/issues/<parent_number>/sub_issues" -F sub_issue_id="$child_id"
```

## Linking a fix to a defect

Put `Fixes <owner>/<repo>#<N>` in the commit message that carries the code fix. The commit is then
linked from the issue timeline automatically, and the issue closes when the commit reaches the
default branch. This replaces writing the commit SHA into a prose paragraph by hand — though
keep doing that too where the SHA matters to the account (which of two commits a jar must include,
for instance; that detail is load-bearing and belongs in the text).

## What `status:running` is and is not

It is a visible marker that a production job is in flight, readable by every session including
ones that started after the job did. It is **not** a lock: there is no atomic
check-and-set, and two sessions can label the same issue within the same second.

`check_schema_year_conflict()` in `common.sh` remains the authority on same-schema+year
concurrency, and checking live `EtlRunner`/`worker.sh` processes yourself remains mandatory before
launching. The label is a third signal, not a replacement for either.

## Migration runbook

Verified end to end in dry-run against the live artifacts on 10 Sep 2026: **69 issues** — 24
defects, 5 remediation jobs, 14 infrastructure fixes, 26 sourcing rows; 35 open, 34 closed.

One script does the whole thing — preflight, repo, labels, migration, verification:

```bash
govdata/scripts/ops/setup-issue-tracking.sh            # rehearse: changes nothing
govdata/scripts/ops/setup-issue-tracking.sh --apply    # do it, prompting before each change
```

It is safe to re-run: the repo is created only if missing, labels reconcile idempotently, and
the migration refuses to run a second time (which would duplicate every issue) unless you pass
`--force-migrate`. `--skip-repo`, `--skip-labels` and `--skip-migrate` let you redo one stage.

The two artifact snapshots it reads live in `govdata/scripts/ops/artifacts/`. They are
point-in-time copies — if either artifact has been edited since, re-save it (`Artifact
action:"read"` writes the full HTML to a file and names the path) and pass `--register` /
`--ledger`, or those edits are lost.

The underlying pieces can also be run on their own:

```bash
govdata/scripts/ops/gh-labels.sh [--dry-run]
govdata/scripts/ops/migrate-artifacts-to-issues.py --register X.html --ledger Y.html [--create]
```

Afterwards, edit the artifacts once to add a banner pointing at the ops repo, and stop writing
to them.

### What the dry run reports, and why to read it

- **Downgraded to `status:needs-dq`** — entries whose own text says the job is not safe to run
  yet ("PREREQUISITE", "do not run this against production"). In the artifacts this was prose a
  reader had to notice; the script turns it into a label, and lists every one so the inference
  gets checked rather than trusted.
- **Job still open under a defect recorded as closed** — a Register/Ledger disagreement. The
  first run found one: **D-029** `sec.risk_factor_sections` is marked Resolved on the Register
  while its production job has never run. Worth resolving either way; as a parent/child pair it
  is visible from both ends from now on.
- **Remediation units with no parent defect** — these get created standalone rather than dropped.

### Known conversion limits

- Intra-register cross-references (`D-214` pointing at `D-217`) are kept as plain text, since the
  target's issue number doesn't exist until it is created. They stay searchable by id. A second
  pass could rewrite them to `#N` once `created.json` exists.
- Titles are built from the index summary, then the magnitude line, then the opening sentence —
  in that order. A handful of older entries have none of the three and fall back to the component
  name; those read thin and are worth a manual edit.
- The `type:sourcing` rows carry no per-row status in the artifact beyond an occasional badge, so
  they all migrate as `status:open` unless badged. Triage them once after migration.

## Archives

Nothing writes to these. They are linked from migrated issues for provenance.

| Artifact | Contents |
|---|---|
| [Defect Register](https://claude.ai/code/artifact/e5262dc4-e879-4bb4-9bea-d7c04c0b6d6d) | Active register at migration — 24 entries |
| [Resolved Archive](https://claude.ai/code/artifact/8933e681-f712-4bea-9459-11e5e0a525d3) | Resolved/withdrawn/capped defects split off 5 Sep 2026 |
| [Remediation Ledger](https://claude.ai/code/artifact/4a9d427b-f15f-4f5d-bd42-dd801e70edec) | Active ledger at migration — 5 units, 3 infra rows |
| [Ledger snapshot](https://claude.ai/code/artifact/ba6c0cb3-a9fe-4309-b4f4-ef33a7ab0f56) | Prior remediation round, archived 7 Sep 2026 |

The Resolved Archive is deliberately **not** migrated. It is closed history, it is large, and
every issue that references one of its entries links to it. Migrating it would mean hundreds of
closed issues nobody queries.
