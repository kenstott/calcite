---
description: "Diagnose a bug and file a structured GitHub issue via gh"
model: sonnet
effort: medium
---

# File GitHub Issue

Investigate a bug or defect and open a GitHub issue: $ARGUMENTS

`$ARGUMENTS` can be:
- A free-text description of the bug ("NullPointerException in SecFilingCache when year is null")
- A log snippet or error message pasted directly
- A file path + line number ("SecFilingCache.java:142 throws NPE")
- Empty — in which case use the current conversation context as the bug description

---

## Step 1 — Gather Evidence

Before writing anything, collect facts. Do not skip this step.

1. **Identify the failure point** — stack trace, log line, or test failure output.
2. **Find the source** — read the file(s) at the relevant lines. Confirm the code matches the symptom.
3. **Check recent history** — `git log --oneline -10 -- <file>` to see if a recent commit introduced it.
4. **Confirm reproducibility** — if a test exists, note its name and module. If not, note what would reproduce it.
5. **Check for existing issues** — `gh issue list --search "<keyword>" --limit 10` to avoid duplicates.

If an existing issue already covers this bug, report it to the user and stop.

---

## Step 2 — Classify the Issue

Assign one of:

| Label | When to use |
|---|---|
| `bug` | Incorrect behavior that contradicts the specification or expected contract |
| `regression` | Was working, now broken (tie to a commit if known) |
| `data-quality` | Produces wrong data silently (no exception, wrong rows/values) |
| `performance` | Correct behavior but unacceptable latency or resource use |
| `schema` | YAML schema definition error or missing table/column |

Add a severity:

| Severity | Criteria |
|---|---|
| `critical` | Data loss, silent corruption, production outage |
| `high` | Feature broken with no workaround |
| `medium` | Degraded behavior; workaround exists |
| `low` | Cosmetic, edge case, minor inconsistency |

---

## Step 3 — Draft the Issue

Compose a GitHub issue with this structure:

```markdown
## Summary

<One sentence: what breaks, where, under what condition.>

## Reproduction

<!-- Exact steps or test command that triggers the bug -->
1. <Step 1>
2. <Step 2>
3. Observe: <what happens>

**Expected:** <what should happen>
**Actual:** <what actually happens>

## Evidence

<!-- Stack trace, log output, or test failure — trimmed to the relevant portion -->
```
<error or log snippet>
```

## Root Cause (if known)

<!-- File path:line — what the code does wrong and why -->

## Affected Files

- `<path/to/File.java>` — <one-line description of relevance>

## Environment

- Branch: `<branch name>`
- Commit: `<short SHA>`
- Java: `<version if relevant>`
```

Keep the title short (≤70 chars): `<type>(<scope>): <what breaks>`
Examples:
- `bug(sec): NPE in SecFilingCache when year dimension is null`
- `regression(energy): eia_natural_gas_storage returns 0 rows after HttpSource fix`
- `data-quality(geo): state_fips mapping returns wrong FIPS for DC`

---

## Step 4 — Confirm Before Filing

Print the full draft to the user. Ask:

> "File this issue? (yes / edit / cancel)"

- **yes** — proceed to Step 5
- **edit** — accept corrections and re-show the draft
- **cancel** — stop; print the draft as a copyable block so the user can file manually

---

## Step 5 — File the Issue

```bash
gh issue create \
  --title "<title>" \
  --body "<body>" \
  --label "<label>,<severity>"
```

After filing, print the issue URL.

If `gh` is not authenticated or the repo has no remote, print the full draft as a Markdown block and instruct the user to paste it manually.

---

## Output Format (end of skill)

```
Issue filed: <URL>
Title: <title>
Labels: <labels>
```

Or if not filed:
```
Draft ready — file manually at: https://github.com/<owner>/<repo>/issues/new
```
