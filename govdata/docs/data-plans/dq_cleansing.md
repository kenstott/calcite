# db_cleansing — Standing Master-Data Cleansing Slot

**Status:** design only, not started.
**Date:** 2026-08-21.
**Origin:** generalizes the `ref.entity_overrides` / `ref.canonical_org_entity_resolved`
recommendation in the Govdata Defect Register (Batch 2, "Recommendation · not a defect") from a
one-schema proposal into a standing, warehouse-wide mechanism.
**Goal:** a new pool slot, `db_cleansing`, that always runs as the last step of the daily script,
using generative AI and statistical/algorithmic methods to surface *probable* master-data defects
— duplicate entities, mis-linked keys, implausible values — into a separate cleanse table with a
defined join-back path, never mutating the source table directly.

---

## 1. Problem

The existing DQ framework (`<schema>_dq.sql`, the T1–T7 pattern) catches what a fixed business
rule can state in advance: null checks, row-count floors, FK orphan checks, enumerated-value
membership, sign/range sanity. It cannot catch the class of defect this session's Defect Register
Batch 2 kept finding in `ref` (entity resolution) — defects that are only visible as a *pattern*
across many rows, not a violation of any single row's declared shape:

- **Duplicate/ambiguous identity** — six distinct LEIs sharing `source_name_normalized='apple'`,
  each carrying identically 190 mentions (B2-1). No row is individually malformed; the defect is
  that six rows agree suspiciously well with each other.
- **Arbitrary selection among near-duplicates** — `canonical_org_entity` linking "Schlumberger
  Technology Corporation" to the `patents_assignee_id` holding 1 patent instead of the one holding
  10,227, because nothing compares candidate ids against each other before picking one (B2-4). A
  magnitude check against a sibling row would have caught it; no per-row rule can.
- **Structurally invisible gaps** — `sec_cik` declared and present in the schema, populated in 1
  row out of 1,073,955, passing every per-row null-tolerance check because the column is nullable
  (B2-2).

None of these trip a T1–T7 assertion. All three are exactly the shape of problem a fuzzy-match /
clustering / outlier-scoring pass — or a model that can read two names and judge whether they're
the same organization — is suited to *flag*, not fix outright. The register's own proposed
solution for `ref` (`entity_overrides` + a resolved view) is the right shape for this. This plan
generalizes it into a mechanism, not a one-off table for one schema.

---

## 2. Architecture principles

Carried forward directly from the register's `entity_overrides` proposal — these are non-
negotiable, not just for `ref`:

1. **Detection is not correction.** A cleansing pass *proposes* candidates with a confidence
   score and evidence. Nothing is applied to a source table without a separate, later decision.
   No automatic write path from "detected" to "applied" in the first version of this mechanism —
   see §7 Phase 1.
2. **Non-destructive.** Corrections apply through a derived `_resolved`/`_cleansed` view, never a
   mutation of the source table. The matcher's/loader's raw output stays reproducible and
   measurable on its own; a source-table refresh (a normal ETL re-run) must not silently discard
   accumulated cleansing work, and cleansing work must not make the source table's own history
   harder to reason about.
3. **Provenance is mandatory.** Every candidate row carries what found it (`method`), how sure it
   is (`confidence`), and why (`evidence` — the actual query, score breakdown, or model
   explanation). A candidate without evidence is not a candidate worth keeping.
4. **"The algorithm can't know this" vs. "the algorithm has a bug."** If a whole class of
   candidates shares the same `issue_type`/`method` and keeps recurring, that is a signal to fix
   the upstream loader/matcher, not to keep approving individual corrections forever. This must be
   an enforced audit (§6), not a written-down hope, exactly per the register's framing.
5. **Structural fixes first, cleansing second.** Per the register's own sequencing note: fix what
   has a real, scoped root cause (a de-fanned bridge, a proper assignee-selection rule, a
   backfilled `sec_cik`) before building automation whose main job would otherwise be papering
   over those same root causes one row at a time. `db_cleansing` is for the residue after the
   register's Batch 1/2 structural fixes land, not a substitute for them.

---

## 3. Scope

**Primary target: `ref`.** `canonical_org_entity`, `entity_org_bridge`, the GLEIF mapping tables
— the schema that motivated this plan and the one with a concretely scoped starting point
(§7 Phase 1).

**Secondary target: master-data / reference tables in other schemas — TBD.** Any table that
functions as a shared dimension joined across many fact tables is a candidate: `geo.states`,
`geo.counties`, `census` cross-walk/reference tables, `econ_reference.nipa_tables` (already fixed
once this session for a uniqueness gap — Defect #7 — a plausible future cleansing target for
similar issues), `edu.ccd_districts`. **Not scoped now.** Extending beyond `ref` is explicitly
deferred to §7 Phase 4 and needs its own per-schema review before inclusion — a table only
qualifies once someone has confirmed it's genuinely shared reference data (joined from multiple
other tables) rather than a fact table that happens to have a natural key.

---

## 4. Cleanse-table pattern

Generalizes `ref.entity_overrides` into a reusable shape. Two real designs are possible; pick one
before Phase 1 starts (see open question in §8):

- **Per-target-schema table** (`ref.entity_overrides`, a hypothetical future
  `geo.reference_overrides`, etc.) — schema-local, simpler to reason about per-domain, but the
  pattern is duplicated per adopting schema.
- **One shared table** (`ref.dq_cleanse_candidates`) with `target_schema`/`target_table` columns
  — one place to query "everything currently flagged," but couples every adopting schema's
  cleansing data through `ref`.

Either way, the row shape is the same:

| column | meaning |
|---|---|
| `id` | candidate id |
| `target_schema`, `target_table` | what's being flagged |
| `target_key` | the row identifier being flagged (schema-appropriate: an LEI, a FIPS code, ...) |
| `issue_type` | `duplicate`, `mislink`, `outlier`, `missing_value`, `implausible_value`, ... |
| `proposed_action` | `merge`, `split`, `relink`, `exclude`, `correct_value` (mirrors `entity_overrides`' four kinds plus a value-correction case for non-entity master data) |
| `proposed_value` | the correction itself, when `proposed_action = correct_value` |
| `method` | which detector produced this (`fuzzy_match_v1`, `llm_review_v1`, `magnitude_outlier_v1`, ...) |
| `confidence` | numeric or categorical score from the detector |
| `evidence` | the query, score breakdown, or model explanation that justifies the candidate — **required**, never blank |
| `detected_at` | when the candidate was produced |
| `status` | `proposed` / `approved` / `rejected` / `applied` |
| `reviewed_by`, `reviewed_at`, `review_note` | the human (or later, a governed auto-apply policy — see §8) that moved it past `proposed` |

**Join-back.** A `<table>_resolved`/`<table>_cleansed` view left-joins the source table to only
the `status = 'applied'` candidates for that `target_table`, exposing `cleansing_applied` and
`cleansing_reason` alongside the original columns — exactly `canonical_org_entity_resolved`'s
`override_applied`/`override_reason` pattern, generalized. Callers who want the raw, unadulterated
loader output keep querying the source table directly; callers who want the best-known-correct
view query the `_resolved` view. Neither path is hidden from the other.

**Guardrail** (per the register's own note on `entity_overrides`): assert at build time that every
candidate's `target_key` still exists in `target_table`. An upstream id that gets reassigned or
dropped must not leave a stale `applied` candidate silently dropping a row from the resolved view.

---

## 5. Detection methods

Two families, meant to compose — statistical methods are cheap and deterministic and should run
first; generative-AI review is reserved for cases a deterministic pass can surface but not itself
judge.

**Statistical / algorithmic** (no external API calls, safe to run frequently):
- Fuzzy/normalized-name clustering (Jaro-Winkler, Levenshtein, or a normalized-token match) to
  surface near-duplicate identities — the class of thing that produced the "6 LEIs share 'apple'"
  finding.
- Magnitude/outlier comparison among rows that share a name or key but disagree by orders of
  magnitude — the Schlumberger pattern (B2-4): when several candidate ids exist for one name,
  compare their downstream counts rather than picking arbitrarily.
- Sparse-but-critical-column scans — flag a declared, analytically important column that is
  populated in a near-zero fraction of rows (the `sec_cik` pattern, B2-2), even though it passes
  every per-row nullability check.
- FK-orphan and cross-schema join-coverage checks, extended from the existing DQ pattern but
  aimed specifically at master-data linkage rather than fact-table completeness.

**Generative AI** (higher cost, reserved for judgment calls a deterministic score can't resolve
on its own):
- Semantic entity-match review: given two candidate names/records a statistical pass flagged as
  *possibly* the same entity, ask a model to judge same/different and produce the `evidence` text
  explaining why — turning a bare similarity score into an auditable reason.
- Anomaly explanation: for a magnitude outlier a statistical pass already found, draft the
  human-readable `evidence` describing what looks wrong and why, so a reviewer isn't starting from
  a bare row diff.
- Candidate ranking when a statistical method surfaces more matches than it can itself
  disambiguate.

Generative AI is a *drafting and judgment* layer on top of statistical detection, not a
replacement for it — every AI-produced candidate must still carry a `confidence` and `evidence`,
same as a purely statistical one, and is still subject to the same non-auto-apply rule (§2.1).

---

## 6. The audit guardrail (enforced, not aspirational)

Per §2.4: query the `status='applied'` (or `approved`) population grouped by `(target_table,
issue_type, method)`. A group with an unusually large count relative to its target table's row
count is a signal that a structural fix belongs upstream, not a backlog of individually-approved
rows. This should run as part of `db_cleansing`'s own reporting, not a manual, easy-to-skip
check — surface it in the same place a DQ run's pass/fail summary already lands (`dq_results` or
its analogue), so it's visible on every run rather than something someone has to remember to look
for.

---

## 7. Implementation phases

**Phase 0 — land the structural fixes first.** Per §2.5, this plan is not a substitute for
Batch 2's own fixes. Backfill `sec_cik` (B2-2), de-fan `entity_org_bridge`'s cross product (B2-1),
and fix `patents_assignee_id` selection by volume (B2-4) before building cleansing automation
whose main early workload would otherwise be exactly those three defects.

**Phase 1 — `ref` only, cleanse-table + resolved-view pattern.** Build `ref.entity_overrides` (or
the shared-table variant — decide per §8) and `ref.canonical_org_entity_resolved` as designed in
the register. Seed it from what's already known: the register's own findings (Schlumberger,
"apple" ×6) and the comparative-eval harness's labelled pairs, which the register notes are
already produced as a by-product of every pinned-entity-set run and double as a regression suite.
No automated detector yet — this phase proves the table shape and the join-back path against
real, already-understood cases before any detection code is written.

**Phase 2 — wire the `db_cleansing` pool slot.** Real integration point, modeled directly on the
existing embeddings post-step in `run-pool.sh` (the `$RUN_EMBEDDINGS` block, current end of file):
a best-effort, non-fatal post-ETL step gated by a new `$RUN_DB_CLEANSING` flag, positioned after
every other daily worker *and* after the embeddings refresh — "very last" in the literal execution
order of the `daily` alias, not just last in the schema list. Mirror `vss-local.sh`'s structure
for the worker script itself (`db-cleansing.sh`): delta-driven and time-boxed
(`DB_CLEANSING_MAX_SECONDS`/`DB_CLEANSING_MAX_ROWS`, same shape as `VSS_MAX_SECONDS`/
`VSS_MAX_ROWS`), so a slow pass drains over successive days rather than blocking the pool, and a
failure here must never poison `run-pool.sh`'s exit code (same reasoning as the embeddings
step: `run-scheduled.sh` would otherwise read it as an ETL crash and restart daily forever).

**Phase 3 — implement detectors.** Statistical/algorithmic methods first (§5) — cheap, fast,
deterministic, easy to validate against the Phase 1 seed cases. Layer in generative-AI-assisted
review once the statistical layer is producing a stable, low-noise candidate stream; an AI review
pass over a noisy statistical layer just produces confidently-worded noise.

**Phase 4 — extend beyond `ref` (TBD).** Only after Phases 1–3 are proven on `ref`. Requires a
per-schema/per-table review (§3) before a table is added to `db_cleansing`'s scope — not an
automatic expansion.

---

## 8. Open questions

- **Per-schema vs. shared cleanse table** (§4) — decide before Phase 1. A shared table is easier
  to query globally but couples every adopting schema through `ref`; per-schema tables avoid that
  coupling but duplicate the pattern. Leaning per-schema, consistent with the register's own
  `ref.entity_overrides` naming, but not decided.
- **Does anything ever auto-apply?** Default position per §2.1: no — every `proposed` candidate
  requires a human `reviewed_by` before `applied`. Whether a future, proven-safe detector class
  (e.g., a statistical method with a track record of zero false positives on the audit in §6)
  earns an auto-apply path is a decision for after Phase 3 has real track-record data, not before.
- **Where do generative-AI calls actually run?** The govdata pipeline is a Java ETL framework;
  `vss-local.sh` (the closest existing precedent — the embeddings step) runs as a separate
  process, not in-JVM. `db-cleansing.sh` likely follows the same separate-process model rather
  than adding an LLM-calling dependency into the core ETL JVM — needs a concrete design once
  Phase 3 starts, including which model/API, cost budgeting, and rate limits (a real operational
  concern once this runs daily against growing master-data tables).
- **`asserted_by`/reviewer identity** — `entity_overrides` (and this generalized version) records
  who approved a candidate but doesn't constrain the value to anything. Fine for a single-operator
  setup; needs a real answer before this is a multi-person process.
- **Regression suite formalization** — the register notes the comparative-eval harness's labelled
  pairs are already a natural regression suite (rerun the matcher, check it still agrees with
  confirmed entities). Phase 1 should make this an explicit, re-runnable check, not just an
  observation.

---

## 9. Related

- Govdata Defect Register, Batch 2, "Recommendation · not a defect" — `ref.entity_overrides` /
  `ref.canonical_org_entity_resolved` (the originating proposal this plan generalizes).
- Govdata Defect Register, Batch 2, B2-1 / B2-2 / B2-4 — the three concrete `ref` defects this
  plan's Phase 0 depends on landing first.
- `govdata/scripts/parallel/run-pool.sh` — the `$RUN_EMBEDDINGS` post-step (near end of file) is
  the direct structural precedent for how `db_cleansing` should be wired into the daily alias.
- `govdata/scripts/parallel/vss-local.sh` — the direct structural precedent for the worker
  script's delta-driven, time-boxed shape.
- `govdata/scripts/parallel/OPERATIONS.md` — documents `run-scheduled.sh`'s daily/historical
  window alternation that `db_cleansing` needs to run inside without disrupting.
