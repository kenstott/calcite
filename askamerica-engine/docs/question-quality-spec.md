# Requirements Specification: Improving Question Quality at the MCP Server

**Status:** Implemented
**Module:** `askamerica-engine` (MCP server)
**Author:** (kenstott)
**Date:** 2026-06-13

---

## 0. Implementation

R1–R10 are implemented across three files:

- `QuestionGuidance.java` — the rubric (§4), the eight contrastive exemplars, and the six
  parameterized prompt templates. R1–R3, R9.
- `QuestionDiagnostics.java` — the envelope and the detectable-defect catalog (§6). R4–R8.
- `McpServer.java` — the wiring: exemplars into tool descriptions, the rubric into
  `initialize` `instructions`, `prompts/list` + `prompts/get`, the `critique_query` tool
  (R10), and the envelope as a second content block on every analytical result.

Two implementation decisions worth stating, because AC6 rests on both:

1. **The envelope is a sibling content block, never merged into the payload.** The data block
   stays `content[0]` and stays exactly the bytes it was, so a host that indexes it and parses
   it as a JSON array is unaffected. Merging would break every such host at once.
2. **A failed diagnostic check reports itself** (`diagnostics_incomplete`) rather than
   returning an empty warnings array. An empty array is indistinguishable from a clean result,
   so a silently-broken check would read as a passing one.

One gap the work surfaced and closed: the no-push-down refusal (R8/AC4) reached the client
with its directed explanation only when the aggregate shared an `EnumerableAggregate` with
another aggregate and failed to *compile*. When it was alone, the stub's own
`UnsupportedOperationException` reached the client verbatim — "no Calcite enumerable
implementation" — with no runnable alternative named. Both shapes now resolve to the same
directed message.

Coverage: `QuestionDiagnosticsTest` (AC1, AC2, the catalog, and the negative cases that keep
the warnings from becoming noise), `QuestionGuidanceTest` (AC3, the templates), and
`McpServerIntegrationTest` (AC6 at the wire, plus prompts and the exemplars in the live
`tools/list`). AC5 is an evaluation, not a test — see §8.

## 1. Background & Problem

AskAmerica is exposed to end users as an **MCP server**: it publishes tools
(`list_schemas`, `list_tables`, `describe_table`, `query`, …) and answers calls. The
conversation — and therefore the *quality of the question* — is driven by the **host**
(Claude Desktop or another model). The server does not own the client and cannot steer the
dialogue.

The quality of the answer is capped by the quality of the question. A well-formed question
("on the margin, does teacher pay or headcount favor NAEP reading, holding poverty fixed, at
state grain — effect size and confidence?") yields an honest, bounded answer. A malformed one
("does education spending work?") yields a confounded state-level correlation that *looks*
authoritative and is not. At n≈51 with more plausible confounds than observations, a naive
question is not just unanswered — it is answerable in a misleading way.

The server therefore needs to raise question quality through the surfaces it **does** control,
without depending on the host being sophisticated, and without overreaching into judgments it
cannot make.

### Design principle: facts, not verdicts

The server can inspect the *form* of a query and the *shape* of the data. It cannot see the
user's intent or the surrounding conversation. Every mechanism below is therefore restricted to
**mechanically-detectable facts** (row counts, coverage, grain, key fan-out, vintage
alignment, field validity, push-down feasibility). The server MUST NOT opine on whether a
question is worth asking or which model specification is "correct" — doing so would relocate
the prior-laundering problem into the server itself.

### Design principle: graceful degradation

Every mechanism MUST be additive. A weak host that ignores the guidance receives the same raw
answer it gets today (no regression); a capable host that reads it produces a better next query
or a better-caveated answer. The naive case is never worse; the capable case is better.

## 2. Goals

- **G1.** Teach host models the shape of an answerable question, ambiently — without requiring
  the user to do anything.
- **G2.** Make every result **self-scoping**: it carries the facts needed to caveat it, so even
  a poorly-framed question yields an honestly-bounded answer.
- **G3.** Give a host a machine-readable signal it can act on — re-query with a control, change
  grain, or hedge — closing an improvement loop.
- **G4.** Offer vetted question templates for users who want them.
- **G5.** Keep all of the above scoped to detectable facts (never intent, never verdicts) and
  additive (never a regression for a weak host).

## 3. Non-Goals

- Judging whether a question is worthwhile (intent — not visible to the server).
- Forcing the host to relay a caveat to the user (the server cannot control the client's final
  message; it can only improve the material the host answers from).
- A first-party UI or agent (this spec covers the server only).
- Replacing analyst judgment or settling contested policy questions.

## 4. The Good-Question Rubric

The rubric the mechanisms below encode. A question scores well when it is:

1. **Bounded** — a specific comparison or quantity, not a sweeping verdict.
2. **Marginal** — framed at the margin ("one more dollar: A or B"), not the absolute total.
3. **Operationalized** — names a measurable outcome and admits it is a proxy (NAEP grade-8
   reading, not "student success").
4. **Minimally & orthogonally controlled** — names the one or two structural confounds that
   matter and are not collinear; does not stack near-duplicate controls (poverty *and* income)
   against a scarce degree of freedom.
5. **Grain-matched** — the unit of analysis fits the data and the confound density, and the
   question knows when state grain (n≈51) is too coarse to answer causally.
6. **Time/vintage-anchored** — specifies the window and respects data revisions (point-in-time
   vs. revised).
7. **Effect-size seeking** — asks "how strong, and how sure?", not "which side is right?", and
   is explicit about the confound it is *not* holding fixed.

Meta-test: **the shape of the answer is statable in advance** (a coefficient, a ranked list, a
number with an interval). If the answer's shape cannot be described, it is a topic, not a
question.

## 5. Requirements

### 5.1 Ambient teaching (auto-injected surfaces)

- **R1.** Each analytical tool's `description` MUST include **contrastive exemplars** — a vague
  question paired with its sharpened rewrite — not answer-only samples. The rewrite is where the
  teaching density is. Tool descriptions are the safest home: every capable client honors them.
- **R2.** Exemplars MUST span distinct question *shapes* (comparison, trend, ranking,
  premise-check) and MUST include at least one **honest-refusal** exemplar ("this cannot be
  answered at this grain, because …"). Teaching the honest "no" separates a grounding tool from
  a prior-launderer.
- **R3.** The server `instructions` field MUST state the rubric (§4) briefly and point at the
  tool-level exemplars, as a global backstop for hosts that read it.

### 5.2 Self-scoping results (the diagnostics envelope)

- **R4.** Every result from an analytical tool MUST carry a structured `diagnostics` envelope
  alongside the data — typed, machine-readable warnings, not only prose. Example shape:
  `{warnings:[{type:"small_n", n:51}, {type:"uncontrolled_confound", note:"result not conditioned on any covariate"}, {type:"row_fanout", key:"state", max_rows_per_key:2}], grain:"state", vintage:{...}}`.
- **R5.** The envelope MUST populate from **detectable facts only**, drawn from this catalog
  (§6). Absence of a warning is not a claim of validity — only that no listed defect was
  detected.
- **R6.** Warnings MUST carry a severity (`info` / `caution` / `high`) so a host can decide how
  hard to hedge, and a power-user host can suppress low severities.

### 5.3 Soft pushback, not blocking

- **R7.** Default behavior is **answer + diagnostics**, never refusal. A merely-imperfect
  question still returns its data with warnings attached.
- **R8.** Hard refusal is reserved for the **un-runnable or corrupt-by-construction**: a
  statistic that cannot compute (e.g. a cross-schema aggregate that cannot push down — the
  server already returns a directed error here), a query over a field known to be broken, or a
  result built from detectably duplicated rows. A refusal MUST explain the mechanical reason and
  suggest the runnable form.

### 5.4 Opt-in templates and on-demand help

- **R9.** The server SHOULD expose **MCP prompts** — vetted, parameterized good-question
  templates — for hosts/users that surface them. These are opt-in (the user selects one), so
  they complement, not replace, R1–R3.
- **R10.** The server MAY expose an on-demand tool (e.g. `suggest_questions` /
  `critique_query`) that returns exemplars or a form-level critique of a proposed query, for
  hosts that choose to call it.

## 6. Detectable-Defect Catalog (pushback triggers)

The complete set of facts the server may warn on. Each is server-observable without knowing
intent.

- **small_n** — the analysis rests on few units (e.g. state grain, n≈51); ecological.
- **low_coverage** — a coverage field is low (e.g. crime `population_coverage_pct`), or the
  result set is near-empty.
- **row_fanout** — a join produced more than one row per declared key (a PK/FK violation the
  server can detect from its own constraints — the defect that corrupted a state ranking during
  design).
- **grain_mismatch** — a causal comparison issued at a grain too coarse for the confounds.
- **vintage_misalignment** — joined tables span mismatched years/vintages, or a revised series
  is compared point-to-point without a revision note.
- **broken_field** — a column with out-of-domain values (e.g. negative rates) or null-dominant
  content.
- **no_pushdown** — an aggregate (corr/regr/median/…) that cannot execute because its inputs
  cross schema/catalog boundaries; suggest the aligned-series path or a single-schema rewrite.
- **collinear_controls** — (best-effort) two requested controls that are near-duplicate.

## 7. Acceptance Criteria

- **AC1.** An uncontrolled state-level correlation returns its coefficient **plus** a
  `diagnostics` envelope naming `small_n` and the absence of any covariate — in one response.
- **AC2.** A join that fans out to >1 row per key returns a `row_fanout` warning naming the key.
- **AC3.** Analytical tool descriptions contain ≥ 6 contrastive (vague → sharpened) exemplars
  spanning ≥ 4 shapes, including ≥ 1 honest-refusal exemplar.
- **AC4.** A non-push-down-able aggregate is refused with a directed, mechanical explanation and
  a runnable alternative (baseline behavior already present; formalize it under R8).
- **AC5.** With a capable host, presence of the diagnostics envelope measurably increases the
  rate of a corrective follow-up (re-query with a control, grain change, or explicit hedge)
  versus its absence.
- **AC6.** With a weak host that ignores diagnostics, the **raw data returned is byte-identical**
  to the pre-change behavior (no regression).

## 8. Evaluation

**Status: AC5 is not verified.** A first attempt is written up in
[question-quality-ac5-findings.md](question-quality-ac5-findings.md). Across four paired
runs spanning two host tiers, both arms reached correct answers every time and no pair
showed the envelope producing a corrective the other arm lacked — but the defect scenarios
did not reliably fire, so that is mostly a measurement of the envelope's value when nothing
has gone wrong. The findings note what a real test would need. Read it before treating
either the mechanism or its absence as validated.

Tie measurement to the existing eval bank
(`.claude/skills/askamerica-comparative-eval`):

- **A/B the exemplars.** Score question quality (against the §4 rubric) for identical user
  intents with exemplars present vs. absent in tool descriptions.
- **Diagnostics uptake.** Measure whether a host, handed the diagnostics envelope, produces a
  better-scoped final answer (grounded vs. web-only, or against a golden answer where one
  exists).
- **No-regression check.** Confirm the raw payload is unchanged for a host that ignores the
  envelope (AC6).

## 9. Risks & Constraints

- **Exemplar anchoring.** A fixed exemplar set narrows what users ask and can steer toward
  vendor-chosen framings. Mitigate with diverse shapes, visible reasoning per exemplar, and the
  required refusal exemplars (R2).
- **Verdict creep.** The strongest failure mode: pushback drifting from facts into opinions on
  the "right" model. Enforce the §1 facts-not-verdicts rule at the catalog boundary (§6) — if a
  warning cannot be derived from a server-observable fact, it does not ship.
- **Client-support variance.** Tool descriptions and `instructions` are honored by every
  capable client; `prompts`/`resources` support is uneven. Ambient teaching MUST live in tool
  descriptions first (R1); prompts (R9) are a bonus where supported.
- **Bounded reach.** The server improves the material the host answers from; it cannot guarantee
  the host relays a caveat to the user. This is a strong lever, not a complete one.

## 10. Affected Components

- MCP tool-registration site (tool `description` strings) — R1–R3.
- Analytical tool result path (`query` and any stat tools) — the `diagnostics` envelope,
  R4–R8, drawing on the server's own constraint metadata for `row_fanout`.
- Server `initialize` `instructions` — R3.
- New `prompts` capability registration — R9.
- Optional `suggest_questions` / `critique_query` tool — R10.
