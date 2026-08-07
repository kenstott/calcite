# AC5 evaluation: does the diagnostics envelope change what a host does?

**Verdict: not demonstrated. The test rig did not create conditions under which it could be.**

AC5 asks whether "presence of the diagnostics envelope measurably increases the rate of a
corrective follow-up (re-query with a control, grain change, or explicit hedge) versus its
absence." This records what was actually run, what it showed, and why the result is weaker
evidence than it looks.

## Method

Two MCP servers from the same branch build, one per arm, each with its own
`MCP_DATA_DIR` (DuckDB permits one writer per catalog). Agents reached them through a
driver that derives its arm from its own directory, so no agent could reach the other
arm's server or response mode.

The arms differ in exactly one respect:

| | `full` | `plain` |
|---|---|---|
| Result blocks | all | first only |
| Tool descriptions | as shipped | restored to pre-change text |
| `critique_query` | present | withheld (did not exist before) |

`plain`'s `query` description was verified byte-identical to what the installed release
returns. `report_issue` and `set_telemetry` were blocked in both arms (see Incidents).

Prompts within a pair were identical except the working directory. Neither agent was told
it was part of a comparison.

## Results

Four paired runs, eight agents. Every pair reached a correct, well-caveated answer in
**both** arms. No pair showed the envelope producing a corrective the other arm lacked.

| Question | Defect the naive path trips | Opus `full` | Opus `plain` | Haiku `full` | Haiku `plain` |
|---|---|---|---|---|---|
| Income ↔ education correlation | `small_n` (n=52), `uncontrolled_confound` | correct | correct | — | — |
| Bachelor's degrees in 2010 | `low_coverage` (outside 2012–2024) | correct | correct | — | — |
| Income + education join | `row_fanout` (88 rows/key, 20.8× inflation) | correct | correct | correct | correct |

Ground truth for the join question was fixed before any result was read: correct total
49,091,464; fan-out total 1,022,954,656.

**The envelope demonstrably acted exactly once.** Opus `full` called `critique_query` on its
draft join and received the `row_fanout` warning naming `county` as the omitted key column,
then corrected the join before running it. Opus `plain` reached the same correctly-keyed
join without any such prompt.

Notable non-effects:

- Haiku `full` never called `critique_query` and its caveats reference no diagnostic. The
  envelope was present and appears to have gone unread.
- Haiku `full` summed `bachelors + masters + doctorate` without catching that the table
  omits professional degrees (a ~5.2M national undercount). Both Opus arms found this
  independently. The envelope does not detect it — it is a semantic gap, not a
  mechanically-detectable one.
- Haiku `plain` paired 1-year income with 5-year education, which both Opus arms explicitly
  warned against. The envelope does not detect that either.

## Why this is weak evidence

The defect scenarios did not reliably fire, so mostly this measured whether the envelope
helps when nothing has gone wrong.

1. **Nobody triggered the fan-out.** All four agents constrained the year, because the
   question says "for 2023" — which defuses the trap the question was built around. A
   fan-out needs a join on `state` alone with the year unconstrained on one side.
2. **The fan-out was too obvious to be a fair test even if triggered.** 1.02 billion
   degree-holders in a ~335M population fails a sanity check without any warning.
3. **The coverage question was already answered elsewhere.** `describe_table`'s coverage
   block predates this work and carries the same signal, so both arms had it.
4. **`small_n` and `uncontrolled_confound` describe things a capable model already caveats
   by habit.** Opus `plain` produced the ecological-fallacy caveat, the n, and a
   both-directions causation warning unprompted.
5. **Four pairs, one repetition each.** Not a rate.

## What would actually test it

Defects that a competent host walks into naturally and that no other surface reports:

- A fan-out whose inflation is *plausible* (2–3×, not 20×), so arithmetic sanity does not
  catch it and the ranking is silently wrong.
- A question whose phrasing does not hand the agent the filter that avoids the defect.
- `broken_field` and `collinear_controls`, neither of which any run exercised.
- Enough repetitions per cell to distinguish an effect from run-to-run variation.

## Cost, for whatever it is worth

`full` used more tokens than `plain` in both tiers (Opus 70.6k vs 54.9k; Haiku 40.6k vs
35.4k). This is confounded — the arms took different investigation paths — but the
envelope is attached to every analytical result, so its overhead is paid on every call
whether or not it is read.

## Incidents

**A false bug report reached the production issue tracker.** During the first run, four
agents shared one server; every statistical tool holds the global `DB_LOCK`, so calls
serialised. One agent measured an `ols_regression` at ~8 minutes that the server logged at
3,559 ms; another hit a client timeout, read the silence as a crash, and filed a report
claiming `commons-math3` was absent from the shaded jar and that an uncaught error had
killed the worker thread. All of it was false: the jar carries 1,386 `commons-math3`
classes including `OLSMultipleLinearRegression`, the HC1 standard-error regression tool
re-runs correctly, and no such error appears anywhere in the server log. A retraction was
filed.

Two things follow. The rig now blocks `report_issue` in both arms, and runs one agent per
server. And the tracker is **write-only** — `GET /v1/issues` returns 404 and there is no
close or update path, so a false report can be annotated but not withdrawn. That is worth
revisiting given an agent can write to it unprompted.

**The first run's timings are unusable** for the same contention reason and are not reported
here.
