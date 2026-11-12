# Judge: GMA "Vaccine exemption rate for kindergarteners reaches record high: CDC" fact-check (q6), 3-persona 2026-11-08

Rank: [askamerica, expert, everyman]

Source: https://www.goodmorningamerica.com/wellness/story/exemption-rate-routine-childhood-vaccines-us-kindergartners-hits-135716718
(GMA/ABC News, 2026-08-17). It reports the CDC's 2025-26 SchoolVaxView release.

**Engine build:** main at 382db82223 (installed). Data server: pgwire-govdata 0.126.0, the draft
release bundle installed on this Mac today.

## Why

**`askamerica`** checked every figure against the warehouse table
`health.cdc_school_vaccination_coverage` after the CDC site returned 403:
- **Exemptions:** 4.2% / 3.6% / 2.2%, with the non-medical vs medical split (4.0 / 0.2).
- **Coverage by vaccine:**
  - MMR 92.5 → 92.4, against 95.2 in 2019-20;
  - varicella 94.0 → 93.4, two-dose 92.1 → 92.2;
  - DTaP 92.1 → 92.0;
  - HepB 94.0 → 94.0.
- **State rankings with values:**
  - highest: Idaho 17.5, Utah 12.7, Oregon 11.0, Arizona 10.6, Nevada 8.7;
  - lowest: CA / NY / WV 0.1, Connecticut 0.5, Maine 0.8.

It graded the piece 0 Pinocchios. It is the only persona that verified the state rankings and the
prior-year per-vaccine values. Both web arms marked those unverifiable.

**`expert`** confirmed the national figures and the current varicella values from CDC and secondary
pages. It could not reach the state table. Grade: 1 Pinocchio.

**`everyman`** confirmed the national figures. It wrongly doubted the article's varicella line,
which the warehouse and `expert` both confirm. Grade: 1 Pinocchio.

**Ranking:** `askamerica` first. It verified every checkable claim in the article from primary
data. `expert` second, `everyman` third.

## Severity check

`askamerica` ranked #1, so no severity flag.

## Gate check — the recorded answer never passed QC

Previews were refused three times, in order:
1. seq 7: the wording gate;
2. seq 9: 2 of 3 linked articles unread;
3. seq 12: claim 6 had no valid `errs` value.

No preview passed. `deliver_report` (seq 8) went through **one second after the first refusal**, so
`agent.md` holds the pre-QC draft, and the agent stopped without a passing preview. Real Desktop
users publish through the gated `create_report_artifact`. The eval's `deliver_report` is ungated
(`McpServer.java:3950`, eval mode only), so it can record a report the product would refuse to
publish. That is an eval-fidelity gap.

There is no dashboard.png for this run.

## Data check

`describe_table` timed out again on its first call (seq 3), at the `primary key` step: "object store
did not answer in time". This was on the fixed 0.126.0 bundle. The same cold-start timeout happened
in my verification of #1129, and the retry succeeded. The first `describe_table` after a data
server starts may be consistently too slow on the primary-key lookup. Not filed yet.

## Recipe check

None needed; `askamerica` ranked #1.

## Gap audit

None.
