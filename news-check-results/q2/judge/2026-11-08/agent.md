# Judge: Medical Daily "Overdose Deaths Climbed in 430 U.S. Counties While the National Toll Fell 12% to Nearly 68,000" fact-check (q2), 3-persona 2026-11-08

Rank: [askamerica, expert, everyman]

Source: https://www.medicaldaily.com/us-overdose-deaths-rise-430-counties-480874 (Joseph James, Medical
Daily, 2026-11-05). It aggregates an Axios analysis of CDC provisional overdose data (Axios, Nov 3;
blocked with HTTP 403 for all three personas) and Axios Cincinnati's Ohio breakdown.

**Engine build:** main at 7f1fa47bb6 (installed). Data server: pgwire-govdata 0.124.4.

## Why

All three gave 1 Pinocchio and agree:
- **National figures:** CDC VSRR shows 12-month deaths ending March 2026 down about 12% to about
  68,000 (reported 77,064 → 67,576, −12.3%; predicted 77,306 → 68,080, −11.9%).
- **The earlier 67,798:** it is from an earlier data vintage.
- **The headline:** it leads with the minority trend; deaths fell in about twice as many counties
  as rose.

**`askamerica`** worked from the warehouse's CDC county table (`health.cdc_county_overdose_deaths`,
data as of 2026-10-04):
- **The 430/842 split:** reproduced on two counting rules (strict: 233 up / 593 down / 352 flat;
  counting moves into and out of suppression: 320 / 776). Neither gives 430/842, so Axios used an
  unstated rule. The 352 flat matches Axios exactly.
- **County figures:** Butler (104→73), Hamilton (200→147), Campbell (13→17) and Rio Arriba (52→69,
  about 175 per 100k) all confirmed.
- **Two findings no other persona made:**
  - **Ohio tally:** 38 counties measurable with both years visible (28 fell, 7 rose, 3 flat),
    against the article's 42 (30/9/3). The difference comes from suppression handling.
  - **"Nearly half" understates suppression:** 1,970 of 3,148 counties (62.6%) have a suppressed
    value in one or both years.

**`expert`** pulled the same CDC county dataset (gb4e-yj24) directly and reached the same county
figures and the same two counting rules (233/590/352; 320/773). It confirmed the 9,500-fewer figure
(9,488 reported) and quantified the unclassifiable counties as 1,248–1,967 depending on the rule.
It did not tally Ohio.

**`everyman`** confirmed the national figures from CDC VSRR, but checked the county claims only
through an Axios search snippet. It did not recompute the 430/842 split or the county counts.

**Ranking:** `askamerica` and `expert` are close. `askamerica` ranks first for the Ohio tally and
the suppression share, both from the warehouse. `everyman` is third.

## Severity check

`askamerica` ranked #1, so no severity flag.

## Gate check — claims bypassed again

There is no claims array and no `source_url`. Same as 2026-11-08 q1: the report published without
claims, so the coverage, figure, measure and external-check gates did not run. That is two runs in
a row. The engine recognizes a validation only by `source_url` or a URL in the restated question;
this run's question and title name a fact-check with a Pinocchio grade and neither carries the URL.

## Recipe check

None needed; `askamerica` ranked #1.

## Gap audit

None. The CDC county and state overdose tables are in the warehouse. The Butler County coroner
series is local and out of scope.
