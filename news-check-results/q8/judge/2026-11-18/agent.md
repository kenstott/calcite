# Judge — news q8 grist-food-water-watch-nitrate-120-million

**Story:** Grist (via Yahoo News, 2026-11-17) on a coalition petition asking EPA to cut the nitrate limit from 10 to 1 mg/L. Claims of interest: Tyler Lobdell (Food & Water Watch), "one in three Americans — nearly 120 million — are currently being served water" with dangerous nitrate levels; about 42% of Iowans; 62% in the top states, "clustered in the West and Midwest"; up to 12,594 cancers a year; $94.5B cost against $364B benefit; reassessment begun under Obama in 2016.

Rank: [askamerica, expert, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **askamerica (1st):** engine 1 Pinocchio (it read the piece as closer to 2). It searched the full petition and pulled the figures the others could not:
  - The petition's own table shows only 2.9M people (2.4%) served by systems with a sample at or above 10 mg/L. That puts Lobdell's "dangerous levels" (119.7M at ≥1 mg/L) in context, and it graded the claim partially false.
  - Iowa is 45.5% in Table 3, not 42%.
  - The "62% in the West and Midwest" states include Pennsylvania, New Jersey, Maryland and Ohio.
  - The cancer figure is a 2,300–12,594 range.
  - $94.5B/$364B is the petition's best case; full treatment costs $130.4B, and the benefit-cost ratio falls to 2.8 at a 7% discount rate.
  - The reassessment began in September 2017, not 2016.

  It added warehouse context from SDWIS: about 730 to 930 nitrate violations a year at roughly 370 to 410 systems in 2021–25, against the petition's 13,205 systems above 1 mg/L. It graded Hirschfeld's Illinois 2 million unsupported after searching.
- **expert (2nd):** 1 Pinocchio. Same core framing catch (119.7M at one tenth of the limit), the 2017 start date, the Temkin 2019 range and 89 FR 59623. It could not parse the petition appendix, so Iowa, the 62% share, the state list and Illinois were left not checkable.
- **everyman (3rd):** 1 Pinocchio. It flagged petitioner-sourced figures and the Clean Water Act vs Safe Drinking Water Act imprecision, but opened none of the primary documents, and it missed the 1 vs 10 mg/L framing.

What would change the ranking: very little. askamerica's lead rests on figures quoted from the petition's tables. If its reading of Table 3 or the ≥10 mg/L table were wrong, expert would move level.

## Recipe check
Nothing to teach. askamerica already did what expert did and more.

## Gaps
Filed kenstott/govdata-ops#1325 (`type:sourcing`, `kind:gap`, `schema:environment`, `status:open`): drinking-water contaminant occurrence (EPA Six-Year Review sample results). The warehouse has SDWIS violations but no below-MCL concentrations, so the 119.7M figure could not be recomputed. No prior nitrate or sample-results issue.

## Shape
claims.json: 2 `subject_claims` (Tyler Lobdell; Robert Hirschfeld), 2 `fidelity`, 8 `author_claims`. Correct.

## Severity invariant
Not triggered.
