# Judge — news q3 sacurrent-texas-maternal-death-rate-pre-pandemic

**Story:** "Texas maternal death rate drops to pre-pandemic levels as mental health becomes the leading cause" (Texas Tribune via San Antonio Current, 2026-11-13). Claim of interest: 20.5 per 100,000 in 2024 vs 23.5 in 2021 (COVID excluded), "slightly above" 2019.

Rank: [expert, askamerica, everyman]

Judged 2026-11-17 on three answers produced the same day.

## Why
- **expert (1st):** 1 Pinocchio, 38 claims extracted from the DSHS/MMMRC 2026 report. Caught the severe maternal morbidity misstatement (report says 83.9, not 83.5), that "last year's report" was two years old, that the Sept 1 "delay" is contested by a 2025 law allowing a Dec 1 filing, that "slightly above 2019" is about 19% higher (17.2 vs 20.5), the 12-of-59 base under "leading cause", and that the contributing-factor "doubling" partly reflects data capture (22 of 60 cases recorded in 2021 vs 57 of 59 in 2024). Widest and most specific.
- **askamerica (2nd):** engine rating 0 Pinocchios, 13 claims. Read the 56-page report in full and caught the same SMM figure and denominator errors, the 19% gap behind "slightly above 2019", and the most material framing point: against the all-cause 2021 ratio of 39.1 the "drop" mostly reflects the end of COVID deaths. Checked the bill histories (HB 286, HB 3339). The warehouse had no maternal mortality table, so the connector added no data here; graded entirely from the primary report. Fewer claims and fewer omission catches than expert.
- **everyman (3rd):** 1 Pinocchio. Could not open the report PDF, so everything rests on the Tribune and other coverage; graded "slightly above 2019" not checkable. Its framing caveats (COVID exclusion, missing 2022-2023) are right but unquantified.

What would change the ranking: askamerica and expert reach the same errors; expert's extra omission findings (data capture, Dec 1 law, report vintage) decide it. A federal maternal mortality series in the warehouse would have let askamerica test "pre-pandemic levels" against NCHS and other states, which no persona did.

## Recipe check
No recipe applies. expert's data-capture point (compare the share of cases with the field recorded before reading a change in that field's share) is a general check worth watching for a second instance.

## Resolution
Data gap, filed: govdata-ops#1282 (no maternal mortality table; NCHS NVSS / CDC WONDER by state and year).

## Shape
Reviewed. 12 `author_claims`, 1 `subject_claims` (speaker Rep. Donna Howard), as delivered. A requested regroup into `fidelity` was refused by the validator and withdrawn; per the engine's current rule (ClaimScoring, since ecae2ba83a) a reported figure is the author's claim, so the original grouping is correct. The subagent reported the engine process restarting mid-session and losing call history (third dispatch today to report this).

## Severity invariant
Not triggered.
