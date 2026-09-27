# Fact-check: "Census announced real median household income was $87,460 in 2025"

## Verdict: TRUE (0 Pinocchios)

## Summary
The claim is a verbatim quotation of the Census Bureau's own newsroom press release (CB26-151, "For Immediate Release: Tuesday, September 15, 2026"), which was fetched and read directly:

> "The U.S. Census Bureau today announced that real median household income was $87,460 in 2025, the highest on record dating back to 1967... Median household income was $87,460 in 2025, an increase of 2.6% from the 2024 estimate of $85,210."

This figure comes from the "Income in the United States: 2025" report (P60-289), based on the 2026 Current Population Survey Annual Social and Economic Supplement (CPS ASEC).

## What was checked in the AskAmerica corpus
- `search_catalog` for median household income surfaced `census.acs1_income` (ACS 1-Year, state-level, the most current single-year Census income series in this warehouse), `census.acs_income` (ACS 5-Year, smoothed), `census.income_summary`, and `census.saipe_poverty`.
- None of these carry CPS ASEC data — `search_catalog` explicitly flagged "asec" as an unmatched term. The Census's annual income/poverty/health-insurance headline release is always CPS ASEC-based, a different survey from ACS, with different sampling/weighting and generally different point estimates for the same concept.
- `data_coverage` on `census.acs1_income` confirms the table's actual loaded window is 2005–2024 (2020 is an interior gap — no ACS 1-Year collection that year due to COVID); 2025 is not loaded. The table is state-level only, with no national row (confirmed empty on a direct `geo_name ILIKE '%United States%'` query).

### Query run (context only, not a recomputation of the claim)
```sql
SELECT year, avg(median_household_income) AS avg_state_median
FROM census.acs1_income
WHERE year IN ('2019','2021','2022','2023','2024')
GROUP BY year ORDER BY year
```
Results (nominal dollars, simple unweighted average across states — not a national point estimate):
- 2019: $64,645
- 2021: $68,339
- 2022: $73,477
- 2023: $76,589
- 2024: $80,424

Direction is consistent with continued year-over-year growth, but the level and percentage change cannot be checked against this corpus because it's a different survey (ACS vs. CPS ASEC), a different weighting method (simple state average vs. national household-weighted), and it does not yet reach 2025.

## Verification against the primary source
Because the corpus cannot reach the actual 2025 CPS ASEC figure, the claim was verified directly against Census's own materials via `web_fetch`:
- https://www.census.gov/newsroom.html — confirms the release, dated Sept 15, 2026, quoting the $87,460 figure.
- https://www.census.gov/newsroom/press-releases/2026/income-poverty-health-insurance-coverage.html (CB26-151) — full text confirms "real median household income was $87,460 in 2025... an increase of 2.6% from the 2024 estimate of $85,210," sourced to the CPS ASEC and detailed in P60-289.

The claim under test matches this primary-source language exactly — figure, year, and the "real" (inflation-adjusted) framing all line up.

## score_claim independent check
Verdict: **true**, confidence 0.88; Pinocchios: 0, confidence 0.81.

## Pinocchio rating: 0
No significant issues. The claim faithfully and exactly restates what the Census Bureau itself announced, confirmed by reading the actual primary document (not a secondary summary).

## Important caveat (disclosed, not glossed over)
AskAmerica's warehouse could **not** independently recompute the $87,460 figure — it has no CPS ASEC table and no 2025 income data of any kind. The $80,424 ACS 1-Year 2024 figure above is offered only as directional context (a different survey, a year earlier, unweighted across states), not as a check against the claimed number. Had the primary-source fetch failed, this claim would have had to be marked "not checkable here" rather than "true."

## Tables/queries/values cited
- `census.acs1_income` — SQL above; values $64,645 (2019) through $80,424 (2024).
- Primary source: Census press release CB26-151 (Sept 15, 2026) and newsroom landing page — $87,460 (2025), $85,210 (2024), +2.6% change.

## Report
Published dashboard + full report: saved to `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q10/askamerica/2026-09-26/report.html` (local ephemeral link also returned by publish_report, not durable beyond this session).
