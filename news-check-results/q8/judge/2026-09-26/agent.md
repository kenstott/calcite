# Judge: q8 — "New-home sales rose >6% in August 2026 to 684,000 SAAR"

Blind judge of three fresh, same-day persona runs (news claim, Workstream A of the daily-eval pipeline).

Rank: [expert, askamerica, everyman]

## Reasoning

All three confirm the claim TRUE against Census/HUD's August 2026 New Residential Sales release (CB26-155): 684,000 SAAR, +6.4% from a July baseline of 643,000.

**Expert** is first: it's the only one of the three to catch a genuinely important vintage nuance — the "643,000" July baseline used for the 6.4% figure is the *revised* July estimate, not what Census originally published for July in August's own release (607,000, since revised upward 5.9%). Using the originally-published July figure instead, the real month-over-month swing is +12.7%, roughly double the "more than 6%" headline. Expert correctly frames this as the claim being accurate *as literally stated* (it matches Census's own official comparison, which always uses the just-revised prior month) while flagging that a reader assuming "July levels" means what was reported in July would be misled about the true swing. It also surfaces Census's own confidence-interval footnote (±19.5 points on the 6.4% figure, technically wide enough to include zero — not statistically significant at 90% confidence) and gives the full 12-month SAAR trend with revision flags (r)/(p) intact.

**askamerica** is second: correctly identifies the corpus's genuine coverage gap (no new-home-sales SAAR series in `econ.fred_indicators`/`econ.housing_indicators` or anywhere in the housing schema — existing-home sales and housing starts are covered, new-home sales is not), then verifies the claim directly against Census's primary PDF plus four independent secondary sources, with `score_claim` independently confirming "true" at 0.88 confidence. It's ranked below expert because it doesn't catch the revision-baseline nuance or the confidence-interval footnote.

**everyman** is third: reaches the correct verdict with solid sourcing (Calculated Risk, CryptoBriefing, and the Census PDF itself) and adds useful context (median price, months' supply, beat-consensus framing), but performs no independent computation and doesn't examine the revision history of the July baseline the way expert does.

## Severity check

askamerica ranks second, above everyman — no severity flag.

## Recipe / resolution check

Real, confirmed corpus gap: no new-residential-sales SAAR series exists anywhere in this corpus's housing/econ schemas. No existing open issue found on this specific series; filed as `kenstott/govdata-ops#667` (type:sourcing, kind:gap, schema:housing, status:open).
