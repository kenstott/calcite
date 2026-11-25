# Judge — Fortune, "Ron Johnson's Social Security bill would borrow $190 trillion, CRFB says", 2026-11-25

**Question/claim:** Fact-check the Fortune article (2026-11-19): Johnson's Stop Scaring Seniors Act would let Treasury move general funds into the retirement trust fund; CRFB says it would add $190 trillion to debt over 75 years (real, 165% of GDP). Central claim: the $190T figure.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep of the everyman and expert files for "askamerica" found no hits, so neither unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only; WebFetch gave a summary of the article, CRFB 2026 Trustees analysis page | Warehouse (trustees projections, SSA county table, FRED, federal debt) plus CRFB release/testimony, Senate Budget report, Johnson 2011 letter | Web only: CRFB release, SSA 2026 Trustees Report, CRS IF10292, Johnson 2011 page |
| Vintage | 2026 Trustees | 2026 Trustees, Dec 2025 SSA, Nov 20 2026 FRED/debt | 2026 Trustees (OASI 4Q 2032 and OASDI 3Q 2034) |
| Central claim ($190T) | "Not checkable here" | True (CRFB release and Aug 2026 testimony; $840T nominal) | Verified (CRFB release, real-dollar basis) |
| Overall grade | 2 Pinocchios (leaning 1) | 0 Pinocchios | 1 Pinocchio |
| Claims graded | 10 | 11 | 13 |
| Other headline figures | 22% cut, 2032, $511B supported | 22% cut, $511B, quotes, 2011 letter true; 70M "mostly true" on 63.8M; TD unsupported | 22% cut is OASI only (OASDI 17%); 2.6 pts, $500, 70M not checkable; TD dates not checkable |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

Central claim. A WebSearch of the topic returns several outlets (housingwire, newsbeep, qz) repeating that CRFB projects about $190 trillion over 75 years, inflation-adjusted, about 165% of GDP. The CRFB release and testimony URLs cited by askamerica were not fetched by me, but the search results match them. This supports askamerica and expert (verified) and shows everyman's "not checkable" was a miss: the CRFB release was reachable.

Other checks:
- 22% cut: askamerica's table value (2032, 78% payable) matches expert's "4Q 2032, 78% payable" and everyman's CRFB figure. Expert adds the OASDI 17% nuance. Only expert stated it.
- $511B: all three agree. Askamerica traced it to the Senate Budget chairman's report p.9.
- 70 million: askamerica's "63.8 million in the 50 states" is wrong. Orchestrator verification (a live warehouse finding I cannot reproduce): it comes from summing county rows of health.ssa_oasdi_county. A live check found that table has no county rows for Maryland, Missouri, Nevada or Virginia in any year (filed as govdata-ops #1495). The table's own 50 state-total rows sum to 68,721,499 for 2025. So the "mostly true" verdict on "roughly 70 million" is reasonable, but the 63.8M figure and its explanation (DC and territories missing) are wrong.
- $500 a month: askamerica graded "True" but its only basis is Fortune's own Aug 14 piece ("I did not read CRFB's own page"). That is the article's outlet repeating itself, not a primary source. Everyman and expert said not checkable. Askamerica's grade is stronger than its evidence.
- TD Securities: askamerica "Unsupported" after one search and CRS/warehouse context; expert "$41.1T limit verified, TD dates not checkable." Similar, both calibrated. Askamerica's "Unsupported" leans slightly toward a negative call where only absence of evidence exists.
- 2.6-point rate rise: askamerica traced it to InvestmentNews Nov 2025 and flagged it came from a different scenario ($150T, 130% of GDP) and that the 10-year yield was 4.92%, not 4%. Expert noted the 6.6% rests on an earlier analysis of a different policy. Everyman called it not checkable. Askamerica went furthest.
- MacGuineas quote: expert flagged that "spook the bond market" is not verbatim in CRFB's release ("almost certainly spark a crisis"). Askamerica marked the same phrase "Not checkable" as a prediction and marked the other MacGuineas quotes verbatim. Expert's check is sharper on this one phrase.
- Askamerica claims.json versus report: engine rating in report.html is "0 Pinocchios"; agent.md states 0 Pinocchios. No mismatch. claims.json has 11 claims (8 true, 1 mostly true, 1 unsupported, 1 not checkable).
- Askamerica's 0 Pinocchios sits beside framing caveats that the headline drops "inflation-adjusted" and "75 years" and that the figure is a generic CRFB estimate, not a score of the bill text. That is close to the line for 1 Pinocchio. Expert's 1 is a little better calibrated on grade; askamerica's caveats are the more specific.

## Ranking rationale

1. askamerica. Correct central verdict with CRFB primary sources. Widest primary-source coverage: Trustees table from the warehouse, Senate Budget report page cite, 2011 letter, debt and yield figures at current dates. Caught the scenario mismatch behind the 6.6% yield and the stale 4% start. Weaknesses: the 63.8M figure and its stated reason are wrong; $500 a month graded true on a secondary source; 0 Pinocchios is generous next to its own caveats.
2. expert. Correct central verdict from the CRFB release. Good nuance (OASI versus OASDI, the not-verbatim MacGuineas quote, CRS on the debt limit). Calibrated "not checkable" use. Fewer primary figures confirmed (no Senate report read, Johnson quote unchecked), no warehouse-grade current data. Margin over everyman is clear; margin under askamerica is narrow.
3. everyman. Worked from a summary of the article, said so, and marked the central $190T claim "not checkable" when the CRFB release was reachable. Four of ten claims left unverified. It added useful scale context (about $31T present-value Trustees shortfall), but that comparison is apples to oranges against a real undiscounted sum and the arm admits it.

What would change the ranking: if the 63.8M error were counted as a graded-wrong headline, expert could move above askamerica; it did not change a verdict, so it stays second-order. If expert's 1 Pinocchio were judged clearly better than 0 on the same facts, the top two would swap.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- Askamerica followed the existing recipe `fact-check-a-news-article` (extract every claim, mark one central, search the measure, fetch another copy when 403). It fetched the article directly.
- Worth teaching: expert separated OASI (22% cut) from combined OASDI (17% cut). That nuance fits `fact-check-a-news-article`; add a line that a trust-fund claim names which fund and checks `econ.social_security_trustees_projections` for both rows. Askamerica had the table but did not state the OASDI figure.
- Worth teaching: do not grade a claim "True" on the article outlet's own earlier piece (the $500 a month). It should be "true as reported" or not checkable. This belongs in the same recipe.
- Beneficiary counts: the county table is incomplete (govdata-ops #1495). Until fixed, a recipe or table comment should point to the table's state-total rows for national beneficiary counts, not a county sum.
- Askamerica is #1, so no cause classification is required. The one warehouse-side cause of a wrong figure is a data defect.

Proposed resolution: a — fix health.ssa_oasdi_county so Maryland, Missouri, Nevada and Virginia county rows load (govdata-ops #1495), and have the recipe tell the agent to sanity-check a county sum against the state-total rows.

## Corpus gaps stated

- "Spook the bond market": "Not checkable" — "A prediction no data can test."
- 70 million beneficiaries: "DC and territories are absent from the table." Stated reason is wrong; the table is missing four states' county rows (orchestrator finding).
- TD Securities $41.1T limit binds in January: "Unsupported" — "No TD note found."
- Bill text: "The bill PDF is image-only, so I could not read it."
- $500 a month: "I did not read CRFB's own page."
