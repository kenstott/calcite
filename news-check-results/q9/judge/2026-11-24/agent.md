# Judge — News q9 (cnbc-stuck-frozen-housing-mobility-turnover), 2026-11-24

**Question/claim:** Fact-check CNBC (Diana Olick, 2026-11-23) "Stuck" housing piece: extract claims, verify against primary sources, grade 0-4 Pinocchio, flag misleading framing.

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. Grep of everyman and expert answers for "askamerica": no hits. No unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web search snippets only | econ.fred_indicators (EXHOSLUSM495S, CSUSHPISA, MORTGAGE30US, PERMIT), plus NAR, Cotality, NAHB, SCF, MND, CNBC linked pages | Web only (NAR, NAHB/Census, MND, JCHS, SCF) |
| Vintage | Oct 2026 | Oct 2026 | Oct 2026 |
| Claims graded | 4 checked of about 10 listed (6 "not checked") | 12 (claims.json) | 12 (about 6 verified) |
| Pinocchio | 1 (low confidence) | 1 | 1 |
| Central claim verdict | Sales claims supported; rate jump overstated | Frozen market supported; rate move "mostly true"; mobility unsupported | Same; mobility 8.3% flagged as possible error |
| Single-family permits -13% | Not checked | "Partially false" (wrong) | Verified -12.9% (right) |
| Visual | None | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

Central permits claim, settled against the Census New Residential Construction October 2026 release (CB26-187, released 2026-11-18, PDF fetched and read):
- Text: total permits 1,295,000, "8.7 percent below the October 2025 rate of 1,418,000" and 7.4 percent below September. Single-family 772,000, 8.7 percent below September's 846,000.
- Table 1a: October 2025 single-family (1 unit) rate is 886 thousand. 772/886 - 1 = -12.9% (-0.1287). Single-family permits are down 12.9% year over year. NAHB reports the same.
- So CNBC's "single-family permits down 13% y/y" is correct. Total permits are -8.7% y/y; the -8.7% single-family figure is month over month.
- Supports expert (-12.9%, verified). Contradicts askamerica, which graded it "partially false" and said "the 13% looks like the all-permits figure with the wrong label". Its own warehouse comparison (-8.7% y/y) was total PERMIT. Its one query that included PERMIT1 (call 14) used dates that omitted 2026-10-01, and no later query pulled PERMIT1 for Oct 2026. It checked the wrong series and graded a true claim as partly false. This is a false accusation against the article.
- everyman did not check permits.

Other claims:
- 16-year-low existing sales: all agree (NAR 3.75M SAAR, lowest since Aug 2010). askamerica also shows 3.75M vs 3.97M in the warehouse.
- Rate 6.60% to 7.61%: all three find the MND figure matches loosely and Freddie Mac is lower. Consistent.
- Builder sentiment (NAHB 25, lowest since Apr 2012): askamerica and expert verified; everyman not checked.
- Prices up over 50%: askamerica computed Case-Shiller 214.99 to 334.23 = +55%, from the warehouse (correct arithmetic: 334.23/214.99 = 1.555). Expert "plausible"; everyman not checked.
- Turnover 3.8% vs 5%: askamerica verified against Cotality; expert and everyman found no source. askamerica extra.
- Redfin six-month buyer decline: askamerica verified; expert did not confirm; everyman not checked.
- 43x wealth gap: askamerica and expert verified ($458,900 vs $10,600, 458.9/10.6 = 43.3).
- Mobility 8.3% vs 11.1%: askamerica "unsupported" and expert "not confirmed" (JCHS 11.2% for 2024). Same calibrated result. Neither graded it false.
- Mortgage jobs -38%: askamerica unsupported (MBA chart -36% through Q2 2025); expert "partly". Both calibrated.
- BLS job counts: expert and everyman not confirmed; askamerica said no matching series. Fine.

askamerica rating check: engine report.html says "Overall: 1 Pinocchio"; agent.md says "Rating: 1 Pinocchio". No mismatch. claims.json has 12 claims, matching the report.

## Ranking rationale

1. expert: no wrong verdict. It got the permits claim right against Census, and kept unverifiable items as "not confirmed". It lost coverage: turnover, Redfin and prices stay unconfirmed or plausible.
2. askamerica: widest checked coverage with primary sources and warehouse numbers (sales, Case-Shiller, rates, 43x, turnover, Redfin). It flagged mobility and jobs calibrated. One real error: grading the single-family permits claim "partially false" and adding a "misleading framing" line about permit labelling that is itself wrong. The overall grade is unaffected, but a false mark on a true claim is a worse failure in a fact-check than an unconfirmed item. Close call with expert; the extra confirmed claims do not outweigh a false negative on a claim that was checkable.
3. everyman: checked only three or four claims, marked most "not checked", no permits, no builder sentiment. Honest limits, but thin coverage.

What would change the ranking: if the warehouse query had pulled PERMIT1 for Oct 2026 and returned -12.9%, askamerica would rank first on coverage. If the orchestrator finds expert's turnover/Redfin non-confirmation to be a miss of readily findable sources, the gap narrows.

## Severity invariant

No flag. askamerica ranks above everyman. (Open item: the warehouse PERMIT1 series for Oct 2026 vs Oct 2025 should be queried live to confirm it returns 772 vs 886.)

## Recipe check

- Existing recipe: `fact-check-a-news-article` (grep recipes.json line 1443) covers claim extraction and testing at the article's own grain. The missed step is the measure grain: "single-family" is a different series (PERMIT1) than total (PERMIT), and the recipe should say to query the series matching the exact qualifier in the claim before grading it. A claim that names a sub-category must not be graded against the total.
- expert's pattern of reading the Census release directly and askamerica's pattern of cross-checking linked pages: both reasonable. askamerica should have read the Census release (primary) rather than trusting CNBC's linked page summary, which it used to label the figure.
- Cause class: (b) reachable pattern not used. PERMIT1 was in the warehouse and was queried once but not at the Oct 2026 date.

Proposed resolution: b — broaden `fact-check-a-news-article` to require querying the exact sub-series named in the claim (econ.fred_indicators PERMIT1 for single-family permits) at the claim's own month and comparing year over year before grading it false.

## Corpus gaps stated

- "The BLS job-loss counts ... were not verified because the warehouse has no matching BLS series." Listed as not checked.
- "One warehouse query for pre-2026 existing-home sales failed on a storage error. The 'lowest since 2010' ranking therefore rests on NAR's release."
- Mobility 8.3%: "No JCHS or Census CPS table with 8.3% loaded" (graded unsupported).
- MBA -38% mortgage jobs: "I did not find a later MBA update" (graded unsupported).

## Orchestrator verification (2026-11-24)

The open item is resolved, and it corrects the cause named above. `econ.fred_indicators` has no PERMIT1 rows at all: a live query for PERMIT1, PERMIT, HOUST1F and PERMIT5 returns only PERMIT (2025-10 1,418; 2026-10 1,295, -8.7% y/y) and HOUST1F. askamerica could not have pulled single-family permits from the warehouse. The loss traces to a data gap, filed as govdata-ops #1476, plus a grading slip: it treated a mismatch with the aggregate as evidence against the sub-series claim instead of grading it unsupported. The existing fact-check recipe already says failing to find a source is not evidence against a claim, so no recipe change is made. The ranking stands.
