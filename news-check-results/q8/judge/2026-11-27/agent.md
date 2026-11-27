# Judge — q8 — WJAC-TV, "Electric rates set to rise Dec. 1 for most PA utilities; PUC urges help options" (2026-11-25), 2026-11-27

**Question/claim:** Fact-check the article. Central claim: 10 of the state's 11 electric utilities raise their residential price to compare on Dec. 1. Also: Penelec 13.142 to 13.564 c/kWh; West Penn 12.075 to 12.386; new rates about 15% and 13% above last December's; 9 of 11 rose last December; PUC ties changes to wholesale/PJM capacity costs; Gov. Shapiro criticized PJM.

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design). A grep of the everyman and expert answers for "askamerica" finds nothing. Neither unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | PUC releases 2026-11-23 and 2025-11-24; article via WebFetch summary only | Same two PUC releases; governor's PJM statement; one warehouse check (EIA PA residential monthly price) | PUC releases 2026-11-23, 2025-11-24, 2026-06-01; governor's PJM statement |
| Vintage | Dec 2026 and Dec 2025 | Same, plus EIA Sept 2026 vs Sept 2025 (22.20 vs 20.43, +8.7%) as context | Dec 2026, Dec 2025, June 2026 |
| Headline figures | All checked numbers match the PUC tables | All match; recomputed 3.21%, +15.5%, +13.1% | All match; same recomputes |
| Pinocchio grade | 1 | 0 (engine report also 0) | 1 |
| Claims graded | 9 | 12 (11 true or mostly true, 1 not checkable) | 14 figures plus 2 not checkable |
| Central claim verdict | Accurate (10 up, UGI down 1.72%) | True | Verified |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

- Central claim, checked myself against the PUC release of 2026-11-23: the table shows 10 increases (Citizens' +11.9%, Duquesne +3%, Met-Ed +3.5%, Penelec +3.2%, Penn Power +3.1%, PECO +10.3%, Pike +8.9% estimated, PPL +10.3%, Wellsboro +10.4%, West Penn +2.6%) and one decrease (UGI -1.72%). Penelec "13.142 to 13.564" and West Penn "12.075 to 12.386" match. All three arms are right on the central claim.
- The release does not mention PJM. It says "regional energy markets" and "wholesale market rates." Everyman called the PJM link "partly checked." Askamerica called it a "minor issue" and "reporter's context." Expert called it "plausible, unverified." All three handled it correctly.
- The 15% and 13% figures: 13.564/11.747 = 15.5%; 12.386/10.947 = 13.1%. Askamerica and expert verified both. Everyman did not list these claims or the "9 of 11 last December" claim, so it missed claims the others caught.
- Shapiro/PJM: everyman said "not checkable here" and did not search for the governor's statement. Askamerica and expert both found a 2026 governor statement and graded it true. Everyman's "not checkable" is a weak use of that label, since the source was reachable.
- Expert noted that the "smaller than last year" framing omits the June 1, 2026 steps (Penelec +11.88%, West Penn +10.3%), and that the 15%/13% figures combine two changes. Askamerica noted the same mid-year reset in agent.md and claims.json but did not put it in the rating.
- Askamerica claims.json vs report: the engine report states "0 Pinocchios (accurate, one minor attribution slip)." agent.md states "0 Pinocchios." No mismatch. Verdicts in claims.json: 11 true, 1 mostly true, 1 not checkable... counted as the file lists them: `true` x11, `mostly true` x1, `not checkable here` x1 (the Facebook quotes).
- Open item for the orchestrator: askamerica's EIA figure (PA residential, 22.20 vs 20.43 c/kWh, Sept 2026 vs Sept 2025) can only be checked by a live query. It is context only and does not drive any verdict.
- Wrong-table check: askamerica correctly did not use the EIA monthly price as a stand-in for utility-level price to compare.

## Ranking rationale

1. expert. Correct central verdict, near-complete claim coverage, primary sources for every claim, and the best framing finding: the June 2026 step that the "smaller than last year" line leaves out. The 1 Pinocchio grade fits one unverified attribution plus selective lead examples. It used "not checkable" only for the two Facebook anecdotes.
2. askamerica. Correct central verdict, 12 claims, primary sources, and a clean split of the one real slip (PJM attribution). It also named the larger increases elsewhere and the mid-year reset. But it graded 0 Pinocchios while writing that the article's PJM attribution is not the PUC's words and that it leaves out the bigger increases and the June reset. That is less calibrated than expert's 1. Its headline sentence ("claims about ... PJM are accurate") also overstates the PJM point it later qualifies. The warehouse check added little.
3. everyman. Right central verdict and right numbers, but it worked from a summary of the article, missed the 15%/13% and nine-of-eleven claims, and gave up on the Shapiro claim without searching. It did raise the framing point about small lead increases.

What would change the ranking: if the orchestrator finds the EIA figure wrong, askamerica drops a little but stays above everyman. If a PUC statement ties the changes to PJM capacity, 0 Pinocchios becomes defensible and askamerica ties expert.

## Severity invariant

No flag. Askamerica ranks above everyman.

## Recipe check

- Existing recipe `fact-check-a-news-article` already covers the pattern: extract every claim, grade from the primary source when the warehouse lacks the measure, and put the search in `measure`. Its triggers include "price to compare." Askamerica followed it: it ran search_catalog, found no utility-level table, and graded from the PUC releases.
- Expert found something askamerica reached but did not carry into the grade: the June 1 PUC release explains the gap between "current" and last December's rate. A broadened step in `fact-check-a-news-article` is warranted: when a claim compares to "last year's" rate, fetch the interim reset release and let omitted intervening steps count as a framing issue in the rating, not only as context. Method is web-based because the warehouse has no utility-level price-to-compare table.
- Rating calibration: a claim graded "mostly true" for an attribution slip plus omitted context should not roll up to 0 Pinocchios.
- Cause for askamerica not #1: (b) reachable pattern not used. Askamerica had the mid-year reset in hand and did not weigh it in the grade.

Proposed resolution: b — extend `fact-check-a-news-article` so that omitted intervening changes and an unverified attribution move the grade to at least 1 Pinocchio.

## Corpus gaps stated

- "search_catalog found no table with utility-level price-to-compare. The closest is energy.eia_electricity_prices_monthly, which gives the PA residential average price ... not the same measure. The utility figures were therefore graded from the PUC primary releases."
- Facebook commenters' quotes (Alicia Sherwood, Missy Ann): "not checkable here. They are personal anecdotes and opinion, and no dataset can test them."
