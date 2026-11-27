# Judge — q5 Patently-O (Crouch), "Issue fee to grant: record weekly patent issues are a USPTO catch-up", 2026-11-27

**Question/claim:** Fact-check the article. Central claims: USPTO issued 9,828 utility patents on 2026-11-03, the largest weekly issue in records back to 1976; the 11/10, 11/17, 11/24 issues rank 2nd-4th; prior record 7,837 on 2023-10-03; issue-fee-to-grant wait doubled and is now 49 days; the records are a catch-up.

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and contain 987654321 (pass); askamerica ran the extension's verbatim news prompt (no fingerprint by design). A grep of the everyman and expert answers for "askamerica" found no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only; Gazette index pages, no counts | Warehouse patents.patent_grants (2010-2025) plus web (Gazette TOC, no counts) | USPTO Official Gazette patent lists, weeks 1-47 of 2026, own tally of patent numbers; Patently-O Oct 25 post |
| Vintage/window | none for numbers | 2010-2025 for the 2023 record; nothing for 2026 | 2023-2026 |
| Headline figures | none | Oct 3 2023 = 7,874 utility (7,891 all types) vs 7,837 claimed; 2026 figures unsupported | Nov 3 = 9,913 (vs 9,828); Nov 10/17/24 = 8,678/8,840/7,948; 2026 max before Nov = 7,646; Oct 2023 = 7,946 by range |
| Overall grade | No rating assigned | agent.md "1 Pinocchio, provisional"; report.html "too few to rate" | 0 Pinocchios |
| Claims graded | 7 listed, 0 verified | 6 (1 true, 5 unsupported) | 7 (4 supported or approximate, 3 not checkable or partial) |
| Central claim verdict | Not checkable | Unsupported (warehouse ends 2025) | Confirmed within 0.9%; 1976 part not tested |
| Visual | none | dashboard.png | agent-dashboard.png |

## Verification

Central claim, primary source: I fetched the USPTO Official Gazette page for the Nov 3, 2026 issue (patentsgazette.uspto.gov/week44/OG/patent.html). The page's issue date is 20261103. The fetch tool tallied about 9,913 sequential utility numbers (12,777,987 to 12,787,899), plus 25 older-numbered entries (about 9,938 total). So the 9,828 claim is close to the Gazette (about 0.9% below the sequential count). This supports expert's verdict and matches its number exactly (9,913). A web search found no outside source for 9,828 and returned only older records (7,669 for Oct 3 2023 in a 2024 Patently-O post), so the Gazette is the only primary check. I did not verify the 2nd-4th ranks, the 49 days or the 1976 record myself.

Cross-arm numbers:
- everyman: no numbers. Marked all 7 claims "not checkable". The Gazette lists were reachable (expert and I read them), so "needs bulk data" was too cautious. It did note the Gazette confirms the issue dates exist.
- askamerica: 7,874 utility on 2023-10-03 from the warehouse, labelled a counting-basis gap vs 7,837 (0.5%). Fair. Open item for the orchestrator: a 2024 Patently-O post (from search) quotes 7,669 for that date, and expert's Gazette range gives 7,946; the three sources disagree by basis. A live query by patent_type would settle which one 7,837 matches. askamerica's "no other week 2010-2025 is higher" rests on the same table and is plausible.
- expert: counts are its own tally of contiguous utility numbers, disclosed as such. The consistent roughly 1% overshoot in 2023 and 2026 is flagged as a likely counting-basis difference. It confirmed the 2nd-4th ranks (8,678, 8,840, 7,948 vs a 2026 max of 7,646 before November) and pulled the author's Oct post for the doubled wait (median 29 to 60 days). It honestly left the 49 days and the 1976 record open.

askamerica claims.json vs report: claims.json has 1 "true" and 5 "unsupported", no overall rating field. report.html headline says "too few to rate; 5 claims could not be confirmed" (no Pinocchio rating). agent.md states "1 Pinocchio, provisional". MISMATCH: agent.md assigns a grade the engine did not compute.

Calibration: askamerica graded 2 to 4 as "unsupported" after reading only the Gazette TOC page (no counts). The patent.html list pages carry the numbers, so "unsupported" was reached without trying the reachable primary page. Its use of "unsupported" instead of true or false is calibrated. everyman likewise. Neither graded without evidence. Expert missed nothing the others caught; askamerica's warehouse check of the 2023 record is a claim expert covered only through the Gazette.

## Ranking rationale

1. expert: only arm that reached the primary data for the central claim and the rank claim, disclosed its counting method, and graded 0 Pinocchios, which agrees with the Gazette check. Left the unprovable parts open.
2. askamerica: one claim verified from the warehouse with a correct basis caveat, central claim honestly left unsupported. Lost points for not finding the Gazette patent lists, and for a stated "1 Pinocchio, provisional" that the engine's report does not carry.
3. everyman: verified nothing, though it was honest and declined to rate. It did not try the Gazette lists.

What would change the ranking: if the Gazette lists were shown to be not the author's basis by a large margin (not the case at 0.9%), expert's confirmation would weaken. If askamerica's live 2023 figure matched 7,837 on a specific basis, askamerica's one verified claim would be stronger, but it would still not pass everything expert covered.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

- everyman: no pattern worth teaching.
- expert: opened the Gazette patent list pages and counted contiguous utility patent numbers per issue; this is the pattern askamerica missed. Existing recipes that touch it: `fact-check-a-news-article` (served to askamerica at seq 1), `live-api-pull-when-headline-figure-is-stale`, `retry-a-working-access-technique-before-declaring-a-source-unreachable`. A broadened `fact-check-a-news-article` or `live-api-pull-when-headline-figure-is-stale` could add: when the warehouse vintage ends before the claim, fetch the agency's own issue/release listing pages (not only the table of contents) and compute the figure from them. Warehouse-native method: patents.patent_grants grouped by patent_date and patent_type for history, then agency listing for the current period.
- askamerica also had the 2010-2025 weekly history correct, which gave a useful baseline.
- Cause classification: askamerica is not #1. The warehouse also ends at grant year 2025 (filed as kenstott/govdata-ops#1602), but the reachable web page was the deciding gap, so the single cause is (b).

Proposed resolution: b — extend the fact-check recipe to say that when the warehouse vintage ends before the claim, read the agency's per-issue listing pages and count, not only the TOC, before grading "unsupported".

## Corpus gaps stated

- "The warehouse ends in 2025." (central claim and ranks 2-4: "No 2026 weekly counts in warehouse or found online.")
- "Warehouse ends 2025; USPTO Official Gazette week 44 page fetched and contained no Nov 3 2026 count; web search found no confirming source." (central claim, claims.json)
- "Author's own computation; no public series found; rest of post paywalled." (49-day wait)
- "The pre-2010 record was only checked against the warehouse's 2010-2025 range." (1976 record)
- Causal claims: "Causal explanation; evidence paywalled" and "Interpretation untestable here."
