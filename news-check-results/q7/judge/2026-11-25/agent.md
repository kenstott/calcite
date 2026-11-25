# Judge — KFF Health News, single-payer cost claims (CEA $47T, Oregon plan) after the midterms, 2026-11-25

**Question/claim:** Fact-check the KFF Health News article "Sticker Shock Looms as a Major Challenge in Push for Single-Payer Healthcare": extract claims, verify against primary sources, grade 0-4 Pinocchios.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep of the everyman and expert answers for "askamerica" found nothing. No unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only (3 searches); article read via summary | Warehouse health.nhe_summary plus 23 web fetches of CEA report, Oregon board deck, Ballotpedia | Web only; CEA report, Oregon draft report, Ballotpedia, CRFB |
| Vintage | NHE 2024 actual | NHE 2024 actual; Oregon Oct 29 board deck; CEA Oct 2026 | NHE projections; Oregon Oct 15 draft; CEA Oct 2026 |
| Pinocchio grade | 1 (provisional, low confidence) | 1 | 1 |
| Claims graded | 10 rows, 3 actually checked | 11 graded (claims.json) plus 5 listed as not verified | 6 sections, about 8 claims |
| Central claim verdict | $47T source not found; $4.7T/yr consistent | CEA $47.41T true; $4.7T/yr derived, mostly true | $47T mislabeled: $47.4T is net deficit, gross $49.4T; $4.7T/yr derived |
| Headline findings | $5.3T confirmed; Oregon $69.7B not found as stated | $5.3T true; $69.7B = $66.2B + $3.5B Part B, best scenario; Milliman $76.3B misattributed; CA Prop 186 26.58% | $5.3T true; CA Prop 186 26.6% so "under a quarter" false; Oregon not checkable (used draft) |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

- Central claim (CEA $47T over ten years). I tried to fetch the CEA PDF and the Oregon board deck. Both returned compressed binary the fetch tool could not read, so I could not confirm $47.4T, $49.4T, or the $66.2B + $3.5B build myself. Open item for the orchestrator.
- Arms agree on the number: askamerica quotes "-47.41T, 2027-2036, constant 2026 dollars"; expert quotes $47.4T net deficit. They differ on framing. Expert adds a gross figure of $49.4T and calls the article's "$47 trillion in federal spending" mislabeled. Askamerica grades it true. If the CEA table does show $47.4T as a deficit and $49.4T as spending, expert's point is sharper. Neither claim could be checked by me.
- Everyman did not find the White House source at all and graded "source not found". That is calibrated but a miss, since the report was findable.
- $5.3T 2024: all three agree, $5,278.6B. Askamerica got it from the warehouse (health.nhe_summary) and the CMS fact sheet. No stale vintage.
- $72T over 2025-2034: askamerica recomputed about $71T by compounding and did not read the projection table; it graded "mostly true". Expert said the projection sums to roughly $72T (series to $8.97T in 2034). Everyman said plausible, did not sum. Askamerica's compounding shortfall is a weak method, but the grade is hedged.
- Oregon $69.7B: askamerica matched the figure exactly to the Oct 29 board deck ($66.2B + $3.5B = 69.7). Substring check: "69.7" appears in claims.json independent_value. Everyman searched Milliman ranges and did not find 69.7. Expert used the earlier Oct 15 draft and stopped at "not checkable". Askamerica reached the newest source.
- Milliman $76.3B: only askamerica caught that it is the board's low case plus Part B, not Milliman's finding. Others missed it.
- Ballot measures: askamerica and expert both found California Prop 186 at 26.58% (above a quarter). Everyman did not check. Expert also noted Massachusetts questions were non-binding and mostly passed; askamerica found no Massachusetts figure.
- Starr 43%, 3 in 5, Taiwan 90%: askamerica graded "unsupported" with stated reasons (403 and empty pages). Expert and everyman left these unchecked or not checkable. Grading "unsupported" where no evidence was found is acceptable here because the reason is stated, though "not checkable" would be the more exact label for the Taiwan claim.
- Mismatch check: engine rating in report.html is "1 Pinocchio (some shading of the facts)". agent.md says 1 Pinocchio. No mismatch.
- All three verdicts of 1 Pinocchio agree.

## Ranking rationale

1. askamerica. Widest claim coverage (11 graded, with the five unverified items named). Only arm to reconcile $69.7B to the board deck and to catch the Milliman misattribution. Used the warehouse for the one claim it covers. Found the "board's most favorable scenario" and "plan about $0.6B below status quo" omission, which is the strongest misleading-framing point. Weak spots: it did not raise the gross-versus-net label on the CEA figure, and its $72T check was a recomputation.
2. expert. Best handling of the central claim: separated gross $49.4T from net deficit $47.4T and added CRFB independent range of $25-36T. Caught the California ballot error. Stopped at the Oct 15 draft for Oregon and called the headline figures "not checkable", so it missed the Milliman issue. Fewer claims.
3. everyman. Only three searches, article read through a summary, most rows "not checked". Did confirm $5.3T and gave useful Oregon ranges, but did not find the CEA source and missed the California ballot error.

What would change the ranking: if the CEA table shows $47.4T is only a net deficit and the article's wording says "federal spending", expert's central verdict is more accurate than askamerica's "true", and expert could move to first. If the $66.2B + $3.5B build on deck p.4 is wrong, askamerica drops.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipe `fact-check-a-news-article` covers extraction, a central claim, and warehouse-first checks. Related: `check-gross-vs-net-before-treating-as-disagreement` (line 593 of recipes.json) fits the expert's gross $49.4T versus net $47.4T point. Askamerica did not apply a gross-versus-net check to the CEA figure. A short pointer from the fact-check recipe to the gross-vs-net recipe for headline dollar totals is warranted. Warehouse-native method: for CEA-style totals, compare the report's gross and net lines before grading a "spending" label. Askamerica is #1, so the cause class is (d), with the optional pointer above.

Proposed resolution: d — askamerica ranks first; optionally link fact-check-a-news-article to the gross-vs-net recipe for headline dollar totals.

## Corpus gaps stated

- "The warehouse covered only the national spending figures (health.nhe_summary). Everything about Oregon, the CEA and the ballot measures was graded from primary documents or searches, not warehouse data." (state-level plan costs, federal proposal scoring, and ballot results are not in the warehouse.)
- Starr 43% "unsupported": "Only the article states the 43% figure; the release was not found... X post not loadable; Starr's legislature page loaded empty."
- Taiwan 90% satisfaction "unsupported": "Health Affairs page returned HTTP 403; no other source read."
- "Up to 3 of 5 pay less" "unsupported": "no 3-in-5 figure found" (board summary says "most households").
- $72T projection: "I did not read the CMS projection table."
- Not independently verified: Vermont 2014, Sanders HELP chair, Nov. 23 release date, WSJ op-ed (401), attributed quotes.

## Orchestrator note: open item resolved

The CEA report ("The Real Cost of Socialism in America", October 2026, chapter 4) gives both numbers: "$49.4 trillion in single-payer health care costs over 2027-2036" and "would increase the federal budget deficit by approximately $47.4 trillion over 2027-2036". The article says the plan "would cost the federal government $47 trillion over 10 years". The net federal cost after the report's offsets is the $47.4T figure, so askamerica's "true" is supported, and expert's "mislabeled" is a narrower reading. The ranking stands.
