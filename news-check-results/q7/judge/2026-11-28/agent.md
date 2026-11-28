# Judge — news q7 — Chalkbeat Colorado, "A college education pays off in Colorado, but not as much as in other states" (HEA Group analysis, 2026-11-24), 2026-11-28

**Question/claim:** Fact-check the article: Colorado completers earn about 55% more than high school-only residents, the second-lowest premium in the US; about $61,000 vs $39,000; 11% of completers in programs earning below a high school graduate; do-no-harm rule; quotes.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files open with 123456789 and close with 987654321 (composer-generated); askamerica ran the production news-check prompt verbatim. No unaided arm's agent.md mentions askamerica (grep count 0 in both); neither cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only; CDHE ROI page; NAICU, EdTrust, TICAS | Web (HEA FAQ PDF, Inside Higher Ed, Deseret, NPR, CMC release) plus warehouse census.acs_earnings_by_education (2023) as context | Web (Deseret citing HEA, Senate HELP FAQ, ED explainer, TICAS) |
| Vintage | HEA 2017-18 cohort; CDHE figures of unstated year | HEA 2017-18 and 2018-19 completers; HEA FAQ; ACS 2023 | Same cohort note; 2024 dollars |
| Headline | 55% "not checkable"; $38,743 CDHE HS baseline matches ~$39k | 55% confirmed, rank 49 of 50; $60,800 vs $39,200; $34,808 x 1.76 = ~$26,450 | 55% confirmed via Deseret; $61k vs $39k = 56%; Alaska 28 lowest |
| Pinocchio grade | 1 (provisional) | 0 | 1 |
| Claims graded | 7 groups | 12 | about 8 items (listed, not itemized) |
| Central claim verdict | Not checkable; not contradicted | True | Mostly accurate; 55% high confidence |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

Central claim, checked with WebFetch on Inside Higher Ed (Oct 28, 2026): "Colorado's earnings premium is 55 percent, ranking 49th of 50 states. Its median undergrad credential earner made $60,800, and its median high school completer made $39,200. The lowest state was Alaska, with a 28 percent premium." This supports askamerica and expert. It contradicts nothing in the article, and it is why everyman's "not checkable" on the 55% is a miss: the figure was findable in press coverage of the HEA release.

- Everyman: used a different measure (CDHE, 43%) and left the central claim open. Fair caution, but under-researched. Missed the 2018-19 cohort detail, the 76% national premium and the $26k gap.
- Expert: found the 55% via Deseret and the cohort discrepancy. Left 76%, $26k and 11% as not checkable, though askamerica found 76% in IHE and computed the $26k gap from the HEA FAQ (34,808 x 1.76 = 61,262; minus 34,808 = 26,454, matches "closer to $26,000").
- Askamerica: $60,800 and $39,200 match the article's rounded "$61,000" and "$39,000". Its extra catches: medians not averages, cohort includes 2018-19, HEA FAQ notes a recalculated October 2026 benchmark ($35,678), CMC release confirms the program cuts but not the "don't benefit students" framing. Warehouse ACS values (Colorado $43,437 etc.) were labelled context, not a check, which is correct. I cannot query the warehouse; those four ACS values are an open item for the orchestrator.
- Unsupported items: all three left the 11% / cosmetology figure and the state-vs-national credential medians unchecked. Askamerica graded the CDHE "no restricted programs" statement "unsupported" because the agency page returned 403. That is calibrated. Everyman and expert marked it not checkable. Same outcome.
- Do-no-harm rule: all three confirmed two of three years and the 2028-29 start with agency or advocacy sources. Everyman noted timing is fuzzy (2028 vs 2029).

Askamerica rating check: claims.json holds 12 claims; report.html states "Rating: 0 Pinocchios (the engine's score from the graded claims ...)"; agent.md states 0 Pinocchios. No mismatch. One mild concern: 0 Pinocchios with four claims graded unsupported (11%, cosmetology, credential medians, CDHE statement) is generous. Everyman and expert gave 1. The engine score follows its rubric, so I do not treat it as an error.

Wrong-table check: the orchestrator notes edu.college_scorecard_programs holds median_earnings_4yr by program. Askamerica ran describe_table on it (seq 7) but never queried it, and no recipe step led it there. See Recipe check.

## Ranking rationale

1. askamerica: correct central verdict with a primary document (HEA FAQ) plus two independent press reports; most claims graded (12); caught the median-vs-average, cohort and benchmark-vintage issues; used "unsupported" only where evidence was blocked. Weakness: did not try the Scorecard table for the 11% claim.
2. expert: correct central verdict (55%, cohort discrepancy, Alaska lowest), good misleading-risk notes, but only one secondary source for the key number, fewer claims itemized, and 76% and $26k left open although they were reachable.
3. everyman: left the central claim not checkable, offered a non-comparable CDHE 43% figure, fewest confirmations. Honest about its limits, but missed what the others found.

What would change the ranking: if a live query showed the Colorado ACS figures askamerica cited are wrong, or if Scorecard data contradicted the 11% claim, askamerica's 0 grade would be weaker and expert could move up. If the HEA table showed Colorado is not 49th, all verdicts change.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipe covering the process: `fact-check-a-news-article` (served at seq 2, followed). Askamerica did not reach a pattern the others missed too: nobody computed the 11% share. The warehouse-native method would be: edu.college_scorecard_programs, filter state = CO, completion cohort matching HEA, compare median_earnings_4yr per program with the state high school benchmark (census.acs_earnings_by_education median_earnings_hs_graduate, with the age caveat), and weight by completer counts if present. Group by CIP code to test the cosmetology share. This is an approximation of HEA's method, so the recipe should require the answer be labelled "Scorecard-based estimate, not HEA's figure". A short broadening of `fact-check-a-news-article` (or a new earnings-vs-benchmark recipe) is warranted. Askamerica is #1, so no loss classification is strictly required; the nearest class is (b).

Proposed resolution: b — add a Scorecard program-earnings vs high school benchmark step to the fact-check recipe so claims like the 11% share are tried against edu.college_scorecard_programs before being graded unsupported.

## Corpus gaps stated

- "The warehouse has no HEA Group aggregates. I searched the catalog for the measure and found census.acs_earnings_by_education and edu.college_scorecard_programs, but the HEA figures are not in them." (stated reason: HEA aggregates absent; Scorecard not queried.)
- "11% ... almost double the national average" and "about a third cosmetology": graded unsupported; reason: "The HEA interactive does not render for automated fetches."
- Bachelor's, associate and certificate state-vs-national figures: graded unsupported; same reason.
- CDHE spokesperson statement: graded unsupported; reason: agency page returned 403, no independent record.
- Hultquist and Nicholas opinions: "not checkable here" (opinions).
- ACS table "uses a different population from HEA's 25-34 benchmark, so it is context, not a check."
