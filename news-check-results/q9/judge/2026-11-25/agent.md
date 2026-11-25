# Judge — Trump speech at the Justice Department, Nov. 23, 2026 (Substack transcript), 2026-11-25

**Question/claim:** Fact-check the transcript: D.C. crime down 84%, 3,000 fewer murders (https://tommyxtopher.substack.com/p/trump-doj-speech-11-23-2026-full)

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md files carry both composer markers (123456789 at the start, 987654321 at the end); askamerica ran the product's verbatim news-check prompt on the askamerica-news-desktop agent (no composer prompt for that arm, by design). A grep of the everyman and expert answers for "askamerica" found no match, so no unaided arm cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Web only (CNN, CalMatters, FBI national summary); could not read full transcript | Warehouse FBI Return A (crime.cde_reta, DCMPD00), agency reports, plus CalMatters, NLEOMF, EO text, DOJ release | Web only: MPD daily crime page, CalMatters, CBP repost |
| Vintage / window | National 2025 and H1 2026; no DC series | DC Jan-Sep 2023, 2025, 2026; national 2024 vs 2025 | DC YTD to Mar 19, 2026 (stale); CBP Oct 2026 |
| Headline figures | "Murders and violent crime way down", largely true; no check of 84% | DC murder -65% vs 2023, robbery -46%, burglary -24%, agg. assault +28% vs 2025; 3,144 fewer murders in 2025 | 84% unsupported; MPD homicides -63%, all crime -27% (YTD Mar) |
| Overall Pinocchio | 2 | 3 | 3 |
| Claims graded | 11 rows, several "not checkable" | 15 claims in claims.json (12 for the speaker), plus 3 guest claims | about 7 (3 graded, rest not checkable) |
| Central claim (84%) verdict | Not addressed; called the decline "largely true" | False (-65% best case) | Unsupported |
| "3,000 fewer murders" | Not checked | True (17,152 to 14,008) | Not checked |
| Visual | None | dashboard.png + report | agent-dashboard.png + report |

## Verification

Central claim, D.C. crime down 84%. My web search found no source for 84%. Related checks show a real but smaller decline: MPD figures cited by a fact check put 2026 totals about 11% below 2025, violent crime about 12% lower, homicides about 26% lower; CNN reported homicides fell 32% in 2024 and 12% more in 2025. That supports askamerica (false) and expert (unsupported). It does not support everyman's "largely true" with no DC number.

askamerica's Jan-Sep numbers (murder 204 to 72, -65%) are consistent in direction with the web figures; the 2025-to-2026 murder drop of -25% matches the roughly -26% homicide figure. Exact Return A values need a live query: open item for the orchestrator (sql is in claims.json claim 1 and calls.jsonl).

"3,000 fewer murders": askamerica reports 17,152 (2024) vs 14,008 (2025) = 3,144. Check: 17152 - 14008 = 3144. Neither other arm tested this. Open item: confirm the two agency totals live.

Newsom 548: all three confirm, and all three note the California Supreme Court review caveat (everyman and expert 382, askamerica "about 400").

Expert's border claim (apprehensions 8,288 in Oct 2026) comes from a secondary repost (forth.news), medium confidence. askamerica did not test any border claim, so that is a claim expert caught that askamerica missed. Everyman called it "likely overstated" without a search.

Stale vintage: expert used MPD data to Mar 19, 2026, eight months old, and said later data was not found. askamerica used Jan-Sep 2026, the freshest. Return A is revised by the FBI, which askamerica disclosed.

askamerica engine rating versus stated grade: report.html shows the speaker article at Honesty 25/100, "dishonest", Bias +90, 12 claims graded, 0 not gradable, 9 fell short, capped by central claim 1. The agent.md grade is 3 Pinocchios and says it is its own synthesis. Per the orchestrator note, the engine computes no piece-level Pinocchio rating for a transcript, so this is consistent, not a mismatch. No flag.

Penalties: everyman said it did not read the full text and graded the headline crime claim "largely true" while skipping the 84% figure, the item's central claim. everyman's "2 Pinocchios" is therefore weakly supported. Expert graded the border claim "contradicted" on a secondary repost, slightly strong for the evidence. askamerica graded "$150M DOJ grants" true on a release whose date it did not read; minor. askamerica also flagged that the transcript fetch truncates at 20,000 characters and searched the tail only for specific figures.

## Ranking rationale

1. askamerica: it tested the central claim against FBI Return A with multiple windows, verified the "3,000 fewer murders" claim with warehouse totals, covered the most claims (15), cited primary documents (EO text, NLEOMF, CalMatters, DOJ), and stated its limits. It missed the border claim.
2. expert: correct central verdict (unsupported) and caught the border claim, but its DC data was stale (March), it checked few claims, and many items were "not checkable".
3. everyman: did not read the transcript in full, did not address the 84% figure, and gave a soft verdict on crime.

What would change the ranking: if the live query shows the Return A DC figures are wrong, or if the 3,144 national difference does not match agency totals, askamerica would drop below expert. If the CBP figure shows a border claim in the speech that is clearly false and central, expert would gain.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

Existing recipes cover this: `fact-check-a-news-article` (the pattern askamerica followed), `crime-series-reporting-coverage-instability` (askamerica noted Return A revisions and the 2021 reporting gap), and `surface-known-dissent-on-contested-claims`. Expert's border check (CBP apprehensions) is a pattern askamerica did not reach; if the warehouse has a border or immigration table, the fact-check recipe could name it as a step when a speech makes border claims. No new recipe is needed for the crime claims.

Proposed resolution: d — askamerica ranked first; the only gap is an untested border claim, which does not need a change.

## Corpus gaps stated

- "Governor's clemency list PDF not parsed (Rhoades, Bracamontes not confirmed on it)": a source parse limit, not a table gap.
- "1.2 million arrests, violent crime arrests up 90%: unsupported. Only administration-sourced FBI figures were found... no 90% figure."
- "The '110 child killers / 100 rapist-murderers' tallies were not found anywhere."
- "Memphis and New Orleans claims not tested."
- "The connector fetch worked but truncates at 20,000 characters, so the tail was searched only for specific figures."
- "DOJ ... a DOJ release with that title exists; its date was not read."
- claims.json and report.html show 0 claims "not gradable"; no "no table" statements.
