# Judge — News q6 (pew-split-senate-delegations-120th-congress), 2026-11-24

**Question/claim:** Pew (DeSilver, 2026-11-23): the 120th Congress will have 10 split Senate delegations (3 in the 119th), the most since the 115th; 23 unified Democratic vs 17 unified Republican; Ron Johnson 13th-most conservative, Tammy Baldwin 6th-most liberal.

Rank: [askamerica, expert, everyman]

All three personas ran fresh today. Fingerprint check: everyman and expert prompt.md carry both markers; askamerica received the product's verbatim news prompt, which carries no markers by design. A grep of the everyman and expert answers for "askamerica" found no match. Neither cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Data used | Wikipedia, NBC tracker, arithmetic | officials.members (Congress 117-119), AP, Pew 2021 table, Senate bio, news | Wikipedia results, Yahoo/NBC, Voteview HS119 csv, local news |
| Vintage | Live 2026 results | 2026 results; Congress 112-119 from warehouse | 2026 results; HS119 |
| Headline figures | 10 "plausible but unconfirmed"; found 8 split states | 119th 3, 116th 9, 115th 12-13; 56-44 fits 23/17/10 | Rebuilt 10 split states (AK, IA, KS, MS, NC, OH, SC, TX, PA, WI), 23 D, 17 R |
| Overall grade | 1 Pinocchio (preliminary) | 0 Pinocchios | 0 Pinocchios |
| Claims graded | 7, four "not checked here" | 10, nine true, one unsupported | about 8 rows, historical series not checkable |
| Central claim | Partial | True | Verified |
| Visual | none | dashboard.png, report.html | agent-dashboard.png, agent-report.html |

## Verification

Central claim, checked on the Pew page via WebFetch: 10 split, 23 unified D, 17 unified R, "first in 14 years" (chart says since 2013), Johnson 13th and Baldwin 6th by DW-NOMINATE. The page does not name the 10 states. This supports askamerica and expert (true) over everyman (partial).

- Arithmetic: 23x2+10=56 and 17x2+10=44. All arms agree it fits the 56-44 Senate.
- everyman's 8-state list omitted PA and WI. Those two are 119th splits that stay split in the 120th. Expert's list (8 flips plus PA and WI) gives 10. everyman's doubt about the count came from an incomplete list, not from evidence against Pew.
- 119th = 3: askamerica's warehouse count (3) and expert's list (ME, PA, WI) agree.
- "Since 2019" in the stated task versus "since the 115th": 116th had 9, 10 exceeds it. Both askamerica and expert reach this.
- Johnson/Baldwin: expert checked Voteview (Johnson 14th with VP Vance, 13th without; Baldwin 6th). askamerica left it "unsupported" (not extracted). Real gap on the askamerica side.
- 96th = 27 and the 1973-94 floor: askamerica matched them to Pew's 2021 table. That is the same author's earlier table, not independent, but it is a cited primary source. expert called it not checkable; everyman did the same.
- Morse 84th, Klobuchar/Blackburn, Daines/Tester, Nelson/Martinez: askamerica fetched sources for each. expert graded the anecdotes from background knowledge without a fetch, and said so.
- Warehouse numbers (113th 17/18, 114th 14, 116th 9, 117th-118th 6) cannot be re-queried by me. Open item for the orchestrator: confirm officials.members split counts per Congress. askamerica says its unified counts for the 118th and 119th were off and it used only split counts, which is a calibrated choice.

askamerica claims.json vs report.html vs agent.md: the report says "Rating: 0 Pinocchios", and agent.md says 0 Pinocchios. claims.json has nine true and one unsupported (not scored). No mismatch.

## Ranking rationale

1. askamerica. Correct central verdict, ten claims graded, each with a named source, including the warehouse for the recent series. It checked the causal Morse claim and the framing. It marked the one claim it could not check as unsupported rather than guessing. Weak point: it skipped Voteview, which was reachable.
2. expert. Correct verdict and the strongest check of the headline count, an independent rebuild of the 10 states, plus the only Voteview verification. But it graded the anecdotes from memory, left the whole historical series unchecked, and covered fewer claims.
3. everyman. Could not confirm the central count, got 8 states, and left four of seven claims unchecked. The 1 Pinocchio is a hedge, not a finding. Its framing note on "wave" is reasonable but adds little.

What would change the ranking: if the warehouse split counts for the 112th-119th prove wrong on a live query, askamerica drops below expert. If Voteview data gave a different rank than Pew's 13th/6th, expert's check would carry more weight.

## Severity invariant

No flag. askamerica ranks above everyman.

## Recipe check

The recipe `fact-check-a-news-article` was served and used. Expert did one thing askamerica did not: it verified the DW-NOMINATE ranks from the Voteview member file. grep of recipes.json finds no Voteview or DW-NOMINATE entry, and grep of the govdata schema resources finds no Voteview table. askamerica's other gap is that it leaned on Pew's own 2021 table for pre-2019 history, which is acceptable but not independent. An independent warehouse source for pre-117th Senate rosters is absent (officials.members covers 117-119 only).

Proposed resolution: a — the warehouse has no Voteview/DW-NOMINATE ideology scores and no member roster before the 117th Congress; a sourcing issue for both would let askamerica check these claims.

## Corpus gaps stated

- "officials.members covers 117-119 only" (claims.json searched field; agent.md: "The table covers Congresses 117-119 for this purpose"). The 96th/1973-94 history was graded against Pew's 2021 table, "not in the warehouse".
- Johnson 13th-most conservative / Baldwin 6th-most liberal graded "unsupported": "I did not extract Voteview rankings." Only the party pairing was confirmed in the warehouse.
- Context links (ScienceDirect, JSTOR, De Gruyter, NYT, bioguide) returned 301/403 or empty pages; treated as context citations, not graded claims.
