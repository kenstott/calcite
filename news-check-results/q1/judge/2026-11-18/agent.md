# Judge — news q1 farmpolicynews-china-non-soy-farm-purchases-7-2b-lowest-since-2019

**Story:** "China unlikely to fulfill $17 billion US ag purchase pledge this year" (Morning Ag Clips / Farm Policy News, 2026-11-17). Claims of interest: China bought $7.2B of non-soybean US farm goods Jan–Sep, lowest for the period since 2019 (Reuters analysis); China "closes in" on a 25 MMT soybean target.

Rank: [expert, askamerica, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **expert (1st):** 1 Pinocchio. Traced all nine claims to the Bloomberg/Reuters/DTN originals, pinned the trade-council date (Fri Nov 13, announced Nov 14), and was the only arm to flag that "close to 25 million tons" rests on an AgResource ~21 MMT estimate while ASA cites ~15 MMT and a September Bloomberg item had China only just over halfway — i.e. the soybean-progress figure is disputed. Did not recompute the $7.2B.
- **askamerica (2nd):** engine rating 0 Pinocchios. Fetched the article, Farm Policy News original, Bloomberg (Yahoo copy), Agri-Pulse; added warehouse evidence the others lack — `ag.fas_export_sales` China corn purchases 14.3 MMT (2022) → 0.03 (2025 label) → 0 (2026 label). But it read soybeans "12.4 MMT (2026 label, partial)" as "consistent with closing in on 25 MMT", which it is not on its face; it did not reconcile marketing-year labels with the calendar-year pledge, so it graded the soybean claim `true` where the evidence it held argued for a caveat. Framing flags (headline overstates Bloomberg's "looks unlikely"; Beijing never confirmed the $17B) match expert's.
- **everyman (3rd):** 1 Pinocchio. Six tool calls, worked from search summaries only, no primary data; correct framing caveats but no independent verification.

What would change the ranking: if the 2026-label 12.4 MMT is in fact the wrong marketing year and China's calendar-2026 total is near 25 MMT, askamerica's `true` would stand and it would tie or lead expert on the strength of its warehouse corn series.

## Harness disclosure
The askamerica arm was resumed twice to fix claim grouping (see Shape). In the second resume I pointed it at `econ.trade_exports` for scale on the $7.2B; it reported US-side Jan–Sep exports to China (HS 01–24 ex-soy) $3.58B 2019, $9.08B 2023, $7.82B 2024, $4.21B 2025, and said the table ends at 2025. That hint is coaching; the ranking above does not credit it. The "ends at 2025" claim rests on a `describe_table` call only (no 2026 query in calls.jsonl) and is unverified — not filed.

## Recipe check
Candidate, not yet written (single instance): FAS export-sales tables are labelled by marketing year (soybeans Sep–Aug); a calendar-year purchase pledge must be compared on a calendar basis, not against a partial marketing-year accumulation. Evidence is from expert's disputed-figure catch plus askamerica's own misread. Held for a second instance per the below-expert discipline.

## Shape
Final claims.json: 6 `author_claims`, 2 `subject_claims` (Ben Buckner/AgResource; American Soybean Association), engine 0 Pinocchios. Original publish had all 8 as `author_claims` (Buckner's quote and ASA's call unattributed) — wrong; resumed and fixed. A request to move relayed figures into `fidelity` was refused by the validator ("A figure or event the piece reports as fact is the piece's own `author_claims` claim ... whether or not the piece attributes it") and withdrawn — same McpServer.java instruction-vs-validator contradiction noted 2026-11-17.

## Gaps
None filed. "Dollar-value customs data is not in the warehouse" refers to Chinese customs (import side) — not a US public dataset to source.

## Severity invariant
Not triggered.
