# q6 judge: StartBusinessByState "Generative AI Patent Tracker: Monthly Filings at the U.S. Patent Office"

Rank: [askamerica, everyman, expert]

A data tracker, updated Nov 3, 2026, with no quotes. Judged 2026-11-14. I compared the three arms against each other. No arm reached USPTO application data (the Open Data Portal API returns HTTP 401 without a key), so every count rests on the publisher's own CSV.

## Ratings given
| Arm | Pinocchios | Sources read |
|---|---|---|
| askamerica | 0 (engine-scored; the arm's own read was 1) | Publisher CSV re-added; all 13 cited links; warehouse grants as a sanity check |
| everyman | 0 (provisional) | Publisher CSV; an earlier snapshot of the page |
| expert | 1 (provisional, low confidence) | Article summary only |

## Assessment
- **Correct verdict: not gradeable against primary data; 0 to 1 on what can be seen.** The piece is internally consistent and discloses its own limits (publications lag filing by about 18 months; the keyword definition is broad). Nothing is shown false. The open risk is revision of recent months.
- **askamerica (1st):** the most work, all of it sound.
  - It re-added the monthly CSV and reproduced every total (939, 1,141, 2,754, 4,187), the 84% rise, the code peak (526) and the AI-share peaks.
  - It fetched all 13 cited links and confirmed the CPC code dates from the January 2023 revision.
  - Sanity check from the warehouse: 327,100 grants dated 2025 against the publisher's 325,041.
  - It flagged the September code spike as exposed to reclassification and the October denominator (50,039, about 36% above a year earlier) as unexplained.
  - It reported that the engine set 0 where it would have said 1.
- **everyman (2nd):** one finding no other arm made.
  - An earlier snapshot of the page showed 3,452 applications for Jan-Sep 2026 and 375 for the September code count; the current page shows 3,603 and 526. Recent months are being revised by 4% and 40%. That is direct evidence for the risk askamerica only inferred.
  - Otherwise it matched the CSV sums and stopped. No Python, no links followed.
- **expert (3rd):** thin. It recomputed one ratio and checked nothing else. Its "ten months of 2026 against full-year 2025" criticism does not hold for the 84% figure, which compares Jan-Oct with Jan-Oct (4,187 vs 2,275).

**What would flip 1st and 2nd:** weighting everyman's revision evidence above askamerica's breadth. I do not; one finding against a full reproduction.

## Recipe check
No recipe. Worth noting for a second instance: for a self-published tracker, compare the current page with an earlier snapshot to measure how far recent months are revised. everyman found it by accident through a stale search result.

## Severity invariant
**Not triggered.** askamerica ranks first.

## Defects
- Data gap: `patents` has grants only, no published applications. Filed as kenstott/govdata-ops#1203.
- Engine rating, second day running: the arm judged 1 and the engine required 0, because unsupported and caveated claims do not move the score. Yesterday's q9 note records the same cause. No change made.
