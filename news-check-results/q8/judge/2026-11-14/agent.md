# q8 judge: ABC News (via ABC30) "Thanksgiving travel forecast: Record number of travelers expected to fly, take trains"

Rank: [expert, askamerica, everyman]

Published Nov 10-11, 2026. Judged 2026-11-14. I compared the three arms against each other. Nearly every figure in the piece is a private forecast, so the check is web research, not warehouse data.

## Ratings given
| Arm | Pinocchios | Sources read |
|---|---|---|
| expert | 2 | A4A Nov 10, 2026 release; Hopper index coverage; coverage of the American and United releases |
| askamerica | 1 | Hopper index; ABC's own Sep 26 piece; A4A 2025 forecast; coverage of the American release |
| everyman | 1 | Search summaries of the same |

## Assessment
- **Correct verdict: 1 to 2 Pinocchios.** Two things are wrong. Hopper's published figure for domestic Thanksgiving fares is +31% ($402), not +39%. And the "record" headline is not supported for air travel: A4A projects nearly 29M passengers against 31M+ forecast last year. The carrier figures check out.
- **expert (1st):** it found the two releases that settle the piece.
  - A4A's Nov 10, 2026 release: nearly 29M for Nov 20-30 (2.7M a day) against 31M+ for the 2025 window (2.8M a day), and no record claim in the release itself.
  - United's Nov 12 figures confirmed through coverage of its release.
  - It noted the "every 13 seconds" figure belongs to the Nov 29 peak in American's release, not the whole period.
  - Same 39% vs 31% finding as askamerica.
- **askamerica (2nd):** the same two core findings, with less behind them.
  - It caught 39% vs 31% and that 29M is below last year's 31M forecast.
  - It could not find A4A's 2026 release or any United release, and left both "unsupported". Both exist.
  - It verified the 13-second arithmetic (6,538 flights a day) and the calendar (Nov 29 is the Sunday after Thanksgiving).
  - It looked in the warehouse first and said correctly that no table carries any of these measures.
- **everyman (3rd):** marked "2.2% more flights" supported, which neither other arm could source, and left the 39% figure "not checkable" though it had found the 31%. It did not test the record claim against last year.

**What would flip 1st and 2nd:** nothing on the findings. Expert has the same conclusions and the primary releases for them.

## Recipe check
No recipe; first instance. The gap was a web-search miss on two trade-body and airline press releases, not a warehouse method. If it recurs: for a claim attributed to a named organisation's forecast, search that organisation's newsroom for the dated release before grading it unsupported.

## Severity invariant
**Not triggered.** askamerica ranks above everyman.

## Defects
- Data gap: no daily air-passenger series for testing "record" travel claims. Filed as kenstott/govdata-ops#1205.
