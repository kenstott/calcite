# q3 judge: RedState "Winning for Veterans: Trump-Era VA Cuts Backlog 70 Percent, Opens 47 Clinics Ahead of Veterans Day"

Rank: [expert, askamerica, everyman]

Published Nov 11, 2026. Judged 2026-11-14. I compared the three arms against each other; the figures they share (3,111,399 claims; 51,936 and 48,968 housed; $1.17B grants) agree across arms.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| expert | 1 | VA Monday Morning Workload Reports (computed values); VA releases |
| askamerica | 1 | VA releases (Oct 15, Oct 29, Nov 9, Nov 11, Nov 12); no warehouse table applied |
| everyman | 1 | None direct (news.va.gov unreachable); press coverage and snippets |

## Assessment
- **Correct verdict: 1 Pinocchio.** The numbers are VA's own and they hold. The shading is in the framing: "backlog" means only claims pending over 125 days, the 47th clinic is a relocation, and the homelessness money was announced in September, not on Veterans Day.
- **expert (1st):** the only arm to test the headline figure from primary data.
  - Backlog 252,406 (Jan 18, 2025) to 79,546 (Nov 7, 2026): -68.5%. Total pending inventory fell only 25% (956,215 to 714,126). That second number is the context the piece leaves out.
  - It could not reproduce "processing time down 46%" (days pending -34%, days to complete -54 to -58%) and said so.
  - It caught the backlog rising again since July, and VA's own "two years" vs "three years" inconsistency.
  - It left the Collins quote unchecked (no transcript).
- **askamerica (2nd):** thorough on VA's releases, but no independent number.
  - It found the relocation behind clinic 47, the September date of the grants, and the narrow backlog definition.
  - It graded the author's "announced a major plan" as mostly false, a finding no other arm made.
  - It tried the weekly workload workbook and got formulas instead of values, so the central claim rests on VA's own statement.
  - It marked "processing time cut 46%" true on the strength of VA's release; expert shows it does not reproduce cleanly.
- **everyman (3rd):** right rating, thinnest support. It reached no VA page, left the Collins quote and the 23.5% uncomputed, and called the backlog claim "even understated" on the July figure without noticing the rise since.

**What would change the ranking:** nothing close. Expert's primary-data check of the central claim separates it clearly.

## Recipe check
No recipe. The gap is data, not method: the warehouse holds no VA operational series, so there is no warehouse-native procedure to teach. Resolution for this loss is the sourcing issue below.

## Severity invariant
**Not triggered.** askamerica ranks above everyman.

## Defects
- Data gap: no VA benefits data (weekly claims inventory and backlog, claims processed). Filed as kenstott/govdata-ops#1201. The same issue records that the engine's web fetch returned formulas, not values, from the VA weekly workbook.
