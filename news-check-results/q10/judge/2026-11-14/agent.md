# q10 judge: Nevada Governor's Office "Nevada Leads Nation in Job Growth for Eleven Straight Months"

Rank: [expert, askamerica, everyman]

The release is dated June 23, 2026 and cites May 2026 data. It is well outside the 7-day window; its date was unconfirmed when it was picked. Judged 2026-11-14. I compared the three arms against each other, read askamerica's call log, and had the askamerica arm re-check two of its own data statements live after delivery.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| expert | 1 | BLS state employment via public API, 51 jurisdictions |
| askamerica | 0 (engine-scored; the arm's own read was higher) | Warehouse state payroll and household series; DETR PDF linked in the release |
| everyman | 1 | BLS state release, latest two months |

## Assessment
- **Correct verdict: 1 Pinocchio.** The streak is real on nonfarm payroll jobs (Nevada first every month from July 2025 through May 2026). The release calls the measure "total employment" and links the household-survey page, where the streak fails. The $6 billion investment figure is the governor's own and is projected, not realized. The credit to policy is asserted.
- **expert (1st, narrowly):** right rating, nothing graded wrongly.
  - Streak confirmed for the same eleven months askamerica found.
  - Two pieces of context no other arm had: the series steps up from 1,577k to 1,595k between June and July 2025, exactly where the streak starts; and Nevada's lead ended after the release (rank 4 in July 2026, rank 3 in September).
  - GOED's $6 billion is projected capital from approved incentives.
- **askamerica (2nd):** the richest findings, one wrong grade, and a 0.
  - It is the only arm to test the household series: on LAUS employment Nevada led only January to March 2026, and South Carolina led April and May. That is the release's central mislabel, found from the warehouse.
  - Unemployment comparisons matched exactly; North Carolina's second place explained as a vintage difference.
  - It graded the industry-leadership figures "unsupported" because its supersector query returned nothing. The data is there. On re-check the arm found it had used the wrong series-id pattern. It also reported "duplicated rows"; the table has none (10,386 rows, 10,386 distinct keys for 2026).
  - It said the piece sits above the engine's 0.
- **everyman (3rd):** verified two of the eleven months. It says the release omits Nevada's 5.2% unemployment rate; askamerica read the release and found the unemployment comparisons in it.

**What would flip 1st and 2nd:** weighting the household-vs-payroll finding above the rating and the wrong grade. It is close.

## Recipe check / resolution
One resolution, made: the BLS state series-id layout was documented only in a YAML code comment on `econ.state_industry`, which the engine never shows. Moved into the table comment in `govdata/src/main/resources/econ/econ-schema.yaml`, with the Nevada supersector ids verified live today. It reaches the engine at the next govdata build.

## Severity invariant
**Not triggered.** askamerica ranks above everyman.

## Defects
- Schema metadata: series-id layout not visible to the engine. Fixed in source today, not yet built.
- Engine/report: title carries a doubled verdict clause ("...overstates its causal and investment claims are biased and accurate"). Second instance today (also q4).
- Engine rating: third story today where the arm said its own read was above the engine's 0 (also q6, q7).
