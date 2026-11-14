# q9 judge: Public Citizen "Corporations Pour Record-Smashing $737 Million Into 2026 Midterms"

Rank: [expert, everyman, askamerica]

**SEVERITY FLAG: askamerica ranks below everyman.**

Published Oct 29, 2026. Judged 2026-11-14. I compared the three arms against each other, read askamerica's call log, and had the askamerica arm run donor-level queries live after it delivered.

## Ratings given
| Arm | Pinocchios | Sources read |
|---|---|---|
| expert | 1 | Full report including Table 2 and the chart; FEC committee page for MAGA Inc.; Fortune, NYT-sourced coverage |
| everyman | 1 | Summary of the piece including Table 2; search snippets |
| askamerica | 0 (engine-scored) | First 6,000 characters of the piece; `fec.committee_summaries` |

## Assessment
- **Correct verdict: 1 Pinocchio.** The arithmetic holds and the MAGA Inc. receipts figure matches FEC. Three things are off: Andreessen Horowitz is $72M in the key findings and $76.2M in Table 2; the "53%" is a share of the 2026 total, not of the surge; and a Musk trust is counted as a corporate entity against the report's own for-profit-corporation method.
- **expert (1st):** read the whole report and found all three.
  - Table 2's split (50 + 23.8 + 2.4) sums to $76.2M, so the key-findings $72M is the inconsistent figure.
  - The chart's nine cycle values sum to $1,813.6M, confirming the 41% share.
  - MAGA Inc. receipts $449,433,545 through Oct 14 from FEC.
- **everyman (2nd):** right rating, and it caught the $72M vs $76.2M inconsistency.
  - Its second "conflict" (MAGA Inc. $449M against $369-401M reports) compares different dates and is not a conflict.
  - No primary source read.
- **askamerica (3rd):** the only arm with primary data, and the wrong verdict.
  - It fetched the article with `max_chars` 6000 and never read the rest. It did not see Table 2, so it noted the $76.2M only as hearsay and did not grade it.
  - It wrote that donor-level figures could not be verified because "donor-name queries are not supported in the warehouse". They are. It ran one query, against `fec.committee_summaries`, and never touched `fec.individual_contributions`.
  - Its one real check is good: MAGA Inc. receipts $449.4M, making $141M the 31% the report states.
  - "Balanced and accurate", 0 Pinocchios, on a partial read.

**What would flip 2nd and 3rd:** weighting askamerica's one primary-data match above everyman's correct rating and its catch of the inconsistency. I do not: a 0 on a piece it read a quarter of is the larger fault.

## What the warehouse shows (run live 2026-11-14, after delivery)
- `fec.individual_contributions`, year 2026, name-filtered: completed with no timeout.
- A16Z CAPITAL MANAGEMENT LLC (ORG) to C00916114: $50,000,000. ANDREESSEN HOROWITZ CAPITAL MANAGEMENT (ORG) to C00504530: $2,400,000. Both match two of Table 2's three components exactly. A16Z to MAGA Inc.: $6,000,000. The $23.8M component was not found under an a16z name.
- 2026 by entity type: ORG $2.16B over 15,491 rows; IND $7.01B.
- MAGA Inc. (C00892471) ORG receipts: $185.5M, against the report's $141M from for-profit corporations giving $5,000 or more. Not reconciled; ORG is broader than for-profit corporations.

## Recipe check / resolution
One resolution, made: the table comment on `fec.individual_contributions` said donor-name lookup "is NOT currently supported ... Do not attempt a name-filtered query" pending kenstott/govdata-ops#780. That issue and its remediation (#901) closed on 2026-10-06, and the queries above ran today. The arm followed a stale instruction. Comment corrected in `govdata/src/main/resources/fec/fec-schema.yaml`: donor-name lookup works with a year filter, and corporate donors are the ORG rows. It reaches the engine at the next govdata build.

Second cause, no change made: the arm chose a 6,000-character window on the article under check. First instance today.

## Severity invariant
**Triggered.** askamerica ranks 3rd, below everyman.

## Defects
- Schema metadata: stale "not supported" warning on `fec.individual_contributions`. Fixed in source today, not yet built.
- askamerica: graded an article after reading its first 6,000 characters.
