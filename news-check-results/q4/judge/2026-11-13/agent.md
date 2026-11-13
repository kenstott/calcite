# q4 judge: BOE Report / Reuters "US power demand to beat record highs in 2026 and 2027 as AI use surges, EIA says"

Rank: [askamerica, expert, everyman]

The story ran Nov 10, 2026, the day EIA released the November STEO. Judged 2026-11-13. The article was read through a summarizing fetch (no verbatim text). The STEO Table 7a PDF was not machine-readable in my fetch, so totals were verified through the checks' agreement (all three report 4,216 / 4,306 / 4,371) and the STEO narrative page. Earlier STEO coverage (APPA) shows EIA itself attributing growth to "data center development and increased manufacturing activity in the commercial and industrial sectors"; the Nov narrative page read does not mention data centers or AI.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 0 | STEO PDF Tables 7a/5a and page-3 shares; warehouse EIA retail sales and generation; EIA gas history |
| expert | 0 | STEO Table 7a, Table 5a, electricity narrative; NREL databook via search |
| everyman | 0 | STEO Tables 7a/5a and web pages |

## Assessment
- **Correct verdict: 0 Pinocchios.** All figures match the STEO within a 1 BkWh rounding (4,370 vs 4,371). The prior-high comparisons are internally consistent and nothing contradicts them. The AI/data-center cause is Reuters' standard framing and matches EIA's prior stated attribution, even though the Nov narrative page names only commercial and industrial demand.
  - Total 4,216 / 4,306 / 4,370: true (rounding).
  - Sector sales 1,546 / 1,557 / 1,061 and highs 1,519 / 1,503: true. Industrial 1,061 vs 1,064 (2000) holds for 2026; the STEO's 2027 industrial figure (1,086) exceeds it, but the article makes no 2027 claim, so this is context, not error.
  - Generation shares (coal 17/15/15, gas 40/40/39, nuclear 18, renewables 24/25/28): match the STEO and warehouse 2025 actuals.
  - Gas sales 12.2 / 9.4 / 23.8 / 37.4 bcfd: true; 1996 residential 14.3 confirmed.
  - Third straight record year: true (warehouse retail sales 2023 down, 2024 and 2025 records).
- **askamerica (1st):** correct rating and the most claims actually verified.
  - It was the only arm to verify third-straight-record from data, the shares including renewables, and the 1996 residential gas high.
  - It flagged the AI attribution as reporter gloss, and the 2027 residential dip, without over-penalizing.
  - Weaknesses: did not notice the 2027 industrial figure exceeding the 2000 high; 1973 industrial gas high unconfirmed (disclosed).
- **expert (2nd):** correct rating and the one unique substantive observation.
  - It caught that STEO industrial sales in 2027 (1,086) exceed the 2000 high, qualifying the "falls short" claim.
  - Weaknesses: left third straight record, pre-2025 sector records, and the gas highs unchecked, so it verified fewer claims.
- **everyman (3rd):** correct rating, least verified.
  - It left coal 2026, the gas 39/40 split and renewables unverified, and the third-straight record unpulled; it never checked prior-high claims.
  - Positives: honest about limits, and noted the STEO page's internal inconsistency between generation text (4,390) and table (4,539).

## Recipe check
Partly. A claim-extraction step mapping every "vs prior high" comparison to a historical series check would have helped everyman and expert. For forecast stories, grading on the primary STEO tables (as all three did) is the right route; the warehouse ends at 2025 actuals and serves only for history. A recipe note that the STEO narrative page can disagree with the tables (text 4,390 vs table 4,539 generation) would save time.

## Severity invariant
**Not triggered:** askamerica ranks above everyman, and all three rated 0, matching the correct verdict.
