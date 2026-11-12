# The Reuters/Investing.com article's claims about US first-half 2026 traffic deaths, rate and trend are balanced and mostly accurate.

**Overall grade: 1 Pinocchio (minor imprecision).** Nothing checked was wrong. The weaknesses are framing and verifiability.

Access: investing.com returned HTTP 403, so I read the identical Reuters (David Shepardson, Nov 10) text through the Taylorville Daily News syndicated copy. The NHTSA first-half press release also returned 403.

## Claim checks
| Claim | Verdict | Evidence |
|---|---|---|
| (Central) First-half deaths at the lowest rate since 2014 | Mostly true | Rate 1.02 per 100M VMT, versus 1.05 a year earlier, per secondary reports only. This is an NHTSA early estimate, not final data. |
| Deaths -2.5% to 16,750 from 17,175; "lowest since 2015" | True on the arithmetic | 16,750/17,175 is -2.47%. The warehouse has no half-year series, so "lowest since 2015" is unverified. |
| On pace for a fifth straight year of declines | Mostly true | FARS in the warehouse: 2021 43,230, 2022 42,721, 2023 41,025, 2024 39,254. NHTSA early estimate for 2025 is 36,640. The fifth decline rests on a projection. |
| 2021 deaths +10.8% to 43,230, most since 2005 | True | Warehouse: 2020 39,007 to 2021 43,230. The 2005 comparison is not loaded. |
| 2025 deaths lowest since 2019; rate 1.10 | True | NHTSA DOT HS 813 829 gives 36,640 for 2025, above 2019's 36,355 (warehouse) and below every later year. Its rate is 1.10. "Second-lowest in history" was not tested. |
| Deaths -5% in Q1 and -0.2% in Q2 | Mostly true | NHTSA's Q1 early estimate was -4.3% (CollisionWeek). The Q2 figure is not verified at the primary source. |
| Bicyclist deaths +4% to 1,148 | True | NHTSA 813 829 Table 2: 1,103 to 1,148, +4%. The "highest in four decades in 2023" part was not checked. |
| Cost figures: $30B taxpayers, $340B economic, $1.37T societal | Mostly true | NHTSA DOT HS 813 403 confirms all three. They are 2019 crash-year figures in 2019 dollars, and the article does not say so. |
| Experts blamed pandemic-era riskier driving (less enforcement, impaired driving) | Not checkable here | Unnamed experts and a causal interpretation. No warehouse table covers driving behavior. |

## Misleading-framing flags
1. **Early estimates are presented as results.** The first-half and 2025 figures are NHTSA projections. The warehouse's FARS final data stops at 2024.
2. **Q2 is nearly flat (-0.2%), which the article gives only a line.** The "fifth straight decline" streak and the 2.5% headline rest mostly on a strong Q1.
3. **The cost study is about 2019, not 2023.** The article calls it a "2023 NHTSA study" without saying the figures are for crash year 2019.
4. **The headline mixes a rate and a count.** "Lowest rate since 2014" is the rate. The death count is "lowest since 2015".

## Method and limits
- Warehouse: `transport.fatal_crashes_by_state_year` (FARS 2008-2024).
- Primary documents read: NHTSA DOT HS 813 829 and 813 403.
- Not read at the primary source: the NHTSA first-half 2026 release and the NSC first-half page, both 403.
- The bicyclist claim has no warehouse column, so it was graded from the NHTSA report.
