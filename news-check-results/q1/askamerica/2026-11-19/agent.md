**The CNBC article's claims about the NAHB builder sentiment index, price cuts and mortgage rates are mostly accurate.**

Rating: 1 Pinocchio.

Method: the warehouse has no NAHB HMI table (search_catalog: "nahb" unmatched). NAHB figures were checked against NAHB's Nov 17, 2026 release, its HMI page and Eye on Housing, all fetched. Rates were checked against the warehouse (econ.fred_indicators, Freddie Mac) and Mortgage News Daily (MND).

## Author claims (CNBC's own voice)
| Claim | Result |
|---|---|
| HMI fell 4 points to 25; lowest since April 2012 (central); Nov 2025 was 38 | True |
| Component readings, regional 3-month averages | True, match NAHB |
| 46% cut prices, post-Covid high; 68% used incentives; 20th straight month at 60%+ | True |
| 30-year hit 7.51% Monday per MND; 6.38% a year ago | True. MND Nov 16 (Monday) = 7.51%, Nov 17 = 7.48%. The year-ago MND figure was not independently verified (Freddie Mac 6.24% Nov 13, 2025, a different survey) |
| Rates "pulled back slightly in October, but surged higher again last week" | Partially false on Freddie Mac: 7.28 on Oct 1, 7.40 peak Oct 8-15, 7.24 Oct 29, 7.11 Nov 12. MND daily October series not checked |

## Subject claims (quoted people)
- **Bill Owens, NAHB chairman (central): enforcement keeps legal workers off sites, fuel prices make subcontractors raise rates "sharply", more builders scale back.** Unsupported. The quote is accurate to the release, but no independent evidence of the labor and cost effects was found. NAHB's own chief economist words it more mildly (enforcement "is affecting labor availability in some markets"; no "sharply"). The HMI measures sentiment, not labor causes. This is an interested party's causal assertion.
- **Robert Dietz, NAHB chief economist (central): rates above 7%, softer consumer confidence and uncertainty are holding buyers back.** Mostly true. Rates were above 7% (Freddie Mac 7.11-7.40 since Oct 1; MND 7.46-7.51), and buyer traffic fell to 16, a 15-year low excluding 2020. The causal link is an economist's judgment, not independently shown.

## Flags
"Plunges" is strong for a 4-point drop, though the level is a 14-year low. The article presents Owens' stronger labor-and-cost causal claim without noting the economist's milder wording of the same point.

Sources: NAHB release; NAHB HMI page; Eye on Housing; Mortgage News Daily; warehouse econ.fred_indicators (MORTGAGE30US).