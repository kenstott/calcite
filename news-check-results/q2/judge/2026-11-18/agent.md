# Judge — news q2 newamerican-cis-foreign-born-acs-174k-growth

**Story:** "Census Bureau: U.S. foreign-born population stagnated or declined in 2025-26" (The New American, 2026-11-12). Claims of interest: ACS foreign-born growth "just 174,000 from mid-2024 to mid-2025" vs 2.4M the year before (Steven Camarota, CIS); CPS ASEC −790k and monthly CPS −590k; Brookings negative 2025 net migration; CBP "lowest since fiscal 1967".

Rank: [expert, askamerica, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **expert (1st):** 2 Pinocchios. Clearest on the headline problem — the piece credits "the Census Bureau" for declines that are CIS's analysis of the CPS, while the ACS actually shows growth through July 2025. Brought in CBO's +400k as the omitted counterweight to Brookings, and CIS's own nonresponse caveat. Graded the CBP 1967 claim not supported against CBP's FY2025 release (lowest since 1970, 237,565 vs 201,780).
- **askamerica (2nd):** engine 1 Pinocchio. Fetched or attempted all 20 linked pages; correctly kept the Camarota figure as a `fidelity` relay. Honest `unsupported` on the 2026 CPS −790k/−590k (CIS 403, no other source). On CBP it went further than the other two — found that the 1967 benchmark depends on nationwide (94,778) vs southwest (~74,000) series and that the FY2026 figure the piece leans on rests on leaked internal numbers — and graded `mostly true`; that is defensible but leaves the reader without a firm verdict. Warehouse evidence (`census.acs_nativity`, ACS 5-year 2019–2024, 44.0M → 47.35M) could not test a single-year change. Missed the Census-vs-CIS attribution problem in the headline that expert led with.
- **everyman (3rd):** 2 Pinocchios. Reasonable framing caveats, but its "SUPPORTED" on the March-2026 CPS ASEC decline cites a January 2026 CIS report, which cannot cover a March 2026 survey — the support is mis-sourced. CBP verdict same as expert.

What would change the ranking: if the article's 1967 benchmark refers to FY2026 nationwide apprehensions (as askamerica's evidence suggests), expert's and everyman's "not supported" are graded against the wrong year and askamerica would edge ahead of expert.

## Recipe check
No recipe. The loss traces to a corpus gap, not a missed warehouse method: the single-year ACS change needs ACS 1-year nativity, which the warehouse does not hold.

## Resolution
Filed kenstott/govdata-ops#1320 (`type:sourcing`, `kind:gap`, `schema:census`, `status:open`): ACS 1-year nativity/citizenship (B05001/B05002). Verified first: the 2025 ACS 1-year is published (`api.census.gov/data/2025/acs/acs1.json` 200) and already loaded for `acs1_population`/`acs1_income` (observedCoverage maxYear 2025); only nativity is missing. #805 (closed as unpublished) is related but distinct.

## Shape
claims.json: 5 `author_claims`, 1 `fidelity` (Camarota/CIS ACS relay). Accepted by the validator; Brookings and CBP "according to" claims sit under `author_claims` per the validator's rule that reported facts are the author's claims. No fix needed.

## Severity invariant
Not triggered.
