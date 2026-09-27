## Verdict: Not checkable from AskAmerica's own data — but both figures in the claim are independently confirmed accurate

### The claim
CDC reports 3,659 confirmed measles cases in the U.S. in 2026 (as of September 24, 2026), and MMR vaccination coverage among U.S. kindergartners decreased from 95.2% (2019-2020 school year) to 92.4% (2025-2026 school year).

### What the corpus can and cannot answer
The AskAmerica/govdata warehouse has **no table for either half of this claim**:

- **No measles case-count table.** Searched `search_catalog` with "measles cases," "NNDSS notifiable disease," and "infectious disease outbreak surveillance" — zero matches anywhere in the corpus. The health schema's disease-surveillance tables are `cdc_wonder_std_morbidity` (STI cases, 1984-2014, frozen), `cdc_wonder_tb` (tuberculosis, 1993-2024), and cause-of-death tables — none carry measles or any notifiable-disease case counts. Confirmed by directly listing all 68 tables in the `health` schema.
- **No kindergarten/school vaccination coverage table.** Searched with "kindergarten vaccination coverage MMR," "school immunization exemption rate," and "vaccination coverage children" — zero matches. The only vaccination table in the corpus, `health.cdc_covid_vaccinations`, is COVID-19-only, national-level, and explicitly documented as frozen at 2023-05-10 with no updates since — structurally irrelevant to MMR or kindergartners.

This is a genuine sourcing gap (measles is tracked by CDC's NNDSS; kindergarten coverage by CDC's SchoolVaxView program — neither pipeline is ingested here), not a defect in an existing table.

### Attempting the primary source
Direct fetches of both the article's cited CDC page (`cdc.gov/measles/data-research`) and CDC's SchoolVaxView data page both returned **HTTP 403** in this session, so even the primary source could not be read directly.

### Independent verification (required secondary check)
Since no warehouse computation was possible, four separate CDC-sourced outlets were fetched **in full** (not just search snippets) and cross-checked:

- **USAFacts** (updated Sep 25, 2026, sourced directly to CDC): "As of September 24, 2026, 3,659 cases have been confirmed in 2026" — exact match.
- **CIDRAP** (Sep 24, 2026, reporting the prior week's CDC update): 3,471 cases as of Sep 17 — one week earlier in the same fast-rising trajectory (150-190 new cases/week at the time), consistent with reaching 3,659 by Sep 24.
- **Cronista** (Sep 25, 2026, explicitly sourced to CDC's measles data page): states verbatim that MMR coverage "fell from 95.2% in the 2019-2020 cycle to 92.4% in 2025-2026 among kindergarten children" — exact match to both halves of the claim.
- **KFF** (issue brief, Aug 21, 2026, sourced to CDC's SchoolVaxView release): 92.4% MMR/polio coverage for 2025-26, down from "95% across all three vaccines" pre-pandemic (2019-20) — same trend, consistent rounding of the more precise 95.2% figure the other sources give.

All four sources agree with each other and with the claim; no discrepancy was found in any of them.

### Bottom line
- **From this corpus alone**: not checkable — no measles surveillance table, no kindergarten vaccination coverage table exist in AskAmerica/govdata.
- **From independently verified CDC-sourced reporting**: both figures in the claim (3,659 cases as of Sep 24, 2026; 95.2% → 92.4% MMR kindergarten coverage) check out exactly.

### Published report
Local (session-only) link: http://127.0.0.1:63132/a/a716f5c99b8d8fe459063842ef95db7e.html
Saved copy: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q6/askamerica/2026-09-26/report.html`

### Key sources
- [CDC Measles Cases and Outbreaks](https://www.cdc.gov/measles/data-research/index.html) (cited source; 403 on direct fetch)
- [CDC SchoolVaxView](https://www.cdc.gov/schoolvaxview/data/index.html) (403 on direct fetch)
- [USAFacts: How many measles cases are there in the US?](https://usafacts.org/answers/how-many-measles-cases-are-there-in-the-us/country/united-states/)
- [CIDRAP: CDC adds 177 cases to US measles total](https://www.cidrap.umn.edu/measles/cdc-adds-177-cases-us-measles-total-has-yet-confirm-deaths-pennsylvania)
- [KFF: Kindergarten Routine Vaccination Rates Continue to Decline](https://www.kff.org/medicaid/kindergarten-routine-vaccination-rates-continue-to-decline/)
- [Cronista: CDC confirms first measles death of 2026](https://www.cronista.com/en/today/cdc-confirms-first-measles-death-of-2026-in-the-united-states-3471-cases-recorded-through-september/)
