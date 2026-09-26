## Verdict: Federal research-funding-cuts claim (AAU backgrounder) — largely accurate, mostly outside this corpus's reach

**Full report:** `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-09-26/report.html`

### Bottom line
The AAU article's specific, attributable facts check out cleanly against independent reporting. Its aggregate NIH/NSF grant-count figures are directionally correct and close to primary-source numbers. The one claim resting purely on AAU's own unattributed assertion (institution-level 10-25%/32% declines) cannot be verified anywhere. Critically, **this data corpus has a structural coverage gap on exactly the period the article discusses (2025-2026)** — this is a sourcing-gap finding, not a contradiction finding.

### Tables checked and what they showed
- **`research.nsf_herd_by_institution`** (NCSES HERD survey, institution grain) — the only table fine enough to check a single university's federal R&D. Declared/observed coverage: FY2010-2024 only (2-year publication lag). Query:
  `SELECT year, rd_expenditure_usd_thousand FROM research.nsf_herd_by_institution WHERE institution ILIKE '%Johns Hopkins%' AND funding_source='Federal' AND federal_agency='Total' AND rd_field='All' ORDER BY year`
  Result: JHU federal R&D rose every year through FY2024 ($2.97B → $3.32B → $3.62B, FY2022-24), the highest on record back to FY2010 ($1.74B). **No row exists for FY2025/26**, so the claimed >$500M CY2025 decline and 32% figure fall entirely outside this table's window and could not be computed from the corpus. A supplementary diff-in-differences check (JHU vs. 6 peer universities, FY2015-19 vs FY2020-24) found JHU's federal R&D grew an additional $663.6M relative to peers over that window (p<0.001, n=40) — confirms the pre-2025 growth trajectory but says nothing about 2025.
- **`research.nih_award_projects`** (NIH RePORTER award microdata — the one table built to answer "is NIH grant count shifting year to year") — declared FY2022-2026, but a `data_coverage` scan found **only FY2026 actually loaded**; FY2022-2025 are missing. Genuine sourcing gap, flagged separately.
- **`research.nsf_federal_rd_obligations`** / **`research.nsf_rd_by_field`** — dollar totals only, not award counts; obligations table covers a single fiscal year (2024). **No NSF award-count-by-year table exists anywhere in this corpus.**
- No table anywhere is AAU-membership-scoped or carries a cross-institution FY2024-vs-later funding-change metric.

### Per-claim verdicts (via independent primary sources, since the corpus couldn't reach these years)
1. **JHU layoffs (110 employees, June 2026) after >$500M federal-portfolio decline** — **True.** Confirmed by 5 independent outlets (Daily Record, WMAR2, Baltimore Banner, FOX 5 NY, WYPR) plus JHU's own Feb 2026 statement citing the exact >$500M figure, 43% less federal funding, 28% fewer awards.
2. **NIH 25% fewer new grants in 2025 vs 2024, behind pace for 2026** — **Mostly true.** NIH's own official report (grants.nih.gov, "FY2025 By the Numbers," March 2026) shows new/competing RPG awards down 20.5% (10,265→8,161) and R01-equivalent awards down 21.8% (7,000→5,471) — a few points below the claimed 25% but same order of magnitude. "Behind pace for 2026" corroborated by Science/AAAS.
3. **NSF new grants down 46% from Biden-era average, lowest since the 1980s** — **Not checkable in this corpus** (no NSF award-count table exists); independently corroborated by Nature ("NSF set to issue lowest number of new grants in four decades") and Science/AAAS.
4. **Some AAU institutions reported 10-25% declines, one 32%** — **Not checkable here.** Verified via Wayback Machine archive that this sentence is verbatim AAU's own language (not a misquote), but AAU names no institutions or source, and no corpus table or independent source could verify the specific figures.

### Fact-check rating
1 Pinocchio — minor shading (the NIH 25% figure runs a few points above NIH's own most precise metric), no invented or contradicted facts.

The report page includes a 4-panel dashboard (JHU federal R&D trend, NIH award-count trend, and two stat tiles for the claims not checkable in this corpus).
