## Fact-check: Sen. Jacky Rosen's EAD-rule impact claims

**Verdict: TRUE. Fidelity to sources: 0 Pinocchios. Claims accuracy: 0 Pinocchios.**

### Bottom line
Rosen's numbers hold up, and if anything are conservative. Her April 29, 2026 Senate floor remarks state, verbatim: "More than three and a half million legally authorized workers nationwide could be impacted," "roughly six hundred thousand" in construction, and "more than half a million" in hospitality. FWD.us's primary analysis (published April 9, 2026, using 2024 ACS data matched to FY2025 EAD-approval administrative data) puts the real figures at approximately 3.8 million total (3,765,000 in its published state-by-state table), 613,000 in construction, and 514,000 in leisure/hospitality — consistent with, and for the topline figure modestly higher than, what Rosen said. The rule remains in effect as of today (Sept 29, 2026).

### AskAmerica warehouse check (per the assignment)
- **(a) Industry employment sanity check — available and checked.** `econ.national_wages` (BLS QCEW, national) gives 2025 U.S. employment of ~8,435,067 in construction (NAICS 23, all ownerships) and ~17,252,863 in combined leisure/hospitality (NAICS 71+72, all ownerships). The claimed ~600K construction and ~510–514K hospitality figures are therefore a plausible 7.1% and 3.0% share of each industry — not implausibly large fractions, consistent with these sectors' well-documented reliance on immigrant/pending-status labor.
- **(b) Direct EAD/work-permit-holder counts by industry — confirmed absent from the corpus.** `search_catalog` returned no matching table/column for immigration, asylum, TPS, or work-authorization terms (all came back as explicitly unmatched). This is a genuine coverage gap, not a data-quality issue. Verified instead against FWD.us's own primary report (fetched directly) and the Federal Register rule text.

### Primary-source verification
- **Rosen's remarks** (fetched directly from rosen.senate.gov, April 29, 2026 transcript): "More than three and a half million legally authorized workers nationwide could be impacted... In construction alone, roughly six hundred thousand workers could be forced off the job... In hospitality... more than half a million workers nationwide could be affected." Every figure in the user's claim matches this transcript exactly.
- **FWD.us analysis** (fetched directly from fwd.us, published April 9, 2026): headline "as many as 3.8 million EAD workers"; detailed table TOTAL = 3,765,000, Construction = 613,000, Leisure and Hospitality = 514,000. Category breakdown confirmed verbatim: ~200,000 refugees/asylees/green-card applicants, ~2.3 million asylum applicants, ~100,000 spouses of H-1B/nonimmigrant-visa holders, ~1.1 million TPS holders/applicants. Every figure the user cited matches exactly.
- **Rule status**: DHS/USCIS Interim Final Rule published in the Federal Register Oct. 30, 2025 (Docket USCIS-2025-0271), ending automatic EAD extensions. Rosen's CRA resolution to overturn it failed in the Senate on April 29, 2026 (bipartisan support, but opposed by most Republicans). A federal lawsuit (H-4 visa holders v. DHS, C.D. Cal., filed Jan. 8, 2026) has produced only a narrow protective order for named plaintiffs as of a June 24, 2026 hearing — no nationwide injunction. The rule remains in force today.

### Did Rosen understate the figures?
Only for the topline number. "More than 3.5 million" is technically true of a 3.8 million actual figure but understates it by ~300,000 (~8%). For construction and hospitality, her hedged language ("roughly," "more than") accurately reflects FWD.us's 613,000 and 514,000 — not an understatement there. The user's framing that "Rosen understated the figure" is accurate for the topline number but overstated as a characterization of the industry-specific figures, which she stated about as precisely as the estimate supports.

### Grading
- **Fidelity (attribution accuracy): 0 Pinocchios.** Every number attributed to Rosen matches her transcript verbatim; every number attributed to FWD.us matches its report verbatim.
- **Claims accuracy (are the underlying numbers true): 0 Pinocchios.** All figures check out against primary sources and against AskAmerica's own BLS employment data as plausible shares of each industry's workforce. No factual errors found.

### Data-source notes
- AskAmerica's warehouse has no EAD/work-permit-holder table by industry (a real coverage gap, reported as such rather than guessed around).
- Construction/leisure-hospitality total employment used as a plausibility check only, not as the source of the affected-worker counts (those come from FWD.us and Rosen's own remarks, both fetched directly).

### Links
- Local report (dies with this session): http://127.0.0.1:60883/a/f0e34e7c44c3ee805d1ec25b096456b6.html
- Saved copy: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/askamerica/2026-09-29/report.html`

### Sources
- [Rosen Senate floor remarks transcript, April 29, 2026](https://www.rosen.senate.gov/2026/04/29/video-rosen-forces-bipartisan-senate-vote-to-reverse-trump-regulation-ending-automatic-extension-of-work-permits/)
- [FWD.us — Ending Automatic Work Authorization Extensions Will Cost Billions](https://www.fwd.us/news/ending-automatic-work-authorization-will-cost-billions/)
- [Federal Register — Removal of the Automatic Extension of Employment Authorization Documents (Oct 30, 2025)](https://www.federalregister.gov/documents/2025/10/30/2025-19702/removal-of-the-automatic-extension-of-employment-authorization-documents)
- [USCIS — DHS Ends Automatic Extension of Employment Authorization](https://www.uscis.gov/newsroom/news-releases/dhs-ends-automatic-extension-of-employment-authorization)
- AskAmerica: `econ.national_wages` (BLS QCEW national aggregates, 2025), industry_code 23/71/72, agglvl_code='14'
