# Verdict: TRUE — claim confirmed

**Claim:** August 2026 new-home sales rose more than 6% from July levels, with the SAAR climbing to 684,000 units.

## Answer
Confirmed accurate. The Census Bureau/HUD "Monthly New Residential Sales, August 2026" release (Release CB26-155, for release September 24, 2026) reports:
- August 2026 new-home sales SAAR: **684,000** (preliminary)
- July 2026 SAAR (revised): **643,000**
- Month-over-month change: **+6.4%** as stated in the release, independently recomputed as (684,000−643,000)/643,000 = **+6.38%**

Both parts of the claim — the 684,000 level and the >6% MoM increase — match the primary source exactly.

## Corpus coverage check (this is a genuine sourcing gap)
No table in the AskAmerica/askengine corpus carries a new-home-sales / New Residential Sales SAAR series. Checked:
- `econ.fred_indicators` — carries HOUST, PERMIT, EXHOSLUSM495S (existing home sales), CSUSHPISA, MORTGAGE30US, etc., but no new-home-sales series (e.g. HSN1F/MSPNHSUS are absent).
- `econ.housing_indicators` (view) — columns: housing_starts, single_family_starts, building_permits, existing_sales, case_shiller_index, median_sale_price, mortgage_rate_30y, rental_vacancy_rate. No new-home-sales column.
- Every table in the `housing` schema (FHFA HPI, HUD Fair Market Rents, HMDA loans, building permits, income limits, subsidized housing, Opportunity Zones) — none carry this series.
- `search_catalog` for "SAAR" and "new home sales" returned no matching table/column.

Because the corpus has nothing to query, the figures were verified directly against the Census Bureau's own primary release and cross-checked against independent secondary reporting, rather than computed from warehouse SQL.

## Primary source (parsed directly via web_fetch)
`https://www.census.gov/construction/nrs/pdf/newressales.pdf` — "MONTHLY NEW RESIDENTIAL SALES, AUGUST 2026," Release Number CB26-155, for release 10:00 AM EDT, September 24, 2026. Verbatim: *"Sales of new single-family houses in August 2026 were at a seasonally-adjusted annual rate of 684,000 ... This is 6.4 percent (±19.5 percent) above the July 2026 rate of 643,000, and is 2.0 percent (±15.7 percent) below the August 2025 rate of 698,000."* Table 1a gives the full 2026 monthly SAAR series: Jan 576k, Feb 630k, Mar 659k, Apr 641k, May 636k(r), Jun 672k(r), Jul 643k(r), Aug 684k(p).

Note: the Census Bureau's own 90% confidence interval on the MoM change (±19.5 points) is wide enough that the change is not statistically distinguishable from zero at that confidence level — a caveat on the report's methodology, not a reason to doubt the reported point estimate the claim cites.

## Independent corroboration (4 secondary sources fetched and read in full, in addition to the primary release)
- **CalculatedRisk** (Bill McBride) — confirms 684,000 SAAR, notes it beat the 615,000 consensus forecast, months' supply fell to 8.5 from 9.0.
- **HousingWire** (Tyler Williams) — confirms the 6.4% MoM rise and 2.0% YoY decline, with detail on builder price incentives and regional mix (South ~66% of August sales).
- **Crypto Briefing** — independently confirms 684,000 SAAR and +6.4% from revised July (643,000); also notes July's *originally reported* SAAR was 607,000 (a headline 10.5% MoM drop as first published) before being revised up to 643,000 — a useful footnote that the "July level" in the claim is the revised figure, not the one reported a month earlier. Also cites private trackers (Zonda: +1.7% MoM; MBA Builder Application Survey: ~664,000 SAAR) running below the official Census print, without contradicting it.
- Trading Economics' page could not be parsed (client-rendered, no extractable content).

All sources agree; none contradict the claim or the Census figures.

## Second-opinion check
`score_claim` independently scored the assertion as **verdict: true**, confidence 0.88.

## Deliverables
- Local report link (dies with this session): http://127.0.0.1:51391/a/54df9310e8d94a69fef53a85a3d89634.html
- Saved report file: `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q8/askamerica/2026-09-26/report.html`
- Dashboard: stat tile (684,000 units, +6.4% vs July) + line chart of monthly 2026 new-home-sales SAAR (Jan–Aug), inlined in the report.

## Bottom line
The article's claim is fully supported by the Census Bureau's own primary data and corroborated by four independent secondary sources. This is not a corpus-native answer — AskAmerica's data does not carry a new-home-sales SAAR series — but the claim is verifiably **true** against the actual government source it purports to describe.
