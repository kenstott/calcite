# Verdict: Not checkable here — genuine corpus gap (no homelessness/shelter data exists in this warehouse)

## Claim under test
Coalition for the Homeless, **"State of the Homeless 2026: A Crisis Inherited, A Choice Ahead"** (published June 2026, coalitionforthehomeless.org). Two headline figures, read directly from the report's own PDF (not a secondary summary):

1. **"In 2025, 194,531 unique individuals utilized the NYC DHS shelter system over the course of the year — the most in the history of the shelter system."**
2. **"The number of longer-term New Yorkers sleeping each night in NYC DHS shelters increased by 27 percent — or by 12,443 people — during the four years of the Adams administration"** (Jan 2022–Dec 2025).

Both are sourced to NYC Department of Homeless Services' own 40-year shelter-census administrative records — not HUD's national Point-in-Time (PIT) count, a different methodology (single January night vs. an annual/nightly DHS census).

Note: the report's webpage intro states "12,442 people" for finding #2, while the PDF itself (Executive Summary, Section II, Figure 2.2 caption) consistently says "12,443" — a minor internal inconsistency in the Coalition's own materials, not a data disagreement.

## Corpus investigation
- `search_catalog("homeless shelter census New York City")` — zero relevant matches; unmatched_terms flagged "homeless", "shelter", "york".
- `search_catalog("point in time count HUD sheltered unsheltered persons")` — zero relevant matches; unmatched_terms flagged "sheltered", "unsheltered".
- `search_catalog("homelessness")` — 0 matches.
- `list_tables(schema="housing")` — full review of all 20 tables/views (building_permits, HMDA loan tables, FHFA HPI, HUD Fair Market Rents, HUD Income Limits, HUD "A Picture of Subsidized Households"). The closest candidate, `hud_subsidized_county`/`hud_subsidized_housing`, measures **occupied, subsidized housing units** (Section 8, public housing, vouchers) — a different population from people in homeless shelters or unsheltered, and not usable even as a proxy.

**Conclusion: no table anywhere in this corpus (housing schema or any other) carries NYC DHS shelter-census data, HUD PIT counts, or homelessness data of any kind.** This is a confirmed, permanent gap, not a search-term problem.

## Corpus-gap issue filed
Checked for duplicates first (`gh issue list -R kenstott/govdata-ops --search "homeless"` — only an unrelated closed VA-facility issue found), then filed:
**[kenstott/govdata-ops#755](https://github.com/kenstott/govdata-ops/issues/755)** — `type:sourcing`, `kind:gap`, `schema:housing`, `status:open`. Names HUD AHAR/PIT data and NYC Open Data's DHS Daily Shelter Census as candidate ingest sources.

## Independent corroboration (external, since the corpus had nothing)
Fetched and text-searched directly (not search-result snippets): HUD's own **2024 Annual Homeless Assessment Report (AHAR) Part 1** PDF (huduser.gov). It states: *"Between 2023 and 2024, New York saw a 53 percent increase [in sheltered PIT homelessness]... New York City... accounted for almost 88 percent of the increase in sheltered homelessness in New York City."* This corroborates the **direction** of the Coalition's claim (a large, NYC-driven surge in sheltered homelessness) from an independent federal source, but measures a different window (Jan 2023→Jan 2024 PIT snapshot vs. the Coalition's Jan 2022–Dec 2025 nightly-census trend) and cannot verify the specific 194,531 or 27%/12,443 figures.

Also checked (fetched directly): the Coalition's report webpage, the full report PDF, and NYC Open Data's DHS Daily Report dataset page (confirmed to exist as the live source series, though its raw table wasn't extractable via this session's fetch tool).

## Verdict shape
**SPLIT Pinocchios** (mandatory since claims are attributed to a named third party):
- **Fidelity: 0** (essentially clean — figures quoted consistently across the report's own Executive Summary, body, and figure captions; the lone flaw is the report's own website-vs-PDF 12,442/12,443 inconsistency).
- **Claims accuracy: 0** (no identified issue, but explicitly caveated: this reflects an inability to independently verify the precise figures against government data, not a confirmed exact match — corroborated only in direction via HUD's AHAR).

Both individual claims scored via `score_claim` as **"not checkable here"** with verdict_confidence 1.0.

## Report published
Local report link (dies when this session ends) and saved file:
`/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q5/askamerica/2026-09-28/report.html`

7 sections, 9 citations, SPLIT Pinocchios rating, full claims table with SQL/tool provenance for each cited figure.
