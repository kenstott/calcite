# Verdict: AIC's "2,450% surge" claim — not checkable in this corpus, but independently mostly true

## Bottom line
The AskAmerica data corpus **has no table covering ICE detention, ICE arrests, or any DHS immigration-enforcement statistic** — confirmed via three separate `search_catalog` queries and a full `list_tables(schema="crime")` listing, all returning zero matches on "detention," "ICE," "immigration," "deportation," or "removals." The `crime` schema is FBI/BJS-only (UCR, NIBRS, NCVS, LEOKA); no other schema in the 420+ table catalog covers this topic either. **This is a genuine sourcing gap**, filed as [kenstott/govdata-ops#754](https://github.com/kenstott/govdata-ops/issues/754) (type:sourcing, kind:gap, schema:crime, status:open — checked for duplicates first, none found).

Because the corpus could not answer this, I fetched and parsed the primary sources directly instead of relying on secondary summaries:

**What AIC's report (Jan 14, 2026 PDF, page 10) actually says, verbatim:**
> "From January through the end of November, the percent of people held in detention after an ICE arrest without a criminal record rose from six percent to 41 percent… In total, the number of people with no criminal record arrested by ICE and sent to detention increased by 2,450 percent from January through the end of November."

Key scope details the bare headline omits: this is specifically **interior ICE arrests** (not CBP/border arrests), over **January–November 29, 2025**, sourced to TRAC's "ICE Detainees" tracker and Deportation Data Project FOIA microdata — not the entire ICE detention system, and not the December 2025–January 2026 period.

**Independent corroboration (different populations/windows, so none reproduces 2,450% exactly, but all confirm the same real, large trend):**
- TRAC's own Nov 24, 2025 report: total detained population with no criminal conviction rose 42,755 → 47,964 (Sept 21 → Nov 16, 2025); 97% of the net population increase in that window was people with no criminal conviction.
- FactCheck.org's Jan 2026 analysis (ICE public stats + Deportation Data Project): no-conviction/no-pending-charge detainees rose 3,165 (Feb 2025) → 25,193 (Jan 2026), ~696% — a broader population/different window than AIC's figure.
- TRAC's live Quick Facts (checked 2026-09-28): 70.6% of current ICE detainees have no criminal conviction, consistent with the trend continuing.

I could not independently recompute the exact 2,450% figure — TRAC's underlying daily interior-arrest-by-criminal-history table is JavaScript-rendered and returned no data via direct fetch.

## Verdict
- **Corpus check: not checkable** (no matching table — genuine, confirmed gap, issue filed).
- **Independent validation: mostly true.** Fidelity to AIC's own report is high (0 Pinocchios) — the claim is accurately quoted, not distorted. Claims-accuracy carries 1 Pinocchio: the number is real and directionally/order-of-magnitude corroborated by two independent primary sources, but it describes a narrow subgroup (interior arrests only) growing off a very small January 2025 base (~6% share), which is exactly the kind of base-rate effect that inflates a real trend into a very large percentage — context lost when the bare number circulates without qualification.

## Deliverables
- Published report (dashboard + full narrative + claim table + SPLIT Pinocchio rating): `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/askamerica/2026-09-28/report.html`
- GitHub sourcing-gap issue filed: https://github.com/kenstott/govdata-ops/issues/754
- Primary sources fetched and parsed directly: AIC report PDF (americanimmigrationcouncil.org/wp-content/uploads/2026/01/immigration-detention-report.pdf), AIC blog (americanimmigrationcouncil.org/blog/ice-expanding-detention-system/), TRAC "Taking Stock" report (tracreports.org/reports/767/), TRAC Quick Facts (tracreports.org/immigration/quickfacts/), FactCheck.org (factcheck.org/2026/01/as-ice-arrests-increased-a-higher-portion-had-no-u-s-criminal-record/)
