# Fact-check: NAHB's Sept 25, 2026 immigration-enforcement claim

## Overall grade (Washington Post Pinocchio scale, SPLIT shape — claim is NAHB's own voice citing its own/affiliate research)
- **Fidelity: 1 Pinocchio** — NAHB accurately represents the sources it cites.
- **Claims accuracy: 2 Pinocchios** — significant one-sided framing/omission, no invented numbers.

Published report (local link, dies with session; durable artifact is the saved HTML): saved to `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q6/askamerica/2026-09-29/report.html`

## Summary
NAHB's Sept. 25, 2026 post is largely accurate in its own arithmetic but built almost entirely on the trade association's own research and its most favorable-to-its-argument figures, presented without that framing. The BLS JOLTS construction job-openings figure (~300,000) checks out exactly against AskAmerica's own live warehouse query (326,000 in July 2026). The HBI/University of Denver study figures (1.98 months, 19,000 homes, $10.806B rounded to "$10 billion") are transcribed accurately from the primary press release. The "more than one-quarter" (26.3%) immigrant-workforce-share figure is corroborated by independent trade-press coverage of NAHB's own Census ACS analysis, though it could not be independently recomputed in this warehouse (no industry-by-nativity crosstab is loaded here). The figure that most needs context and doesn't get it is the "housing shortage of about 1.2 million units" — NAHB's own estimate, using a narrower, self-described "lower-bound" methodology, sitting at the low end of a range that runs to 3.7–4.5 million from Freddie Mac, Zillow, and Up For Growth.

## Claim-by-claim findings

**1. "Housing shortage of about 1.2 million units" — Partially true.**
This is NAHB's own Feb 2026 estimate, explicitly self-described by NAHB as a narrow "lower-bound" methodology (vacant units needed to restore 2024 metro vacancy rates to equilibrium, excluding pent-up household formation and obsolete-stock replacement). Independent estimates run far higher: Freddie Mac ~3.7M (Q3 2024), Up For Growth ~3.78M, Fannie Mae ~4.4M, Zillow ~4.5M — only Zonda (~1.0M) is lower than NAHB's. Presenting 1.2M as "the" shortage, without disclosing it's near the bottom of a 1–4.5M range and is NAHB's own number, is materially misleading framing.

**2. "Government data... short about 300,000 workers" — Mostly true.**
Independently re-verified live against `econ.jolts_industry` (BLS JOLTS, series JTS230000...JOL, industry_code 230000 = Construction): 326,000 construction job openings in July 2026 (298,000 June, 291,000 May) — matches NAHB's rounded figure almost exactly, and confirms the question's own JOLTS hypothesis. Caveat: JOLTS "openings" is a proxy for shortage (unfilled recruiting positions), not a literal unmet-demand count — NAHB's phrasing elides that distinction.

**3. "More than one-quarter" (26.3%) immigrant construction workforce, 2024 record — Not checkable in this warehouse (confirmed data gap), but externally corroborated.**
`census.acs_industry` has no nativity breakdown and `census.acs_nativity` has no industry breakdown — this corpus cannot recompute NAHB's own ACS-PUMS analysis directly. Multiple independent trade-press outlets (Scotsman Guide, ConstructionOwners.com, SBCA) report the identical 26.3% figure from NAHB's own published analysis (authored by Natalia Siniavskaia), with trade-level breakdowns (57% drywall/ceiling installers, 56% plasterers/stucco masons, 53% roofers, 43% construction laborers, 35% carpenters). Warehouse-confirmed population-wide foreign-born share (13.9% nationally, 46.1M/332.4M, 2023 ACS) is consistent directional context for a much higher concentration within construction specifically.

**4. HBI/University of Denver study (1.98 months, 19,000 homes, $10.8B) — True, verified against primary source.**
Fetched NAHB's June 10, 2025 press release directly: $10.806B aggregate annual economic impact ($2.663B direct carrying-cost impact + $8.143B from ~19,000 single-family homes not built in 2024); 1.98-month unweighted average construction-time increase. NAHB's Sept 2026 post transcribes these precisely and rounds $10.806B to "$10 billion" defensibly.

## The material caveat (flagged explicitly, not buried)
Three of the four load-bearing figures (housing shortage, immigrant workforce share, HBI economic-impact study) trace directly to NAHB or its affiliate HBI's own commissioned/authored research. Only the JOLTS figure is genuine independent third-party government data. None of the four numbers were found to be fabricated or contradicted — NAHB is faithful to its own sources — but calling this collectively "government data" and its own analysis obscures how much of the post's evidentiary weight rests on advocacy-funded research supporting NAHB's own policy position (looser immigration enforcement for its members' workforce). This is advocacy research accurately reported, not neutral government data as the post's framing implies for the whole picture.

## Sources
- [NAHB blog: NAHB Responds as Immigration Enforcement Keeps Legal Workers Off Job Sites](https://www.nahb.org/blog/2026/09/immigration-enforcement) (primary source under test)
- [NAHB press release: New Study Reveals Significant Economic Impact of Housing Industry Labor Shortage](https://www.nahb.org/news-and-economics/press-releases/2025/06/new-study-reveals-significant-economic-impact-of-housing-industry-labor-shortage) (HBI/Denver study, primary)
- [Eye on Housing: The Size of the Housing Shortage: 2024 Data](https://eyeonhousing.org/2026/02/the-size-of-the-housing-shortage-2024-data/)
- [ResiClub: Freddie Mac housing shortage delayed formation of 1 million households](https://www.resiclubanalytics.com/p/freddie-mac-housing-shortage-has-delayed-the-formation-of-1-million-households)
- [Scotsman Guide: Immigrant workers reach record share of U.S. construction workforce](https://www.scotsmanguide.com/news/immigrant-workers-reach-record-share-of-us-construction-workforce/)
- [NAHB: The States and Construction Trades Most Reliant on Immigrant Workers](https://www.nahb.org/blog/2026/04/Which-States-and-Construction-Trades-Depend-the-Most-on-Immigrant-Workers)
- BLS JOLTS via AskAmerica warehouse: `econ.jolts_industry` (live query, industry_code='230000', metric_type IN ('JOL','JOR'))
- Census ACS via AskAmerica warehouse: `census.acs_nativity`, `census.acs_industry` (live queries, 2023 vintage)

## AskAmerica queries run (for reproducibility)
```sql
-- BLS JOLTS construction job openings
SELECT "year", "date", metric_type, value FROM econ.jolts_industry
WHERE industry_code='230000' AND metric_type IN ('JOL','JOR') AND "year">=2025
ORDER BY "year" DESC, "date" DESC FETCH FIRST 30 ROWS ONLY;

-- National foreign-born population share, 2023
SELECT SUM(total_population) AS total_pop, SUM(foreign_born) AS total_foreign_born
FROM census.acs_nativity WHERE "year"='2023' AND geography='state';

-- National construction employment (all workers), 2023
SELECT SUM(construction) AS total_construction_employed
FROM census.acs_industry WHERE "year"='2023' AND geography='state';
```
