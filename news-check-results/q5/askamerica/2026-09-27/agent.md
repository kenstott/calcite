# Fact-check: Hochul "Safer Streets" crime release (Sept 3, 2026)

**Article:** https://www.governor.ny.gov/news/safer-streets-governor-hochul-announces-continued-declines-crime-across-new-york
**Report saved to:** /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q5/askamerica/2026-09-27/report.html

## Verdict: 1 Pinocchio

Every headline number that could be checked against a primary source is accurate. NYC's "safest summer on record" claim (195 shooting incidents, 249 shooting victims, 66 murders, June-Aug 2026) and the NYPD August major-crime figures (10,276 vs. 11,000, -6.6%; Bronx -11.9% YTD) match NYPD's own official press release (nyc.gov PR014, Sept 3, 2026) verbatim, digit for digit. As an independent cross-check not relying on either NYPD or the Governor's office, AskAmerica's `crime.cde_reta` (FBI Return A federal data) confirmed that 78 murders (2017) genuinely was the prior record the release claims to have beaten for summer NYPD murder counts.

The one material issue: the release states its GIVE-program statewide aggregate (7% index-crime decline, 48% murder decline) is a January-May 2025 vs. 2026 one-year comparison, then in the very next sentence lists 11 police departments — including Rochester and Utica — as having "double-digit percentage decreases in index crime," with no time window stated, implying the same one-year basis. Independent FBI agency data shows Rochester's and Utica's actual one-year Jan-May decline was single-digit (about 8%), and a local outlet (Rome Sentinel) citing the same official DCJS data sheet confirms Utica's real double-digit figure (37%) is a **nine-year** comparison (2017-2026), not one-year. The release never discloses this switch in comparison window — a real, if narrow, case of making a smaller one-year improvement look larger by borrowing the register of the surrounding one-year statistics.

## Key findings

| Claim | Verdict | Source |
|---|---|---|
| NYC safest summer for shootings/murders (195/249/66) | **True** | NYPD PR014, cross-checked vs. FBI Return A |
| NYC Aug. major crime -6.6%; Bronx YTD -11.9% | **True** | NYPD PR014 |
| GIVE statewide 7% index-crime / 48% murder decline (Jan-May 2026) | Not independently recomputable here; treated as accurately relayed | State DCJS PDF (403 on fetch) |
| 11 GIVE departments "double-digit" decrease, same paragraph as 1-yr aggregate | **Misleading** — at least one cited department's (Utica's) true double-digit figure is a 9-year comparison; Rochester's and Utica's 1-yr FBI-derived changes were single-digit (-8.5%, -8.0%) | FBI Return A + Rome Sentinel |
| Quotes (Hochul, Commissioner Rosado) | Consistent across independent copies; content not disputable | Multiple outlets |
| "Nearly $4 billion in public safety" | Self-reported cumulative total; a companion itemized release only sums to ~$2.4B of named categories, remainder unbroken out | Governor's own Aug. 14 release |

## Methodology notes / limitations
- Primary sources used: NYPD's own PR014 press release (fetched directly), the Rome Sentinel (which cites the same official DCJS GIVE PDF the article links to), and AskAmerica's `crime.cde_reta` (FBI Return A monthly agency-level offense data) as an independent statistical check.
- The state's own linked source, the DCJS "GIVE_Agencies_Index_Crime_Data" PDF, returned HTTP 403 on direct fetch and could not be opened in full in this session.
- FBI federal data for NYPD currently runs only through July 2026, so NYC's specific June-August 2026 figures could be checked only against NYPD's own release, not a second independent federal count.
- "Index crime" (NY DCJS) and "actual_all_crimes" (FBI Return A) are related but not guaranteed identically scoped; the discrepancy found is presented primarily as a comparison-window issue (directly confirmed for Utica), not ruled out as partly definitional.

The full report (with dashboard, per-claim SQL, and sourcing) is at the link above and saved to the path noted.
