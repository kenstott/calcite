# Fact-check: "Michigan violent crime hits seven-year low, governor's office says" (WNEM, 2026-10-26)

**Verdict: 1 Pinocchio (mostly accurate; headline statistics hold up against FBI data, but the piece is a relay of a government press release with no independent source, no data citation, and implied credit to policy that the data cannot establish).**

## Source access
- WNEM URL fetched; the returned text appears to be the WILX/News 10 (Brayden Willis) version carried by the same group. Claims below are from that fetch (summarized by the fetch tool, not verbatim).
- Governor's release (michigan.gov) returned HTTP 403. Search snippets and a WXYZ copy confirm the same numbers.
- Independent data: FBI Crime Data Explorer public API (api.usa.gov/crime/fbi/cde/summarized/state/MI/..., DEMO_KEY, the public demo key), annual sums of monthly counts, 2015-2025. Michigan population coverage 97-98% in 2025 (96.4% by Dec 2025).

## Claims and grades
| Claim | Finding | Grade |
|---|---|---|
| Violent crime lowest in 7 years | FBI CDE Michigan violent crime rate per 100k: 2015 421.1; 2016 461.9; 2017 453.0; 2018 455.9; 2019 444.3; 2020 496.8; 2021 500.0; 2022 471.1; 2023 468.1; 2024 442.7; 2025 431.1. Counts 2021 49,124 -> 2025 42,285 (-14%). 2025 is below every year 2016-2024, so "lowest in 7+ years" is true (indeed lowest since 2015). Caveat: data run through Dec 2025, not Oct 2026, so the "current" framing is ~10 months stale; the release does not say which dataset it used. | Supported |
| Murders down 40% since 2021 | FBI homicide counts 759 (2021) -> 442 (2025) = -41.8%; rate 7.7 -> 4.5 per 100k = -41.6%. Robustly supported. | Supported |
| Robberies cut by 50% since Whitmer took office | Robbery counts 2018: 5,637; 2025: 2,692 (-52%); rate 56.8 -> 27.5 (-52%). Supported if 2018 is the baseline (she took office Jan 2019; 2019 baseline 5,344 -> -50%). | Supported |
| Nearly one-third of violent crimes involve a firearm | Not checked (no accessible primary MSP/FBI breakdown). | Not checkable here |
| 1,000+ illegal guns removed (Operation Safe Neighborhoods) | Release/search snippets only; no independent confirmation. | Unverified |
| 11,000+ check-ins with felony offenders | Unverified, government-reported program metric. | Unverified |
| ~$2 billion public safety investment since 2018; 20% trooper raise | Unverified. WXYZ's copy reportedly printed both "$2 billion" and "$2 million" (internal inconsistency in that outlet; WNEM's text read $2 billion). Sub-items ($100M revenue sharing, $25.2M violence intervention) not checked. | Unverified |
| Whitmer on families getting ahead; universal background checks/safe storage pushed | Paraphrase of stated position; it is an attributed opinion, not a fact to test. | N/A |
| Quotes: Ronald Rampe, Tony Gant (Safe and Just Michigan) | Attributed opinions from interviews; cannot be verified independently; Gant's "amazing news"/"don't step on the brakes" are opinion. | Not checkable |

## Materially misleading / missing context
1. **Comparison group.** Credit is implied for Michigan policy, but the same period shows the same drop nationally. FBI CDE US homicide rate 6.9 (2021) -> 4.3 (2025), -38%, vs Michigan -42%. US robbery 87.1 (2018) -> 49.9 (2025), -43%, vs Michigan -52%. Michigan's improvement is slightly larger but most of it is a nationwide decline; the data cannot attribute it to the $2B or any program. The piece never gives national context.
2. **Cherry-picked baselines.** Murder is measured from 2021, near the peak (2020: 763; 2021: 759), and robbery from 2018 (a long pre-existing downtrend: 7,795 in 2015 -> 5,637 in 2018 before she took office; robbery was falling at ~8%/yr before 2019). Violent crime overall is down only ~14% in count since 2021, a much less dramatic figure than the two headline subcategories. Aggravated assault (the bulk of violent crime) is not mentioned.
3. **Data/definition caveats.** The 2021 FBI NIBRS transition and agency coverage changes affect cross-year comparisons (Michigan coverage 97.6% in 2021, ~97.0% in 2025; the US fell to ~75-79% in 2021). Michigan-level series are less affected than national ones but still not a clean seam. The piece names no data source.
4. **Sourcing.** All statistical claims come from the governor's office (an interested party, in a gubernatorial election-year, term-limited context); outside voices are two opinion interviews. No independent expert or dataset is cited.
5. Gun-seizure and check-in numbers are program outputs, not outcomes; they do not show that they reduced crime.

## Confidence
High on the three FBI-checkable figures (single source, FBI CDE API; cross-consistent with 2024 figures reported by other outlets, ~43k violent crimes). Not cross-checked against the MSP dashboard (not located). Medium on the "seven-year" wording because the release's exact dataset is unknown.

## Overall
Core numbers are accurate; framing is uncritical and implies causation with no comparison data. **1 Pinocchio.**
