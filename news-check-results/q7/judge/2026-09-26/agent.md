# Judge: q7 — "52,253 wildfires burned 8.24M acres through August 2026; April 10 YTD was 231% of the 10-year average"

Blind judge of three fresh, same-day persona runs (news claim, Workstream A of the daily-eval pipeline).

Rank: [expert, everyman, askamerica]

## A real finding: the claim is partly false, and only one of the three personas caught it

**Expert** reached NICC's own dated Incident Management Situation Reports (IMSR) directly — not just today's live cumulative page — and found that while the fire count (52,253), acreage (8,238,284) and April 10 acreage (1,707,778) all match exactly, the "231 percent of the ten-year average" figure does not belong to April 10 at all. Computing directly from NICC's own April 10 report (1,707,778 acres ÷ a published 788,185-acre ten-year average) gives 216.7%, not 231%. Expert then found where 231% (230.9%, rounding to 231%) actually comes from: **March 31, 2026's** ratio (1,615,683 acres ÷ 699,816-acre average) — a different date with a different acreage total. The claim spliced one date's percentage onto another date's acreage figure. Expert validated its own calculation method against three other dates where NICC does print a percentage (Jun 30: 159%, Jul 31: 133%, Aug 31: 166%, all matching to the point), and separately flagged that the "9 deaths" figure traces only to a stale early-July Wikipedia snapshot, not an August-dated primary source — a second, independent inaccuracy in the claim as framed.

**Neither everyman nor askamerica caught this.** Everyman explicitly concluded "the claim is accurate" and treated the 231% figure as verified via a secondary source (DLA Piper) that itself just restates the claim without an independent NICC-sourced check against the correct date. askamerica reported the whole claim as "not checkable from this corpus" (correct, given the genuine corpus gap) but then treated the claim's own figures as independently confirmed via secondary sources without ever attempting to pull NICC's actual historical, date-specific reports the way expert did — so it also missed the splice.

## Reasoning for the ranking

**Expert** is first, by a wide margin: it is the only answer that actually falsified part of the claim with primary-source evidence, rather than accepting the claim's own framing because a secondary source happened to repeat the same numbers.

**Everyman** moves to second this round: despite missing the splice, it did independently notice a real oddity worth crediting — it flagged that a mid-May NIFC-sourced figure (194% of average) sitting between the claimed April (231%) and later-year lower percentages was "directionally consistent... expected, not contradictory" without verifying the specific chain, which is the closest either non-expert answer came to questioning the percentage figure, even though it ultimately didn't pursue it to a real check.

**askamerica** is third this round — **a severity flag, per the daily-eval plan's standing invariant** ("askamerica ranking at or below everyman is always a defect, independent of whether it's a repeat pattern"). Its underlying diagnosis of the corpus gap was correct and useful, but on the substantive fact-check itself it did no better than everyman, and it had the same access to search for NICC's dated historical reports as everyman had — it simply didn't attempt what expert did (going past the live current-totals page to the dated report archive).

## Severity flag

**askamerica ranked third here, at/below everyman — flagged per the standing invariant.** This is not primarily a connector-capability gap (askamerica correctly identified that no warehouse table covers this), but a research-thoroughness gap: it stopped at "the corpus can't answer this, but secondary sources agree with the claim" rather than pursuing NICC's own historical report archive directly, which was available via the same web-fetch tooling askamerica has.

## Recipe / resolution check

No corpus fix applies (the underlying data — NICC/NIFC wildfire statistics — isn't ingested into AskAmerica's warehouse at all, a separate and already-tracked gap distinct from the finding above). The actionable lesson is a research-practice one for the `askamerica-news-check`/`askamerica-daily-eval` prompting: when a claim cites a percentage-of-baseline figure tied to a specific date, dispatched personas should be pointed toward the primary source's dated historical archive, not just its live current-totals page or a secondary restatement — worth adding to the skill's own guidance rather than a McpServer.java or recipes.json change, since this is a research-methodology gap, not a connector defect.
