# Judge: MIT Technology Review, Southwest hydropower at decade low (news q3, 2026-11-17 story)

Rank: [askamerica, expert, everyman]

Story: https://www.technologyreview.com/2026/11/17/1148742/hydropower-drought-colorado-river/. Claimant: the reporter's reading of EIA and IEA data.

Verdicts: askamerica 0 Pinocchios (engine-scored; its own note says it would give 1), expert 0, everyman 1.

## Ranking

**1. askamerica.** Graded all nine claims. Confirmed the EIA figures (28% below the 10-year average, 19% below 2025, Powell 3,518 ft, Mead 9% above 950 ft, 2026-27 outlook) and added a warehouse cross-check: Arizona hydro generation in `energy.eia_electricity_generation` fell 19% from 2025 to its lowest since 2010. On the one claim it could not confirm, the two plants' "nearly seven terawatt-hours" in 2025, it went further than either other arm: reporting on EIA data puts Glen Canyon plus Hoover near 5.5 TWh, while about 6.7 TWh is the whole EIA Southwest region, so the figure looks like the regional total attached to two plants. It also flagged that "Southwest" is EIA's supply region and that hydro is about 4% of that region's generation, which the piece omits. Checked the 85% snowmelt share (other estimates 70-80%).

**2. expert.** Same core confirmations with exact arithmetic (Powell 0.8% above minimum, Mead 9.3%), the IEA 40% share, and the EIA STEO 6.13 billion kWh. Reached the same doubt about the 7 TWh figure (the implied regional total leaves little room) but graded it not checkable without the plant-level evidence. Left the snowmelt share unchecked.

**3. everyman.** Confirmed the headline EIA figures and the lake arithmetic but left four claims, including the 7 TWh figure and the snowmelt share, unchecked.

What would change the order: little; the top two agree on verdict and facts. askamerica's plant-versus-region reasoning and region-definition context are the difference.

## Severity invariant

askamerica ranks first. No defect flag.

## Claim-shape check

All claims are the reporter's own unattributed assertions, `author_claims`; compliant. Rating 0 in report.html and agent.md. Note the engine's 0 sits beside one unsupported central claim; unsupported claims do not move the engine rating (known behavior, not changed today).

## Gaps and resolution

askamerica found no plant-level generation table. Filed govdata-ops **#1408**, `energy: add EIA-923 plant-level monthly net generation` (type:sourcing, kind:gap, schema:energy, status:open). askamerica ranked first, so this is a gap filing, not a loss resolution.

## Recipe check

Nothing from the unaided arms that askamerica did not already do.
