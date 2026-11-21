# Judge: CNBC, the property tax revolt and homeowners' bills (news q6, 2026-11-21)

Rank: [askamerica, expert, everyman]

Story: https://www.cnbc.com/2026/11/16/property-tax-relief-homeowners-bills.html (2026-11-16). Claimants: NACo (Matthew Chase), Tax Foundation (Jared Walczak), NAR, Census ACS, Lincoln Institute.

Verdicts: askamerica 0 Pinocchios (engine-scored), expert about 0.5 ("0 to 1"), everyman 1.

## Ranking

**1. askamerica.** Checked every claim. Ballot outcomes (Florida 62% against a 60% threshold; NC passed; Oklahoma and Wyoming rejected; Ohio missed the 2026 ballot), the NACo count, the Tax Foundation Florida cost estimates, Lincoln's 28%, and the county bill range all matched. It caught the one numerical slip: the 2020 "$300,000" is closer to NAR's all-types median ($296,500) than the single-family series used for 2025 ($419,300), so the rise is slightly overstated. Found the rock-salt example covers one town via an Erie County contract, not several counties. Added the useful omission from NACo's own report: inflation-adjusted bills rose only 9% from 2010 to 2024. Walczak's line correctly graded as a policy judgment, not checkable.

**2. expert.** Same core confirmations with source detail (Lincoln 28.4%), and good context (Florida estimates are static and assume no millage response; NC's amendment sets no cap itself). Left the 2020 price baseline, the bill range and the rock-salt claim unchecked, and found the Census $3,211 only on secondary sites.

**3. everyman.** Confirmed the ballot results and NACo count, but suggested the Florida cost figures were misattributed to the Tax Foundation; expert located them on the Tax Foundation page, so that flag is wrong. Left most data claims unchecked.

What would change the order: little. The top two agree; askamerica checked three more claims and caught the price-baseline mismatch.

## Severity invariant

askamerica ranks first. No defect flag.

## Claim-shape check

Compliant: piece-level and cited-organization figures as `author_claims`/`fidelity`; Jared Walczak as `subject_claims` with speaker. agent.md rating 0; report.html shows the rating without the literal "Pinocchio" text the check greps for, no contradiction.

## Gaps and resolution

askamerica stated the warehouse has no ACS B25103 (median real estate taxes). Filed govdata-ops **#1411**, `census: add ACS B25103/B25102 real estate taxes paid by homeowners` (type:sourcing, kind:gap, schema:census, status:open). askamerica ranked first, so this is a gap filing, not a loss resolution.

## Recipe check

Nothing the unaided arms did that askamerica did not.
