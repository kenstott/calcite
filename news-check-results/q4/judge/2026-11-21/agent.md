# Judge: Fortune, housing squeeze reaches middle-class renters (news q4, 2026-11-21)

Rank: [expert, askamerica, everyman]

Story: https://fortune.com/2026/11/16/housing-squeeze-middle-class-renters-cost-burdened-harvard-bofa/ (2026-11-16). Claimants: Harvard Joint Center for Housing Studies, Bank of America Institute.

Verdicts: expert 0 Pinocchios, askamerica 0 (engine-scored), everyman 1.

## Ranking

**1. expert.** Reached the JCHS rental housing report itself and verified the four income-band figures the story is built on (49% of $45,000-$74,999 renters, up 9.5 points; 83% under $30,000; 14% at $75,000+, up 4.1 points; about $120,000 to afford the median home), plus the BofA one-in-four figures. Separated primary from secondary support claim by claim and gave the framing caveats that matter: BofA uses customer income thirds, not JCHS dollar bands, and its burden line is 50%, not 30%; BofA shows renter burden easing slightly in 2026.

**2. askamerica.** Added the one government-data check: ACS B25070 from `census.acs_congressional_district`, summed over districts, shows all-renter cost burden rising from 45.9% (2019) to 47.4% (2024) and severe burden 22.9% to 24.0%, which supports the trend. Confirmed the BofA figures, the Brooke Amendment history and McMillan's rent, and found that the quoted BofA line is not in the BofA PDF or HTML. But the JCHS blog returned 403 and it found no mirror, so the four income-band figures, the core of the story, are graded unsupported; the warehouse has no income breakdown to substitute. Same framing point about BofA lacking a middle-income figure.

**3. everyman.** Confirmed the 49% figure and the Brooke history but could not reach most sources; its framing point (middle-income 49% equals the all-renter rate) is useful but it checked far fewer claims.

What would change the order: had askamerica reached the JCHS report (expert shows it is reachable), its warehouse trend would put it level with or above expert.

## Severity invariant

askamerica ranks above everyman. No defect flag.

## Claim-shape check

First delivery graded the quoted Bank of America Institute line as an unattributed `author_claims` entry. Resumed the same subagent; it republished with that line under `subject_claims`, speaker "Bank of America Institute". Verified on disk in claims.json after the fix; rating unchanged at 0 in report.html and agent.md.

## Resolution (one)

Govdata-ops **#1409**, `census: add ACS B25074 renter cost burden by household income (national/state/county)` (type:sourcing, kind:gap, schema:census, status:open). With income-band burden in the warehouse, askamerica could have reproduced the JCHS figures from ACS directly instead of depending on a blocked page.

## Recipe check

expert's habit of finding the underlying report when a blog page 403s is general web practice; no recipe.
