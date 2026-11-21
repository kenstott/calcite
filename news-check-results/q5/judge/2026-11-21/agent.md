# Judge: Fortune, U.S. Chamber's Neil Bradley on 20 years of 2% growth (news q5, 2026-11-21)

Rank: [askamerica, expert, everyman]

Story: https://fortune.com/2026/11/16/us-chamber-neil-bradley-midterms-growth-easy-button-price-controls/ (2026-11-16). Claimants: Neil Bradley (U.S. Chamber), Raj Chetty et al., CBO, AP VoteCast.

Verdicts: askamerica 0 Pinocchios (engine-scored), expert 0, everyman 1.

## Ranking

**1. askamerica (narrow).** Computed the compound growth rates from FRED GDPCA (3.46% 1950-2005, 2.04% 2005-2025), summed Census BFS monthly applications (5.67M SA, 5.70M NSA; record since 2004; 2024 5.23M, 2021 5.39M) and high-propensity applications (1.84M, 1.71M, 1.67M for 2023-2025), and explained the roughly 1% level differences from Fortune's figures as the November annual revision. Graded Bradley's "5.68 million new business starts" as partially false for the right reason: applications are EIN requests, not businesses started, and high-propensity applications fell. Verified the election-control claim and the 2026 results against NPR/AP rather than memory, and the Section 815 buyback limits. CBO 1.8% rests on search quotes after a 403, stated.

**2. expert.** The same GDP arithmetic, BFS sum (5.70M, a record) and high-propensity level (1.69M), confirmed CBO directly, and caught one wording issue askamerica did not (Sanders-Hawley is a five-year cap, not one-year). But it checked the election-control claim "from my own knowledge of the results", and it did not grade Bradley's applications-as-starts wording, leaving that to the article's own correction.

**3. everyman.** Confirmed Chetty and CBO, did not pull the GDP or BFS data, and left the 5.68M record unconfirmed.

What would change the order: the two top answers agree on every number; askamerica's explicit grading of Bradley's own business-starts claim against expert's Sanders-Hawley catch is the margin, and a reader weighting the latter could swap them.

## Severity invariant

askamerica ranks first. No defect flag.

## Claim-shape check

First delivery recorded Bradley's own claims only as `fidelity` entries, with no `subject_claims`. Resumed the same subagent; it added two `subject_claims` with speaker "Neil Bradley" (2% growth line: true; 5.68M business starts record: partially false). Verified on disk in claims.json; engine rating stayed 0.

## Gaps and resolution

The warehouse has no Business Formation Statistics table; askamerica summed Census releases from the web. Filed govdata-ops **#1410**, `econ: add Census Business Formation Statistics` (type:sourcing, kind:gap, schema:econ, status:open). askamerica ranked first, so this is a gap filing, not a loss resolution.

## Recipe check

Nothing the unaided arms did that askamerica did not.
