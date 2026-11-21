# Judge: OPB / Oregon Capital Chronicle, Oregon chronic absenteeism (news q1, 2026-11-21)

Rank: [expert, askamerica, everyman]

Story: https://www.opb.org/article/2026/11/19/poor-school-attendance-persists-in-oregon-remains-among-lowest-in-nation-post-covid-19/ (2026-11-19). Claimants: Oregon Department of Education report cards, Future-Ed (national figure), Chris Wells on SB 817.

Verdicts: expert 1 Pinocchio, askamerica 2 (engine-scored), everyman 1.

## Ranking

**1. expert.** Traced the 2024-25 baselines (86.6% on track, 33.5% chronic absence) to OPB's 2025-11-20 report, checked SB 817 against ODE guidance and legislative analyses, and placed the national "roughly 21%" at the low end of published estimates (RAND, AEI, K-12 Dive: 21.8-23%), noting there is no official national figure and the comparison mixes years. Flags the 69% regular / 32% chronic definitional mismatch. Grades only the quotes and grade-level rates it could not reach as not checkable. Its 1 Pinocchio fits a piece whose figures are sourced to ODE and broadly consistent.

**2. askamerica.** Found one real inconsistency the others missed: the article's counts do not reproduce its rates (36,512 / 41,713 = 87.5%, 36,200 / 41,240 = 87.8%, against the stated 86.6% and 86.8%). Read the enrolled SB 817 text to confirm the truancy-fee removal, and kept Wells's causal argument separate from the factual part. But it graded the national 21% as unsupported because the linked Future-Ed page lacked it, when published estimates near that figure exist, and it did not locate the 2024-25 ODE baselines expert found. The warehouse could not help: `edu.crdc_schools` is biennial and ends in 2021-22. The resulting 2 Pinocchios is harsher than the evidence supports.

**3. everyman.** Same 1-Pinocchio verdict as expert but from secondary summaries only; it could not open the ODE files or the full article and checked fewer claims.

What would change the order: if the count/rate mismatch were the article's own error rather than a denominator difference in ODE's method, askamerica's harsher grade would gain weight and the top two would be close.

## Severity invariant

askamerica ranks above everyman. No defect flag.

## Claim-shape check

Compliant: piece-level figures are `author_claims`; Chris Wells is a `subject_claims` entry with `speaker`. Rating in report.html and agent.md both 2.

## Resolution (one)

Govdata-ops **#1406**, `edu: add annual EDFacts chronic absenteeism by state/district` (type:sourcing, kind:gap, schema:edu, status:open). With an annual federal series, askamerica could have checked the national rate and Oregon's past-year rate from the warehouse instead of from a page that lacked the figure.

## Recipe check

expert's move of bracketing an unsourced national figure with several published estimates is general web practice; no warehouse recipe.
