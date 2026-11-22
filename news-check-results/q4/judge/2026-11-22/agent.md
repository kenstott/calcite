# Judge: news q4, Chalkbeat "NYC public school enrollment drops by 23,000 students" (2026-11-16) (2026-11-22)

Rank: [askamerica, expert, everyman]

Story claims: NYC K-12 enrollment fell about 23,000 (nearly 3%) to about 766,900; 863,100 with preschool (down about 2.6%); about 200,000 (nearly 20%) lost over a decade; nearly $2B spent since 2020 on hold-harmless budgets; a projected further 130,000 decline by 2034-35; 30 states with declines since 2014 (citing NYT); quotes from Chancellor Kamar Samuels; closure proposals.

## What each answer found

- **askamerica (0 Pinocchios).** It fetched the article and its linked sources. NY1 independently gives 766,900 K-12, down 23,000 (2.9%), and 863,100 vs 885,860 with preschool (-2.57%). It computed the decade loss from NY1's 964,132 in 2016-17 to 766,900 (-20.5%), so "nearly 20%" slightly understates it. Hold-harmless spending is $1.9B per Chalkbeat's June piece. It read the SCA projection PDF directly: 849,652 to 721,251 (-128,401), noting the PK-12-excluding-D75 base. "Thirty states" is mostly true: its warehouse query (`edu.ccd_districts`) shows 31 of 51 jurisdictions lower in 2024 than 2014. Samuels' quote is graded unsupported. It checked 15 claims in all.
- **expert (1 Pinocchio).** Consistent figures. Decade loss from the Comptroller (-187,000, -19% since 2013-14). 128,000 projection. 31 of 51 lower via NCES Digest table 203.20. It could not retrieve the DOE release, the closure list or the quote, and it skipped several claims.
- **everyman (1 Pinocchio).** Secondary-source check only. It could not source the 766,900 figure and skipped several claims.

## Ranking reasoning

1. **askamerica.** It covered the most claims and was the only one to confirm the 766,900 headline figure against an independent outlet and to read the SCA projection's own numbers and base. Its national "30 states" check came from warehouse data and matches expert's NCES Digest count exactly (31 of 51).
2. **expert.** Accurate, with a good primary national check, but it confirmed less of the NYC-specific detail.
3. **everyman.** Thin.

What would change the ranking: if askamerica's NY1-sourced figures were wrong. Expert's Comptroller numbers are consistent with them, so this is unlikely.

## Severity check

askamerica ranks above everyman, so there is no severity flag.

## Rating shape and gap audit

- `claims.json`: `author_claims`, one `fidelity` (the NYT-attributed "thirty states"), and `subject_claims` with `speaker: "Kamar Samuels"` for the quote. That is the correct shape. Rating: 0 in both report.html and agent.md.
- Gap check: askamerica wrote that "the warehouse has no usable NYC row (the NYC LEA shows null or zero enrollment)." I checked live: `edu.ccd_districts` carries NYC as 32 "NEW YORK CITY GEOGRAPHIC DISTRICT # n" LEAs with real enrollment (for example, District 2 in 2016 has 62,602). Only the umbrella LEA is empty. This is a catalog-navigation miss, not a data gap, so no issue was filed. It did not affect the outcome, because askamerica got the NYC figures from NY1 and the SCA.

## Recipe check

Candidate pattern, from this run's own evidence: NYC public schools in NCES CCD are reported as 32 geographic-district LEAs plus District 75/79 and charters. The umbrella "New York City Department of Education" LEA has no enrollment, so the NYC total must be summed across the geographic districts. askamerica ranked first, so no resolution is required today. If a NYC-schools story loses on this in future, add it as a `recipes.json` entry.
