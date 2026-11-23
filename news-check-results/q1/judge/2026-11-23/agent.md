# Judge - Q1 (news fact-check), 2026-11-23

**Story:** Center of the American Experiment (John Phelan, 2026-11-17), "Since 2014, Minnesota's median household income growth ranks 49th out of 50 states". https://www.americanexperiment.org/since-2014-minnesotas-median-household-income-growth-ranks-49th-out-of-50-states/

Rank: [expert, askamerica, everyman]

All three personas ran fresh today. Fingerprint check passed for everyman, askamerica and expert. The askamerica answer is the post-recipe rerun (calls.jsonl entries 25-53, the second session). Neither everyman nor expert cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Rating | 2 Pinocchios, provisional | 0 Pinocchios ("from the engine's claim scores") | 1 Pinocchio |
| Claims verified | 0 of 6 (all "not checkable here") | 7 true, 1 unsupported (8 claims) | 9 of 9 checkable reproduced, 1 not retrievable (earlier posts) |
| GDP per capita gap (+4,658 / -2,767) | not checked; a USAFacts rank raised a doubt it could not resolve | reproduced from BEA SAGDP9 / SAINC1 | reproduced from the same BEA series |
| 1997-2014 GDP per capita rank (17th) | not checked | unsupported: warehouse population starts 2007 | reproduced, +30.7% vs US +26.1%, rank 17 |
| Income ranks 16th / 49th | not checked | reproduced from CPS H-8 (+11.1%, +7.3%) | reproduced from CPS H-8, same values |
| Correlation claim | not checked | "TRUE as reported", post read, not re-estimated | re-estimated: r=0.52, p=0.0001, n=50; r=0.48 without ND |
| Sampling error | named as a concern, no number | quotes CPS SEs ($5,191 vs $2,013) but no rank-uncertainty estimate | Monte Carlo from published SEs: rank >= 49 in ~25% of draws, median 45, 5-95th pct 28-50 |
| Alternative series | none | ACS 1-year from the warehouse: MN 38th of 50 | 3-year averages 2013-15 vs 2023-25: 45th; ACS not run (needs a key) |
| Other framing | level rank 14th, advocacy, GDP is not household income | start year is MN's GDP peak; Figure 2 sourcing; r used as causal support | level rank 6th to 17th (convergence); 2013 CPS redesign inside the 1997-2014 window; ecological inference; start-year choice |

## Ranking rationale

**1. expert.** It got every claim right, including the one askamerica could not support (1997-2014 GDP per capita rank, built from the BEA zip files that go back to 1997). It re-estimated the correlation instead of repeating the post's figure. Its calibration is the best of the three. It states that the 49th rank is the point-estimate rank, then quantifies how fragile it is: the simulation puts rank >= 49 in about 25% of draws, and the 3-year-average rank is 45th. It concludes the honest statement is "bottom decile", not "49th". It also catches the 2013 CPS redesign inside the 1997-2014 window, which neither other persona raised, and the level-rank convergence (6th to 17th). Its 1 Pinocchio matches the evidence: true numbers, fragile headline, no causal support. Its weak spot is that it did not run the ACS cross-check, and it says so.

**2. askamerica.** It matches the expert on the GDP per capita claims (0 to 3, 4, 5 as above) and on both CPS income ranks, and it reaches the primary CPS file. Its distinctive contribution is the ACS 1-year cross-check from the warehouse: Minnesota is 38th of 50 for 2014-2025, not 49th. That is stronger evidence against the headline than a simulation, because it is a second published series, not a model of the first. Puerto Rico and DC handling is stated. It places below the expert for three reasons:
- It left claim 5 unsupported. The warehouse series for state population starts in 2007, so a 1997 per-capita figure could not be built. The "first time on record" claim (claim 2) is likewise confirmed back to 2007 only, where the expert's BEA series reaches 1997.
- Its rating is miscalibrated. It gives 0 Pinocchios, "from the engine's claim scores", while its own framing section says the 49th rank flips to 38th on ACS and carries a standard error of $5,191. A central claim that does not hold on a second federal series, plus an unexplained start-year choice, is not a 0. The claim-score mechanism graded each assertion as reproduced and never reconciled the rating with the framing findings. The expert's 1 is the better-supported grade.
- It did not re-estimate the correlation and it did not turn the SEs into a rank-uncertainty statement. It reports the SEs but leaves the reader to infer the effect on rank. The ACS result is a divergence it flags without explaining (CPS vs ACS differ in design, supplements and income definitions; it does not say which is preferred for state rank comparisons).

**3. everyman.** It could not open any primary table, so no numeric claim is verified. Its method note is honest and its framing points are sound (rank of growth is not rank of level, endpoint sensitivity, margins of error, advocacy source, GDP is not household income). Its 2 Pinocchios is "provisional" and rests on unverified claims, so it is a hedge more than a grade. It also treats a secondary USAFacts rank as a possible contradiction without being able to resolve it. askamerica clearly beats it: 7 of 8 claims verified against data, a real second-series check, and a rating with evidence behind it, albeit the wrong value.

**What would change the ranking:** askamerica vs expert is a clear call, not a close one, because expert wins on claims verified, on calibration and on the framing analysis. It would narrow if the grader weighted a second-series check over a simulation. The simulation models sampling error inside one survey; the ACS check shows disagreement across surveys. A reader might trust the ACS result more. If askamerica had also rated 1 or 2 Pinocchios and supported claim 5, it would take first. If the expert's simulation inputs were shown to be wrong (SE treated as independent across years), the expert's lead on calibration would shrink, though the 3-year-average result would still stand.

## Severity invariant

askamerica ranks above everyman. No defect flag. It does rank below the expert, which is a loss to log, not a connector defect.

## Recipe check

Step 6 note: today's first askamerica run stopped at census.pep_population (ends 2023) and left the GDP-per-capita claims unsupported. Recipe `fact-check-a-news-article` gained item 11 (use BEA population, econ.regional_income SAINC1 line_code 2, as the per-capita denominator) and was built and installed to engine-dev. The askamerica answer judged here is the rerun on that jar; the pre-recipe answer is kept at `~/askamerica-daily/news-q1-askamerica-pre-recipe-2026-11-23/`. PEP vintage 2024/2025 sourcing is filed as govdata-ops #1451.

**Did item 11 take effect?** Yes. The rerun queries `econ.regional_income` with `tablename='SAINC1'`, `line_code='2'`, `geo_fips_set='STATE'` (calls 33 and 36), and claims 1, 2, 3 and 4 move from unsupported to true with the expected values (+$4,658, -$312 in 2023, -$2,767, 46th). That is the targeted behavior change. The expert reached the same numbers with the same BEA tables, so the evidence behind item 11 was the unaided kind.

**Remaining gaps, what each rests on:**
1. Rating calibration. The expert (unaided, so the stronger kind of evidence) rated 1 Pinocchio from the same facts, with the fragility quantified. askamerica's 0 came from averaging per-claim scores while its own framing section showed the central claim failing on ACS. This is a recipe gap, not a data gap: the existing `fact-check-a-news-article` recipe should say that when the central claim's rank or sign changes on a second warehouse series (here `census.acs1_income`), or the series' own SE puts the rank within noise, the overall rating cannot be 0 even if every number reproduces. The warehouse-native method: compute the rank on `census.acs1_income`, compare it to the article's survey, and carry the disagreement into the rating.
2. Rank-uncertainty. The expert's Monte Carlo can be done in warehouse terms: the CPS SEs are published in H-8, and the recipe could point to them. This is lower priority than item 1, since askamerica's ACS cross-check already shows the instability.
3. 1997 per-capita history. The SAINC1 population in `econ.regional_income` starts in 2007 in the warehouse, so 1997-2014 per-capita claims stay unsupported. This is a data-depth gap, not a method gap. The expert used the BEA SAINC zip, which goes back to 1997. It is a sourcing matter (backfill of SAINC1 pre-2007). It is separate from #1451, which covers PEP vintages 2024/2025.

**Resolution (one):** recipes.json change to `fact-check-a-news-article`, adding the calibration rule in gap 1 (a central claim that fails a second warehouse series, or sits within the series' sampling error, caps the rating above 0). The 1997 backfill is noted here as a separate sourcing follow-up and is not this pass's resolution. This judge pass changed no recipe file; the edit is proposed.
