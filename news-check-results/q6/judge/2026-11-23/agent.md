# Judge — q6 news fact-check (TPR / Texas Tribune, 2026-11-18, "One in three Texas students leaves college without a degree"), 2026-11-23

**Story:** Philanthropy Advocates / Jacob Kirksey "Stopouts Report 1". Each persona fact-checked every claim and graded 0-4 Pinocchios.

Rank: [expert, askamerica, everyman]

Fingerprint check passed for all three. Neither everyman nor expert cites askamerica.ai. All three grade 1 Pinocchio; the ranking turns on verdict accuracy against the primary report, not the headline grade.

## Answers in brief

| Claim | everyman | askamerica | expert (read full report) |
|---|---|---|---|
| 38% / 347,061 left | supported, "one in three" understated | mostly true, same point | exact (443,473 / 347,061 / 122,909) |
| $1.1B debt, "last five years" | supported, window "ambiguous" | mostly true, a floor ("at least $1.09B") | misframed: a floor by entering class, not a five-year accumulation |
| GPA "just above 1.0" | differs from "half at 1.0 or lower" | partially false (same basis) | roughly right; median year-one exit GPA about 1.04, "averages" imprecise |
| "Nearly 30% had debt" | "not reconciled", implies a far higher share, unconfirmed | could not verify | 29% of six-year stopouts, confirmed |
| $6,250 "average" | likely mislabeled median | median, not average | median; mean is $12,155 |
| $20,800 wage gap | supported | true; ACS Texas gap $23,116 as context check | true; but the cohort is HS classes 2012-2016, not 2017-22 |
| Abbott 60% goal | not re-verified | true: governor's office announced 60x30 on 2015-11-16 (25-34); "credential of value" and working-age wording come from the later coordinating board plan | loose attribution, medium confidence; found no Abbott statement with the exact wording |
| Hinojosa / Pollock / Grantham quote | "not checkable" | not in the final claim set | election confirmed, Pollock medium, quote not checkable |

## Ranking rationale

**1. expert.** It read the report PDF in full text. That is the one thing the other two lacked. It confirmed the 29% share, found the mean ($12,155) against the median, found the debt is observed 2 to 7 years by entering class, and found the wage gap uses earlier cohorts. It also gave the "left college" caveat (the 14% still enrolled are excluded). Its one soft spot is the Abbott claim. It calls it "loose" and found no dated Abbott statement. That reading is defensible but stricter than the evidence needs; see the 60% goal below.

**2. askamerica.** The `calls.jsonl` latest session (13:02-13:04 UTC, seq 1-17) shows it tried the report PDF (`web_fetch`, max_pages 20) and the final answer states the PDF exceeded the 25MB cap. It then relied on the FOX 4 relay, so it did rely on FOX 4 for every report figure. That is the same limitation everyman had. What it added over everyman, each checked in the file:
- 60% goal: it fetched the governor's 2015 release and graded the claim true with the precise split of what the governor announced and what the board added. A WebSearch I ran agrees: Abbott announced the plan in November 2015 for ages 25-34, and "credential of value" and the 25-64 wording come from the later Building a Talent Strong Texas framework. So true-with-a-caveat is right. The expert's "loose" is an over-reading and everyman's "not re-verified" is no verdict.
- Earnings gap: an ACS cross-check from `census.acs_earnings_by_education` (gap $23,116, same magnitude). It is a warehouse check the unaided runs could not do. It also shows the study's $20,800 is plausible, though the ACS tier definitions (bachelor's against some college) are not the study's.
- Median/average: caught, and the evidence is stated.
- GPA: graded "partially false". The right call is closer to "imprecise" (the expert's medium-confidence read, median 1.04). This is slightly harsh but the stated basis is correct.
- 29%: left unverified ("could not be verified"), and graded "mostly true" with the $6,250 claim. That is honest about the gap, but the claim was verifiable in the report. Everyman got it worse: it said the 29% looked inconsistent with the 331,373 / 347,061 borrower count, which is a misread of two different denominators and could mislead a reader.
- Calibration: the "Not done" note names the unread PDF and says the 30% and GPA wording rest on FOX 4. That matches reality. The rating of 1 Pinocchio matches the expert's.
- Gaps against expert: it never reached the 29%, the $12,155 mean, the five-year framing as a misframe, or the wage-gap cohort mismatch. It dropped the Hinojosa, Pollock and Grantham claims from the final set although the first session fetched the Tribune Hinojosa piece. That is incomplete coverage of "every claim".

**3. everyman.** It lists all 14 claims, which is the fullest enumeration. Its verdicts on the numbers are sound and it spotted the median/average issue and the "one in three" understatement. But it left the 60% goal unverified, called three claims "not checkable here", and gave a wrong-direction concern on the 29% share. It also left the wage gap and debt window without the cohort/framing caveats. No connector, no primary report.

**What would change the ranking:** expert over askamerica is not close, because the primary report decided four claims. Askamerica over everyman is moderately close. Everyman covers more claims (14 against 8), so a grader who counts claim coverage over verdict quality could swap them. I weight verdict accuracy and the verified 60% goal, the ACS check and the unaided misread of the 29% share more heavily.

## Severity invariant

askamerica ranks above everyman. No defect flag.

## Recipe check

"Step 6 note: today's first askamerica run graded 'Abbott set the 60% goal' partially false because it found no page. Recipe `fact-check-a-news-article` item 13 gained a sentence (failing to find a source is 'unsupported', not false; check the issuing office's own news page), built and installed to engine-dev (backup `askamerica-engine.jar.bak-2026-11-23-q6-q8-recipes`). The askamerica answer judged here is the rerun on that jar; the pre-recipe answer and first-pass judge doc are at `~/askamerica-daily/news-q6-askamerica-pre-recipe-2026-11-23/`."

**Analysis.** The recipe change took. The latest session served `fact-check-a-news-article` (seq 1 and 2) and then fetched the governor's own news page (seq 6, `gov.texas.gov/news/post/governor_abbott_announces_60x30...`). The claim moved from partially false to true with a stated caveat, which a WebSearch confirms is the correct verdict. The evidence for the recipe fix came from the unaided arms' pattern of checking the issuing office, and askamerica followed it once the recipe said so.

**Remaining gap, not a recipe gap.** Askamerica could not read the 25MB-plus report PDF through `web_fetch`, while the expert read it in full through a shell. Every verdict that moved between the two (29%, mean $12,155, debt framing, wage-gap cohort) traces to that one fetch. A recipe cannot fix a size cap on the fetch tool. A second candidate is a recipe line to say, when the primary document cannot be fetched, which claims in the answer rest on a secondary relay. The answer already does this in its "Not done" note, so no recipe edit is needed. Claim coverage (dropping the Hinojosa, Pollock and Grantham claims) is a small recipe candidate: item 13 could say every checkable claim in the article gets a row, with "not checkable" as the verdict when it cannot be checked. I am not proposing it from one run, since the existing evidence is askamerica's own omission and the unaided everyman only shows the same pattern is reachable.

**Resolution:** none this run.
