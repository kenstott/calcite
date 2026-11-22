# Judge: news q7, Fox News "One industry increased DC lobbying 337% under Trump" (2026-11-14) (2026-11-22)

Rank: [askamerica, expert, everyman]

Story claims (from an Advancing American Freedom report, author Rachel Greszler): spending on lobbying filings that mention tariffs rose 337% from Q3 2024 to Q3 2026; tariff-mentioning disclosures rose 239%; 14 organizations spent over $1M (up from 2) and 105 over $100K (up from 26); the NY Fed estimate that tariffs added 2.9 points to goods inflation; AAF's job-loss estimates; US-Canada trade fell $50B (6.5%); Greszler quote.

## What each answer found

- **askamerica (1 Pinocchio).** It is the only persona that tested the central claim against the filings themselves, using warehouse LDA tables (`law.lobbying_filings`, `lobbying_activities`), Q3 2024 vs Q3 2026, filings mentioning "tariff". Reported dollars rose 197% ($108.1M to $321.6M), not 337%. Tariff-mentioning filings rose 251% (374 to 1,312), against the article's 239%. On a broad definition, clients over $1M were 73 vs 21 and over $100K 436 vs 130, so the 14/2 and 105/26 counts were graded partially false. It disclosed its method and denominators (filings without "tariff" excluded; all-filing spending +30%). NY Fed 2.9 points and AAF job estimates are true as reported, and Section 338 Canada tariffs are real. The causal job claim, the Canada $50B figure and Greszler's assertion are unsupported. Framing: Pence-founded advocacy source, omitted larger Q1/Q2 readings, and a pre-tariff 2024 base.
- **expert (2 Pinocchios).** It could not reach the AAF report or lda.senate.gov, so the lobbying figures went unverified. It verified the NY Fed figure, adding the date and basket scope and the easing to about 2% by August. It found no source for Canada $50B. Its framing critique was good.
- **everyman (2 Pinocchios).** It checked secondary coverage only. Good catch on AAF's shifting earlier percentages (599%/655%/690%) and the NY Fed scope.

## Ranking reasoning

1. **askamerica.** It reproduced the claim from primary filings and found the dollars figure materially lower (197% vs 337%) while the count figure was close. That is the one thing an unaided reader cannot do from the open web, and it is the centre of the story.
2. **expert.** Sound on the checkable secondary claims and framing, but no primary check of the lobbying numbers.
3. **everyman.** Similar to expert, thinner.

A note on askamerica's grading: it graded the 337% claim "mostly true" because the article accurately relays AAF's published number and flags that it may include non-tariff issues, while its own reproduction got 197%. A reader may find "mostly true" generous against a 197% reproduction. That is defensible as a fidelity call, since AAF's definition is undisclosed, and it does not change the ranking.

## Severity check

askamerica ranks above everyman, so there is no severity flag.

## Rating shape and gap audit

- `claims.json`: `author_claims` plus `subject_claims` with `speaker: "Rachel Greszler"`, which is correct. Rating: 1 in both report.html and agent.md.
- Gaps: none to file. The lobbying data exists and was used. The Canada $50B figure was found by no persona, and no public source carries it, so it is unsupported rather than a corpus gap.

## Recipe check

Nothing to teach. Querying the LDA filings for the article's exact comparison windows is the right warehouse-native method.
