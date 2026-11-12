# Face the Nation (Nov. 8, 2026) transcript fact-check

Overall: 1 Pinocchio (mostly accurate). The engine rated the claim set "too few to rate" because most of the transcript is opinion, event reporting and allegation.

Fetched in full: CBS transcript and the linked sub-transcripts.

## Graded claims
- Hill: "20 percent inflation under Biden; down from 9 to 3 percent". Mostly true. CPI-U rose 21.4% from Jan 2021 to Jan 2025 (econ.fred_indicators CPIAUCSL). The mid-2022 peak was about 9.1% YoY. Current YoY is 3.4% (Aug 2026) and 3.7% (Sep 2026), so "3 percent" is slightly low.
- Means: "Continental Strategies revenue went from zero to $30M a year". Partially false. LDA income was $0.57M in 2023, $0.20M in 2024 and $25.3M in 2025 (law.lobbying_filings, matched by name). The roughly 100-fold jump is real, but "zero" and "$30M" are exaggerated.
- Means: "Ballard Partners is the highest revenue lobbying firm in history". Mostly true, as hyperbole. Ballard ranked #1 for 2025 with $79.9M against BGR's $70.3M. The all-time ranking was not tested.
  - Disclosure: the ranking query used `f.income IS NOT NULL`. It excluded Hackensack Meridian Health, Amanda Gort, Natl Assc of State Directors of Career Technical Education and Human Coalition Action. Null-income rows otherwise sort first.

## Not checkable here
- Reynolds American $5M donation to a Trump-backed PAC. The catalog has no Reynolds entity and no outside source was found.
- "Nine out of 10 voters cited corruption". It is an exit-poll figure and the catalog has no such measure.
- Hill's 4,000-vote margin. officials.house_election_results ends at 2022.
- The CENTCOM $20M reward.
- The CBS politics page corroborates the Massie Hegseth impeachment articles.

## Misleading framing
- Means ties Continental's revenue to Katie Wiles' employment. Lobbying receipts cannot show that, and Continental Strategy disputes it in the transcript.
- His allegations of direct payments and of Fabrizio's clients are unverified.

Report saved at /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q5/askamerica/2026-11-09/report.html
