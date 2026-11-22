# Judge: news q6, InvestmentNews "SEC enforcement against public companies hits record low under Atkins" (2026-11-12) (2026-11-22)

Rank: [askamerica, expert, everyman]

Story claims (from the Cornerstone Research / NYU Pollack SEED FY2026 report): 16 actions against public companies and subsidiaries in FY2026, down 71% from 56, the lowest since SEED began (FY2010); prior low 37 (FY2013); $184M in settlements, also a record low; 99% civil penalties; 63% of settling defendants paid money; 53% had cooperation noted; no admissions; no FCPA actions; half the actions involved issuer reporting; 25% civil actions; sample of 1,065 actions against 903 entities. Quotes from Chair Paul Atkins (May 8 speech) and co-author Stephen Choi.

## What each answer found

- **askamerica (0 Pinocchios, 12 of 12 true).** It fetched and read the full Cornerstone FY2026 report PDF, the NYU Stern release, SEC press release 2026-113 and Atkins's speech. It verified every figure, including the cooperation share (53%) and the average/median settlement ($12M / $1M), which the other two could not retrieve. It confirmed the Atkins bracketed alteration matches the report and the Choi quote is verbatim. It added the context the article omits: SEC-wide standalone actions fell only about 22% (303 to 235), total actions were 348 with $1.095B in relief, and public-company actions were under 7% of standalone actions. It noted that the 25% civil share rests on 4 actions and that FY2025 overlapped Atkins's tenure.
- **expert (0 on facts, up to 1 for context).** It matched the headline figures via the NYU Stern summary and D&O Diary. It could not get the full report, so the cooperation figures and the settlement-decline percentages were not checkable. Its context points were good: scope, causal framing, Atkins's own caveat, and average vs median skew. Its SEC-wide -24% came from American Banker and was not verified.
- **everyman (1 Pinocchio).** It verified most figures against the release but missed several. It flagged a possible misattribution of a Woodcock quote, which askamerica verified through the report's footnote. It made similar context points.

## Ranking reasoning

1. **askamerica.** It read the primary report itself and checked all 12 claims. It also gave the decisive context with figures sourced from the SEC's own release (SEC-wide down 22%, against the article's 71%).
2. **expert.** Correct on what it reached, with good framing, but it never read the report.
3. **everyman.** Mostly right, with fewer verifications.

## Severity check

askamerica ranks above everyman, so there is no severity flag.

## Rating shape and gap audit

- `claims.json`: 10 `author_claims` and `subject_claims` for Atkins and Choi with `speaker` set, which is correct. Rating: 0 in both report.html and agent.md.
- Gap: the warehouse has no SEC enforcement actions table (only the Fed's `banking.enforcement_actions`). Filed kenstott/govdata-ops#1437 (`type:sourcing`, `kind:gap`, `schema:sec`, `status:open`), sourcing SEC litigation releases, administrative proceedings and annual results. askamerica won without the warehouse here. The gap limits independent checking of the next enforcement-count claim.

## Recipe check

Nothing to teach. askamerica read the cited report directly, which is the right first move.
