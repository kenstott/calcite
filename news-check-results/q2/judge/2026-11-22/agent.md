# Judge: news q2, Axios Indianapolis "Indy pedestrian deaths test Vision Zero promises" (2026-11-22)

Rank: [askamerica, expert, everyman]

Story: Axios Indianapolis, 2026-11-12. Claims visible in search summaries: Indianapolis ranked 47th of 101 metros in Smart Growth America's Dangerous by Design 2026; 231 metro pedestrian deaths in 2020-24 vs 170 in 2015-19; pedestrian deaths "nearly doubled" since the Vision Zero plan was adopted; goal of zero fatal and serious-injury crashes by 2035.

**Axios returned 403 to all three personas, and no copy of the story exists online.** Every grade is provisional, built from search summaries of the article.

**askamerica did not deliver.** The engine's article-text gate refused `preview_report` because the source and every copy search failed. That gate has no exit, so the report could never publish (filed as kenstott/govdata-ops#1434). askamerica's agent.md is recovered verbatim from its final message and marked as such. There is no report.html or claims.json, so the rating-shape check cannot run.

## What each answer found

- **askamerica (provisional 2 Pinocchios).** It confirmed #47, 231 and 170, and 2.17 per 100k from the Smart Growth America rankings table. It added an independent warehouse check: FARS pedestrian deaths in Marion County (`transport.fatal_crashes`) were 150 in 2015-19 vs 212 in 2020-24 (+41%), with a 2020 peak of 48 and 38 in 2024. It graded "nearly doubled since the plan" mostly false as framed: the cited rise is 2020-24, before the December 2025 plan, and FARS 2026 is not published. It flagged the framing as implying a post-plan surge that neither source shows. It noted the 23 year-to-date deaths and the "outpacing last year" quote as a possible narrower basis it could not grade.
- **expert (provisional 1 Pinocchio).** It confirmed the same Smart Growth America figures and the December 2, 2025 plan date. It graded "nearly doubled" not checkable, and made the same point that pre-plan metro counts don't test Vision Zero. It produced the required board and report.
- **everyman (provisional 1 Pinocchio).** It confirmed the 2035 goal and plan timing. It found the 23-of-61 YTD task-force figure but no 2025 baseline. It graded the Smart Growth America ranking "not checkable" (it never reached the report), and made a similar framing critique.

## Ranking reasoning

1. **askamerica.** It is the only persona with an independent primary-data check (FARS county series), and the only one to grade the central "nearly doubled" framing on evidence rather than leaving it open. Its substance is the strongest of the three. Its delivery failed through no fault of the analysis: the engine blocked it.
2. **expert.** It verified the cited report directly and was candid about limits, but added no independent data.
3. **everyman.** It did not reach the cited report.

What would change the ranking: scoring on delivered artifacts instead of content would drop askamerica to last, because a real user received nothing. That is the product cost of #1434. I rank on what each persona found and said, per the eval's recovered-run rule, and flag the delivery failure here and in the run report.

## Severity check

askamerica ranks above everyman on content, so there is no severity flag. The delivery failure is logged as a defect (#1434), not a ranking.

## Gap audit

- 2026 pedestrian fatalities: FARS 2026 is not yet published (normal lag), so there is no gap to file.
- The block was the engine's publish gate, not data. Filed as #1434, which also records that resuming the subagent restarted the MCP server and dropped the session's call record, which then caused further false refusals.

## Recipe check

Nothing to teach. askamerica's county FARS check is the right warehouse-native move.
