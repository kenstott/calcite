# Judge: news q1, Urban Milwaukee on Wisconsin for-profit vs public utility rates (2026-11-22)

Rank: [expert, askamerica, everyman]

Story: John D. Johnson, "Wisconsin's For-Profit Electric Utilities Hike Rates More Than Public Utilities," Urban Milwaukee, 2026-11-11. Central claims: residential cost per kWh grew 29% for investor-owned utilities (IOUs) since 2020 against 15% for cooperatives and 14% for municipals; in 2025 the IOU rate was 115% of the co-op rate and 148% of the municipal rate; plus bill-growth figures per utility.

All three personas ran fresh today; everyman and expert prompts were composed by `compose_prompt.py`.

## What each answer found

- **expert (0 Pinocchios).** It reproduced every headline number from EIA-861 through 2025: +29.4% / +14.9% / +14.4%; 115% and 148%; We Energies $391, Alliant $408, WPS $276, Xcel $177; Cedarburg 13.44c vs Wisconsin Electric 20.53c ($567). It confirmed the November 5 PSC approvals via press coverage. It flagged the causal framing (no control for scale, density or generation) and the lower 2020 municipal base. Small differences: 66 matched utilities against the article's 65, and hypothetical bills within $5.
- **askamerica (1 Pinocchio).** From `energy.eia_utility_annual` it confirmed the pattern for 2020-2023 (+20.3% / +6.7% / +10.9%) and the article's 2020 base levels. It said plainly that 2024-2025 are not in the warehouse, so the 29/15/14 endpoints were not checked. It added context the others lack: WPR's report that the PSC trimmed the requests and cut allowed ROE from 9.8% to 9.5%, which the article omits. It made the same causal-framing critique as expert and graded the "costs affect all utilities equally" line partially false. Its doubt about the Xcel $177 figure turned out to be unfounded, since expert reproduced it from 2025 data.
- **everyman (provisional 1 Pinocchio).** It never reached EIA-861 and graded all the core numbers "not checkable here." It confirmed the PSC approvals from news reports and made the same framing critique. It also noted the survivorship filter in the 65-utility sample, a good point.

## Ranking reasoning

1. **expert.** It is the only one that verified the article's actual claims, which are 2025 endpoints, number by number.
2. **askamerica.** It confirmed the direction and the 2020 base from warehouse data, and its ROE/trimming context is the best on the regulatory claim. It could not check the endpoints because the warehouse table stops at 2023. That is a real data-freshness gap, not a method failure. It also cast unwarranted doubt on one figure (Xcel) because of that gap.
3. **everyman.** It checked no core numbers.

What would change the ranking: with 2024-2025 EIA-861 loaded, askamerica would very likely match expert on values and lead on regulatory context.

## Severity check

askamerica ranks above everyman, so there is no severity flag.

## Rating shape and gap audit

- askamerica's `claims.json` has 8 `author_claims`. The piece is an analyst's own column with no quoted third parties, so no `subject_claims` are needed. The rating is consistent: 1 Pinocchio in both report.html and agent.md.
- Gap: `energy.eia_utility_annual` holds report years only through 2023 (live query today: 2021 = 2,799, 2022 = 2,829, 2023 = 2,822, nothing later). EIA's 2024 final is published. Filed kenstott/govdata-ops#1433 (`type:defect`, `kind:gap`, `schema:energy`, `status:open`).

## Resolution (askamerica not #1)

The loss traces to the data gap above. Resolution: govdata-ops issue #1433. No recipe or McpServer change, since askamerica used the right table and method and disclosed the coverage limit correctly.

## Recipe check

Nothing to teach. askamerica's method (bundled-service utilities present in both years, revenue/kWh by entity type) matches expert's.
