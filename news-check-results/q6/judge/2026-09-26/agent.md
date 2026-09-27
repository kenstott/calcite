# Judge: q6 — "CDC: 3,659 confirmed 2026 measles cases; kindergarten MMR coverage 95.2%→92.4%"

Blind judge of three fresh, same-day persona runs (news claim, Workstream A of the daily-eval pipeline).

Rank: [expert, askamerica, everyman]

## Reasoning

All three independently confirm both halves of the claim as true, cross-checked against CDC-sourced reporting: 3,659 confirmed 2026 measles cases as of September 24, 2026, and kindergarten MMR coverage declining from 95.2% (2019-20) to 92.4% (2025-26). No corpus (AskAmerica or otherwise) among the three answers could compute either figure directly — this is a genuine, three-way-confirmed sourcing gap, not a query-construction failure by any one persona.

**Expert** is first: it reaches CDC's own MMWR series directly (mm7003a2, mm7245a2, mm7341a3) rather than relying only on secondary summaries, and reconstructs the full 7-year monotonic decline (95.2 → 93.9 → 93.5 → 93.1 → 92.7 → 92.5 → 92.4) rather than just the two claimed endpoints — ruling out a "two-point comparison hiding a rebound" concern the other two don't check. It adds real context the other two lack: the ~93-95% herd-immunity threshold (every year since 2021-22 sits below it), that 2026 already exceeds every full year since measles elimination was declared in 2000, and an explicit note on CDC's own cross-year comparability caveats (state variation in requirements, COVID-19 disruption to the 2019-20 baseline) — checked and found not to change the reported national topline.

**askamerica** is second: it correctly and thoroughly diagnoses the corpus gap (confirmed via `search_catalog` across all 68 health-schema tables, naming the specific reason each near-miss table doesn't qualify — `cdc_covid_vaccinations` is COVID-only and frozen since 2023-05-10), which is itself a genuine, actionable finding (filed as govdata-ops #662). Its external verification is thorough — four sources fetched in full, including a direct week-earlier data point (CIDRAP, 3,471 cases as of Sep 17) that corroborates the claimed trajectory rather than just restating the same headline number. It's ranked below expert because it stops at the two claimed endpoints rather than reconstructing the full multi-year series, and its two attempts to fetch CDC's own primary pages both 403'd without a retry via an alternate route (e.g., a cache/mirror), which expert's dispatch did not encounter or worked around.

**everyman** is third: reaches the same correct verdict with solid sourcing (CDC's own pages plus Washington Times, ContagionLive, Axios, CNBC, Forbes) and one figure neither other answer states as concretely (an estimated ~280,000 kindergartners nationally lacking documented MMR completion), but does not reconstruct the intervening years of the coverage trend and has no original computation or corpus check of its own.

## Severity check

askamerica ranks second, above everyman — no severity flag. Its "not checkable from this corpus" framing is accurate and appropriately humble given the genuine data gap; it is not a case of askamerica underperforming everyman despite having the connector.

## Recipe / resolution check

Real, confirmed corpus gap — already filed as `kenstott/govdata-ops#662` (measles/NNDSS + SchoolVaxView sourcing gap) during this run, per `askamerica-comparative-eval` Step 5/6 discipline (verified live, proper labels, `status:open`). No further resolution attempted here since the gap is a genuine missing-pipeline issue, not a fixable recipe/instruction problem.
