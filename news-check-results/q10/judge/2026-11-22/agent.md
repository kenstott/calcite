# Judge: news q10, FOX 4 "Flu activity climbs nationwide, with 6 states reporting high levels, CDC says" (2026-11-19) (2026-11-22)

Rank: [askamerica, expert, everyman]

Story claims: CDC week 44 (ending Nov 7) shows flu rising; six states at high levels, Louisiana the only "very high"; about 11% of tests positive; more than 3% of visits for flu-like illness; at least 2.6M illnesses, 36,000 hospitalizations, 2,100 deaths, five pediatric deaths; Dr. Anna Durbin (Johns Hopkins) says the mid-October hospitalization rate was the highest in 15 years; CDC's annual vaccine recommendation.

The askamerica answer judged here is a same-day rerun on the engine-dev jar after a `recipes.json` change (the resolution described below). It is the answer saved under `q10/askamerica/2026-11-22`. The ranking reasons only from the three current answer files.

## What each answer found

- **askamerica (0 Pinocchios).** It matched all national figures to CDC FluView week 44, which it fetched directly: 11.3%, 3.2%, 2.6M / 36,000 / 2,100, five pediatric deaths. It found CDC counts seven jurisdictions at high or very high, not six, with Washington omitted. It tested Durbin's hospitalization claim against the warehouse (`health.cdc_resp_net_rates`, FluSurv-NET weekly rate, overall population): 1.6 per 100,000 in the week ending Oct 17, 2026, against at most 0.6 in the same week of every season loaded since 2018-19. It said plainly that the pre-2018 part rests on Durbin citing CDC. It added context no other arm has: by the Nov 5-7 week, 2022-23's rate (2.5) was nearly as high as this season's (2.7), so the early-season record does not imply an unusual season so far. It also noted that CDC rates severity as low and that the state map counts flu-like illness, not confirmed flu.
- **expert (1 Pinocchio).** It matched the same national figures to FluView week 44 and found the seven-vs-six state count. It confirmed Durbin's claim from her Oct 29 briefing ("highest since 2010-11"). It noted that ILI counts any respiratory pathogen. It added the next week's FluView (week 45: 3.8% ILI, 11 jurisdictions high or very high, 3.7M illnesses, 50,000 hospitalizations, 2,800 deaths, 7 pediatric deaths), showing the article was already stale. It could not verify the state names.
- **everyman (1 Pinocchio).** Same national matches and the seven-vs-six finding, with Washington from secondary reports. It confirmed "15 years" via Becker's (2.1 per 100k, highest for that point since 2010-11). It could not find Durbin's attribution.

## Ranking reasoning

1. **askamerica.** It is the only arm that tested the article's one superlative claim against measured data, covering every season the warehouse holds. It also produced the most useful framing finding: 2022-23 caught up by early November, so "highest in 15 years" describes timing, not severity. It is candid about the pre-2018 limit.
2. **expert.** It verified Durbin's claim against the primary briefing (back to 2010-11, which askamerica's table cannot reach) and showed the article was a week stale. It is strong, but it ran no original test.
3. **everyman.** Matches on national figures, with secondary confirmation of "15 years". Its Durbin attribution was unconfirmed.

This is close between askamerica and expert. Expert's briefing source covers the full 15-year span, while askamerica's data covers 8 seasons. I put askamerica ahead because its data test plus the 2022-23 catch-up point tells a reader more about what the claim means than the confirmation alone.

## Severity check

askamerica ranks above everyman, so there is no severity flag.

## Rating shape and gap audit

- `claims.json`: `author_claims`, one `fidelity`, and `subject_claims` with `speaker: "Dr. Anna Durbin"` for her three statements. That is the correct shape. Rating: 0 in both report.html and agent.md. On its first pass, askamerica grouped the Durbin claim as `author_claims`. I resumed that subagent to regroup it, and re-checked the file afterwards (3 `subject_claims` with speaker set) before the rerun replaced it.
- Gaps: none to file. Flu-specific state ILI levels: `health.cdc_ili_activity_state_archive` is an archive. CDC FluView sourcing was already handled under govdata-ops#1234 (closed). FluSurv-NET is loaded and current.

## Resolution (askamerica not #1 on its first pass)

On its first pass today, askamerica graded the hospitalization claim from secondary reporting and never queried `health.cdc_resp_net_rates`. That table held FluSurv-NET weekly rates from 2018-19 through the week ending 2026-11-14, so the miss was a reachable pattern, not a data gap. Resolution: **recipe**. I broadened the existing `fact-check-a-news-article` recipe (no new neighbour) with step 10, which says respiratory-disease surveillance comparisons must be tested against `health.cdc_resp_net_rates` at the same season week across all loaded seasons, and that `cdc_ari_activity_state` cannot test a flu-specific claim. I also added triggers ("flu season", "flu activity", "hospitalization rate", "highest in 15 years", "earliest flu season").

Step 6 checks:
- Built with `:askamerica-engine:shadowJar`.
- Verified that `recipes.json` in the jar contains "news q10, FOX 4 flu".
- `smoke_test.py` returned OK.
- Installed atomically (backup `askamerica-engine.jar.bak-2026-11-22-flu-recipe`, then `cp` to a temp file and `mv`) to `~/.askamerica/engine-dev/`, and re-verified the content there. `search_catalog` against the installed jar answered.
- Rerun: the calls log shows `describe_table health.cdc_resp_net_rates` and two FluSurv-NET queries, so the behaviour changed as intended.
