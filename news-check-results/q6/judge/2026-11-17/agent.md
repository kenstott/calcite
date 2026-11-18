# Judge — news q6 wgme-portland-maine-warm-start-november

**Story:** "Portland logs second-warmest start to November as drought lingers across Maine" (WGME). Claim of interest: third-warmest Nov 1-15 on record (48.0°F; 2022 warmest at 50.6°F), while the URL slug and title tag say second-warmest; 1.13 in of rain against a 4.25 in monthly normal.

Rank: [expert, askamerica, everyman]

Judged 2026-11-17 on three answers produced the same day (this resume run).

## Why
- **expert (1st):** 1 Pinocchio. Ranked against both the Jetport record since 1941 (tied 3rd/4th with 1969) and the full threaded Portland record since 1871, where 1938 (50.17°F) is second and 2026 is tied 4th/5th. Of the three, it alone flagged that setting 1.13 in so far against the whole-month 4.25 in normal is misleading (the to-date normal is about 2.0 in). It read the actual CPC 8-14 day outlook (50% above-normal temperature; precipitation leaning slightly above median) and found that the article's "typical" 43.1°F matches a 2006-2025 mean rather than the NWS 1991-2020 normal. It also counted 11 of 15 days above normal and 5 measurable rain days, not 6.
- **askamerica (2nd):** engine rating 1 Pinocchio, 12 claims. Queried `weather.ghcnd_daily` for the Jetport station (holds about 16 years) and ACIS from 1941 for the rank, getting the same tied 3rd/4th as expert's 1941 view and the same slug/headline mismatch. It used `weather.drought_monitor_weekly` at county grain to show the "Downeast eased to abnormally dry" sentence overstates the improvement (only 53.9% of Washington County and 41.6% of Hancock County were D0; the rest was still D1). That is a finding no other persona made. It missed the full-period record (1938) and the partial-month precipitation comparison. Its CPC fetch hit a stale cached page, so it graded the outlook unsupported.
- **everyman (3rd):** 1 Pinocchio, provisionally. It worked from a fetch summary of the article, left the temperature figures unchecked, and could not resolve second vs third.

What would change the ranking: askamerica's county-grain drought finding is real and specific. Expert's full-record rank, the misleading partial-period comparison and the CPC read are three findings to askamerica's one, and the precipitation point is the kind of framing problem a fact-check exists to catch.

## Recipe check
Yes. Expert did two things askamerica did not reach uncoached: (1) it compared a month-to-date total against the month-to-date normal rather than the full-month normal; (2) it ranked against the station's full period of record, the NWS threaded (ThreadEx) record, rather than only the years the warehouse table or a single-site record holds. Both are general methods for any "so far this month/year" or "Nth warmest/driest on record" claim, and neither is in `recipes.json`. askamerica called `find_recipe` for "fact-check news article", which returns `fact-check-a-news-article`, so that recipe is the one to broaden, not a new neighbor. The evidence comes from the unaided expert persona, which is the stronger kind.

## Resolution
Recipe: broadened `fact-check-a-news-article` with a step 8 covering partial-period comparisons and record ranks. It points to the warehouse first (`weather.ghcnd_daily`) and to ACIS StnData with the threaded station id (verified live: `PWMthr` returns Portland Area 1874-04-01 onward) for the full record. Rebuilt, smoke-tested and installed to engine-dev.

Rerun (Step 6.3, `news-check-results/q6/rerun/2026-11-17/`, askamerica only, same prompt except run_subpath): **confirmed.** The rerun called `find_recipe`, then queried ACIS StnData with `sid: PWMthr` from 1871 (8 calls in calls.jsonl). It ranked 2026 tied fourth on the 1874-2026 record (2022 50.60, 1938 50.17, 1975 48.23, 1969 48.00), graded the 1.13-of-4.25-inches comparison misleading against a Nov 1-15 normal of about 2.0 inches (56%, not 27%), and kept the 5-measurable-days finding. The original run had none of the first two. Not a rejudge: the frozen rank above stands.

## Shape
Reviewed. 12 `author_claims`, no speakers; the figures are the station's own data relayed by the reporter. The one "not checkable" item is a forecaster's judgment ("several days of soaking rain"). Compliant.

## Severity invariant
Not triggered.
