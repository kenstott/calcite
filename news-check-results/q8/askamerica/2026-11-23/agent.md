The Inquirer article's claims about Camden's poverty rate, household income and local hardship are too few to rate; 9 claims could not be confirmed.

Source: https://www.inquirer.com/south-jersey/camden-poverty-rate-census-20261119.html (loaded in full).

## Verdict
**Pinocchio rating: 1 (analyst judgment; the engine's scorer returned no rating).** One checkable fact is true. The headline claim could not be reproduced at its source, and the article omits a key caveat.

## Why the headline is unconfirmed
- The article says Camden city's poverty rate fell to 24.9% in 2025, the lowest since at least 2010.
- The warehouse has no city-level ACS table (`census.saipe_poverty`, `acs_poverty` and `acs_income` are state and county only).
- The 2025 table could not be read. data.census.gov returned an empty page, the Census API requires a key, and QuickFacts returned 403.
- Census Reporter's ACS 2024 1-year data gives Camden city 23,632 of 69,729 below poverty, which is 33.9%. Camden County is 61,129 of 528,535, which is 11.6%.
- Five-year ACS values for the city are 28.5% (2019-23) and 30.2% (2020-24).
- A fall from 33.9% to 24.9% is 9 points in one year. The 2024 city poverty count carried a margin of error of about 3,900 people, roughly 5.6 points. The article never mentions sampling error, so "lowest in 15 years" is stated more firmly than one noisy single-year city estimate supports.

## Claims
| Claim | Result |
|---|---|
| City poverty 24.9% in 2025, lowest since 2010 (central) | Unsupported. The 2025 table was unreadable; 2024 city rate was 33.9%. |
| County poverty 9.9% in 2025 | Unsupported but plausible. Warehouse SAIPE 2024 is 11.4%; ACS 2024 1-year is 11.6%. |
| City poverty above 40% multiple times in the 2010s | Unsupported. No 2010s city 1-year values were retrievable. |
| 2025 federal poverty line $32,150 for a family of four | **True.** The Federal Register 2025 guidelines list $32,150 for four persons. |
| About 76% of households above poverty but unable to cover basics (United Way ALICE, 2024 data) | Unsupported. unitedforalice.org returned 403 and the warehouse has no ALICE data. The wording may misstate what the 76% measures. |
| City median income up 8% since 2010 in real terms, like the county, versus 14% nationally | Unsupported. No city data. County median went from $60,976 (2010) to $88,755 (2024) nominal, roughly +10% real; national 2025 median is $84,780, consistent with about +14% real. |
| Camden median $45,900 in 2025, ranked 10th among cities of 65,000 or more | Unsupported. No place-level income source reachable. |
| Income peaked at $47,200 in 2021, then fell about $10,000 in two years | Unsupported. Against $45,900 in 2025 it implies a trough near $37,000, which is possible in a noisy city sample but unconfirmed. |
| Cathedral Kitchen: 50 years, about 6,000 people a month, up to 800 in a day | Unsupported. Its site confirms service since 1976, up to 700 guests daily and 3,000 meals delivered monthly; the 6,000 and 800 figures are not on the page. |
| Mayor's office did not respond to requests | Unsupported. Reporter's account of private communications. |
| Quotes from Flewelling ("a really low bar"), Golderer and Noonan on policy | Not checkable here. These are opinions. |

## Materially misleading or missing
1. No margin of error is given for a one-year city estimate that moved 9 points.
2. The implied drop from 33.9% to 24.9% is not questioned. The smoother 5-year series (28.5%, 30.2%) shows a much flatter decline.
3. The 76% ALICE figure may mean poverty plus ALICE households combined, not only households above the poverty line.
4. Positive: the article does give the right "still struggling" context with income and benefit-cliff points.

## Limits of this check
- City-level ACS is not in the warehouse; Census sources were blocked or key-gated, so most city figures are unsupported rather than false.
- Chart: Camden County SAIPE poverty rate 2010-2024 and Camden city ACS estimates, in the saved report.

## Sources
- Census Reporter ACS 2024 1-year B17001: https://api.censusreporter.org/1.0/data/show/latest?table_ids=B17001&geo_ids=16000US3410000,05000US34007
- Federal Register 2025 poverty guidelines: https://www.govinfo.gov/content/pkg/FR-2025-01-17/html/2025-01377.htm
- Cathedral Kitchen: https://cathedralkitchen.org/
- Census ACS 2025 release page: https://www.census.gov/programs-surveys/acs/news/data-releases.2025.html
- Warehouse: `census.saipe_poverty`, `census.acs_income`, `census.acs1_income`
- Saved report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q8/askamerica/2026-11-23/report.html
