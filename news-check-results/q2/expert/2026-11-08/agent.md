# Fact-check: Medical Daily, "Overdose Deaths Climbed in 430 U.S. Counties While the National Toll Fell 12% to Nearly 68,000" (Nov 5, 2026, Joseph James)

## Verdict: 1 Pinocchio (minor imprecision, not materially false)

Medical Daily's article fetched fine. It is an aggregation of an Axios analysis of CDC provisional data (Axios, Nov 3, 2026) plus Axios Cincinnati. The Axios page returned HTTP 403, so I checked Axios's figures through search-engine excerpts of it and then tested them against CDC data directly.

**Main caveat.** The headline "rises in 430 counties" is accurate to the source, but only if read narrowly. It means 430 of about 1,624 classifiable counties. Those counties have about 10 or more deaths in at least one period. Roughly 1,500 counties, about half the country, cannot be classified. I could not reproduce the 430/842 split exactly, and the article never says how suppressed cells were handled.

**Claim-by-claim**

| Claim | Verdict | Source / confidence |
|---|---|---|
| Overdose deaths rose in 430 counties (12 months ending Mar 2025 vs Mar 2026) | Not reproduced. Direction matches, exact count unverified. | CDC county dataset gb4e-yj24 (data as of 2026-10-04). Using only counties with both values published (n=1,175), I get 233 up, 590 down, 352 flat. If a suppressed 1-9 cell compared with a published cell of 10 or more counts as a change, I get 320 up and 773 down. The 352 flat count matches the reported figure exactly. Axios's 430/842 needs an unstated rule for the suppressed cells. Medium confidence. |
| Fell in 842 counties; 352 unchanged | Same as above. | The 352 flat is confirmed. 842 is not reproduced. |
| National toll fell about 12% to nearly 68,000 | True. | CDC VSRR national dataset xkb8-kh2a: reported 77,064 (Mar 2025) to 67,576 (Mar 2026), -12.3%. Predicted 77,306 to 68,080, -11.9%. High confidence. |
| About 9,500 fewer deaths than the prior year (via Axios) | True. | Reported difference is 9,488. The predicted-value difference is 9,226. High confidence. |
| The earlier MedicalDaily report put the estimate at 67,798, a 12.3% decrease | Not checkable. | The current dashboard shows 67,576 reported and 68,080 predicted. The 12.3% matches the reported series, but 67,798 matches neither. It is probably from an earlier data vintage. Low confidence. |
| Rio Arriba, NM, about 173 deaths per 100,000, the highest calculable rate | Plausible and consistent. | CDC county data gives 69 deaths for the 12 months ending Mar 2026, up from 52. With a population of about 39-40k that is about 174 per 100k. I did not check the "highest" ranking across all counties. Medium-high confidence. |
| Several neighboring Arizona and New Mexico counties saw increases | Rio Arriba is confirmed up (52 to 69). Other counties not checked. | CDC county data. |
| CDC withholds counts of 1-9, so trends are unavailable for nearly half of counties | True. | CDC footnote on the county dataset. 1,248 to 1,967 of 3,142 counties are unclassifiable depending on the rule. |
| Butler County, OH, fell from 104 to 73 (-30%) | True. | CDC county data: 104 to 73, -29.8%. High confidence. |
| Hamilton County, OH, fell from 200 to 147 (-27%) | True. | CDC county data: 200 to 147, -26.5%. High confidence. |
| Campbell County, KY, rose from 13 to 17 (+31%) | True. | CDC county data: 13 to 17, +30.8%. High confidence. |
| Of 42 Ohio counties with a measurable trend, 30 fell, 9 rose and 3 were flat | Not checked. | Attributed to Axios Cincinnati. I did not tally Ohio. |
| Butler County coroner data: peak of 236 in 2017, 87 in 2025 | Not checkable here. | Local coroner data. |
| Grant Baldwin quote: "The most useful data does more than show us what has happened; it helps us decide what to do next." | Not checkable. | I could not locate the original. |
| Causes of the national decline are unsettled; the CDC cites naloxone, treatment and the drug supply | Consistent with the Axios excerpts. | Medium confidence. |
| Arizona researcher on fentanyl, methamphetamine and long rural drive times (Arkansas Public Media) | Not checkable. | |
| Safety resources: SAMHSA 1-800-662-4357, 988, FindTreatment.gov | Not checked against the sources. | These are the standard published numbers. |

**Potentially misleading framing**
- The headline stresses rises, but the article's own figures show that for classifiable counties, deaths fell in about twice as many as they rose. The national total fell about 12%.
- The article never says the 430 comes from a journalistic tally rather than a CDC statistic, or how suppressed cells were treated.
- Small counts swing wildly. Campbell County's +31% is 4 deaths.
- The data are provisional and "Underreported due to incomplete data", and the latest period is the most affected. Medical Daily does flag provisionality.
- It is county-level ecological data. Rio Arriba's rate is a count over population with a small denominator. It says nothing about individuals.

**Not done:** I did not read the Axios text itself (403). I did not verify the Ohio tally, the coroner series, the Baldwin quote or the Rio Arriba ranking.
