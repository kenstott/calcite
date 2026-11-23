# Judge - News q2 (WW, "Violent crime rose in Portland over past two years"), 2026-11-23

**Story:** Willamette Week, 2026-11-19, https://www.wweek.com/news/2026/11/19/violent-crime-rose-in-portland-over-past-two-years/. PPB person-crime tracker (city, NIBRS) set against FBI Oregon figures. Task: fact-check every claim, grade 0-4 Pinocchios.

Rank: [askamerica, everyman, expert]

All three personas ran fresh today. Fingerprints pass for everyman, askamerica and expert (given by the orchestrator). None of the three cites askamerica.ai.

## Answers in brief

| | everyman | askamerica | expert |
|---|---|---|---|
| Rating | 2 Pinocchios | 1 Pinocchio (engine-computed) | 1 Pinocchio |
| Claims graded | 9 listed; 6 "not checkable here" | 12+ in claims.json: 5 true, 1 mostly false, 7 unsupported | about 6 bullets; PPB figures "matched WW's reading", others "not independently checked" |
| Warehouse use | none | `crime.cde_offenses`, `crime.cde_reta`, `crime.cde_agencies` (9 cde_reta mentions in calls.jsonl) | none |
| Portland-vs-statewide mislabel | not found | found: the 2,600 property offenses and 1,500 vehicle thefts are Portland figures per KATU/FBI, graded mostly false | not found |
| Headline "violent" vs "person crime" | found | found | found |
| Mixed definitions (NIBRS person vs FBI violent) | found | found | found |
| 2026 Portland data | none | none; stated "no 2026 city data exists in the warehouse" | none |

## What I verified

- askamerica `calls.jsonl` (21 calls) shows the cde_reta queries at seq 7-11 and a catalog search for 2026 city data at seq 14, 16 and 17. The searches found nothing, and askamerica then stopped. It never ran `SELECT max(year)` or a 2026 filter on cde_reta for ori OR02602. Its SQL in claims.json filters `year IN (2024,2025)` and the 2023-2025 murder series.
- The orchestrator's live check shows cde_reta holds Portland PD for 2026 months 1-9. Jan-Sep 2025 vs 2026: murder 34 -> 20, simple assault 4,724 -> 5,188 (+9.8%), total assault 7,008 -> 7,503 (+7.1%). Aggravated assault is total minus simple: 2,284 -> 2,315 (+1.4%).
- Those numbers test several "unsupported" claims directly. Homicide down year to date: 34 -> 20, supported. Aggravated assault up about 1%: +1.4%, close. Simple assault driving the increase: +464 in Return A against the article's "more than 412" (a different category and period, but the same direction and size). Person crime up 6% year to date: not testable directly, though the assault components point the same way.
- askamerica's mislabel finding is genuine and checks out against its own sources. The KATU passage reads "In Portland, the FBI said the city reported about 2,600 fewer property crimes". The article labels the same figure "statewide". askamerica's arithmetic holds: -12.1% of about 90,500 is roughly 12,000, not 2,600. Its warehouse tie-out (Portland PD vehicle theft 5,492 -> 4,004, -1,488) fits "nearly 1,500".
- Neither unaided persona lists the 2,600 or 1,500 figures, so neither could have caught the mislabel.

## Ranking rationale

**1. askamerica.** It is the only run that found a factual error in the article, and it graded that claim mostly false using the primary release text plus a warehouse tie-out. It also tested the state figures against the warehouse (-7.3% vs the FBI's -7.9%, -24.3% vs -24.4%, burglary -20.0% vs -20.4%). Both unaided personas had only KATU's rounded figures. It shows its work, with SQL, warehouse values and a per-claim reason, and it flags the NIBRS vs UCR mixed bases. Its weakness is the one named below, so the lead is real but smaller than it should be.

Deduction: it graded all the 2026 Portland claims "unsupported" on the basis that "the warehouse has no 2026 city data". That statement is wrong. cde_reta carries Portland PD through September 2026. It never checked the table's latest month. Claims that were partly testable (homicide decreasing year to date, aggravated assault up about 1%, simple assault as the driver) should have carried warehouse values and a firmer verdict. Seven "unsupported" grades on the author's claims is a weaker answer than the data allowed. It also pushes the engine's rating toward the lower end for the wrong reason: unchecked claims count as neutral instead of being confirmed. I count this against askamerica on those claims, but not enough to put it behind a persona that missed the error and tested nothing.

**2. everyman.** A web-only agent that was honest about its limits (summarizing fetch, no tracker access, no Python). Its 2-Pinocchio rating is the closer of the two unaided calls once the mislabel is counted, and it gives the right reasons: a "violent crime" headline over "person crime" data, aggravated assault nearly flat, homicide and shootings down, the 2024 baseline against a decline since July, and a Portland-vs-state comparison mixing categories. It adds the point that reporting changes could move intimidation and simple assault counts. Six of its nine claims are "not checkable here", which leaves the central claim ungraded. It did not find the statewide mislabel.

**3. expert.** Same sources and the same headline/category insight as everyman, with a tidier report and dashboard. It rates 1 Pinocchio and writes "Every figure I could trace matched its source", which is true as stated but leaves the article's "statewide" label standing. It did not open the warehouse and did not test any figure beyond KATU. It concedes it never reached the PPB tracker and read the article only through a summarizing fetch. Extra equipment bought no extra evidence, so it sits below everyman by a small margin: its rating is lower and its tone more confident, with less to back it.

**What would change the ranking:** the everyman/expert order is a near tie. The expert's cleaner presentation and confidence calibration ("medium") could outweigh everyman's rating if a reader prefers the 1-Pinocchio call. For askamerica, a rerun that tested 2026 in cde_reta and graded the homicide, aggravated-assault and simple-assault claims from it would widen its lead substantially. If the mislabel catch were discounted (for example, if the FBI Portland release were found to use "statewide" loosely), askamerica's lead would shrink to the data work alone, and it could tie everyman.

## Severity invariant

askamerica ranks above everyman. No defect flag on rank. The 2026-data miss is still a defect in the answer and is handled below.

## Recipe check

**The miss.** askamerica stated "no 2026 city data exists in the warehouse" and graded the article's 2026 Portland claims "unsupported". `crime.cde_reta` (FBI Return A, agency-month) holds Portland PD (ori OR02602) for 2026 months 1-9. A single query would have yielded murder 34 -> 20, simple assault 4,724 -> 5,188 (+9.8%) and total assault 7,008 -> 7,503 (+7.1%) for Jan-Sep 2025 vs 2026. Neither unaided persona could have done this, so the evidence here is askamerica's own miss against warehouse truth, which is the weaker kind for a new recipe but a direct, verified defect in its method.

**Existing coverage.** I read `askamerica-engine/src/main/resources/recipes.json`. The recipe `fact-check-a-news-article` already governs this question type and was served on seq 1-2. Its step 6 says a missing table never makes a factual claim uncheckable, and it carries Return-A-free rules for tests of partial-period and trend claims. No step tells the caller to find the table's latest period before declaring a current-year gap. A search for "cde_reta", "city" and "police" found no recipe on city-agency crime. `live-api-pull-when-headline-figure-is-stale` covers a stale published figure, not a stale query window. So a new neighbor recipe would be redundant, and the right change is a new numbered step in `fact-check-a-news-article`.

**Resolution (one):** recipes.json change to the existing `fact-check-a-news-article`. Add a step: before grading any current-year or year-to-date claim "unsupported for lack of data", run `SELECT max(year), max(month)` on the candidate table, filtered to the agency or geography in the claim, and test the claim on the months present. For city crime claims, name `crime.cde_reta` (agency-month Return A; join `crime.cde_agencies` for the ori) with the Portland case as the measured instance: Jan-Sep 2026 held, askamerica claimed none. Add triggers "year to date", "through October", "police bureau", "person crime". Rebuild, install atomically and rerun q2 per Step 6 (not done in this judge pass).
