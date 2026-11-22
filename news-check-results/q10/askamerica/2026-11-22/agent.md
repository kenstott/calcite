The FOX 4 article's claims about CDC flu activity, state flu levels and hospitalizations are accurate.

**Rating: 0 Pinocchios.**

**Method.** I fetched the article, then checked it against the CDC FluView Week 44 report (week ending 2026-11-07, released 2026-11-13), which I fetched directly. In the warehouse I queried health.cdc_resp_net_rates (FluSurv-NET weekly hospitalization rates) and health.cdc_ari_activity_state. The warehouse has no table for lab test positivity or the ILINet map, so those were checked against CDC directly.

**Claims**
- Test positivity of about 11% and rising: TRUE. CDC reports 5,911 of 52,287 specimens positive, or 11.3%, up from the prior week.
- More than 3% of doctor visits for flu-like illness: TRUE. ILINet shows 3.2%, above the 2.9% baseline. The piece correctly describes these as flu-like symptoms, not confirmed flu.
- Louisiana as the only state in the Very High category: TRUE. CDC's table shows 1 Very High jurisdiction, and secondary coverage names Louisiana. The CDC map itself is an image, so the state names come from that coverage.
- "Six states" at high levels, with SC, TX, CA, TN and OR also High: MOSTLY TRUE. CDC counts 1 Very High plus 6 High, or 7 jurisdictions. The piece omits Washington. The within-High "ranking" is not a CDC ranking.
- 2.6M illnesses, 36,000 hospitalizations, 2,100 deaths: TRUE. These are CDC preliminary estimates, stated as "at least".
- Five pediatric deaths: TRUE. CDC counts these separately from the 2,100 estimate, so "five of the deaths" is loosely worded.
- Dr. Anna Durbin's claim that mid-October flu hospitalizations were the highest in 15 years: MOSTLY TRUE.
  - She actually said it was the highest early-season mid-October rate since 2010-11, for the week ending Oct 17.
  - Warehouse FluSurv-NET shows 1.6 per 100,000 on 2026-10-17, against 0.6 in 2022-23 and 0.1 to 0.3 in the other seasons loaded.
  - The table starts at 2018-19, so the pre-2018 comparison rests on Durbin citing CDC. I confirmed her wording at Chief Healthcare Executive.
- "Flu season is starting a bit earlier": TRUE. CDC says activity is higher than typical for this time of year.
- Durbin's "it's really important to get vaccinated": opinion, not checkable here.
- CDC recommends an annual flu vaccine for everyone 6 months and older: TRUE.
- Flu activity is on the rise across the country: MOSTLY TRUE. CDC says indicators are rising in many parts of the country, not uniformly; positivity fell in Region 9. The warehouse weekly hospitalization rate went 2.7 (Nov 7) then 2.3 (Nov 14). The warehouse ARI map for Nov 14 (an all-virus metric) shows 8 Moderate jurisdictions and none High.

**Flags (do not change the rating)**
- The piece omits CDC's assessment that severity is low across all ages.
- The state map is based on ILINet, which counts any fever-plus-cough illness, not confirmed flu.
- At the Nov 5-7 week, 2022-23 (2.5 per 100,000) was nearly as high as this season (2.7). The early start is notable but not unprecedented in the 2018-19 onward data.

Report: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q10/askamerica/2026-11-22/report.html

Sources: [CDC FluView Week 44](https://www.cdc.gov/fluview/surveillance/2026-week-44.html), [Chief Healthcare Executive](https://www.chiefhealthcareexecutive.com/view/what-to-watch-this-respiratory-season-from-h1n1-to-mrna-vaccines), [Latin Times](https://www.latintimes.com/seven-states-high-flu-activity-early-season-601368)