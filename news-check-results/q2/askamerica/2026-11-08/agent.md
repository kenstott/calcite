# The MedicalDaily article's claims about county overdose deaths, the national decline and Cincinnati-area counties

**Rating: 1 Pinocchio.** Most of the article's figures check out against CDC data in the warehouse. The headline county tally (430 counties up, 842 down) comes from Axios and does not reproduce.

## Claim-by-claim

| Claim | Warehouse result | Verdict |
|---|---|---|
| National toll fell about 12% to nearly 68,000. The article also gives 67,798, down 12.3%. | The warehouse (data as of 2026-10-04) shows a predicted 68,080 against 77,306, which is -11.9%. The reported count is 67,576 against 77,064, which is -12.3%. 67,798 is an earlier CDC release. | True |
| Deaths rose in 430 counties and fell in 842. | Counties with an unsuppressed count in both windows: 233 rose, 593 fell, 352 equal, out of 1,178 determinable. The Axios page returned 403, so I could not see its method. | Not reproduced (partially/mostly false) |
| Trends could not be determined in nearly half of counties because CDC suppresses counts of 1-9. | The suppression rule is confirmed by the table footnote. In March 2026, 1,629 of 3,144 counties (52%) were suppressed. 1,970 of 3,148 (63%) lacked a count in at least one of the two windows. | Mostly true (understates the share) |
| Ohio: of 42 counties with a measurable trend, 30 fell, 9 rose and 3 stayed the same. | 38 Ohio counties have counts in both windows: 28 fell, 7 rose, 3 equal. | Partially false (close) |
| Butler fell 30% (104 to 73), Hamilton fell 27% (200 to 147), Campbell KY rose 31% (13 to 17). | Exact match: -29.8%, -26.5%, +30.8%. | True |
| Rio Arriba NM had about 173 deaths per 100,000, the highest calculable rate. | 69 deaths / 40,070 residents (ACS 2024) = 172.2 per 100,000. It is the highest among counties with 1,000+ residents and an unsuppressed count. | True |
| Butler County coroner data: peak of 236 in 2017, 87 in 2025. | The warehouse has no coroner data. The Axios source returned 403. | Not checkable here |
| A University of Arizona researcher cited potent fentanyl and meth and long rural drive times. | NPR (via Alabama Public Radio, May 28 2026) quotes Beth Meyerson saying this. | True |
| Grant Baldwin quote on useful data. | The CDC release returned 403. The earlier MedicalDaily article carries only a shorter version of the quote. | Not checkable here |
| Naloxone is sold over the counter. Buprenorphine and methadone lower death risk. The SAMHSA helpline is 1-800-662-4357. | General public-health facts, not warehouse measures. I did not re-source them. | Not independently re-sourced |

## Exclusions disclosed

My county queries used the predicates `a.c IS NOT NULL` and `b.c IS NOT NULL`. They excluded every county whose count is suppressed in either window. That removed 1,970 of 3,148 counties and left 1,178 for the up/down tally. The Ohio tally excluded suppressed Ohio counties the same way, leaving 38. The March 2026 suppression share used `provisional_drug_overdose IS NULL` to count the 1,629 of 3,144 suppressed counties, not to drop them. The Rio Arriba rank excluded suppressed counties and counties under 1,000 residents. Including suppressed counties gives no trend to compute, so there is no alternative headline figure.

## Misleading framing

- The headline treats "430 counties rose" as established, but I could not reproduce it. It also covers only counties with reportable counts, which skew larger.
- The article does caveat small numbers (13 to 17) and provisional data. That tempers the framing.
- The windows are the 12 months ending March, so "the past year" is not calendar 2025.

## Limits

- Both Axios pages, the CDC press release and the CDC dashboard returned HTTP 403 to this tool.
- I read NPR via APR and the article's linked MedicalDaily pages. None contradicts the checked figures.
- Warehouse figures are the 2026-10-04 vintage. Axios may have used a different vintage or a different rule for counties with equal counts.

## Sources

- Warehouse: `health.cdc_county_overdose_deaths`, `health.cdc_state_drug_overdose_deaths`, `census.acs_population`
- NPR via APR: https://www.apr.org/2026-05-28/u-s-street-drug-deaths-keep-dropping-but-some-western-states-see-deadly-overdose-surge

Report saved at /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q2/askamerica/2026-11-08/report.html
