The Paubox HIPAA Breach Report for November 2026 article's claims about October 2026 breach counts, people affected and year-over-year comparisons are balanced and accurate.

**Rating: 0 Pinocchios (factual accuracy).**

**Warehouse check:** search_catalog has no HIPAA or HHS OCR breach table (unmatched terms: hipaa, breach). I verified against independent sources instead. The HHS OCR portal is JavaScript-rendered and returned no data. I recomputed every total from the Google Sheet Paubox links, which is a derivative copy of HHS data, and cross-checked against Paubox's own 2022-2025 reports and HIPAA Journal.

**Claims, all true**
- Network server breaches affected 10,106,558 people. Recomputed exactly.
- EMR breaches affected 64,728 people, up from 20,081 in 2025. Recomputed, and the 2025 report confirms 20,081. "Most common" is loose wording, since this is a ranking by people affected.
- Other breaches affected 34,828 people, third behind network server and EMR. Recomputed.
- There were 27 network server breaches, up from 22 in 2025. Other had 5 breaches and email had 4. Recomputed. Network server was the most frequent type in every year from 2022 to 2026 (42, 28, 35, 22, 27).
- Email breaches fell to 4 from 6 in 2025 and 15 in 2024. Confirmed. The 2024 figure was a spike (16, 8, 15, 6, 4).
- Network server people affected were the highest in five years (central claim). The five Octobers are 1.98M, 1.15M, 5.13M, 1.42M and 10.11M, so 2026 is the highest and about twice the prior high.
- iRhythm was the largest breach at 8,179,772 people. This matches the sheet. SecurityWeek returned 403, so the corroboration comes from search summaries only.
- COPC was second at 1,351,650 people. HIPAA Journal's Nov 12 update confirms the HHS portal figure.
- 10,116,362 people across 34 breaches. The sheet has exactly 34 rows summing to that total.

**Misleading context (not counted in the rating)**
1. **One breach drives the headline.** iRhythm is 80.9% of the total, and iRhythm plus COPC is 94%. Without iRhythm, network server people affected would be about 1.93M, well below October 2024's 5.13M. The piece never says the five-year high is a single-incident story.
2. **The month looks partial.** All 34 submission dates fall between 10/01 and 10/16/2026, yet the figures are compared with full prior Octobers. HIPAA Journal says COPC reached the portal around Nov 12.
3. **Unresolved date conflict.** The sheet dates iRhythm at 10/03/2026, while one search summary says the HHS portal listed it Oct 29.
4. **Breach month versus posting month.** iRhythm's intrusion was in June 2026 and COPC's in August 2026. The report counts by HHS submission or posting date, not when the attacks happened.
5. **Loose terms.** "Attack vector" really means breach location, and multi-location breaches are counted under every location.
6. **Commercial context.** Paubox sells email security and closes with a pitch, while the data shows email as a minor vector this month (4 incidents, 7,649 people).

**Not verified:** the HHS OCR portal itself, because it would not render.

Report saved at /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q7/askamerica/2026-11-19/report.html.

Sources:
- [Paubox linked sheet (CSV)](https://docs.google.com/spreadsheets/d/1fZ6AjyegRYj50k17MLB0xMjn5OjXCOaR9cMJ87tpRMY/export?format=csv&gid=0)
- [Paubox Nov 2025](https://www.paubox.com/blog/hipaa-breach-report-for-november-2025)
- [Paubox Nov 2024](https://www.paubox.com/blog/hipaa-breach-report-for-november-2024)
- [Paubox Nov 2023](https://www.paubox.com/blog/hipaa-breach-report-for-november-2023)
- [Paubox Nov 2022](https://www.paubox.com/blog/hipaa-breach-report-november-2022)
- [HIPAA Journal on COPC](https://www.hipaajournal.com/central-ohio-primary-care-physicians-data-breach/)
- [SecurityWeek on iRhythm (403 on fetch)](https://securityweek.com/hhs-filing-puts-irhythm-data-breach-toll-at-8-2-million)
