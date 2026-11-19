# q7 judge — paubox-hipaa-breach-report-october-2026-network-server-highest-five-years (2026-11-19)

Rank: [askamerica, expert, everyman]

Story: Paubox, 2026-11-15, "HIPAA breach report for November 2026" (October 2026 HHS OCR filings). 34 breaches, 10,116,362 individuals; network-server breaches affected 10,106,558, "the highest in five years".

The warehouse has no HHS OCR breach data, so every persona worked from the web. Paubox's own Google Sheet of the HHS data reproduces every figure, and both askamerica and expert recomputed from it.

**askamerica (#1).** Recomputed every total from the sheet (34 rows, 10,116,362). It verified the five-year claim against all five prior-October Paubox reports (1.98M, 1.15M, 5.13M, 1.42M, 10.11M). Its best finding is one nobody else made: every submission date in the sheet falls between 10/01 and 10/16, so a half month is being compared with full prior Octobers. It also notes counting by HHS posting date, not intrusion date (iRhythm June, COPC August), and a date conflict for iRhythm (10/03 in the sheet against Oct 29 in one summary). It shows iRhythm is 80.9% of the total (network server about 1.93M without it), flags the loose "attack vector" wording and Paubox's commercial interest. Engine rating 0, with the misleading context listed separately.

**expert (#2).** Same recomputation, plus three points askamerica lacks. The by-type counts (27+5+4+3) exceed 34 because one breach can list several locations. EMR is called "second most common" though it has the fewest breaches. And "accessed" overstates HHS's "affected". It gives the actual attack mechanisms (social engineering, voice phishing) to show "attack vector" is wrong. The 2022 comparison returned 404, so "five years" is only checked against three. Rating 1.

**everyman (#3).** Confirmed iRhythm and approximately COPC. It spotted the 36-against-34 count mismatch but read it as an inconsistency rather than overlapping categories. It could not reach the HHS data, so the totals and comparisons stay not checkable.

**What would change it.** askamerica and expert are close. Each found a real problem the other missed. askamerica's partial-month finding undercuts the article's central comparison; expert's points are about wording and categorisation. That is why askamerica is first. If the sheet's dates turn out to be incomplete rather than the portal's (the possibility the iRhythm date conflict raises), the partial-month point weakens.

Severity check: askamerica above everyman. No defect.

Claim shape: no attributed quotes; all author claims. Correct. Report and agent.md agree at 0 Pinocchios.

Gap audit: "warehouse has no HIPAA or HHS OCR breach table". Filed govdata-ops#1360 (HHS OCR breach portal 2009-; type:sourcing, kind:gap, schema:health + schema:cyber_threat, status:open).

## Recipe check

Nothing an unaided persona did needs warehouse-side teaching; the data is not in the warehouse yet (#1360). No recipe. askamerica ranked #1.
