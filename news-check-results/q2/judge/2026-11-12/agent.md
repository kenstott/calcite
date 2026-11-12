# q2 judge: Stateline "Collections are coming for millions of student loans in default"

Rank: [askamerica, expert, everyman]

The story ran Nov 12, 2026 (Tim Henderson). Judged 2026-11-12. stateline.org and newsfromthestates.com both return HTTP 403 to my fetch tools, so I could not read the article; I worked from the claims as the three checks quote them, and verified against the NY Fed Liberty Street post (May 2026, read directly) plus search coverage (AP/Fortune, The College Investor).

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 1 | Warehouse edu.fsa_loan_portfolio_status; NY Fed chart workbook; ED Jan 16 release via news; AP via Fortune |
| expert | 1 | FSA GENERAL-26-57, Newsweek, ed.gov release, NY Fed Liberty Street post |
| everyman | 1 | None primary; Forbes, Substack, AP analysis, search snippets of CRS |

## Assessment
- **Correct verdict: 0 Pinocchios (not verifiable as shaded).** Every claim that can be traced matches; nothing found is false or shaded by the outlet. The unverified items (state ranking, vocational-school figure) are unconfirmed, not contradicted.
  - **9M+ in default, about $234B: true.** askamerica's warehouse gives 9.3M and $234.1B at 2026-06-30; expert cites FSA 9.3M/$234B; AP analysis (July 2026) gives about 9.5M and $233.3B.
  - **Collections paused again in January: true.** ED announced the delay on 2026-01-16; collections had restarted May 2025, so "again" is fair.
  - **NY Fed state shares: consistent.** The NY Fed post states "at least 10 percent of borrowers" in Louisiana, Mississippi, Alabama, Georgia and South Carolina, and "at least 4 percent" in the lowest states. These are lower bounds from a map image, so 13% for LA/MS and a 5% floor are compatible with the text. askamerica reports the chart workbook shows 13/13/5; I could not open the workbook, so that is plausible, not independently confirmed. Expert's "not confirmed" and "4% minimum vs 5%" treat a threshold as a contradiction, which it is not.
  - **State ranking: unconfirmed.** AP has Mississippi highest at 28.3%, Louisiana near the top, New Mexico within the top 15. The Massachusetts/NH/Utah/Wisconsin 15% floor is not found anywhere online.
  - **Forbearance "expired last year": loosely fine.** Payments resumed Oct 2023, on-ramp ended Oct 2024, first defaults 2025:Q4; SAVE forbearance still covers about 8M, so the phrase is loose but not wrong.
  - **Vocational schools, late-payment rates up to two-thirds: unverified.** No check found the 2025 federal report.
  - **Collections "coming" next year:** a hedged expert prediction; the body states no date is set. The headline is stronger than the body, but this is ordinary headline compression.
- **askamerica (1st):** most claims verified against data, correct handling of unverifiable items.
  - It checked the headline count and dollars in the warehouse, the Washington Sun report, older-borrower claim and the NY Fed state figures, and separated "unsupported" from "false".
  - It flagged useful context (9M is a cumulative stock including about 5.3M pre-pandemic defaults; NY Fed says the largest wave has passed).
  - Weaknesses: rated 1 where 0 is correct, apparently for the unsupported vocational figure and loose timing; the report's headline sentence is garbled.
- **expert (2nd):** correct on the core, terse, and mis-flagged the NY Fed figures.
  - It confirmed 9.3M/$234B, the Jan 16 pause and the 1.0M + 2.6M NY Fed default restart with named sources.
  - Weaknesses: treated "at least 10%" and "at least 4%" as conflicting with 13% and 5%, which they are not; left the Washington Sun report and ranking unchecked; rating 1 is one step off.
- **everyman (3rd):** verified the least and inflated framing concerns.
  - It confirmed the headline numbers via secondary sources and the January pause, but all state-level, older-borrower, NY Fed and vocational claims were left "not verified". It never reached the NY Fed post.
  - It stated the CRS 7.8M figure, an older date, as evidence the count "moves", and called the headline's framing stronger than the evidence, which weighs against the piece without a verified finding.
  - Its rating of 1 rests on what it could not check.

## Recipe check
Not assessable from the three files; no tool logs were read. The apparent gap for askamerica is that the NY Fed LA/MS and every-state claims were checked against the workbook rather than the posted text, so the threshold wording was not cross-read. A reusable step: when a primary page states thresholds ("at least X"), reconcile them against the workbook values before stating TRUE or "not confirmed".

## Severity invariant
**Not triggered.** askamerica (1) is not rated more severely than everyman (1); both are equal and one step above the correct verdict of 0.
