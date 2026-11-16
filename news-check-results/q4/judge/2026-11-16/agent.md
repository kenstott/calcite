# Judge — news q4 cbs-iie-foreign-students-down-9-percent

**Story:** Fewer foreign students studying at U.S. universities due to Trump administration policies, study shows (CBS News/AP, 2026-11-16)

Rank: [expert, askamerica, everyman]

Judged 2026-11-16 on three answers produced the same day. Close between the first two.

## Why
- **expert (1st):** 1 Pinocchio. Matched every IIE Fall 2026 Snapshot figure in the piece, including the two askamerica could not find: undergraduate -3% and OPT -6%, plus the 88% and 70% survey shares and the 809-institution base, read through PRNewswire's carriage of the release because IIE's page returned 404. Dated the $70,000 OPT fee proposal (Oct. 8) and the four-year-cap block (Sept. 14). Left the $2.4B NAFSA figure and Cleveland State's 46% unverified.
- **askamerica (2nd):** engine score 1. Confirmed the 9%, 15% and 15% from NYT and GBH coverage, and made the one finding nobody else did: Cleveland State's "46% in two years" spans one year in the article's own numbers, and another outlet reports 1,249 for fall 2025, not 1,542. Graded the OPT and undergraduate figures unsupported; they are in the release. Noted community-college international enrollment rose 3%.
- **everyman (3rd):** 1 Pinocchio from search summaries. Same $2.4B-against-$3.4B flag; no check of Cleveland State.

## Recipe check
No recipe applies. The warehouse had nothing to contribute on this story: no fall-enrollment table and no SEVIS series.

## Resolution
Issues, filed as the gap surfaced: kenstott/govdata-ops#1258 (IPEDS Fall Enrollment and Admissions, which would test institution-level international counts such as Cleveland State's) and #1259 (SEVIS student and OPT counts).

## Severity invariant
Not triggered.

## Shape
First delivery put all 10 claims in `author_claims`. The same subagent was resumed and re-published; the file now holds 6 fidelity, 5 author and 2 subject claims with speaker (re-checked in `claims.json`). Verdicts and the rating did not change.
