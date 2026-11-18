# Judge — news q4 dotnews-boston-public-schools-enrollment-21-percent-decade

**Story:** "BPS enrollment continues to decline; nearly 2,000 fewer students this year" (Dorchester Reporter, 2026-11-12). Claim of interest: Superintendent Skipper says enrollment fell about 21 percent in a decade (roughly 5,000 students and 9 percent over the last five years).

Rank: [askamerica, expert, everyman]

Judged 2026-11-17 on three answers produced the same day (this resume run).

## Why
- **askamerica (1st):** engine rating 2 Pinocchios, 10 claims. Pulled Boston and Seattle district enrollment from `edu.ccd_districts` (NCES CCD, 2013-2024), which let it test both benchmarks the piece uses. It found two errors no other persona found: the "slightly lower than BPS" pre-pandemic Seattle comparison is backwards (CCD 2019: Seattle 55,986 vs Boston 50,480), and the 11,014-since-2017-18 figure implies a 2017-18 base CCD does not show. It graded the 21 percent decade claim partially false against WBUR (about 9,000) and CCD (14 percent, 2016-2024). It also flagged the stale current Seattle figure (48,957) and the missed December projection (1,300 vs 1,978 actual). It did not verify the grade-band figures or the five-year 9 percent figure, and says so. The SQL is logged in calls.jsonl but the values are not, so the CCD figures rest on its report; the direction (Seattle larger than Boston before 2020) matches SPS's own published October 2019 count.
- **expert (2nd):** 2 Pinocchios, 9 claims. Reached the same verdict on the decade figure (16 percent from the Globe's 53,263) and the stale Seattle number, and noted the missed projection. It misread the piece: it treated "roughly 5,000 students" as the decade figure and called the paragraph internally inconsistent, when the 5,000 and 9 percent are the five-year figures. It did not test the pre-pandemic Seattle comparison or the 11,014 figure.
- **everyman (3rd):** 1 Pinocchio. WebFetch returned only a summary of the article, so it never saw the 21 percent claim, the Seattle comparison or the 11,014 figure, and graded a piece it had not read in full. Its framing points (count definitions differ; the one-year framing hides a 9,000-student decade decline) are correct.

What would change the ranking: askamerica and expert are close on the decade claim. askamerica's Seattle-comparison catch, which came from the warehouse's CCD table, decides it. If the CCD figures in askamerica's report were wrong, expert would move up.

## Recipe check
No recipe change. askamerica used the warehouse table for the only outside benchmark in the piece. No other persona did anything askamerica missed that a recipe would teach.

## Resolution
Not needed (askamerica ranked first). Gap audit: CCD ends at 2024. 2025-26 is not yet published by NCES, so this is release lag, not a sourcing gap. Nothing filed.

## Shape
Reviewed. 8 `author_claims`, 2 `subject_claims` (speaker Mary Skipper), engine-computed rating. Compliant.

## Severity invariant
Not triggered.
