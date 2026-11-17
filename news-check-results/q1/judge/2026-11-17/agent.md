# Judge — news q1 mpamag-existing-home-sales-16-year-low-rates-since-2000

**Story:** "Existing-home sales fall to their lowest level since 2010" (Mortgage Professional America, 2026-11-12). Claim of interest: NAR's Lawrence Yun, rates "highest levels since 2000, aside from a single month in 2023".

Rank: [askamerica, expert, everyman]

Judged 2026-11-17 on three answers produced the same day.

## Why
- **askamerica (1st):** engine rating 1 Pinocchio. Checked the headline series against the warehouse rather than only against NAR: `econ.fred_indicators` EXHOSLUSM495S gives 3.75M for October 2026 (from 3.97M in September and 4.11M a year earlier), the lowest month since at least January 2010; MORTGAGE30US monthly mean 7.328%. Found the article's "highest mark since 2000" and "23-year high" wrong because October 2023 (7.62%) and November 2023 (7.44%) were both higher, and noted that this also makes Yun's own "single month in 2023" one month short. The only persona to catch the second 2023 month.
- **expert (2nd):** 1 Pinocchio. Verified every NAR figure against the release and computed October 2023 at about 7.62% from four weekly Freddie Mac readings. Did not check November 2023, so it accepted Yun's "single month" caveat as correct. Could not reach Freddie Mac's monthly archive.
- **everyman (3rd):** 1 Pinocchio. Same NAR verification, flagged the omitted 2023 exception without quantifying it, added useful context (consensus miss, inventory up). No independent series check.

What would change the ranking: the margin between askamerica and expert is the second 2023 month; if November 2023's monthly average were below 7.33% the two would tie.

## Recipe check
No recipe applies. The warehouse route (FRED monthly mortgage series over a long window) is what found the extra month; askamerica already took it.

## Shape
Final file on disk: 11 `author_claims`, 1 `subject_claims` (speaker Lawrence Yun), engine rating 1 Pinocchio. This is the subagent's original grouping. I first asked it to move NAR-relayed figures into `fidelity`, citing McpServer.java's older instruction ("a figure relayed correctly from a cited source MUST be grouped `fidelity`"); the validator refused part of that, and its rule (ClaimScoring, since ecae2ba83a on 2026-11-11) is the current one: a figure the piece reports as fact is the author's claim, attributed or not. I withdrew the request and the original grouping was restored. The two instructions in the engine contradict each other; see the run report.

## Gaps
None. No claim graded "not checkable here" for want of a table; the ungraded items are a broker's opinions.

## Severity invariant
Not triggered.
