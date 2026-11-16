# Judge — news q5 fortune-colleges-summer-melt-yield

**Story:** Priced out and unsure it pays off, more Gen Z students are getting in and not showing up—so colleges are cutting prices (Fortune, 2026-11-16)

Rank: [askamerica, expert, everyman]

Judged 2026-11-16 on three answers produced the same day. Close between the first two.

## Why
- **askamerica (1st):** engine score 0. Resolved the most claims to a source: BLS college-going 69.7% (2016) and 62.8% (2024) behind the article's 70% and 63%; the yield declines (32% to 22% public, 24% to 18% private) in the Hechinger Report text; the Human Capital survey figures; Georgia State's $125,000 campaign, $3.2M revenue and 22%-to-8% melt; NACUBO's 57.1%. The other two found no source for the yield or Georgia State figures. Framing: the headline treats falling yield as summer melt with no national melt trend cited; Human Capital's own page says affordability worries eased on most measures; the survey covers 91 institutions. One miss: it graded the Common App line true, where 820,000 offers belong to the 215-institution 2025-26 cycle and 234 is the 2026-27 count.
- **expert (2nd):** 1 Pinocchio. Caught the Common App blend and a second point nobody else made: NACUBO's 57.1% is preliminary and the prior year's preliminary was 56.3% (final 54.5%), so the comparison overstates the rise. Could not source the yield figures, Georgia State's numbers or the 2016 BLS figure.
- **everyman (3rd):** 1 Pinocchio. Caught the Common App blend; treated the yield and Georgia State figures as doubtful for lack of a source.

## Recipe check
No recipe applies; no warehouse table bears on admissions yield (covered by kenstott/govdata-ops#1258, filed on q4 today).

## Resolution
Not needed for rank.

## Severity invariant
Not triggered.

## Shape
First delivery put all 14 claims in `author_claims` and its page said "1 Pinocchio" against an engine score of 0. The same subagent was resumed and re-published; `claims.json` now holds 7 fidelity, 14 author and 1 subject claim with speaker, and the page states 0 (both re-checked in the files).
