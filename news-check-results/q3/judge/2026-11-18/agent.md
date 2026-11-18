# Judge — news q3 fox17-nashville-crime-down-17-percent-oconnell

**Story:** "Nashville mayor says crime is down 17% this year as more residents report feeling safe" (Fox 17, 2026-11-13). Claims of interest: Mayor Freddie O'Connell — crime down 17% YTD, auto thefts −30%, homicides −44%; Alliance-commissioned survey 70% feel safe (from 67%).

Rank: [askamerica, expert, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **askamerica (1st):** engine 0 Pinocchios (it said its own judgment was 1, and reported the engine's). The only arm with independent counts: `crime.cde_reta` (FBI Return A, Metro Nashville PD, Jan–Sep 2025 vs 2026) all crimes 37,747 → 33,073 (−12.4%), motor-vehicle theft 3,299 → 2,213 (−32.9%), murder 45 → 27 (−40%), alongside MNPD's H1 Part I −19.2%. Read the survey slides themselves and found what the others could not: overall MOE ±3.1 (so +3 is within noise), neighborhood MOE ±10.4 for the 60%→76% figure, and Nashville still 4th-highest of ten peer cities on 2025 crime rate — an omission the piece makes. Caveat: the SQL matched `agency_name ILIKE '%NASHVILLE%'` and returned 15 rows across ORIs; the report attributes the totals to Metro PD, which I could not confirm from the log alone.
- **expert (2nd):** 1 Pinocchio. MNPD H1 −19.2% and the September 16%; flagged auto-theft figure inconsistent with other 2026 reporting (34–40%). Did not find the survey MOE, did not pull independent homicide counts.
- **everyman (3rd):** 1 Pinocchio. Five tool calls; good catch on the 30% vs 37–40% auto-theft inconsistency and the 47/48 split on city handling, but no primary data.

What would change the ranking: if the cde_reta totals summed multiple Nashville-area agencies, askamerica's −12.4% would be mis-scoped, though its survey-slide findings alone would still keep it level with expert.

## Recipe check
Nothing to teach: askamerica already took the warehouse route (Return A monthly agency counts) that neither unaided arm reached.

## Shape
claims.json: 2 `subject_claims` (speaker Freddie O'Connell), 2 `author_claims`, 2 `fidelity` (survey). Correct as delivered.

## Gaps
Precinct-level Nashville crime (North Nashville −13%) — city open data, outside the federal corpus scope; not filed.

## Severity invariant
Not triggered.
