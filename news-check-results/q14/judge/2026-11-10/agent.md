# q14 judge: Reuters/Investing.com "US traffic deaths fall in first half of 2026, lowest rate since 2014"

Rank: [expert, askamerica, everyman]

Story dated 2026-11-10, judged 2026-11-11. askamerica ran on engine-dev 1.42.0-SNAPSHOT.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| expert | 0 | NHTSA DOT HS 813 833 Table 1; 813 800 (2025 rate); 813 829 (bicyclists); 813 403 (cost study); NSC release |
| askamerica | 1 | warehouse FARS (transport.fatal_crashes), annual 2018-2024 and first-half 2015-2024; Reuters text via wiky.com |
| everyman | 1 | none (nhtsa.gov 403); search excerpts, Carscoops, syndicated copies |

## Assessment
- **Correct verdict: 0 Pinocchios.** The article's figures reproduce from NHTSA's own tables:
  - H1 2025 = 8,120 + 9,055 = 17,175, and 16,750 is -2.5% from that.
  - H1 2014 rate was about 1.01, so "lowest rate since 2014" holds.
  - H1 2015 deaths were 16,193, so "lowest count since 2015" holds.

  The omitted context (early estimates, the Q2 stall, NSC's different count) is worth noting but is not an inaccuracy in the article.
- **expert (1st):** correct rating. It traced every numeric claim to a named NHTSA publication:
  - Recomputed the H1 2026 rate (1.019) from VMT.
  - Reproduced 16,750 from the quarter split.
  - Confirmed the bicyclist count and the cost figures from the primary studies.
  - Tested the five-year trend year by year.
- **askamerica (2nd):** the widest claim coverage of the three arms (10 claims).
  - It tested "lowest number since 2015" directly against warehouse first-half FARS for every year 2015-2024, which no other arm did.
  - It tested the five-year streak and the 2019 and 2021 comparisons against warehouse FARS.
  - It graded the cost study with its data year.

  Two weaknesses:
  - It read no NHTSA document: nhtsa.gov returned 403 and it did not try crashstats.nhtsa.dot.gov, so the H1 2025 baseline, the bicyclist figure and the 2025 rate came from secondary reports.
  - Its 1-Pinocchio deduction is for missing context, not for any claim it found wrong. That is a defensible but miscalibrated rating.
- **everyman (3rd):** same rating as askamerica, with less support.
  - It read no primary document.
  - It left "lowest since 2015" unconfirmed.
  - Its deduction rests on reading "second-lowest" and "lowest since 2014" as being in tension, though both are true.

## Severity invariant
askamerica ranks above everyman. No flag.

## Defects
None new.

## Connector procedure check
The fact-check procedure reached this run, but the engine does not log tool results, so the path is not recorded. Two candidates:
- `find_recipe` ("traffic fatalities trend lowest since year").
- The first article fetch (wiky.com), which carries the procedure.

What it produced:
- Step 2 (every streak phrase and attributed cause as its own claim) was followed.
- Step 6 held: the central claim lists its search in `searched`.
- Step 2's "exactly one central claim" was not followed: two claims are marked central.
