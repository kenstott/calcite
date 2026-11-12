# Judge: Breitbart "Jobless Claims Fall To 57-Year Low of 189,000" fact-check (q2), 3-persona 2026-11-07

Rank: [expert, askamerica, everyman]

Source: https://www.breitbart.com/economy/2026/11/05/jobless-claims-fall-to-57-year-low-of-189000/
(John Carney, Breitbart, 2026-11-05). This is the outlet's own interpretive framing of the DOL weekly
claims release. It has no quotes.

**Engine build:** main e3fe1935c4 (installed 15:16 on 2026-11-07, cksum 2523950303), which includes the
#1101/#1102 fixes. The run produced a `claims.json` with 6 graded claims.

## Judge's own verification

- **FRED ICSA, read from the CSV:** the last week at or below 189,000 before 2026 was 1969-09-06
  (182,000). No week from 1969-09-13 through 2025-12-27 is at or below 189,000. So "lowest since 1969"
  is correct for the weekly figure. The 1969 low was 177,000 (Apr 26 and May 3); the 1968 low was
  162,000 (Nov 30). 2026-04-25 reads 190,000 after revision.
- **Warehouse:** `econ.fred_indicators` ICSA and CCSA both start 1986-01-04, and IURSA and IC4WSA are
  absent. The cause is the table-wide 1986 floor at `econ-schema.yaml:5809`.

## Why

All three personas agree that the headline numbers are right: 189,000, down 6,000; ties July 18;
four-week average of 194,750; continuing claims of 1.718M, up 23,000. The ranking turns on how many of
the remaining claims each persona actually checked, and whether it got any wrong.

**`expert`** checked every claim, mostly against the DOL release PDF it read directly and its own
recomputation from FRED:
- 225,500 a year earlier and the 13.6% fall
- the 2019 mean of 217,711.5 and its minimum of 196,000
- the year-to-date average of 208,068.2 over 44 weeks
- continuing claims 244,000 below a year earlier (1,962,000)
- the insured unemployment rate falling from 1.3% to 1.1%

It also gave the correct 1969 and 1968 lows. Its only unchecked item was whether the year-to-date
average is the "lowest since 1969."

One point it made contradicts itself. It flags the missing labor-force adjustment as "materially
misleading" while conceding that the adjustment would make the low more impressive, not less. The
omission therefore understates the result rather than overstating it.

**`askamerica`** checked the same core from the warehouse (`econ.fred_indicators` ICSA/CCSA):
- the four-week average and the 13.6% fall
- the 2019 mean and minimum
- the year-to-date average over 44 weeks, which is below every full year from 1986 on
- continuing claims up 23,000 and down 244,000

It added two pieces of context the others missed: April 25 was first reported at 189,000 and revised
to 190,000, and claims have been flat at 195,000 to 200,000 since September.

Its weaknesses:
- It mislabels 182,000 on Sept 6, 1969 as "the 1969 low." That was the last sub-189,000 week, which
  still supports the claim, but the 1969 low was 177,000.
- It couldn't test anything before 1986 or the insured unemployment rate from the warehouse. The DOL
  PDF returned 403 and the FRED history fetch timed out. So it graded the year-to-date claim and the
  1.3%-to-1.1% claim "mostly true" without confirming them.
- It graded the piece 0 Pinocchios, which is defensible given the "57-year low" tie framing.

**`everyman`** checked the core figures against FRED and secondary reports but left five claims
unchecked: the year-ago average, the 2019 figures, the year-to-date average and its ranking, and
continuing claims year over year. It also got one wrong. It graded the 1.3%-to-1.1% insured
unemployment claim "likely wrong" on the basis that the rate "held at 1.1%." That is the week-to-week
figure. The article's comparison is year over year, which `expert` confirmed against DOL and FRED.

## Severity check

`askamerica` ranked above `everyman`, so no severity flag.

## Pinocchio shape and claims check

- **Shape:** every graded claim is the article's own assertion about DOL data, with no named
  third-party claimant, so a single rating is allowed.
- **Claims array:** `claims.json` is present with 6 claims (4 true, 2 mostly true), each carrying its
  table and SQL. This is the first news run on a build with #1101/#1102, and the claims array was
  produced as intended.

## Recipe check

`askamerica` ranked below `expert` because of coverage, not method. The two claims it couldn't confirm
need pre-1986 ICSA history and IURSA, which the warehouse lacks. This is resolved as a data gap (see
below), so no recipe change is needed.

## Gap audit

- **Filed govdata-ops #1117** (type:sourcing, kind:gap, schema:econ, status:open). ICSA and CCSA are cut
  off at 1986 by the table-wide floor, while FRED has them from 1967. IURSA and IC4WSA are absent.
  Searched "ICSA", "jobless claims", "fred_indicators", "initial claims", "insured unemployment",
  "IURSA" and "fred history 1986". Related closed issues (#249, #643, #592) covered other series or the
  2010 cap, not these.
