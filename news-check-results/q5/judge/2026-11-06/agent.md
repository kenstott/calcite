# Judge: ABC News "Ahead of midterms, backlash grows over soaring electricity bills" fact-check (q5), 3-persona 2026-11-06

Rank: [expert, everyman, askamerica]

Source: https://abcnews.com/US/ahead-midterms-backlash-grows-soaring-electricity-bills/story?id=137675396
(Elizabeth Schulze, published 2026-10-29).

## Why

The article's lead figures cite EIA without stating a window: residential electricity costs up about
5% nationally "over the past year", 16% in Maine and 12% in Ohio. The window turns out to decide the
verdict.

**`expert`** (1 Pinocchio) pulled the EIA API v2 retail-sales series itself and tested more than
one month. ABC's figures match **July 2026** year over year exactly: US +5.3%, Maine +16.2%, Ohio
+12.2%. They fall in August (+3.4%, +14.1%, +10.4%). Its conclusion: the numbers are accurate for
the latest month available when the piece was drafted, though "past year" from a single month is a
framing weakness. It ranked all states (62 geographies, filtered to states), confirmed the
February 45% poll baseline (n=2,589) and graded the unseen October 57% figure and the $250-to-$850
anecdote as not checkable here, which is correct. Caveat: expert's prompt carried the line
"absence of confirmation is not contradiction" (expert-tier methodology), which bears directly on
the failure below.

**`everyman`** (1 Pinocchio) used secondary EIA snapshots, but its snapshot was July: national
+4.9% and Maine +16%. It graded both supported, and Ohio "plausible, in range." Its verdicts match
the primary data, though with weaker sourcing.

**`askamerica`** got the lead claim wrong. It tested only August 2026 (EIA Table 5.6.A) and graded
"about 5% residential" **partially false**, telling the reader ABC had used the all-sector number.
It never considered July, the month the figures actually match. It also graded the 57% poll figure
and the anecdote `unsupported` rather than not checkable. The engine scored ABC's honesty at 65/100
("mixed") and called the piece "biased and partly false"; both rest on these grades. It did the most
supplementary work of the three:
- the top-10 state list
- PJM capacity prices ($28.92 to $329.17/MW-day), the most specific driver context of any persona
- the four states where prices fell

Its warehouse figures are correct and were confirmed live by the judge from
`energy.eia_electricity_prices`: US residential 16.48 → 17.30¢ (+5.0%) for calendar 2025, Maine
+14.4%. But a wrong verdict on the headline claim outweighs that.

## Severity check

**SEVERITY FLAG: `askamerica` ranked at or below `everyman` (#3 vs #2).** That is the second today
(q4 was the first). Root cause: the same class as govdata-ops #1102, a contrary grade without
contrary evidence. Here the evidence came from a different window than the claim's.

## Resolution (exactly one)

Added this third instance to **govdata-ops #1102**, widening the proposed rule: when a claim's
window is unstated, test the plausible windows (the latest month and the one before it, year-to-date,
calendar year) before grading contrary.

## Other problems in the askamerica answer

- **First publish had no `claims` array.** That makes 5 of 5 dispatches today (#1101). Fixed on
  resume.
- **It kept a rating of its own** ("My own reading remains 2 Pinocchios"), which rule 8 forbids.

## Gap audit

- **Filed govdata-ops #1103** (type:defect, kind:gap, schema:energy).
  `energy.eia_electricity_prices` is annual only: the source URL hardcodes `frequency=annual`,
  `price_month` is null in every row, and the latest year is 2025. With monthly data the persona
  could have scanned July and August from the warehouse directly.
- `econ.average_prices` (the BLS electricity series) hit the #992 binder error again.
- The October poll topline, the Loudoun meeting and the anecdote are not public government
  data, so there is nothing to file for them.
