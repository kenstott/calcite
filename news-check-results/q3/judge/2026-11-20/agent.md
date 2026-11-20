# Judge — news q3 (newsom-california-record-low-crime-rates) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: Office of Gov. Gavin Newsom, "Record low crime rates: crime drops again in major cities across California" (gov.ca.gov, 2026-11-17). Claimant: Gov. Newsom's office, citing state DOJ, CDC and Major Cities Chiefs Association data. Verdicts: all three 2 Pinocchios.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — The only arm that recomputed the release's numbers from their sources and tested the headline against a long series. It read the MCCA nine-month PDF and recomputed the nine-jurisdiction totals (violent -6.3%, homicide -11.9%) and the city bullets, then used the same table to show what the release left out: homicide rose in San Diego (31→45), Fresno (15→22), Long Beach (23→29) and San Jose (20→21). From the warehouse it confirmed the 2025 homicide rate (3.48) and showed the "record low crime rates" headline is mostly false for crime overall: the violent crime rate was 434 per 100,000 in 2025, higher than 2011-2014 (391 in 2014). It found that the "since taking office" percentages only reproduce from a 2018 baseline, the year before Newsom took office (from 2019: vehicle theft -11% not -19%, robbery -28% not -31%, violent -1.5% not -3%), and that the retail-theft grant figure conflicts with the Governor's own 2023 release ($267M to 55 agencies, not $242M to 38). It adds the national comparison (MCCA national homicide -17.3%, steeper than California's -11.9%). Weakness: it graded the CDC state comparison unsupported after searching the catalog for a state homicide table that does not exist (a guessed table name), which is a real gap, now filed.

2. **expert** — Accurate on the statewide figures (3.5, 1,666 → 1,374) and makes the right framing point (violent crime only -3% since 2019; the record is homicide only), but it took the since-2019 percentages from the Governor's own July release without testing them, so it missed the baseline-year problem, and it could not read the MCCA nine-month file (404), so the city claims and the omitted increases went unchecked.

3. **everyman** — Confirms the 3.5 rate and the 1966 claim, and raises the national-trend context, but suspects the Sacramento 44% homicide drop is doubtful from older partial-year reports; askamerica's recomputation from the nine-month file shows it is correct. Summary-only reading.

Not close at the top. The baseline-year finding and the omitted city increases are the two most material checks on the piece, and only askamerica made them.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 16 claims, all `author_claims` (a press release in the office's own voice; no quoted third-party figures graded separately); compliant. Gap stated: no CDC state homicide-rate table. Verified: the cause-of-death tables are national-only and no state-by-cause table exists. No prior issue matched. Filed kenstott/govdata-ops#1374 (`type:sourcing`, `kind:gap`, `schema:health`, `status:open`). The gun-law ranking and program totals are not dataset gaps.

## Recipe check

Nothing to teach from the other arms; askamerica already tested the baseline year and the omitted rows unprompted.
