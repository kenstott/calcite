# Judge — news q7 (foxbusiness-edmunds-new-vehicle-price-50k) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: Fox Business, "New vehicle affordability squeezed as record prices, high interest rates hit buyers" (2026-11-18). Claimant: Edmunds (Ivan Drury). Verdicts: askamerica 0 Pinocchios (engine; its own narrative says about 1 for framing), expert 1, everyman 1.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — Read the Fox piece and all five pages it links; Edmunds itself returned 403, so the figures were checked against a syndicated Edmunds copy and KPRC's coverage of Edmunds' Q3 report. All headline numbers match, and it notes the +1.8% would be +2.0% against the reported October 2025 base. Two findings test the article's framing with data: the national Edmunds APR was 7.0% and flat year over year, so "higher interest rates" rests on a single metro (Los Angeles 7.3%); and BLS's new-vehicle CPI in the warehouse (`econ.new_vehicles_cpi`) rose only 0.74% year over year (180.039 vs 178.723), a quality-adjusted measure showing milder price growth than the nominal record. It also notes KBB crossed $50K first (December 2025), so "first time ever" holds only for Edmunds' series. Drury's quote is a `subject_claims` entry graded on Edmunds' Q3 record payment and 84-month share.

2. **expert** — Strong and slightly broader on claim extraction: it checked the negative-equity "record" and found it partly unsupported (29.1% of trade-ins underwater in Q3 2026, below Q1's 30.9%; only total interest paid is a record), a claim askamerica did not extract. Also the KBB crossing, the EV-drop recalculation (12.6% vs 11.8%), and the LA-is-one-metro point. Worked from Fox4 and search summaries, no national-APR or CPI comparison.

3. **everyman** — Summary-only; most Edmunds figures left not checkable; correct on the KBB comparison and the 2.0% recalculation, but its EV comparison mixes KBB's -2.7% with Edmunds' -11.8% across methods.

Close call between askamerica and expert. It flips if the negative-equity claim is weighted above the interest-rate framing: the expert caught one overstated claim askamerica missed, while askamerica's national-APR and CPI checks address the article's central framing.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 7 claims; Drury quote is `subject_claims` with `speaker`; compliant. Report and agent.md both state 0. No dataset gap (Edmunds/KBB are private series; BLS new-vehicle CPI is in the warehouse). Nothing filed.

## Recipe check

The expert's catch is an extraction-completeness point (every "record" in the piece is a claim), already covered by the engine's "extract every factual assertion" instruction; one instance is not grounds for a change.
