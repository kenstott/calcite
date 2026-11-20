# Judge — news q5 (epochtimes-realtor-rents-first-annual-increase) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: The Epoch Times, "US rents post first annual increase in more than three years: report" (2026-11-19). Claimants: Realtor.com October rent report, Freddie Mac, NAR, Neighbors Bank, relayed by the outlet. Verdicts: askamerica 0 Pinocchios (engine score; the agent's own read was 1 for framing and it says so), expert 1, everyman 1.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — Read the article directly and read the full Realtor.com release, so every rent, unit-size and completions figure is checked against primary text, not a summary. It is the only arm that confirmed the Freddie Mac 7.28% for Nov 19, from `econ.fred_indicators` (MORTGAGE30US), up from 7.11%; both other arms could confirm only the prior week. It caught two framing slips the others missed: "highest level in four weeks" is loose (7.32% on Oct 22, exactly four weeks earlier, was higher), and the article dates the construction boom to 2021-2022 while the release cites 2023-2025 completions. It grades the one Neighbors Bank claim it could not find in the page text as unsupported. It also names the scope of "national median rent" (50 largest metros, studio to two-bedroom).

2. **expert** — Same core verification from a fetch summary, with the strongest context paragraph: studio rents falling for a 39th month, a separate Realtor.com finding that renting a starter home is cheaper in every metro it lists (conflicting with the Neighbors Bank result the article uses), and that Neighbors Bank is a lender. Could not confirm 7.28%.

3. **everyman** — Correct core figures from secondary coverage, notes the $1 change is flat and that two-bedroom rents last rose in May 2023 (a fair nuance on "first since June 2023"), but did not open primary documents and could not confirm the rate.

askamerica vs expert is fairly close. The expert's conflicting-Realtor.com-study point is the most useful single piece of context in any of the three; askamerica wins on primary-text reading and the one warehouse-confirmed figure the others could not reach.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 10 claims, all `author_claims` (relayed report figures); compliant. The engine's 0 differs from the agent's narrative 1; the published rating is the engine's, and the narrative discloses the difference. No dataset gap: Realtor.com, NAR and Neighbors Bank are private sources. Nothing filed.

## Recipe check

Nothing to teach; askamerica already won.
