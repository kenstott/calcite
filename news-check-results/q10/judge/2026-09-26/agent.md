# Judge: q10 — "Census announced real median household income was $87,460 in 2025"

Blind judge of the persona runs available for this story (news claim, Workstream A of the daily-eval pipeline).

Rank: [askamerica, everyman]

**expert did not run for this story** — `news-check-results/q10/expert/2026-09-26/` exists but is empty (no `agent.md`, no dashboard, no report). This run was interrupted before the expert dispatch completed or delivered; per the instruction not to rerun any persona for q10, expert is omitted from the ranking rather than judged as a loss. This is a process gap for this story, not a persona defect — it should not be read as "expert failed the question."

## Reasoning

Both personas reach the correct verdict (**TRUE**) and cite the same primary source (Census press release CB26-151, Sept 15, 2026), with matching headline figures ($87,460 in 2025, up 2.6% from $85,210 in 2024). The distinguishing factor is depth of verification and honest disclosure of what could and couldn't be checked.

**askamerica** is first: it did the more rigorous investigation. It searched the corpus (`search_catalog`) for a matching income series, found `census.acs1_income`/`census.acs_income`/`census.income_summary`/`census.saipe_poverty`, and — critically — determined that none of them carry CPS ASEC data (the survey the claim's figure actually comes from), explicitly noting `search_catalog` flagged "asec" as unmatched. It ran `data_coverage` to confirm `census.acs1_income`'s loaded window (2005–2024, no 2025, no national row, state-level only), then queried the table anyway for directional context (unweighted state averages, $64,645 in 2019 through $80,424 in 2024) while explicitly labeling that query as "not a recomputation of the claim" because it's a different survey, different weighting, and a year short. Only after establishing the corpus could not independently verify the number did it fall back to a direct primary-source fetch (the same Census press release) to confirm the claim. It also ran an independent `score_claim` check (true, confidence 0.88) and disclosed plainly: "Had the primary-source fetch failed, this claim would have had to be marked 'not checkable here' rather than 'true.'" This is exactly the calibrated-limits behavior the harness rewards — it shows its work distinguishing "verified via primary source" from "the warehouse itself confirmed it," rather than blurring the two.

**everyman** is second: it reached the same correct verdict via web search/fetch of the same and additional sources (CNBC, Axios, Washington Examiner, the Census "story" page), and added some genuinely useful context the askamerica answer omitted — the poverty rate figure (10.2%, down 0.5pp) and the uneven distribution of income gains across demographic groups (+3.0% White, +4.8% Black households). But it never attempts or discusses whether a government-data warehouse could independently corroborate the number — there's no equivalent of the corpus-search step, so there's nothing to calibrate against beyond "multiple outlets say the same thing," which is agreement with secondary sources, not independent verification. It also doesn't flag that the underlying data comes from CPS ASEC specifically (it names it, but doesn't discuss why that matters or how it differs from other Census income series) the way askamerica's investigation does.

## What would change the ranking

If everyman had also surfaced the CPS-ASEC-vs-ACS distinction, or if askamerica had also relayed the poverty/distributional detail everyman found, this would be a closer call. As is, askamerica's edge comes from a genuinely deeper, better-disclosed investigation, not just from having the connector — though the connector's `data_coverage`/`search_catalog` tools are precisely what let it show the "corpus can't reach this, and here's exactly why" reasoning everyman has no equivalent tool to produce.

## Severity check

askamerica ranks first among the personas that ran — no severity flag. (No comparison against expert is possible for this story; see the note above.)

## Recipe check

No new recipe warranted. askamerica's handling of "the corpus doesn't cover this specific series (CPS ASEC), here's the closest available table and why it can't substitute" is already the correct, uncoached behavior this harness wants to see repeated — not a gap to patch.
