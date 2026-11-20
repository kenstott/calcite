# Judge — news q4 (wwno-march-of-dimes-gulf-south-preterm-births) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: WWNO, "Gulf South ranks last in the U.S. for premature births as Medicaid cuts loom, report finds" (2026-11-18). Claimant: March of Dimes (Dr. Michael Warren), relayed by WWNO. Verdicts: askamerica 0 Pinocchios, expert 1, everyman 1.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — Read the article directly and covered the most ground: ranks 50/51/52, the 2025 preterm rates against the US 10.4%, the Medicaid shares and birth counts (30,119 / 17,338 / 20,263), the infant and maternal mortality figures and ranks, and the Warren quote as a `subject_claims` entry, checked as rounding of the report's annual death counts. It confirmed Medicaid expansion status from the warehouse and caught the one factual slip none of the others found: the Rural Health Transformation Program is $50B over five fiscal years (CMS), not ten; the ten-year window belongs to the Medicaid cuts. It also qualifies "each state got an F" (true for these three; half of states got D or F) and leaves the chart-only breakdowns as unsupported rather than guessed. Limit: nearly all figures are checked against the claimant's own March of Dimes pages, because the warehouse has no state natality series (gap filed).

2. **expert** — Correct on the core figures, and it adds that the 52 jurisdictions include DC and Puerto Rico (the ranking still holds among states). It says the article does not mention that the year-over-year changes were not statistically significant; askamerica, which read the article text rather than a fetch summary, reports that it does, so that point counts against the expert's reading. It flagged a Mississippi "Adopted" expansion icon as a rendering error, correctly. It did not check the mortality figures or the RHTP length.

3. **everyman** — Summary-only; correct on the core numbers, more items left unchecked (Mississippi and Alabama Medicaid shares, the quote), and its "misleading framing" points are mostly definitional.

askamerica vs expert is reasonably clear on breadth and the RHTP catch. All three check the same claimant source, so none tested the March of Dimes numbers against NCHS directly; that is the corpus gap below.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 11 claims; the Warren quote is `subject_claims` with `speaker`; the rest `author_claims`; compliant. Gap stated: no state preterm-birth / Medicaid-paid-births table. Verified: `health.cdc_wonder_natality` is national-only and nothing else matches. No prior issue. Filed kenstott/govdata-ops#1375 (`type:sourcing`, `kind:gap`, `schema:health`, `status:open`). The uninsured-women figure (ACS by sex and age) is an ACS detail table question, not filed.

## Recipe check

Nothing to teach; askamerica already won.
