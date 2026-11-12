# Judge: The Colorado Sun, "Jared Polis proposes $600M in cuts to Medicaid to address state budget crunch" (q8), 3-persona 2026-11-09

Rank: [expert, everyman, askamerica]

Source: https://coloradosun.com/2026/11/02/jared-polis-november-budget-proposal-2027-2028-medicaid-cuts/
(Brian Eason, Nov. 2, 2026, updated Nov. 3).

**Engine build:** the private dev jar, `~/.askamerica/engine-dev/askamerica-engine.jar` (27c4b39413, all
gates). **Data server:** the patched local overlay on port 45533.

**Severity: askamerica ranked below everyman. This is a defect under the standing invariant.**

## Judge's own verification

A web search confirmed Denver7, Colorado Politics and others carry the article's central budget figures:
a $250 million general fund increase for Medicaid, a 4.3% growth cap equal to the TABOR rate, about $820
million more in total funds, and per-pupil funding up about $488. So these were checkable by ordinary
search. I did not read the governor's letter. No arm did.

The setup caused one error: askamerica's queries on `health.medicaid_chip_enrollment_monthly` and
`health.medicaid_expenditures_by_state_quarter` failed with "object not found". The dev jar, built from
current main, lists those tables; the patched data server's older model does not have them. That is my
configuration, not a product defect, though it cost askamerica a primary-data check of "1 in 5".

## Why

**`expert` is first.** 25 claims, with most of the budget figures and the Polis quote corroborated across
outlets. It explained that "$600M" is a cut to projected growth, that other outlets report about $988M total
across some 30 actions, and that adult dental is reported two ways. It read only a summary, never read the
letter, and its 0 Pinocchios is marked provisional.

**`everyman` is second.** Working from a summary, it still corroborated the dental cuts with dollar figures,
Cover All Coloradans, the Fox quote, the January update and TABOR near 4%, and honestly marked the rest not
checkable, including the 828% comparison.

**`askamerica` is third.** It computed "1 in 5" (1,229,956 of 5,862,189, about 21%) and added useful context
on enrollment and Amendment 87. But:
- It fetched only Colorado Sun pages and a Google Drive link that returned "Loading...", then bundled six of
  the article's central figures into one "not checkable here" claim with no web search listed. The prompt
  told it to look for other outlets.
- It graded 828% against 271% "mostly true" on the strength of the same newspaper's own explainer, which is
  circular. It said "not independently verified", yet still gave a supporting grade.
- It graded five claims of a 22-claim article.

## Severity invariant

`askamerica` ranked below `everyman`. Flagged.

## Compliance

All five claims are `author_claims`, although Polis's budget figures and the $900 million projection are
attributed in the article to Polis and program officials.

## Gate check

No gate refused. The new checks do not reach this failure: nothing was graded 'unsupported', and "not
checkable here" passed the external-check gate because the Drive link counted as an attempt.

## Recipe check

Candidate resolution for the user to decide: a claim graded "not checkable here" for a figure the article
attributes to a source should need at least one attempt off the article's own domain and off a page that
did not load. Not changed in this run.
