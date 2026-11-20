# Judge — news q10 (farmpolicynews-cobank-barge-fleet-shrinking) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: Farm Policy News (University of Illinois), "Shrinking U.S. barge fleet could strain grain exports, farm margins" (2026-11-13), a roundup of coverage of CoBank's Nov 10 report (Tanner Ehmke, Emmie Noyes). Verdicts: askamerica 0 Pinocchios (engine), expert 1, everyman 1.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — Read the CoBank report and checked every fleet figure and the projection arithmetic against it. It is the one arm that traced the barge-price claim to its source: CoBank cites Opentug for "$1 million or more" and never states the "$650,000 a decade ago" figure, which appears only in secondary copies, and Opentug's own range is $500,000 to over $1 million (graded mostly true). everyman also could not find $650,000 on CoBank's page; the expert reports it as matched, which the other two arms' reading contradicts. askamerica checked the soybean-exports claim against warehouse data (`ag.fas_export_sales`, peak about 60.6 Mt in the 2020/21-era year, below since) and correctly treats WRDA's $6B as an authorization that has since passed the House. It also notes that five outlets repeating one study are not independent confirmation.

2. **expert** — Same core verification and arithmetic, the most specific WRDA status (House passed 415-9 on Sept 16, so "proposed" is stale), and the authorization point. Took the $650,000 as matching CoBank, and confirmed the "structural tightening" quote only through Maritime Executive.

3. **everyman** — Summary-only reading, but a useful context point the others lacked (CoBank's barge rate $20.04/ton vs an $18.37 average, so the squeeze is not yet in freight rates) and the capacity-vs-count distinction.

Close among all three; every arm is checking CoBank against itself because no government fleet series exists. askamerica leads on the source trace for the price claim and one warehouse cross-check.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 10 claims, all `author_claims` (relayed CoBank figures); compliant. Gap stated: no barge data in the warehouse. Verified (transport schema has only the BTS freight index). No prior issue. Filed kenstott/govdata-ops#1377 (`type:sourcing`, `kind:gap`, `schema:transport`, `status:open`): USACE vessel characteristics (barge fleet by type and year built) and USDA AMS Grain Transportation Report barge movements and rates.

## Recipe check

Nothing to teach; askamerica already won.
