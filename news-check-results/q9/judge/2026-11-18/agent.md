# Judge — news q9 benzinga-kobeissi-social-security-deficit-221b

**Story:** "Social Security 'deteriorating rapidly,' says market commentator as fund deficit hit record $221 billion in fiscal year 2026" (Benzinga, 2026-11-16). Claims of interest: The Kobeissi Letter says the trust fund deficit rose $39B y/y to a record $221B and reserves fell to $2.18T, "lowest since 2007". The piece also cites Trustees depletion dates and AARP's payroll-share figures.

Rank: [expert, askamerica, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **expert (1st):** 1 Pinocchio. It matched every core figure to Wolf Street's reading of SSA data, with the deficit at $220B and OASI only: costs +$83B to $1.5T, income +$44B to $1.3T, $2.18T reserves, $640B below the 2017 peak of $2.82T. It used SSA Tables 4.A1/4.A3 to show that combined reserves were $2.56T and that DI ran a ~$40B surplus. It sharpened "lowest since 2007" to strictly "since 2008" (2007 $2.02T, 2008 $2.20T) and named the omitted drivers: Social Security Fairness Act retroactive payments and lower benefit-taxation revenue.
- **askamerica (2nd):** engine 1 Pinocchio. It got the article through the Yahoo syndication despite Benzinga's 403. It confirmed the Trustees depletion dates and payable shares from the warehouse (`econ.social_security_trustees_projections`: OASI 2032/78%, OASDI 2034/83%). It gave the right framing correction: the figure is OASI only, while CRFB puts the combined 2026 cash deficit at $270B. It confirmed the reserve history (2007 $2.02T, 2008 $2.20T). But it graded the two Kobeissi claims `true` on "consistent with" CRS calendar-year projections ($200B→$243B), which do not reproduce $221B/+$39B, and it left the cost and income breakdown unverified. A `broken_field` false positive also told it to discard two valid Trustees columns (see Gaps).
- **everyman (3rd):** provisional 1 Pinocchio. It could not read the article (403) and so never saw the body claims. It found Wolf Street's ~$220B and the OASI-only and SSFA context, but checked only the headline.

What would change the ranking: if SSA's FY2026 table (unreachable for all three) showed $221B rather than $220B, askamerica's `true` grades would be right, though still for the wrong reason. Expert's lead rests on decomposing the deficit, which askamerica never attempted.

## Recipe check
No recipe. The missing piece is data, trust-fund operations, which no warehouse table holds. Once #1328 lands, a recipe could point fiscal-year deficit claims at it.

## Resolution / gaps
- Filed kenstott/govdata-ops#1328 (`type:sourcing`, `kind:gap`, `schema:fiscal`, `status:open`): SSA OCACT trust fund operations, annual + monthly. With it, askamerica could have reproduced expert's decomposition in the warehouse.
- Filed kenstott/govdata-ops#1327 (`type:defect`, `kind:core`, `schema:econ`, `status:open`): the `broken_field` out_of_domain diagnostic flags `max_trust_fund_ratio_pct` (DI 857) and `actuarial_balance_pct_payroll` (−4.42), both valid per the schema comments. The cause is the name heuristic in QuestionDiagnostics.brokenFields, the same class as #585/#798. Verified from calls.jsonl (`broken_field:high` ×2) and from the code.

## Shape
claims.json: 2 `subject_claims` (The Kobeissi Letter), 2 `fidelity` (Trustees report), 3 `author_claims`. Correct.

## Severity invariant
Not triggered.
