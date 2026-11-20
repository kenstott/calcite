# Judge — news q6 (housingwire-mba-mortgage-delinquencies-q3-2026) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: HousingWire, "Mortgage delinquencies rise to 4.42% in Q3 2026" (2026-11-19, automated with editor review). Claimant: Mortgage Bankers Association (Marina Walsh). Verdicts: askamerica 0 Pinocchios (engine), expert 1, everyman 1.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — No arm could open MBA's own Q3 release (403 / not found), so all three relied on the Scotsman Guide copy plus arithmetic against prior-quarter MBA data. askamerica did the same for the survey figures and then separated Walsh's statements into `subject_claims`, testing each: her household-stress points (wages lagging inflation, falling savings, rising card, auto and student-loan delinquency) against warehouse series, all true; her "closely tracks unemployment" line against BLS (unemployment 4.4% → 4.2% Sept 2025 → Sept 2026 while delinquency rose 43 bps), partially false. It graded the loan-type serious-delinquency changes it could not find in any copy as unsupported, and noted that Q3 is still below Q1 2026's 4.44%.

2. **expert** — Same survey verification, a precise reconciliation to Q2 2026 (4.37%) and Q3 2025 (3.99%), and a sharp framing list (Q2 fell 7 bps; FHA delinquency actually fell; Wyoming's +72 bps is a small-state swing; the unemployment link is weak). It left Walsh's household-stress claims not checkable after the 403.

3. **everyman** — Accurate on the headline figures, with the Q1 4.44% context, but more left unverified and Walsh's quote unchecked.

askamerica vs expert is close: framing is comparable; the difference is that askamerica tested the quoted official's supporting claims against public data instead of leaving them unchecked.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 11 claims; Walsh's three statements are `subject_claims` with `speaker`; compliant. The first saved report.html footnote said "Rating: 1 Pinocchio" against the engine's 0; the same subagent was resumed and republished, and the saved page now reads "Rating: 0 Pinocchios (engine-computed)" (rechecked on disk). No dataset gap: the MBA National Delinquency Survey is a private trade survey. Nothing filed.

## Recipe check

Nothing to teach; askamerica already won.
