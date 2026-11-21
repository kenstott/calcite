# Judge: Bridge Michigan, fecal levels in 3 of 4 Michigan rivers (news q9, 2026-11-21)

Rank: [askamerica, expert, everyman]

Story: https://bridgemi.com/michigan-environment-watch/pure-michigan-has-a-poop-problem-dangerous-fecal-levels-befoul-3-of-4-rivers/ (2026-11-18). Claimants: Michigan EGLE (Molly Rippke), Environment America, CDC study, Rep. Mike McFall.

Verdicts: askamerica 0 Pinocchios (engine-scored), expert 2, everyman 1.

## Ranking

**1. askamerica.** The only arm to test the central "75% of rivers" estimate against data: Water Quality Portal samples in the warehouse show 78% of 1,493 Michigan monitoring sites (2021-2025) had at least one sample above 300 E. coli/100 mL, which it correctly labels a single-sample proxy on non-random sites, not the legal test. It also found EGLE's own E. coli page still says roughly half of rivers exceed the swimming standard, a discrepancy the article does not reconcile. Corrected two smaller points: the standards are geometric means applied May-October, and "could top a million" understates the CDC paper's 5.61 million recreational-water illnesses. Indiana/Illinois shares, the one-employee and $40,000 claims were left unsupported, stated.

**2. expert.** Verified the Michigan rule (R 323.1062), the Graziano et al. home-price study (8.94%, temporary), the CDC estimate with its credible interval, and Environment America's 69% via local coverage, and made a fair metric-mismatch point (river miles against sites unsafe on one day). But it had no way to test the 75% estimate, and rated the piece 2 Pinocchios largely because that central figure was unverified, which treats an unconfirmed but plausibly sourced estimate as an inaccuracy.

**3. everyman.** Thin by its own account (three searches, summary only): confirmed the standards and home-price figure and caught the "estimate stated as fact" headline problem, but checked little else.

What would change the order: if askamerica's site-exceedance proxy were judged too loose to bear on a river-mile estimate, expert's broader document checks would move it close; its 2-Pinocchio grade still reads as miscalibrated.

## Severity invariant

askamerica ranks first. No defect flag.

## Claim-shape check

Compliant: EGLE's estimate and the article's own figures as `author_claims` (a figure the piece reports as fact), the CDC and home-price study as `fidelity`, Rep. Mike McFall as `subject_claims` with speaker. Rating 0 in report.html and agent.md.

## Gaps and resolution

The Indiana and Illinois assessed-river-mile shares need EPA ATTAINS, which the warehouse lacks. Filed govdata-ops **#1414**, `environment: add EPA ATTAINS state assessment summaries` (type:sourcing, kind:gap, schema:environment, status:open). askamerica ranked first, so this is a gap filing, not a loss resolution.

## Recipe check

Nothing from the unaided arms that askamerica did not already reach.
