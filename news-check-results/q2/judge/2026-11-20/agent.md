# Judge — news q2 (yankee-institute-ct-electric-bills-lawmaker-claims) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: Yankee Institute, "Connecticut candidates promised lower electric bills. Now comes the hard part" (2026-11-13). Claimants: Sen. Saud Anwar and other Democratic legislators; the piece's own claims about bills and the renewable portfolio standard. Verdicts: all three 1 Pinocchio.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — Read the article text and extracted 12 claims, with each legislator's statement as a `subject_claims` entry (Anwar, Demicco, Allie-Brennan) rather than folded into the author's voice. It checked the piece's state-ranking claim against EIA data in the warehouse: typical Connecticut household bill $206.44/month in 2025, second to Hawaii at $207.92, so "about $200, second-highest" is true. It found the one substantively misleading line: "stop ratcheting up" the RPS, when PURA's schedule shows the Class I share dropping from 30% (2025) to 25% (2026) before rising to 29% by 2030 (graded partially false). It also notes an omitted dispute (Sen. Gordon's ~$200M/year savings estimate) and grades the campaign figures it could not find on the candidates' own pages as unsupported rather than guessing.

2. **expert** — Worked from a fetch summary (8 claims, no verbatim quotes), but added useful context the others lacked: the AG's $451M / ~11% ex-storm figure, PURA's ~$500M July 2026 storm-cost cut, and 2026 bonding against public-benefit costs. It flags the past-tense "cut bills by $800 million" as overstating a projection, a fair point askamerica graded true on attribution. It did not check the second-highest-bill ranking and treated the RPS line as opinion.

3. **everyman** — Summary-only, fewer claims, low confidence stated; correct on the rate request and on the $800M being a projection.

askamerica vs expert is reasonably clear: the EIA ranking check and the RPS schedule finding are the two checks that test the piece's own factual framing, and only askamerica made them. The expert's context paragraph is the best of the three and would narrow the gap if it had read the article verbatim.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 12 claims; quoted legislators are `subject_claims` with `speaker`; compliant. "Unsupported" items are legislative status pages that failed to load (TLS error) and campaign pages, not dataset gaps. Nothing filed.

## Recipe check

The expert's point (a supporter's projection reported in past tense as a realized cut) is worth noting but is already covered by the engine's claim-grading rules on attributed figures; no recipe.
