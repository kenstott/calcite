# Judge — news q1 (constructiondive-asce-new-york-infrastructure-c-minus) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: Construction Dive via Yahoo, "ASCE gives New York infrastructure a C-" (2026-11-19). Claimant: American Society of Civil Engineers, New York section. Verdicts: askamerica 1 Pinocchio, expert 0, everyman 0.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py` (fingerprint markers present); askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — The only arm that checked the central number against the underlying public data rather than against ASCE's own release. Counting FHWA National Bridge Inventory records in the warehouse, it gets New York's poor-condition bridge share at 9.2% (2022) and 9.8% (2025) against 6.7% nationally, so the article's 9.5% → 9.9% is graded "mostly true", with the 0.3-point 2022 difference attributed to method. The dollar, road, transit and water figures are checked against ASCE directly and graded with the "more than" qualifiers noted. It also flags framing the other two missed or under-weighted: the lede never says the grade fell from C, the "adequate, stable" gloss softens ASCE's "fair condition", the IIJA expired in September with no successor, and the new stormwater category (D) muddies the 2022 comparison. Weakness: it did not extract the attributed Andrea Hunt quote as a claim.

2. **expert** — Thorough against ASCE's materials (22 claims, the three declining categories, the Hunt attribution, the C-key gloss), but every figure is checked against the claimant's own report card, which is close to circular for a piece relaying ASCE's numbers. 0 Pinocchios is defensible on that basis but misses the omitted grade drop in the framing.

3. **everyman** — Same approach as expert with fewer claims; it does note the IIJA expiry and an internal ASCE inconsistency, and 0 Pinocchios. Read the article only as a summary.

Not close at the top: askamerica's independent NBI count is the one piece of verification the piece's readers could not get from ASCE itself. expert over everyman is narrow (breadth of claims).

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 9 claims, all `author_claims` (relayed ASCE figures reported as fact), no `subject_claims`; compliant with the current grouping rule, though the Hunt quote was not extracted. Stated gap: ASCE grades are not in the warehouse. Not a sourcing gap (a private organization's report card, checkable on its own site). Nothing filed.

## Recipe check

Nothing to teach: askamerica already did the move worth teaching (check a relayed advocacy figure against the public dataset behind it).
