# Judge — news q8 (fortune-oxford-economics-recession-warning) — 2026-11-20

Rank: [askamerica, expert, everyman]

Story: Fortune, "Recession vulnerabilities" (Oxford Economics, AI debt, hiring; 2026-11-18). Claimants: Michael Pearce (Oxford Economics), Goldman Sachs, ADP, NY Fed, relayed by Fortune. Verdicts: askamerica 0 Pinocchios (engine), expert 1, everyman 1.

All three answers are same-day and blind to each other. everyman/expert prompts came from `compose_prompt.py`; askamerica used the extension's news prompt verbatim.

## Ranking and reasons

1. **askamerica** — Checked every macro claim against warehouse series rather than summaries: Q1-Q3 2026 GDP about 2.5%, 2.2%, 3.4% (so "more than 2% every quarter" is true for all three, which the expert did not check past Q2); unemployment 4.1-4.4% ("around 4%" mostly true); Sahm 0.00; and Pearce's dot-com claim, tested as `subject_claims` (3.8% in April 2000, not reached again until May 2018). It read the NY Fed PDF for the 12.4% probability (the expert found it only in a secondary blog) and ADP's release. On Goldman it found the same conflation the others did ($420B is hyperscaler bond issuance, roughly 40% of AI debt) and added that the "$500B" was a three-month-old "nearly $500B" figure. Disclosed limits: the S&P 500 year-end base came from memory after a transient R2 SSL error blocked the FRED query, and the S&P figure moved to about +4.8% by Nov 19.

2. **expert** — Correct on GDP Q3, unemployment, Sahm, and the $420B universe mismatch (with Reuters and FT context), but from a fetch summary; the NY Fed figure and Pearce's 2000/2018 claim were taken from a secondary source and from background knowledge respectively.

3. **everyman** — Similar findings with the most items not checked, including the Pearce claim and the quarterly GDP path; notes the September unemployment uptick.

askamerica vs expert is reasonably clear: same framing finding, but askamerica's checks came from primary series and documents where the expert's came from summaries.

## Severity check

askamerica ranks above everyman. No severity flag.

## Pinocchio shape and gaps

claims.json: 11 claims; Pearce is `subject_claims` with `speaker`; the two Goldman figures are grouped `fidelity` (relayed third-party estimates); compliant. Infrastructure note: one `query` on `econ.fred_indicators` (SP500) failed with "IO Error: SSL connect error" fetching a parquet file from R2; a single transient failure, not filed. Unsupported items (Oxford's proprietary note, Goldman's forecast) have no public dataset. Nothing filed.

## Recipe check

Nothing to teach; askamerica already won.
