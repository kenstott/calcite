# Verdict: Partially confirmed — cycle totals check out within ~15%; the four-week window claims are not checkable in this corpus

## Table used
`fec.independent_expenditures` (FEC bulk data, `independent_expenditure_2026.csv`) — 19,119 rows for `election_year=2026`, one row per independent-expenditure transaction, with `candidate_id/name`, `spender_id/name`, `support_oppose` (S/O), `amount`, `amendment_indicator`, and `transaction_date`.

## What I found

**1. Cycle total ($1,452,997,175 across 881 candidates).** The raw `SUM(amount)` for 2026 is $40.16 billion — a data-quality artifact. 29 rows attributed to one candidate ("Bettis, Shawn", a minor FL House candidate) carry fabricated $9B/$8B/$1B line items from spender names like "TANKING" and "Warren Buffet" — physically impossible amounts that are almost certainly spam/prank filings FEC's bulk files accept without vetting. Excluding only that candidate's rows:
- All filings: **$1,661,230,718** across **841** distinct candidates (backfilling the 1,748 null-`candidate_id` rows by name)
- Non-amended filings only (`amendment_indicator='N'`): **$1,551,814,924**

Both land **7%–14% above** the article's $1.453B, and 841 candidates is **4.5% below** the article's 881. Same order of magnitude and direction — plausibly reconciled by how FEC nets amendments against originals (via `prev_file_num`) on its own site versus a flat sum here, and/or a small vintage gap. **Verdict: partially true.**

**2. Four-week total, Aug 24–Sep 14, 2026 ($328,333,233).** `transaction_date` is **NULL for 100% of all 681,504 rows** in this table (confirmed via `data_coverage` and directly re-verified for every 2026 row). No other per-transaction date field exists. **This is structurally impossible to check from this table — not a data-quality judgment call, a missing column for the grain the claim needs. Verdict: not checkable here.**

**3. Talarico (TX Senate) four-week figures ($59,564,521 / 17 committees / $2,086,549 support / $57,477,973 oppose).** Same date-column limitation applies — the window can't be isolated. What I could compute instead is the **cycle-to-date** total for `candidate_id='S6TX00479'`: **$72,348,678** from **28** committees, **$6,633,153** support / **$65,715,525** oppose (90.8% oppose share, vs. the article's 96.5% for its narrower window). This independently confirms Talarico's race as the **#1** IE target in the 2026 cycle (ahead of Paxton at $64.7M and El-Sayed at $59.8M) and confirms the same heavily lopsided anti-Talarico direction — consistent with the claim's shape but not proof of its specific window numbers. **Verdict: not checkable here** for the exact figures, though the underlying pattern (Talarico's race as the top target, heavily opposed) is independently corroborated.

## Bottom line
The claim is not fabricated or contradicted — the cycle-level structure of the data (total magnitude, candidate count, and Talarico as the top race) all check out within a plausible, explainable margin. But two of its three sub-claims rest on a real-time date filter this corpus's copy of the table cannot reproduce at all, because `transaction_date` is entirely unpopulated. That's a sourcing/vintage gap in this connector, not a demonstrated error in FEC.gov's own live figures.

## Report
Published report (dashboard + full disclosure of the exclusion, methodology, and caveats): saved to `/Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q3/askamerica/2026-09-26/report.html` (local link only, does not survive this session).
