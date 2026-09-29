# Judge: NYT "$1B dark money" fact-check (q2), 3-persona 2026-09-29

Rank: [askamerica, expert, everyman]

## Why

All three reached the same verdict: the article holds up well, the MAGA Inc "$400M+ vs. $294M"
tension is real fundraising growth across 2026, not a contradiction. This is strong triple
convergence on the piece's most checkable claim.

**`askamerica`** delivered the most precise verification: it queried its own `fec.*` tables first
(found a real defect — `committee_summaries.cash_end` is NULL for this committee, now filed as
[#772](https://github.com/kenstott/govdata-ops/issues/772)), then went to OpenFEC's live API
directly and confirmed the MAGA Inc trajectory to the exact cent across three filing dates
($294,416,079.21 → $403,450,026.85 → $415,776,043.10). It correctly graded the $1B topline "not
checkable here" (a proprietary NYT tally, not reproducible from any single public dataset) rather
than forcing a verdict on an unfalsifiable figure.

**`expert`** matched askamerica's FEC verification almost exactly (same committee, same August
filing figure) and added one genuinely new finding neither other persona surfaced: 5 of the 6
Delaware candidates the Working Families Party PAC backed were primary challengers to *incumbent
Democrats*, not general-election opponents — a detail that complicates the article's implicit
left/right dark-money framing and is worth a reader's attention on its own.

**`everyman`** correctly resolved the core MAGA Inc "discrepancy" as chronology rather than
contradiction, matching both other personas' conclusion, but without citing FEC data to the exact
dollar the way askamerica and expert did — solid, but the least precisely sourced of the three.

## Corpus gap — filed

`askamerica`'s finding: `fec.committee_summaries.cash_end` NULL for MAGA Inc (C00892471) despite
OpenFEC having real, non-null data. Checked `gh issue list` first — no match. Filed as
[kenstott/govdata-ops#772](https://github.com/kenstott/govdata-ops/issues/772), `type:defect`,
`kind:gap`, `schema:fec`, `status:open`.

## Severity check

`askamerica` ranked #1 — no severity flag.

## Recipe check

Nothing filed — this is a clean win driven by data availability (FEC data is exactly what the
warehouse should be strong at), not a guidance gap.
