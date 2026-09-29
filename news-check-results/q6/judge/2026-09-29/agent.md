# Judge: NAHB immigration-enforcement/housing claim fact-check (q6), 3-persona 2026-09-29

Rank: [askamerica, expert, everyman]

## Why

All three reached the same nuanced verdict: NAHB's individual figures are accurate to their
sources, but three of four load-bearing numbers trace back to NAHB/its affiliate HBI's own
commissioned research, and the headline "1.2 million unit housing shortage" is NAHB's own
narrowest-scope, low-outlier estimate — independent institutions (Freddie Mac ~3.7M, Fannie Mae
~4.4M, Zillow ~4.5-4.7M, Up For Growth ~3.78M, Brookings ~5M, McKinsey ~8M per expert's fuller
list) run far higher — presented without disclosing it's NAHB's own number or where it sits in
that range.

**`askamerica`** verified the 300,000-worker-shortage claim live against its own
`econ.jolts_industry` table (326,000 construction job openings in July 2026, matching NAHB's
figure almost exactly) rather than relying on secondary reporting, and correctly identified a
genuine corpus gap: no nativity-by-industry crosstab exists to independently recompute the 26.3%
immigrant-workforce-share claim. Both are direct, warehouse-grounded findings.

**`expert`** reached the same overall verdict independently, with a slightly wider set of
independent housing-shortage estimates (adding Brookings ~5M and McKinsey ~8M to the range) and a
useful additional caveat on the HBI study: its 1.98-month delay figure is an unweighted average
from a builder self-reported survey, not administrative permit data — a real, disclosed
limitation worth surfacing.

**`everyman`** reached a comparable conclusion with solid but slightly less precisely sourced
verification of the same points.

## Corpus gap — filed

No ACS crosstab of nativity by industry exists in this corpus. Checked `gh issue list` first — no
match. Filed as
[kenstott/govdata-ops#775](https://github.com/kenstott/govdata-ops/issues/775), `type:sourcing`,
`kind:gap`, `schema:census`, `status:open`.

## Severity check

`askamerica` ranked #1 — no severity flag.

## Recipe check

Nothing filed — all three performed well; the sourcing-bias caveat was correctly surfaced by all
three independently, which is the right outcome, not a gap to fix.
