# Judge: Council on Criminal Justice homicide-decline fact-check (q7), 3-persona 2026-09-29

Rank: [askamerica, expert, everyman]

## Why

Exceptional convergence and depth across all three — this is the strongest-verified story of the
day. All three confirmed the 18% and 51% figures accurately, correctly treated "lowest since at
least 1900" as CCJ's own explicitly hedged projection (not asserted fact), and correctly flagged
that reliable US national homicide data doesn't really exist that far back (pre-1930s Death
Registration Area coverage was partial and homicides were often misclassified).

**`askamerica`** did the deepest verification: rather than relying on CCJ's or Asher's own
numbers, it independently reconstructed a comparable 29-large-city sample from its own warehouse
table (`crime.cde_reta`, FBI Return A monthly data) and recomputed the decline from raw counts —
getting -29.3% (steeper than CCJ's 18%), correctly explained by a different city set and FBI's
own documented pattern of upward-revising current-year counts as late corrections arrive (meaning
the raw current-year number is a known undercount). It also honestly corrected its own initial
claim about which tables it had queried during a validation pass rather than letting an
overstated method description stand. This is the clearest demonstration of the product's actual
value: not just corroborating a claim, but independently recomputing it from primary data.

**`expert`** found something neither other persona surfaced: FBI's own preliminary quarterly UCR
release, showing an even larger 23% national decline for the same period — a genuinely
independent third data point beyond CCJ and Asher. It also specifically checked which of the 30
sampled cities bucked the trend (8 showed increases — Norfolk +64%, SF +55%, Dallas +30%, Salt
Lake City +20%), directly addressing whether the convenience sample was cherry-picked. Excellent,
genuinely additive work.

**`everyman`** correctly verified the core claims and cited Asher's Real Time Crime Index as
independent corroboration, matching both other personas' conclusion, but did not independently
recompute anything or find a source beyond what CCJ/Asher already published.

## Corpus gap — not filed

`askamerica` correctly noted this corpus's crime tables don't reach back to 1900 (or even much
before 2010/2017) to independently verify the "lowest since 1900" framing. This is an inherent
scope limitation, not an actionable gap — reliable, computable national crime data simply doesn't
exist that far back in any source. Checked `gh issue list` for related coverage tickets; nothing
applicable to file.

## Severity check

`askamerica` ranked #1 — no severity flag.

## Recipe check

Nothing filed — this is a model example of the intended `askamerica` behavior (query the
warehouse, recompute independently, explain any discrepancy from secondary reporting rather than
just repeating it) with no gap to close.
