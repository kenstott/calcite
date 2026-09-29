# Judge: EPA power plant rule repeal lawsuit fact-check (q4), 3-persona 2026-09-29

Rank: [askamerica, expert, everyman]

## Why

Strong triple convergence: all three confirmed every headline figure checks out, EDF's repeal
analysis is genuinely built on EPA's own RIA methodology (not an independent model), and the
"unlawful" framing is correctly an unresolved legal allegation, not adjudicated fact.

**`askamerica`** achieved the deepest primary-source verification of the three — it directly
fetched and quoted EPA's own 2024 fact sheet PDF verbatim (not just secondary corroboration) and
EDF's press release verbatim, disclosed the genuine corpus gap (no EPA RIA/litigation/asthma
tables — confirmed via search_catalog), and correctly graded the docket-existence sub-claim
"mostly true" rather than "true" when Justia's docket page itself returned a 403 rather than
inflating confidence past what it could verify.

**`expert`** caught one genuinely useful detail neither other persona found: the article claim's
"Sept. 18" filing date is off by one day — three independent press releases agree the suit was
actually filed Sept. 17. It correctly explained the car-equivalence factor discrepancy between
the two claims (2024-rule: ~4.21 t/car; EDF repeal: ~4.69 t/car) as likely different EPA
calculator vintages rather than an error. It could not directly parse EPA's own RIA PDF (a tool
failure, disclosed honestly) and relied on secondary corroboration for two claims askamerica
verified against the primary document directly.

**`everyman`** reached the same substantive conclusions with good rigor (independently computed
the car-equivalence math, correctly framed "unlawful" as allegation) but found no unique detail
beyond what the other two surfaced.

## Corpus gap — filed

No table covers EPA regulatory-impact-analysis projections or asthma/respiratory outcomes.
Checked `gh issue list` first — no match. Filed as
[kenstott/govdata-ops#774](https://github.com/kenstott/govdata-ops/issues/774), `type:sourcing`,
`kind:gap`, `schema:environment`/`schema:health`, `status:open`.

## Severity check

`askamerica` ranked #1 — no severity flag.

## Recipe check

Nothing filed — all three performed well; this was a data-availability gap, not a guidance gap.
