# Judge — news q4 oilprice-us-lng-flows-record-21-8-bcf

**Story:** "US LNG Flows Hit Record High as Global Gas Supply Crisis Deepens" (Oilprice.com, 2026-11-16). Claims of interest: feedgas to US LNG plants a record 21.8 Bcf on Saturday, +12% y/y (BNEF); EIA H1-2026 exports 17.4 Bcf/d, +23%; EU storage 70.5% on Nov 14; framing that US LNG relieves Europe.

Rank: [expert, askamerica, everyman]

Judged 2026-11-18 on three answers produced the same day.

## Why
- **expert (1st):** 1 Pinocchio. Confirmed the 21.8 feedgas record and EIA's 17.4 Bcf/d, and set them against other trackers (NGI 21.2 on Nov 13, LSEG 20.4 on Nov 9). The decisive catch: EIA's own breakdown shows H1 exports to Europe +1% vs Asia +108%, so the piece's "relief for Europe" framing is overstated. Also: the Russian ban from 1 Jan 2027 covers long-term contracts only (short-term banned since 25 Apr 2026, Reg. (EU) 2026/261); Qatar force-majeure extension single-sourced to Oilprice.
- **askamerica (2nd):** engine 0 Pinocchios. Confirmed the central 21.8 figure through a Marcellus Drilling News copy of BNEF (Bloomberg 403) and graded it `mostly true` because it is single-day feedgas, not exports, and drops "per day" — the right distinction. Read EIA directly (17.4 Bcf/d, +23%; ~20% of global LNG cut by Hormuz), caught the long-term-only Russian ban, added EIA's flat H2 forecast (17.3 Bcf/d). Missed the Europe-vs-Asia destination split that undercuts the article's thesis. No warehouse figure: the corpus has no gas export series (see Resolution).
- **everyman (3rd):** 1 Pinocchio. Found the Europe +1%/Asia +108% split and that the storage five-year average (~89%) makes "well below" an understatement — strong framing work — but could not confirm the central 21.8 figure (graded not checkable) and did not catch the long-term-only Russian ban.

What would change the ranking: weighting the Europe/Asia framing catch above confirmation of the headline figure would put everyman level with askamerica; this is a close call between them.

## Recipe check
No recipe. The relevant method (EIA exports by destination) is not reachable in the warehouse; it is a sourcing gap.

## Resolution
Filed kenstott/govdata-ops#1321 (`type:sourcing`, `kind:gap`, `schema:energy`, `status:open`): EIA natural gas exports/imports by type and country, monthly. Checked first: energy schema has gas production/storage/price only; no existing issue for LNG or gas exports. With that table askamerica could have produced the destination split from the warehouse.

## Shape
claims.json: 5 `author_claims`, 5 `fidelity` (BNEF, EIA, QatarEnergy, GIE relays). No named-speaker quotes in the piece. Correct.

## Severity invariant
Not triggered (askamerica above everyman, narrowly).
