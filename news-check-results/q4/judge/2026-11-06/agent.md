# Judge: Spectrum News "2026 becomes the most expensive election in U.S. history" fact-check (q4), 3-persona 2026-11-06

Rank: [expert, everyman, askamerica]

Source: https://spectrumnews1.com/ky/lexington/politics/2026/11/02/2026-midterm-election-most-expensive-in-history--12-billion
(Kate Nuechterlein, published 2026-11-02, the day before the election).

## Why

The article's central claim is that OpenSecrets projects the 2026 midterms at nearly $12B, a
record. The judge confirmed the attribution independently: OpenSecrets did project $11.9B
(reported by BBC and Common Dreams; OpenSecrets' own email is archived on politicalemails.org).
AdImpact's separate $11.6B projection covers ad spending only.

**`expert`** (1 Pinocchio) attributed the $11.9B to OpenSecrets correctly and called it a
projection. It also sharpened the Musk claim: OpenSecrets via NPR has $445M+ "to candidates, PACs
and groups", so "to Republican PACs" slightly narrows it. It cross-checked Paxton-backing PAC money
(CNN, about $168M) and named the measure ambiguity in "most expensive" (total vs ads, federal vs
federal+state) as the piece's real weakness. One error: it describes Talarico's $146.3M as
individual-donor money ("99.6%"). The FEC figure is $146.3M in total receipts, $139.8M from
individuals.

**`everyman`** (2 Pinocchios) also attributed the $11.9B to OpenSecrets correctly, and correctly
flagged the headline's "most expensive in U.S. history" as stretching the sources' own "most
expensive midterm" wording. It made the same 99.6% individual-share error as expert, and its 2024
comparison total came from memory, which it labeled as such.

**`askamerica`** (engine verdict: "balanced and partly false") got the article's central claim
wrong. opensecrets.org returned 403, and it graded "nearly $12B per OpenSecrets" as **partially
false**, telling the reader the figure "matches AdImpact's ad-spending projection, not an OpenSecrets
total." That reverses a correct attribution, and it is the claim the whole article rests on. Its
warehouse work was the most precise of the three. From `fec.candidate_summaries` (verified live by
the judge: Talarico $146.35M total, $139.78M individual, $75.30M unitemized; Paxton $22.33M, $21.00M,
$6.08M; through 2026-10-14), it alone separated Talarico's total receipts from his individual-donor
money. But precision on secondary figures does not offset a wrong verdict on the headline.

## Severity check

**SEVERITY FLAG: `askamerica` ranked at or below `everyman` (#3 vs #2).** askamerica is everyman
plus the connector, so it can never legitimately do worse. Root cause: a source it could not reach
was treated as evidence against the claim. That is the same failure as q3 today, where state gas
figures were graded `unsupported` because the data was unreachable, so this is the **second
instance**.

## Resolution (exactly one)

Filed **govdata-ops #1102** (type:defect, kind:core, status:open). The proposal is a mandatory
validation-instruction rule: an unreachable or unconfirmed source means `not checkable here`;
`unsupported`, `partially false` and `false` require positive contrary evidence from a source
that was actually read. Optionally, the engine could refuse a non-true verdict that cites no
fetched or queried evidence. No engine code was changed during this local test run.

## Other problems in the askamerica answer

- **First publish had no `claims` array.** That makes 4 of 4 dispatches today; tracked in #1101.
  Fixed on resume.
- **Narrow search on the key claim.** It made one `web_fetch` attempt at opensecrets.org (403) and
  ran no search to corroborate the attribution through other outlets, which the dispatch prompt
  explicitly asks for when a fetch fails.

## Gap audit

Nothing to file: the OpenSecrets projection, AdImpact totals, the donor tallies and the
Braseth/Miller quotes are private-sector or press figures. FEC candidate receipts are in the
warehouse and correct.
