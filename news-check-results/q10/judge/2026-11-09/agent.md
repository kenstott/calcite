# Judge: theGrio, "Birmingham homicides drop to historic lows under Mayor Randall Woodfin's leadership" (q10), 3-persona 2026-11-09

Rank: [askamerica, expert, everyman]

Source: https://thegrio.com/2026/10/15/birmingham-homicides-mayor-randall-woodfin-violence-prevention/
(Oct. 15, 2026). **25 days old, outside the seven-day default**, used after no in-window story with checkable
figures could be found.

**Engine build:** the private dev jar, from 0af22733bd, all gates. **Data server:** the rebuilt
`~/.askamerica/pgwire-dev` on port 45533.

## Judge's own verification

A web search confirmed: 34 homicides through Q3 2026 against 61 and 129 at the same point in 2025 and 2024
(ABC 33/40), and 88 for 2025 against 152 for 2024 (AL.com via Ground News), with other reports putting 2024 at
150 to 153. The article's "63" and "130" are not those year-end totals.

That they are same-point counts is an inference, not established: 63 and 130 do not equal 61 and 129. `expert`
labels it an inference; `askamerica` states it as fact. The article's error (calling them the full-year
totals) stands either way. I did not verify the 1951 benchmark, which no arm could either.

## Why

**`askamerica` is first.** All three found the mislabelled baseline and graded the piece 2 Pinocchios.
`askamerica` went further into primary and local documents: the city ARPA report, finding its own inconsistency
($5,512,378 once, $5,412,379 twice, $2 million elsewhere), WVTM13 for the Pickett quote and the 31 of 34 closures,
and the omitted domestic-violence rise and enforcement credit. It listed real windows on the central claim. No
warehouse table holds city homicide counts, so this was web work. It overstated the inference.

**`expert` is second.** Same core finding, properly marked as inference, and it noticed that 46% fits 34 against
63 while police counts give 44%. But it could not confirm the Pickett quote or the ARPA figure, and read a
summary of the article.

**`everyman` is third.** Same core finding from secondary reporting, and a useful note on the post-Q3 spike, but
it left the closures, the quote and the ARPA figure unchecked.

## Severity invariant

`askamerica` ranks above `everyman`. Not triggered.

## Gate check

The office-page gate refused one draft of `create_report_artifact`, its first live refusal. All three refusal
gates have now fired in live runs (named-subject and not-checkable on q9).

## Recipe check

`askamerica` is #1. No change warranted.
