# The CNBC article's claims about heating oil prices, EIA winter forecasts and LIHEAP aid are mostly accurate.

**Rating: 1 Pinocchio.**

Article: https://cnbc.com/2026/11/14/heating-oil-price-spike-household-budgets.html (fetched successfully; no 403).

## Verdicts
Warehouse: no heating-oil, LIHEAP or reserve table (search_catalog "heating", "liheap" unmatched). Those claims were graded from primary documents. Only the Caribou weather claims were computed in the warehouse.

| Claim | Verdict | Evidence |
|---|---|---|
| US heating oil $5.91/gal as of Nov 9, +$2.19, +59% (central) | True | EIA 11/09/26: $5.915, +$2.194 y/y |
| Key Points: same $5.91 "as of Nov. 2" | Partially false | $5.91 is the Nov 9 reading; Nov 2 was about $6.14 (week change -$0.221). Article body says Nov 9. |
| Maine $5.83, +$2.49, ~75% | True | EIA: 5.833, +2.490 (74.5%) |
| ~50% of Maine homes use heating oil | True | Gov. Mills: "just over 50 percent" |
| EIA: $2,115 Nov-Mar, +21% (estimate) | True | EIA Winter Fuels Outlook (price +34%, consumption -9%) |
| Natural gas $640 (-9%), propane $1,246 (-3%), electricity $1,196 (+4%) | True | Same EIA outlook |
| Warmer Northeast winter offsets prices | True | EIA Northeast heating degree days -12% |
| 4.1M oil-heated households, 3.4M in Northeast | True | EIA table 4,134k / 3,388k |
| Maine 100-gallon minimum on 275-gal tank; ~$600 | True | Maine AG; 100 x $5.83 = $583 |
| Collins release: ~$38M LIHEAP to Maine and tribes; "outgoing" | True | $36.68M + $1.41M = $38.08M; Collins lost Nov 3 to Troy Jackson |
| DOE to release 300,000 bbl from NE reserve, Nov 4 | Mostly true | It is an exchange, repaid with a premium; deliveries due Nov 6 |
| $3.63B LIHEAP on Oct 9, 90% of ~$4B under CR; Dec 11 shutdown | True | ACF page; CR (Pub. L. 119-103) runs to Dec 11 |
| Oct 14 House letter and Oct 1 Senate letter seek $3B | True | Both letters read |
| Caribou, ME: Jan/Feb highs in the 20s, lows single digits; Jan 24 high -2F | True | Warehouse weather.ghcnd_daily: mean Jan high 23.2F, low 5.9F; Feb 25.4F / 5.9F; 2026-01-24 TMAX -18.9C = -2.0F |
| KC Fed attributes prices to Iran/Russia shocks, low inventories, refining limits | Mostly true | Bulletin fetch timed out; a search summary confirms bottlenecks, low inventories and record margins. The Iran/Russia attribution is unconfirmed. |
| Parent: HEAP benefit $750 vs $550; 150 voicemails | Unsupported | Agency-internal figures; nothing public found. Not false. |
| Rubin: 10-12% of a month's / 1.5% of season supply | Unsupported | Expert estimate with no stated base. My rough check puts 300k bbl at under 1% of residential season demand, but his denominator is unknown. |

Lafleche's personal circumstances were not tested (personal testimony).

## Flags for misleading framing
- The article calls it a "spike" but omits that EIA's price fell $0.22 in the week to Nov 9.
- $2,115 is an October forecast covering only the roughly 3% of households that heat with oil.
- "Release" of reserve barrels hides that it is a loan.

Report saved at /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q4/askamerica/2026-11-15/report.html
