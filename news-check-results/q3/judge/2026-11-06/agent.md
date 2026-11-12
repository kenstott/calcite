# Judge: NBC "Senate battleground states: gas, groceries, housing" fact-check (q3), 3-persona 2026-11-06

Rank: [expert, askamerica, everyman]

Source: https://www.nbcnews.com/business/economy/senate-gas-grocery-housing-costs-trump-rcna607097
(Alexandra Meier, published 2026-11-03).

## Why

All three personas agree on the national figures: CPI 3.7% y/y for September, wages +3.0%, real
hourly earnings −0.7%, the 30-year mortgage near 7.2-7.3%, and gas up about 40% since Feb 28. All
three also flag the same framing problem: the battleground states are handpicked, there is no
comparison group, and the causes (tariffs, the Iran war) are asserted, not shown. The ranking
turns on what each persona found beyond that.

**`expert`** (2 Pinocchios) found the two most material problems with the piece:
- **Housing figures not corroborated.** NBC's Akron +22% and Cleveland +13% don't match Redfin's
  own first-half 2026 data (Akron +8.1%, Cleveland +5.7%). That's a possible substantive error,
  not just missing context.
- **Gas context omitted.** From EIA's weekly series via FRED, gas peaked at $4.478 on Sep 21 and
  has fallen about 7% since. The piece's since-February framing leaves that out.

It also checked the WalletHub and NTUF rankings against the sources' own pages, and pointed out
that both measure burden and exposure, not price growth, so neither supports "rising faster."

**`askamerica`** checked CPI (from `econ.fred_indicators`) and the Freddie Mac mortgage rate in the
warehouse; the judge confirmed both live (CPIAUCNS 324.80 → 336.94, +3.74%; MORTGAGE30US 7.24 on
Oct 29 and 7.18 on Nov 5). It fetched NTUF's tariffs.org and Redfin directly, and it found Yale
Budget Lab's $1,100 figure (on Yale's July 2026 page) where expert couldn't. The two disagree here,
and the judge did not resolve it. It produced a correctly grouped claims array (6 fidelity, 4
author_claims) and reported the engine's honesty score (63/100, "mixed") as returned. But it did not
test the Akron or Cleveland housing figures and did not establish the gas price trajectory, so it
missed both of expert's main findings.

**`everyman`** (1 Pinocchio) reached the same national checks from secondary sources and raised
the same framing concerns. Most of the state, housing and ranking claims were left unchecked.

## Severity check

`askamerica` (#2) ranked above `everyman` (#3), so no severity flag. Ranking below `expert` is normal.

## Problems in the askamerica answer

1. **First publish had no `claims` array** — this time a stated choice ("I put the claim-by-claim
   results in the report sections"). Fixed on resume. That is **3 of 3 dispatches today** (q1, q2, q3).
2. **"Unsupported" where it meant "not checkable here".** The state gas increases (Iowa/Nebraska
   about 55%, Kansas 53%) were graded `unsupported` because the warehouse has no state gasoline
   series and NBC's tracker was not reachable. Missing data is not evidence against the claim, and
   the `unsupported` grade lowers NBC's computed honesty score.
3. **Broken warehouse tables.** `econ.average_prices` and `econ.inflation_metrics` both hit the
   #992 `periodName`/`period_name` binder error. It fell back to `econ.fred_indicators`, which was a
   correct workaround. Added to #992 today.
4. **Narrow catalog search.** It ran one `search_catalog` call ("state gasoline retail price by
   state"). It never searched for a national weekly gas series, which would also have come up empty
   (see gap audit), or for anything on housing prices.

## Recipe check

The missing-`claims` failure is now 3 of 3, which is past the threshold for action. The fix belongs
in the tool, not a recipe: `preview_report` and `create_report_artifact` should refuse a report
that carries a `source_url` (or is otherwise identifiable as an article validation) but no
`claims` array, the same way they already refuse a missing `question_coverage`. Not changed during
this local test run; flagged for the user.

## Gap audit

- **Filed: govdata-ops #1100** (type:sourcing, kind:gap, schema:energy, status:open). EIA weekly
  retail gasoline prices (national, PADD, nine states, ten cities) are not in the warehouse. The
  only gas price is BLS's monthly APU000074714, which is currently unqueryable (#992).
- The state gas figures for Iowa, Nebraska and Kansas come from NBC's own tracker. EIA doesn't
  publish weekly prices for those states, so there's no government series to file for.
- The poll, the WalletHub rankings and the Redfin metro figures are private-sector data. Nothing to
  file.
