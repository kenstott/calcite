# The Axios credit-card-debt article's claims about record balances, delinquency and the income gap (Axios text not read; graded on CNBC copy) are accurate.

**Rating: 0 Pinocchios (engine-computed), with a framing flag below.**

## Important limitation
Axios returned HTTP 403 to the engine fetch and to WebFetch. I never read the Axios text. I extracted claims from CNBC's story of the same Nov 10 NY Fed release (https://www.cnbc.com/2026/11/10/credit-card-debt-record.html) and a Yahoo/BeInCrypto copy. Axios's own wording, quotes and any extra claims are ungraded, so the rating describes the CNBC copy and only presumptively Axios.

## Warehouse check
search_catalog found no NY Fed household-debt or card-delinquency table. A FRED card-delinquency series query returned no 2025+ rows. Claims were verified against primary pages instead.

## Claims and verdicts
- Card balances +$22B in Q3 to a record $1.29T: TRUE. NY Fed release shows $1.285T.
- Up 4.2% y/y: TRUE. +$52B on a ~$1.233T base is 4.2%.
- Total household debt +$334B to $19.1T: TRUE (NY Fed, 1.8% quarterly).
- TransUnion avg balance $6,678, +2.4%: TRUE ($6,523 a year earlier).
- "People with credit cards rose 2% to 178M from 174.8M": MOSTLY TRUE. TransUnion's metric is consumers carrying a balance (+1.8%), not all cardholders.
- Increasing share of new cards to lower-income and lower-score borrowers, now showing distress: TRUE per the Liberty Street post. The post limits this to 2020-2023, and cards issued since early 2023 went to higher-score borrowers.
- Inflation outpacing wage growth for low-income brackets: TRUE (verbatim from the post).
- Low/high-income-area delinquency gap widened since 2022: TRUE. It is measured by neighborhood (zip) income, not household income.
- "Americans increasingly turning to credit cards as financial pressures mount": MOSTLY TRUE. Balances grew, but see framing flag.
- Average new-card APR about 24% (LendingTree): TRUE, 23.96%. The average for accounts actually accruing interest is 22.36%.
- October CPI rose more than expected: TRUE (+0.4% vs 0.3% expected; BLS page not fetched, CNBC report used).
- Rossman and Moore quotes: not checkable (interpretive opinion).

## Framing flag
The "record" headline is nominal and not adjusted for inflation or income. The same NY Fed release shows card flow into serious delinquency improved (7.05% to 6.87%) and says transition rates for cards improved slightly. The NY Fed expects card delinquencies to keep improving absent shocks, and calls the pandemic-vintage effect "waning". Overall delinquency did worsen (5.0% of debt, +0.3 pt), driven by mortgages and autos. TransUnion shows borrower-level card delinquency up (2.37% to 2.50%) but balance-level down (2.05% to 1.99%). A reader could come away thinking card distress is worsening when the card delinquency flow is easing; the income-gap point is real but is about the gap widening since 2022, not new deterioration.

Report saved: /Volumes/main/Users/kennethstott/IdeaProjects/calcite/news-check-results/q1/askamerica/2026-11-12/report.html
