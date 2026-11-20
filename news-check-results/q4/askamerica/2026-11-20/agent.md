The WWNO article's claims about the March of Dimes 2026 Report Card, Gulf South preterm birth and Medicaid are balanced and accurate; 2 claims could not be confirmed.

Rating: 0 Pinocchios (Washington Post scale).

Report: http://127.0.0.1:60986/a/3cbfd0205c541542fd6aa9d1a377f770.html (saved at /Users/kennethstott/.askamerica/reports/20261120-065927-the-wwno-article-s-claims-about-the-marc.html)

Method: the article loaded directly. The warehouse has no state preterm-birth, ranking or Medicaid-share-of-births table. The one check it supports is Medicaid expansion status (health.medicaid_chip_enrollment_monthly: LA Y, MS N, AL N). A state infant-death query on cdc_state_vital_provisional returned no rows. Everything else was graded against the March of Dimes 2026 report cards for LA, MS, AL and the US, which I fetched.

Verified true:
- Ranks 50, 51, 52 of 52 for AL, LA, MS (central claim).
- 2025 preterm rates of 13.1%, 13.7% and 14.9% against a US rate of 10.4%.
- Medicaid paid for 58.2%, 52.7% and 35.4% of births, and covered 30,119, 17,338 and 20,263 births.
- Infant mortality rates and ranks, and the 404 and 409 infant deaths in 2024.
- Maternal mortality, severe maternal morbidity, prenatal care, cesarean and risk-factor figures.
- US grade of D+.
- Warren's "two babies every hour, two moms every day", which is rounding of over 20,000 infant deaths and 649 maternal deaths.

Imprecise (mostly true):
- "$50 billion, 10-year Rural Health Transformation Program": CMS describes $50B over five fiscal years (FY2026-2030). The 10-year window belongs to the law's Medicaid reductions.
- "Each state got an F": it applies to the three Gulf South states. Half of all states got a D or F.

Unsupported (not contradicted):
- Breakdowns by race, insurance and cause, and the policy counts of 5, 3 and 2. These sit in interactive charts and icons that the fetched pages do not expose.
- Louisiana's uninsured-women drop from 25.2% to 9.2% (63%), and the 35% versus 48% comparison. The cited maternity care deserts page returned no matching text. The arithmetic checks (63.5%), but the warehouse ACS and SAHIE tables cover all residents, not women of reproductive age.

Fetch failures: congress.gov and commonwealthfund.org returned 403, including on retry with WebFetch.

Framing: nothing materially misleading. The article states that the 2024-to-2025 changes were not statistically significant. The Medicaid-cuts causal link is forward-looking and was not tested.