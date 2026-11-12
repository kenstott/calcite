# q15 judge: Stacker/Shipley Energy via WFTV "How data center demand is reshaping electricity costs across the US"

Rank: [expert, askamerica, everyman]

Story dated 2026-11-09, judged 2026-11-11.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| expert | 1 | PJM 2028/29 BRA release; Monitoring Analytics; Dallas Fed WP 2606 original and r1; MISO PRA; EIA STEO; DOE 202(c) order; Georgia PSC/Power; LBNL; NERC LTRA; Grid Strategies |
| askamerica | 0 | warehouse energy schema (PJM RTO clearing prices, EIA residential price, EIA Henry Hub daily, ND sales/price); 43 web fetches incl. PJM release, DOE order, Dallas Fed wp2606.pdf (original only), PJM 2024 SOM, Oregon PUC report |
| everyman | 1 | PJM release; Monitoring Analytics; Dallas Fed abstract; EIA STEO; PUCO order; Oregon PUC; about 10 claims marked NOT CHECKED |

## Assessment
- **Correct verdict: 1 Pinocchio.** Most numbers check out, but there is one false characterization, one outdated figure, and one omitted caveat:
  - **"Despite a record price" (PJM 2028/29) is false.** The auction cleared at $325/MW-day. That is 2.5% below the 2027/28 cap of $333.44 and 1.3% below 2026/27's $329.17 (PJM 2026-07-14 release; Enel, OPIS). The record was the 2027/28 auction.
  - **The Dallas Fed "2% to 6%" figure is superseded.** wp2606 (March 2026) gave 2-6%. The landing page now carries wp2606r1, "Revised October 2026", whose abstract says 6.9% nationally by 2025, 15.0% in northern Virginia and 12.3% by 2028. A Nov 9 piece citing the March draft a month after the revision quotes an outdated number. It counts as an inaccuracy, a minor one. It understates the article's own thesis, so it is not slanted.
  - **North Dakota (+~40% demand, real price -~3 c/kWh): not refuted, not located.** LBNL/Brattle's 2026 update names ND as a high-load-growth state with falling real prices over 2019-2025. A secondary reading of the LBNL data file puts ND's real all-sector price at -26.2%, which is about 3 c on a ~11 c base. The exact 40% figure was not found. Unsupported, not false.
  - **Henry Hub $30.57 on Jan 26: mostly true.** EIA's record is $30.72 on trade date Jan 23. NGI's $30.565 is indexed to flow date Jan 26 (the weekend strip). This is a date-convention difference, not an error.
  - **Georgia $20/month: accurately quoted but stripped of context.** In Nov 2025, staff testified to "$20 or more" for the full buildout, with a recommendation to certify only about 3 GW. In Dec 2025 staff reversed and stipulated to 9,885 MW, and Georgia Power called the $20 figure inaccurate. This is omitted context, not an inaccuracy.
- **expert (1st):** correct rating.
  - It is the only arm that identified the Dallas Fed revision with the right numbers.
  - It flagged the Georgia condition and the MISO annual-vs-summer labeling.
  - It ran the Henry Hub source conflict to ground (NGI vs EIA).
  - Miss: it graded PJM 2028/29 "verified" and did not test "record price".
- **askamerica (2nd):** the widest coverage (27 claims) and the only arm that caught the "record price" error, against warehouse PJM clearing prices.
  - It tested Henry Hub against EIA daily data in the warehouse and named the vintage gap in the residential price.
  - Errors:
    - It read only the original wp2606.pdf and graded the 2-6% "true", missing the October revision.
    - It graded "wholesale is about half of retail" true from Dallas Fed commentary, an unlocated attribution.
  - Its ND test used a 2016-2025 window with nominal prices, which does not match LBNL's 2019-2025 real-price frame. It correctly stopped at "unsupported".
  - Rating: the engine scored 0 although askamerica itself graded one claim partially false and named the Georgia omission. Its own read of "high 0 to low 1" was closer to correct than the score it published.
- **everyman (3rd):** the right rating, on partly wrong grounds.
  - It says the original Dallas Fed abstract gave "3-5%" and that 2-6% "appears only in press coverage". Both statements are wrong: the March paper said 2-6%.
  - It missed the record-price error.
  - It left about 10 claims unchecked, including ND, Henry Hub, Georgia and NERC.

## Recipe check
Three gaps a reusable warehouse procedure would have closed:
- **Revision check.** When a claim cites a working paper or report, fetch the publication's landing page (not a cached PDF) and check for an `rN` revision or a "Revised" date later than the original. Compare headline numbers and grade a stale figure "outdated". askamerica fetched `wp2606.pdf` directly and never saw r1.
- **Window/deflator match before a warehouse test.** For "demand grew X% while real prices fell Y" claims, take the source's window (LBNL: 2019-2025) and deflate with CPI before querying state sales and prices. The warehouse has both series. The 2016-2025 nominal test could not reproduce the claim by construction.
- **Rating floor.** Any claim graded partially false plus a named material omission should put the score at a minimum of 1 Pinocchio. The engine's score of 0 contradicted its own claim grades.

## Severity invariant
askamerica ranks above everyman. No flag. The margin is narrow: everyman's rating is correct and askamerica's is one notch low. askamerica places ahead on coverage (27 claims vs about 9), the unique record-price catch, and fewer factual errors in its reasoning.

## Defects
None new in the warehouse. The engine's Pinocchio score of 0 with a partially false claim is a calibration issue for the rating step.

## Connector procedure check
- `find_recipe` was called ("electricity prices data centers fact check trend") and served `fact-check-a-news-article`. It was called only after a publish-gate reminder.
- Exactly one claim is marked central (wholesale-to-retail pass-through), so step 2 holds.
- The unsupported claims (ND, PJM West Hub) list their searches in `searched`, so step 6 holds.
