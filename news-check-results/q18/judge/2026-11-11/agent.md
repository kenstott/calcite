# q18 judge: American Press "Louisiana unemployment insurance claims decline for 11 consecutive weeks"

Rank: [expert, askamerica, everyman]

Story ran 2026-11-05 (Center Square rewrite). Judged 2026-11-11. I verified the weekly claims against FRED state series LAICLAIMS and LACCLAIMS (DOL ETA 539 derived), pulled directly. DOL pages 403, so FRED was the route. BLS and investment figures were taken from the arms' own agreeing findings (both read BLS data and agree).

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| expert | 1 | DOL ETA 539 ar539.csv; BLS API LAUS/CES; LED/WRKF |
| askamerica | 0 | Warehouse econ.regional_employment; BLS-derived warehouse rows; weekly claims not retrievable |
| everyman | 1 | BLS Economy at a Glance; weekly claims unreadable |

## Assessment
- **Correct verdict: 0 Pinocchios.** Every checkable figure in the outlet's own reporting is accurate. The causal credit is the governor's office's spin, and the article itself says the data do not establish it.
  - **Initial claims 1,179 (week ending 10/17/26), prior 1,261, year-ago 1,581 (FRED week ending 10/18/25): true.** -25.4%, article says 25%.
  - **Continued claims 5,295 (report week 10/17, FRED reflecting week 10/10), prior 5,423, year-ago 7,898: true.** -33.0%, article says about 33%. The week-alignment conventions differ between FRED and the ETA report week; the numbers match either way.
  - **11 consecutive weeks of continued-claims decline: true.** FRED shows declines every week from 8/1 (8,136) through 10/10 (5,295), and further to 5,211 and 4,964. The streak is continued claims only, while the headline says "claims" (minor).
  - **BLS Sep 2026: rate 4.0% vs 4.2% Aug, labor force down (2,107,531 to 2,102,971), payrolls +1.8% YoY: true.** Construction is +13.5% (expert, askamerica) or 13.9% (everyman); "nearly 14%" is generous but within rounding of the framing.
  - Hyundai, Meta, SpaceX and the $150B+ total are announced commitments, consistent with state sources. The article hedges that they are years from producing jobs.
- **expert (1st):** the only arm that pulled the ETA data, confirmed the streak and every claims figure, caught the year-ago week alignment (7,837/1,557 for the matching ETA week), and checked BLS via API. Weakness: its 1 Pinocchio contradicts its own finding that every figure is accurate; it charged the outlet for the governor's causal framing, which the article hedges.
- **askamerica (2nd):** correct rating of 0 and good separation of "unsupported" from "false", with accurate BLS and arithmetic checks and a sound note that the streak is continued claims only. Weakness: it marked the weekly figures and the streak "not confirmable" although FRED/ETA data were reachable, so the headline claim went unverified.
- **everyman (3rd):** rating of 1 rests on spin by the governor and unverified headline claims. It checked only BLS, left the streak, 5,295, 1,179, 33% and 25% unverified, and did not check the investments. It did flag honestly what it could not verify.

## Recipe check
Partly. Weekly state claims are not in the warehouse (national only), so a recipe pointer to FRED LAICLAIMS/LACCLAIMS (or ar539.csv) as a fallback source when DOL pages 403 would have let askamerica verify the headline claim. A "governor credits X" attribution step should grade the article's reporting separately from the quoted official's causal claim.

## Severity invariant
**Not triggered:** askamerica (0) is below everyman (1), and askamerica's rating matches the correct verdict.
