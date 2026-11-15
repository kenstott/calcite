# Judge — news q1 texastribune-vouchers-students-disabilities

**Story:** Parents of kids with disabilities fear losing Texas school voucher (Texas Tribune, 2026-11-13)

Rank: [expert, askamerica, everyman]

Judged 2026-11-15 on three answers produced the same day.

## Why
- **expert (1st):** 1 Pinocchio. The only arm to test the participant figures against comptroller-sourced reporting: the article's 17% disability share against about 24% (Community Impact) and 28% (NCR, May); "at least 60%" low-income against about 80%; "more than 910,000" special-education students against TEA's 903,072. Placed the $227M as a floor for 184 districts with a full estimate near $294-300M. States which items it took from memory.
- **askamerica (2nd):** engine score 0; its own draft said 1. Read the article in full and confirmed the dollar tiers, the Huffines request, the House flip and the quotes against the Tribune's earlier reporting. Left the 18,800 / 17% / 60% figures and the $227M as unsupported because the comptroller dashboard and osod.org did not load, where expert found secondary sources. Warehouse contributed one trend (special-education count 532,341 in 2018 to 635,327 in 2021) and nothing later.
- **everyman (3rd):** 1 Pinocchio on a partial check. Could not read the article verbatim; did not confirm the House result; suggested the article confused Eckhardt with Huffines on the audit, which the other two arms contradict.

## Recipe check
No recipe applies; the claims are state program figures outside the warehouse.

## Resolution
One: kenstott/govdata-ops#1228. `edu.ccd_districts.spec_ed_students` is null for all rows in 2022-2024 (verified live), so the 910,000 figure had no warehouse check.

## Severity invariant
Not triggered.

## Shape
Third-party claims present (advocacy groups, officials); claims.json reviewed.
