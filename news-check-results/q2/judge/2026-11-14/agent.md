# q2 judge: Fortune "CBO October budget review: deficit running more than $100 billion a week"

Rank: [askamerica, expert, everyman]

Published Nov 10, 2026. Judged 2026-11-14. I compared the three arms against each other. The Treasury figures askamerica and expert quote independently agree (October deficit $404B; FY2026 receipts $5.403T, outlays $7.396T).

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 2 | Warehouse MTS and 30-year yield series; CNBC transcript and interview; AAF quoting CBO |
| expert | 2 | Treasury Monthly Treasury Statement; CRFB and Fiscal Times for CBO |
| everyman | 2 | None direct; CRFB and AAF for CBO |

No arm could open cbo.gov (HTTP 403).

## Assessment
- **Correct verdict: 2 Pinocchios.** The October numbers are right. Three supporting figures are wrong: the FY2026 deficit was $2.0T, not $1.9T; outlays rose $386B, not $217B (that is the deficit increase); and $409B over a 31-day month is about $92B a week, not "more than $100B".
- **askamerica (1st, narrowly):** widest coverage and the only arm to test the quotes.
  - It found all three numeric errors from warehouse MTS rows.
  - It read the CNBC transcript and found "Treasury twist" was the host's phrase, not Bessent's. No other arm settled this.
  - It confirmed the Sullivan quote at source and noted the trim.
  - It tested "brought down longer-dated yields" against the 30-year series (below October's peak, above August).
  - Miss: it left the $1.14T interest figure "unsupported". Expert reconciled it.
  - Weak reasoning: it says the Nov 1 shift "cancels out" year over year. Treasury's adjusted rise is about $94B against $120B unadjusted, so the shift explains roughly a quarter of the jump. The "partially false" grade stands; the stated reason is too strong.
- **expert (2nd):** same three numeric errors, plus two things askamerica lacked.
  - It reconciled $1.14T: gross interest $1,360B less trust-fund interest $218B. Net interest is $1,052B.
  - It gave the calendar-adjusted October deficit ($285B) and adjusted rise ($94B), the right context for the timing claim.
  - It did not verify the Sullivan quote or settle who said "Treasury twist".
- **everyman (3rd):** caught the $1.9T and $217B errors and the weak timing explanation, all from secondary sources.
  - Six items left "not checkable", including the Sullivan quote and the interest figure, both of which other arms resolved.
  - It did not compute the per-week figure.

**What would flip 1st and 2nd:** weighting the interest reconciliation and adjusted-deficit context above the two quote checks. It is close.

## Recipe check
No recipe proposed; first instance. What expert did that askamerica did not: when an article's interest figure does not match MTS net interest, test the gross-less-trust-fund definition before grading it unsupported. The warehouse cannot do this today (no gross interest table), so a recipe would have nothing to point at. Filed as a sourcing gap instead.

## Severity invariant
**Not triggered.** askamerica ranks above everyman.

## Defects
- Data gap: no gross interest expense on the public debt in the warehouse. Filed as kenstott/govdata-ops#1199.
