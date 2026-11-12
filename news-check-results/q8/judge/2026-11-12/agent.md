# q8 judge: Motley Fool "Social Security's Trump Bump-Led 2027 COLA Is a Double-Edged Sword" (Sean Williams, 2026-11-07)

Rank: [askamerica, everyman, expert]

Judged 2026-11-12. SSA.gov pages return 403 to fetch tools, so I verified against search excerpts of the 2026 Trustees Report, CRS R49500, Gallup releases, and a fetch of the article's own sentences.

## Ratings given
| Arm | Pinocchios | Primary sources read |
|---|---|---|
| askamerica | 1 | Medicare.gov, Gallup, ACASignups fetched; SSA blocked; CPI-W index arithmetic; warehouse holds only CPI-U |
| everyman | 1 | SSA/Trustees via search excerpts only; BLS CPI release |
| expert | 1 | KFF, MOAA via search; SSA blocked |

## Assessment
- **Correct verdict: 1 Pinocchio.** The hard numbers hold. The shading is in the framing and in two loosely sourced reliance and enrollment figures.
  - **3.6% COLA (CPI-W basis): true.** The official figure is 3.6%, and the COLA is built from the Q3 CPI-W average.
  - **Tie for fifth-largest since 1993: true.** Larger raises are 8.7, 5.9, 5.8 and 4.1; the 2012 COLA also equals 3.6. 1992 (3.7) is outside the stated window, so everyman's "sixth if 1992 counted" is moot.
  - **Sixth straight year of 2.5% or more: true** (5.9, 8.7, 3.2, 2.5, 2.8, 3.6).
  - **Part B $202.90 to a projected $209.50 (+3.25%): true as a Trustees projection.** The final CMS figure is due later in November, and the article does say "expected."
  - **OASI depletion in Q4 2032: true.**
  - **22% cut: true.** Continuing income covers 78% of OASI benefits at depletion.
  - **$29.3 trillion through 2100:** the report gives it as the combined OASDI open-group present value, not OASI-only. The article says "unfunded obligation" without saying OASDI, but it never calls it OASI. This is a labeling gap, not an error.
  - **Payroll tax above 91% of income:** consistent with Trustees income composition (askamerica: 91.3%).
  - **Gallup "80% to 90% of retirees consistently rely":** unsupported as phrased. Gallup's recent measure is about 62% of retirees calling Social Security a major source. A "receive/rely in some capacity" reading may reach the 80s, but the article presents it as consistent Gallup data. This is the one soft factual claim.
  - **29 million traditional Medicare enrollees:** not traceable. CMS shows about 34M traditional Medicare overall, so 29M is plausible for some subset (for example those on Social Security and Part B) but is unsourced.
  - **Causal story (tariffs and the Hormuz closure as the COLA's driver, "Trump bump"):** an energy shock is well supported (Sept gasoline +34% y/y, CPI-W +3.9%). Apportioning the COLA to specific policies is the author's interpretation and is asserted as fact.
  - **"May now be less than six years away" / larger COLA hastens insolvency:** speculation. It is directionally plausible but unquantified, and the Trustees date predates the COLA.
  - Net: accurate core with an overconfident causal frame, one loose Gallup figure and speculative solvency language. 1 Pinocchio.
- **askamerica (1st):** the most claims checked, with calibrated handling.
  - It checked the CPI-W arithmetic, the COLA rank, the Part B history versus COLAs, and payroll share.
  - It caught the Gallup 62% versus 80-90% discrepancy and the 29M versus 34.3M traditional Medicare figure. Neither other arm flagged Gallup, and only this one challenged 29M.
  - It separated "unsupported" from "false" and disclosed that the warehouse holds no CPI-W, COLA or Trustees tables.
  - Weakness: it labelled the solvency speculation "mostly false" when it is unverifiable speculation. It missed the OASDI vs OASI label on $29.3T.
- **everyman (2nd):** correct rating and good framing analysis.
  - It flagged the $29.3T OASDI vs OASI mix-up (the most precise label catch) and tied the energy-shock evidence to the tariff attribution.
  - Weaknesses: it never checked Gallup or payroll share, said 29M was "not checkable" rather than testing it, and its ranking discussion hedged to "borderline" when the 1993 window makes it a clean tie for fifth.
- **expert (3rd):** correct rating but the thinnest.
  - It accepted 29M as "64M - 35M MA roughly" without checking it against traditional-Medicare counts, skipped Gallup, left payroll share uncheckable, and did not engage with the COLA rank or the $29.3T label.
  - It did correctly mark the under-six-years claim as inference and the Part B figure as a projection.

## Recipe check
Not assessed beyond the arm outputs. The askamerica log was not in scope for this judging. Useful procedure for such pieces: a recipe step that maps each lede number (rank, reliance share, enrollment count) to a claim, and prefers Gallup and CMS enrollment tables over search summaries when SSA.gov is blocked.

## Severity invariant
**Not triggered:** askamerica ranks above everyman, and all three gave the correct 1 Pinocchio.

## Defects
- askamerica: marked the speculative solvency claim "mostly false" rather than unverifiable inference; did not notice the OASDI vs OASI label on $29.3T.
